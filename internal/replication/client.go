package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// Client is the follower-side HTTP loop. Storage generation management stays
// in Replica callbacks; Client only decides snapshot versus incremental tail.
type Client struct {
	BaseURL  string
	Dataset  string
	Username string
	Password string
	HTTP     *http.Client
	Replica  *Replica
	// RequireHistory é o follower que declara a tabela histórica: diante de
	// um master que anuncia historyExcluded, ele recusa (ErrHistoryNotPublished,
	// estado history_unavailable) em vez de seguir com a histórica vazia.
	RequireHistory bool
	// Logger recebe o ERROR da histórica indisponível. Nil descarta.
	Logger *logging.Logger
}

// HTTPError reports a non-success response from the master replication API.
type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("replication: HTTP %d: %s", e.Status, e.Message)
}

func (c *Client) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) endpoint(path string, q url.Values) (*http.Request, error) {
	if c.BaseURL == "" || c.Dataset == "" || c.Replica == nil {
		return nil, fmt.Errorf("replication: client requires base URL, dataset and replica")
	}
	u := strings.TrimRight(c.BaseURL, "/") + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return http.NewRequest(http.MethodGet, u, nil)
}

func (c *Client) get(ctx context.Context, path string, q url.Values, out any) error {
	r, err := c.endpoint(path, q)
	if err != nil {
		return err
	}
	r = r.WithContext(ctx)
	if c.Username != "" {
		r.SetBasicAuth(c.Username, c.Password)
	}
	res, err := c.client().Do(r)
	if err != nil {
		return &transportError{err: err}
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
		return &HTTPError{Status: res.StatusCode, Message: string(b)}
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		// Corpo truncado (o master derrubou a conexão no meio do snapshot) é
		// falha de transporte, como a da conexão.
		return &transportError{err: err}
	}
	return nil
}

// transportError marca a falha de falar com o master (conexão, corpo
// ilegível). Com o HTTPError, é o que o Follow trata como desconexão.
type transportError struct{ err error }

func (e *transportError) Error() string { return "replication: " + e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// isDisconnect diz se o erro é de transporte ou de HTTP: só ele é desconexão
// (retomável a partir de catching_up/following). Erro local — o apply que o
// Store recusou, uma resposta que viola o protocolo — não é: repetir o tail
// daria o mesmo erro.
func isDisconnect(err error) bool {
	var he *HTTPError
	var te *transportError
	return errors.As(err, &he) || errors.As(err, &te)
}

// SyncOnce advances one snapshot or one batch. It returns ErrResyncRequired
// when the server says the cursor is no longer retained; callers normally call
// it again, which then obtains a fresh snapshot.
//
// Ordem das decisões, depois do cursor do master:
//  1. com RequireHistory e o master anunciando historyExcluded, devolve
//     ErrHistoryNotPublished sem aplicar nada (estado history_unavailable);
//  2. em catching_up, following ou na desconexão que veio de um dos dois, e
//     com a MESMA epoch do master, segue pelo tail: é a retomada depois do
//     reinício limpo do master. Um 409 cai no resync; um 410 (o master
//     encerrando) e um 503 (a conferência de rewind do master ainda
//     pendente) são desconexão, que guarda o cursor, e a próxima rodada
//     tenta o tail de novo. O 503 nunca leva ao snapshot: se a conferência
//     achar o rewind, é a geração nova que manda o follower a ele;
//  3. qualquer outro caso baixa um snapshot.
func (c *Client) SyncOnce(ctx context.Context) error {
	_, err := c.syncOnce(ctx)
	return err
}

// syncProgress é o que uma rodada bem-sucedida provou: nada (snapshot ainda
// em catching_up, lote sem entrada nova), o tail andou, ou o snapshot chegou
// a following.
type syncProgress uint8

const (
	progressNone syncProgress = iota
	progressTail
	progressFollowing
)

// syncOnce é o SyncOnce que também diz o PROGRESSO da rodada. É o que o
// Follow usa para decidir quando o backoff volta ao mínimo.
func (c *Client) syncOnce(ctx context.Context) (syncProgress, error) {
	var remote Status
	if err := c.get(ctx, "/v1/replication/cursor", url.Values{"dataset": {c.Dataset}}, &remote); err != nil {
		return progressNone, err
	}
	if c.RequireHistory && remote.HistoryExcluded {
		if c.Replica.HistoryUnavailable() {
			c.logger().Error("replication.follower.history_unavailable",
				"the follower declares the history table and the master does not publish it (replication.publishHistory: false); nothing is applied until the master publishes it",
				"dataset", c.Dataset, "epoch", remote.Epoch)
		}
		return progressNone, ErrHistoryNotPublished
	}
	epoch, cursor, tail := c.Replica.TailCursor()
	if !tail || epoch != remote.Epoch {
		var snap Snapshot
		if err := c.get(ctx, "/v1/replication/snapshot", url.Values{"dataset": {c.Dataset}}, &snap); err != nil {
			return progressNone, err
		}
		if err := c.Replica.StartSnapshot(snap); err != nil {
			return progressNone, err
		}
		if err := c.Replica.FinishSnapshot(); err != nil {
			return progressNone, err
		}
		var latest Status
		if err := c.get(ctx, "/v1/replication/cursor", url.Values{"dataset": {c.Dataset}}, &latest); err != nil {
			return progressNone, err
		}
		if latest.Epoch == snap.Epoch && latest.Produced == snap.Cursor {
			if err := c.Replica.MarkFollowing(); err != nil {
				return progressNone, err
			}
			return progressFollowing, nil
		}
		// Snapshot aplicado, mas ainda em catching_up: progresso só quando o
		// tail andar.
		return progressNone, nil
	}
	b, err := func() (*Batch, error) {
		var out Batch
		e := c.get(ctx, "/v1/replication/batch", url.Values{"dataset": {c.Dataset}, "epoch": {epoch}, "after": {strconv.FormatUint(cursor, 10)}}, &out)
		return &out, e
	}()
	if err != nil {
		var he *HTTPError
		if errors.As(err, &he) && he.Status == http.StatusConflict {
			c.Replica.RequireResync()
			return progressNone, ErrResyncRequired
		}
		return progressNone, err
	}
	if err := c.Replica.ApplyBatch(b); err != nil {
		return progressNone, err
	}
	if _, _, applied := c.Replica.State(); applied > cursor {
		return progressTail, nil
	}
	return progressNone, nil
}

func (c *Client) logger() *logging.Logger {
	if c.Logger == nil {
		return logging.Discard()
	}
	return c.Logger
}

// Follow keeps the follower current until cancellation. A resync response is
// intentionally retried through SyncOnce: the next call sees the resync state
// and downloads a new snapshot.
//
// O backoff (200 ms dobrando até 5 s) só volta ao mínimo com progresso: um
// lote do tail que avançou o cursor, ou o snapshot que chegou a following sem
// erro local pendente. Depois de um erro local, só o tail que anda o zera.
//
// Só erro de transporte ou de HTTP é desconexão (retomável). A histórica
// indisponível não é: o estado history_unavailable fica, e a retentativa com
// backoff só o deixa quando o master volta a publicar a histórica; a rodada
// seguinte baixa então um snapshot. Qualquer outro erro é local (o apply que
// o Store recusou, por exemplo) e leva a resync_required: a rodada seguinte,
// depois do backoff, baixa um snapshot em vez de repetir o tail.
func (c *Client) Follow(ctx context.Context) error {
	backoff := followMinBackoff
	// localFailure: um erro local (apply recusado pelo Store) mandou o
	// follower ao snapshot, e ainda não houve progresso que prove que ele
	// passou. Enquanto ele vale, o snapshot bem-sucedido NÃO zera o backoff:
	// um erro determinístico só no tail faria um snapshot completo a cada
	// volta mínima, para sempre.
	localFailure := false
	for {
		progress, err := c.syncOnce(ctx)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return err
			}
			if errors.Is(err, ErrResyncRequired) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			switch {
			case errors.Is(err, ErrHistoryNotPublished):
				// O estado history_unavailable fica até o master mudar.
			case isDisconnect(err):
				c.Replica.Disconnect()
			default:
				// Erro local: o caminho é o snapshot, com backoff, e nunca a
				// retomada do tail que falhou. O ERROR sai na transição, e
				// não a cada volta.
				if !localFailure {
					c.logApplyFailed(err)
				}
				localFailure = true
				c.Replica.RequireResync()
			}
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				if !t.Stop() {
					<-t.C
				}
				return ctx.Err()
			case <-t.C:
			}
			backoff = min(2*backoff, followMaxBackoff)
			continue
		}
		// O tail que anda prova que a falha local passou; o snapshot que
		// chega a following só vale sem falha local pendente (ele não passa
		// pela entrada que falhou).
		if progress == progressTail || (progress == progressFollowing && !localFailure) {
			localFailure = false
			backoff = followMinBackoff
		}
	}
}

// Backoff do Follow entre rodadas que falharam.
const (
	followMinBackoff = 200 * time.Millisecond
	followMaxBackoff = 5 * time.Second
)

// logApplyFailed emite o ERROR da falha local, com a entrada quando ela é
// conhecida (ApplyError).
func (c *Client) logApplyFailed(err error) {
	args := []any{"dataset", c.Dataset, "error", err.Error()}
	var ae *ApplyError
	if errors.As(err, &ae) {
		args = append(args, "sequence", ae.Sequence, "table", ae.Table)
	}
	c.logger().Error("replication.follower.apply_failed",
		"the follower could not apply a replicated change locally; it resynchronizes with a snapshot, with growing backoff while the failure repeats",
		args...)
}
