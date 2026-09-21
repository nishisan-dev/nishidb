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
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
		return &HTTPError{Status: res.StatusCode, Message: string(b)}
	}
	return json.NewDecoder(res.Body).Decode(out)
}

// SyncOnce advances one snapshot or one batch. It returns ErrResyncRequired
// when the server says the cursor is no longer retained; callers normally call
// it again, which then obtains a fresh snapshot.
func (c *Client) SyncOnce(ctx context.Context) error {
	var remote Status
	if err := c.get(ctx, "/v1/replication/cursor", url.Values{"dataset": {c.Dataset}}, &remote); err != nil {
		return err
	}
	state, epoch, cursor := c.Replica.State()
	if (state != ReplicaCatchingUp && state != ReplicaFollowing) || epoch != remote.Epoch {
		var snap Snapshot
		if err := c.get(ctx, "/v1/replication/snapshot", url.Values{"dataset": {c.Dataset}}, &snap); err != nil {
			return err
		}
		if err := c.Replica.StartSnapshot(snap); err != nil {
			return err
		}
		if err := c.Replica.FinishSnapshot(); err != nil {
			return err
		}
		var latest Status
		if err := c.get(ctx, "/v1/replication/cursor", url.Values{"dataset": {c.Dataset}}, &latest); err != nil {
			return err
		}
		if latest.Epoch == snap.Epoch && latest.Produced == snap.Cursor {
			return c.Replica.MarkFollowing()
		}
		return nil
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
			return ErrResyncRequired
		}
		return err
	}
	return c.Replica.ApplyBatch(b)
}

// Follow keeps the follower current until cancellation. A resync response is
// intentionally retried through SyncOnce: the next call sees the resync state
// and downloads a new snapshot.
func (c *Client) Follow(ctx context.Context) error {
	backoff := 200 * time.Millisecond
	for {
		if err := c.SyncOnce(ctx); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return err
			}
			if errors.Is(err, ErrResyncRequired) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.Replica.Disconnect()
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				if !t.Stop() {
					<-t.C
				}
				return ctx.Err()
			case <-t.C:
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
			continue
		}
		backoff = 200 * time.Millisecond
	}
}
