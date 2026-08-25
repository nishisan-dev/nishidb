package cli

import (
	"errors"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// runOffsets é o comando administrativo de posição no tópico.
//
// Existe porque três decisões do M2 precisam de uma saída DENTRO do produto,
// e a spec não previu nenhuma:
//
//   - `errors.invalidEvent: stop` para a ingestão exatamente no evento
//     problemático. Sem um jeito de pular esse evento, `stop` seria uma pílula
//     de veneno: o reinício reproduz o mesmo evento e para de novo.
//   - Dataset com linhas e sem posição (restore parcial) recusa a subida. A
//     alternativa a este comando seria o operador apagar o dataset.
//   - Trocar de tópico ou de cluster recusa a subida. A troca LEGÍTIMA
//     (promoção de teste para produção) precisa de um ato explícito.
//
// O reposicionamento é operação de ADMINISTRAÇÃO e não campo de arquivo: o
// §49 torna o checkpoint autoritativo, e uma chave de YAML que reposiciona
// seria aplicada de novo a cada reinício, silenciosamente.
//
// Toma o flock do dataset, logo exige o servidor parado. É proposital: mexer
// na posição com o consumer de pé faria o writer sobrescrever a decisão no
// checkpoint seguinte.
func runOffsets(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("offsets", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		argConfig = fs.String("config", "", "caminho do nishidb.yaml")
		dataset   = fs.String("dataset", "", "nome do dataset")
		mostrar   = fs.Bool("show", false, "imprime a posição persistida")
		reiniciar = fs.String("reset", "", "earliest|latest — descarta a posição e deixa o autoOffsetReset valer")
		definir   = fs.String("set", "", "particao:offset[,particao:offset...]")
		confirmar = fs.Bool("yes", false, "confirma a escrita (obrigatório com --reset e --set)")
	)
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	if *argConfig == "" || *dataset == "" {
		fmt.Fprintln(amb.Stderr, "offsets: --config e --dataset são obrigatórios")
		return ExitUso
	}
	acoes := 0
	for _, ligada := range []bool{*mostrar, *reiniciar != "", *definir != ""} {
		if ligada {
			acoes++
		}
	}
	if acoes != 1 {
		fmt.Fprintln(amb.Stderr, "offsets: escolha exatamente uma ação: --show, --reset ou --set")
		return ExitUso
	}

	cfg, err := nishiconfig.CarregaConfig(*argConfig)
	if err != nil {
		fmt.Fprintf(amb.Stderr, "offsets: %v\n", err)
		return ExitUso
	}
	spec, err := schema.Load(cfg.SpecDoDataset(*dataset), cfg.DefaultsDeQuery())
	if err != nil {
		fmt.Fprintf(amb.Stderr, "offsets: %v\n", err)
		return ExitUso
	}
	if spec.Source.Kafka == nil {
		fmt.Fprintf(amb.Stderr, "offsets: o dataset %q não declara source.kafka\n", *dataset)
		return ExitUso
	}

	// Os campos com bitmap PRECISAM ser declarados: o Store recusa abrir um
	// dataset cujo manifest declara índices que a abertura não conhece,
	// porque o índice pararia de ser atualizado e reapareceria incompleto.
	bitmaps := schema.CamposComBitmap(spec)
	st, err := storage.Open(storage.Options{
		Dir:              cfg.DirDoDataset(*dataset),
		Dataset:          *dataset,
		SegmentSize:      cfg.SegmentSize(),
		BitmapFieldNames: bitmaps,
		ColunasDerivadas: spec.NomesDasExtracoes(),
		ExpireAfterWrite: spec.ExpiraApos(),
		CampoDeEscrita:   schema.CampoInstanteDeEscrita,
	})
	if err != nil {
		fmt.Fprintf(amb.Stderr, "offsets: %v\n", err)
		return codigoDoErro(err)
	}
	defer st.Close()

	atual, existe, errLeitura := st.SourceOffsets()

	switch {
	case *mostrar:
		return imprimeOffsets(amb, *dataset, st.RowCount(), atual, existe, errLeitura)

	case *reiniciar != "":
		if *reiniciar != schema.AutoOffsetResetEarliest && *reiniciar != schema.AutoOffsetResetLatest {
			fmt.Fprintf(amb.Stderr, "offsets: --reset %q inválido (%s|%s)\n",
				*reiniciar, schema.AutoOffsetResetEarliest, schema.AutoOffsetResetLatest)
			return ExitUso
		}
		if !*confirmar {
			fmt.Fprintf(amb.Stderr,
				"offsets: --reset %s DESCARTA a posição de %q e reprocessa ou pula uma janela do tópico. "+
					"Confirme com --yes\n", *reiniciar, *dataset)
			return ExitUso
		}
		// Reset é a posição VAZIA com a origem do YAML: na próxima subida o
		// dataset estará "sem posição", e o autoOffsetReset governa. O
		// RowCount deixa de bloquear porque a decisão foi tomada aqui, de
		// forma explícita e registrada.
		nova := checkpoint.SourceOffsets{
			Origem:  origemDaSpec(spec),
			Offsets: map[int32]int64{},
		}
		if err := grava(st, nova); err != nil {
			fmt.Fprintf(amb.Stderr, "offsets: %v\n", err)
			return codigoDoErro(err)
		}
		fmt.Fprintf(amb.Stdout, "offsets: posição de %q descartada; a próxima subida usa autoOffsetReset=%s\n",
			*dataset, *reiniciar)
		return ExitOK

	default:
		offsets, err := parseOffsets(*definir)
		if err != nil {
			fmt.Fprintf(amb.Stderr, "offsets: %v\n", err)
			return ExitUso
		}
		if !*confirmar {
			fmt.Fprintf(amb.Stderr,
				"offsets: --set sobrescreve a posição de %q por %s. Confirme com --yes\n",
				*dataset, formataOffsets(offsets))
			return ExitUso
		}
		nova := checkpoint.SourceOffsets{Origem: origemDaSpec(spec), Offsets: offsets}
		if err := grava(st, nova); err != nil {
			fmt.Fprintf(amb.Stderr, "offsets: %v\n", err)
			return codigoDoErro(err)
		}
		fmt.Fprintf(amb.Stdout, "offsets: posição de %q definida como %s\n", *dataset, formataOffsets(offsets))
		return ExitOK
	}
}

// origemDaSpec reconstrói a identidade a partir do YAML. O cluster id fica
// vazio: quem o preenche é a primeira subida, que fala com o broker — e a
// Divergencia trata o vazio como "não confere", não como "divergiu".
func origemDaSpec(spec *schema.DatasetSpec) checkpoint.Origem {
	k := spec.Source.Kafka
	return checkpoint.Origem{
		Topic:     k.Topic,
		GroupID:   k.Consumer.GroupID,
		Bootstrap: checkpoint.CanonizaBootstrap(k.BootstrapServers),
	}
}

func grava(st *storage.Store, o checkpoint.SourceOffsets) error {
	if err := st.SetSourceOffsets(o); err != nil {
		return err
	}
	return st.Checkpoint()
}

func imprimeOffsets(amb Ambiente, dataset string, rows uint64,
	o checkpoint.SourceOffsets, existe bool, errLeitura error,
) int {
	fmt.Fprintf(amb.Stdout, "dataset: %s\nrows: %d\n", dataset, rows)
	switch {
	case errLeitura != nil:
		fmt.Fprintf(amb.Stdout, "posição: ILEGÍVEL (%v)\n", errLeitura)
		if rows > 0 {
			fmt.Fprintln(amb.Stdout,
				"a ingestão NÃO sobe neste estado: use --reset ou --set para decidir explicitamente")
		}
		return ExitDataset
	case !existe:
		fmt.Fprintln(amb.Stdout, "posição: ausente")
		if rows > 0 {
			fmt.Fprintln(amb.Stdout,
				"o dataset tem linhas e não tem posição: a ingestão NÃO sobe até --reset ou --set")
		}
		return ExitOK
	}
	fmt.Fprintf(amb.Stdout, "topic: %s\ngroupId: %s\nbootstrap: %s\n",
		o.Origem.Topic, o.Origem.GroupID, o.Origem.Bootstrap)
	if o.Origem.ClusterID != "" {
		fmt.Fprintf(amb.Stdout, "cluster: %s\n", o.Origem.ClusterID)
	}
	if len(o.Offsets) == 0 {
		fmt.Fprintln(amb.Stdout, "offsets: nenhum (a próxima subida usa autoOffsetReset)")
		return ExitOK
	}
	for _, p := range o.Particoes() {
		fmt.Fprintf(amb.Stdout, "offset: %d=%d\n", p, o.Offsets[p])
	}
	return ExitOK
}

// parseOffsets lê "0:100,1:250".
func parseOffsets(s string) (map[int32]int64, error) {
	out := map[int32]int64{}
	for _, par := range strings.Split(s, ",") {
		par = strings.TrimSpace(par)
		if par == "" {
			continue
		}
		p, o, ok := strings.Cut(par, ":")
		if !ok {
			return nil, fmt.Errorf("--set %q inválido (esperado particao:offset)", par)
		}
		particao, err := strconv.ParseInt(strings.TrimSpace(p), 10, 32)
		if err != nil || particao < 0 {
			return nil, fmt.Errorf("--set %q: partição inválida", par)
		}
		offset, err := strconv.ParseInt(strings.TrimSpace(o), 10, 64)
		if err != nil || offset < 0 {
			return nil, fmt.Errorf("--set %q: offset inválido (>= 0)", par)
		}
		if _, dup := out[int32(particao)]; dup {
			return nil, fmt.Errorf("--set: partição %d repetida", particao)
		}
		out[int32(particao)] = offset
	}
	if len(out) == 0 {
		return nil, errors.New("--set vazio")
	}
	return out, nil
}

func formataOffsets(o map[int32]int64) string {
	ps := make([]int, 0, len(o))
	for p := range o {
		ps = append(ps, int(p))
	}
	sort.Ints(ps)
	partes := make([]string, 0, len(ps))
	for _, p := range ps {
		partes = append(partes, fmt.Sprintf("%d=%d", p, o[int32(p)]))
	}
	return strings.Join(partes, " ")
}
