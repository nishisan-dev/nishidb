package server_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
)

// amostraExposta é uma linha do /metrics já casada com o `# HELP` e o `# TYPE`
// da família.
//
// O valueOfMetrica que já existe aqui devolve só o número, e o número não
// basta nesta fase: metade do contrato de nishidb_storage_garbage_bytes está
// no TEXTO DE AJUDA, que é onde a ressalva de "limite inferior" viaja até o
// operador. Uma série publicada com o valor certo e a ajuda errada seria lida
// como contagem exata, que é exatamente o defeito que o comentário do M2
// evitou ao deixá-la de fora.
//
// O Tipo entra pela Task 8, e pelo mesmo motivo: nishidb_compaction_bytes_read
// e _bytes_written só servem ao painel como CONTADOR, porque o que o operador
// pergunta é a TAXA — quantos bytes por segundo a compaction está movendo. As
// mesmas linhas publicadas como medidor produzem um número que sobe para
// sempre e um `rate()` que o Prometheus recusa a calcular, e a diferença entre
// as duas leituras não aparece no valor: aparece no `# TYPE`.
// Os Labels entram pela Tarefa 16, e por um motivo que os campos anteriores não
// cobriam: as séries do índice por segmento só existem em conjunto com os
// rótulos. `nishidb_index_segmentos` sem o `estado` é um número sem pergunta, e
// o contrato que precisa de prova — quatro estados fechados, seis combinações
// SEMPRE emitidas — é inteiramente sobre eles. Um helper que descarta rótulo
// aprovaria uma implementação que publicasse a mesma série quatro vezes.
type amostraExposta struct {
	Name   string
	Ajuda  string
	Tipo   metrics.Type
	Labels map[string]string
	Value  float64
}

// coletaSamples raspa o /metrics e devolve as amostras com a ajuda da
// família já anexada a cada linha.
//
// Parseia o formato de exposição em vez de chamar o coletor direto porque o
// pacote de teste é externo (server_test): o que se prova aqui é o que o
// Prometheus enxerga, não o que a função interna devolveu antes de passar
// pelo Registry.
func coletaSamples(t *testing.T, srv *server.Server) []amostraExposta {
	t.Helper()
	var (
		out   []amostraExposta
		ajuda = make(map[string]string)
		tipos = make(map[string]metrics.Type)
	)
	for _, row := range strings.Split(fetch(t, srv, "/metrics"), "\n") {
		if row == "" {
			continue
		}
		if depois, achou := strings.CutPrefix(row, "# HELP "); achou {
			familia, text, _ := strings.Cut(depois, " ")
			ajuda[familia] = text
			continue
		}
		if depois, achou := strings.CutPrefix(row, "# TYPE "); achou {
			familia, text, _ := strings.Cut(depois, " ")
			tipos[familia] = tipoOfScrape(t, text)
			continue
		}
		if strings.HasPrefix(row, "#") {
			continue
		}
		i := strings.LastIndexByte(row, ' ')
		if i < 0 {
			t.Fatalf("linha sem valor no scrape: %q", row)
		}
		name, resto, _ := strings.Cut(row[:i], "{")
		v, err := strconv.ParseFloat(row[i+1:], 64)
		if err != nil {
			t.Fatalf("valor ilegível em %q: %v", row, err)
		}
		rotulos := rotulosOfScrape(t, row, resto)
		// A ajuda e o tipo são indexados pela FAMÍLIA, e o nome da linha pode
		// carregar sufixo de histograma (_bucket/_sum/_count). O sufixo não
		// chega às séries de disco nem às de compaction, mas o helper serve o
		// pacote inteiro.
		familia := familiaOf(name, ajuda)
		out = append(out, amostraExposta{
			Name: name, Ajuda: ajuda[familia], Tipo: tipos[familia], Labels: rotulos, Value: v,
		})
	}
	return out
}

// rotulosOfScrape desmonta o `{k="v",k2="v2"}` de uma linha de exposição.
//
// Escrito à mão, e não por Split na vírgula, porque o formato permite vírgula e
// aspa DENTRO do valor (escapadas), e um split ingênuo partiria a linha no lugar
// errado — devolvendo rótulos que ninguém publicou. O oráculo dos conjuntos
// fechados leria esse lixo como "valor de estado inesperado" e a falha apontaria
// para o produto em vez de para o parser.
//
// Linha sem `{` devolve nil, que é o certo: série sem rótulo nenhum.
func rotulosOfScrape(t *testing.T, row, dentro string) map[string]string {
	t.Helper()
	dentro = strings.TrimSuffix(dentro, "}")
	if dentro == "" {
		return nil
	}
	out := map[string]string{}
	for i := 0; i < len(dentro); {
		eq := strings.IndexByte(dentro[i:], '=')
		if eq < 0 || i+eq+1 >= len(dentro) || dentro[i+eq+1] != '"' {
			t.Fatalf("rótulo malformado em %q", row)
		}
		name := dentro[i : i+eq]
		var value strings.Builder
		j := i + eq + 2
		for ; j < len(dentro) && dentro[j] != '"'; j++ {
			if dentro[j] == '\\' && j+1 < len(dentro) {
				j++
				switch dentro[j] {
				case 'n':
					value.WriteByte('\n')
				default:
					value.WriteByte(dentro[j])
				}
				continue
			}
			value.WriteByte(dentro[j])
		}
		if j >= len(dentro) {
			t.Fatalf("aspa não fechada em %q", row)
		}
		out[name] = value.String()
		i = j + 1
		if i < len(dentro) && dentro[i] == ',' {
			i++
		}
	}
	return out
}

// achaSerie devolve a amostra da série com EXATAMENTE os rótulos pedidos.
//
// O achaAmostra acima devolve a primeira do nome e serve aos testes de um
// dataset só; este é o que as séries de índice exigem, porque elas saem quatro
// vezes por dataset com o mesmo nome e uma bancada com tabela histórica sobe
// DOIS datasets. Pegar a primeira ali seria pegar uma série qualquer e afirmar
// coisas sobre ela.
func achaSerie(samples []amostraExposta, name string, rotulos map[string]string) (amostraExposta, bool) {
	for _, a := range samples {
		if a.Name != name {
			continue
		}
		casa := true
		for k, v := range rotulos {
			if a.Labels[k] != v {
				casa = false
				break
			}
		}
		if casa {
			return a, true
		}
	}
	return amostraExposta{}, false
}

// tipoOfScrape traduz o `# TYPE` de volta ao tipo do registry.
//
// A tradução é feita no teste, e não lida do registry, de propósito: o que
// interessa aqui é o que o Prometheus LÊ na porta. Um coletor que devolvesse
// TypeCounter e um formatador que escrevesse "gauge" não teriam como ser
// distinguidos por uma asserção que perguntasse ao coletor.
//
// Tipo desconhecido é falha, e não um default silencioso: `untyped` é o que o
// formatador emite quando o Tipo não é nenhum dos três, e aceitá-lo aqui faria
// o oráculo do contador passar sobre uma série sem tipo nenhum.
func tipoOfScrape(t *testing.T, text string) metrics.Type {
	t.Helper()
	switch strings.TrimSpace(text) {
	case "counter":
		return metrics.TypeCounter
	case "gauge":
		return metrics.TypeGauge
	case "histogram":
		return metrics.TypeHistogram
	}
	t.Fatalf("tipo %q no scrape: o formato de exposição só reconhece counter, gauge e histogram", text)
	return metrics.TypeCounter
}

// familiaOf resolve o nome-base de uma linha: o próprio nome, quando ele é a
// família, ou o nome sem o sufixo de histograma.
func familiaOf(name string, ajuda map[string]string) string {
	if _, ok := ajuda[name]; ok {
		return name
	}
	for _, sufixo := range []string{"_bucket", "_sum", "_count"} {
		if base, achou := strings.CutSuffix(name, sufixo); achou {
			return base
		}
	}
	return name
}

// achaAmostra devolve a primeira amostra da série. Os testes deste pacote
// sobem processo de UM dataset só, então "primeira" e "única" coincidem.
func achaAmostra(samples []amostraExposta, name string) (amostraExposta, bool) {
	for _, a := range samples {
		if a.Name == name {
			return a, true
		}
	}
	return amostraExposta{}, false
}

// TestMetricaDeDiscoEhPublicadaComSemanticaDeLimite prova que storage_bytes
// sai como número exato e que garbage_bytes se declara limite INFERIOR no
// texto de ajuda. Publicar o segundo sem a ressalva repetiria o defeito que o
// comentário do M2 evitou: o tally de liveness é limite SUPERIOR (débitos 38
// e 45), então `total - live` só é honesto se vier dito que há PELO MENOS
// aquilo de lixo. Quem dimensiona compaction pelo número precisa da direção
// do erro junto com o número.
func TestMetricaDeDiscoEhPublicadaComSemanticaDeLimite(t *testing.T) {
	srv := serverWithData(t, 100, blockPadrao)
	samples := coletaSamples(t, srv)

	bytes, ok := achaAmostra(samples, metrics.MetStorageBytes)
	if !ok {
		t.Fatal("nishidb_storage_bytes não foi publicada")
	}
	if bytes.Value <= 0 {
		t.Errorf("storage_bytes = %v, queria > 0", bytes.Value)
	}

	garbage, ok := achaAmostra(samples, metrics.MetStorageGarbageBytes)
	if !ok {
		t.Fatal("nishidb_storage_garbage_bytes não foi publicada")
	}
	if !strings.Contains(garbage.Ajuda, "pelo menos") {
		t.Errorf("Ajuda = %q; precisa declarar que é limite inferior", garbage.Ajuda)
	}
	// O VALOR, e não só a presença dele. Sem update no seed — os ids são
	// S-%06d, um por evento —, todo record apendado continua vivo, e o único
	// byte em disco que o tally não reivindica é o header de cada segmento,
	// que não é record. Logo `disco - vivo` tem aqui um resultado EXATO e
	// previsível, e travá-lo é o que separa uma série calculada de uma
	// constante: sem esta linha, uma implementação que publicasse zero fixo
	// satisfaria "não é negativa" e "não passa do storage_bytes" para sempre
	// — métrica que mente pelo outro lado, uma curva de garbage travada no
	// chão enquanto o disco enche.
	//
	// Os dois números saem da MESMA raspagem, que é o que amarra o resultado
	// ao estado real do dataset em vez de a um literal: mudar o seed muda os
	// dois lados juntos.
	segmentos, ok := achaAmostra(samples, metrics.MetSegmentsTotal)
	if !ok {
		t.Fatal("nishidb_segments_total não foi publicada")
	}
	if expected := float64(segment.HeaderSize) * segmentos.Value; garbage.Value != expected {
		t.Errorf("garbage_bytes = %v, queria %v (%d bytes de header por segmento × %v segmentos; o seed não sobrescreve nenhum id, então nada além do header é lixo)",
			garbage.Value, expected, segment.HeaderSize, segmentos.Value)
	}
	if garbage.Value > bytes.Value {
		t.Errorf("garbage_bytes = %v acima de storage_bytes = %v", garbage.Value, bytes.Value)
	}

	// A terceira série do §92, nishidb_storage_live_bytes, continua FORA: ela
	// é o tally cru, e publicá-lo sob um nome que promete exatidão é a
	// métrica que mente. Sai quando a compaction reconstruir o tally por
	// varredura.
	if _, ok := achaAmostra(samples, metrics.MetStorageLiveBytes); ok {
		t.Error("nishidb_storage_live_bytes foi publicada: o tally ainda é limite superior")
	}

	// As outras duas declaradas e sem alimentador passam a ter a MESMA
	// proteção, e pela mesma razão: sem teste, "sem alimentador por decisão" e
	// "sem alimentador por esquecimento" são indistinguíveis, e a próxima
	// pessoa alimenta a série achando que fecha uma pendência.
	//
	// nishidb_ingest_events_rate: taxa derivada no exportador tem janela que o
	// operador não escolheu, e discordaria do contador ao lado. A pergunta é
	// rate() sobre o contador, com a janela declarada no alerta.
	if _, ok := achaAmostra(samples, metrics.MetIngestEventsRate); ok {
		t.Error("nishidb_ingest_events_rate foi publicada: a taxa é do alerta, não do exportador")
	}
	// nishidb_materialized_tombstones: exige contador de lápides vivas
	// mantido em applyRecord e reconstruível no replay. É milestone, não
	// encanamento de métrica.
	if _, ok := achaAmostra(samples, metrics.MetMaterializedTombstones); ok {
		t.Error("nishidb_materialized_tombstones foi publicada sem o contador de lápides vivas")
	}
}

// TestStorageBytesBateComOsArquivosEmDisco é o oráculo que faltava: a série
// tem de casar com a soma dos segment-*.dat, não apenas ser positiva.
//
// `Value > 0` não é oráculo, é sinal de vida. A revisão adversarial provou
// por mutação: publicar st.LiveBytesUpperBound — ou st.RowCount — no lugar de
// st.StorageBytes deixava a suíte inteira verde. Ou seja, exatamente a
// "métrica que mente" que o comentário de metrics_dataset.go declara estar
// evitando: o tally de liveness, que é limite SUPERIOR (débitos 38 e 45), sob
// um nome que promete tamanho de arquivo. É o débito 6 de novo — medir sem
// oráculo.
//
// A soma é medida por os.Stat, e não por segment.ListSegments, DE PROPÓSITO:
// ListSegments é a primitiva de onde o próprio StorageBytes sai, e um oráculo
// que reusasse a fonte da medida provaria só que a soma foi copiada. O que
// esta asserção afirma é a igualdade com o filesystem.
//
// A igualdade é EXATA, não aproximada, porque o seed fecha o store com
// Checkpoint antes de o servidor abrir e nada apenda depois: o segSize do
// último ListSegments descreve os arquivos que estão lá agora. Sob escrita
// concorrente a série é limite inferior por frescor — a ressalva de frescor
// está no godoc de Stats.StorageBytes —, mas aqui não há escrita concorrente,
// e afrouxar a asserção para uma tolerância devolveria o buraco por onde o
// mutante passou.
func TestStorageBytesBateComOsArquivosEmDisco(t *testing.T) {
	srv, dir := serverWithDataEDiretorio(t, 100, blockPadrao)

	amostra, ok := achaAmostra(coletaSamples(t, srv), metrics.MetStorageBytes)
	if !ok {
		t.Fatal("nishidb_storage_bytes não foi publicada")
	}

	entradas, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.dat"))
	if err != nil {
		t.Fatalf("glob dos segmentos: %v", err)
	}
	var inDisk int64
	for _, p := range entradas {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		inDisk += fi.Size()
	}
	if inDisk == 0 {
		t.Fatal("nenhum segmento em disco: o teste não estaria provando nada")
	}
	if int64(amostra.Value) != inDisk {
		t.Errorf("storage_bytes = %d e os arquivos em disco somam %d: a série não descreve o disco",
			int64(amostra.Value), inDisk)
	}
}

// TestStorageBytesSomaTodosOsSegmentosENaoApenasUm fecha a lacuna que o oráculo
// irmão deixa aberta: com um único segmento em disco, toda conta que percorre
// segmentos é indistinguível de uma que lê só o corrente. Medido — publicar
// `StorageBytes / SegmentsTotal` sobrevive à suíte inteira quando o fixture
// materializa um segmento só, porque a divisão vira identidade.
//
// Aqui o segmentSize é pequeno de propósito, para que a ingestão ROTACIONE: a
// série só está certa se for a SOMA dos arquivos, e é isso que a compaction do
// §53-55 vai exigir dela quando começar a remover segmentos do meio.
func TestStorageBytesSomaTodosOsSegmentosENaoApenasUm(t *testing.T) {
	srv, dir := serverWithDataDiretorioEStorage(t, 400, blockPadrao, "  segmentSize: 8KiB\n")

	entradas, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.dat"))
	if err != nil {
		t.Fatalf("glob dos segmentos: %v", err)
	}
	if len(entradas) < 2 {
		t.Fatalf("o dataset tem %d segmento(s): sem rotação este teste não distingue soma de leitura do corrente", len(entradas))
	}
	var inDisk int64
	for _, p := range entradas {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		inDisk += fi.Size()
	}

	amostra, ok := achaAmostra(coletaSamples(t, srv), metrics.MetStorageBytes)
	if !ok {
		t.Fatal("nishidb_storage_bytes não foi publicada")
	}
	if int64(amostra.Value) != inDisk {
		t.Errorf("storage_bytes = %d e os %d segmentos somam %d: a série não soma o disco inteiro",
			int64(amostra.Value), len(entradas), inDisk)
	}
}

// somaOfLogOfRetirement soma um campo numérico de todas as linhas
// `compaction.segmento_aposentado` do log do servidor.
//
// O log é o segundo lugar em que estes números aparecem — até esta tarefa ele
// era o ÚNICO ([compaction.go], godoc da linha) — e tem oráculo próprio do lado
// do storage (TestAposentadoriaRegistraNoLogOsBytesQueODiscoDevolveu), que
// amarra `bytes_lidos` ao arquivo em disco e `bytes_escritos` ao que os
// segmentos de destino cresceram. É por isso que ele serve de oráculo aqui: o
// número que sai pela porta tem de ser o MESMO que a aposentadoria registrou,
// e não um segundo número parecido.
//
// Lê o formato TEXTO porque é o que a bancada configura. O casamento é por
// prefixo de token (`bytes_lidos=`), o que atravessa sem susto o campo
// `message`, que é o único com espaço e por isso o único entre aspas.
func somaOfLogOfRetirement(t *testing.T, saida, field string) int64 {
	t.Helper()
	var total int64
	for _, row := range strings.Split(saida, "\n") {
		if !strings.Contains(row, "event=compaction.segmento_aposentado") {
			continue
		}
		for _, token := range strings.Fields(row) {
			value, achou := strings.CutPrefix(token, field+"=")
			if !achou {
				continue
			}
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				t.Fatalf("o campo %s saiu ilegível na linha %q: %v", field, row, err)
			}
			total += n
		}
	}
	return total
}

// TestSeriesDeCompactionGanhamAlimentador prova que as três séries do §92
// deixaram de ser nome sem número — o estado em que names.go as declarou desde
// o M2, e que a Task 8 encerra. É o critério dela na camada em que se AFERE
// número; o e2e pelo binário o cobra de novo, com o processo de verdade.
//
// O que cada asserção afirma, e por que nenhuma substitui a outra:
//
//   - as três SAEM. Name de série sem alimentador é o débito 16, e o painel
//     vazio não distingue "a compaction não rodou" de "ninguém publicou";
//   - `bytes_read` e `bytes_written` saem como CONTADOR. O painel do §86
//     pergunta a TAXA — quantos bytes por segundo a compaction move —, e ela
//     só existe sobre uma série monotônica. Publicá-las como medidor devolve
//     um número que sobe para sempre e um `rate()` que não se calcula, e a
//     diferença não aparece no valor: aparece no `# TYPE`;
//   - os dois VALORES batem com o log da própria aposentadoria, que é a única
//     outra fonte destes números e que tem oráculo contra o disco do lado do
//     storage. Sem esta parte, publicar uma constante — ou publicar `lidos` no
//     lugar de `escritos` — passaria por "a série existe e é positiva", que é
//     medir sem oráculo (débito 6);
//   - e a convergência só vale com `bytes_escritos` do log JÁ POSITIVO. Sem
//     essa guarda o teste é vacuoso na metade das corridas, e não por acaso:
//     o corpus é feito de rodadas sobre o mesmo key set, então quase todo
//     segmento eleito está 100% morto e é aposentado copiando ZERO record
//     vivo. O laço converge na primeira raspagem em que scrape e log
//     coincidem, e se ela chega antes do primeiro copy-forward a conferência
//     de `escritos` é 0 == 0 — medido: `Value: 0` fixo na amostra de
//     bytes_written sobrevivia a este pacote inteiro, e só o e2e o matava;
//   - `running` vale 1 com o laço de pé, e o teste irmão exige 0 com a chave
//     desligada. Um par que não se separa é constante disfarçada de medidor.
//
// A convergência é um LAÇO, e não uma leitura: os contadores sobem dentro da
// varredura e o log sai depois da aposentadoria, então uma raspagem apanhada no
// meio de uma passada legitimamente encontra a série à frente do log. O que não
// pode acontecer é os dois nunca se encontrarem — sem ingestão o dataset chega
// ao fim do que há para compactar, e aí as duas leituras têm de coincidir.
func TestSeriesDeCompactionGanhamAlimentador(t *testing.T) {
	srv, _, record, _ := benchOfCompaction(t, "true")

	var (
		samples           []amostraExposta
		lidos, escritos   amostraExposta
		lidosLog, escrLog int64
		convergiu         bool
		prazo             = time.Now().Add(60 * time.Second)
		faltando          string
	)
	for time.Now().Before(prazo) {
		samples = coletaSamples(t, srv)
		saida := record.String()
		lidosLog = somaOfLogOfRetirement(t, saida, "bytes_lidos")
		escrLog = somaOfLogOfRetirement(t, saida, "bytes_escritos")

		var okL, okE bool
		lidos, okL = achaAmostra(samples, metrics.MetCompactionBytesRead)
		escritos, okE = achaAmostra(samples, metrics.MetCompactionBytesWritten)
		switch {
		case !okL || !okE:
			faltando = "as séries de bytes não foram publicadas"
		case lidosLog == 0:
			faltando = "nenhuma aposentadoria no log: sem passada não há número a conferir"
		case escrLog == 0:
			faltando = "nenhuma aposentadoria copiou record vivo para frente ainda: com 0 escritos a conferência " +
				"abaixo seria 0 == 0 e uma constante zero na série passaria por ela"
		case int64(lidos.Value) != lidosLog || int64(escritos.Value) != escrLog:
			faltando = fmt.Sprintf("o scrape diz %d lidos / %d escritos e o log soma %d / %d",
				int64(lidos.Value), int64(escritos.Value), lidosLog, escrLog)
		default:
			convergiu = true
		}
		if convergiu {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !convergiu {
		t.Fatalf("as séries de compaction não alcançaram o que a aposentadoria registrou em 60s (%s):\n%s",
			faltando, record.String())
	}
	t.Logf("pela porta: %d bytes lidos e %d escritos, os mesmos que %d aposentadoria(s) registraram no log",
		int64(lidos.Value), int64(escritos.Value), strings.Count(record.String(), "event=compaction.segmento_aposentado"))

	if lidos.Tipo != metrics.TypeCounter {
		t.Errorf("%s saiu como %v, queria contador: o painel do §86 lê a TAXA de bytes movidos, e taxa não se calcula sobre medidor",
			metrics.MetCompactionBytesRead, lidos.Tipo)
	}
	if escritos.Tipo != metrics.TypeCounter {
		t.Errorf("%s saiu como %v, queria contador: o painel do §86 lê a TAXA de bytes movidos, e taxa não se calcula sobre medidor",
			metrics.MetCompactionBytesWritten, escritos.Tipo)
	}
	if escritos.Value >= lidos.Value {
		t.Errorf("a compaction leu %v bytes e escreveu %v: a distância entre os dois é o disco devolvido (§86), "+
			"e sem ela as duas séries estão medindo a mesma coisa", lidos.Value, escritos.Value)
	}

	rodando, ok := achaAmostra(samples, metrics.MetCompactionRunning)
	if !ok {
		t.Fatalf("%s não foi publicada", metrics.MetCompactionRunning)
	}
	if rodando.Tipo != metrics.TypeGauge {
		t.Errorf("%s saiu como %v, queria medidor: é estado, não acumulado", metrics.MetCompactionRunning, rodando.Tipo)
	}
	if rodando.Value != 1 {
		t.Errorf("%s = %v com o laço do §53-55 de pé e devolvendo disco, queria 1: "+
			"o operador não tem como distinguir uma compaction viva de uma que morreu no meio da noite",
			metrics.MetCompactionRunning, rodando.Value)
	}
}

// TestSeriesDeCompactionSaemZeradasComAChaveDesligada é a outra metade do par,
// e sem ela a primeira não prova nada: publicar `1` fixo no medidor e uma
// constante nos contadores satisfaria "a série existe e bate com o log" no
// dataset que compactou, e o painel mostraria compaction de pé num processo
// que honrou o `enabled: false` do operador.
//
// As três continuam SAINDO com a chave desligada, e isso é decisão e não
// descuido: série que some do scrape vira lacuna no gráfico, indistinguível de
// alvo fora do ar. O que muda é o número — zero é uma resposta, ausência não é.
func TestSeriesDeCompactionSaemZeradasComAChaveDesligada(t *testing.T) {
	srv, _ := serverWithDataDiretorioEStorage(t, 100, blockPadrao, "  compaction:\n    enabled: false\n")
	samples := coletaSamples(t, srv)

	for _, name := range []string{
		metrics.MetCompactionRunning,
		metrics.MetCompactionBytesRead,
		metrics.MetCompactionBytesWritten,
	} {
		a, ok := achaAmostra(samples, name)
		if !ok {
			t.Errorf("%s não foi publicada com a compaction desligada: buraco no gráfico não se distingue de alvo fora do ar", name)
			continue
		}
		if a.Value != 0 {
			t.Errorf("%s = %v com storage.compaction.enabled: false; queria 0, porque nada roda e nada foi lido ou escrito", name, a.Value)
		}
	}
}
