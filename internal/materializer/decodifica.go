package materializer

import (
	"errors"
	"sort"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// instanteDoAplica é o placeholder que o decode entrega ao Parse no lugar do
// relógio: o instante de verdade só é conhecido — e só pode ser lido — na
// aplicação, por carimbaRelogio.
//
// É o zero de time.Time, e não time.Now(), porque o zero é RUIDOSO. Ele vira
// um UnixMilli de -6.795.364.578.871 (ano 1754), que quebra qualquer teste de
// expiração de forma imediata e nomeável; o instante do decode passaria por
// correto em toda asserção e só apareceria em produção, como entidade que
// morre cedo.
var instanteDoAplica = time.Time{}

// Decodificado é um evento já tokenizado, resolvido contra o schema e
// convertido — tudo o que o Apply fazia ANTES de tocar o storage, junto dos
// buffers que essa parte reusa.
//
// POR QUE ELE É UM TIPO, e não um par de métodos sobre o Materializer. O
// decode é a metade cara e paralelizável do Apply (298 dos 902 µs/evento
// medidos no piloto em 2026-08-26); a aplicação é a metade que tem de ficar
// numa goroutine só, porque escreve. Enquanto houver um decodificador, um
// Decodificado basta. Para haver N, precisam existir N donos de buffer — e é
// esse dono que este tipo é.
//
// O PatchBuffer é EMBUTIDO, e não apontado, porque todo buffer do decode já
// mora nele: arena, entries, os parsers por nível da segunda passada, o
// scratch da folha e o buffer de nome. Separar os dois criaria dois donos para
// a mesma vida, e a marca d'água de memória por slot passaria a ser somada em
// dois lugares.
type Decodificado struct {
	PatchBuffer

	// evento é uma JANELA sobre os bytes recebidos, não uma cópia. Ele
	// precisa sobreviver até a aplicação por duas razões: a tabela histórica
	// grava o evento cru na coluna `_raw`, e o evento que não parseia só é
	// gravado como ruim lá, não aqui.
	//
	// Quem entrega o evento é dono de mantê-lo vivo até o Aplica retornar. No
	// caminho Kafka isso é o `kgo.Record`, que o franz-go não reutiliza.
	evento []byte

	// erro é o desfecho do decode, CRU: sem classificação e sem contagem.
	//
	// Classificar aqui seria barato e errado. `failure` incrementa
	// `byCat`/`invalid`/`failed`, e contador é efeito: um evento decodificado
	// duas vezes — que é exatamente o que o reprocesso por descoberta faz —
	// contaria dois. O balde do §101 é decidido por quem aplica, uma vez.
	erro error

	// precisaDoWriter marca o evento que o decode não pôde terminar porque
	// teria de CRIAR metadado — coluna nova ou tipo fixado. O conteúdo do
	// Decodificado é lixo parcial neste caso, e o único uso legítimo dele é o
	// `evento`: quem aplica refaz o parse do zero, pelo caminho completo, na
	// goroutine que pode escrever.
	precisaDoWriter bool
}

// Decodifica converte o evento em entries tipadas e ordenadas, sem tocar no
// storage e SEM LER O RELÓGIO.
//
// A ausência do relógio aqui é desenho, não economia. O carimbo de
// `_writtenAt` é o que governa o idle timeout e, desde a agenda de vencimentos
// (§57), também o instante em que a linha vence: vencimento = `_writtenAt` +
// `_expiresIn`. Um decode adiantado — pelo tempo de fila, que sob backpressure
// é a fila inteira — deslocaria esse relógio para trás em silêncio, e a
// entidade morreria cedo. O Parse recebe o instante ZERO de propósito: se o
// carimbo do Aplica falhar, a coluna sai com um timestamp absurdo (ano 1754) e
// o defeito é visível, em vez de ser um deslocamento de alguns segundos que
// nenhum teste distingue de ruído.
//
// O decode é SOMENTE LEITURA sobre o schema: ele resolve contra as colunas que
// já existem e para no primeiro metadado que teria de criar, marcando
// precisaDoWriter. Isso não é precaução — é o que mantém o FieldID
// determinístico. LookupOrCreate atribui o id na ordem de chamada, o id é
// formato de disco e a posição da coluna no `SELECT *` é contrato do driver
// JDBC; com N decodificadores, essa ordem viraria a de quem ganha o mutex.
//
// Vale MESMO com um decodificador só. Um caminho de código, dimensionado
// diferente: se o serial usasse o caminho completo e só o paralelo usasse este,
// a suíte inteira exercitaria o caminho que não é o de produção, e o
// reprocesso — a parte nova e a que pode errar — não teria cobertura nenhuma.
//
// Não devolve erro: o desfecho fica em d.erro, porque quem transforma erro em
// categoria, contador e política é o Aplica.
func (m *Materializer) Decodifica(d *Decodificado, event []byte) {
	d.evento = event
	d.soLeitura = true
	d.precisaDoWriter = false
	d.erro = d.Parse(event, m.schema, instanteDoAplica)
	if errors.Is(d.erro, errPrecisaDoWriter) {
		d.precisaDoWriter, d.erro = true, nil
	}
}

// reprocessa refaz o decode de um evento pelo caminho COMPLETO, na goroutine
// do writer, e devolve o Decodificado do writer no lugar do que abortou.
//
// É a segunda metade do desenho da Fase 4.2. O decodificador para no primeiro
// metadado que teria de criar; aqui a coluna nasce e o tipo é fixado, na ordem
// de offset, por uma goroutine só — a ordem que a execução serial produziria, e
// portanto o mesmo FieldID e a mesma posição no `SELECT *`.
//
// Refaz o evento INTEIRO, e não "continua de onde parou". Continuar exigiria
// que o estado parcial do decodificador fosse transportável e confiável, e ele
// é declaradamente lixo depois de um erro (ver o godoc do Parse). Mais: um
// documento com onze chaves novas cria as onze nesta única passagem, então o
// custo é um parse dobrado por EVENTO de descoberta, não por chave.
//
// Em regime o custo é zero: o piloto tem ~1.700 colunas e não descobre mais
// nenhuma. O caso ruim — origem que emite chave nova a cada evento — degrada
// para a vazão de antes da Fase 4, nunca para menos, e aparece em
// nishidb_ingest_decode_serial_total.
func (m *Materializer) reprocessa(evento []byte) *Decodificado {
	m.metrics.decodeSerial.Add(1)
	antes := m.schema.CamposConhecidos()

	d := &m.patch
	d.evento = evento
	d.soLeitura = false
	d.precisaDoWriter = false
	d.erro = d.Parse(evento, m.schema, instanteDoAplica)

	m.avaliaReprocessoInutil(m.schema.CamposConhecidos() > antes)
	return d
}

// avaliaReprocessoInutil dispara, uma vez, o aviso de que o reprocesso deixou
// de ser transitório.
//
// O QUE ELE DETECTA. Todo reprocesso legítimo produz metadado: uma coluna nova,
// ou o tipo de uma coluna auto. Os dois são limitados pelo número de colunas do
// dataset, então uma sequência longa de reprocessos que NÃO faz o dicionário
// crescer não é descoberta em curso — é o decodificador pedindo o writer para
// alguma coisa que o writer não entrega. O caso conhecido é o dicionário no
// teto sob `onMaxCols: ignore`: o campo nunca vira coluna, e todo evento que o
// traga volta a ser reprocessado, para sempre.
//
// A fixação de tipo não faz o dicionário crescer e por isso "suja" a série,
// mas ela é limitada pelo número de colunas: mil delas seguidas sem uma coluna
// nova é impossível num dataset saudável.
//
// O limiar é a única grandeza arbitrária daqui, e está declarada como tal:
// grande o bastante para que uma rajada de descoberta com N workers — em que
// vários decodificadores pedem a MESMA coluna e só o primeiro reprocesso a cria
// — não o alcance, e pequeno o bastante para caber em minutos de ingestão real.
const reprocessosInuteisParaAvisar = 1000

func (m *Materializer) avaliaReprocessoInutil(criouColuna bool) {
	if criouColuna {
		m.reprocessosInuteis = 0
		return
	}
	m.reprocessosInuteis++
	if m.reprocessosInuteis < reprocessosInuteisParaAvisar {
		return
	}
	if m.aoReprocessar == nil || !m.avisouReprocesso.CompareAndSwap(false, true) {
		return
	}
	m.aoReprocessar(m.reprocessosInuteis, m.schema.CamposConhecidos())
}

// AoReprocessar registra o aviso de reprocesso improdutivo. Chamado na fiação
// do dataset, antes de a ingestão começar — como os avisos irmãos do schema.
func (m *Materializer) AoReprocessar(fn func(seguidos int64, colunas int)) { m.aoReprocessar = fn }

// carimbaRelogio grava o instante da ESCRITA nas entries que o representam.
//
// São duas famílias e elas têm o mesmo dono: o campo de sistema `_writtenAt`
// (schema.Escrita) e as congeladas cuja origem é o relógio (`@writeTime`). O
// Parse emitiu as duas com o instante zero; aqui elas recebem o instante de
// verdade, uma vez só, o mesmo para o evento inteiro.
//
// A busca é BINÁRIA, e não uma varredura. Quando o Parse retorna, as entries
// já estão ordenadas por FieldID e sem duplicatas, e um documento do piloto
// traz ~1.650 delas: varrer todas para achar uma ou duas seria pagar uma
// passada inteira por evento no caminho quente. É a mesma escolha que o
// `slotDe` do prazo já faz, e pela mesma razão.
func (d *Decodificado) carimbaRelogio(c *schema.Compiled, agoraMs int64) {
	if c.Escrita != nil {
		d.carimba(c.Escrita.ID, agoraMs)
	}
	for _, cf := range c.Congeladas {
		if cf.DoRelogio {
			d.carimba(cf.ID, agoraMs)
		}
	}
}

// carimba escreve agoraMs na entry do campo, se ela existir.
//
// A ausência é legítima e silenciosa: um dataset sem `expireAfterWrite` não
// tem `_writtenAt`, e uma congelada de relógio só entra em entries quando o
// Parse a capturou.
func (d *Decodificado) carimba(id types.FieldID, agoraMs int64) {
	i := sort.Search(len(d.entries), func(i int) bool { return d.entries[i].field.ID >= id })
	if i < len(d.entries) && d.entries[i].field.ID == id {
		d.entries[i].val.I64 = agoraMs
	}
}
