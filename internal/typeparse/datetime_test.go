package typeparse

import (
	"errors"
	"strings"
	"testing"
	"testing/quick"
	"time"
	// Embute o banco de fusos no binário de TESTE: os casos de DST abaixo
	// dependem de regras históricas que um host sem /usr/share/zoneinfo não
	// teria, e um teste que vira skip silencioso não prova nada. O produto
	// não importa isto — lá vale o time.Local do host.
	_ "time/tzdata"
)

const padraoPiloto = "MMM d, yyyy hh:mm:ss a"

func mustCompila(t *testing.T, padrao string, d Dominio) *Layout {
	t.Helper()
	l, err := CompilaLayout(padrao, d)
	if err != nil {
		t.Fatalf("CompilaLayout(%q): %v", padrao, err)
	}
	return l
}

func mustLoc(t *testing.T, nome string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(nome)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", nome, err)
	}
	return loc
}

// ---------- compilação ----------

func TestCompilaLayoutRecusaTokenDesconhecido(t *testing.T) {
	for _, padrao := range []string{"yyyy-MM-dd EEE", "yyyy-QQ-dd", "yyyy-MM-dd HH:mm:ss zzz"} {
		if _, err := CompilaLayout(padrao, DominioDateTime); err == nil {
			t.Errorf("%q: esperava recusa", padrao)
		}
	}
}

// Letra nua é reservada, como no CLDR: aceitar 'T' como literal faria um
// token digitado errado virar literal em silêncio, e o dataset recusaria
// 100%% dos eventos com uma mensagem que não aponta para o padrão.
func TestCompilaLayoutRecusaLetraNuaEAceitaAEscapada(t *testing.T) {
	if _, err := CompilaLayout("yyyy-MM-ddTHH:mm:ss", DominioDateTime); err == nil {
		t.Fatal("esperava recusa do T sem aspas")
	} else if !strings.Contains(err.Error(), "'") {
		t.Errorf("a mensagem precisa ensinar o escape por aspas: %v", err)
	}
	mustCompila(t, "yyyy-MM-dd'T'HH:mm:ss", DominioDateTime)
}

func TestCompilaLayoutRecusaTokensNumericosDeLarguraVariavelAdjacentes(t *testing.T) {
	if _, err := CompilaLayout("d MMM yyyy h:mm:ss a", DominioDateTime); err != nil {
		t.Fatalf("padrão sem ambiguidade foi recusado: %v", err)
	}
	if _, err := CompilaLayout("d MMM yyyy H:mm:ss", DominioDateTime); err != nil {
		t.Fatalf("padrão sem ambiguidade foi recusado: %v", err)
	}
	for _, padrao := range []string{"yyyyMd HH:mm", "yyyy-MM-dd Hmm:ss"} {
		if _, err := CompilaLayout(padrao, DominioDateTime); err == nil {
			t.Errorf("%q: esperava recusa por ambiguidade", padrao)
		}
	}
}

func TestCompilaLayoutRecusaCampoRepetido(t *testing.T) {
	if _, err := CompilaLayout("yyyy-MM-dd HH:mm:ss yyyy", DominioDateTime); err == nil {
		t.Error("esperava recusa de ano repetido")
	}
}

func TestCompilaLayoutAmarraMeridianoAoRelogioDe12h(t *testing.T) {
	if _, err := CompilaLayout("yyyy-MM-dd hh:mm:ss", DominioDateTime); err == nil {
		t.Error("hh sem meridiano precisa ser recusado: 08:54 seria ambíguo entre manhã e noite")
	}
	if _, err := CompilaLayout("yyyy-MM-dd HH:mm:ss a", DominioDateTime); err == nil {
		t.Error("HH com meridiano precisa ser recusado: o relógio de 24h não tem AM/PM")
	}
}

func TestCompilaLayoutDominioTimeRecusaTokensDeData(t *testing.T) {
	for _, padrao := range []string{"yyyy hh:mm a", "MMM dd hh:mm a", "dd HH:mm"} {
		if _, err := CompilaLayout(padrao, DominioTime); err == nil {
			t.Errorf("%q: time não tem data", padrao)
		}
	}
	mustCompila(t, "hh:mm:ss a", DominioTime)
	mustCompila(t, "HH:mm:ss.SSS", DominioTime)
}

func TestCompilaLayoutDominioDateTimeExigeDataEHoraCompletas(t *testing.T) {
	for _, padrao := range []string{"yyyy-MM-dd", "HH:mm:ss", "yyyy-MM HH:mm", "yyyy-MM-dd HH"} {
		if _, err := CompilaLayout(padrao, DominioDateTime); err == nil {
			t.Errorf("%q: esperava recusa por componente ausente", padrao)
		}
	}
}

// ---------- parse: a amostra real ----------

func TestLayoutAceitaAAmostraRealDoPiloto(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	loc := mustLoc(t, "America/Sao_Paulo")

	got, err := l.DateTime([]byte("Aug 22, 2026 08:54:02 PM"), loc)
	if err != nil {
		t.Fatalf("DateTime: %v", err)
	}
	want := time.Date(2026, time.August, 22, 20, 54, 2, 0, loc).UnixMilli()
	if got != want {
		t.Fatalf("epoch millis: obtido %d, esperado %d", got, want)
	}
}

// 12 AM é meia-noite e 12 PM é meio-dia. É a única regra deste formato que
// não sai certa por acidente: h%%12 zera o 12 e o PM soma 12 por cima.
func TestMeridianoConverteMeiaNoiteEMeioDia(t *testing.T) {
	l := mustCompila(t, "hh:mm:ss a", DominioTime)
	casos := map[string]int64{
		"12:00:00 AM": 0,
		"12:30:00 AM": 30 * 60 * 1000,
		"01:00:00 AM": 3600 * 1000,
		"11:59:59 AM": (11*3600 + 59*60 + 59) * 1000,
		"12:00:00 PM": 12 * 3600 * 1000,
		"12:00:01 PM": (12*3600 + 1) * 1000,
		"01:00:00 PM": 13 * 3600 * 1000,
		"11:59:59 PM": (23*3600 + 59*60 + 59) * 1000,
	}
	for in, want := range casos {
		got, err := l.TimeOfDay([]byte(in))
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("%q: obtido %d, esperado %d", in, got, want)
		}
	}
}

// A amostra veio zero-padded, mas 'd' e 'h' do CLDR não são: um provider de
// locale diferente na JVM do produtor emitiria um dígito só, e sob
// log-and-skip isso seria descarte silencioso por evento.
func TestFormaVariavelAceitaUmOuDoisDigitos(t *testing.T) {
	l := mustCompila(t, "MMM d, yyyy h:mm:ss a", DominioDateTime)
	loc := time.UTC
	a, err := l.DateTime([]byte("Aug 2, 2026 8:54:02 PM"), loc)
	if err != nil {
		t.Fatalf("um dígito: %v", err)
	}
	b, err := l.DateTime([]byte("Aug 02, 2026 08:54:02 PM"), loc)
	if err != nil {
		t.Fatalf("dois dígitos: %v", err)
	}
	if a != b {
		t.Fatalf("as duas grafias precisam dar o mesmo instante: %d vs %d", a, b)
	}
}

// A tolerância é do TOKEN declarado, não do parser: 'dd' e 'hh' são estritos.
// Isso é o que dá ao operador a escolha entre recusar uma grafia inesperada e
// aceitá-la — e é por isso que a recomendação para uma origem cuja grafia foi
// vista numa amostra só é declarar as formas variáveis.
func TestFormaDeLarguraFixaEhEstrita(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime) // MMM d, yyyy hh:mm:ss a
	if _, err := l.DateTime([]byte("Aug 22, 2026 8:54:02 PM"), time.UTC); err == nil {
		t.Error("hh declarado precisa recusar hora de um dígito")
	}
	if _, err := l.DateTime([]byte("Aug 2, 2026 08:54:02 PM"), time.UTC); err != nil {
		t.Errorf("d declarado precisa aceitar dia de um dígito: %v", err)
	}
}

func TestAceitaMesEMeridianoEmQualquerCaixa(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	ref, err := l.DateTime([]byte("Aug 22, 2026 08:54:02 PM"), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []string{"AUG 22, 2026 08:54:02 pm", "aug 22, 2026 08:54:02 Pm"} {
		got, err := l.DateTime([]byte(in), time.UTC)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got != ref {
			t.Errorf("%q: obtido %d, esperado %d", in, got, ref)
		}
	}
}

func TestRecusaMesEmOutroIdioma(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	for _, in := range []string{"Ago 22, 2026 08:54:02 PM", "Set 1, 2026 08:54:02 PM"} {
		if _, err := l.DateTime([]byte(in), time.UTC); err == nil {
			t.Errorf("%q: esperava recusa", in)
		} else if !errors.Is(err, ErrParse) {
			t.Errorf("%q: erro fora da categoria ErrParse: %v", in, err)
		}
	}
}

// Este padrão não tem fuso. Aceitar um offset colado no fim e ignorá-lo daria
// o instante errado sem nenhum sinal.
func TestRecusaConteudoAposOFimDoPadrao(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	for _, in := range []string{"Aug 22, 2026 08:54:02 PM -03:00", "Aug 22, 2026 08:54:02 PMX", "Aug 22, 2026 08:54:02 PM "} {
		if _, err := l.DateTime([]byte(in), time.UTC); err == nil {
			t.Errorf("%q: esperava recusa", in)
		}
	}
}

func TestRecusaDataInexistente(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	for _, in := range []string{"Feb 30, 2026 01:00:00 AM", "Feb 29, 2026 01:00:00 AM", "Apr 31, 2026 01:00:00 AM"} {
		if _, err := l.DateTime([]byte(in), time.UTC); err == nil {
			t.Errorf("%q: esperava recusa", in)
		}
	}
	if _, err := l.DateTime([]byte("Feb 29, 2024 01:00:00 AM"), time.UTC); err != nil {
		t.Errorf("ano bissexto foi recusado: %v", err)
	}
}

func TestRecusaHoraForaDoRelogioDe12h(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	for _, in := range []string{"Aug 22, 2026 00:54:02 PM", "Aug 22, 2026 13:00:00 AM"} {
		if _, err := l.DateTime([]byte(in), time.UTC); err == nil {
			t.Errorf("%q: esperava recusa", in)
		}
	}
}

// ---------- fuso ----------

func TestDateTimeResolveNoFusoRecebido(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	const texto = "Aug 22, 2026 08:54:02 PM"
	sp, err := l.DateTime([]byte(texto), mustLoc(t, "America/Sao_Paulo"))
	if err != nil {
		t.Fatal(err)
	}
	utc, err := l.DateTime([]byte(texto), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if d := sp - utc; d != 3*3600*1000 {
		t.Fatalf("SP menos UTC = %d ms, esperado %d (3h)", d, 3*3600*1000)
	}
}

// A hora que acontece duas vezes: time.Date escolhe a primeira ocorrência.
// Não há como acertar — a informação que desempataria não está no dado — e o
// que importa é ser DETERMINÍSTICO, para que ingestão e query concordem.
func TestHoraAmbiguaDeDSTEhResolvidaDeFormaDeterministica(t *testing.T) {
	l := mustCompila(t, "yyyy-MM-dd HH:mm:ss", DominioDateTime)
	loc := mustLoc(t, "America/Sao_Paulo")
	const texto = "2018-02-17 23:30:00" // fim do horário de verão de 2018

	primeiro, err := l.DateTime([]byte(texto), loc)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		again, err := l.DateTime([]byte(texto), loc)
		if err != nil {
			t.Fatal(err)
		}
		if again != primeiro {
			t.Fatalf("mesma entrada deu instantes diferentes: %d vs %d", primeiro, again)
		}
	}
	if want := time.Date(2018, 2, 17, 23, 30, 0, 0, loc).UnixMilli(); primeiro != want {
		t.Fatalf("obtido %d, esperado o mesmo que time.Date: %d", primeiro, want)
	}
}

// O caso que só a validação em UTC salva. Em America/Santiago a virada de DST
// de 1990 recua a meia-noite: time.Date(1990-09-16 00:30, loc) devolve
// 1990-09-15 23:30. Validar a data comparando o round-trip EM LOC recusaria
// 16/09/1990 como "data inexistente" — e ela existe.
func TestDataValidaNaViradaDeDSTNaoEhRecusada(t *testing.T) {
	l := mustCompila(t, "yyyy-MM-dd HH:mm:ss", DominioDateTime)
	loc := mustLoc(t, "America/Santiago")

	got, err := l.DateTime([]byte("1990-09-16 00:30:00"), loc)
	if err != nil {
		t.Fatalf("data de calendário válida foi recusada: %v", err)
	}
	if want := time.Date(1990, 9, 16, 0, 30, 0, 0, loc).UnixMilli(); got != want {
		t.Fatalf("obtido %d, esperado %d", got, want)
	}
}

// ---------- hora do dia ----------

func TestTimeOfDayNaoDependeDeFusoNemDeData(t *testing.T) {
	l := mustCompila(t, "hh:mm:ss a", DominioTime)
	got, err := l.TimeOfDay([]byte("08:54:02 PM"))
	if err != nil {
		t.Fatal(err)
	}
	const want = int64((20*3600 + 54*60 + 2) * 1000)
	if got != want {
		t.Fatalf("obtido %d, esperado %d", got, want)
	}
	// A assinatura não recebe *time.Location: se um dia receber, este teste
	// deixa de compilar, que é o sinal certo.
	if got >= 86_400_000 || got < 0 {
		t.Fatalf("fora do dia: %d", got)
	}
}

func TestTimeOfDayCobreOsExtremosDoDia(t *testing.T) {
	l := mustCompila(t, "HH:mm:ss.SSS", DominioTime)
	casos := map[string]int64{
		"00:00:00.000": 0,
		"23:59:59.999": 86_399_999,
		"12:00:00.500": 12*3600*1000 + 500,
	}
	for in, want := range casos {
		got, err := l.TimeOfDay([]byte(in))
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("%q: obtido %d, esperado %d", in, got, want)
		}
	}
}

// ---------- ISO local ----------

func TestISOLocalComMillis(t *testing.T) {
	l := mustCompila(t, "yyyy-MM-dd'T'HH:mm:ss.SSS", DominioDateTime)
	got, err := l.DateTime([]byte("2026-08-22T20:54:02.123"), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 22, 20, 54, 2, 123e6, time.UTC).UnixMilli()
	if got != want {
		t.Fatalf("obtido %d, esperado %d", got, want)
	}
}

// ---------- propriedades do hot path ----------

func TestParsersDeDataNaoAlocam(t *testing.T) {
	dt := mustCompila(t, padraoPiloto, DominioDateTime)
	tod := mustCompila(t, "hh:mm:ss a", DominioTime)
	entrada := []byte("Aug 22, 2026 08:54:02 PM")
	hora := []byte("08:54:02 PM")

	if n := testing.AllocsPerRun(200, func() {
		if _, err := dt.DateTime(entrada, time.UTC); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("DateTime alocou %v vezes por chamada", n)
	}
	if n := testing.AllocsPerRun(200, func() {
		if _, err := tod.TimeOfDay(hora); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("TimeOfDay alocou %v vezes por chamada", n)
	}
}

// O buffer do tokenizer JSON morre no próximo Next: reter a entrada faria o
// valor gravado ser outro, e só às vezes.
func TestParsersDeDataNaoRetemEntrada(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	buf := []byte("Aug 22, 2026 08:54:02 PM")
	got, err := l.DateTime(buf, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	for i := range buf {
		buf[i] = 'x'
	}
	again, err := l.DateTime([]byte("Aug 22, 2026 08:54:02 PM"), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if got != again {
		t.Fatalf("resultado mudou depois de sujar o buffer de entrada: %d vs %d", got, again)
	}
}

// Nenhuma entrada arbitrária pode causar panic ou índice fora de faixa.
func TestParseNuncaEntraEmPanicComEntradaArbitraria(t *testing.T) {
	l := mustCompila(t, padraoPiloto, DominioDateTime)
	f := func(s string) bool {
		_, _ = l.DateTime([]byte(s), time.UTC)
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 2000}); err != nil {
		t.Fatal(err)
	}
}
