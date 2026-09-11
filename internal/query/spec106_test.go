package query_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Os 12 casos obrigatórios da §106, fim a fim: do texto SQL ao resultado,
// sobre dados que entraram pelo Materializer. Os testes homônimos das camadas
// (lexer, parser, binder, planner) cobrem cada uma delas; a rastreabilidade
// normativa é ESTA, porque é a única que exercita a costura.

// evento base: uma sessão RADIUS completa.
func eventos() []string {
	return []string{
		`{"id":"CMP-0001","_SEQ":1,"Acct-Status-Type":"Stop","Framed-Protocol":"PPPoE",
		  "NAS-IP-Address":"10.0.0.1","NAS-Identifier":"NAS-7","Framed-IP-Address":"200.1.1.1",
		  "User-Name":"joao","Connect-Info":100,"Acct-Session-Time":3600}`,
		`{"id":"CMP-0002","_SEQ":1,"Acct-Status-Type":"Start","Framed-Protocol":"PPPoE",
		  "NAS-IP-Address":"10.0.0.1","NAS-Identifier":"NAS-7","Framed-IP-Address":"200.1.1.2",
		  "User-Name":"maria","Connect-Info":200,"Acct-Session-Time":60}`,
		`{"id":"CMP-0003","_SEQ":1,"Acct-Status-Type":"Stop","Framed-Protocol":"PPP",
		  "NAS-IP-Address":"10.0.0.2","NAS-Identifier":"NAS-9","Framed-IP-Address":"200.1.1.3",
		  "User-Name":"joao","Connect-Info":300,"Acct-Session-Time":120}`,
		`{"id":"CMP-0004","_SEQ":1,"Acct-Status-Type":"Stop","Framed-Protocol":"PPPoE",
		  "NAS-IP-Address":"10.0.0.2","NAS-Identifier":"NAS-9","Framed-IP-Address":"200.1.1.4",
		  "User-Name":"ana","Connect-Info":400,"Acct-Session-Time":900}`,
	}
}

func semeado(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.ingere(eventos()...)
	return h
}

// §106 — primary lookup
func TestSpec106PrimaryLookup(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(`SELECT id, username FROM radius_sessions WHERE id = 'CMP-0003'`)

	if got := chaves(rows, "id"); !reflect.DeepEqual(got, []string{"CMP-0003"}) {
		t.Fatalf("resultado = %v, want [CMP-0003]", got)
	}
	// O caminho é o da primary: nenhum scan foi aberto.
	if n := h.engine.Metrics().SourceFullScan.Load(); n != 0 {
		t.Errorf("SourceFullScan = %d, want 0", n)
	}
	if n := h.engine.Metrics().PointLookups.Load(); n != 1 {
		t.Errorf("PointLookups = %d, want 1", n)
	}
}

// §106 — bitmap lookup
func TestSpec106BitmapLookup(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE status = 'Stop'`)

	if got := chaves(rows, "id"); !reflect.DeepEqual(got, []string{"CMP-0001", "CMP-0003", "CMP-0004"}) {
		t.Fatalf("resultado = %v", got)
	}
	if n := h.engine.Metrics().SourceFullScan.Load(); n != 0 {
		t.Errorf("SourceFullScan = %d, want 0: o bitmap deveria ter atendido", n)
	}
}

// §106 — item histórico "hash lookup": antes da D5, `nas` (NAS-Identifier)
// era `index: hash` e o fixture o mantinha DELIBERADAMENTE fora do conjunto
// bitmap, para provar que o plano de hash respondia sozinho. Com `index: true`
// como grafia única, `nas` compila para o mesmo estado que qualquer outro campo
// indexado (era IndexKind, hoje CompiledField.Indexed) e entra no MESMO
// conjunto — newHarness não distingue mais os dois. O nome do teste ficou porque o
// item numerado do §106 é este; o que ele prova — a consulta por `nas`
// resolve sem varredura — não mudou.
func TestSpec106HashLookup(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE nas = 'NAS-9'`)

	if got := chaves(rows, "id"); !reflect.DeepEqual(got, []string{"CMP-0003", "CMP-0004"}) {
		t.Fatalf("resultado = %v", got)
	}
	if n := h.engine.Metrics().SourceFullScan.Load(); n != 0 {
		t.Errorf("SourceFullScan = %d, want 0", n)
	}
}

// §106 — AND
func TestSpec106And(t *testing.T) {
	h := semeado(t)
	indexed := h.consultaOK(
		`SELECT id FROM radius_sessions WHERE status = 'Stop' AND protocol = 'PPPoE'`)
	// O mesmo predicado sem índice tem de dar o mesmo resultado: se a
	// interseção perder candidato, a divergência aparece aqui.
	scan := h.consultaOK(
		`SELECT id FROM radius_sessions WHERE username != 'zzz' AND status = 'Stop' AND protocol = 'PPPoE'`)

	if got := chaves(indexed, "id"); !reflect.DeepEqual(got, []string{"CMP-0001", "CMP-0004"}) {
		t.Fatalf("resultado = %v", got)
	}
	if !reflect.DeepEqual(chaves(indexed, "id"), chaves(scan, "id")) {
		t.Errorf("indexado = %v, scan = %v: a interseção divergiu do full scan",
			chaves(indexed, "id"), chaves(scan, "id"))
	}
}

// O AND com a fonte NÃO indexada: aqui o predicado é a única coisa que
// filtra, e a esquerda é falsa enquanto a direita é verdadeira.
//
// Todas as outras queries com AND deste pacote usam interseção de bitmaps,
// onde a FONTE já pré-filtra os dois termos e o predicado fica redundante —
// por isso a mutação que desliga o curto-circuito do AND (fazendo `A AND B`
// devolver o resultado de B) passava na suíte inteira. A assimetria com o OR
// é estrutural: lá a fonte é união, que não pré-filtra.
func TestAndSobreFullScanFiltraPelosDoisLados(t *testing.T) {
	h := semeado(t)

	// username='zzz' é falso para todas as rows; connect_info=200 é verdadeiro
	// para CMP-0002. Se o AND devolver o lado direito, CMP-0002 aparece.
	got := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE username = 'zzz' AND connect_info = 200`), "id")
	if len(got) != 0 {
		t.Errorf("resultado = %v, want vazio: a esquerda do AND é falsa para todas as rows", got)
	}

	// E o simétrico, para que "vazio sempre" não passe: com a esquerda
	// verdadeira e a direita também, a row sai.
	got = chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE username = 'maria' AND connect_info = 200`), "id")
	if len(got) != 1 || got[0] != "CMP-0002" {
		t.Errorf("resultado = %v, want [CMP-0002]", got)
	}

	// Com a direita falsa e a esquerda verdadeira, também não sai.
	got = chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE username = 'maria' AND connect_info = 999`), "id")
	if len(got) != 0 {
		t.Errorf("resultado = %v, want vazio: a direita do AND é falsa", got)
	}
}

// §106 — OR
func TestSpec106Or(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(
		`SELECT id FROM radius_sessions WHERE status = 'Stop' OR status = 'Start'`)

	got := chaves(rows, "id")
	want := []string{"CMP-0001", "CMP-0002", "CMP-0003", "CMP-0004"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resultado = %v, want %v", got, want)
	}
	// Dedup: uma row que casa nos dois lados do OR sai UMA vez. Aqui os
	// conjuntos são disjuntos, então o teste que morde é o de baixo.
	rows = h.consultaOK(
		`SELECT id FROM radius_sessions WHERE status = 'Stop' OR protocol = 'PPPoE'`)
	got = chaves(rows, "id")
	want = []string{"CMP-0001", "CMP-0002", "CMP-0003", "CMP-0004"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("união com sobreposição = %v, want %v (sem duplicata)", got, want)
	}
}

// A união MISTURA índices: um termo de bitmap e um de hash na mesma consulta.
// Assumir homogeneidade abriria os dois pelo mesmo caminho e o termo de hash
// falharia (o campo não tem bitmap) — ou, pior, devolveria vazio.
func TestUniaoMisturaBitmapEHash(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(
		`SELECT id FROM radius_sessions WHERE status = 'Start' OR nas_ip = '10.0.0.2'`)

	// CMP-0002 por status='Start', CMP-0003 e CMP-0004 por nas_ip — os dois
	// termos indexados (o nome do teste é histórico: antes da D5 um dos dois era
	// `index: hash`; hoje os dois compilam para o mesmo estado — era o mesmo
	// IndexKind, hoje o mesmo bool).
	got := chaves(rows, "id")
	if !reflect.DeepEqual(got, []string{"CMP-0002", "CMP-0003", "CMP-0004"}) {
		t.Fatalf("resultado = %v", got)
	}
	if n := h.engine.Metrics().SourceFullScan.Load(); n != 0 {
		t.Errorf("SourceFullScan = %d, want 0: a união deveria ter atendido", n)
	}

	// Com sobreposição real: Stop = {1,3,4} e nas_ip 10.0.0.1 = {1,2}. A união
	// é {1,2,3,4}, e CMP-0001 — que casa nos DOIS lados — sai uma vez só.
	rows = h.consultaOK(
		`SELECT id FROM radius_sessions WHERE status = 'Stop' OR nas_ip = '10.0.0.1'`)
	got = chaves(rows, "id")
	if !reflect.DeepEqual(got, []string{"CMP-0001", "CMP-0002", "CMP-0003", "CMP-0004"}) {
		t.Fatalf("união com sobreposição = %v", got)
	}
	vezes := 0
	for _, id := range got {
		if id == "CMP-0001" {
			vezes++
		}
	}
	if vezes != 1 {
		t.Errorf("CMP-0001 saiu %d vezes: casa nos dois lados do OR e o dedup falhou", vezes)
	}
}

// §106 — comparison: cada operador × cada tipo comparável.
func TestSpec106ComparacaoPorTipoEOperador(t *testing.T) {
	h := semeado(t)
	casos := []struct {
		where string
		want  []string
	}{
		// string
		{`username = 'joao'`, []string{"CMP-0001", "CMP-0003"}},
		{`username != 'joao'`, []string{"CMP-0002", "CMP-0004"}},
		{`username > 'joao'`, []string{"CMP-0002"}},
		{`username >= 'joao'`, []string{"CMP-0001", "CMP-0002", "CMP-0003"}},
		{`username < 'joao'`, []string{"CMP-0004"}},
		{`username <= 'joao'`, []string{"CMP-0001", "CMP-0003", "CMP-0004"}},
		// int64
		{`connect_info = 300`, []string{"CMP-0003"}},
		{`connect_info != 300`, []string{"CMP-0001", "CMP-0002", "CMP-0004"}},
		{`connect_info > 300`, []string{"CMP-0004"}},
		{`connect_info >= 300`, []string{"CMP-0003", "CMP-0004"}},
		{`connect_info < 300`, []string{"CMP-0001", "CMP-0002"}},
		{`connect_info <= 300`, []string{"CMP-0001", "CMP-0002", "CMP-0003"}},
		// ipv4 — a ordem lexicográfica dos bytes em network order É a numérica
		{`nas_ip = '10.0.0.2'`, []string{"CMP-0003", "CMP-0004"}},
		{`nas_ip != '10.0.0.2'`, []string{"CMP-0001", "CMP-0002"}},
		{`nas_ip > '10.0.0.1'`, []string{"CMP-0003", "CMP-0004"}},
		{`ip <= '200.1.1.2'`, []string{"CMP-0001", "CMP-0002"}},
		// coluna int64 comparada a literal FLOAT (CompareIntAsFloat)
		{`connect_info > 299.5`, []string{"CMP-0003", "CMP-0004"}},
		{`connect_info = 300.0`, []string{"CMP-0003"}},
	}
	for _, tc := range casos {
		t.Run(tc.where, func(t *testing.T) {
			rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE ` + tc.where)
			if got := chaves(rows, "id"); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("resultado = %v, want %v", got, tc.want)
			}
		})
	}
}

// §106 — NULL. Cruza com D2: NULL é AUSÊNCIA, e um patch com null REMOVE o
// campo.
func TestSpec106Null(t *testing.T) {
	h := semeado(t)
	// CMP-0005 nasce sem Framed-Protocol; CMP-0001 perde o dele por patch.
	h.ingere(
		`{"id":"CMP-0005","_SEQ":1,"Acct-Status-Type":"Stop","User-Name":"bia"}`,
		`{"id":"CMP-0001","_SEQ":2,"Framed-Protocol":null}`,
	)

	ausentes := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE protocol IS NULL`), "id")
	if !reflect.DeepEqual(ausentes, []string{"CMP-0001", "CMP-0005"}) {
		t.Fatalf("IS NULL = %v, want [CMP-0001 CMP-0005]", ausentes)
	}

	presentes := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE protocol IS NOT NULL`), "id")
	if !reflect.DeepEqual(presentes, []string{"CMP-0002", "CMP-0003", "CMP-0004"}) {
		t.Fatalf("IS NOT NULL = %v", presentes)
	}

	// Campo ausente compara FALSE para todo operador — inclusive `!=`. A
	// intuição SQL diria que CMP-0001 e CMP-0005 saem no `!=`; aqui não saem, e
	// é deliberado: sem NOT genérico, a lógica trivalente não vaza.
	igual := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE protocol = 'PPPoE'`), "id")
	diferente := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE protocol != 'PPPoE'`), "id")
	for _, id := range []string{"CMP-0001", "CMP-0005"} {
		if contem(igual, id) || contem(diferente, id) {
			t.Errorf("row %s com campo ausente saiu em = ou !=", id)
		}
	}
	if len(igual)+len(diferente) != 3 {
		t.Errorf("= (%v) e != (%v) deveriam particionar só as rows com o campo", igual, diferente)
	}
}

// §106 — LIMIT
func TestSpec106Limit(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE status = 'Stop' LIMIT 2`)
	if got := chaves(rows, "id"); !reflect.DeepEqual(got, []string{"CMP-0001", "CMP-0003"}) {
		t.Fatalf("resultado = %v", got)
	}
	// LIMIT explícito abaixo do teto não é truncamento: Err() é nil.
	if _, err := h.consulta(`SELECT id FROM radius_sessions LIMIT 1`); err != nil {
		t.Errorf("LIMIT explícito devolveu erro: %v", err)
	}
}

// §106 — full scan
func TestSpec106FullScan(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE username = 'joao'`)

	if got := chaves(rows, "id"); !reflect.DeepEqual(got, []string{"CMP-0001", "CMP-0003"}) {
		t.Fatalf("resultado = %v", got)
	}
	if n := h.engine.Metrics().SourceFullScan.Load(); n != 1 {
		t.Errorf("SourceFullScan = %d, want 1", n)
	}
	// A métrica NORMATIVA da §64 é a do planner, contada em plan-time.
	if n := h.engine.PlanMetrics().FullScan.Load(); n != 1 {
		t.Errorf("query.full_scan = %d, want 1", n)
	}
}

// §106 — aliases: no WHERE e na projeção, com o nome de saída sendo o que o
// usuário escreveu.
func TestSpec106Aliases(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(`SELECT service_id, status FROM radius_sessions WHERE status = 'Start'`)
	if len(rows) != 1 {
		t.Fatalf("resultado = %v", rows)
	}
	if rows[0]["service_id"] != "CMP-0002" || rows[0]["status"] != "Start" {
		t.Errorf("linha = %v, want colunas nomeadas pelo alias", rows[0])
	}
	// O nome ORIGINAL resolve o mesmo campo.
	orig := h.consultaOK(
		`SELECT id FROM radius_sessions WHERE "Acct-Status-Type" = 'Start'`)
	if !reflect.DeepEqual(chaves(orig, "id"), []string{"CMP-0002"}) {
		t.Errorf("nome original = %v", chaves(orig, "id"))
	}
}

// §106 — quoted fields
func TestSpec106QuotedFields(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(
		`SELECT "Acct-Status-Type" FROM radius_sessions WHERE "Acct-Status-Type" = 'Start'`)
	if len(rows) != 1 || rows[0]["Acct-Status-Type"] != "Start" {
		t.Fatalf("resultado = %v", rows)
	}
}

// §106 — parameters
func TestSpec106Parameters(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(
		`SELECT id FROM radius_sessions WHERE status = ? AND nas_ip = ?`,
		types.Value{Type: types.TypeString, Bytes: []byte("Stop")},
		types.Value{Type: types.TypeString, Bytes: []byte("10.0.0.2")},
	)
	if got := chaves(rows, "id"); !reflect.DeepEqual(got, []string{"CMP-0003", "CMP-0004"}) {
		t.Fatalf("resultado = %v", got)
	}

	// Contagem errada de parâmetros é erro de Execute, antes de qualquer
	// leitura.
	_, err := h.consulta(`SELECT id FROM radius_sessions WHERE status = ? AND nas_ip = ?`,
		types.Value{Type: types.TypeString, Bytes: []byte("Stop")})
	if err == nil {
		t.Fatal("contagem errada de parâmetros deveria falhar")
	}
	if !errors.Is(err, binder.ErrBind) || !strings.Contains(err.Error(), "parâmetros") {
		t.Errorf("erro = %v, want ErrBind mencionando parâmetros", err)
	}
}

func contem(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
