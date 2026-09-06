package storage

// A propriedade de que o §44 inteiro depende, e o arquivo existe para que ela
// SOBREVIVA à troca de motor de índice.
//
// Ela nasceu em store_recovery_bitmap_test.go, junto dos cenários que a
// estrearam, e mudou de arquivo antes de o motor antigo sair. Seis cenários de
// recovery chamam este guarda como asserção final, e é a única prova de ausência
// de falso negativo que eles têm — deixá-la junto dos testes que atacavam o
// formato antigo teria feito a remoção do motor levar junto a rede de proteção
// do motor novo, sem que nada acusasse.
//
// A fonte do conjunto indexado é o camposIndexados, e não um mapa por motor: com
// um motor só, "campo com bitmap" e "campo com hash" viraram a mesma pergunta.

import (
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// exigeBitmapCompleto verifica a propriedade da qual a spec §44 depende:
// para toda row viva, todo campo indexado do payload CORRENTE aparece nas
// postings do seu valor.
//
// É uma propriedade, e não um caso de teste, por causa da assimetria do
// §14.16: falso positivo o executor filtra na revalidação, falso negativo é
// invisível — nenhuma camada acima recupera um candidato que o índice não
// devolveu, e a query volta silenciosamente incompleta.
//
// Limite: cobre as rows alcançáveis pelo ScanLive. Uma row cuja location se
// perdeu não é varrida aqui — isso é responsabilidade das asserções de
// visibilidade por key de cada cenário.
func exigeBitmapCompleto(t *testing.T, s *Store) {
	t.Helper()
	// Sem índice declarado o guarda não verifica nada. Falhar aqui evita que
	// ele vire um no-op invisível se algum cenário perder a configuração de
	// índice — uma rede que passa sempre é pior que rede nenhuma.
	if len(s.camposIndexados) == 0 {
		t.Fatal("guarda de bitmap chamado num Store sem índice: o cenário precisa declarar CamposIndexados, senão a asserção não verifica nada")
	}
	it := s.ScanLive(s.presente())
	defer it.Close()
	var buf []byte
	for it.Next() {
		id := it.RowID()
		rec, b, vivo, err := s.GetByRowID(s.presente(), id, buf)
		buf = b
		if err != nil {
			t.Fatalf("guarda de bitmap: GetByRowID(%d): %v", id, err)
		}
		if !vivo {
			continue
		}
		var r encoding.FieldReader
		r.Reset(rec.Payload)
		for {
			fid, v, ok := r.Next()
			if !ok {
				break
			}
			if !slices.Contains(s.camposIndexados, fid) {
				continue
			}
			postings, err := s.Lookup(fid, v)
			if err != nil {
				t.Fatalf("guarda de bitmap: Lookup(%d): %v", fid, err)
			}
			achou := false
			for postings.Next() {
				if postings.RowID() == id {
					achou = true
					break
				}
			}
			postings.Close()
			if !achou {
				t.Fatalf("FALSO NEGATIVO no bitmap: a row %d tem o campo %d=%q no payload corrente, mas não aparece nas postings desse valor",
					id, fid, v.Bytes)
			}
		}
		if err := r.Err(); err != nil {
			t.Fatalf("guarda de bitmap: payload da row %d: %v", id, err)
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("guarda de bitmap: ScanLive: %v", err)
	}
}

// exigeCandidatos afirma o CONTRATO do índice contra a lista de rows que o
// predicado realmente satisfaz: elas TÊM de estar entre os candidatos.
//
// Ela não afirma igualdade, e a assimetria é o desenho (spec §5.2): o índice por
// segmento não tem fase de CLEAR, então a posting de um valor que a linha já não
// tem sobrevive como candidato até o segmento ser compactado ou aposentado.
// Candidato a mais o executor descarta na revalidação do §44; candidato a menos
// é falso negativo, e nenhuma camada acima recupera uma linha que o índice não
// devolveu.
//
// Um teste que exija igualdade aqui está medindo a ESTREITEZA do superconjunto,
// que era propriedade do motor global — lá a posting não pertencia a segmento
// nenhum e o Clear tinha onde acontecer. Quem quiser afirmar o que o produto
// devolve deve subir um nível e olhar o resultado pós-revalidação.
func exigeCandidatos(t *testing.T, rotulo string, candidatos, querem []types.RowID) {
	t.Helper()
	for _, id := range querem {
		if !slices.Contains(candidatos, id) {
			t.Errorf("%s: a row %d satisfaz o predicado e NÃO está entre os candidatos %v — falso negativo",
				rotulo, id, candidatos)
		}
	}
	for i := 1; i < len(candidatos); i++ {
		if candidatos[i-1] >= candidatos[i] {
			t.Errorf("%s: candidatos fora de ordem estritamente crescente: %v", rotulo, candidatos)
			return
		}
	}
}
