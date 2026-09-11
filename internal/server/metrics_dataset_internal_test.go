package server

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage"
)

// TestLixoDeDiscoNuncaPassaPorBaixoDeZero fixa a ÚNICA conta que a série
// nishidb_storage_garbage_bytes do §92 acrescenta ao coletor: `disco - vivo`,
// com piso em zero.
//
// O piso tinha comentário e não tinha cenário. O caso que ele descreve — o
// vivo passando o disco entre dois checkpoints — é inalcançável por um
// servidor de teste, porque o store sobe de um manifest já checkpointado e
// não recebe Put pela rota: ali `StorageBytes > LiveBytesUpperBound` sempre, e
// o ramo do piso nunca é exercitado. Um invariante que só um comentário
// afirma não é invariante; é intenção. A conta virou função justamente para
// ter tabela, e a tabela entra pelos dois lados da desigualdade.
//
// O que está em jogo não é estética. `StorageBytes` é int64 e
// `LiveBytesUpperBound` é uint64: a subtração acontece em uint64, e uint64 não
// tem negativo — ela DÁ A VOLTA. Sem piso, um vivo 64 bytes à frente do disco
// não publica "-64", publica 1,8e19, e um medidor de bytes com 18 exabytes de
// lixo não é um gráfico estranho, é um alerta de compaction disparando contra
// um dataset saudável. Os débitos 38 e 45 garantem que o vivo é limite
// SUPERIOR, então esse desencontro é o comportamento esperado do tally, não
// uma anomalia rara.
func TestLixoDeDiscoNuncaPassaPorBaixoDeZero(t *testing.T) {
	casos := []struct {
		name string
		disk int64
		vivo uint64
		want uint64
	}{
		{
			// O caso comum, e o que o painel lê: o checkpoint mediu o
			// diretório, o tally reivindica menos que isso, e a diferença é
			// o que a compaction do §53-55 tem para recuperar.
			name: "disco à frente do vivo: o lixo é a diferença",
			disk: 8314,
			vivo: 8250,
			want: 64,
		},
		{
			// Seed sem update nenhum: todo record apendado continua vivo e o
			// único byte que o tally não reivindica é o header do segmento,
			// que não é record. O piso da série não é zero — é header por
			// segmento —, e quem ler a série precisa saber disso antes de
			// chamar 64 bytes de "lixo detectado".
			name: "só o header sobra quando nada foi sobrescrito",
			disk: 8314,
			vivo: 8314 - 64,
			want: 64,
		},
		{
			// A igualdade é o limite exato do ramo: um `>=` no lugar do `>`
			// daria o mesmo zero, mas um `>` trocado por `!=` deixaria a
			// subtração rodar. Fixar o zero aqui é fixar a fronteira.
			name: "vivo exatamente igual ao disco: zero, não a fronteira",
			disk: 4096,
			vivo: 4096,
			want: 0,
		},
		{
			// O cenário que o comentário do piso descreve, com os números
			// medidos no campo Stats.StorageBytes: 64 bytes reportados pelo
			// último ListSegments contra 28.256 bytes já apendados em quatro
			// arquivos. Sem piso, `64 - 28256` em uint64 é
			// 18.446.744.073.709.523.424.
			name: "vivo à frente do disco entre dois checkpoints",
			disk: 64,
			vivo: 28256,
			want: 0,
		},
		{
			// Dataset recém-aberto cujo ListSegments ainda não mediu nada,
			// com o tally já andando. É o mesmo desencontro do caso acima
			// levado ao extremo, e é ele que responde por que o teste do
			// coletor exige storage_bytes > 0 antes de comparar.
			name: "disco ainda não medido, tally já andando",
			disk: 0,
			vivo: 4096,
			want: 0,
		},
		{
			// Disco negativo não sai de uma soma de tamanho de arquivo, e é
			// exatamente por isso que a guarda `> 0` é carga e não ritual: a
			// guarda não protege contra o número negativo, protege contra a
			// CONVERSÃO dele. `uint64(int64(-1))` é 1,8e19, que passaria por
			// cima do vivo e publicaria o disco inteiro do planeta como
			// lixo. Trocar a guarda por uma comparação em int64 mudaria a
			// resposta deste caso.
			name: "disco negativo não vira 1,8e19 pela conversão",
			disk: -1,
			vivo: 0,
			want: 0,
		},
	}
	for _, c := range casos {
		t.Run(c.name, func(t *testing.T) {
			st := storage.Stats{StorageBytes: c.disk, LiveBytesUpperBound: c.vivo}
			if got := lixoDeDisco(st); got != c.want {
				t.Errorf("lixoDeDisco(disco=%d, vivo=%d) = %d, queria %d",
					c.disk, c.vivo, got, c.want)
			}
		})
	}
}
