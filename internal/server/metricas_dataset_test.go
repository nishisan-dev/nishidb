package server_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
)

// amostraExposta é uma linha do /metrics já casada com o `# HELP` da família.
//
// O valorDaMetrica que já existe aqui devolve só o número, e o número não
// basta nesta fase: metade do contrato de nishidb_storage_garbage_bytes está
// no TEXTO DE AJUDA, que é onde a ressalva de "limite inferior" viaja até o
// operador. Uma série publicada com o valor certo e a ajuda errada seria lida
// como contagem exata, que é exatamente o defeito que o comentário do M2
// evitou ao deixá-la de fora.
type amostraExposta struct {
	Nome  string
	Ajuda string
	Valor float64
}

// coletaAmostras raspa o /metrics e devolve as amostras com a ajuda da
// família já anexada a cada linha.
//
// Parseia o formato de exposição em vez de chamar o coletor direto porque o
// pacote de teste é externo (server_test): o que se prova aqui é o que o
// Prometheus enxerga, não o que a função interna devolveu antes de passar
// pelo Registry.
func coletaAmostras(t *testing.T, srv *server.Servidor) []amostraExposta {
	t.Helper()
	var (
		out   []amostraExposta
		ajuda = make(map[string]string)
	)
	for _, linha := range strings.Split(pega(t, srv, "/metrics"), "\n") {
		if linha == "" {
			continue
		}
		if depois, achou := strings.CutPrefix(linha, "# HELP "); achou {
			familia, texto, _ := strings.Cut(depois, " ")
			ajuda[familia] = texto
			continue
		}
		if strings.HasPrefix(linha, "#") {
			continue
		}
		i := strings.LastIndexByte(linha, ' ')
		if i < 0 {
			t.Fatalf("linha sem valor no scrape: %q", linha)
		}
		nome, _, _ := strings.Cut(linha[:i], "{")
		v, err := strconv.ParseFloat(linha[i+1:], 64)
		if err != nil {
			t.Fatalf("valor ilegível em %q: %v", linha, err)
		}
		// A ajuda é indexada pela FAMÍLIA, e o nome da linha pode carregar
		// sufixo de histograma (_bucket/_sum/_count). O sufixo não chega às
		// séries de disco, mas o helper serve o pacote inteiro.
		out = append(out, amostraExposta{Nome: nome, Ajuda: ajuda[familiaDe(nome, ajuda)], Valor: v})
	}
	return out
}

// familiaDe resolve o nome-base de uma linha: o próprio nome, quando ele é a
// família, ou o nome sem o sufixo de histograma.
func familiaDe(nome string, ajuda map[string]string) string {
	if _, ok := ajuda[nome]; ok {
		return nome
	}
	for _, sufixo := range []string{"_bucket", "_sum", "_count"} {
		if base, achou := strings.CutSuffix(nome, sufixo); achou {
			return base
		}
	}
	return nome
}

// achaAmostra devolve a primeira amostra da série. Os testes deste pacote
// sobem processo de UM dataset só, então "primeira" e "única" coincidem.
func achaAmostra(amostras []amostraExposta, nome string) (amostraExposta, bool) {
	for _, a := range amostras {
		if a.Nome == nome {
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
	srv := servidorComDados(t, 100, blocoPadrao)
	amostras := coletaAmostras(t, srv)

	bytes, ok := achaAmostra(amostras, metrics.MetStorageBytes)
	if !ok {
		t.Fatal("nishidb_storage_bytes não foi publicada")
	}
	if bytes.Valor <= 0 {
		t.Errorf("storage_bytes = %v, queria > 0", bytes.Valor)
	}

	lixo, ok := achaAmostra(amostras, metrics.MetStorageGarbageBytes)
	if !ok {
		t.Fatal("nishidb_storage_garbage_bytes não foi publicada")
	}
	if !strings.Contains(lixo.Ajuda, "pelo menos") {
		t.Errorf("Ajuda = %q; precisa declarar que é limite inferior", lixo.Ajuda)
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
	segmentos, ok := achaAmostra(amostras, metrics.MetSegmentsTotal)
	if !ok {
		t.Fatal("nishidb_segments_total não foi publicada")
	}
	if esperado := float64(segment.HeaderSize) * segmentos.Valor; lixo.Valor != esperado {
		t.Errorf("garbage_bytes = %v, queria %v (%d bytes de header por segmento × %v segmentos; o seed não sobrescreve nenhum id, então nada além do header é lixo)",
			lixo.Valor, esperado, segment.HeaderSize, segmentos.Valor)
	}
	if lixo.Valor > bytes.Valor {
		t.Errorf("garbage_bytes = %v acima de storage_bytes = %v", lixo.Valor, bytes.Valor)
	}

	// A terceira série do §92, nishidb_storage_live_bytes, continua FORA: ela
	// é o tally cru, e publicá-lo sob um nome que promete exatidão é a
	// métrica que mente. Sai quando a compaction reconstruir o tally por
	// varredura.
	if _, ok := achaAmostra(amostras, metrics.MetStorageLiveBytes); ok {
		t.Error("nishidb_storage_live_bytes foi publicada: o tally ainda é limite superior")
	}
}
