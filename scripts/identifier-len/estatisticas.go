package main

import (
	"math"
	"sort"
)

// Histograma acumula o comprimento (em bytes) do campo extraído de cada
// mensagem lida do tópico.
//
// A cardinalidade de comprimentos DISTINTOS é minúscula perto do volume de
// mensagens: o teto de formato é 1024 (MaxKeySize, record.go:36) e a cauda
// que a issue #15 investiga vive a poucas centenas de bytes dele — não há
// milhões de comprimentos diferentes, há no máximo alguns milhares. Por isso
// um map[int]int64 basta para percentis EXATOS, sem amostragem (t-digest,
// HDRHistogram) nem perda de precisão por balde.
type Histograma struct {
	comprimentos map[int]int64
	totalLido    int64
	comCampo     int64
	semCampo     int64
	invalidos    int64
}

// NovoHistograma devolve um histograma vazio, pronto para Registra.
func NovoHistograma() *Histograma {
	return &Histograma{comprimentos: make(map[int]int64)}
}

// Registra soma uma mensagem ao histograma. `tamanho` só é significativo
// quando `resultado == ExtraidoOK` — os outros dois desfechos não têm
// comprimento de campo a contar.
func (h *Histograma) Registra(resultado ResultadoExtracao, tamanho int) {
	h.totalLido++
	switch resultado {
	case ExtraidoOK:
		h.comCampo++
		h.comprimentos[tamanho]++
	case ExtraidoSemCampo:
		h.semCampo++
	default:
		h.invalidos++
	}
}

// TotalLido é o total de mensagens processadas, nos quatro desfechos somados.
func (h *Histograma) TotalLido() int64 { return h.totalLido }

// ComCampo é o total de mensagens em que o campo foi extraído com sucesso —
// o denominador dos buckets e dos percentis.
func (h *Histograma) ComCampo() int64 { return h.comCampo }

// SemCampo é o total de mensagens cujo JSON parseou mas não tinha o campo.
func (h *Histograma) SemCampo() int64 { return h.semCampo }

// Invalidos é o total de mensagens cujo payload não parseou como JSON, ou
// cujo campo não era uma string.
func (h *Histograma) Invalidos() int64 { return h.invalidos }

// Bucket é uma faixa de comprimento do relatório final.
type Bucket struct {
	Rotulo     string
	Contagem   int64
	Percentual float64
}

// limitesDosBuckets são os tetos EXATOS do brief: as três frações do teto de
// formato MaxKeySize (1024 = encoding.MaxKeySize) mais o próprio teto — a
// quinta faixa (>1024) é o que a issue #15 existe para medir.
var limitesDosBuckets = [4]int{256, 512, 768, 1024}

var rotulosDosBuckets = [5]string{"≤256", "≤512", "≤768", "≤1024", ">1024"}

// Buckets classifica cada comprimento observado nas cinco faixas do brief.
// A percentagem é sobre o total COM CAMPO: é a distribuição do campo em si,
// e mensagens sem_campo/inválidas não têm comprimento a distribuir.
func (h *Histograma) Buckets() []Bucket {
	contagens := make([]int64, len(rotulosDosBuckets))
	for comprimento, n := range h.comprimentos {
		i := sort.SearchInts(limitesDosBuckets[:], comprimento)
		// SearchInts devolve o primeiro índice cujo limite é >= comprimento
		// só quando o limite bate exatamente por cima; um comprimento IGUAL
		// a um limite (256, 512, 768, 1024) tem de cair no balde "≤", e
		// SearchInts com o alvo exato já devolve esse índice — é a busca por
		// ">=", não por ">". Comprimento 0 cai em "≤256" (índice 0), e
		// qualquer coisa acima de 1024 estoura para o último índice (4).
		contagens[i] += n
	}
	buckets := make([]Bucket, len(rotulosDosBuckets))
	for i, rotulo := range rotulosDosBuckets {
		var pct float64
		if h.comCampo > 0 {
			pct = float64(contagens[i]) / float64(h.comCampo) * 100
		}
		buckets[i] = Bucket{Rotulo: rotulo, Contagem: contagens[i], Percentual: pct}
	}
	return buckets
}

// chavesOrdenadas devolve os comprimentos distintos observados, em ordem
// crescente — a base de Percentil, Maximo e TopN.
func (h *Histograma) chavesOrdenadas() []int {
	chaves := make([]int, 0, len(h.comprimentos))
	for c := range h.comprimentos {
		chaves = append(chaves, c)
	}
	sort.Ints(chaves)
	return chaves
}

// Percentil devolve o comprimento EXATO no percentil p (0–100), pelo método
// do posto mais próximo (nearest-rank) — sem interpolação. Interpolar
// inventaria um comprimento que nenhuma mensagem tem; o brief pede exatos, e
// map[int]int64 é o que permite isso sem custo, dada a cardinalidade baixa.
func (h *Histograma) Percentil(p float64) int {
	if h.comCampo == 0 {
		return 0
	}
	posto := int64(math.Ceil(p / 100 * float64(h.comCampo)))
	if posto < 1 {
		posto = 1
	}
	var acumulado int64
	for _, c := range h.chavesOrdenadas() {
		acumulado += h.comprimentos[c]
		if acumulado >= posto {
			return c
		}
	}
	// Só chega aqui se `posto` exceder comCampo por erro de arredondamento;
	// o maior comprimento observado é a resposta mais próxima possível.
	return h.Maximo()
}

// Maximo é o maior comprimento observado, ou 0 se nada foi registrado.
func (h *Histograma) Maximo() int {
	chaves := h.chavesOrdenadas()
	if len(chaves) == 0 {
		return 0
	}
	return chaves[len(chaves)-1]
}

// TopN devolve os N maiores comprimentos observados, maior primeiro,
// REPETINDO um comprimento uma vez por mensagem que o tiver — é a cauda que
// o operador quer ver (quantas mensagens, não quantos valores distintos).
func (h *Histograma) TopN(n int) []int {
	if n <= 0 {
		return nil
	}
	chaves := h.chavesOrdenadas()
	out := make([]int, 0, n)
	for i := len(chaves) - 1; i >= 0 && len(out) < n; i-- {
		c := chaves[i]
		for k := int64(0); k < h.comprimentos[c] && len(out) < n; k++ {
			out = append(out, c)
		}
	}
	return out
}
