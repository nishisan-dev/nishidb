package storage

import (
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// adotaOuConstroiAgenda decide, na abertura, entre usar a agenda que está em
// disco e reconstruí-la.
//
// # Por que isto NÃO é o débito 22
//
// O débito 22 é sobre bitmap e hash: aqueles índices mapeiam um valor
// EXTRAÍDO do payload, e reconstruí-los exige ler todo record de todo
// segmento — a janela de horas que faz "declare o índice na criação do
// dataset ou recrie" ser a política.
//
// A agenda não tem essa propriedade. Ela deriva de expiry.idx, prazos.idx e
// locations.idx, que já estão populados, densos e em mmap: um u64 por linha,
// lidos sequencialmente. Zero records lidos, zero dicionário tocado. É a
// mesma classe de operação que o hash.Index já faz ao recontar no Open e que
// a primary já faz ao reconstruir por rehash — e é por isso que um dataset
// existente ganha a agenda ao subir o binário novo, sem recriação e sem
// reingestão.
//
// # Quando reconstrói
//
// Sempre que os parâmetros de que a árvore deriva não são exatamente os desta
// abertura. O caso que importa é a DURAÇÃO DE FUNDO: a linha sem prazo
// próprio herda dela, então mudá-la no YAML muda o vencimento de toda linha
// com prazo desconhecido. Hoje esse efeito é invisível — o predicado
// simplesmente passa a responder outra coisa. Com a agenda ele vira um
// rebuild explícito, logado e com custo medido, e essa visibilidade é ganho,
// não custo.
//
// Manifest ausente também reconstrói: é o dataset novo, em que a varredura
// não custa nada, e é o dataset anterior à agenda, que é o caso da migração.
//
// E reconstrói quando os ARQUIVOS não descrevem o dataset, que é a pergunta
// que o manifest não responde: ele declara os parâmetros da árvore, não a
// existência dela. Ver rowindex.Agenda.PrecisaConstruir.
func (s *Store) adotaOuConstroiAgenda(m *checkpoint.Manifest) error {
	if s.agenda == nil {
		return nil
	}
	querido := s.infoDaAgenda()
	var atual *checkpoint.AgendaInfo
	if m != nil {
		atual = m.Agenda
	}
	limite := types.RowID(s.nextRowID.Load())
	semArquivos := s.agenda.PrecisaConstruir(limite)
	if atual != nil && *atual == *querido && !semArquivos {
		return nil
	}

	var motivo string
	switch {
	case atual == nil:
		motivo = "o dataset não tinha agenda de vencimentos"
	case semArquivos:
		motivo = "os arquivos da agenda não descrevem as linhas do dataset"
	default:
		motivo = fmt.Sprintf("os parâmetros mudaram (duração de fundo %s → %s, geometria %d/%d → %d/%d)",
			time.Duration(atual.DuracaoDeFundoMs)*time.Millisecond,
			time.Duration(querido.DuracaoDeFundoMs)*time.Millisecond,
			atual.BlockShift, atual.Niveis, querido.BlockShift, querido.Niveis)
	}

	inicio := time.Now()
	if err := s.agenda.Reconstroi(limite, s.vencimentoDe); err != nil {
		return fmt.Errorf("storage: construir a agenda de vencimentos de %s: %w", s.opts.Dir, err)
	}
	s.agendaReconstruida = true
	// O log nomeia o custo porque ele é a única coisa que o operador precisa
	// decidir: uma reconstrução de segundos numa subida é aceitável, uma de
	// minutos num dataset grande muda a janela de manutenção. Sem o número
	// medido a linha seria só ruído.
	s.opts.Log.Info("agenda.construida",
		"a agenda de vencimentos foi construída a partir dos índices de escrita e prazo, sem ler records",
		"motivo", motivo, "linhas", uint64(limite), "duracao", time.Since(inicio).String())
	return nil
}

// AuditaAgenda confere que a agenda descreve o mesmo que os índices de que ela
// deriva, linha a linha, e que os sumários são limites inferiores fiéis.
//
// São duas checagens porque são dois modos de falha diferentes, e nenhuma
// cobre a outra. A das FOLHAS pega a agenda que ficou para trás do estado —
// uma linha que venceu e cujo vencimento não foi atualizado. A dos SUMÁRIOS
// (rowindex.Confere) pega a árvore cujas folhas estão certas mas cujo caminho
// até elas foi quebrado, que é o que a descida percorre.
//
// O plano previa comparar contra uma árvore reconstruída numa região de
// rascunho. Comparar contra vencimentoDe é mais forte e não aloca a segunda
// árvore: a função pura é a definição do que a agenda deveria conter, e uma
// reconstrução para rascunho provaria apenas que dois caminhos de código
// concordam entre si.
//
// Não roda sozinha em produção — é O(linhas) e o Open existe para não pagar
// isso. É o instrumento dos testes de crash e da investigação de um dataset
// suspeito.
func (s *Store) AuditaAgenda() error {
	if s.agenda == nil {
		return nil
	}
	if err := s.entra(); err != nil {
		return err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()

	limite := types.RowID(s.nextRowID.Load())
	for id := types.RowID(0); id < limite; id++ {
		agendado, querido := s.agenda.Get(id), s.vencimentoDe(id)
		if agendado == querido {
			continue
		}
		// Vencimento ANTES do devido é frouxo: custa uma descida que não acha
		// nada, e a passada seguinte o corrige. Depois do devido é a falha
		// fatal — a linha vence para o predicado e o coletor nunca a alcança.
		if agendado != rowindex.Nunca && querido != rowindex.Nunca && agendado < querido {
			continue
		}
		return fmt.Errorf("%w: row %d agendada para %d, mas os índices dizem %d",
			ErrDatasetCorrupt, id, agendado, querido)
	}
	return s.agenda.Confere()
}
