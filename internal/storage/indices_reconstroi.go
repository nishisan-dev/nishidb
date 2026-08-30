package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ConstroiIndiceDoSegmento deriva o índice SELADO de um segmento a partir dos
// records dele, e o publica pelo mesmo .tmp + fsync + rename da selagem.
//
// Ela é o mecanismo da §2.4, e só ele: o laço de fundo, o pacing contra o
// orçamento de ciclo de trabalho e as métricas são de quem a chama. Aqui não há
// goroutine, e o ctx existe para que o chamador possa desistir no meio de uma
// varredura que pode custar 512 MiB de leitura.
//
// A construção NÃO consulta locations.idx, NÃO filtra tombstone e NÃO decide
// liveness. Isso não é economia: o contrato do índice é SUPERCONJUNTO de
// candidatos, com o executor revalidando o WHERE inteiro (§44), e filtrar aqui
// criaria uma SEGUNDA definição de "vivo" — a desta função — que divergiria da
// do executor no primeiro dia em que uma das duas mudasse. A divergência que ela
// produziria é a única que o produto não tolera: candidato a menos é falso
// negativo, e o §44 só sabe descartar candidato a mais.
//
// O arquivo sai igual ao da selagem por CONSTRUÇÃO, porque os dois caminhos
// alimentam o mesmo segindex.Escritor — é ele quem ordena, deduplica, re-baseia
// e escolhe a largura da posting. O que o golden
// (TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP) prova é a outra metade: que
// a ALIMENTAÇÃO é equivalente, que é onde o erro moraria. A igualdade vale para a
// mesma COBERTURA, e é por isso que reconstruir só o campo que falta produz o
// mesmo arquivo que reconstruir tudo — a cobertura é a mesma nos dois casos
// (TestReconstruirSoOsCamposQueFaltamPreservaOsQueJaEstavam).
//
// Segmento sem posting nenhuma não gera arquivo (contrato de Escritor.Escreve):
// "sem índice" é um estado que o Lookup resolve por enumeração, e um .sidx vazio
// faria o segmento passar por indexado. Um .sidx ANTERIOR nesse caso é removido,
// porque ele só pode conter seções de campos que deixaram de ser indexados.
//
// `campos` são os campos PENDENTES daquele segmento, e podem ser um subconjunto
// do conjunto indexado — é a §2.6, que precisa acrescentar um campo ao schema de
// um dataset povoado. O arquivo publicado cobre a UNIÃO deles com o que o .sidx
// atual já cobria e continua declarado (coberturaDaReconstrucao), e a união é
// obrigatória: o arquivo é imutável, "acrescentar seção" é reescrevê-lo inteiro,
// e reescrevê-lo só com o campo pedido apagaria as seções que já estavam lá.
//
// A PUBLICAÇÃO NÃO TOMA O wmu, e a §2.4 emendada é explícita quanto a isso: a
// exigência anterior — tomá-lo "só no rename" — era inexequível, porque
// Escritor.Escreve faz escrita-tmp, fsync, rename e fsync do diretório numa
// chamada só, e segurar o lock pela escrita inteira é o que a mesma seção
// proíbe. O que substitui o lock são três fatos: o rename já é atômico, o
// temporário passa a ter nome único por escritor (é da Tarefa 14, junto com o
// laço — hoje ele ainda é fixo), e a selagem e a reconstrução produzem arquivos
// BYTE A BYTE IDÊNTICOS, o que o golden prova. Com a igualdade provada, uma
// corrida no nome final é neutra em conteúdo: quem renomear por último grava os
// mesmos bytes.
//
// O QUE O CHAMADOR AINDA PRECISA GARANTIR, e é concreto: esta função lê pelo
// s.segR, e a aposentadoria fecha o descritor por segR.FechaSegmento, que exige
// que nenhuma leitura esteja em voo — barreira que no Store é o aposMu. O Close
// fecha o segR inteiro. Um laço de fundo que varra sem o mesmo par claim/Add que
// a compaction usa colhe os.ErrClosed no meio da varredura, e o resíduo aceito
// da §2.4 (publicar o .sidx de um segmento recém-aposentado) vira erro em vez de
// órfão recuperável.
func (s *Store) ConstroiIndiceDoSegmento(ctx context.Context, seg uint32, campos []types.FieldID) error {
	if len(campos) == 0 {
		return nil
	}
	// A recusa que sobrou é a que continua verdadeira: campo FORA do conjunto
	// declarado. Gravar a seção dele produziria um índice que a ingestão não
	// alimenta — o próximo record daquele segmento não teria posting, e o
	// arquivo passaria a afirmar mais do que sabe.
	//
	// A recusa de SUBCONJUNTO saiu, e a ordem em que isso pôde acontecer é o
	// desenho, não o histórico: ela existia porque a pendência era decidida por
	// "tem .sidx?", e um .sidx imutável e parcial faria o segmento contar como
	// PRONTO com campos que ninguém indexou — falso negativo petrificado, sem
	// erro e sem log. Só depois de o teste de pendência passar a olhar SEÇÃO POR
	// CAMPO (SegmentosPendentesDeReconstrucao) um arquivo incompleto deixou de
	// poder se declarar completo, e só então pedir "reconstrua o campo que
	// falta" virou pergunta legítima.
	for _, campo := range campos {
		if !slices.Contains(s.camposIndexados, campo) {
			return fmt.Errorf("%w: reconstrução do segmento %d pedida para o campo %d, que o dataset não indexa (indexados: %v); "+
				"a seção existiria sem ninguém para alimentá-la",
				ErrInvalidArgument, seg, campo, s.camposIndexados)
		}
	}
	// O segmento CORRENTE é recusado, e a guarda não é defensiva: o .sidx é
	// IMUTÁVEL, e gravá-lo para um segmento que ainda recebe records
	// petrificaria um índice sem tudo que vier depois. Falso negativo
	// permanente, que é exatamente o desfecho que esta tarefa existe para
	// eliminar. O índice do corrente é o WIP, e ele se reconstrói na abertura.
	if corrente := s.curSegID.Load(); seg == corrente {
		return fmt.Errorf("%w: o segmento %d é o corrente; o índice dele é o WIP, não um .sidx imutável",
			ErrInvalidArgument, seg)
	}
	caminho := segindex.CaminhoSelado(s.dirDeDados(), seg)
	cobertura, err := s.coberturaDaReconstrucao(caminho, campos)
	if err != nil {
		return err
	}
	e := segindex.NovoEscritor()
	if err := s.varreOsRecordsDoSegmento(ctx, seg, cobertura,
		func(campo types.FieldID, h uint64, row types.RowID) error {
			e.Adiciona(campo, h, row)
			return nil
		}); err != nil {
		return err
	}
	if e.Vazio() {
		// Nenhuma posting sob a cobertura inteira: o índice CORRETO deste
		// segmento é arquivo AUSENTE, e é isso que o Escritor vazio já diz ao
		// não criar nada. O que ele não faz — porque não conhece o que estava lá
		// antes — é tirar do caminho um .sidx anterior, e aqui ele só pode ser
		// uma de duas coisas: seções de campos que deixaram de ser indexados, ou
		// um arquivo que não descreve este segmento. Nos dois casos remover é o
		// certo; deixá-lo seria manter em disco um arquivo que ninguém consulta
		// e que o IndexBytes conta, que é o mesmo modo de falha que o .widx
		// órfão obrigou a fechar.
		return s.removeSeladoObsoleto(seg, caminho)
	}
	if err := e.Escreve(caminho, seg); err != nil {
		return err
	}
	// A evição vem DEPOIS do rename, e sem ela a reconstrução seria trabalho
	// invisível: o s.selados guarda o mapeamento do arquivo ANTERIOR, e o mmap
	// sobrevive ao rename (é o inode que ele segura, não o nome). Uma consulta
	// que já tivesse tocado este segmento continuaria lendo o índice velho —
	// sem a seção do campo novo, portanto ENUMERANDO — até a próxima abertura
	// do dataset, e ainda prenderia o inode substituído.
	//
	// Ela usa o mecanismo que já existe, e não um segundo: o Fecha do Selado
	// impede leitores novos e adia o munmap até a última Fonte em voo sair
	// (contagem de vidas). É a mesma barreira da aposentadoria de segmento, pelo
	// mesmo selMu — e é ela que torna a reescrita segura contra uma varredura em
	// voo. A varredura que sobrevive lê o arquivo ANTIGO até terminar, o que é
	// correto: índice é superconjunto de candidatos, e o executor revalida (§44).
	s.esqueceSelado(seg)
	return nil
}

// coberturaDaReconstrucao decide quais campos o arquivo novo vai cobrir: os
// pedidos MAIS os que o .sidx atual já cobre e continuam declarados.
//
// A união não é generosidade, é a consequência de o arquivo ser IMUTÁVEL.
// "Acrescentar uma seção" é reescrever o arquivo inteiro (§2.6), e um arquivo
// reescrito só com o campo pedido APAGARIA as seções que já estavam lá. O falso
// negativo petrificado voltaria pela porta oposta — e pior, oscilando: a passada
// seguinte veria o campo antigo pendente, reescreveria com ele e perderia o
// novo, para sempre.
//
// Preservar é RE-DERIVAR, e não copiar do arquivo velho, e essa escolha é o
// desenho inteiro em uma linha: os records de um segmento não corrente são
// imutáveis, então as postings derivadas deles são bit a bit as mesmas que estão
// no .sidx atual — e a varredura do segmento, que é o custo inteiro da operação,
// já vai acontecer de qualquer jeito para o campo que falta. Copiar do índice
// velho seria mais código para os mesmos bytes, com um segundo caminho de
// produção do .sidx ao lado do Escritor e uma dependência nova: confiar em dado
// derivado para produzir dado derivado, que é justamente o que a §2.5 recusa
// fazer com o .widx.
//
// Um .sidx ausente ou ilegível não impede nada: não há o que preservar, e a
// cobertura é o que o chamador pediu. Erro que não é de formato sobe — EIO é
// falha de infra, e reescrever o índice por cima dela seria decidir sobre um
// arquivo que ninguém conseguiu ler (§101).
func (s *Store) coberturaDaReconstrucao(caminho string, campos []types.FieldID) ([]types.FieldID, error) {
	cobertura := slices.Clone(campos)
	existentes, err := segindex.CamposSelados(caminho)
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, segindex.ErrFormato):
		existentes = nil
	case err != nil:
		return nil, err
	}
	for _, campo := range existentes {
		// O filtro pelo conjunto declarado é o que faz a §2.6 valer nos DOIS
		// sentidos: a seção de um campo que deixou de ser indexado não entra na
		// cobertura, e por isso a reescrita a apaga. É a única forma de remover
		// seção de um arquivo imutável, e é a mesma operação da selagem.
		if slices.Contains(s.camposIndexados, campo) && !slices.Contains(cobertura, campo) {
			cobertura = append(cobertura, campo)
		}
	}
	// Ordenada e sem repetição: é o que o varreOsRecordsDoSegmento espera e o
	// que torna o arquivo produzido independente da ORDEM em que o chamador
	// listou os campos pendentes — sem isso, dois chamadores com o mesmo
	// conjunto poderiam produzir arquivos diferentes e o golden da §6.3 passaria
	// a depender de quem chamou.
	slices.Sort(cobertura)
	return slices.Compact(cobertura), nil
}

// removeSeladoObsoleto tira do caminho um .sidx que a reconstrução acabou de
// declarar sem conteúdo. Ausência do arquivo não é anomalia: é o estado normal
// de um segmento sem posting nenhuma.
func (s *Store) removeSeladoObsoleto(seg uint32, caminho string) error {
	// O mapeamento é largado ANTES do unlink, e não depois, porque é ele que
	// segura o inode: na ordem inversa, o disco só voltaria ao dispositivo
	// quando a última Fonte em voo terminasse, e um leitor lento manteria vivo
	// um arquivo que já não tem nome.
	s.esqueceSelado(seg)
	if err := os.Remove(caminho); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("storage: remover o índice obsoleto %s: %w", caminho, err)
	}
	return nil
}

// SegmentosPendentesDeReconstrucao devolve, em ordem crescente, os segmentos
// cujo índice selado não descreve o conjunto indexado do dataset.
//
// A fila é DERIVADA do disco a cada chamada, e não uma lista persistida, pela
// mesma razão que o índice é derivado dos records: um metadado a menos é uma
// janela de crash a menos. Uma fila em arquivo teria de ser durabilizada em
// ordem contra a selagem e contra a aposentadoria, e uma fila em memória se
// perderia no restart — que é justamente quando ela é mais necessária.
//
// O TESTE É POR (SEGMENTO, CAMPO), E NÃO POR "TEM .sidx?" — é a metade do débito
// 22 que esta função entrega. Um segmento selado ANTES de um campo ser declarado
// indexado TEM o
// arquivo e não tem a seção daquele campo: pela pergunta antiga ele contava como
// pronto, e o campo novo enumerava indefinidamente, a um custo proporcional ao
// dataset inteiro e não ao resultado. A comparação é de CONJUNTO, nos dois
// sentidos, e a segunda direção é o outro lado da §2.6: seção de um campo que
// deixou de ser indexado também põe o segmento na fila, porque a reescrita é o
// único jeito de tirá-la de um arquivo imutável.
//
// O PREÇO DA PERGUNTA NOVA é um open + mmap + munmap por segmento, no lugar de
// um stat: só o header e a tabela de seções são tocados (segindex.CamposSelados),
// e nem o crc nem uma página de diretório entram na conta. São dezenas de
// microssegundos por arquivo contra a varredura de até 512 MiB que a resposta
// agenda — e é o mínimo que responde a pergunta certa, porque "existe o arquivo?"
// não distingue os dois estados que importam.
//
// Quem entra, então, é o segmento com .dat cujo .sidx está ausente, ilegível ou
// com um conjunto de seções diferente do declarado — exceto o corrente, cujo
// índice é o WIP que a abertura reconstrói antes do primeiro Put. Um .widx de
// segmento sem .dat nunca aparece aqui porque a enumeração parte dos DADOS —
// enfileirá-lo seria mandar a reconstrução varrer um arquivo que não existe,
// para sempre. A remoção dele é da abertura (§2.5, terceiro caso).
//
// Dataset sem campo indexado devolve fila vazia: não há o que reconstruir, e é
// por isso que tirar o ÚLTIMO `index: true` não apaga os .sidx que sobraram —
// sem campo declarado não há reconstrução que os reescreva. Eles não respondem
// nada (o Lookup recusa campo não indexado antes de chegar ao arquivo) e saem de
// disco quando a compaction ou a retenção aposenta o segmento, pelo
// removeIndiceDoSegmento. Uma varredura de limpeza na abertura seria a
// alternativa, e ela é pior: apagaria o índice inteiro de um dataset aberto por
// qualquer caminho que simplesmente não declare os campos.
//
// A FILA É UM SUPERCONJUNTO DO TRABALHO REAL, e isso é deliberado — é a mesma
// doutrina do índice que ela alimenta (§44): candidato a mais custa trabalho,
// candidato a menos é falso negativo, e aqui o falso negativo é um campo que
// enumera para sempre. Dois casos dizem "pendente" sem terem o que fazer, e o
// laço de fundo (§2.4) NÃO pode tratar a fila como trabalho que encolhe a cada
// passada:
//
//   - segmento cujos records não têm campo indexado nenhum: o Escritor vazio não
//     cria arquivo, de propósito, e "ausente" continua sendo pendente;
//   - segmento em que UM dos campos declarados não aparece em record algum: a
//     seção dele não existe e não tem como existir. Distinguir isso de "a seção
//     ainda não foi construída" exigiria o formato dizer quais campos foram
//     CONSIDERADOS, e não só quais produziram posting — e o formato do .sidx não
//     muda neste ciclo.
//
// DUAS ARESTAS que quem montar o laço de fundo (§2.4) precisa tratar, e que
// ficam escritas aqui porque nenhuma delas é visível de dentro desta função:
//
//   - A resposta é um instantâneo, e o segmento recém-rotacionado aparece nela
//     por uma janela. O aposAppend troca o curSegID ANTES de selar o anterior
//     (indices.go), então entre as duas coisas o anterior tem dado e ainda não
//     tem .sidx. A corrida no nome FINAL é neutra — a igualdade byte a byte é
//     provada pelo golden —, mas hoje os dois caminhos escreveriam no MESMO
//     `<sidx>.tmp`, que ainda é nome FIXO (segindex/escritor.go), e o arquivo
//     sairia rasgado. É esta a correção que a §2.4 emendada põe na Tarefa 14:
//     nome de temporário único por escritor, e não exclusão mútua.
//   - Um leitor que abra o .sidx ANTES da reescrita e o publique no cache DEPOIS
//     da evição que a reescrita faz (ConstroiIndiceDoSegmento) fica com o
//     mapeamento velho até a próxima abertura do dataset. Degrada na direção
//     segura — seção ausente ENUMERA —, mas o segmento perde o ganho até lá.
//     Fechar isso pede uma geração no cache de selados, e ela pertence a quem
//     introduzir a concorrência: hoje não há chamador de fundo.
//
// DOS QUATRO GATILHOS DA §2.4, ESTA FUNÇÃO COBRE DOIS: o formato novo sobre
// dataset antigo (migração) e o campo acrescentado ao schema (§2.6). A janela
// alargada depende de a janela existir; o crc32c divergente (§1.5) só é
// conhecido na primeira consulta da seção, e quem o detectar precisa apagar o
// arquivo para o segmento voltar a ser pendente por AUSÊNCIA, que é a pergunta
// barata desta função.
func (s *Store) SegmentosPendentesDeReconstrucao() ([]uint32, error) {
	if len(s.camposIndexados) == 0 {
		return nil, nil
	}
	dir := s.dirDeDados()
	refs, err := segment.ListSegments(dir)
	if err != nil {
		return nil, err
	}
	corrente := s.curSegID.Load()
	var pendentes []uint32
	for _, ref := range refs {
		if ref.ID == corrente {
			continue
		}
		pendente, err := s.segmentoPendente(dir, ref.ID)
		if err != nil {
			return nil, err
		}
		if pendente {
			pendentes = append(pendentes, ref.ID)
		}
	}
	return pendentes, nil // ListSegments já devolve ordenado por ID
}

// segmentoPendente responde se o .sidx do segmento descreve exatamente o
// conjunto indexado do dataset.
//
// Os dois conjuntos são comparados com slices.Equal porque os DOIS são
// crescentes por contrato: a tabela de seções é validada como ordenada na
// abertura (segindex.leTabelaDeSecoes) e o s.camposIndexados sai ordenado do
// resolveCamposIndexados. Ordenar aqui de novo seria esconder uma quebra dessas
// duas garantias em vez de deixá-la aparecer.
//
// Arquivo ILEGÍVEL conta como pendente, e não como erro: um .sidx que este
// binário não decodifica é dado derivado que não serve, e a resposta para dado
// derivado que não serve é derivá-lo de novo. Erro que NÃO é de formato sobe
// intacto — EIO ao abrir o índice é falha de infra, e chamá-la de "reconstrua"
// mandaria o laço varrer 512 MiB por um disco doente (§101, três baldes).
func (s *Store) segmentoPendente(dir string, seg uint32) (bool, error) {
	campos, err := segindex.CamposSelados(segindex.CaminhoSelado(dir, seg))
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, segindex.ErrFormato):
		return true, nil
	case err != nil:
		return false, err
	}
	return !slices.Equal(campos, s.camposIndexados), nil
}

// reconstroiOIndiceDoCorrente realimenta os WIPs recém-abertos lendo o próprio
// segmento corrente.
//
// É a segunda metade da §2.5, e sem ela a primeira é um defeito: depois de todo
// restart o WIP do corrente nasce VAZIO — o replay da recovery roda antes de os
// índices existirem, e o .widx anterior é descartado porque um SIGKILL deixa
// header atrasado sobre diretório adiantado. Se esse segmento rotacionar antes de
// alguém reconstruí-lo, a selagem grava um .sidx IMUTÁVEL sem os records
// anteriores ao restart: falso negativo petrificado, e a revalidação do §44 só
// sabe descartar candidato a mais.
//
// O CUSTO É LIMITADO POR CONSTRUÇÃO, e é o argumento inteiro do desenho: um
// segmento tem no máximo 512 MiB, então isto é uma leitura sequencial de ~1 s na
// subida, uma vez. O que ela compra é não confiar no WIP através de um restart —
// e com isso somem o msync do índice no checkpoint, a invariante de ordem contra
// o durableOffset e a classe inteira de janelas de crash do índice secundário.
// Índice é dado derivado: a recuperação mais barata que está correta é derivá-lo
// de novo.
//
// Roda dentro do Open, depois do abreWIPsDoCorrente e ANTES de o primeiro Put ser
// aceito. Não há ctx porque não há quem cancele: a abertura ou termina ou falha.
func (s *Store) reconstroiOIndiceDoCorrente() error {
	if len(s.wips) == 0 {
		return nil
	}
	return s.varreOsRecordsDoSegmento(context.Background(), s.curSegID.Load(), s.camposIndexados,
		func(campo types.FieldID, h uint64, row types.RowID) error {
			// O mapa não pode errar: ele foi construído a partir DESTE mesmo
			// s.camposIndexados, no abreWIPsDoCorrente, algumas linhas acima.
			return s.wips[campo].Set(h, row)
		})
}

// varreOsRecordsDoSegmento percorre os records do segmento e emite, para cada
// campo indexado presente no payload de cada record, a tripla que o índice
// guarda: (campo, hash da chave canônica, rowID).
//
// UM varredor só, para os dois destinos — o Escritor da forma selada e os WIPs do
// segmento corrente. Dois varredores do mesmo formato divergiriam, e a
// divergência seria invisível: os dois produziriam arquivos bem formados, com
// conjuntos de postings diferentes.
//
// O ScanFrom é o MESMO iterador que o replay do tail usa, pela mesma razão que o
// replay passa pelo applyRecord: um segundo leitor do formato de record é um
// segundo lugar para o formato mudar pela metade. Ele para no primeiro record
// inválido sem erro (§52), e isso está certo aqui: no segmento corrente o
// OpenWriter já truncou o lixo além do fim válido, e nenhum outro segmento pode
// ter tail rasgado — a recovery recusa o dataset em que isso aconteça, e a
// rotação sela o anterior com fdatasync antes de criar o próximo.
//
// A pertinência do campo sai de `campos`, que é sempre um SUBCONJUNTO do que a
// ingestão indexa (s.camposIndexados): o WIP do corrente usa o conjunto inteiro e
// a reconstrução usa a cobertura decidida pelo coberturaDaReconstrucao. É essa
// contenção que faz a alimentação incremental e a reconstruída coincidirem campo
// a campo — um campo FORA do conjunto indexado ganharia seção que a ingestão não
// alimenta, e é por isso que ConstroiIndiceDoSegmento o recusa antes de chegar
// aqui. slices.Contains sobre um conjunto ordenado de punhado de campos é mais
// barato que o mapa que o substituiria.
//
// A chave canônica é montada com valuekey.Append sobre um buffer LOCAL, e não
// sobre o s.chaveIndiceBuf do caminho de escrita: aquele é reutilizado sob o wmu,
// e esta varredura roda fora dele.
func (s *Store) varreOsRecordsDoSegmento(
	ctx context.Context,
	seg uint32,
	campos []types.FieldID,
	emite func(campo types.FieldID, h uint64, row types.RowID) error,
) error {
	var (
		chave  []byte
		leitor encoding.FieldReader
	)
	_, err := s.segR.ScanFrom(seg, int64(segment.HeaderSize), func(_ uint32, rec types.Record) error {
		// O cancelamento é conferido por RECORD, e não por segmento: um
		// segmento de 512 MiB é a unidade que se quer poder abandonar no meio,
		// e conferir só na entrada tornaria o ctx decorativo.
		if err := ctx.Err(); err != nil {
			return err
		}
		leitor.Reset(rec.Payload)
		for {
			id, v, ok := leitor.Next()
			if !ok {
				return leitor.Err()
			}
			if !slices.Contains(campos, id) {
				continue
			}
			k, err := valuekey.Append(chave[:0], v)
			if err != nil {
				return err
			}
			chave = k
			// primary.HashKey é a MESMA função da ingestão, e a escolha não é
			// gosto: ela nunca devolve 0 nem 1, e 0 é o sentinela de slot vazio
			// do WIP. Trocá-la aqui daria um arquivo bem formado e um conjunto
			// de postings que não casa com nada.
			if err := emite(id, primary.HashKey(k), rec.RowID); err != nil {
				return err
			}
		}
	})
	return err
}
