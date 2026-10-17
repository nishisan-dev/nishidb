// Package metareplication serve a tabela virtual `_meta.replication`: a
// publicação do feed de replicação do dataset master nos destinos Kafka, por
// SQL, uma linha por destino.
//
// Mora fora de `internal/replicationkafka` e fora de `internal/query` pelo
// argumento do `internal/metakafka`: precisa dos dois lados (o retrato do
// publicador e do feed de um, o Store virtual do outro), e nenhum dos dois
// pode passar a importar o outro.
package metareplication

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
)

// TableName é o nome da tabela sob o `_meta`.
const TableName = "replication"

// CheckpointRow é a linha de um destino lida do disco, sem publicador no ar:
// o que o `nishidb query --config` sabe de um destino declarado em
// `replication.targets`.
type CheckpointRow struct {
	// Destination é o nome do destino em `replication.targets`.
	Destination string
	// Topic é o tópico do destino na configuração (`kafkaTargets:`), que vale
	// mesmo sem .ckpt.
	Topic string
	// Found diz se o .ckpt existe e foi lido; com Found=false, Checkpoint é
	// zero e as colunas dele saem NULL.
	Found      bool
	Checkpoint replicationkafka.CheckpointRow
	// Err é a leitura do .ckpt que falhou (arquivo ilegível). A linha existe
	// do mesmo jeito, com o erro em lastError: o destino declarado não some
	// da tabela porque o arquivo dele está corrompido.
	Err error
}

// Source é o que a tabela precisa do dataset, RESOLVIDO NA HORA da consulta.
//
// Todo closure é opcional. Nil é ausência, e ausência vira NULL (ou nenhuma
// linha), nunca zero:
//
//   - Feed devolve o retrato do feed e ok=false quando não há feed (seguidor,
//     replicação desligada, ou, offline, feed que não fechou limpo). As
//     colunas do feed repetem em toda linha, como as de dataset da
//     `_meta.kafka`.
//   - Destinations devolve o retrato de cada publicador vivo, na ordem de
//     `replication.targets`. Quando não é nil, é ELE a fonte das linhas:
//     o publicador vivo sabe tudo o que o .ckpt sabe, e mais.
//   - Checkpoints é a fonte das linhas sem processo (o CLI offline), usada só
//     com Destinations nil. As colunas que só existem num publicador vivo —
//     state, cursor, lag, cache, lastResetReason, lastSuccessAt — saem NULL.
//
// Nenhum campo guarda o publicador: no servidor eles nascem depois do
// registro da tabela (openReplicationPublishers roda com todos os datasets
// abertos), e um retrato capturado no registro descreveria para sempre o
// nada que havia ali.
type Source struct {
	Feed         func() (replication.Status, bool)
	Destinations func() []replicationkafka.Stats
	Checkpoints  func() []CheckpointRow
}

// Register declara `_meta.replication` sob o dataset já registrado no motor.
//
// Chamado por todo caminho que registra um dataset BASE — servidor e CLI —,
// SEMPRE, inclusive num dataset sem replicação: "não publica em Kafka" é uma
// resposta (a tabela vazia), e `SELECT * FROM _meta.replication` nunca pode
// ser erro.
func Register(eng *query.Engine, dataset string, src Source) error {
	sc, store, err := Table(src)
	if err != nil {
		return err
	}
	return eng.RegisterVirtual(dataset, meta.Namespace, TableName, sc, store)
}

func columnError(name string) error {
	return fmt.Errorf("metareplication: column %q has no FieldID in the synthetic schema", name)
}
