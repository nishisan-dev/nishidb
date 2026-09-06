package protocol

// Os corpos de POST /v1/compact e POST /v1/checkpoint (§96, Fase 11, Task
// B5, decisão D3 do roadmap): as duas primeiras rotas que MUDAM estado pelo
// protocolo remoto, atrás do papel ADMIN.
//
// As duas compartilham o mesmo corpo de REQUISIÇÃO — um dataset por nome, ou
// nenhum campo para TODOS, na ordem determinística da subida — porque a
// pergunta "em qual dataset" é idêntica nas duas rotas, e um cliente que já
// sabe montar uma não aprende um segundo formato para a outra.

// RequisicaoDataset é o corpo OPCIONAL de POST /v1/compact e POST
// /v1/checkpoint. Corpo ausente (Content-Length: 0) é aceito e equivale a
// Dataset == "" — as duas rotas tratam os dois casos da mesma forma.
type RequisicaoDataset struct {
	Dataset string `json:"dataset,omitempty"`
}

// RespostaCheckpoint é o corpo de 200 de POST /v1/checkpoint.
type RespostaCheckpoint struct {
	Datasets []CheckpointDataset `json:"datasets"`
}

// CheckpointDataset é o resultado do checkpoint de UM dataset.
//
// Não há campo de "mudou alguma coisa": o checkpoint é idempotente por
// EFEITO — repetido sobre estado inalterado ele custa o stall real da
// ingestão (é o mutex do writer, não um cache) mas não move manifest nenhum
// de forma observável. DurationMs é a única medida que distingue as
// chamadas entre si.
type CheckpointDataset struct {
	Name       string `json:"name"`
	DurationMs int64  `json:"durationMs"`
}

// RespostaCompact é o corpo de 200 de POST /v1/compact.
type RespostaCompact struct {
	Datasets []CompactDataset `json:"datasets"`
}

// CompactDataset é o resultado de UMA passada manual de compaction sobre um
// dataset. SegmentsRetired, BytesRead e BytesWritten saem ZERADOS quando a
// eleição não encontrou candidato — resultado LEGÍTIMO ("nada para
// compactar agora"), não ausência de resposta; é o mesmo "idempotente por
// efeito" do checkpoint, e a razão de não haver um campo booleano de
// "aconteceu alguma coisa" ao lado dos números. DurationMs NÃO segue essa
// regra: mesmo uma eleição vazia custa a varredura do tally sob o mutex do
// writer, e a duração observada é a real, não um zero convencionado.
type CompactDataset struct {
	Name            string `json:"name"`
	SegmentsRetired int    `json:"segmentsRetired"`
	BytesRead       int64  `json:"bytesRead"`
	BytesWritten    int64  `json:"bytesWritten"`
	DurationMs      int64  `json:"durationMs"`
}
