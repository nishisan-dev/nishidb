package kafka

// MaxProducedRecordBytes é o teto do registro que os produtores do nishidb
// (a série e os alarmes) aceitam entregar ao kgo: chave, valor e cabeçalhos
// somados, no molde do userSize do kgo (franz-go v1.21.6,
// record_and_fetch.go:157-163).
//
// O kgo recusa com MessageTooLarge o registro que não cabe num lote vazio de
// 1.000.012 bytes, o default do `max.message.bytes` do Kafka (config.go:666),
// e o lote soma ao registro o cabeçalho do lote (sink.go:1968-1981) e os
// varints do registro (sink.go:2341-2373). Os 128 bytes de folga cobrem os
// dois. Um teto local abaixo do do kgo é o que deixa a recusa acontecer na
// ADMISSÃO, com motivo próprio, e não na tentativa de entrega.
const MaxProducedRecordBytes = 1_000_012 - 128
