// Package micro reúne os micro-benchmarks integrados do M1: o pipeline de
// ingestão (jsonp → typeconv → schema → merge → Put) e os três caminhos de
// leitura que o executor SQL vai usar por baixo (point lookup, bitmap com
// limite, full scan).
//
// Existe separado dos benchmarks de cada pacote porque mede a COMPOSIÇÃO com
// corpus realista (radiusgen + YAML de produção), enquanto os de pacote medem
// uma peça com entrada sintética. É daqui que saem os baselines de
// benchmark/baselines/, comparados com benchstat entre commits.
//
// Onde cada benchmark escreve, e por quê:
//
//   - os de LEITURA carregam um dataset pequeno (~5k entidades, alguns MiB) em
//     b.TempDir() — cabe em qualquer lugar, inclusive tmpfs;
//   - os de ESCRITA no Store real exigem NISHIDB_BENCH_DIR apontando para
//     DISCO, e pulam sem ela. Cada iteração appenda um record: uma rodada de
//     poucos segundos gera centenas de MiB de segmentos, com fallocate real.
//     Em /tmp montado como tmpfs isso vira RAM e trava a máquina — foi por
//     isso que a exigência virou regra do projeto, não sugestão;
//   - os de Apply puro rodam contra dublês que não retêm estado, então não
//     escrevem nada e não crescem.
package micro
