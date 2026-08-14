LOAD: criando 8000000 entidades...
KAFKA: produzindo 8000000 eventos em nishidb-bench...
LOAD: 8000000 eventos em 57.5s (139249 ev/s; ingestão 54.6s a 146419 ev/s, 16 checkpoints em 2.8s), RSS 463.0 MiB
STEADY: 1m0s a 500 ev/s com 0 leitores e 4 consultas SQL...
KAFKA: produzindo 30000 eventos em nishidb-bench...
STEADY: varredura completa em 21.033s
STEADY: 30000 eventos, RSS max 466.0 MiB, 0 erros de query
RESTART: 0.28s (§125)

--- GATES ---
PASS     rss_limit              §109   max(VmRSS) = 491.0 MiB, limite 1.00 GiB
PASS     heap_decoupled         §110   heap 2.3 MiB (8000000 entidades) vs 668.5 KiB (100000 entidades): 3.55x de heap para 80.0x de dados — abaixo do piso de 16.0 MiB, onde a razão vira ruído de GC e o desacoplamento está provado pela grandeza absoluta
PASS     ingest_throughput      §88    LOAD a 139249 ev/s, mínimo 2000
PASS     allocs_per_event       §111   0.0206 allocs/evento na fase load (ingestão pura) (alvo 9; soft no M1); steady com 4 leitores SQL = 665.02 (inclui as alocações das queries)
PASS     no_query_error         §106   0 erros nas consultas SQL concorrentes com a ingestão
PASS     no_event_loss          §101   0 eventos descartados (skipped+failed)
PASS     latency_sql_point_lookup §89    p95 = 19 µs, teto 10000 µs (246356 amostras)
PASS     latency_sql_bitmap_limit_100 §90    p95 = 321 µs, teto 50000 µs (246357 amostras)
PASS     latency_sql_intersect_limit_100 §90    p95 = 469 µs, teto 50000 µs (246357 amostras)
PASS     latency_sql_union_limit_100 §90    p95 = 342 µs, teto 50000 µs (246358 amostras)
PASS     latency_sql_full_scan_limit_100 §90    p95 = 2223 µs, teto 50000 µs (246358 amostras)

rodada aprovada
