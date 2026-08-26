# Aceite — client Python (DB-API 2.0)

Cada critério aponta para o **nome de um teste** da suíte, e
`TestEvidenciasCitadasNosDocumentosExistem` reprova se algum deles não existir.
É o que impede este documento de afirmar cobertura que não há.

Suítes: `make test-python` (unitários, sem servidor e sem `pip install`) e
`make test-python-it` (contra o binário real, com TLS e credencial).

---

## 1. Contrato da PEP 249

| # | Critério | Evidência |
|---|---|---|
| 1.1 | O módulo anuncia `apilevel`, `threadsafety` e `paramstyle`, e o paramstyle é o que o servidor aceita | `TestOClientPythonAnunciaOParamstyleDoServidor` |
| 1.2 | `Cursor.description` publica as sete tuplas, saídas da linha de schema | `test_as_sete_tuplas_saem_da_linha_de_schema` |
| 1.3 | `type_code` compara igual aos objetos de tipo do módulo | `test_o_type_code_compara_com_os_objetos_de_tipo_da_pep` |
| 1.4 | `description` está pronto assim que `execute()` volta | `test_description_fica_pronto_antes_da_primeira_linha` |
| 1.5 | `rowcount` é -1 enquanto o stream corre e a contagem real depois do fecho | `test_e_menos_um_enquanto_o_stream_corre` |
| 1.6 | `fetchmany` respeita o tamanho pedido; `fetchall` traz o que resta | `test_fetchmany_respeita_o_tamanho`, `test_fetchall_traz_o_que_resta` |
| 1.7 | `commit()` é no-op e `rollback()` lança | `test_commit_e_no_op`, `test_rollback_lanca` |
| 1.8 | `executemany()` lança nomeando a razão | `test_executemany_lanca` |
| 1.9 | Cursor e conexão fechados recusam uso | `test_cursor_fechado_recusa_uso`, `test_conexao_fechada_recusa_uso` |
| 1.10 | A hierarquia de exceções está exposta na conexão | `test_a_hierarquia_de_excecoes_esta_na_conexao` |
| 1.11 | Os construtores de data/hora não dependem do fuso da máquina | `test_os_construtores_nao_dependem_do_fuso_da_maquina` |

## 2. Streaming (§128 no cliente)

| # | Critério | Evidência |
|---|---|---|
| 2.1 | O pico de memória NÃO cresce com o número de linhas | `test_o_pico_nao_cresce_com_o_numero_de_linhas` |
| 2.2 | A medida é sensível — `fetchall`, que promete materializar, materializa | `test_fetchall_materializa_e_e_isso_que_ele_promete` |
| 2.3 | 120 mil linhas do servidor REAL atravessam com memória limitada | `test_o_resultado_inteiro_atravessa_com_memoria_limitada` |
| 2.4 | O passthrough NDJSON também atravessa | `test_as_linhas_cruas_tambem_atravessam` |
| 2.5 | As linhas cruas preservam a grafia exata do servidor | `test_linhas_cruas_preservam_a_grafia_do_servidor` |

## 3. Desfechos do stream

Os três ficam SEPARADOS: fim, erro do servidor e falha de transporte.

| # | Critério | Evidência |
|---|---|---|
| 3.1 | Stream que acaba sem linha de fecho é FALHA, não resultado curto | `test_stream_sem_fecho_e_falha` |
| 3.2 | A primeira linha tem de ser o schema | `test_primeira_linha_que_nao_e_schema_e_recusada` |
| 3.3 | Moldura desconhecida é recusada | `test_moldura_desconhecida_e_recusada` |
| 3.4 | O fecho de erro cita as linhas já entregues | `test_fecho_de_erro_vira_excecao_citando_as_linhas_entregues` |
| 3.5 | A falha é STICKY: quem engole a exceção e chama de novo não recebe "acabou" | `test_a_falha_e_sticky` |
| 3.6 | Erro que nasce antes da primeira linha falha no `execute()` | `test_fecho_de_erro_logo_apos_o_schema_falha_no_execute` |
| 3.7 | `MAX_ROWS_EXCEEDED` é AVISO, nunca exceção, e preserva as linhas | `test_max_rows_exceeded_e_aviso_e_preserva_as_linhas` |
| 3.8 | `truncated` pedido pelo próprio cliente é sucesso | `test_truncado_pelo_proprio_cliente_e_sucesso` |

## 4. Casamento row↔schema

| # | Critério | Evidência |
|---|---|---|
| 4.1 | `SELECT a, a` não perde a coluna repetida | `test_coluna_repetida_nao_some` |
| 4.2 | A linha é sequência E mapping | `test_a_linha_e_sequencia_e_mapping` |
| 4.3 | Nome exato vence; sem caixa funciona como segundo passo | `test_nome_de_coluna_sem_caixa_funciona_mas_o_exato_vence` |
| 4.4 | Ambiguidade sem caixa é recusada, não resolvida por sorteio | `test_nome_ambiguo_sem_caixa_e_recusado` |

## 5. Tipos

| # | Critério | Evidência |
|---|---|---|
| 5.1 | A matriz inteira vale em UTC e em `Asia/Tokyo` | `test_em_utc`, `test_em_asia_tokyo`, `test_em_america_sao_paulo` |
| 5.2 | `timestamp` volta AWARE em UTC; `time` volta naive | (na matriz acima) |
| 5.3 | `int64` nos limites é exato, sem passar por float | `test_int64_no_limite_viaja_exato`, `test_os_valores_de_borda_atravessam_intactos` |
| 5.4 | Double faz round-trip de BITS, não de texto | `test_double_faz_round_trip_de_bits` |
| 5.5 | Double de valor inteiro continua float — dtype estável na coluna | `test_double_de_valor_inteiro_continua_float` |
| 5.6 | Base64 só é decodificado em coluna binária | `test_base64_so_e_decodificado_em_coluna_binaria` |
| 5.7 | Lixo no base64 é erro, não array truncado | `test_lixo_a_direita_e_erro_e_nao_array_truncado` |
| 5.8 | Coluna `json` sai inline, como objeto | `test_o_json_sai_inline_e_nao_como_texto` |
| 5.9 | Coluna OTHER segue o token | `test_coluna_other_segue_o_token` |
| 5.10 | `jdbc` desconhecido não quebra o cursor | `test_jdbc_desconhecido_nao_quebra_o_cursor` |
| 5.11 | Booleano não passa por BIGINT nem por DOUBLE | `test_booleano_nao_passa_por_bigint`, `test_booleano_nao_passa_por_double` |

## 6. Parâmetros

| # | Critério | Evidência |
|---|---|---|
| 6.1 | Os quatro tipos de wire, e só eles | `test_os_quatro_tipos_de_wire` |
| 6.2 | `bool` não vira inteiro (é subclasse de `int` no Python) | `test_booleano_nao_vira_inteiro` |
| 6.3 | `datetime` viaja como epoch millis, nunca ISO-8601 | `test_datetime_viaja_como_epoch_millis` |
| 6.4 | `datetime` naive é RECUSADO, e não interpretado no fuso da máquina | `test_datetime_naive_e_recusado` |
| 6.5 | O epoch millis não depende do fuso da máquina | `test_o_epoch_millis_nao_depende_do_fuso_da_maquina` |
| 6.6 | `None` é recusado ensinando `IS NULL` | `test_parametro_nulo_ensina_is_null` |
| 6.7 | Contagem errada de `?` é recusada no cliente | `test_contagem_de_parametros_errada_e_recusada_no_cliente` |
| 6.8 | Ponta a ponta contra o servidor real | `test_parametro_de_texto_filtra`, `test_parametro_int64_no_limite`, `test_like_filtra_por_padrao` |

## 7. Corpo da requisição

| # | Critério | Evidência |
|---|---|---|
| 7.1 | Exatamente os campos conhecidos (o servidor usa `DisallowUnknownFields`) | `test_o_corpo_tem_exatamente_os_campos_conhecidos` |
| 7.2 | O dataset sai do `FROM`, com o da conexão como fallback | `test_o_dataset_sai_do_from`, `test_sem_from_reconhecivel_vale_o_dataset_da_conexao` |
| 7.3 | `maxRows` acima do teto ou zero é OMITIDO, nunca enviado | `test_max_rows_acima_do_teto_e_omitido`, `test_max_rows_zero_e_omitido` |
| 7.4 | `timeoutMs` é aparado contra o teto do dataset | `test_timeout_e_aparado_contra_o_teto_do_dataset` |

## 8. Scanner de SQL

| # | Critério | Evidência |
|---|---|---|
| 8.1 | Nunca levanta, para entrada nenhuma — é extrator de palpite | `test_nunca_levanta_para_entrada_nenhuma` |
| 8.2 | A caixa do nome é preservada (identificadores são case-sensitive) | `test_a_caixa_do_nome_e_preservada` |
| 8.3 | Branco é ASCII-only, como no lexer do servidor | `test_espaco_nao_quebravel_nao_e_branco` |
| 8.4 | Nome qualificado (`_meta.fields`) abandona o palpite | `test_nome_qualificado_abandona_o_palpite` |
| 8.5 | `?` e `FROM` dentro de literal não contam | `test_interrogacao_dentro_de_literal_nao_conta`, `test_a_palavra_from_dentro_de_literal_nao_dispara` |
| 8.6 | Aspa duplicada é escape, não fechamento | `test_aspa_duplicada_no_literal_mantem_o_interrogacao_dentro` |

## 9. Transporte e segurança

| # | Critério | Evidência |
|---|---|---|
| 9.1 | Basic é PREEMPTIVO (o servidor cobra 100 ms por falha de auth) | `test_basic_e_preemptivo_no_primeiro_request` |
| 9.2 | Sem credencial, nenhum header `Authorization` | `test_anonimo_nao_manda_authorization` |
| 9.3 | Senha na string de conexão é RECUSADA | `test_senha_na_query_string_e_recusada` |
| 9.4 | Credencial embutida na URL é recusada, não ignorada | `test_credencial_embutida_e_recusada_e_nao_ignorada` |
| 9.5 | Fonte única de senha, com as quatro recusas do CLI | `test_senha_sem_usuario_e_recusada`, `test_usuario_sem_senha_e_recusado`, `test_duas_fontes_ao_mesmo_tempo_sao_recusadas` |
| 9.6 | Variável definida e VAZIA não é o mesmo que ausente | `test_variavel_definida_e_vazia_nao_e_o_mesmo_que_ausente` |
| 9.7 | Nenhuma mensagem de erro ecoa o valor da senha | `test_nenhuma_mensagem_ecoa_o_valor_da_senha` |
| 9.8 | **Sem a CA, a conexão TLS FALHA** — a prova de que não há verificação desligada | `test_sem_a_ca_a_conexao_falha` |
| 9.9 | `tls_ca` exige `tls=true` | `test_tls_ca_exige_tls` |
| 9.10 | `tls` só aceita `true`/`false` — nunca "falso por default" | `test_tls_so_aceita_true_ou_false` |
| 9.11 | A conexão é reusável entre consultas | `test_duas_consultas_na_mesma_conexao` |
| 9.12 | Stream abandonado no meio derruba a conexão em vez de reaproveitá-la | `test_consulta_depois_de_resultado_abandonado_no_meio` |

## 10. Erros

| # | Critério | Evidência |
|---|---|---|
| 10.1 | TODO código do protocolo tem exceção — cobrado do lado Go | `TestTodoCodigoDoProtocoloTemExcecaoNoClientPython` |
| 10.2 | Código desconhecido NÃO degrada para `INTERNAL` | `test_codigo_desconhecido_nao_degrada_para_internal` |
| 10.3 | `UNSUPPORTED_CLAUSE` é distinto de erro de sintaxe | `test_clausula_nao_suportada_e_distinta_de_erro_de_sintaxe`, `test_group_by_e_clausula_nao_suportada_e_nao_erro_de_sintaxe` |
| 10.4 | Erro de sintaxe traz linha e coluna | `test_erro_antes_do_stream_traz_linha_e_coluna` |
| 10.5 | Código transiente traz `transient` e `retry_after` | `test_codigo_transiente_traz_a_dica_de_repetir` |
| 10.6 | Dataset inexistente falha ao ABRIR a conexão | `test_dataset_inexistente_falha_ao_abrir_a_conexao`, `test_dataset_inexistente_falha_ao_abrir` |
| 10.7 | Credencial errada não diz qual metade falhou | `test_credencial_errada_e_recusada_sem_dizer_qual_metade_falhou` |

## 11. Metadados contra o servidor real

| # | Critério | Evidência |
|---|---|---|
| 11.1 | Chave e sequence saem NOT NULL | `test_a_chave_e_a_sequence_sao_nao_nulaveis` |
| 11.2 | Precision/scale/displaySize vêm do servidor, e o `ipv6` é 39 | `test_os_numeros_do_descritor_vem_do_servidor` |
| 11.3 | As duas rotas nomeiam a coluna com alias de formas diferentes | `test_as_duas_rotas_nomeiam_a_coluna_com_alias_de_formas_diferentes` |
| 11.4 | Os tetos efetivos são publicados e lidos no connect | `test_os_tetos_efetivos_sao_publicados`, `test_os_tetos_sao_buscados_antes_da_primeira_consulta` |
| 11.5 | `_meta.fields` traz o índice físico | `test_describe_traz_o_indice_fisico` |
| 11.6 | `/v1/health` com 503 é resposta, não falha | `test_503_no_health_e_resposta_e_nao_falha`, `test_health_responde_up` |
| 11.7 | `ORDER BY` EXISTE no dialeto e não é recusado | `test_order_by_existe_e_nao_e_recusado` |

## 12. Empacotamento e CI

| # | Critério | Evidência |
|---|---|---|
| 12.1 | Zero dependência de runtime | `TestOPyprojectDoClientPythonNaoTemDependenciaDeRuntime` |
| 12.2 | A versão do pacote e a do módulo não divergem | `TestAVersaoDoClientPythonEhADoPyproject` |
| 12.3 | Os artefatos de build não são versionáveis por acidente | `TestOsArtefatosDoClientPythonEstaoNoGitignore` |
| 12.4 | O CI roda a integração contra o binário real | `TestWorkflowDeCIRodaOTesteDeIntegracaoDoClientPython` |
| 12.5 | O CI roda lint e tipagem | `TestWorkflowDeCIRodaOLintDoClientPython` |
| 12.6 | A versão de Python do CI é o piso EXATO do `requires-python` | `TestWorkflowDeCIPinaPythonNoPisoDoPyproject` |
| 12.7 | O painel e o client rodam no mesmo alvo, sem instalar nada | `TestWorkflowDeCIRodaOTestePython` |

---

## Fora do escopo, e por quê

| Item | Razão |
|---|---|
| Rotas `POST /v1/compact` e `/v1/checkpoint` (papel ADMIN) | Decisão de escopo: o client é de LEITURA. São dois POSTs sobre o mesmo transporte já pronto e entram depois sem redesenho |
| Dialeto SQLAlchemy | Fase seguinte, sobre um DB-API já provado. O ganho é BI (Superset, Metabase); o custo é declarar honestamente a ausência de JOIN e GROUP BY, que é trabalho de projeto e não de tradução |
| Client asyncio | Dobraria a superfície de teste, e o núcleo é síncrono ponta a ponta |
| Integração explícita com pandas | Não é preciso: `pandas.read_sql` fala DB-API. Escrever um adaptador seria criar um segundo caminho para o que já funciona |

## Limites conhecidos do client

Registrados aqui porque um limite não escrito vira surpresa em produção:

- **Não há parâmetro nulo** no protocolo: `IS NULL` vai na própria query.
- **`datetime` sem fuso é recusado.** Assumir o fuso da máquina deslocaria o
  valor por horas sem nada acusar.
- **Quatro nulos do fio não são NULL** e não são distinguíveis no client:
  double não-finito, `time` fora do dia, `json` vazio e valor malformado. Quem
  procurar a causa tem de olhar o dado.
- **Coluna `json` passa por `json.loads`**, então um double dentro dela
  round-trip por `float`. Quem precisa dos bytes exatos usa `query_raw()`.
- **Uma conexão executa uma consulta por vez** (`threadsafety = 1`).
