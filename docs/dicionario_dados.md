# Dicionario de dados - Fila Justa

**Escopo:** 14 tabelas permanentes + 3 views publicadas (inclui `silver.v_chuva_48h_area`). Campos levantados diretamente do DDL de `include/sql/01_bronze.sql`, `02_silver.sql`, `03_gold.sql`, views corrigidas e migracao de `gold.fact_score` em `score.py`.

**Nota de origem:** a camada bronze conserva os campos operacionais selecionados, nao o CSV inteiro: endereco, CEP e texto livre nao sao persistidos. `—` em Exemplo significa que as evidencias disponibilizadas nao mostram um valor daquela coluna; nao e um dado ausente na base. Exemplos que trazem anotacoes municipais ou rodadas anteriores sao referencias de validacao, nao pares de valores da mesma linha.

**Chaves:** PK = chave primaria no banco. Em views, nao existe PK fisica. A Gold conserva scores por `run_id`, enquanto `gold.fact_ocorrencia` e estado corrente por cluster.

## bronze.raw_sp156 (TABELA)

**Origem principal:** SP156: CSVs trimestrais publicados (campos sanitizados).

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| id **[PK]** | BIGINT | Identificador tecnico interno (identity). | SP156: CSVs trimestrais publicados (campos sanitizados) | — |
| fonte_arquivo | TEXT | Identificador estavel do recurso trimestral SP156. | SP156: CSVs trimestrais publicados (campos sanitizados) | sp156_2026_T1 |
| numero_linha | INTEGER | Posicao da linha no CSV de origem; chave de idempotencia por arquivo. | SP156: CSVs trimestrais publicados (campos sanitizados) | — |
| data_abertura_raw | TEXT | Data de abertura sem conversao (formato do CSV). | SP156: CSVs trimestrais publicados (campos sanitizados) | 07/01/2026 |
| data_finalizacao_raw | TEXT | Data de finalizacao como texto recebido. | SP156: CSVs trimestrais publicados (campos sanitizados) | — |
| meio_comunicacao_raw | TEXT | Canal de entrada declarado na fonte municipal. | SP156: CSVs trimestrais publicados (campos sanitizados) | PORTAL |
| tema_raw | TEXT | Tema original do solicitacao SP156. | SP156: CSVs trimestrais publicados (campos sanitizados) | Rua e bairro |
| assunto_raw | TEXT | Campo previsto no DDL; o conector atual nao preenche texto livre. | SP156: CSVs trimestrais publicados (campos sanitizados) | — |
| servico_raw | TEXT | Descricao oficial do servico SP156. | SP156: CSVs trimestrais publicados (campos sanitizados) | Tapa-buraco |
| status_raw | TEXT | Situacao de origem antes da normalizacao. | SP156: CSVs trimestrais publicados (campos sanitizados) | Aberto |
| latitude_raw | TEXT | Latitude da fonte em formato textual. | SP156: CSVs trimestrais publicados (campos sanitizados) | — |
| longitude_raw | TEXT | Longitude da fonte em formato textual. | SP156: CSVs trimestrais publicados (campos sanitizados) | — |
| distrito_raw | TEXT | Distrito informado (nome ou codigo sem mapeamento). | SP156: CSVs trimestrais publicados (campos sanitizados) | — |
| prefeitura_operacional_raw | TEXT | Unidade operacional presente no SP156. | SP156: CSVs trimestrais publicados (campos sanitizados) | — |
| _ingested_at | TIMESTAMPTZ | Instante automatico da ingestao no warehouse. | DEFAULT now() | — |
| _source | TEXT | Marcador do sistema de origem. | Default / script da fonte | SP156 |
| _run_id | TEXT | Identificador da execucao que inseriu/atualizou o registro. | Script de ingestao | — |

## bronze.raw_ibge_area (TABELA)

**Origem principal:** IBGE: Censo 2022, arquivo basico por distrito.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| id **[PK]** | BIGINT | Identificador tecnico interno (identity). | IBGE: Censo 2022, arquivo basico por distrito | — |
| arquivo_origem | TEXT | Nome do arquivo de agregados do Censo usado na carga. | IBGE: Censo 2022, arquivo basico por distrito | — |
| codigo_distrito_raw | TEXT | Codigo oficial do distrito IBGE. | IBGE: Censo 2022, arquivo basico por distrito | 355030839 |
| codigo_municipio_raw | TEXT | Codigo IBGE do municipio. | IBGE: Censo 2022, arquivo basico por distrito | 3550308 |
| nome_distrito_raw | TEXT | Nome do distrito no arquivo original. | IBGE: Censo 2022, arquivo basico por distrito | Jaçanã |
| populacao_raw | TEXT | Populacao total (v0001) antes da tipagem numerica. | IBGE: Censo 2022, arquivo basico por distrito | — |
| domicilios_raw | TEXT | Total de domicilios (v0002) antes da tipagem numerica. | IBGE: Censo 2022, arquivo basico por distrito | — |
| payload_publico | JSONB | Outros agregados publicos da linha do Censo em JSON. | IBGE: Censo 2022, arquivo basico por distrito | — |
| _ingested_at | TIMESTAMPTZ | Instante automatico da ingestao no warehouse. | DEFAULT now() | — |
| _source | TEXT | Marcador do sistema de origem. | Default / script da fonte | IBGE_CENSO_2022 |
| _run_id | TEXT | Identificador da execucao que inseriu/atualizou o registro. | Script de ingestao | — |

## bronze.raw_clima (TABELA)

**Origem principal:** Open-Meteo: previsoes horarias por ponto.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| id **[PK]** | BIGINT | Identificador tecnico interno (identity). | Open-Meteo: previsoes horarias por ponto | — |
| area_id | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | Open-Meteo: previsoes horarias por ponto | 355030839 |
| hora_raw | TEXT | Instante da hora da previsao como texto. | Open-Meteo: previsoes horarias por ponto | — |
| precip_mm_raw | TEXT | Precipitacao horaria em milimetros como texto. | Open-Meteo: previsoes horarias por ponto | — |
| latitude_grade | NUMERIC(10,6) | Latitude do ponto de grade efetivamente retornado pela API. | Open-Meteo: previsoes horarias por ponto | — |
| longitude_grade | NUMERIC(10,6) | Longitude do ponto de grade retornado pela API. | Open-Meteo: previsoes horarias por ponto | — |
| consultado_em | TIMESTAMPTZ | Timestamp do snapshot da consulta climatica. | Open-Meteo: previsoes horarias por ponto | 2026-11-13T01:00:00Z |
| _ingested_at | TIMESTAMPTZ | Instante automatico da ingestao no warehouse. | DEFAULT now() | — |
| _source | TEXT | Marcador do sistema de origem. | Default / script da fonte | OPEN_METEO |
| _run_id | TEXT | Identificador da execucao que inseriu/atualizou o registro. | Script de ingestao | — |

## bronze.raw_relatos_app (TABELA)

**Origem principal:** Gerador deterministico: relatos_app_sim.py.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| relato_id **[PK]** | TEXT | Identificador estavel do relato simulado. | Gerador deterministico: relatos_app_sim.py | — |
| tipo_raw | TEXT | Tipo da ocorrencia gerada antes da normalizacao. | Gerador deterministico: relatos_app_sim.py | buraco |
| latitude_raw | TEXT | Latitude da fonte em formato textual. | Gerador deterministico: relatos_app_sim.py | — |
| longitude_raw | TEXT | Longitude da fonte em formato textual. | Gerador deterministico: relatos_app_sim.py | — |
| area_id_raw | TEXT | Distrito indicado pelo gerador de relatos. | Gerador deterministico: relatos_app_sim.py | — |
| aberta_em_raw | TEXT | Instante da abertura gerada, mantido como texto. | Gerador deterministico: relatos_app_sim.py | — |
| status_raw | TEXT | Situacao de origem antes da normalizacao. | Gerador deterministico: relatos_app_sim.py | Aberto |
| _ingested_at | TIMESTAMPTZ | Instante automatico da ingestao no warehouse. | DEFAULT now() | — |
| _source | TEXT | Marcador do sistema de origem. | Default / script da fonte | APP_SIMULADO |
| _run_id | TEXT | Identificador da execucao que inseriu/atualizou o registro. | Script de ingestao | — |

## silver.ocorrencia_limpa (TABELA)

**Origem principal:** Bronze SP156 + App; SQL 04 + H3 em Python.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| ocorrencia_id **[PK]** | TEXT | Identificador estavel da ocorrencia normalizada, distinto por canal. | Bronze SP156 + App; SQL 04 + H3 em Python | — |
| canal | TEXT | Canal de origem do relato padronizado. | Bronze SP156 + App; SQL 04 + H3 em Python | — |
| municipio_cod | CHAR(7) | Codigo IBGE do municipio, inicialmente Sao Paulo. | Bronze SP156 + App; SQL 04 + H3 em Python | 3550308 |
| tipo_padrao | TEXT | Categoria normalizada, conforme seed de tipos. | Bronze SP156 + App; SQL 04 + H3 em Python | buraco |
| lat | NUMERIC(10,7) | Latitude numerica da ocorrencia. | Bronze SP156 + App; SQL 04 + H3 em Python | — |
| lon | NUMERIC(10,7) | Longitude numerica da ocorrencia. | Bronze SP156 + App; SQL 04 + H3 em Python | — |
| h3 | TEXT | Celula espacial H3 da ocorrencia (resolucao 9). | h3.latlng_to_cell em Python | — |
| area_id | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | Bronze SP156 + App; SQL 04 + H3 em Python | 355030839 |
| aberta_em | TIMESTAMPTZ | Instante padronizado de abertura. | Bronze SP156 + App; SQL 04 + H3 em Python | — |
| status | TEXT | Situacao padronizada da ocorrencia ou cluster. | Bronze SP156 + App; SQL 04 + H3 em Python | — |
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |

## silver.cluster_ocorrencia (TABELA)

**Origem principal:** silver.ocorrencia_limpa; SQL 05.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| ocorrencia_id **[PK]** | TEXT | Identificador estavel da ocorrencia normalizada, distinto por canal. | silver.ocorrencia_limpa; SQL 05 | — |
| cluster_id | TEXT | Identificador deterministico do grupo de relatos deduplicados. | silver.ocorrencia_limpa; SQL 05 | cl_480f239837f018ec6704111c |
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |

## silver.clima_previsao (TABELA)

**Origem principal:** bronze.raw_clima; SQL 07.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| area_id | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | bronze.raw_clima; SQL 07 | 355030839 |
| hora | TIMESTAMPTZ | Instante da previsao horaria padronizada. | bronze.raw_clima; SQL 07 | — |
| precip_mm | NUMERIC(10,3) | Precipitacao horaria nao negativa, em mm. | bronze.raw_clima; SQL 07 | — |
| consultado_em | TIMESTAMPTZ | Timestamp do snapshot da consulta climatica. | bronze.raw_clima; SQL 07 | 2026-11-13T01:00:00Z |
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |

## silver.descarte_ocorrencia (TABELA)

**Origem principal:** bronze SP156 + App; validacoes de SQL 04.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |
| canal | TEXT | Canal de origem do relato padronizado. | bronze SP156 + App; validacoes de SQL 04 | — |
| motivo | TEXT | Motivo de descarte identificado na Silver. | bronze SP156 + App; validacoes de SQL 04 | — |
| quantidade | INTEGER | Quantidade de linhas descartadas no motivo e na rodada. | bronze SP156 + App; validacoes de SQL 04 | — |

## silver.v_chuva_48h_area (VIEW)

**Origem principal:** silver.clima_previsao; ultimo snapshot.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| area_id | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | silver.clima_previsao; ultimo snapshot | 355030839 |
| consultado_em | TIMESTAMPTZ | Timestamp do snapshot da consulta climatica. | silver.clima_previsao; ultimo snapshot | 2026-11-13T01:00:00Z |
| chuva_48h_mm | NUMERIC(12,3) | Soma prevista da precipitacao em 48h por distrito. | silver.clima_previsao; ultimo snapshot | 6.500 |
| horas | INTEGER | Quantidade de horarios agregados na janela climatica. | silver.clima_previsao; ultimo snapshot | 48 |

## gold.dim_area (TABELA)

**Origem principal:** bronze.raw_ibge_area + seed pontos distritais.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| area_id **[PK]** | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | bronze.raw_ibge_area + seed pontos distritais | 355030839 |
| municipio_cod | CHAR(7) | Codigo IBGE do municipio, inicialmente Sao Paulo. | bronze.raw_ibge_area + seed pontos distritais | 3550308 |
| nome | TEXT | Nome oficial do distrito do IBGE. | bronze.raw_ibge_area + seed pontos distritais | Jaçanã |
| populacao | BIGINT | Populacao total do distrito no Censo 2022. | bronze.raw_ibge_area + seed pontos distritais | — |
| domicilios | BIGINT | Total de domicilios do distrito no Censo 2022. | bronze.raw_ibge_area + seed pontos distritais | — |
| necessidade | NUMERIC(8,4) | Peso de necessidade do distrito; inicialmente 1. | bronze.raw_ibge_area + seed pontos distritais | — |
| latitude_centroide | NUMERIC(10,7) | Latitude do ponto territorial representativo da area. | Malha distrital IBGE | — |
| longitude_centroide | NUMERIC(10,7) | Longitude do ponto territorial representativo da area. | Malha distrital IBGE | — |

## gold.dim_tipo (TABELA)

**Origem principal:** include/seeds/dim_tipo.csv.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| tipo_padrao **[PK]** | TEXT | Categoria normalizada, conforme seed de tipos. | include/seeds/dim_tipo.csv | buraco |
| risco_vida | NUMERIC(5,4) | Hipotese de risco a vida do tipo entre 0 e 1. | include/seeds/dim_tipo.csv | 0.60 (seed) |
| sens_clima | NUMERIC(5,4) | Hipotese de sensibilidade a chuva do tipo entre 0 e 1. | include/seeds/dim_tipo.csv | 0.55 (seed) |

## gold.dim_tempo (TABELA)

**Origem principal:** Datas da gold.fact_ocorrencia.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| data **[PK]** | DATE | Data civil para dimensao de tempo. | Datas da gold.fact_ocorrencia | — |
| ano | SMALLINT | Ano da data. | Datas da gold.fact_ocorrencia | 2026 |
| mes | SMALLINT | Mes da data (1 a 12). | Datas da gold.fact_ocorrencia | — |
| dia | SMALLINT | Dia do mes. | Datas da gold.fact_ocorrencia | — |
| trimestre | SMALLINT | Trimestre civil (1 a 4). | Datas da gold.fact_ocorrencia | — |

## gold.fact_ocorrencia (TABELA)

**Origem principal:** silver.cluster_ocorrencia + silver.ocorrencia_limpa.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| cluster_id **[PK]** | TEXT | Identificador deterministico do grupo de relatos deduplicados. | silver.cluster_ocorrencia + silver.ocorrencia_limpa | cl_480f239837f018ec6704111c |
| municipio_cod | CHAR(7) | Codigo IBGE do municipio, inicialmente Sao Paulo. | silver.cluster_ocorrencia + silver.ocorrencia_limpa | 3550308 |
| area_id | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | silver.cluster_ocorrencia + silver.ocorrencia_limpa | 355030839 |
| tipo_padrao | TEXT | Categoria normalizada, conforme seed de tipos. | silver.cluster_ocorrencia + silver.ocorrencia_limpa | buraco |
| n_relatos | INTEGER | Quantidade de relatos no cluster. | silver.cluster_ocorrencia + silver.ocorrencia_limpa | 22 |
| aberta_em | TIMESTAMPTZ | Instante padronizado de abertura. | silver.cluster_ocorrencia + silver.ocorrencia_limpa | — |
| status | TEXT | Situacao padronizada da ocorrencia ou cluster. | silver.cluster_ocorrencia + silver.ocorrencia_limpa | — |
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |

## gold.fact_equidade_area (TABELA)

**Origem principal:** gold.dim_area + relatos Silver elegiveis.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |
| area_id | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | gold.dim_area + relatos Silver elegiveis | 355030839 |
| janela_inicio | TIMESTAMPTZ | Inicio inclusivo da janela de equidade. | gold.dim_area + relatos Silver elegiveis | — |
| janela_fim | TIMESTAMPTZ | Fim exclusivo da janela de equidade. | gold.dim_area + relatos Silver elegiveis | — |
| observado | INTEGER | Contagem de relatos elegiveis por area antes de deduplicar. | gold.dim_area + relatos Silver elegiveis | 287 (Jaçanã, rodada anterior) |
| esperado | NUMERIC(16,5) | Relatos esperados proporcionalmente a populacao vezes necessidade. | gold.dim_area + relatos Silver elegiveis | 126.22748 (Jaçanã, rodada anterior) |
| razao | NUMERIC(16,6) | Relacao entre observado e esperado. | gold.dim_area + relatos Silver elegiveis | 2.273673 (Jaçanã, rodada anterior) |
| fator_equidade | NUMERIC(9,6) | Multiplicador de exposicao entre 1 e M (inicialmente 2). | gold.dim_area + relatos Silver elegiveis | 1.138601 |

## gold.fact_score (TABELA)

**Origem principal:** fato corrente + equidade + clima + weights.yml.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |
| cluster_id | TEXT | Identificador deterministico do grupo de relatos deduplicados. | fato corrente + equidade + clima + weights.yml | cl_480f239837f018ec6704111c |
| fator_equidade | NUMERIC(9,6) | Multiplicador de exposicao entre 1 e M (inicialmente 2). | fato corrente + equidade + clima + weights.yml | 1.138601 |
| chuva_48h_mm | NUMERIC(12,3) | Soma prevista da precipitacao em 48h por distrito. | fato corrente + equidade + clima + weights.yml | 6.500 |
| dias_aberta | INTEGER | Dias decorridos entre abertura do cluster e snapshot de avaliacao. | fato corrente + equidade + clima + weights.yml | — |
| comp_risco | NUMERIC(12,5) | Componente risco ponderado da pontuacao. | seed dim_tipo + weights.yml | 24.00 |
| comp_exposicao | NUMERIC(12,5) | Componente exposicao ponderado e ajustado pela equidade. | n_relatos + fator_equidade + pesos | — |
| comp_tempo | NUMERIC(12,5) | Componente tempo ponderado, com saturacao. | aberta_em + snapshot + pesos | — |
| comp_clima | NUMERIC(12,5) | Componente chuva ponderado por sensibilidade. | chuva_48h_mm + seed + pesos | — |
| score_sem_equidade | NUMERIC(12,5) | Soma dos quatro fatores com exposicao sem ajuste. | fato corrente + equidade + clima + weights.yml | — |
| score_com_equidade | NUMERIC(12,5) | Soma dos quatro fatores com exposicao corrigida. | Soma dos quatro componentes | — |
| score_0_100 | NUMERIC(9,5) | Score normalizado pelo maximo teorico configurado. | score_com_equidade / maximo teorico | 50.33077 |
| rank_sem | INTEGER | Posicao sem ajuste de equidade para comparacao. | fato corrente + equidade + clima + weights.yml | 1 |
| rank_com | INTEGER | Posicao com ajuste de equidade. | fato corrente + equidade + clima + weights.yml | 1 |
| explicacao | TEXT | Descricao deterministica das parcelas da nota. | Template SQL 08 | — |
| avaliado_em | TIMESTAMPTZ | Instante de referencia da avaliacao do score (snapshot). | fato corrente + equidade + clima + weights.yml | — |
| snapshot_clima | TIMESTAMPTZ | Snapshot das previsoes empregado no score. | fato corrente + equidade + clima + weights.yml | — |

## gold.mart_fila_priorizacao (VIEW)

**Origem principal:** gold.fact_score + fato corrente + dim_area.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| rank | INTEGER | Posicao publicada na fila ordenada por score. | gold.fact_score + fato corrente + dim_area | 1 |
| cluster_id | TEXT | Identificador deterministico do grupo de relatos deduplicados. | gold.fact_score + fato corrente + dim_area | cl_480f239837f018ec6704111c |
| tipo | TEXT | Nome da categoria normalizada publicada. | gold.fact_score + fato corrente + dim_area | — |
| area_id | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | gold.fact_score + fato corrente + dim_area | 355030839 |
| area_nome | TEXT | Nome do distrito para leitura humana. | gold.fact_score + fato corrente + dim_area | Santo Amaro |
| n_relatos | INTEGER | Quantidade de relatos no cluster. | gold.fact_score + fato corrente + dim_area | 22 |
| dias_aberta | INTEGER | Dias decorridos entre abertura do cluster e snapshot de avaliacao. | gold.fact_score + fato corrente + dim_area | — |
| chuva_48h_mm | NUMERIC(12,3) | Soma prevista da precipitacao em 48h por distrito. | gold.fact_score + fato corrente + dim_area | 6.500 |
| fator_equidade | NUMERIC(9,6) | Multiplicador de exposicao entre 1 e M (inicialmente 2). | gold.fact_score + fato corrente + dim_area | 1.138601 |
| comp_risco | NUMERIC(12,5) | Componente risco ponderado da pontuacao. | seed dim_tipo + weights.yml | 24.00 |
| comp_exposicao | NUMERIC(12,5) | Componente exposicao ponderado e ajustado pela equidade. | n_relatos + fator_equidade + pesos | — |
| comp_tempo | NUMERIC(12,5) | Componente tempo ponderado, com saturacao. | aberta_em + snapshot + pesos | — |
| comp_clima | NUMERIC(12,5) | Componente chuva ponderado por sensibilidade. | chuva_48h_mm + seed + pesos | — |
| score | NUMERIC(12,5) | Pontuacao publicada, ja com equidade. | gold.fact_score + fato corrente + dim_area | — |
| score_0_100 | NUMERIC(9,5) | Score normalizado pelo maximo teorico configurado. | score_com_equidade / maximo teorico | 50.33077 |
| explicacao | TEXT | Descricao deterministica das parcelas da nota. | Template SQL 08 | — |
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |

## gold.mart_comparacao_equidade (VIEW)

**Origem principal:** score + fatos + equidade da mesma rodada.

| Coluna | Tipo SQL | Descricao | Origem | Exemplo |
|---|---|---|---|---|
| run_id | TEXT | Rodada associada ao registro ou score. | Script da transformacao / DAG | fj2026_433d14a4d3d60536f6aa88aa |
| area_id | TEXT | Chave territorial do distrito (CD_DIST do IBGE). | score + fatos + equidade da mesma rodada | 355030839 |
| area_nome | TEXT | Nome do distrito para leitura humana. | score + fatos + equidade da mesma rodada | Santo Amaro |
| cluster_id | TEXT | Identificador deterministico do grupo de relatos deduplicados. | score + fatos + equidade da mesma rodada | cl_480f239837f018ec6704111c |
| rank_sem | INTEGER | Posicao sem ajuste de equidade para comparacao. | score + fatos + equidade da mesma rodada | 1 |
| rank_com | INTEGER | Posicao com ajuste de equidade. | score + fatos + equidade da mesma rodada | 1 |
| score_sem_equidade | NUMERIC(12,5) | Soma dos quatro fatores com exposicao sem ajuste. | score + fatos + equidade da mesma rodada | — |
| score_com_equidade | NUMERIC(12,5) | Soma dos quatro fatores com exposicao corrigida. | Soma dos quatro componentes | — |
| observado | INTEGER | Contagem de relatos elegiveis por area antes de deduplicar. | score + fatos + equidade da mesma rodada | — |
| esperado | NUMERIC(16,5) | Relatos esperados proporcionalmente a populacao vezes necessidade. | score + fatos + equidade da mesma rodada | 126.22748 (Jaçanã, rodada anterior) |
| razao | NUMERIC(16,6) | Relacao entre observado e esperado. | score + fatos + equidade da mesma rodada | 2.273673 (Jaçanã, rodada anterior) |
| fator_equidade | NUMERIC(9,6) | Multiplicador de exposicao entre 1 e M (inicialmente 2). | score + fatos + equidade da mesma rodada | 1.138601 |
| faixa_propensao_reporte | TEXT | Classe da razao observado/esperado (baixa, media, alta ou indefinida). | CASE sobre razao em view | alta |

## Regras de publicacao e integridade

- `gold.fact_score` tem chave composta `(run_id, cluster_id)` e conserva rodadas de score; `gold.fact_ocorrencia` tem PK `cluster_id` e representa a versao corrente.
- As views Gold ligam `fact_score` ao fato corrente por **cluster_id e run_id**, evitando rankings de rodadas divergentes. A view de comparacao tambem exige o mesmo `run_id` em `fact_equidade_area`.
- `silver.v_chuva_48h_area` inclui somente o snapshot climatico mais recente, com 48 horas por distrito.
- Em `gold.fact_ocorrencia`, o distrito de clusters que atravessam areas e o do primeiro relato cronologico (empate por ID). A distribuicao de equidade conta os relatos elegiveis antes da deduplicacao.
- As tabelas temporarias `stage_*` criadas dentro das transacoes nao estao incluidas porque nao fazem parte do contrato persistente.
