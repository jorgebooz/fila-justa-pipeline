# Modelagem efetiva - Fila Justa

O DDL do warehouse PostgreSQL 16 está em `include/sql/00_schemas.sql` a `03_gold.sql`. O pipeline aplica transformações SQL versionadas entre 04 e 09; `10_corrigir_publicacao_marts.sql` preserva a coerência das views por rodada, e `include/transform/score.py` aplica a migração de `avaliado_em` e `snapshot_clima` para `gold.fact_score`.

## Diagramas

- Arquitetura: `docs/arquitetura.png`
- Modelo lógico Gold: `docs/modelo_er_gold.png`
- Dicionário de todas as tabelas e views: `docs/dicionario_dados.md`

## Tabelas e visões persistentes

- Bronze: 4 tabelas (`raw_sp156`, `raw_ibge_area`, `raw_clima`, `raw_relatos_app`).
- Silver: 4 tabelas (`ocorrencia_limpa`, `cluster_ocorrencia`, `clima_previsao`, `descarte_ocorrencia`) + `v_chuva_48h_area`.
- Gold: 3 dimensões (`dim_area`, `dim_tipo`, `dim_tempo`), 3 fatos (`fact_ocorrencia`, `fact_equidade_area`, `fact_score`) e 2 views (`mart_fila_priorizacao`, `mart_comparacao_equidade`).

## Granularidade e relacionamentos

- `silver.ocorrencia_limpa`: uma linha por relato elegível, após conversão e H3.
- `silver.cluster_ocorrencia`: cada ocorrência é associada a um cluster; cluster depende de mesmo tipo, célula H3 resolução 9 e janela de 7 dias ancorada no primeiro relato.
- `gold.fact_ocorrencia`: 1 linha por cluster corrente (`cluster_id` PK); distrito pelo relato cronologicamente mais antigo, desempate por `ocorrencia_id`.
- `gold.fact_equidade_area`: 1 linha por distrito/rodada (PK `run_id`, `area_id`). Observado é contagem de relatos Silver antes dos clusters.
- `gold.fact_score`: 1 linha por cluster/rodada (PK `run_id`, `cluster_id`). Guarda componentes, ranks, score e explicação.
- As views Gold filtram por `fact_ocorrencia.run_id = fact_score.run_id`; comparação exige também `fact_equidade_area.run_id` correspondente.
- `gold.dim_tempo` é povoada com datas de abertura, mas ainda não é vinculada por chave estrangeira ao fato.

## Integridade e limites

O DDL aplica tipos, chaves, checks e FKs para o modelo persistente; a DAG aplica 37 verificações adicionais de consistência. Idempotência usa chave de origem nas ingestões e `ON CONFLICT` com atualização condicional. A bronze armazena **campos selecionados**, não cópia integral do CSV, para evitar exposição de dados pessoais.

`limitação:` a Gold de fatos de ocorrência é uma fotografia corrente (não reconstitui todas as rodadas historicamente se clusters forem removidos); a Silver valida coordenadas por caixa delimitadora, não ponto no polígono; e a deduplicação por célula H3 não une células vizinhas. Para o clima, o snapshot mais recente representa previsão futura, não histórico de precipitação no dia da ocorrência.
