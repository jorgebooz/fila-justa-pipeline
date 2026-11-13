# Passo 10 — Score explicável e fila priorizada

## Arquivos do patch

- `include/transform/score.py`
- `include/sql/08_score_fila.sql`
- `tests/test_score.py`

Nenhum conector da Bronze/Silver é substituído. O script usa `AIRFLOW_CONN_WAREHOUSE` do scheduler. Execução **manual** por enquanto; ainda não integra uma DAG.

## Premissas verificadas nas etapas anteriores

- `silver.ocorrencia_limpa`: 16.553 relatos, 96 distritos.
- `silver.cluster_ocorrencia`: 16.553 vínculos e 11.163 clusters.
- `gold.dim_area`: 96 distritos com população do IBGE.
- `gold.fact_equidade_area`: 96 linhas para **`run_id=equidade_2026`**.
- `silver.v_chuva_48h_area`: 96 distritos, 48 horas por distrito, mesmo snapshot.

O `run_id` da pontuação precisa ser **o mesmo que o da equidade**: a view `gold.mart_comparacao_equidade` une os fatos por rodada e área. Mais adiante, na DAG, passe o mesmo run_id a ambas as tarefas.

## 1. Testes locais (Git Bash)

```bash
python -m unittest discover -s tests -p 'test_score.py' -v
```

## 2. Dry-run: não altera Gold

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/score.py --run-id equidade_2026 --dry-run
```

Esperado, enquanto as cargas não mudarem: `clusters=11163 | relatos=16553` e mensagem `OK: dry-run, Gold não alterada`.

## 3. Publicar os fatos e marts

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/score.py --run-id equidade_2026
```

O script cria `gold.dim_tipo` a partir do seed, popula `gold.dim_tempo` com as datas de abertura, produz `gold.fact_ocorrencia`, `gold.fact_score`, e atualiza as duas views Gold. Acrescenta duas colunas **sem destruir dados** a `gold.fact_score` (`avaliado_em`, `snapshot_clima`) para selecionar a rodada publicada. Tudo transacional: se falhar, o PostgreSQL desfaz a rodada.

## 4. Validação SQL no Git Bash

Execute as consultas abaixo **separadamente**. Usamos `<<'SQL'` para não precisar escapar aspas no Git Bash.

**4.1 Contagem, soma de componentes e limites de score**

```bash
docker compose exec -T warehouse sh -c \
'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT COUNT(*) AS clusters,
       COUNT(DISTINCT cluster_id) AS clusters_unicos,
       MIN(score_0_100) AS score_min,
       MAX(score_0_100) AS score_max,
       COUNT(*) FILTER (WHERE score_0_100 NOT BETWEEN 0 AND 100) AS score_invalido,
       COUNT(*) FILTER (
           WHERE score_com_equidade <> comp_risco + comp_exposicao + comp_tempo + comp_clima
       ) AS soma_invalida
FROM gold.fact_score
WHERE run_id = 'equidade_2026';
SQL
```

Esperado: 11.163 clusters únicos; `score_invalido=0`, `soma_invalida=0`. Valores mínimo e máximo variam com os dados.

**4.2 Top 10 da fila explicável**

```bash
docker compose exec -T warehouse sh -c \
'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT rank, area_nome, tipo, n_relatos, fator_equidade,
       chuva_48h_mm, comp_risco, comp_exposicao, comp_tempo,
       comp_clima, score, score_0_100, explicacao
FROM gold.mart_fila_priorizacao
ORDER BY rank LIMIT 10;
SQL
```

**4.3 Antes e depois da equidade no topo 100**

```bash
docker compose exec -T warehouse sh -c \
'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT faixa_propensao_reporte,
       COUNT(*) FILTER (WHERE rank_sem <= 100) AS top100_sem,
       COUNT(*) FILTER (WHERE rank_com <= 100) AS top100_com
FROM gold.mart_comparacao_equidade
GROUP BY faixa_propensao_reporte ORDER BY faixa_propensao_reporte;
SQL
```

## 5. Reexecução — idempotência

Repita o passo 3 **sem atualizar a Silver nem o snapshot climático**. Esperado: `tipos=0`, `tempo=0`, `fact_ocorrencia=0`, `fact_score=0`, `scores_removidos=0`. Se o snapshot do clima mudar, uma alteração legítima de scores é esperada.

## Regras de negócio

- Risco = `w_risco × risco_vida(tipo)`.
- Exposição = `w_expo × min(n_relatos/20, 1) × fator_equidade`.
- Tempo = `w_tempo × min(dias_aberta/30, 1)`.
- Clima = `w_clima × sens_clima(tipo) × min(chuva_48h_mm/50, 1)`.
- Score sem equidade fixa fator = 1. O risco **não** recebe fator de equidade.
- Score normalizado usa teto teórico: `40 + 30×2 + 10 + 20 = 130`.
- Ranks com e sem equidade usam desempate por `cluster_id` e começam em 1.
- `dias_aberta` é contado na data/hora do snapshot da previsão (UTC), com piso de zero. Assim a mesma execução é reproduzível.
- O distrito e o tipo de um cluster vêm do relato mais antigo (desempate por `ocorrencia_id`). Isso resolve de forma determinística os 115 clusters que cruzam distritos. O agrupamento completo permanece na Silver.
- A explicação é montada por template SQL, sem IA generativa.

## Limitações conhecidas

1. O `score` é um modelo **hipotético**, não uma prioridade validada por especialistas; documentar sensibilidade a `alpha` e `M` no PDF final.
2. A previsão de chuva é **atual** e não uma reconstrução histórica dos dias em que os relatos foram abertos. Recomenda-se atualizar o clima antes de priorizar uma fila operacional.
3. O `gold.fact_ocorrencia` armazena apenas o estado atual por `cluster_id`; se os clusters mudarem, não se mantém uma cópia independente do fato para cada rodada antiga. Evolução: versionamento completo por `run_id`.
4. Se a fonte, a equidade e a Silver mudarem, execute novamente as etapas anteriores com a **mesma rodada** antes do score. O patch não reexecuta automaticamente dependências (isso será papel do Airflow na etapa 12).
5. Valores da view publicados são só da rodada mais recente; dados antigos permanecem em `gold.fact_score` para auditoria. Empates na data de avaliação são desempatados pelo texto do `run_id`.
