-- Passo 10: gerar um registro por cluster e score explicável por rodada.
-- Regras: mesmo tipo, mesmo H3, primeiro relato cronológico define área.
-- A contagem do score é de relatos POR CLUSTER, não por área.
-- Os 115 clusters transdistritais continuam íntegros; o distrito para
-- a Gold é o do primeiro relato (desempate por ocorrencia_id).
-- limitação: gold.fact_ocorrencia é estado atual por cluster, não histórico
-- por rodada. Evolução: fato com PK composta (run_id, cluster_id).

CREATE TEMP TABLE stage_fact_ocorrencia ON COMMIT DROP AS
WITH primeiro AS (
    SELECT DISTINCT ON (c.cluster_id)
        c.cluster_id, o.municipio_cod, o.area_id, o.tipo_padrao,
        o.aberta_em, o.status
    FROM silver.cluster_ocorrencia c
    JOIN silver.ocorrencia_limpa o ON o.ocorrencia_id = c.ocorrencia_id
    ORDER BY c.cluster_id, o.aberta_em, o.ocorrencia_id
), totais AS (
    SELECT c.cluster_id, COUNT(*)::integer AS n_relatos
    FROM silver.cluster_ocorrencia c
    GROUP BY c.cluster_id
)
SELECT p.cluster_id, p.municipio_cod, p.area_id, p.tipo_padrao,
       t.n_relatos, p.aberta_em, p.status
FROM primeiro p
JOIN totais t ON t.cluster_id = p.cluster_id;

-- A previsão é do snapshot mais recente, presente na view da Silver.
-- O snapshot é usado como relógio de avaliação: reexecutar sobre os mesmos
-- insumos produz os mesmos dias_aberta e os mesmos rankings.
CREATE TEMP TABLE stage_score ON COMMIT DROP AS
WITH base AS (
    SELECT
        o.cluster_id, o.area_id, o.tipo_padrao, o.n_relatos,
        o.aberta_em, t.risco_vida, t.sens_clima,
        e.fator_equidade, v.chuva_48h_mm,
        GREATEST(
            0::numeric,
            FLOOR(EXTRACT(EPOCH FROM (
                current_setting('fila_justa.as_of')::timestamptz - o.aberta_em
            ))::numeric / 86400)
        )::integer AS dias_aberta
    FROM stage_fact_ocorrencia o
    JOIN stage_seed_tipo t ON t.tipo_padrao = o.tipo_padrao
    JOIN gold.fact_equidade_area e
      ON e.area_id = o.area_id
     AND e.run_id = current_setting('fila_justa.run_id')
    JOIN silver.v_chuva_48h_area v ON v.area_id = o.area_id
), componentes AS (
    SELECT
        b.*,
        ROUND(current_setting('fila_justa.w_risco')::numeric * b.risco_vida, 5) AS comp_risco,
        ROUND(current_setting('fila_justa.w_expo')::numeric *
              LEAST(b.n_relatos::numeric / current_setting('fila_justa.sat_relatos')::numeric, 1::numeric), 5)
              AS comp_expo_sem,
        ROUND(current_setting('fila_justa.w_expo')::numeric *
              LEAST(b.n_relatos::numeric / current_setting('fila_justa.sat_relatos')::numeric, 1::numeric)
              * b.fator_equidade, 5) AS comp_exposicao,
        ROUND(current_setting('fila_justa.w_tempo')::numeric *
              LEAST(b.dias_aberta::numeric / current_setting('fila_justa.sat_dias')::numeric, 1::numeric), 5)
              AS comp_tempo,
        ROUND(current_setting('fila_justa.w_clima')::numeric * b.sens_clima *
              LEAST(b.chuva_48h_mm / current_setting('fila_justa.sat_chuva')::numeric, 1::numeric), 5)
              AS comp_clima
    FROM base b
), pontuacoes AS (
    SELECT c.*,
       (c.comp_risco + c.comp_expo_sem + c.comp_tempo + c.comp_clima)::numeric(12,5)
           AS score_sem_equidade,
       (c.comp_risco + c.comp_exposicao + c.comp_tempo + c.comp_clima)::numeric(12,5)
           AS score_com_equidade
    FROM componentes c
), ordenadas AS (
    SELECT p.*,
           ROW_NUMBER() OVER (ORDER BY p.score_sem_equidade DESC, p.cluster_id)::integer AS rank_sem,
           ROW_NUMBER() OVER (ORDER BY p.score_com_equidade DESC, p.cluster_id)::integer AS rank_com
    FROM pontuacoes p
)
SELECT
    o.cluster_id, o.fator_equidade,
    o.chuva_48h_mm::numeric(12,3) AS chuva_48h_mm,
    o.dias_aberta, o.comp_risco, o.comp_exposicao,
    o.comp_tempo, o.comp_clima, o.score_sem_equidade,
    o.score_com_equidade,
    ROUND(100::numeric * o.score_com_equidade / (
      current_setting('fila_justa.w_risco')::numeric +
      current_setting('fila_justa.w_expo')::numeric * current_setting('fila_justa.eq_max')::numeric +
      current_setting('fila_justa.w_tempo')::numeric +
      current_setting('fila_justa.w_clima')::numeric
    ), 5)::numeric(9,5) AS score_0_100,
    o.rank_sem, o.rank_com,
    CONCAT('Risco: ', ROUND(o.comp_risco, 2)::text,
      '; exposição: ', ROUND(o.comp_exposicao, 2)::text,
      ' (', o.n_relatos, ' relatos; fator de equidade ',
      ROUND(o.fator_equidade, 3)::text,
      '); tempo: ', ROUND(o.comp_tempo, 2)::text,
      ' (', o.dias_aberta, ' dias); clima: ',
      ROUND(o.comp_clima, 2)::text,
      ' (', ROUND(o.chuva_48h_mm, 1)::text, ' mm em 48h).') AS explicacao
FROM ordenadas o;
