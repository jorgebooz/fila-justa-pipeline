-- Passo 8: IBGE -> dim_area e fator de equidade por distrito e janela.
-- Janela em [inicio, fim), informada pelo Python (fuso America/Sao_Paulo).
-- IMPORTANTE: observado = relatos elegiveis da Silver ANTES da clusterizacao.
-- Isso preserva o numero bruto de reportes elegiveis (nao clusters), mas
-- exclui dados descartados na padronizacao. Ver limitacoes no guia.
-- Nao utiliza endereco pessoal nem texto livre.

-- A tabela temporaria reflete a revisao fixa do IBGE e preserva 'necessidade'
-- previamente definida em dim_area; seu valor inicial e 1 (hipotese).
CREATE TEMP TABLE stage_dim_area ON COMMIT DROP AS
SELECT
    b.codigo_distrito_raw AS area_id,
    b.codigo_municipio_raw::char(7) AS municipio_cod,
    b.nome_distrito_raw AS nome,
    b.populacao_raw::bigint AS populacao,
    b.domicilios_raw::bigint AS domicilios,
    COALESCE(a.necessidade, 1.0::numeric)::numeric(8,4) AS necessidade
FROM bronze.raw_ibge_area b
LEFT JOIN gold.dim_area a ON a.area_id = b.codigo_distrito_raw
WHERE b.arquivo_origem = 'Agregados_por_distritos_basico_BR_20260520.zip'
  AND b.codigo_municipio_raw = '3550308'
  AND b.codigo_distrito_raw ~ '^3550308[0-9]{2}$'
  AND b.populacao_raw ~ '^[0-9]+$'
  AND b.domicilios_raw ~ '^[0-9]+$';

-- O observado e contado por area, sem deduplicacao; cada linha e um relato.
CREATE TEMP TABLE stage_observado ON COMMIT DROP AS
SELECT area_id, COUNT(*)::integer AS observado
FROM silver.ocorrencia_limpa
WHERE municipio_cod = '3550308'
  AND aberta_em >= current_setting('fila_justa.janela_inicio')::timestamptz
  AND aberta_em <  current_setting('fila_justa.janela_fim')::timestamptz
GROUP BY area_id;

-- 'esperado' distribui o TOTAL de reportes elegiveis entre 96 distritos
-- na proporcao populacao * necessidade. O limite M afeta somente exposicao
-- na etapa do score, nunca o componente risco de vida.
CREATE TEMP TABLE stage_equidade ON COMMIT DROP AS
WITH pesos AS (
    SELECT a.area_id, a.populacao::numeric * a.necessidade AS peso,
           COALESCE(o.observado, 0)::integer AS observado
    FROM stage_dim_area a
    LEFT JOIN stage_observado o ON o.area_id = a.area_id
), totais AS (
    SELECT SUM(p.observado)::numeric AS total_relatos,
           SUM(p.peso) AS soma_pesos FROM pesos p
), base AS (
    SELECT p.area_id, p.observado,
           ROUND(t.total_relatos * p.peso / NULLIF(t.soma_pesos, 0), 5) AS esperado
    FROM pesos p CROSS JOIN totais t
)
SELECT b.area_id, b.observado, b.esperado,
       ROUND(b.observado::numeric / NULLIF(b.esperado, 0), 6) AS razao,
       ROUND(LEAST(
           current_setting('fila_justa.equidade_max')::numeric,
           GREATEST(1::numeric,
               POWER(
                   (b.esperado + current_setting('fila_justa.equidade_k')::numeric)
                   / (b.observado + current_setting('fila_justa.equidade_k')::numeric),
                   current_setting('fila_justa.equidade_alpha')::numeric
               )
           )
       ), 6) AS fator_equidade
FROM base b;
