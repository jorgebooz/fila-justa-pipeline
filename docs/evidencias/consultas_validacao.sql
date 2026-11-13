-- Evidência 1: contagens por camada
SELECT 'bronze.raw_sp156' AS tabela, COUNT(*) AS registros
FROM bronze.raw_sp156
UNION ALL
SELECT 'bronze.raw_ibge_area', COUNT(*)
FROM bronze.raw_ibge_area
UNION ALL
SELECT 'bronze.raw_clima', COUNT(*)
FROM bronze.raw_clima
UNION ALL
SELECT 'bronze.raw_relatos_app', COUNT(*)
FROM bronze.raw_relatos_app
UNION ALL
SELECT 'silver.ocorrencia_limpa', COUNT(*)
FROM silver.ocorrencia_limpa
UNION ALL
SELECT 'silver.cluster_ocorrencia', COUNT(*)
FROM silver.cluster_ocorrencia
UNION ALL
SELECT 'silver.clima_previsao', COUNT(*)
FROM silver.clima_previsao
UNION ALL
SELECT 'gold.dim_area', COUNT(*)
FROM gold.dim_area
UNION ALL
SELECT 'gold.fact_ocorrencia', COUNT(*)
FROM gold.fact_ocorrencia
UNION ALL
SELECT 'gold.fact_score', COUNT(*)
FROM gold.fact_score
UNION ALL
SELECT 'gold.mart_fila_priorizacao', COUNT(*)
FROM gold.mart_fila_priorizacao;

-- Evidência 2: rodada publicada
SELECT run_id, COUNT(*) AS clusters
FROM gold.mart_fila_priorizacao
GROUP BY run_id;

-- Evidência 3: dez primeiros da fila
SELECT
    rank,
    area_nome,
    tipo,
    n_relatos,
    score_0_100,
    fator_equidade,
    chuva_48h_mm
FROM gold.mart_fila_priorizacao
ORDER BY rank
LIMIT 10;
