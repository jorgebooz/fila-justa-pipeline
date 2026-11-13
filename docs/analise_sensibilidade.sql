-- Analise de sensibilidade das HIPOTESES de equidade.
-- Execute somente depois de DAG verde; os resultados NAO foram simulados neste documento.
-- Compara alfa e teto M; risco, tempo e clima mantidos como na rodada publicada.
-- Reconstroi exposicao base por n_relatos, sem ajuste, com pesos configurados
-- 30 e saturacao de 20 (weights.yml vigente). Ao mudar pesos, atualize o SQL.
WITH cenarios(alpha, teto) AS (
  VALUES (0.0::numeric,1.0::numeric),
         (0.5::numeric,2.0::numeric),
         (1.0::numeric,2.0::numeric),
         (0.5::numeric,1.5::numeric),
         (1.0::numeric,1.5::numeric)
), base AS (
  SELECT o.cluster_id, o.n_relatos, s.comp_risco, s.comp_tempo, s.comp_clima,
         e.observado, e.esperado,
         (30.0::numeric * LEAST(o.n_relatos::numeric/20.0,1)) AS exposicao_base
  FROM gold.fact_ocorrencia o
  JOIN gold.fact_score s ON s.cluster_id=o.cluster_id AND s.run_id=o.run_id
  JOIN gold.fact_equidade_area e ON e.area_id=o.area_id AND e.run_id=s.run_id
), calculos AS (
  SELECT c.alpha, c.teto, b.cluster_id,
         b.comp_risco+b.comp_tempo+b.comp_clima+
         ROUND(b.exposicao_base * LEAST(c.teto,
              GREATEST(1::numeric,POWER((b.esperado+10)/(b.observado+10), c.alpha))),5) AS nota
  FROM cenarios c CROSS JOIN base b
), ranking AS (
  SELECT alpha,teto,cluster_id,nota,
         ROW_NUMBER() OVER(PARTITION BY alpha,teto ORDER BY nota DESC,cluster_id) AS posicao
  FROM calculos
), ref AS (
  SELECT cluster_id,posicao AS posicao_sem FROM ranking WHERE alpha=0 AND teto=1
)
SELECT r.alpha, r.teto,
       COUNT(*) FILTER (WHERE r.posicao<=100) AS quantidade_top100,
       COUNT(*) FILTER (WHERE r.posicao<=100 AND ref.posicao_sem>100) AS novos_no_top100,
       ROUND(AVG(ABS(r.posicao-ref.posicao_sem)) FILTER (WHERE ref.posicao_sem<=100),2)
         AS mudanca_media_posicao_top100_original
FROM ranking r JOIN ref USING(cluster_id)
GROUP BY r.alpha,r.teto
ORDER BY r.alpha,r.teto;
