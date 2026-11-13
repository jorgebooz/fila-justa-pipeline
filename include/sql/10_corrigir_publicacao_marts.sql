-- Views exibem apenas a rodada que está marcada nos fatos atuais.
-- gold.fact_ocorrencia.run_id é atualizado, na mesma transação do score,
-- para todos os clusters no momento da publicação.
-- Não ordenar por avaliado_em: ele é o snapshot climático (mesmo timestamp
-- entre disparos feitos na mesma hora); desempatar pelo hash de run_id
-- não identifica qual execução do Airflow terminou por último.
CREATE OR REPLACE VIEW gold.mart_fila_priorizacao AS
SELECT s.rank_com AS rank, o.cluster_id, o.tipo_padrao AS tipo,
       o.area_id, a.nome AS area_nome, o.n_relatos, s.dias_aberta,
       s.chuva_48h_mm, s.fator_equidade, s.comp_risco,
       s.comp_exposicao, s.comp_tempo, s.comp_clima,
       s.score_com_equidade AS score, s.score_0_100,
       s.explicacao, s.run_id
FROM gold.fact_score s
JOIN gold.fact_ocorrencia o
  ON o.cluster_id = s.cluster_id AND o.run_id = s.run_id
JOIN gold.dim_area a ON a.area_id = o.area_id;

CREATE OR REPLACE VIEW gold.mart_comparacao_equidade AS
SELECT s.run_id, o.area_id, a.nome AS area_nome, o.cluster_id,
       s.rank_sem, s.rank_com, s.score_sem_equidade, s.score_com_equidade,
       e.observado, e.esperado, e.razao, e.fator_equidade,
       CASE WHEN e.razao IS NULL THEN 'indefinida'
            WHEN e.razao < 0.75 THEN 'baixa'
            WHEN e.razao <= 1.25 THEN 'media'
            ELSE 'alta' END AS faixa_propensao_reporte
FROM gold.fact_score s
JOIN gold.fact_ocorrencia o
  ON o.cluster_id = s.cluster_id AND o.run_id = s.run_id
JOIN gold.dim_area a ON a.area_id = o.area_id
JOIN gold.fact_equidade_area e
  ON e.area_id = o.area_id AND e.run_id = s.run_id;
