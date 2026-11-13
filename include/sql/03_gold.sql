CREATE TABLE IF NOT EXISTS gold.dim_area (
  area_id TEXT PRIMARY KEY, municipio_cod CHAR(7) NOT NULL,
  nome TEXT NOT NULL, populacao BIGINT NOT NULL CHECK (populacao >= 0),
  domicilios BIGINT CHECK (domicilios >= 0),
  necessidade NUMERIC(8,4) NOT NULL DEFAULT 1 CHECK (necessidade > 0),
  latitude_centroide NUMERIC(10,7), longitude_centroide NUMERIC(10,7)
);
CREATE TABLE IF NOT EXISTS gold.dim_tipo (
  tipo_padrao TEXT PRIMARY KEY,
  risco_vida NUMERIC(5,4) NOT NULL CHECK (risco_vida BETWEEN 0 AND 1),
  sens_clima NUMERIC(5,4) NOT NULL CHECK (sens_clima BETWEEN 0 AND 1)
);
CREATE TABLE IF NOT EXISTS gold.dim_tempo (
  data DATE PRIMARY KEY, ano SMALLINT NOT NULL, mes SMALLINT NOT NULL,
  dia SMALLINT NOT NULL, trimestre SMALLINT NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.fact_ocorrencia (
  cluster_id TEXT PRIMARY KEY, municipio_cod CHAR(7) NOT NULL,
  area_id TEXT NOT NULL REFERENCES gold.dim_area(area_id),
  tipo_padrao TEXT NOT NULL REFERENCES gold.dim_tipo(tipo_padrao),
  n_relatos INTEGER NOT NULL CHECK(n_relatos > 0),
  aberta_em TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL, run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.fact_equidade_area (
  run_id TEXT NOT NULL, area_id TEXT NOT NULL REFERENCES gold.dim_area(area_id),
  janela_inicio TIMESTAMPTZ NOT NULL, janela_fim TIMESTAMPTZ NOT NULL,
  observado INTEGER NOT NULL CHECK(observado >= 0),
  esperado NUMERIC(16,5) NOT NULL CHECK(esperado >= 0),
  razao NUMERIC(16,6), fator_equidade NUMERIC(9,6) NOT NULL CHECK(fator_equidade >= 1),
  PRIMARY KEY (run_id, area_id)
);
CREATE TABLE IF NOT EXISTS gold.fact_score (
  run_id TEXT NOT NULL, cluster_id TEXT NOT NULL REFERENCES gold.fact_ocorrencia(cluster_id),
  fator_equidade NUMERIC(9,6) NOT NULL CHECK (fator_equidade >= 1),
  chuva_48h_mm NUMERIC(12,3) NOT NULL DEFAULT 0 CHECK(chuva_48h_mm >= 0),
  dias_aberta INTEGER NOT NULL CHECK(dias_aberta >= 0),
  comp_risco NUMERIC(12,5) NOT NULL, comp_exposicao NUMERIC(12,5) NOT NULL,
  comp_tempo NUMERIC(12,5) NOT NULL, comp_clima NUMERIC(12,5) NOT NULL,
  score_sem_equidade NUMERIC(12,5) NOT NULL,
  score_com_equidade NUMERIC(12,5) NOT NULL,
  score_0_100 NUMERIC(9,5) NOT NULL CHECK(score_0_100 BETWEEN 0 AND 100),
  rank_sem INTEGER NOT NULL, rank_com INTEGER NOT NULL,
  explicacao TEXT NOT NULL,
  PRIMARY KEY(run_id, cluster_id)
);
-- A linha de maior rank por cluster/run é definida na etapa de transformação.
CREATE OR REPLACE VIEW gold.mart_fila_priorizacao AS
SELECT s.rank_com AS rank, o.cluster_id, o.tipo_padrao AS tipo,
 o.area_id, a.nome AS area_nome, o.n_relatos, s.dias_aberta,
 s.chuva_48h_mm, s.fator_equidade,
 s.comp_risco, s.comp_exposicao, s.comp_tempo, s.comp_clima,
 s.score_com_equidade AS score, s.score_0_100, s.explicacao, s.run_id
FROM gold.fact_score s JOIN gold.fact_ocorrencia o USING(cluster_id)
JOIN gold.dim_area a ON a.area_id = o.area_id;
CREATE OR REPLACE VIEW gold.mart_comparacao_equidade AS
SELECT s.run_id, o.area_id, a.nome AS area_nome, o.cluster_id,
 s.rank_sem, s.rank_com, s.score_sem_equidade, s.score_com_equidade,
 e.observado, e.esperado, e.razao, e.fator_equidade,
 CASE WHEN e.razao IS NULL THEN 'indefinida'
 WHEN e.razao < 0.75 THEN 'baixa'
 WHEN e.razao <= 1.25 THEN 'media' ELSE 'alta' END AS faixa_propensao_reporte
FROM gold.fact_score s JOIN gold.fact_ocorrencia o USING(cluster_id)
JOIN gold.dim_area a ON a.area_id = o.area_id
JOIN gold.fact_equidade_area e ON e.area_id = o.area_id AND e.run_id = s.run_id;
