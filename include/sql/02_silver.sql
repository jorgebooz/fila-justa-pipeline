CREATE TABLE IF NOT EXISTS silver.ocorrencia_limpa (
  ocorrencia_id TEXT PRIMARY KEY,
  canal TEXT NOT NULL, municipio_cod CHAR(7) NOT NULL DEFAULT '3550308',
  tipo_padrao TEXT NOT NULL, lat NUMERIC(10,7) NOT NULL,
  lon NUMERIC(10,7) NOT NULL, h3 TEXT NOT NULL, area_id TEXT NOT NULL,
  aberta_em TIMESTAMPTZ NOT NULL, status TEXT NOT NULL,
  run_id TEXT NOT NULL,
  CONSTRAINT ocorrencia_lat_sp CHECK (lat BETWEEN -24.1 AND -23.3),
  CONSTRAINT ocorrencia_lon_sp CHECK (lon BETWEEN -47.2 AND -46.2)
);
-- limitação: bounding box acima é filtro preliminar, não polígono do município.
-- Evolução: teste point-in-polygon contra a malha municipal do IBGE.
CREATE INDEX IF NOT EXISTS ix_ocorrencia_grupo
ON silver.ocorrencia_limpa (tipo_padrao, h3, aberta_em);
CREATE TABLE IF NOT EXISTS silver.cluster_ocorrencia (
  ocorrencia_id TEXT PRIMARY KEY REFERENCES silver.ocorrencia_limpa(ocorrencia_id),
  cluster_id TEXT NOT NULL, run_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_cluster ON silver.cluster_ocorrencia (cluster_id);
CREATE TABLE IF NOT EXISTS silver.clima_previsao (
  area_id TEXT NOT NULL, hora TIMESTAMPTZ NOT NULL,
  precip_mm NUMERIC(10,3) NOT NULL CHECK (precip_mm >= 0),
  consultado_em TIMESTAMPTZ NOT NULL, run_id TEXT NOT NULL,
  PRIMARY KEY(area_id, hora, consultado_em)
);
CREATE TABLE IF NOT EXISTS silver.descarte_ocorrencia (
  run_id TEXT NOT NULL, canal TEXT NOT NULL, motivo TEXT NOT NULL,
  quantidade INTEGER NOT NULL CHECK (quantidade >= 0),
  PRIMARY KEY(run_id, canal, motivo)
);
