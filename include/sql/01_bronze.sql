-- Bronze guarda apenas campos operacionais necessários, não endereços nem texto livre.
-- limitação: esta bronze é uma projeção fiel dos campos selecionados, não uma cópia integral
-- do CSV por causa da LGPD. Evolução: armazenamento bruto restrito e política de retenção.
CREATE TABLE IF NOT EXISTS bronze.raw_sp156 (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fonte_arquivo TEXT NOT NULL,
  numero_linha INTEGER NOT NULL CHECK (numero_linha > 0),
  data_abertura_raw TEXT, data_finalizacao_raw TEXT, meio_comunicacao_raw TEXT,
  tema_raw TEXT, assunto_raw TEXT, servico_raw TEXT, status_raw TEXT,
  latitude_raw TEXT, longitude_raw TEXT, distrito_raw TEXT,
  prefeitura_operacional_raw TEXT,
  _ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  _source TEXT NOT NULL DEFAULT 'SP156', _run_id TEXT NOT NULL,
  UNIQUE(fonte_arquivo, numero_linha)
);
-- O payload guarda agregados públicos do IBGE sem colunas identificadoras de pessoas.
CREATE TABLE IF NOT EXISTS bronze.raw_ibge_area (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  arquivo_origem TEXT NOT NULL, codigo_distrito_raw TEXT NOT NULL,
  codigo_municipio_raw TEXT, nome_distrito_raw TEXT,
  populacao_raw TEXT, domicilios_raw TEXT,
  payload_publico JSONB NOT NULL DEFAULT '{}'::jsonb,
  _ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  _source TEXT NOT NULL DEFAULT 'IBGE_CENSO_2022', _run_id TEXT NOT NULL,
  UNIQUE(arquivo_origem, codigo_distrito_raw)
);
CREATE TABLE IF NOT EXISTS bronze.raw_clima (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  area_id TEXT NOT NULL, hora_raw TEXT NOT NULL,
  precip_mm_raw TEXT, latitude_grade NUMERIC(10,6), longitude_grade NUMERIC(10,6),
  consultado_em TIMESTAMPTZ NOT NULL,
  _ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  _source TEXT NOT NULL DEFAULT 'OPEN_METEO', _run_id TEXT NOT NULL,
  UNIQUE(area_id, hora_raw, consultado_em)
);
CREATE TABLE IF NOT EXISTS bronze.raw_relatos_app (
  relato_id TEXT PRIMARY KEY, tipo_raw TEXT NOT NULL,
  latitude_raw TEXT, longitude_raw TEXT, area_id_raw TEXT,
  aberta_em_raw TEXT, status_raw TEXT,
  _ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  _source TEXT NOT NULL DEFAULT 'APP_SIMULADO', _run_id TEXT NOT NULL
);
