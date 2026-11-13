-- Passo 6: bronze -> stage SQL; H3 é aplicado em Python antes da gravação silver.
-- Os mapeamentos validados são copiados para tabelas TEMP pelo executável silver.py.
-- PostgreSQL 16: pg_input_is_valid protege o cast de datas inválidas.
-- limitação: a data SP156 que contém só dia recebe 12:00 BRT, pois o CSV
-- não dispõe de precisão temporal adicional. Não inferimos a hora real.
CREATE TEMP TABLE stage_silver_padronizada ON COMMIT DROP AS
WITH fontes AS (
    SELECT
        'SP156:' || fonte_arquivo || ':' || numero_linha::text AS ocorrencia_id,
        'SP156'::text AS canal,
        servico_raw AS tipo_raw,
        status_raw,
        distrito_raw AS area_raw,
        latitude_raw AS lat_raw,
        longitude_raw AS lon_raw,
        data_abertura_raw AS data_raw
    FROM bronze.raw_sp156
    UNION ALL
    SELECT
        'APP:' || relato_id, 'APP_SIMULADO', tipo_raw, status_raw,
        area_id_raw, latitude_raw, longitude_raw, aberta_em_raw
    FROM bronze.raw_relatos_app
),
identificados AS (
    SELECT f.*,
        CASE WHEN f.canal = 'SP156' THEN serv.categoria ELSE tipos.tipo_padrao END AS tipo_padrao,
        CASE WHEN f.canal = 'SP156' THEN distr.area_id ELSE area_app.area_id END AS area_id,
        CASE WHEN upper(btrim(coalesce(f.status_raw, ''))) = 'ABERTO' THEN 'aberto'
             WHEN upper(btrim(coalesce(f.status_raw, ''))) = 'EM ANDAMENTO' THEN 'em_andamento'
        END AS status,
        CASE WHEN btrim(coalesce(f.lat_raw, '')) ~ '^[+-]?[0-9]{1,3}([.,][0-9]+)?$'
             THEN replace(btrim(f.lat_raw), ',', '.')::numeric END AS lat,
        CASE WHEN btrim(coalesce(f.lon_raw, '')) ~ '^[+-]?[0-9]{1,3}([.,][0-9]+)?$'
             THEN replace(btrim(f.lon_raw), ',', '.')::numeric END AS lon,
        CASE
            WHEN f.canal = 'SP156'
                 AND btrim(coalesce(f.data_raw, '')) ~
                     '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}([[:space:]]+[0-9]{1,2}:[0-9]{2}(:[0-9]{2})?)?$'
            THEN
                substr(split_part(btrim(f.data_raw), '/', 3), 1, 4) || '-' ||
                lpad(split_part(btrim(f.data_raw), '/', 1), 2, '0') || '-' ||
                lpad(split_part(btrim(f.data_raw), '/', 2), 2, '0') || 'T' ||
                CASE
                    WHEN position(' ' IN btrim(f.data_raw)) > 0
                    THEN btrim(split_part(btrim(f.data_raw), ' ', 2))
                    ELSE '12:00:00'
                END || '-03:00'
            WHEN f.canal = 'APP_SIMULADO' THEN btrim(f.data_raw)
        END AS data_iso
    FROM fontes f
    LEFT JOIN stage_seed_servicos serv ON f.canal = 'SP156'
       AND btrim(f.tipo_raw) = serv.servico AND serv.incluir
    LEFT JOIN stage_seed_tipos tipos ON f.canal = 'APP_SIMULADO'
       AND btrim(f.tipo_raw) = tipos.tipo_padrao
    LEFT JOIN stage_seed_distritos distr ON f.canal = 'SP156'
       AND regexp_replace(
            translate(upper(btrim(coalesce(f.area_raw, ''))),
              'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC'),
            '[[:space:]]+', ' ', 'g') = distr.nome_normalizado
    LEFT JOIN stage_seed_distritos area_app ON f.canal = 'APP_SIMULADO'
       AND btrim(f.area_raw) = area_app.area_id
),
convertidos AS (
    SELECT i.*,
        CASE
            WHEN i.data_iso IS NOT NULL
                 AND pg_input_is_valid(i.data_iso, 'timestamp with time zone')
            THEN i.data_iso::timestamptz
        END AS aberta_em
    FROM identificados i
)
SELECT ocorrencia_id, canal, tipo_padrao, lat, lon, area_id, aberta_em, status,
       CASE
           WHEN tipo_padrao IS NULL THEN 'tipo_fora_do_escopo'
           WHEN status IS NULL THEN 'status_nao_ativo'
           WHEN area_id IS NULL THEN 'area_nao_mapeada'
           WHEN lat_raw IS NULL OR lon_raw IS NULL
             OR btrim(lat_raw) = '' OR btrim(lon_raw) = '' THEN 'coordenada_ausente'
           WHEN lat IS NULL OR lon IS NULL THEN 'coordenada_invalida'
           WHEN lat NOT BETWEEN -24.1 AND -23.3 OR lon NOT BETWEEN -47.2 AND -46.2
             THEN 'coordenada_fora_limites'
           WHEN aberta_em IS NULL THEN 'data_invalida'
           WHEN aberta_em > now() THEN 'data_futura'
       END AS motivo
FROM convertidos;
