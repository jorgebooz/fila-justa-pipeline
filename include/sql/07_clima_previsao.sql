-- Passo 9: clima bruto -> Silver para o snapshot selecionado pelo Python.
-- O script valida contagens, valores e continuidade dos 48 horarios ANTES do upsert.
-- Cada snapshot historico permanece disponível para auditoria (sem misturar previsões).
CREATE TEMP TABLE stage_clima_previsao ON COMMIT DROP AS
SELECT
    area_id,
    hora_raw::timestamptz AS hora,
    precip_mm_raw::numeric(10,3) AS precip_mm,
    consultado_em
FROM bronze.raw_clima
WHERE consultado_em = current_setting('fila_justa.clima_snapshot')::timestamptz;
