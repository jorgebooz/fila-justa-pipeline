-- Passo 11: regras de qualidade executadas NO warehouse PostgreSQL.
-- Os valores de run_id e equidade_max são definidos como configurações locais
-- à transação pelo script include/transform/qualidade.py.
-- Cada linha retorna (regra, quantidade de falhas, detalhe). A tarefa falha
-- se QUALQUER regra tiver falhas > 0; nunca corrige dados silenciosamente.
-- limitação: coordenadas são validadas por bounding box de SP, não polígono;
-- evolução: usar PostGIS + malha municipal para verificar limites reais.
WITH
  cfg AS (
    SELECT current_setting('fila_justa.qa_run_id')::text AS run_id,
           current_setting('fila_justa.qa_equidade_max')::numeric AS eq_max
  ),
  contagens AS (
    SELECT
      (SELECT COUNT(*) FROM bronze.raw_sp156) AS br_sp,
      (SELECT COUNT(*) FROM bronze.raw_ibge_area WHERE codigo_municipio_raw = '3550308') AS br_ibge,
      (SELECT COUNT(*) FROM bronze.raw_clima) AS br_clima,
      (SELECT COUNT(*) FROM bronze.raw_relatos_app) AS br_app,
      (SELECT COUNT(*) FROM silver.ocorrencia_limpa) AS sil_oc,
      (SELECT COUNT(*) FROM silver.cluster_ocorrencia) AS sil_cl,
      (SELECT COUNT(*) FROM silver.clima_previsao) AS sil_chuva,
      (SELECT COUNT(*) FROM gold.dim_area WHERE municipio_cod = '3550308') AS gd_area,
      (SELECT COUNT(*) FROM gold.dim_tipo) AS gd_tipo,
      (SELECT COUNT(*) FROM gold.dim_tempo) AS gd_tempo,
      (SELECT COUNT(*) FROM gold.fact_ocorrencia) AS gf_oc,
      (SELECT COUNT(*) FROM gold.fact_equidade_area WHERE run_id = (SELECT run_id FROM cfg)) AS gf_eq,
      (SELECT COUNT(*) FROM gold.fact_score WHERE run_id = (SELECT run_id FROM cfg)) AS gf_score,
      (SELECT COUNT(*) FROM gold.mart_fila_priorizacao) AS mart_fila,
      (SELECT COUNT(*) FROM gold.mart_comparacao_equidade) AS mart_eq
  ),
  clima_ultimo AS (
    SELECT MAX(consultado_em) AS snapshot FROM bronze.raw_clima
  ),
  checagens AS (
    SELECT '01_bronze_sp156_nao_vazia'::text AS regra,
           CASE WHEN br_sp > 0 THEN 0 ELSE 1 END::bigint AS falhas,
           'registros=' || br_sp AS detalhe FROM contagens
    UNION ALL
    SELECT '02_bronze_ibge_96', CASE WHEN br_ibge = 96 THEN 0 ELSE abs(br_ibge - 96) END,
           'distritos=' || br_ibge FROM contagens
    UNION ALL
    SELECT '03_bronze_clima_nao_vazia', CASE WHEN br_clima > 0 THEN 0 ELSE 1 END,
           'previsoes=' || br_clima FROM contagens
    UNION ALL
    SELECT '04_bronze_app_nao_vazia', CASE WHEN br_app > 0 THEN 0 ELSE 1 END,
           'relatos=' || br_app FROM contagens
    UNION ALL
    SELECT '05_silver_ocorrencia_nao_vazia', CASE WHEN sil_oc > 0 THEN 0 ELSE 1 END,
           'ocorrencias=' || sil_oc FROM contagens
    UNION ALL
    SELECT '06_silver_cluster_cobre_ocorrencias', abs(sil_cl - sil_oc),
           'vinculos=' || sil_cl || ', ocorrencias=' || sil_oc FROM contagens
    UNION ALL
    SELECT '07_silver_clima_nao_vazia', CASE WHEN sil_chuva > 0 THEN 0 ELSE 1 END,
           'previsoes=' || sil_chuva FROM contagens
    UNION ALL
    SELECT '08_gold_dim_area_96', abs(gd_area - 96),
           'distritos=' || gd_area FROM contagens
    UNION ALL
    SELECT '09_gold_dim_tipo_nao_vazia', CASE WHEN gd_tipo > 0 THEN 0 ELSE 1 END,
           'tipos=' || gd_tipo FROM contagens
    UNION ALL
    SELECT '10_gold_dim_tempo_nao_vazia', CASE WHEN gd_tempo > 0 THEN 0 ELSE 1 END,
           'datas=' || gd_tempo FROM contagens
    UNION ALL
    SELECT '11_gold_fact_ocorrencia_vs_clusters',
           abs(gf_oc - (SELECT COUNT(DISTINCT cluster_id) FROM silver.cluster_ocorrencia)),
           'fatos=' || gf_oc || ', clusters_distintos=' ||
             (SELECT COUNT(DISTINCT cluster_id) FROM silver.cluster_ocorrencia) FROM contagens
    -- O número de clusters não é igual ao número de vínculos. Confira
    -- o total de clusters distintos no controle separado abaixo.
    UNION ALL
    SELECT '12_gold_equidade_96_por_rodada', abs(gf_eq - 96),
           'areas=' || gf_eq FROM contagens
    UNION ALL
    SELECT '13_gold_score_vs_fact_ocorrencia', abs(gf_score - gf_oc),
           'scores=' || gf_score || ', clusters=' || gf_oc FROM contagens
    UNION ALL
    SELECT '14_mart_fila_vs_score', abs(mart_fila - gf_score),
           'fila=' || mart_fila || ', scores=' || gf_score FROM contagens
    UNION ALL
    SELECT '15_mart_equidade_vs_score', abs(mart_eq - gf_score),
           'comparacao=' || mart_eq || ', scores=' || gf_score FROM contagens
    UNION ALL
    SELECT '16_bronze_sp156_chaves_unicas',
           (COUNT(*) - COUNT(DISTINCT (fonte_arquivo, numero_linha)))::bigint,
           'registros=' || COUNT(*) FROM bronze.raw_sp156
    UNION ALL
    SELECT '17_bronze_app_chaves_unicas',
           (COUNT(*) - COUNT(DISTINCT relato_id))::bigint,
           'registros=' || COUNT(*) FROM bronze.raw_relatos_app
    UNION ALL
    SELECT '18_silver_ocorrencia_campos_validos', COUNT(*) FILTER (WHERE
            ocorrencia_id IS NULL OR btrim(ocorrencia_id) = ''
            OR area_id IS NULL OR btrim(area_id) = ''
            OR tipo_padrao IS NULL OR btrim(tipo_padrao) = ''
            OR h3 IS NULL OR btrim(h3) = ''
            OR aberta_em IS NULL OR aberta_em > CURRENT_TIMESTAMP
            OR lat IS NULL OR lon IS NULL
            OR lat NOT BETWEEN -24.1 AND -23.3
            OR lon NOT BETWEEN -47.2 AND -46.2
            OR status NOT IN ('aberto', 'em_andamento'))::bigint,
           'fora_das_regras=' || COUNT(*) FILTER (WHERE
            ocorrencia_id IS NULL OR btrim(ocorrencia_id) = ''
            OR area_id IS NULL OR btrim(area_id) = ''
            OR tipo_padrao IS NULL OR btrim(tipo_padrao) = ''
            OR h3 IS NULL OR btrim(h3) = ''
            OR aberta_em IS NULL OR aberta_em > CURRENT_TIMESTAMP
            OR lat IS NULL OR lon IS NULL
            OR lat NOT BETWEEN -24.1 AND -23.3
            OR lon NOT BETWEEN -47.2 AND -46.2
            OR status NOT IN ('aberto', 'em_andamento'))
      FROM silver.ocorrencia_limpa
    UNION ALL
    SELECT '19_silver_cluster_chaves_unicas',
           (COUNT(*) - COUNT(DISTINCT ocorrencia_id))::bigint,
           'vinculos=' || COUNT(*) FROM silver.cluster_ocorrencia
    UNION ALL
    SELECT '20_silver_cluster_tipo_h3_consistente', COUNT(*)::bigint,
           'grupos_invalidos=' || COUNT(*) FROM (
             SELECT c.cluster_id FROM silver.cluster_ocorrencia c
             JOIN silver.ocorrencia_limpa o ON o.ocorrencia_id = c.ocorrencia_id
             GROUP BY c.cluster_id
             HAVING COUNT(DISTINCT o.tipo_padrao) <> 1 OR COUNT(DISTINCT o.h3) <> 1
           ) grupos_invalidos
    UNION ALL
    SELECT '21_silver_clusters_cobertura_referencial', COUNT(*)::bigint,
           'sem_ocorrencia=' || COUNT(*) FROM silver.cluster_ocorrencia c
           LEFT JOIN silver.ocorrencia_limpa o ON o.ocorrencia_id=c.ocorrencia_id
           WHERE o.ocorrencia_id IS NULL
    UNION ALL
    SELECT '22_gold_dim_area_valores', COUNT(*) FILTER (WHERE
           area_id IS NULL OR nome IS NULL OR btrim(nome) = ''
           OR populacao <= 0 OR domicilios < 0 OR necessidade <= 0)::bigint,
           'areas_invalidas=' || COUNT(*) FILTER (WHERE
           area_id IS NULL OR nome IS NULL OR btrim(nome) = ''
           OR populacao <= 0 OR domicilios < 0 OR necessidade <= 0)
           FROM gold.dim_area
    UNION ALL
    SELECT '23_gold_equidade_limites', COUNT(*) FILTER (WHERE
           fator_equidade < 1 OR fator_equidade > cfg.eq_max
           OR observado < 0 OR esperado < 0 OR janela_inicio >= janela_fim)::bigint,
           'limite_M=' || cfg.eq_max FROM gold.fact_equidade_area e CROSS JOIN cfg
           WHERE e.run_id=cfg.run_id GROUP BY cfg.eq_max
    UNION ALL
    SELECT '24_gold_equidade_soma_relatos',
           abs((SELECT COALESCE(SUM(observado), 0) FROM gold.fact_equidade_area e
              WHERE e.run_id=cfg.run_id)
               - (SELECT COUNT(*) FROM silver.ocorrencia_limpa o
                  WHERE EXISTS (SELECT 1 FROM gold.fact_equidade_area e
                      WHERE e.run_id=cfg.run_id AND e.area_id=o.area_id
                        AND o.aberta_em >= e.janela_inicio
                        AND o.aberta_em < e.janela_fim)))::bigint,
           'comparacao_elegiveis_por_rodada' FROM cfg
    UNION ALL
    SELECT '25_gold_score_valores_e_formula', COUNT(*) FILTER (WHERE
            fator_equidade < 1 OR fator_equidade > cfg.eq_max
            OR chuva_48h_mm < 0 OR dias_aberta < 0
            OR comp_risco < 0 OR comp_exposicao < 0 OR comp_tempo < 0 OR comp_clima < 0
            OR score_0_100 NOT BETWEEN 0 AND 100
            OR comp_risco + comp_exposicao + comp_tempo + comp_clima <> score_com_equidade
            OR score_com_equidade < score_sem_equidade
            OR rank_com < 1 OR rank_sem < 1
            OR avaliado_em IS NULL OR snapshot_clima IS NULL)::bigint,
           'scores_invalidos=' || COUNT(*) FILTER (WHERE
            fator_equidade < 1 OR fator_equidade > cfg.eq_max
            OR chuva_48h_mm < 0 OR dias_aberta < 0
            OR comp_risco < 0 OR comp_exposicao < 0 OR comp_tempo < 0 OR comp_clima < 0
            OR score_0_100 NOT BETWEEN 0 AND 100
            OR comp_risco + comp_exposicao + comp_tempo + comp_clima <> score_com_equidade
            OR score_com_equidade < score_sem_equidade
            OR rank_com < 1 OR rank_sem < 1
            OR avaliado_em IS NULL OR snapshot_clima IS NULL)
      FROM gold.fact_score s CROSS JOIN cfg WHERE s.run_id=cfg.run_id
      GROUP BY cfg.eq_max
    UNION ALL
    SELECT '26_gold_score_explicacao', COUNT(*) FILTER (WHERE
           explicacao IS NULL OR btrim(explicacao) = '')::bigint,
           'sem_explicacao=' || COUNT(*) FILTER (WHERE
           explicacao IS NULL OR btrim(explicacao) = '')
      FROM gold.fact_score s CROSS JOIN cfg WHERE s.run_id=cfg.run_id
    UNION ALL
    SELECT '27_gold_rank_contiguo',
           CASE WHEN COUNT(*) = 0 OR MIN(rank_com) <> 1 OR MAX(rank_com) <> COUNT(*)
                  OR COUNT(DISTINCT rank_com) <> COUNT(*)
                  OR MIN(rank_sem) <> 1 OR MAX(rank_sem) <> COUNT(*)
                  OR COUNT(DISTINCT rank_sem) <> COUNT(*)
                THEN 1 ELSE 0 END::bigint,
           'ranks=' || COUNT(*) FROM gold.fact_score s CROSS JOIN cfg WHERE s.run_id=cfg.run_id
    UNION ALL
    SELECT '28_gold_mart_rodada_correta', COUNT(*) FILTER (WHERE
            m.run_id <> cfg.run_id OR m.rank < 1)::bigint,
           'linhas_mart=' || COUNT(*)
      FROM gold.mart_fila_priorizacao m CROSS JOIN cfg
    UNION ALL
    SELECT '29_gold_fila_componentes', COUNT(*) FILTER (WHERE
           score <> (comp_risco + comp_exposicao + comp_tempo + comp_clima)
           OR score_0_100 NOT BETWEEN 0 AND 100
           OR explicacao IS NULL OR btrim(explicacao) = '')::bigint,
           'linhas_mart=' || COUNT(*) FROM gold.mart_fila_priorizacao
    UNION ALL
    SELECT '30_gold_cluster_pertence_silver', COUNT(*)::bigint,
           'clusters_sem_silver=' || COUNT(*) FROM gold.fact_ocorrencia g
           WHERE NOT EXISTS (SELECT 1 FROM silver.cluster_ocorrencia c
                             WHERE c.cluster_id=g.cluster_id)
    UNION ALL
    SELECT '31_gold_fact_soma_relatos',
           abs((SELECT COALESCE(SUM(n_relatos), 0) FROM gold.fact_ocorrencia)
             - (SELECT COUNT(*) FROM silver.cluster_ocorrencia))::bigint,
           'soma_de_relatos_vs_vinculos' FROM cfg
    UNION ALL
    SELECT '32_gold_cluster_count_distinto',
           abs((SELECT COUNT(*) FROM gold.fact_ocorrencia)
               - (SELECT COUNT(DISTINCT cluster_id) FROM silver.cluster_ocorrencia))::bigint,
           'clusters_distintos_vs_gold' FROM cfg
    UNION ALL
    SELECT '33_silver_snapshot_clima_atualizado',
           CASE WHEN (SELECT MAX(consultado_em) FROM silver.clima_previsao)
                    IS NOT DISTINCT FROM (SELECT snapshot FROM clima_ultimo)
                THEN 0 ELSE 1 END::bigint,
           'bronze_e_silver_mesmo_snapshot' FROM cfg
    UNION ALL
    SELECT '34_silver_clima_96x48',
           CASE WHEN COUNT(*) = 4608 AND COUNT(DISTINCT area_id) = 96
                  AND COUNT(DISTINCT hora) = 48
                THEN 0 ELSE 1 END::bigint,
           'linhas=' || COUNT(*) || ', areas=' || COUNT(DISTINCT area_id)
      FROM silver.clima_previsao p WHERE p.consultado_em=(SELECT snapshot FROM clima_ultimo)
    UNION ALL
    SELECT '35_silver_clima_horas_por_area', COUNT(*)::bigint,
           'areas_com_horas_diferentes_de_48=' || COUNT(*) FROM (
              SELECT area_id FROM silver.clima_previsao
              WHERE consultado_em=(SELECT snapshot FROM clima_ultimo)
              GROUP BY area_id HAVING COUNT(DISTINCT hora) <> 48
           ) anomalia
    UNION ALL
    SELECT '36_silver_clima_sem_valores_invalidos', COUNT(*)::bigint,
           'previsoes_invalidas=' || COUNT(*) FROM silver.clima_previsao
           WHERE precip_mm < 0 OR precip_mm IS NULL
    UNION ALL
    SELECT '37_gold_score_snapshot_corresponde_clima', COUNT(*) FILTER (WHERE
           s.snapshot_clima IS DISTINCT FROM (SELECT MAX(consultado_em) FROM silver.clima_previsao))::bigint,
           'scores_com_snapshot_divergente=' || COUNT(*) FILTER (WHERE
           s.snapshot_clima IS DISTINCT FROM (SELECT MAX(consultado_em) FROM silver.clima_previsao))
      FROM gold.fact_score s CROSS JOIN cfg WHERE s.run_id=cfg.run_id
  )
SELECT regra, falhas::bigint, detalhe FROM checagens ORDER BY regra;
