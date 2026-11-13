-- Passo 7. A silver ja possui o H3 calculado na resolucao de weights.yml.
-- A janela em dias vem da configuracao de sessao fila_justa.cluster_janela_dias.
-- Regra: mesmo tipo + mesma celula H3 + ate N dias do PRIMEIRO relato
-- de cada grupo (ancora). Um grupo nao se estende indefinidamente por
-- correntes de relatos separados por intervalos menores que N dias.
-- Em empates de data/hora, ocorrencia_id e o desempate estavel.
-- limitação: relatos em celulas H3 vizinhas nao sao unidos; mesmo H3 pode
-- atravessar fronteiras de distritos. Evolucao: vizinhanca H3 e intersecao
-- com a malha municipal/distrital, sem perder auditoria das ligacoes.
CREATE TEMP TABLE stage_clusters_calculados ON COMMIT DROP AS
WITH RECURSIVE trilha (
    ocorrencia_id, tipo_padrao, h3, aberta_em, ordem,
    ancora_id, ancora_em
) AS (
    SELECT
        o.ocorrencia_id, o.tipo_padrao, o.h3, o.aberta_em, o.ordem,
        o.ocorrencia_id AS ancora_id,
        o.aberta_em AS ancora_em
    FROM stage_clusters_ordenados o
    WHERE o.ordem = 1

    UNION ALL

    SELECT
        o.ocorrencia_id, o.tipo_padrao, o.h3, o.aberta_em, o.ordem,
        CASE
            WHEN o.aberta_em <= t.ancora_em + (current_setting('fila_justa.cluster_janela_dias')::int * INTERVAL '1 day')
            THEN t.ancora_id
            ELSE o.ocorrencia_id
        END AS ancora_id,
        CASE
            WHEN o.aberta_em <= t.ancora_em + (current_setting('fila_justa.cluster_janela_dias')::int * INTERVAL '1 day')
            THEN t.ancora_em
            ELSE o.aberta_em
        END AS ancora_em
    FROM trilha t
    JOIN stage_clusters_ordenados o
      ON o.tipo_padrao = t.tipo_padrao
     AND o.h3 = t.h3
     AND o.ordem = t.ordem + 1
)
SELECT
    ocorrencia_id,
    'cl_' || substr(md5(tipo_padrao || ':' || h3 || ':' || ancora_id), 1, 24)
        AS cluster_id,
    tipo_padrao, h3, aberta_em, ancora_id, ancora_em
FROM trilha;
