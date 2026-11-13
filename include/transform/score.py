"""Passo 10: cria fatos Gold, score auditável e fila de priorização.

Uso (no scheduler Airflow):
    python -u /opt/airflow/include/transform/score.py --dry-run
    python -u /opt/airflow/include/transform/score.py --run-id equidade_2026

O run_id DEVE ser o mesmo usado na etapa de equidade para que a
comparação antes/depois permaneça auditável. Não usa IA generativa.

limitação: a dimensão fact_ocorrencia representa o estado atual do cluster,
não um histórico completo por rodada. A previsão de chuva é de agora,
não de quando a solicitação de 2026 foi aberta; os pesos são hipóteses.
"""
from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WEIGHTS = ROOT / 'include/config/weights.yml'
TIPOS = ROOT / 'include/seeds/dim_tipo.csv'
STAGES = ROOT / 'include/sql/08_score_fila.sql'

CONFIG = {
    'w_risco', 'w_expo', 'w_tempo', 'w_clima',
    'saturacao_relatos', 'saturacao_dias', 'saturacao_chuva_mm',
    'equidade_max',
}

SQL_MIGRATION = """
ALTER TABLE gold.fact_score
    ADD COLUMN IF NOT EXISTS avaliado_em TIMESTAMPTZ;
ALTER TABLE gold.fact_score
    ADD COLUMN IF NOT EXISTS snapshot_clima TIMESTAMPTZ;
"""

SQL_UPSERT_TIPOS = """
INSERT INTO gold.dim_tipo (tipo_padrao, risco_vida, sens_clima)
SELECT tipo_padrao, risco_vida, sens_clima FROM stage_seed_tipo WHERE true
ON CONFLICT (tipo_padrao) DO UPDATE SET
    risco_vida = EXCLUDED.risco_vida,
    sens_clima = EXCLUDED.sens_clima
WHERE (gold.dim_tipo.risco_vida, gold.dim_tipo.sens_clima)
  IS DISTINCT FROM (EXCLUDED.risco_vida, EXCLUDED.sens_clima)
"""

SQL_UPSERT_OCORRENCIAS = """
INSERT INTO gold.fact_ocorrencia
(cluster_id, municipio_cod, area_id, tipo_padrao, n_relatos, aberta_em, status, run_id)
SELECT cluster_id, municipio_cod, area_id, tipo_padrao, n_relatos, aberta_em, status, %s
FROM stage_fact_ocorrencia WHERE true
ON CONFLICT (cluster_id) DO UPDATE SET
    municipio_cod = EXCLUDED.municipio_cod,
    area_id = EXCLUDED.area_id,
    tipo_padrao = EXCLUDED.tipo_padrao,
    n_relatos = EXCLUDED.n_relatos,
    aberta_em = EXCLUDED.aberta_em,
    status = EXCLUDED.status,
    run_id = EXCLUDED.run_id
WHERE (gold.fact_ocorrencia.municipio_cod, gold.fact_ocorrencia.area_id,
       gold.fact_ocorrencia.tipo_padrao, gold.fact_ocorrencia.n_relatos,
       gold.fact_ocorrencia.aberta_em, gold.fact_ocorrencia.status,
       gold.fact_ocorrencia.run_id)
 IS DISTINCT FROM
      (EXCLUDED.municipio_cod, EXCLUDED.area_id, EXCLUDED.tipo_padrao,
       EXCLUDED.n_relatos, EXCLUDED.aberta_em, EXCLUDED.status, EXCLUDED.run_id)
"""

SQL_UPSERT_SCORE = """
INSERT INTO gold.fact_score (
    run_id, cluster_id, fator_equidade, chuva_48h_mm, dias_aberta,
    comp_risco, comp_exposicao, comp_tempo, comp_clima,
    score_sem_equidade, score_com_equidade, score_0_100,
    rank_sem, rank_com, explicacao, avaliado_em, snapshot_clima
)
SELECT %s, cluster_id, fator_equidade, chuva_48h_mm, dias_aberta,
       comp_risco, comp_exposicao, comp_tempo, comp_clima,
       score_sem_equidade, score_com_equidade, score_0_100,
       rank_sem, rank_com, explicacao, %s, %s
FROM stage_score WHERE true
ON CONFLICT (run_id, cluster_id) DO UPDATE SET
    fator_equidade = EXCLUDED.fator_equidade,
    chuva_48h_mm = EXCLUDED.chuva_48h_mm,
    dias_aberta = EXCLUDED.dias_aberta,
    comp_risco = EXCLUDED.comp_risco,
    comp_exposicao = EXCLUDED.comp_exposicao,
    comp_tempo = EXCLUDED.comp_tempo,
    comp_clima = EXCLUDED.comp_clima,
    score_sem_equidade = EXCLUDED.score_sem_equidade,
    score_com_equidade = EXCLUDED.score_com_equidade,
    score_0_100 = EXCLUDED.score_0_100,
    rank_sem = EXCLUDED.rank_sem,
    rank_com = EXCLUDED.rank_com,
    explicacao = EXCLUDED.explicacao,
    avaliado_em = EXCLUDED.avaliado_em,
    snapshot_clima = EXCLUDED.snapshot_clima
WHERE (gold.fact_score.fator_equidade, gold.fact_score.chuva_48h_mm,
       gold.fact_score.dias_aberta, gold.fact_score.comp_risco,
       gold.fact_score.comp_exposicao, gold.fact_score.comp_tempo,
       gold.fact_score.comp_clima, gold.fact_score.score_sem_equidade,
       gold.fact_score.score_com_equidade, gold.fact_score.score_0_100,
       gold.fact_score.rank_sem, gold.fact_score.rank_com,
       gold.fact_score.explicacao, gold.fact_score.avaliado_em,
       gold.fact_score.snapshot_clima)
 IS DISTINCT FROM
      (EXCLUDED.fator_equidade, EXCLUDED.chuva_48h_mm,
       EXCLUDED.dias_aberta, EXCLUDED.comp_risco,
       EXCLUDED.comp_exposicao, EXCLUDED.comp_tempo,
       EXCLUDED.comp_clima, EXCLUDED.score_sem_equidade,
       EXCLUDED.score_com_equidade, EXCLUDED.score_0_100,
       EXCLUDED.rank_sem, EXCLUDED.rank_com,
       EXCLUDED.explicacao, EXCLUDED.avaliado_em,
       EXCLUDED.snapshot_clima)
"""

SQL_UPSERT_TEMPO = """
INSERT INTO gold.dim_tempo (data, ano, mes, dia, trimestre)
SELECT DISTINCT d,
       EXTRACT(YEAR FROM d)::smallint, EXTRACT(MONTH FROM d)::smallint,
       EXTRACT(DAY FROM d)::smallint, EXTRACT(QUARTER FROM d)::smallint
FROM (
    SELECT (aberta_em AT TIME ZONE 'America/Sao_Paulo')::date AS d
    FROM stage_fact_ocorrencia
) datas
WHERE true
ON CONFLICT (data) DO NOTHING
"""

SQL_MARTS = """
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
"""


def parametros(caminho=WEIGHTS):
    valores = {}
    for linha in Path(caminho).read_text(encoding='utf-8').splitlines():
        linha = linha.split('#', 1)[0].strip()
        if not linha or ':' not in linha:
            continue
        k, v = (x.strip() for x in linha.split(':', 1))
        if k in CONFIG:
            if k in valores:
                raise ValueError(f'Parâmetro duplicado: {k}')
            try:
                valores[k] = Decimal(v)
            except InvalidOperation as exc:
                raise ValueError(f'Valor inválido para {k}: {v}') from exc
    if set(valores) != CONFIG:
        raise ValueError(f'Parâmetros ausentes: {sorted(CONFIG - set(valores))}')
    if not all(v.is_finite() for v in valores.values()):
        raise ValueError('Todos os parâmetros devem ser finitos')
    if any(valores[k] < 0 for k in ('w_risco', 'w_expo', 'w_tempo', 'w_clima')):
        raise ValueError('Pesos não podem ser negativos')
    if sum(valores[k] for k in ('w_risco', 'w_expo', 'w_tempo', 'w_clima')) <= 0:
        raise ValueError('Soma dos pesos deve ser positiva')
    if any(valores[k] <= 0 for k in ('saturacao_relatos', 'saturacao_dias', 'saturacao_chuva_mm')):
        raise ValueError('Saturações devem ser positivas')
    if not 1 <= valores['equidade_max'] <= 5:
        raise ValueError('Limite de equidade deve estar entre 1 e 5')
    return valores


def ler_tipos(caminho=TIPOS):
    with Path(caminho).open(encoding='utf-8-sig', newline='') as arq:
        linhas = list(csv.DictReader(arq))
    if not linhas:
        raise ValueError('dim_tipo.csv vazio')
    saida = []
    for linha in linhas:
        nome = linha['tipo_padrao'].strip()
        try:
            risco = Decimal(linha['risco_vida'])
            clima = Decimal(linha['sens_clima'])
        except InvalidOperation as exc:
            raise ValueError('Risco ou sensibilidade inválidos no seed') from exc
        if not nome or not risco.is_finite() or not clima.is_finite() or not (0 <= risco <= 1) or not (0 <= clima <= 1):
            raise ValueError(f'Tipo com valores inválidos: {nome}')
        saida.append((nome, risco, clima))
    if len(saida) != len({n for n, _, _ in saida}):
        raise ValueError('Tipos duplicados no seed')
    return saida


def score_referencia(n_relatos, dias, chuva, risco, sens_clima, fator, cfg):
    """Implementação de referência (não usada para carregar); valida fórmulas."""
    def rnd(v):
        return v.quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)
    n = Decimal(str(n_relatos))
    d = Decimal(str(dias))
    c = Decimal(str(chuva))
    r = Decimal(str(risco))
    sens = Decimal(str(sens_clima))
    f = Decimal(str(fator))
    if n <= 0 or min(d, c) < 0 or not (0 <= r <= 1) or not (0 <= sens <= 1) or not (1 <= f <= cfg['equidade_max']):
        raise ValueError('Entradas de score inválidas')
    exposicao_base = min(n / cfg['saturacao_relatos'], Decimal('1'))
    comps = {
        'risco': rnd(cfg['w_risco'] * r),
        'expo': rnd(cfg['w_expo'] * exposicao_base * f),
        'expo_sem': rnd(cfg['w_expo'] * exposicao_base),
        'tempo': rnd(cfg['w_tempo'] * min(d / cfg['saturacao_dias'], Decimal('1'))),
        'clima': rnd(cfg['w_clima'] * sens * min(c / cfg['saturacao_chuva_mm'], Decimal('1'))),
    }
    comps['score_com'] = comps['risco'] + comps['expo'] + comps['tempo'] + comps['clima']
    comps['score_sem'] = comps['risco'] + comps['expo_sem'] + comps['tempo'] + comps['clima']
    score_max = cfg['w_risco'] + cfg['w_expo'] * cfg['equidade_max'] + cfg['w_tempo'] + cfg['w_clima']
    comps['score_0_100'] = rnd(100 * comps['score_com'] / score_max)
    return comps


def preparar_seed(cur, tipos):
    cur.execute('''
        CREATE TEMP TABLE stage_seed_tipo (
          tipo_padrao text PRIMARY KEY, risco_vida numeric(5,4), sens_clima numeric(5,4)
        ) ON COMMIT DROP
    ''')
    with cur.copy('COPY stage_seed_tipo (tipo_padrao,risco_vida,sens_clima) FROM STDIN') as cp:
        for linha in tipos:
            cp.write_row(linha)


def obter_snapshot(cur):
    cur.execute('''
        SELECT COUNT(*), COUNT(DISTINCT area_id), MIN(horas), MAX(horas),
               MIN(consultado_em), MAX(consultado_em)
        FROM silver.v_chuva_48h_area
    ''')
    n, areas, minimo, maximo, snap_min, snap_max = cur.fetchone()
    if (n, areas, minimo, maximo) != (96, 96, 48, 48) or snap_min is None or snap_min != snap_max:
        raise RuntimeError('Clima incompleto: esperado 96 distritos × 48 horas no mesmo snapshot')
    return snap_min


def validar_clusters(cur):
    cur.execute('''
        SELECT COUNT(*), COUNT(DISTINCT cluster_id), COUNT(DISTINCT ocorrencia_id)
        FROM silver.cluster_ocorrencia
    ''')
    n, clusters, ids = cur.fetchone()
    if n == 0 or n != ids:
        raise RuntimeError('Vínculos de cluster vazios ou inconsistentes')
    cur.execute('''
        SELECT COUNT(*) FROM (
          SELECT c.cluster_id FROM silver.cluster_ocorrencia c
          JOIN silver.ocorrencia_limpa o USING (ocorrencia_id)
          GROUP BY c.cluster_id
          HAVING COUNT(DISTINCT o.tipo_padrao) > 1
              OR COUNT(DISTINCT o.h3) > 1
        ) misturados
    ''')
    if cur.fetchone()[0]:
        raise RuntimeError('Clusters possuem tipos ou células H3 misturados')
    return n, clusters


def verificar_stage(cur, vinculadas, clusters, cfg):
    cur.execute('''
        SELECT COUNT(*), COUNT(DISTINCT cluster_id), SUM(n_relatos),
               COUNT(*) FILTER (WHERE n_relatos < 1 OR municipio_cod != '3550308')
        FROM stage_fact_ocorrencia
    ''')
    n, unicos, soma, invalidos = cur.fetchone()
    if (n, unicos, soma, invalidos) != (clusters, clusters, vinculadas, 0):
        raise RuntimeError(f'Fato ocorrência inconsistente: {(n, unicos, soma, invalidos)}')
    cur.execute('''
        SELECT COUNT(*) FROM stage_fact_ocorrencia o
        LEFT JOIN gold.dim_area a ON a.area_id=o.area_id
        LEFT JOIN stage_seed_tipo t ON t.tipo_padrao=o.tipo_padrao
        LEFT JOIN silver.v_chuva_48h_area v ON v.area_id=o.area_id
        LEFT JOIN gold.fact_equidade_area e
          ON e.area_id=o.area_id AND e.run_id=current_setting('fila_justa.run_id')
        WHERE a.area_id IS NULL OR t.tipo_padrao IS NULL OR v.area_id IS NULL
           OR e.area_id IS NULL
    ''')
    if cur.fetchone()[0]:
        raise RuntimeError('Clusters sem correspondência de área, tipo, clima ou equidade/run_id')
    cur.execute('''
        SELECT COUNT(*), COUNT(DISTINCT cluster_id),
               MIN(rank_com), MAX(rank_com), COUNT(DISTINCT rank_com),
               MIN(rank_sem), MAX(rank_sem), COUNT(DISTINCT rank_sem),
               COUNT(*) FILTER (
                 WHERE score_0_100 < 0 OR score_0_100 > 100
                    OR fator_equidade < 1
                    OR fator_equidade > current_setting('fila_justa.eq_max')::numeric
                    OR comp_risco + comp_exposicao + comp_tempo + comp_clima != score_com_equidade
                    OR score_com_equidade < score_sem_equidade
                    OR explicacao IS NULL OR btrim(explicacao) = ''
               )
        FROM stage_score
    ''')
    (n, unicos, menor_com, maior_com, ranks_com,
     menor_sem, maior_sem, ranks_sem, invalidos) = cur.fetchone()
    if ((n, unicos, menor_com, maior_com, ranks_com,
         menor_sem, maior_sem, ranks_sem, invalidos)
        != (clusters, clusters, 1, clusters, clusters, 1, clusters, clusters, 0)):
        raise RuntimeError(f'Scores/ranks inconsistentes: clusters={clusters}, stage={n}, inválidos={invalidos}')


def executar(conexao, run_id, dry_run=False):
    if not run_id or len(run_id) > 200:
        raise ValueError('run_id inválido')
    cfg = parametros()
    tipos = ler_tipos()
    with conexao.transaction():
        with conexao.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('fila_justa_score'))")
            vinculadas, clusters = validar_clusters(cur)
            # A mesma rodada deve conter as 96 linhas do passo de equidade.
            cur.execute('''
                SELECT COUNT(*), MIN(janela_inicio), MAX(janela_fim)
                FROM gold.fact_equidade_area WHERE run_id = %s
            ''', (run_id,))
            areas_equidade, inicio, fim = cur.fetchone()
            if areas_equidade != 96:
                raise RuntimeError(f'Equidade da rodada {run_id!r} incompleta: {areas_equidade}/96')
            snap = obter_snapshot(cur)
            if snap.tzinfo is None:
                raise RuntimeError('Snapshot climático sem fuso horário')
            for chave, valor in {
                'fila_justa.run_id': run_id,
                'fila_justa.as_of': snap.isoformat(),
                'fila_justa.w_risco': cfg['w_risco'],
                'fila_justa.w_expo': cfg['w_expo'],
                'fila_justa.w_tempo': cfg['w_tempo'],
                'fila_justa.w_clima': cfg['w_clima'],
                'fila_justa.sat_relatos': cfg['saturacao_relatos'],
                'fila_justa.sat_dias': cfg['saturacao_dias'],
                'fila_justa.sat_chuva': cfg['saturacao_chuva_mm'],
                'fila_justa.eq_max': cfg['equidade_max'],
            }.items():
                cur.execute('SELECT set_config(%s,%s,true)', (chave, str(valor)))
            preparar_seed(cur, tipos)
            cur.execute(STAGES.read_text(encoding='utf-8'))
            verificar_stage(cur, vinculadas, clusters, cfg)
            changes = {'tipos': 0, 'tempo': 0, 'ocorrencias': 0, 'scores': 0, 'scores_removidos': 0}
            if not dry_run:
                cur.execute(SQL_MIGRATION)
                cur.execute(SQL_UPSERT_TIPOS)
                changes['tipos'] = cur.rowcount
                cur.execute(SQL_UPSERT_TEMPO)
                changes['tempo'] = cur.rowcount
                cur.execute(SQL_UPSERT_OCORRENCIAS, (run_id,))
                changes['ocorrencias'] = cur.rowcount
                cur.execute(SQL_UPSERT_SCORE, (run_id, snap, snap))
                changes['scores'] = cur.rowcount
                cur.execute('''
                    DELETE FROM gold.fact_score s
                    WHERE s.run_id = %s
                      AND NOT EXISTS (
                        SELECT 1 FROM stage_score a WHERE a.cluster_id = s.cluster_id
                      )
                ''', (run_id,))
                changes['scores_removidos'] = cur.rowcount
                cur.execute(SQL_MARTS)
                cur.execute('SELECT COUNT(*), MIN(rank), MAX(rank) FROM gold.mart_fila_priorizacao')
                mart_n, primeiro, ultimo = cur.fetchone()
                if (mart_n, primeiro, ultimo) != (clusters, 1, clusters):
                    raise RuntimeError('Mart de fila incompleto ou ranks inválidos')
                cur.execute('SELECT COUNT(*) FROM gold.mart_comparacao_equidade')
                if cur.fetchone()[0] != clusters:
                    raise RuntimeError('Mart de comparação sem todos os clusters')
    return {'clusters': clusters, 'relatos': vinculadas, 'snapshot': snap,
            'mudancas': changes, 'dry_run': dry_run}


def main(argv=None):
    ap = argparse.ArgumentParser(description='Passo 10: score e fila priorizada')
    ap.add_argument('--run-id', default='equidade_2026',
                    help='Mesmo run_id usado em gold.fact_equidade_area')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args(argv)
    dsn = os.environ.get('AIRFLOW_CONN_WAREHOUSE')
    if not dsn:
        raise SystemExit('AIRFLOW_CONN_WAREHOUSE não configurada')
    import psycopg
    with psycopg.connect(dsn) as conexao:
        result = executar(conexao, args.run_id, args.dry_run)
    print(f"SCORE: run_id={args.run_id} | clusters={result['clusters']} | "
          f"relatos={result['relatos']} | snapshot_clima={result['snapshot'].isoformat()}", flush=True)
    if result['dry_run']:
        print('OK: dry-run, Gold não alterada', flush=True)
    else:
        c = result['mudancas']
        print(f"GOLD: tipos={c['tipos']} | tempo={c['tempo']} | "
              f"fact_ocorrencia={c['ocorrencias']} | fact_score={c['scores']} | "
              f"scores_removidos={c['scores_removidos']}", flush=True)
        print('OK: fila priorizada e comparação de equidade publicadas', flush=True)
    idade = datetime.now(timezone.utc) - result['snapshot']
    if idade > timedelta(hours=6):
        print('AVISO: snapshot tem mais de 6 horas. Atualize o clima antes de gerar uma fila operacional.', flush=True)


if __name__ == '__main__':
    main()
