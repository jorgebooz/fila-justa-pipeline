"""Passo 9: padroniza o snapshot do Open-Meteo para silver.clima_previsao.

Executar dentro do Airflow (depende de AIRFLOW_CONN_WAREHOUSE):
    python /opt/airflow/include/transform/clima_silver.py --dry-run
    python /opt/airflow/include/transform/clima_silver.py --run-id clima_silver_2026

Mantém os snapshots separados; não mistura consultas de datas diferentes.
A view silver.v_chuva_48h_area mostra o snapshot mais recente e a chuva por área.

limitação: a previsão é relativa ao momento de consulta, não ao ano de abertura
histórico dos relatos. Para a priorização operacional, atualizar a ingestão clima
antes do score; valores ausentes não devem ser confundidos com chuva zero.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL_FILE = ROOT / 'include/sql/07_clima_previsao.sql'

SQL_UPSERT = """
INSERT INTO silver.clima_previsao (area_id, hora, precip_mm, consultado_em, run_id)
SELECT area_id, hora, precip_mm, consultado_em, %s
FROM stage_clima_previsao
WHERE true
ON CONFLICT (area_id, hora, consultado_em) DO UPDATE SET
    precip_mm = EXCLUDED.precip_mm,
    run_id = EXCLUDED.run_id
WHERE silver.clima_previsao.precip_mm IS DISTINCT FROM EXCLUDED.precip_mm
"""

SQL_VIEW = """
CREATE OR REPLACE VIEW silver.v_chuva_48h_area AS
WITH ultimo AS (
    SELECT MAX(consultado_em) AS consultado_em FROM silver.clima_previsao
)
SELECT p.area_id, p.consultado_em,
       SUM(p.precip_mm)::numeric(12,3) AS chuva_48h_mm,
       COUNT(*)::integer AS horas
FROM silver.clima_previsao p
JOIN ultimo u ON p.consultado_em = u.consultado_em
WHERE p.hora >= p.consultado_em
  AND p.hora < p.consultado_em + INTERVAL '48 hours'
GROUP BY p.area_id, p.consultado_em
"""

# Apenas formato númerico finito e não-negativo (não aceitar 'NaN', '-1', branco etc).
SQL_SOURCE_SUMMARY = """
SELECT COUNT(*) AS linhas,
       COUNT(DISTINCT area_id) AS areas,
       COUNT(DISTINCT hora_raw) AS horas,
       COUNT(*) FILTER (
         WHERE area_id IS NULL OR btrim(area_id) = ''
           OR hora_raw IS NULL OR btrim(hora_raw) = ''
           OR precip_mm_raw IS NULL
           OR btrim(precip_mm_raw) !~ '^[+]?[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$'
       ) AS malformadas
FROM bronze.raw_clima WHERE consultado_em = %s
"""


def parse_snapshot(valor: str) -> datetime:
    """Entrada explícita é ISO8601 com offset (ex.: 2026-11-12T02:00:00+00:00)."""
    instante = datetime.fromisoformat(valor.replace('Z', '+00:00'))
    if instante.tzinfo is None or instante.utcoffset() is None:
        raise ValueError('Snapshot precisa de fuso explícito: +00:00 ou Z')
    return instante.astimezone(timezone.utc)


def verificar_metricas(linhas: int, areas: int, horas: int, malformadas: int):
    if (linhas, areas, horas, malformadas) != (4608, 96, 48, 0):
        raise RuntimeError(
            f'Snapshot incompleto/inválido: linhas={linhas}/4608, '
            f'areas={areas}/96, horas={horas}/48, malformadas={malformadas}'
        )


def executar(conexao, run_id: str, snapshot: datetime | None, dry_run: bool):
    with conexao.transaction():
        with conexao.cursor() as cur:
            if snapshot is None:
                cur.execute('SELECT MAX(consultado_em) FROM bronze.raw_clima')
                snapshot = cur.fetchone()[0]
            if snapshot is None:
                raise RuntimeError('Bronze do clima vazia; execute include/ingest/clima.py')
            if snapshot.tzinfo is None:
                raise RuntimeError('consultado_em sem timezone na bronze')

            cur.execute(SQL_SOURCE_SUMMARY, (snapshot,))
            linhas, areas, horas, malformadas = cur.fetchone()
            verificar_metricas(linhas, areas, horas, malformadas)

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT area_id, COUNT(*) AS n, COUNT(DISTINCT hora_raw) AS distintos
                    FROM bronze.raw_clima
                    WHERE consultado_em = %s
                    GROUP BY area_id
                    HAVING COUNT(*) != 48 OR COUNT(DISTINCT hora_raw) != 48
                ) incompletos
            """, (snapshot,))
            if cur.fetchone()[0]:
                raise RuntimeError('Alguns distritos não possuem exatamente 48 horas distintas')

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT b.area_id
                    FROM bronze.raw_clima b
                    LEFT JOIN gold.dim_area a ON a.area_id = b.area_id
                    WHERE b.consultado_em = %s AND a.area_id IS NULL
                ) desconhecidas
            """, (snapshot,))
            if cur.fetchone()[0]:
                raise RuntimeError('Clima com distrito sem correspondência em gold.dim_area')

            # Staging cria valores tipados dentro do warehouse (ELT).
            cur.execute('SELECT set_config(%s, %s, true)',
                        ('fila_justa.clima_snapshot', snapshot.isoformat()))
            cur.execute(SQL_FILE.read_text(encoding='utf-8'))
            cur.execute('''
                SELECT COUNT(*), COUNT(DISTINCT area_id), COUNT(DISTINCT hora),
                       MIN(hora), MAX(hora),
                       COUNT(*) FILTER (WHERE precip_mm < 0)
                FROM stage_clima_previsao
            ''')
            n, n_areas, n_horas, inicio, fim, negativas = cur.fetchone()
            verificar_metricas(n, n_areas, n_horas, negativas)
            if inicio != snapshot or fim != snapshot + timedelta(hours=47):
                raise RuntimeError(
                    f'Janela temporal inválida: {inicio}..{fim}; '
                    f'esperado {snapshot}..{snapshot + timedelta(hours=47)}'
                )

            cur.execute('SELECT SUM(precip_mm) FROM stage_clima_previsao')
            chuva_total = cur.fetchone()[0]
            alteradas = 0
            if not dry_run:
                cur.execute(SQL_UPSERT, (run_id,))
                alteradas = cur.rowcount
                cur.execute('''
                    SELECT COUNT(*), COUNT(DISTINCT area_id)
                    FROM silver.clima_previsao WHERE consultado_em = %s
                ''', (snapshot,))
                n, n_areas = cur.fetchone()
                if (n, n_areas) != (4608, 96):
                    raise RuntimeError('Silver climática não contém os 4.608 registros esperados')
                cur.execute(SQL_VIEW)
                # Não permitir view parcial / snapshots inconsistentes.
                cur.execute('''
                    SELECT COUNT(*), MIN(horas), MAX(horas)
                    FROM silver.v_chuva_48h_area
                ''')
                v_areas, v_min, v_max = cur.fetchone()
                if (v_areas, v_min, v_max) != (96, 48, 48):
                    raise RuntimeError('View de chuva 48h incompleta')

    return dict(snapshot=snapshot, linhas=4608, areas=96, chuva_total=chuva_total,
                alteradas=alteradas, dry_run=dry_run)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Passo 9: Open-Meteo Bronze -> Silver')
    parser.add_argument('--run-id', default='clima_silver_2026')
    parser.add_argument('--snapshot', help='ISO8601 com fuso; padrão: último na Bronze')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    snapshot = parse_snapshot(args.snapshot) if args.snapshot else None
    dsn = os.getenv('AIRFLOW_CONN_WAREHOUSE')
    if not dsn:
        raise SystemExit('AIRFLOW_CONN_WAREHOUSE não configurada')
    import psycopg
    with psycopg.connect(dsn) as conexao:
        r = executar(conexao, args.run_id, snapshot, args.dry_run)
    print(f"CLIMA SILVER: snapshot={r['snapshot'].isoformat()} | "
          f"registros={r['linhas']} | distritos={r['areas']} | "
          f"soma_das_96_previsoes_mm={r['chuva_total']}", flush=True)
    if r['dry_run']:
        print('OK: dry-run validado; nenhuma linha Silver gravada', flush=True)
    else:
        print(f"CLIMA SILVER: inseridas_ou_alteradas={r['alteradas']}", flush=True)
        print('OK: previsão na Silver e view chuva_48h criadas', flush=True)
    idade = datetime.now(timezone.utc) - r['snapshot']
    if idade > timedelta(hours=6):
        print('AVISO: snapshot tem mais de 6 horas; atualize o clima antes do score operacional',
              flush=True)


if __name__ == '__main__':
    main()
