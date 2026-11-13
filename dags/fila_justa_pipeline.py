"""Fila Justa — Passo 12: Bronze -> Silver -> Gold, em uma DAG diária.

Airflow 3.4 (TaskFlow SDK). As tarefas chamam os mesmos scripts que
já foram validados manualmente; nenhum pipeline paralelo foi reimplementado.

limitação: esta DAG trata São Paulo e relatos de 2026, com Censo 2022.
O quarto trimestre só entra quando o CSV correspondente for publicado no SP156.
A previsão de chuva é operacional (próximas 48h), e não histórica.
"""
from __future__ import annotations

import csv
import hashlib
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

BASE = Path('/opt/airflow/include')
SQL_BASE = BASE / 'sql'
SEED_PONTOS = BASE / 'seeds/pontos_clima_ibge_sp.csv'
SEED_MAPA = BASE / 'seeds/mapa_distritos_ibge_sp.csv'


def id_rodada(dag_run_id: str) -> str:
    """Identificador estável mesmo com retries, diferente entre execuções."""
    if not dag_run_id or not dag_run_id.strip():
        raise ValueError('Identificador de execução do Airflow vazio')
    return 'fj2026_' + hashlib.sha256(dag_run_id.encode('utf-8')).hexdigest()[:24]


def rodada_atual() -> str:
    return id_rodada(get_current_context()['run_id'])


def executar_script(subpasta: str, script: str, *opcoes: str) -> None:
    """check=True impede a DAG de prosseguir se o processo falhar."""
    if subpasta not in ('ingest', 'transform') or not script.endswith('.py') or '/' in script:
        raise ValueError('Script não permitido')
    arquivo = BASE / subpasta / script
    comando = [sys.executable, '-u', str(arquivo), *opcoes]
    print('Executando:', subpasta, '/', script, flush=True)
    subprocess.run(comando, check=True)


def conexao_warehouse():
    dsn = os.environ.get('AIRFLOW_CONN_WAREHOUSE')
    if not dsn:
        raise RuntimeError('AIRFLOW_CONN_WAREHOUSE ausente no worker Airflow')
    import psycopg
    return psycopg.connect(dsn)


def verificar_pontos_existentes() -> bool:
    """Não baixa a malha em todos os dias; falha se o seed estiver corrompido."""
    if not SEED_PONTOS.exists():
        return False
    with SEED_MAPA.open(encoding='utf-8-sig', newline='') as f:
        mapa = list(csv.DictReader(f))
    with SEED_PONTOS.open(encoding='utf-8-sig', newline='') as f:
        pontos = list(csv.DictReader(f))
    codigos = {x['area_id'] for x in mapa if x['municipio_cod'] == '3550308'}
    codigos_pontos = {x['area_id'] for x in pontos}
    if len(mapa) != 96 or len(codigos) != 96 or len(pontos) != 96 or codigos_pontos != codigos:
        raise RuntimeError('Seed de pontos climáticos inconsistente com o IBGE')
    for p in pontos:
        lat, lon = float(p['latitude']), float(p['longitude'])
        if not (-24.1 <= lat <= -23.3 and -47.2 <= lon <= -46.2):
            raise RuntimeError(f'Coordenada climática inválida: {p["area_id"]}')
    return True


@dag(
    dag_id='fila_justa_pipeline',
    description='SP156 + IBGE + Open-Meteo + app -> Silver -> equidade -> score -> qualidade',
    schedule='0 6 * * *',  # 06:00 de São Paulo, disparo manual também disponível
    start_date=pendulum.datetime(2026, 11, 12, tz='America/Sao_Paulo'),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
    tags=['fila-justa', 'bronze', 'silver', 'gold'],
)
def fila_justa_pipeline():
    @task(task_id='preparar_warehouse', execution_timeout=timedelta(minutes=10))
    def preparar_warehouse():
        arquivos = ('00_schemas.sql', '01_bronze.sql', '02_silver.sql', '03_gold.sql')
        with conexao_warehouse() as conexao:
            with conexao.cursor() as cur:
                # A tabela fact_score indica se o modelo Gold já foi criado.
                # Não executar 03_gold.sql novamente em um banco existente:
                # ele recriaria views iniciais sem filtro da rodada mais recente.
                for nome in arquivos[:3]:
                    caminho = SQL_BASE / nome
                    cur.execute(caminho.read_text(encoding='utf-8'))
                    print(f'DDL conferido: {nome}', flush=True)
                cur.execute("SELECT to_regclass('gold.fact_score')")
                gold_existe = cur.fetchone()[0] is not None
                if not gold_existe:
                    cur.execute((SQL_BASE / arquivos[3]).read_text(encoding='utf-8'))
                    print('DDL inicial aplicado: 03_gold.sql', flush=True)
                else:
                    cur.execute("""
                        SELECT COUNT(*) FROM (
                            VALUES ('gold.dim_area'), ('gold.dim_tipo'), ('gold.dim_tempo'),
                                   ('gold.fact_ocorrencia'), ('gold.fact_equidade_area'),
                                   ('gold.fact_score'), ('gold.mart_fila_priorizacao'),
                                   ('gold.mart_comparacao_equidade')
                        ) t(nome) WHERE to_regclass(t.nome) IS NULL
                    """)
                    if cur.fetchone()[0]:
                        raise RuntimeError('Modelo Gold existente está incompleto; revisar DDL')
                    print('Gold existente preservado (views e scores mantidos)', flush=True)
        print('OK: schemas e modelo base prontos', flush=True)

    @task(task_id='garantir_pontos_distritais', execution_timeout=timedelta(minutes=15))
    def garantir_pontos_distritais():
        if verificar_pontos_existentes():
            print('OK: pontos dos 96 distritos já disponíveis', flush=True)
        else:
            executar_script('ingest', 'gerar_pontos_clima.py')
            if not verificar_pontos_existentes():
                raise RuntimeError('Geração dos pontos climáticos não produziu seed válido')

    @task(task_id='ingestao_sp156', execution_timeout=timedelta(hours=5))
    def ingestao_sp156():
        executar_script('ingest', 'sp156.py', '--ano', '2026', '--run-id', rodada_atual())

    @task(task_id='ingestao_ibge', execution_timeout=timedelta(minutes=30))
    def ingestao_ibge():
        executar_script('ingest', 'ibge.py', '--run-id', rodada_atual())

    @task(task_id='ingestao_clima', execution_timeout=timedelta(minutes=40))
    def ingestao_clima():
        executar_script('ingest', 'clima.py', '--run-id', rodada_atual())

    @task(task_id='ingestao_app_simulado', execution_timeout=timedelta(minutes=20))
    def ingestao_app_simulado():
        executar_script('ingest', 'relatos_app_sim.py', '--run-id', rodada_atual())

    @task(task_id='silver_padronizacao', execution_timeout=timedelta(hours=2))
    def silver_padronizacao():
        executar_script('transform', 'silver.py', '--run-id', rodada_atual())

    @task(task_id='silver_clusters', execution_timeout=timedelta(hours=2))
    def silver_clusters():
        executar_script('transform', 'clusters.py', '--run-id', rodada_atual())

    @task(task_id='gold_areas_equidade', execution_timeout=timedelta(minutes=40))
    def gold_areas_equidade():
        executar_script('transform', 'equidade.py', '--run-id', rodada_atual(),
                        '--inicio', '2026-01-01', '--fim', '2027-01-01')

    @task(task_id='silver_clima_48h', execution_timeout=timedelta(minutes=30))
    def silver_clima_48h():
        executar_script('transform', 'clima_silver.py', '--run-id', rodada_atual())

    @task(task_id='gold_score_fila', execution_timeout=timedelta(hours=1))
    def gold_score_fila():
        executar_script('transform', 'score.py', '--run-id', rodada_atual())

    @task(task_id='validar_37_regras', execution_timeout=timedelta(minutes=30))
    def validar_37_regras():
        # Exit code != 0 gera falha Airflow; NÃO usar --simular-falha aqui.
        executar_script('transform', 'qualidade.py', '--run-id', rodada_atual())

    @task(task_id='conferir_marts_publicados', execution_timeout=timedelta(minutes=10))
    def conferir_marts_publicados():
        run_id = rodada_atual()
        with conexao_warehouse() as conexao:
            with conexao.cursor() as cur:
                cur.execute('SELECT COUNT(DISTINCT cluster_id) FROM silver.cluster_ocorrencia')
                esperados = cur.fetchone()[0]
                cur.execute('''
                    SELECT COUNT(*), COUNT(DISTINCT cluster_id), COUNT(DISTINCT run_id),
                           MIN(run_id), MIN(rank), MAX(rank)
                    FROM gold.mart_fila_priorizacao
                ''')
                n, unicos, rodadas, publicada, rank_min, rank_max = cur.fetchone()
                cur.execute('SELECT COUNT(*) FROM gold.mart_comparacao_equidade WHERE run_id = %s', (run_id,))
                comparacao = cur.fetchone()[0]
                cur.execute('SELECT COUNT(*) FROM gold.fact_score WHERE run_id = %s', (run_id,))
                scores = cur.fetchone()[0]
                if (esperados <= 0 or (n, unicos, rodadas, publicada, rank_min, rank_max,
                                       comparacao, scores)
                        != (esperados, esperados, 1, run_id, 1, esperados,
                            esperados, esperados)):
                    raise RuntimeError(
                        f'Marts inconsistentes: clusters={esperados}, fila={n}, unicos={unicos}, '
                        f'rodadas={rodadas}, publicada={publicada}, ranks={rank_min}..{rank_max}, '
                        f'comparacao={comparacao}, scores={scores}, esperado_run_id={run_id}'
                    )
                print(f'OK: marts publicados | run_id={run_id} | clusters={esperados}', flush=True)

    banco = preparar_warehouse()
    pontos = garantir_pontos_distritais()
    sp156 = ingestao_sp156()
    ibge = ingestao_ibge()
    clima = ingestao_clima()
    app = ingestao_app_simulado()
    silver = silver_padronizacao()
    clusters = silver_clusters()
    equidade = gold_areas_equidade()
    clima_silver = silver_clima_48h()
    score = gold_score_fila()
    qualidade = validar_37_regras()
    marts = conferir_marts_publicados()

    banco >> pontos
    banco >> sp156
    banco >> ibge
    pontos >> clima
    pontos >> app
    for origem in (sp156, ibge, clima, app):
        origem >> silver
    silver >> clusters >> equidade >> clima_silver >> score >> qualidade >> marts


fila_justa_pipeline()
