"""Passo 8: gold.dim_area + gold.fact_equidade_area.

Reexecutavel com o mesmo --run-id; 'inseridas_ou_alteradas=0' se nao mudou nada.
Usa as ocorrencias elegiveis na Silver (relatos, antes dos clusters),
na janela [--inicio, --fim) do ano 2026. Populacao: IBGE Censo 2022.

limitação: IBGE 2022 usado como proxy populacional para relatos de 2026.
limitação: 'observado' considera somente relatos elegiveis apos filtros da Silver,
nao todos os 1,5 milhao registros brutos do SP156; sem validacao de necessidade
em campo, necessidade=1 e parametro de negocio hipotetico.
"""
from __future__ import annotations

import argparse
import math
import os
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
WEIGHTS = ROOT / "include/config/weights.yml"
SQL_FILE = ROOT / "include/sql/06_areas_equidade.sql"

SQL_UPSERT_AREA = """
INSERT INTO gold.dim_area (
    area_id, municipio_cod, nome, populacao, domicilios, necessidade
)
SELECT area_id, municipio_cod, nome, populacao, domicilios, necessidade
FROM stage_dim_area
WHERE true
ON CONFLICT (area_id) DO UPDATE SET
    municipio_cod = EXCLUDED.municipio_cod,
    nome = EXCLUDED.nome,
    populacao = EXCLUDED.populacao,
    domicilios = EXCLUDED.domicilios
WHERE (
    gold.dim_area.municipio_cod, gold.dim_area.nome,
    gold.dim_area.populacao, gold.dim_area.domicilios
) IS DISTINCT FROM (
    EXCLUDED.municipio_cod, EXCLUDED.nome,
    EXCLUDED.populacao, EXCLUDED.domicilios
)
"""

SQL_UPSERT_EQUIDADE = """
INSERT INTO gold.fact_equidade_area (
    run_id, area_id, janela_inicio, janela_fim,
    observado, esperado, razao, fator_equidade
)
SELECT %s, area_id, %s, %s, observado, esperado, razao, fator_equidade
FROM stage_equidade
WHERE true
ON CONFLICT (run_id, area_id) DO UPDATE SET
    janela_inicio = EXCLUDED.janela_inicio,
    janela_fim = EXCLUDED.janela_fim,
    observado = EXCLUDED.observado,
    esperado = EXCLUDED.esperado,
    razao = EXCLUDED.razao,
    fator_equidade = EXCLUDED.fator_equidade
WHERE (
    gold.fact_equidade_area.janela_inicio,
    gold.fact_equidade_area.janela_fim,
    gold.fact_equidade_area.observado,
    gold.fact_equidade_area.esperado,
    gold.fact_equidade_area.razao,
    gold.fact_equidade_area.fator_equidade
) IS DISTINCT FROM (
    EXCLUDED.janela_inicio, EXCLUDED.janela_fim,
    EXCLUDED.observado, EXCLUDED.esperado,
    EXCLUDED.razao, EXCLUDED.fator_equidade
)
"""


def ler_parametros(caminho=WEIGHTS):
    """Ler tres escalares do YAML plano, sem adicionar bibliotecas."""
    chaves = {"equidade_k", "equidade_alpha", "equidade_max"}
    valores = {}
    for linha in Path(caminho).read_text(encoding="utf-8").splitlines():
        linha = linha.split("#", 1)[0].strip()
        if not linha or ":" not in linha:
            continue
        chave, valor = linha.split(":", 1)
        if chave.strip() in chaves:
            if chave.strip() in valores:
                raise ValueError(f"Configuracao repetida: {chave}")
            valores[chave.strip()] = float(valor.strip())
    if set(valores) != chaves:
        raise ValueError(f"Faltam parametros de equidade: {sorted(chaves - set(valores))}")
    k, alpha, maximo = (valores[x] for x in ("equidade_k", "equidade_alpha", "equidade_max"))
    if not all(math.isfinite(x) for x in (k, alpha, maximo)):
        raise ValueError("Parametros devem ser finitos")
    if k <= 0 or not (0 < alpha <= 2) or not (1 <= maximo <= 5):
        raise ValueError("equidade_k>0, 0<alpha<=2, 1<=equidade_max<=5")
    return {"k": k, "alpha": alpha, "maximo": maximo}


def fator_equidade(observado, esperado, k=10.0, alpha=0.5, maximo=2.0):
    """Referencia pura da formula do guia, util para testes unitarios."""
    if observado < 0 or esperado < 0 or k <= 0 or not 0 < alpha <= 2 or not 1 <= maximo <= 5:
        raise ValueError("Entradas invalidas para fator de equidade")
    return min(max(((esperado + k) / (observado + k)) ** alpha, 1.0), maximo)


def intervalo(inicio, fim):
    """Datas civis locais: inicio inclusivo e fim exclusivo."""
    ini = date.fromisoformat(inicio)
    end = date.fromisoformat(fim)
    if ini >= end:
        raise ValueError("--fim deve ser posterior a --inicio")
    tz = ZoneInfo("America/Sao_Paulo")
    return datetime.combine(ini, time.min, tz), datetime.combine(end, time.min, tz)


def executar(conexao, run_id, inicio, fim, parametros, dry_run=False):
    if not run_id or len(run_id) > 200:
        raise ValueError("run_id deve conter 1 a 200 caracteres")
    if inicio >= fim:
        raise ValueError("Janela temporal invalida")
    with conexao.transaction():
        with conexao.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('fila_justa_equidade'))")
            for chave, valor in (
                ("fila_justa.janela_inicio", inicio.isoformat()),
                ("fila_justa.janela_fim", fim.isoformat()),
                ("fila_justa.equidade_k", str(parametros["k"])),
                ("fila_justa.equidade_alpha", str(parametros["alpha"])),
                ("fila_justa.equidade_max", str(parametros["maximo"])),
            ):
                cur.execute("SELECT set_config(%s, %s, true)", (chave, valor))
            cur.execute(SQL_FILE.read_text(encoding="utf-8"))

            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT area_id),
                       SUM(populacao), SUM(domicilios),
                       COUNT(*) FILTER (WHERE populacao <= 0 OR domicilios < 0
                          OR necessidade <= 0 OR nome IS NULL OR btrim(nome) = '')
                FROM stage_dim_area
            """)
            n_areas, unicas, populacao, domicilios, invalidas = cur.fetchone()
            if n_areas != 96 or unicas != 96 or invalidas:
                raise RuntimeError(f"IBGE divergente: areas={n_areas}, unicas={unicas}, invalidas={invalidas}")
            cur.execute("""
                SELECT COUNT(*) FROM stage_observado o
                WHERE NOT EXISTS (SELECT 1 FROM stage_dim_area a WHERE a.area_id = o.area_id)
            """)
            areas_nao_mapeadas = cur.fetchone()[0]
            if areas_nao_mapeadas:
                raise RuntimeError(f"Silver com {areas_nao_mapeadas} distritos nao presentes no IBGE")
            cur.execute("""
                SELECT COUNT(*), SUM(observado), SUM(esperado),
                       MIN(fator_equidade), MAX(fator_equidade),
                       COUNT(*) FILTER (WHERE fator_equidade > 1),
                       COUNT(*) FILTER (WHERE observado::numeric < esperado * 0.75
                           AND fator_equidade <= 1),
                       COUNT(*) FILTER (WHERE fator_equidade < 1 OR
                           fator_equidade > current_setting('fila_justa.equidade_max')::numeric)
                FROM stage_equidade
            """)
            (n_fatos, relatos, soma_esperado, fator_min, fator_max,
             reforcadas, subrepresentadas_sem_reforco, fora_limite) = cur.fetchone()
            if n_fatos != 96 or not relatos or subrepresentadas_sem_reforco or fora_limite:
                raise RuntimeError("Fact equidade invalida: contagens ou limites do fator")
            if abs(soma_esperado - relatos) > 0.01:
                raise RuntimeError(f"Esperado nao fecha com observado: {soma_esperado} != {relatos}")

            alteradas_area = alteradas_equidade = 0
            if not dry_run:
                cur.execute(SQL_UPSERT_AREA)
                alteradas_area = cur.rowcount
                cur.execute(SQL_UPSERT_EQUIDADE, (run_id, inicio, fim))
                alteradas_equidade = cur.rowcount
                cur.execute("SELECT COUNT(*) FROM gold.dim_area WHERE municipio_cod = '3550308'")
                if cur.fetchone()[0] != 96:
                    raise RuntimeError("dim_area nao tem 96 distritos")
                cur.execute("SELECT COUNT(*) FROM gold.fact_equidade_area WHERE run_id = %s", (run_id,))
                if cur.fetchone()[0] != 96:
                    raise RuntimeError("fact_equidade_area nao tem 96 linhas para run_id")
    return dict(areas=n_areas, populacao=populacao, domicilios=domicilios,
                relatos=relatos, fator_min=fator_min, fator_max=fator_max,
                reforcadas=reforcadas, alteradas_area=alteradas_area,
                alteradas_equidade=alteradas_equidade, dry_run=dry_run)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Passo 8: areas IBGE e equidade por distrito")
    parser.add_argument("--run-id", default="equidade_2026")
    parser.add_argument("--inicio", default="2026-01-01", help="Data local inclusiva YYYY-MM-DD")
    parser.add_argument("--fim", default="2027-01-01", help="Data local exclusiva YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Testar sem gravar na Gold")
    args = parser.parse_args(argv)
    inicio, fim = intervalo(args.inicio, args.fim)
    parametros = ler_parametros()
    import psycopg
    dsn = os.environ.get("AIRFLOW_CONN_WAREHOUSE")
    if not dsn:
        raise SystemExit("AIRFLOW_CONN_WAREHOUSE nao configurada")
    with psycopg.connect(dsn) as conexao:
        resultado = executar(conexao, args.run_id, inicio, fim, parametros, args.dry_run)
    print(f"IBGE: areas={resultado['areas']} | populacao={resultado['populacao']} | "
          f"domicilios={resultado['domicilios']}", flush=True)
    print(f"EQUIDADE: janela=[{args.inicio}, {args.fim}) | "
          f"relatos_elegiveis={resultado['relatos']} | "
          f"areas_reforcadas={resultado['reforcadas']} | "
          f"fator_min={resultado['fator_min']} | fator_max={resultado['fator_max']}", flush=True)
    if resultado['dry_run']:
        print("OK: dry-run de equidade validado; nenhuma linha Gold gravada", flush=True)
    else:
        print(f"GOLD: dim_area inseridas_ou_alteradas={resultado['alteradas_area']} | "
              f"fact_equidade_area inseridas_ou_alteradas={resultado['alteradas_equidade']}", flush=True)
        print("OK: areas e equidade concluidas", flush=True)


if __name__ == "__main__":
    main()
