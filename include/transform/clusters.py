"""Passo 7 - clusters de ocorrencias Silver, idempotentes e auditaveis.

Requisitos: Silver carregada, PostgreSQL acessivel via AIRFLOW_CONN_WAREHOUSE.
Nao altera silver.ocorrencia_limpa nem as tabelas bronze/gold.
A janela e ancorada no primeiro relato do grupo; algoritmos de lacunas
consecutivas poderiam juntar relatos separados por meses.

limitação: grupos usam o mesmo tipo e a mesma celula H3, sem considerar
celulas vizinhas; futuras alteracoes na Silver podem mudar os IDs dos grupos.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WEIGHTS = ROOT / "include/config/weights.yml"
SQL_FILE = ROOT / "include/sql/05_clusters.sql"

SQL_ORDENAR = """
CREATE TEMP TABLE stage_clusters_ordenados ON COMMIT DROP AS
SELECT ocorrencia_id, tipo_padrao, h3, aberta_em,
       row_number() OVER (
           PARTITION BY tipo_padrao, h3
           ORDER BY aberta_em, ocorrencia_id
       ) AS ordem
FROM silver.ocorrencia_limpa
"""

SQL_UPSERT = """
INSERT INTO silver.cluster_ocorrencia (ocorrencia_id, cluster_id, run_id)
SELECT ocorrencia_id, cluster_id, %s
FROM stage_clusters_calculados
WHERE true
ON CONFLICT (ocorrencia_id) DO UPDATE
SET cluster_id = EXCLUDED.cluster_id, run_id = EXCLUDED.run_id
WHERE silver.cluster_ocorrencia.cluster_id IS DISTINCT FROM EXCLUDED.cluster_id
"""

SQL_DELETE_STALE = """
DELETE FROM silver.cluster_ocorrencia c
WHERE NOT EXISTS (
    SELECT 1 FROM stage_clusters_calculados s
    WHERE s.ocorrencia_id = c.ocorrencia_id
)
"""

SQL_AUDITAR = """
SELECT COUNT(*)
FROM (
    SELECT cluster_id
    FROM stage_clusters_calculados
    GROUP BY cluster_id
    HAVING COUNT(DISTINCT tipo_padrao) <> 1
        OR COUNT(DISTINCT h3) <> 1
        OR MAX(aberta_em) - MIN(aberta_em) > (%s::int * interval '1 day')
        OR COUNT(DISTINCT ancora_id) <> 1
) erros
"""

SQL_APP_DIVIDIDOS = """
SELECT COUNT(*) FROM (
    SELECT regexp_replace(o.ocorrencia_id, '-[0-9]{2}$', '') AS grupo
    FROM silver.ocorrencia_limpa o
    JOIN stage_clusters_calculados s USING (ocorrencia_id)
    WHERE o.canal = 'APP_SIMULADO'
      AND o.ocorrencia_id LIKE 'APP:APP26-%'
    GROUP BY 1
    HAVING COUNT(DISTINCT s.cluster_id) > 1
) divisao
"""


def ler_janela(caminho=WEIGHTS):
    """Le apenas a chave do arquivo YAML plano; sem dependencia PyYAML."""
    for linha in Path(caminho).read_text(encoding="utf-8").splitlines():
        texto = linha.split("#", 1)[0].strip()
        if not texto or ":" not in texto:
            continue
        chave, valor = texto.split(":", 1)
        if chave.strip() == "cluster_janela_dias":
            try:
                dias = int(valor.strip().strip("'\""))
            except ValueError as erro:
                raise ValueError("cluster_janela_dias deve ser inteiro") from erro
            if not 1 <= dias <= 30:
                raise ValueError("cluster_janela_dias deve estar entre 1 e 30")
            return dias
    raise ValueError("cluster_janela_dias nao encontrado em weights.yml")


def clusterizar_referencia(registros, dias=7):
    """Modelo PURO para testes do agrupamento SQL.

    registros: tuplas (ocorrencia_id, tipo_padrao, h3, datetime timezone-aware).
    Retorna dict de ocorrencia_id para ID de cluster deterministico.
    """
    if not 1 <= dias <= 30:
        raise ValueError("janela deve ficar entre 1 e 30 dias")
    vistos = set()
    grupos = {}
    for identificador, tipo, h3, instante in registros:
        if identificador in vistos:
            raise ValueError("ocorrencia_id duplicado na entrada")
        vistos.add(identificador)
        if not isinstance(instante, datetime) or instante.tzinfo is None or instante.utcoffset() is None:
            raise ValueError("aberta_em precisa ter fuso horario")
        grupos.setdefault((tipo, h3), []).append((instante, identificador))
    saida = {}
    for (tipo, h3), eventos in grupos.items():
        ancora_id = None
        ancora_em = None
        for instante, identificador in sorted(eventos):
            if ancora_em is None or instante > ancora_em + timedelta(days=dias):
                ancora_id, ancora_em = identificador, instante
            chave = f"{tipo}:{h3}:{ancora_id}"
            cid = "cl_" + hashlib.md5(chave.encode("utf-8")).hexdigest()[:24]
            saida[identificador] = cid
    return saida


def executar(conexao, run_id, dias, dry_run=False):
    if not run_id or len(run_id) > 200:
        raise ValueError("run_id invalido")
    if not 1 <= dias <= 30:
        raise ValueError("janela invalida")

    with conexao.transaction():
        with conexao.cursor() as cur:
            # Evita duas reconciliações concorrentes da mesma tabela.
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('fila_justa_clusters'))")
            # CTAS no PostgreSQL nao aceita parametros vinculados diretamente;
            # usa SET LOCAL via set_config com valor validado como inteiro.
            cur.execute("SELECT set_config('fila_justa.cluster_janela_dias', %s, true)", (str(dias),))
            cur.execute(SQL_ORDENAR)
            cur.execute("""
                CREATE UNIQUE INDEX idx_stage_clusters_ordem
                ON stage_clusters_ordenados (tipo_padrao, h3, ordem)
            """)
            cur.execute("ANALYZE stage_clusters_ordenados")
            sql = SQL_FILE.read_text(encoding="utf-8")
            cur.execute(sql)
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT cluster_id) FROM stage_clusters_calculados")
            total, clusters = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM silver.ocorrencia_limpa")
            silver = cur.fetchone()[0]
            if total == 0 or total != silver:
                raise RuntimeError(f"Stage={total}; Silver={silver}: nenhuma linha pode desaparecer")
            cur.execute(SQL_AUDITAR, (dias,))
            if cur.fetchone()[0]:
                raise RuntimeError("Clusters violam tipo, H3, ancora ou janela de dias")
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT cluster_id
                    FROM stage_clusters_calculados
                    GROUP BY cluster_id
                    HAVING COUNT(DISTINCT (tipo_padrao, h3, ancora_id)) <> 1
                ) colisoes
            """)
            if cur.fetchone()[0]:
                raise RuntimeError("Colisao no identificador de cluster")
            cur.execute(SQL_APP_DIVIDIDOS)
            app_divididos = cur.fetchone()[0]
            if app_divididos:
                raise RuntimeError(f"{app_divididos} grupos simulados foram divididos entre clusters")
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT cluster_id FROM stage_clusters_calculados
                    GROUP BY cluster_id HAVING COUNT(*) > 1
                ) x
            """)
            multiplos = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*)
                FROM silver.ocorrencia_limpa o
                JOIN stage_clusters_calculados s USING (ocorrencia_id)
                WHERE o.canal='APP_SIMULADO'
            """)
            app = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT cluster_id FROM stage_clusters_calculados s
                    JOIN silver.ocorrencia_limpa o USING(ocorrencia_id)
                    WHERE o.canal='APP_SIMULADO'
                    GROUP BY cluster_id
                ) x
            """)
            clusters_app = cur.fetchone()[0]
            alteradas = removidas = 0
            if not dry_run:
                cur.execute(SQL_UPSERT, (run_id,))
                alteradas = cur.rowcount
                cur.execute(SQL_DELETE_STALE)
                removidas = cur.rowcount
                cur.execute("SELECT COUNT(*) FROM silver.cluster_ocorrencia")
                gravados = cur.fetchone()[0]
                if gravados != total:
                    raise RuntimeError(f"Vinculos gravados={gravados}; esperado={total}")

    return {
        "silver": total, "clusters": clusters, "multiplos": multiplos,
        "app": app, "clusters_app": clusters_app,
        "app_divididos": app_divididos, "alteradas": alteradas,
        "removidas": removidas, "janela": dias, "dry_run": dry_run,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Passo 7: deduplicar Silver em clusters por H3")
    parser.add_argument("--run-id", default="clusters_2026")
    parser.add_argument("--dry-run", action="store_true", help="Calcula e valida sem gravar")
    args = parser.parse_args(argv)
    dias = ler_janela()
    import psycopg
    dsn = os.environ.get("AIRFLOW_CONN_WAREHOUSE")
    if not dsn:
        raise SystemExit("AIRFLOW_CONN_WAREHOUSE nao configurada")
    with psycopg.connect(dsn) as conexao:
        resumo = executar(conexao, args.run_id, dias, args.dry_run)
    print(f"Clusters: janela={resumo['janela']} dias | Silver={resumo['silver']} | "
          f"clusters={resumo['clusters']} | clusters_com_mais_de_um_relato={resumo['multiplos']}", flush=True)
    print(f"APP_SIMULADO: relatos={resumo['app']} | clusters_com_app={resumo['clusters_app']} | "
          f"grupos_divididos={resumo['app_divididos']}", flush=True)
    if args.dry_run:
        print("OK: dry-run validado; nenhuma linha de cluster foi gravada", flush=True)
    else:
        print(f"CLUSTERS: inseridas_ou_alteradas={resumo['alteradas']} | "
              f"removidas={resumo['removidas']} | total={resumo['silver']}", flush=True)
        print("OK: deduplicacao concluida", flush=True)


if __name__ == "__main__":
    main()
