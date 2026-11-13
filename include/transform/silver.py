"""Passo 6: bronze -> silver, padronização auditável e H3 (resolução 9).

Run no container Airflow, após preencher bronze. Filtra apenas serviços mapeados e
ocorrências abertas/em andamento. A maior parte do ELT está em SQL no warehouse;
Python é usado apenas para leitura dos seeds e cálculo H3.

limitação: SP156 não traz ID estável da solicitação nem geometria territorial por
registro; usamos (arquivo, linha) como identificador e o nome do distrito.
Códigos numéricos e distritos ausentes são descartados e contabilizados.
Apenas a caixa geográfica é validada, não há point-in-polygon nesta etapa.
"""
from __future__ import annotations

import argparse
import csv
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAPA = ROOT / "include/seeds/mapa_distritos_ibge_sp.csv"
SERVICOS = ROOT / "include/seeds/servicos_interesse.csv"
TIPOS = ROOT / "include/seeds/dim_tipo.csv"
SQL = ROOT / "include/sql/04_silver_padronizacao.sql"
PESOS = ROOT / "include/config/weights.yml"

SQL_CREATE_SEEDS = (
    "CREATE TEMP TABLE stage_seed_servicos (servico TEXT PRIMARY KEY, categoria TEXT NOT NULL, incluir BOOLEAN NOT NULL) ON COMMIT DROP",
    "CREATE TEMP TABLE stage_seed_tipos (tipo_padrao TEXT PRIMARY KEY) ON COMMIT DROP",
    "CREATE TEMP TABLE stage_seed_distritos (area_id TEXT PRIMARY KEY, nome_normalizado TEXT NOT NULL UNIQUE) ON COMMIT DROP",
)

SQL_STAGE_H3 = """
CREATE TEMP TABLE stage_silver_h3 (
    ocorrencia_id TEXT PRIMARY KEY, canal TEXT NOT NULL, tipo_padrao TEXT NOT NULL,
    lat NUMERIC(10,7) NOT NULL, lon NUMERIC(10,7) NOT NULL, h3 TEXT NOT NULL,
    area_id TEXT NOT NULL, aberta_em TIMESTAMPTZ NOT NULL, status TEXT NOT NULL
) ON COMMIT DROP
"""

SQL_UPSERT = """
INSERT INTO silver.ocorrencia_limpa (
    ocorrencia_id, canal, municipio_cod, tipo_padrao, lat, lon, h3,
    area_id, aberta_em, status, run_id
)
SELECT ocorrencia_id, canal, '3550308', tipo_padrao, lat, lon, h3,
       area_id, aberta_em, status, %s
FROM stage_silver_h3
WHERE true
ON CONFLICT (ocorrencia_id) DO UPDATE SET
    canal = EXCLUDED.canal,
    tipo_padrao = EXCLUDED.tipo_padrao,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon,
    h3 = EXCLUDED.h3,
    area_id = EXCLUDED.area_id,
    aberta_em = EXCLUDED.aberta_em,
    status = EXCLUDED.status,
    run_id = EXCLUDED.run_id
WHERE (
    silver.ocorrencia_limpa.canal,
    silver.ocorrencia_limpa.tipo_padrao,
    silver.ocorrencia_limpa.lat,
    silver.ocorrencia_limpa.lon,
    silver.ocorrencia_limpa.h3,
    silver.ocorrencia_limpa.area_id,
    silver.ocorrencia_limpa.aberta_em,
    silver.ocorrencia_limpa.status
) IS DISTINCT FROM (
    EXCLUDED.canal, EXCLUDED.tipo_padrao, EXCLUDED.lat, EXCLUDED.lon,
    EXCLUDED.h3, EXCLUDED.area_id, EXCLUDED.aberta_em, EXCLUDED.status
)
"""

SQL_DELETE_STALE_CLUSTER = """
DELETE FROM silver.cluster_ocorrencia c
USING silver.ocorrencia_limpa o
WHERE c.ocorrencia_id = o.ocorrencia_id
  AND o.canal IN ('SP156', 'APP_SIMULADO')
  AND NOT EXISTS (
      SELECT 1 FROM stage_silver_h3 s WHERE s.ocorrencia_id = o.ocorrencia_id
  )
"""
SQL_DELETE_STALE = """
DELETE FROM silver.ocorrencia_limpa o
WHERE o.canal IN ('SP156', 'APP_SIMULADO')
  AND NOT EXISTS (
      SELECT 1 FROM stage_silver_h3 s WHERE s.ocorrencia_id = o.ocorrencia_id
  )
"""


def ler_seeds():
    with SERVICOS.open(encoding="utf-8-sig", newline="") as arquivo:
        servicos = [(r["servico"].strip(), r["categoria"].strip(),
                     r["incluir"].strip().lower() == "sim")
                    for r in csv.DictReader(arquivo)]
    with TIPOS.open(encoding="utf-8-sig", newline="") as arquivo:
        tipos = [r["tipo_padrao"].strip() for r in csv.DictReader(arquivo)]
    with MAPA.open(encoding="utf-8-sig", newline="") as arquivo:
        distritos = [(r["area_id"].strip(), r["distrito_nome_normalizado"].strip())
                     for r in csv.DictReader(arquivo) if r["municipio_cod"].strip() == "3550308"]

    if len(distritos) != 96 or len(set(distritos)) != 96 or len({r[0] for r in distritos}) != 96:
        raise ValueError("Mapa IBGE: esperado exatamente 96 distritos distintos")
    if len({r[1] for r in distritos}) != 96:
        raise ValueError("Mapa IBGE: nomes normalizados não únicos")
    if not servicos or len(servicos) != len({r[0] for r in servicos}):
        raise ValueError("Seed de serviços ausente ou com nomes repetidos")
    if any(r[1] not in tipos for r in servicos if r[2]):
        raise ValueError("Serviço ativo com categoria ausente de dim_tipo.csv")
    if len(tipos) != len(set(tipos)):
        raise ValueError("Tipos duplicados no seed")
    return servicos, tipos, distritos


def validar_resolucao(resolucao):
    if not isinstance(resolucao, int) or not 0 <= resolucao <= 15:
        raise ValueError("Resolução H3 inválida (intervalo: 0 a 15)")
    return resolucao


def ler_resolucao_h3():
    # Leitura mínima de uma chave escalar do YAML, sem acrescentar PyYAML.
    for linha in PESOS.read_text(encoding="utf-8").splitlines():
        limpa = linha.split("#", 1)[0].strip()
        if limpa.startswith("h3_resolucao:"):
            return validar_resolucao(int(limpa.split(":", 1)[1].strip()))
    raise ValueError("Configuração h3_resolucao ausente em weights.yml")


def calcular_h3(lat, lon, h3_modulo, resolucao=9):
    validar_resolucao(resolucao)
    lat, lon = float(lat), float(lon)
    if not (-24.1 <= lat <= -23.3 and -47.2 <= lon <= -46.2):
        raise ValueError("Coordenada fora dos limites da Silver")
    codigo = h3_modulo.latlng_to_cell(lat, lon, resolucao)
    if not h3_modulo.is_valid_cell(codigo):
        raise ValueError("Biblioteca H3 retornou célula inválida")
    return codigo


def carregar_seeds(cur, servicos, tipos, distritos):
    for ddl in SQL_CREATE_SEEDS:
        cur.execute(ddl)
    with cur.copy("COPY stage_seed_servicos (servico,categoria,incluir) FROM STDIN") as cp:
        for linha in servicos:
            cp.write_row(linha)
    with cur.copy("COPY stage_seed_tipos (tipo_padrao) FROM STDIN") as cp:
        for tipo in tipos:
            cp.write_row((tipo,))
    with cur.copy("COPY stage_seed_distritos (area_id,nome_normalizado) FROM STDIN") as cp:
        for linha in distritos:
            cp.write_row(linha)


def executar(conexao, h3_modulo, run_id="silver_2026"):
    if not run_id or len(run_id) > 100:
        raise ValueError("run_id obrigatório, com até 100 caracteres")
    servicos, tipos, distritos = ler_seeds()
    resolucao = ler_resolucao_h3()
    with conexao.transaction():
        with conexao.cursor() as cur:
            carregar_seeds(cur, servicos, tipos, distritos)
            cur.execute(SQL.read_text(encoding="utf-8"))
            cur.execute("SELECT canal, motivo, COUNT(*) FROM stage_silver_padronizada WHERE motivo IS NOT NULL GROUP BY canal, motivo ORDER BY canal, motivo")
            descartes = {(c, m): qtd for c, m, qtd in cur.fetchall()}

            cur.execute("SELECT canal, COUNT(*) FROM stage_silver_padronizada WHERE motivo IS NULL GROUP BY canal ORDER BY canal")
            validos_por_canal = dict(cur.fetchall())
            if not validos_por_canal.get("APP_SIMULADO") or not validos_por_canal.get("SP156"):
                raise RuntimeError(f"Canal sem registros válidos; verifique seeds, datas e filtros: {validos_por_canal}")

            cur.execute(SQL_STAGE_H3)
            # Computa H3 em Python. CSV temporário mantém uso de memória pequeno.
            with tempfile.TemporaryFile(mode="w+t", encoding="utf-8", newline="") as temporario:
                escritor = csv.writer(temporario)
                cur.execute("""
                    SELECT ocorrencia_id, canal, tipo_padrao, lat, lon,
                           area_id, aberta_em, status
                    FROM stage_silver_padronizada
                    WHERE motivo IS NULL
                    ORDER BY ocorrencia_id
                """)
                total = 0
                while True:
                    lote = cur.fetchmany(10000)
                    if not lote:
                        break
                    for ocorrencia_id, canal, tipo, lat, lon, area, aberta_em, status in lote:
                        h3 = calcular_h3(lat, lon, h3_modulo, resolucao)
                        escritor.writerow((ocorrencia_id, canal, tipo, lat, lon, h3,
                                          area, aberta_em.isoformat(), status))
                        total += 1
                if total != sum(validos_por_canal.values()):
                    raise RuntimeError("Total H3 diverge do total validado em SQL")
                temporario.flush()
                temporario.seek(0)
                with cur.copy("COPY stage_silver_h3 (ocorrencia_id,canal,tipo_padrao,lat,lon,h3,area_id,aberta_em,status) FROM STDIN WITH (FORMAT CSV)") as cp:
                    while True:
                        bloco = temporario.read(131072)
                        if not bloco:
                            break
                        cp.write(bloco)

            cur.execute("SELECT COUNT(*) FROM stage_silver_h3")
            if cur.fetchone()[0] != total:
                raise RuntimeError("COPY dos registros H3 não corresponde ao SQL")
            cur.execute(SQL_UPSERT, (run_id,))
            alteradas = cur.rowcount
            cur.execute(SQL_DELETE_STALE_CLUSTER)
            vinculos_removidos = cur.rowcount
            cur.execute(SQL_DELETE_STALE)
            removidas = cur.rowcount

            cur.execute("DELETE FROM silver.descarte_ocorrencia WHERE run_id = %s AND canal IN ('SP156', 'APP_SIMULADO')", (run_id,))
            for (canal, motivo), quantidade in sorted(descartes.items()):
                cur.execute("""
                    INSERT INTO silver.descarte_ocorrencia (run_id,canal,motivo,quantidade)
                    VALUES (%s,%s,%s,%s)
                """, (run_id, canal, motivo, quantidade))

            cur.execute("SELECT COUNT(*) FROM silver.ocorrencia_limpa WHERE canal IN ('SP156','APP_SIMULADO')")
            total_silver = cur.fetchone()[0]
            if total_silver != total:
                raise RuntimeError(f"Silver tem {total_silver}, esperado {total}. Operação revertida.")

    return {"validos": validos_por_canal, "descartes": descartes, "h3_resolucao": resolucao,
            "inseridas_ou_alteradas": alteradas, "removidas": removidas,
            "vinculos_removidos": vinculos_removidos, "total_silver": total}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Padronização Silver SP156+APP com H3")
    parser.add_argument("--run-id", default="silver_2026")
    args = parser.parse_args(argv)
    try:
        import h3
    except ImportError as erro:
        raise SystemExit("Dependência H3 ausente. Siga docs/VALIDACAO_PASSO6_SILVER.md para construir a imagem Airflow com h3==4.5.0") from erro
    import psycopg
    dsn = os.environ.get("AIRFLOW_CONN_WAREHOUSE")
    if not dsn:
        raise SystemExit("AIRFLOW_CONN_WAREHOUSE não configurada no Airflow")
    with psycopg.connect(dsn) as conexao:
        resultado = executar(conexao, h3, args.run_id)
    print(f"H3: resolução={resultado['h3_resolucao']}", flush=True)
    for canal, qtd in sorted(resultado["validos"].items()):
        print(f"Silver {canal}: válidas={qtd}", flush=True)
    for (canal, motivo), qtd in sorted(resultado["descartes"].items()):
        print(f"DESCARTE {canal} | {motivo}: {qtd}", flush=True)
    print(f"SILVER: inseridas_ou_alteradas={resultado['inseridas_ou_alteradas']} | "
          f"removidas={resultado['removidas']} | "
          f"total={resultado['total_silver']}", flush=True)
    print("OK: padronização Silver concluída", flush=True)


if __name__ == "__main__":
    main()
