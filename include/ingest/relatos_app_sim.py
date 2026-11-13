"""Passo 5: gera relatos SIMULADOS e carrega bronze.raw_relatos_app.

Uso dentro do Airflow scheduler:
    python -u /opt/airflow/include/ingest/relatos_app_sim.py --dry-run
    python -u /opt/airflow/include/ingest/relatos_app_sim.py

Sem internet e sem dados pessoais. Usa os pontos distritais públicos da malha
IBGE já gerados para o clima. Não substitui ou altera os conectores reais.

limitação: os pontos representam uma área, não locais reais de incidentes;
o deslocamento sintético ao redor de cada ponto não é testado contra a malha
poligonal. Evolução: gerar pontos internos por teste point-in-polygon.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
PONTOS = RAIZ / "include/seeds/pontos_clima_ibge_sp.csv"
MAPA = RAIZ / "include/seeds/mapa_distritos_ibge_sp.csv"

TIPOS = ("alagamento", "bueiro_entupido", "buraco", "arvore_caida", "entulho")
STATUS = ("Aberto", "Em Andamento")
SEED_PADRAO = 2026
DATA_BASE_PADRAO = "2026-11-11T12:00:00-03:00"

CAMPOS = (
    "relato_id", "tipo_raw", "latitude_raw", "longitude_raw",
    "area_id_raw", "aberta_em_raw", "status_raw", "_source", "_run_id",
)

SQL_STAGE = """
CREATE TEMP TABLE stage_relatos_app (
    relato_id TEXT NOT NULL,
    tipo_raw TEXT NOT NULL,
    latitude_raw TEXT,
    longitude_raw TEXT,
    area_id_raw TEXT,
    aberta_em_raw TEXT,
    status_raw TEXT,
    _source TEXT NOT NULL,
    _run_id TEXT NOT NULL
) ON COMMIT DROP
"""

# Atualização somente se o payload mudou; _run_id/_ingested_at permanecem
# intactos em uma segunda execução com os mesmos parâmetros.
SQL_UPSERT = """
INSERT INTO bronze.raw_relatos_app (
    relato_id, tipo_raw, latitude_raw, longitude_raw, area_id_raw,
    aberta_em_raw, status_raw, _source, _run_id
)
SELECT
    relato_id, tipo_raw, latitude_raw, longitude_raw, area_id_raw,
    aberta_em_raw, status_raw, _source, _run_id
FROM stage_relatos_app
WHERE true
ON CONFLICT (relato_id) DO UPDATE SET
    tipo_raw = EXCLUDED.tipo_raw,
    latitude_raw = EXCLUDED.latitude_raw,
    longitude_raw = EXCLUDED.longitude_raw,
    area_id_raw = EXCLUDED.area_id_raw,
    aberta_em_raw = EXCLUDED.aberta_em_raw,
    status_raw = EXCLUDED.status_raw,
    _source = EXCLUDED._source,
    _run_id = EXCLUDED._run_id,
    _ingested_at = NOW()
WHERE (
    bronze.raw_relatos_app.tipo_raw,
    bronze.raw_relatos_app.latitude_raw,
    bronze.raw_relatos_app.longitude_raw,
    bronze.raw_relatos_app.area_id_raw,
    bronze.raw_relatos_app.aberta_em_raw,
    bronze.raw_relatos_app.status_raw,
    bronze.raw_relatos_app._source
) IS DISTINCT FROM (
    EXCLUDED.tipo_raw,
    EXCLUDED.latitude_raw,
    EXCLUDED.longitude_raw,
    EXCLUDED.area_id_raw,
    EXCLUDED.aberta_em_raw,
    EXCLUDED.status_raw,
    EXCLUDED._source
)
"""


def validar_data_base(texto: str) -> datetime:
    try:
        data_base = datetime.fromisoformat(texto)
    except ValueError as erro:
        raise ValueError("--data-base deve ser ISO 8601, com fuso; exemplo: 2026-11-11T12:00:00-03:00") from erro
    if data_base.tzinfo is None or data_base.utcoffset() is None:
        raise ValueError("--data-base deve incluir o fuso -03:00 ou equivalente")
    if data_base.year != 2026:
        raise ValueError("Nesta etapa, a simulação deve estar no ano de 2026")
    return data_base


def carregar_pontos(caminho=PONTOS, caminho_mapa=MAPA):
    """Lista ordenada de (area_id, lat, lon). Valida contra códigos IBGE."""
    with Path(caminho_mapa).open(encoding="utf-8-sig", newline="") as f:
        mapa = list(csv.DictReader(f))
    codigos = {r["area_id"] for r in mapa if r["municipio_cod"] == "3550308"}
    if len(codigos) != 96 or len(mapa) != 96:
        raise ValueError("Mapa de distritos IBGE deve ter 96 códigos únicos de São Paulo")

    with Path(caminho).open(encoding="utf-8-sig", newline="") as f:
        registros = list(csv.DictReader(f))
    if len(registros) != 96 or {r["area_id"] for r in registros} != codigos:
        raise ValueError("Pontos climáticos e mapa IBGE devem ter os mesmos 96 distritos")

    pontos = []
    for r in registros:
        area_id = r["area_id"]
        lat, lon = float(r["latitude"]), float(r["longitude"])
        if not (-24.1 <= lat <= -23.3 and -47.2 <= lon <= -46.2):
            raise ValueError(f"Ponto fora dos limites do modelo silver: {area_id}: {lat},{lon}")
        pontos.append((area_id, lat, lon))
    return sorted(pontos)


def sortear_faixas(area_ids, seed):
    """Divide 96 áreas em 32 de baixa, 32 média e 32 alta propensão."""
    if len(area_ids) != 96 or len(set(area_ids)) != 96:
        raise ValueError("A simulação requer 96 distritos únicos")
    ids = sorted(area_ids)
    random.Random(seed).shuffle(ids)
    return {area: ("baixa" if i < 32 else "media" if i < 64 else "alta")
            for i, area in enumerate(ids)}


def gerar_relatos(pontos, seed=SEED_PADRAO, data_base=DATA_BASE_PADRAO):
    """Gera (linhas, resumo) sem efeito colateral; mesmos argumentos = mesmas linhas."""
    base = validar_data_base(data_base) if isinstance(data_base, str) else data_base
    if base.tzinfo is None or base.year != 2026:
        raise ValueError("Data-base precisa conter fuso e pertencer a 2026")
    faixas = sortear_faixas([p[0] for p in pontos], seed)
    registros = []
    grupos = 0
    por_faixa = Counter()
    por_area = Counter()
    grupos_com_duplicatas = 0
    esperados = Counter()

    for area_id, lat, lon in sorted(pontos):
        # RNG por distrito: a ordem de leitura dos pontos não muda os relatos.
        digest = hashlib.sha256(f"{seed}:{area_id}".encode()).digest()
        rng = random.Random(int.from_bytes(digest[:8], "big"))
        faixa = faixas[area_id]
        if faixa == "baixa":
            n_grupos = rng.randint(1, 2)
            n_relatos = (1, 2)
        elif faixa == "media":
            n_grupos = rng.randint(2, 3)
            n_relatos = (2, 3)
        else:
            n_grupos = rng.randint(3, 5)
            n_relatos = (4, 6)
        grupos += n_grupos

        for grupo in range(n_grupos):
            tipo = TIPOS[rng.randrange(len(TIPOS))]
            # Mesmo ponto, mesmo tipo e intervalo de até 2h -> duplicatas
            # intencionais para testar agrupamento H3 + janela de 7 dias.
            # Eventos diferentes ficam separados por >=8 dias.
            lat_grupo = round(lat + rng.uniform(-0.00010, 0.00010), 7)
            lon_grupo = round(lon + rng.uniform(-0.00010, 0.00010), 7)
            instante = base - timedelta(days=9 * grupo, hours=rng.randint(4, 12))
            quantidade = rng.randint(*n_relatos)
            if quantidade > 1:
                grupos_com_duplicatas += 1
            esperados[(area_id, grupo)] = quantidade
            for indice in range(quantidade):
                # O primeiro relato é o marcador temporal do grupo.
                instante_relato = instante + timedelta(minutes=rng.randint(0, 120))
                status = STATUS[rng.randrange(len(STATUS))]
                relato_id = f"APP26-{seed}-{area_id}-{grupo:02d}-{indice:02d}"
                registros.append((
                    relato_id, tipo, f"{lat_grupo:.7f}", f"{lon_grupo:.7f}",
                    area_id, instante_relato.isoformat(), status,
                    "APP_SIMULADO", "simulacao_2026",
                ))
                por_faixa[faixa] += 1
                por_area[area_id] += 1

    if len(registros) != len({r[0] for r in registros}):
        raise RuntimeError("IDs simulados duplicados")
    if len(por_area) != 96 or not all(r[1] in TIPOS for r in registros):
        raise RuntimeError("Cobertura dos distritos/tipos inválida")
    # Não aceitar data de ocorrência posterior ao instante de referência.
    if any(datetime.fromisoformat(r[5]) > base for r in registros):
        raise RuntimeError("Simulação produziu data futura relativa à data-base")
    resumo = {
        "total": len(registros),
        "areas": len(por_area),
        "grupos": grupos,
        "grupos_com_duplicatas": grupos_com_duplicatas,
        "baixa": por_faixa["baixa"],
        "media": por_faixa["media"],
        "alta": por_faixa["alta"],
        "min_por_area": min(por_area.values()),
        "max_por_area": max(por_area.values()),
        "tamanho_grupo": dict(esperados),
    }
    return registros, resumo


def carregar_no_postgres(conexao, registros, pontos, run_id):
    """Uma transação atômica; sem apagar dados de execuções anteriores."""
    with conexao.transaction():
        with conexao.cursor() as cur:
            cur.execute("""
                SELECT codigo_distrito_raw
                FROM bronze.raw_ibge_area
                WHERE codigo_municipio_raw = '3550308'
            """)
            ibge = [r[0] for r in cur.fetchall()]
            if len(ibge) != 96 or set(ibge) != {p[0] for p in pontos}:
                raise RuntimeError("IBGE bronze não corresponde aos 96 distritos simulados")

            cur.execute(SQL_STAGE)
            with cur.copy("COPY stage_relatos_app (" + ", ".join(CAMPOS) + ") FROM STDIN") as copia:
                for r in registros:
                    copia.write_row((*r[:-1], run_id))

            cur.execute("SELECT COUNT(*) FROM stage_relatos_app")
            if cur.fetchone()[0] != len(registros):
                raise RuntimeError("Quantidade da staging diferente do gerador")
            cur.execute(SQL_UPSERT)
            alteradas = cur.rowcount
            cur.execute("""
                SELECT COUNT(*)
                FROM bronze.raw_relatos_app AS b
                JOIN stage_relatos_app AS s USING (relato_id)
            """)
            total_carregado = cur.fetchone()[0]
            if total_carregado != len(registros):
                raise RuntimeError("Quantidade esperada não encontrada na bronze")
    return alteradas, total_carregado


def main(argv=None):
    ap = argparse.ArgumentParser(description="Gerador determinístico de relatos simulados (SP 2026)")
    ap.add_argument("--seed", type=int, default=SEED_PADRAO)
    ap.add_argument("--data-base", default=DATA_BASE_PADRAO,
                    help="ISO 8601 com fuso. Padrão: 2026-11-11T12:00:00-03:00")
    ap.add_argument("--dry-run", action="store_true", help="Gera e valida sem gravar na bronze")
    ap.add_argument("--run-id", default="simulacao_2026", help="Metadado da execução")
    args = ap.parse_args(argv)

    pontos = carregar_pontos()
    registros, resumo = gerar_relatos(pontos, args.seed, args.data_base)
    print(f"Simulação: distritos={resumo['areas']} | grupos={resumo['grupos']} | "
          f"relatos={resumo['total']} | grupos_duplicados={resumo['grupos_com_duplicatas']}", flush=True)
    print(f"Propensão a reportar: baixa={resumo['baixa']} | "
          f"média={resumo['media']} | alta={resumo['alta']}", flush=True)
    print(f"Relatos por distrito: min={resumo['min_por_area']}, max={resumo['max_por_area']} | "
          f"seed={args.seed} | data-base={args.data_base}", flush=True)
    if args.dry_run:
        print("OK: dry-run validado; nenhum registro foi gravado", flush=True)
        return

    import psycopg
    dsn = os.environ.get("AIRFLOW_CONN_WAREHOUSE")
    if not dsn:
        raise RuntimeError("AIRFLOW_CONN_WAREHOUSE não configurada")
    with psycopg.connect(dsn) as conexao:
        alteradas, total = carregar_no_postgres(conexao, registros, pontos, args.run_id)
    print(f"APP_SIMULADO: inseridas_ou_alteradas={alteradas} | bronze={total}", flush=True)
    print("OK: carga de relatos simulados concluída", flush=True)


if __name__ == "__main__":
    main()
