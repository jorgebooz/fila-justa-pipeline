"""Previsão Open-Meteo para os 96 distritos oficiais de São Paulo -> bronze.raw_clima.

Uso no Airflow:
    python /opt/airflow/include/ingest/clima.py

Pré-requisitos:
  - bronze.raw_ibge_area carregada (96 distritos)
  - include/seeds/pontos_clima_ibge_sp.csv gerado por scripts/gerar_pontos_clima.py

Sem dados inventados. Usa forecasts dos próximos 48 horários a partir da hora atual UTC.
Salva snapshot de consulta arredondado à hora UTC; executar duas vezes na mesma
hora não duplica registros. Nova hora gera novo snapshot auditável.

limitação: previsão por ponto representativo de distrito não captura microclimas.
Evolução: grade espacial e ponderação espacial das células da previsão.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
SEED = RAIZ / "include/seeds/pontos_clima_ibge_sp.csv"
URL = "https://api.open-meteo.com/v1/forecast"

SQL_UPSERT = """
INSERT INTO bronze.raw_clima (
    area_id, hora_raw, precip_mm_raw, latitude_grade, longitude_grade,
    consultado_em, _source, _run_id
) VALUES (%s, %s, %s, %s, %s, %s, 'OPEN_METEO', %s)
ON CONFLICT (area_id, hora_raw, consultado_em) DO UPDATE SET
    precip_mm_raw = EXCLUDED.precip_mm_raw,
    latitude_grade = EXCLUDED.latitude_grade,
    longitude_grade = EXCLUDED.longitude_grade,
    _run_id = EXCLUDED._run_id,
    _ingested_at = NOW()
WHERE (
    bronze.raw_clima.precip_mm_raw,
    bronze.raw_clima.latitude_grade,
    bronze.raw_clima.longitude_grade
) IS DISTINCT FROM (
    EXCLUDED.precip_mm_raw,
    EXCLUDED.latitude_grade,
    EXCLUDED.longitude_grade
)
"""


def carregar_pontos(caminho=SEED):
    with Path(caminho).open(encoding="utf-8-sig", newline="") as f:
        linhas = list(csv.DictReader(f))
    if len(linhas) != 96 or len({r["area_id"] for r in linhas}) != 96:
        raise ValueError("Seed de pontos deve conter exatamente 96 distritos distintos")
    saida = []
    for linha in linhas:
        codigo = linha["area_id"]
        lat, lon = float(linha["latitude"]), float(linha["longitude"])
        if len(codigo) != 9 or not codigo.isdigit():
            raise ValueError(f"Código inválido: {codigo!r}")
        if not (-24.2 < lat < -23.2 and -47.3 < lon < -46.1):
            raise ValueError(f"Coordenada fora de SP: {codigo} {lat} {lon}")
        saida.append((codigo, lat, lon))
    return sorted(saida)


def montar_url(pontos):
    args = {
        "latitude": ",".join(str(p[1]) for p in pontos),
        "longitude": ",".join(str(p[2]) for p in pontos),
        "hourly": "precipitation",
        "forecast_hours": 48,
        "timezone": "GMT",
        "timeformat": "unixtime",
        "precipitation_unit": "mm",
    }
    return URL + "?" + urllib.parse.urlencode(args)


def requisitar(url, tentativas=3):
    ultimo_erro = None
    for i in range(tentativas):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "fila-justa-academico/1.0"})
            with urllib.request.urlopen(req, timeout=90) as resposta:
                return json.load(resposta)
        except (urllib.error.HTTPError, urllib.error.URLError, OSError, TimeoutError, ValueError) as erro:
            ultimo_erro = erro
            if i < tentativas - 1:
                time.sleep(2 ** i)
    raise RuntimeError("Falha ao consultar Open-Meteo após tentativas") from ultimo_erro


def interpretar_resposta(pontos, payload, consultado_em, run_id):
    dados = payload if isinstance(payload, list) else [payload]
    if len(dados) != len(pontos):
        raise ValueError(f"Open-Meteo retornou {len(dados)} locais; esperado {len(pontos)}")
    linhas = []
    for (area_id, _, _), local in zip(pontos, dados):
        horas = local.get("hourly", {}).get("time", [])
        valores = local.get("hourly", {}).get("precipitation", [])
        unidades = local.get("hourly_units", {})
        if unidades.get("precipitation") != "mm":
            raise ValueError(f"Unidade de precipitação inesperada para {area_id}")
        if len(horas) != 48 or len(valores) != 48:
            raise ValueError(f"Forecast incompleto em {area_id}: {len(horas)} horários")
        lat_grade = local.get("latitude")
        lon_grade = local.get("longitude")
        if lat_grade is None or lon_grade is None:
            raise ValueError(f"Latitude/longitude da grade não disponíveis em {area_id}")
        anteriores = None
        for hora, valor in zip(horas, valores):
            if not isinstance(hora, (int, float)) or not math.isfinite(hora):
                raise ValueError(f"Hora Unix inválida em {area_id}")
            if anteriores is not None and hora - anteriores != 3600:
                raise ValueError(f"Horas não consecutivas em {area_id}")
            anteriores = hora
            if valor is None or not isinstance(valor, (int, float)) or not math.isfinite(valor) or valor < 0:
                raise ValueError(f"Precipitação inválida/ausente em {area_id}: {valor!r}")
            instante = datetime.fromtimestamp(hora, timezone.utc).isoformat()
            linhas.append((area_id, instante, str(valor), lat_grade, lon_grade, consultado_em, run_id))
    return linhas


def validar_distritos_bronze(cursor, pontos):
    cursor.execute("SELECT codigo_distrito_raw FROM bronze.raw_ibge_area WHERE codigo_municipio_raw='3550308'")
    codigos = [r[0] for r in cursor.fetchall()]
    if len(codigos) != 96 or set(codigos) != {p[0] for p in pontos}:
        raise RuntimeError("Códigos no IBGE bronze divergem dos 96 pontos da malha; pare antes de carregar o clima")


def carregar(conexao, pontos, linhas, consultado_em):
    # Lida com falhas sem deixar apenas parte dos distritos em um snapshot.
    with conexao.transaction():
        with conexao.cursor() as cursor:
            validar_distritos_bronze(cursor, pontos)
            cursor.executemany(SQL_UPSERT, linhas)
            alteradas = cursor.rowcount
            cursor.execute(
                "SELECT COUNT(*), COUNT(DISTINCT area_id) FROM bronze.raw_clima WHERE consultado_em=%s",
                (consultado_em,),
            )
            total, areas = cursor.fetchone()
            esperado = len(pontos) * 48
            if (total, areas) != (esperado, len(pontos)):
                raise RuntimeError(
                    f"Snapshot climático incompleto: linhas={total}/{esperado}, áreas={areas}/{len(pontos)}"
                )
    return alteradas, total, areas


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingestão Open-Meteo por distrito SP")
    parser.add_argument("--run-id", help="Identificação da rodada")
    args = parser.parse_args(argv)
    pontos = carregar_pontos()
    agora = datetime.now(timezone.utc)
    consultado_em = agora.replace(minute=0, second=0, microsecond=0)
    run_id = args.run_id or agora.strftime("manual_clima_%Y%m%dT%H%M%SZ")
    print(f"Open-Meteo: consultando {len(pontos)} distritos | snapshot={consultado_em.isoformat()}", flush=True)
    dados = []
    for inicio in range(0, len(pontos), 12):
        grupo = pontos[inicio:inicio+12]
        resultado = requisitar(montar_url(grupo))
        novas = interpretar_resposta(grupo, resultado, consultado_em, run_id)
        dados.extend(novas)
        print(f"  Consultados {inicio + len(grupo)}/{len(pontos)} distritos", flush=True)
    if len(dados) != len(pontos) * 48:
        raise RuntimeError("Resposta climática incompleta")
    dsn = os.environ.get("AIRFLOW_CONN_WAREHOUSE")
    if not dsn:
        raise RuntimeError("AIRFLOW_CONN_WAREHOUSE ausente")
    import psycopg
    with psycopg.connect(dsn) as conexao:
        alteradas, total, areas = carregar(conexao, pontos, dados, consultado_em)
    print(f"CLIMA: inseridas_ou_alteradas={alteradas} | bronze={total} | áreas={areas}", flush=True)
    print("OK: ingestão climática concluída", flush=True)


if __name__ == "__main__":
    main()
