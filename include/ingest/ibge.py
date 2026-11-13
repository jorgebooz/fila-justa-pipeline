"""Ingestão dos 96 distritos de São Paulo — IBGE Censo 2022 -> bronze.raw_ibge_area.

Executar no airflow-scheduler:
  python /opt/airflow/include/ingest/ibge.py

O ZIP oficial é fixado pela data da publicação, garantindo reprodutibilidade.
Use --arquivo para processar uma cópia local do mesmo ZIP, sem novo download.

limitação: usa a revisão do arquivo básico de 20/05/2026 do Censo 2022;
para atualizar a revisão, alterar explicitamente a fonte e revalidar o esquema.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

URL_IBGE = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios/Agregados_por_Distrito_csv/"
    "Agregados_por_distritos_basico_BR_20260520.zip"
)
NOME_ZIP = "Agregados_por_distritos_basico_BR_20260520.zip"
NOME_CSV = "Agregados_por_distritos_basico_BR.csv"
COD_MUNICIPIO = "3550308"
COLUNAS_NECESSARIAS = {"CD_MUN", "CD_DIST", "NM_DIST", "v0001", "v0002", "v0007"}

SQL_UPSERT = """
INSERT INTO bronze.raw_ibge_area (
    arquivo_origem, codigo_distrito_raw, codigo_municipio_raw,
    nome_distrito_raw, populacao_raw, domicilios_raw,
    payload_publico, _source, _run_id
) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, 'IBGE_CENSO_2022', %s)
ON CONFLICT (arquivo_origem, codigo_distrito_raw) DO UPDATE SET
    codigo_municipio_raw = EXCLUDED.codigo_municipio_raw,
    nome_distrito_raw = EXCLUDED.nome_distrito_raw,
    populacao_raw = EXCLUDED.populacao_raw,
    domicilios_raw = EXCLUDED.domicilios_raw,
    payload_publico = EXCLUDED.payload_publico,
    _run_id = EXCLUDED._run_id,
    _ingested_at = now()
WHERE (
    bronze.raw_ibge_area.codigo_municipio_raw,
    bronze.raw_ibge_area.nome_distrito_raw,
    bronze.raw_ibge_area.populacao_raw,
    bronze.raw_ibge_area.domicilios_raw,
    bronze.raw_ibge_area.payload_publico
) IS DISTINCT FROM (
    EXCLUDED.codigo_municipio_raw,
    EXCLUDED.nome_distrito_raw,
    EXCLUDED.populacao_raw,
    EXCLUDED.domicilios_raw,
    EXCLUDED.payload_publico
)
"""


def obter_zip(arquivo: str | None) -> bytes:
    if arquivo:
        caminho = Path(arquivo)
        if caminho.name != NOME_ZIP:
            raise ValueError(f"Arquivo local deve ser {NOME_ZIP}")
        print(f"Lendo ZIP local: {caminho}", flush=True)
        return caminho.read_bytes()

    print(f"Baixando do IBGE: {URL_IBGE}", flush=True)
    req = urllib.request.Request(
        URL_IBGE,
        headers={"User-Agent": "fila-justa-dados-academico/1.0"},
    )
    # O arquivo oficial tem ~590 KB; download em memória, sem versionar dados brutos.
    with urllib.request.urlopen(req, timeout=120) as resposta:
        return resposta.read()


def extrair_distritos(conteudo_zip: bytes, esperado: int = 96) -> list[dict[str, str]]:
    with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as z:
        if NOME_CSV not in z.namelist():
            raise ValueError(f"CSV não encontrado no ZIP: {NOME_CSV}")
        with z.open(NOME_CSV) as stream:
            with io.TextIOWrapper(stream, encoding="latin-1", newline="") as arquivo:
                leitor = csv.DictReader(arquivo, delimiter=";")
                faltando = COLUNAS_NECESSARIAS - set(leitor.fieldnames or [])
                if faltando:
                    raise ValueError(f"Colunas IBGE ausentes: {sorted(faltando)}")

                distritos = []
                codigos = set()
                for numero, linha in enumerate(leitor, start=2):
                    if linha.get("CD_MUN") != COD_MUNICIPIO:
                        continue
                    cod = (linha["CD_DIST"] or "").strip()
                    if len(cod) != 9 or not cod.isdigit() or cod in codigos:
                        raise ValueError(f"Código de distrito inválido/repetido na linha {numero}: {cod!r}")
                    for coluna in ("v0001", "v0002", "v0007"):
                        valor = (linha[coluna] or "").strip()
                        if not valor.isdigit():
                            raise ValueError(f"{coluna} não numérico no distrito {cod}: {valor!r}")
                    if not (linha["NM_DIST"] or "").strip():
                        raise ValueError(f"Nome de distrito vazio: {cod}")
                    codigos.add(cod)
                    distritos.append(linha)

    if len(distritos) != esperado:
        raise ValueError(f"Esperados {esperado} distritos de São Paulo; encontrados {len(distritos)}")
    return sorted(distritos, key=lambda linha: linha["CD_DIST"])


def carregar(conexao, distritos: list[dict[str, str]], run_id: str):
    inseridas_ou_alteradas = 0
    with conexao.transaction():
        with conexao.cursor() as cursor:
            for linha in distritos:
                valores = (
                    NOME_ZIP,
                    linha["CD_DIST"],
                    linha["CD_MUN"],
                    linha["NM_DIST"],
                    linha["v0001"],
                    linha["v0002"],
                    json.dumps(linha, ensure_ascii=False, sort_keys=True),
                    run_id,
                )
                cursor.execute(SQL_UPSERT, valores)
                inseridas_ou_alteradas += cursor.rowcount

            cursor.execute(
                """SELECT COUNT(*), SUM(populacao_raw::bigint), SUM(domicilios_raw::bigint)
                   FROM bronze.raw_ibge_area WHERE arquivo_origem = %s""",
                (NOME_ZIP,),
            )
            no_banco, pop_banco, dom_banco = cursor.fetchone()
            populacao = sum(int(l["v0001"]) for l in distritos)
            domicilios = sum(int(l["v0002"]) for l in distritos)
            if (no_banco, pop_banco, dom_banco) != (len(distritos), populacao, domicilios):
                raise RuntimeError(
                    f"IBGE divergente: banco={(no_banco, pop_banco, dom_banco)} "
                    f"fonte={(len(distritos), populacao, domicilios)}"
                )
    return inseridas_ou_alteradas, len(distritos), populacao, domicilios


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingerir IBGE Censo 2022, distritos de São Paulo")
    parser.add_argument("--arquivo", help="ZIP local oficial (opcional; padrão: baixar do IBGE)")
    parser.add_argument("--run-id", help="Identificação da execução, para orquestração futura")
    args = parser.parse_args(argv)

    dsn = os.getenv("AIRFLOW_CONN_WAREHOUSE")
    if not dsn:
        raise RuntimeError("AIRFLOW_CONN_WAREHOUSE ausente no container")

    conteudo = obter_zip(args.arquivo)
    distritos = extrair_distritos(conteudo)
    print(
        f"Fonte: {len(distritos)} distritos | "
        f"população={sum(int(l['v0001']) for l in distritos)} | "
        f"domicílios={sum(int(l['v0002']) for l in distritos)}",
        flush=True,
    )

    import psycopg  # disponível na imagem do Airflow

    run_id = args.run_id or datetime.now(timezone.utc).strftime("manual_ibge_%Y%m%dT%H%M%SZ")
    with psycopg.connect(dsn) as conexao:
        alteradas, total, pop, dom = carregar(conexao, distritos, run_id)
    print(
        f"IBGE: inseridas_ou_alteradas={alteradas} | "
        f"bronze={total} | população={pop} | domicílios={dom}",
        flush=True,
    )
    print("OK: ingestão IBGE concluída", flush=True)


if __name__ == "__main__":
    main()
