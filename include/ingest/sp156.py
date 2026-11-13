
"""Ingestão real SP156 -> bronze.raw_sp156, por trimestre do ano informado.

limitação: cada recurso trimestral publicado é tratado como snapshot.
Linhas removidas do portal não são removidas automaticamente da bronze.
Evolução: reconciliação por snapshot com staging versionado.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import ssl
import sys
import time
import unicodedata
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlparse
import psycopg

CKAN = (
    "https://dados.prefeitura.sp.gov.br/api/3/action/"
    "package_show?id=dados-do-sp156"
)

COLUNAS = (
    "Data de Abertura",
    "Data de Finalização",
    "Meio de Comunicação",
    "Tema",
    "Serviço",
    "Status",
    "Latitude",
    "Longitude",
    "Distrito",
    "Prefeitura Operacional",
)

CAMPOS = (
    "fonte_arquivo",
    "numero_linha",
    "data_abertura_raw",
    "data_finalizacao_raw",
    "meio_comunicacao_raw",
    "tema_raw",
    "servico_raw",
    "status_raw",
    "latitude_raw",
    "longitude_raw",
    "distrito_raw",
    "prefeitura_operacional_raw",
    "_source",
    "_run_id",
)

SQL_CRIAR_STAGE = """
CREATE TEMP TABLE stage_sp156 (
    fonte_arquivo TEXT,
    numero_linha INTEGER,
    data_abertura_raw TEXT,
    data_finalizacao_raw TEXT,
    meio_comunicacao_raw TEXT,
    tema_raw TEXT,
    servico_raw TEXT,
    status_raw TEXT,
    latitude_raw TEXT,
    longitude_raw TEXT,
    distrito_raw TEXT,
    prefeitura_operacional_raw TEXT,
    _source TEXT,
    _run_id TEXT
) ON COMMIT DROP
"""

SQL_UPSERT = """
INSERT INTO bronze.raw_sp156 (
    fonte_arquivo,
    numero_linha,
    data_abertura_raw,
    data_finalizacao_raw,
    meio_comunicacao_raw,
    tema_raw,
    servico_raw,
    status_raw,
    latitude_raw,
    longitude_raw,
    distrito_raw,
    prefeitura_operacional_raw,
    _source,
    _run_id
)
SELECT
    fonte_arquivo,
    numero_linha,
    data_abertura_raw,
    data_finalizacao_raw,
    meio_comunicacao_raw,
    tema_raw,
    servico_raw,
    status_raw,
    latitude_raw,
    longitude_raw,
    distrito_raw,
    prefeitura_operacional_raw,
    _source,
    _run_id
FROM stage_sp156
WHERE true
ON CONFLICT (fonte_arquivo, numero_linha)
DO UPDATE SET
    data_abertura_raw = EXCLUDED.data_abertura_raw,
    data_finalizacao_raw = EXCLUDED.data_finalizacao_raw,
    meio_comunicacao_raw = EXCLUDED.meio_comunicacao_raw,
    tema_raw = EXCLUDED.tema_raw,
    servico_raw = EXCLUDED.servico_raw,
    status_raw = EXCLUDED.status_raw,
    latitude_raw = EXCLUDED.latitude_raw,
    longitude_raw = EXCLUDED.longitude_raw,
    distrito_raw = EXCLUDED.distrito_raw,
    prefeitura_operacional_raw = EXCLUDED.prefeitura_operacional_raw,
    _run_id = EXCLUDED._run_id,
    _ingested_at = NOW()
WHERE (
    bronze.raw_sp156.data_abertura_raw,
    bronze.raw_sp156.data_finalizacao_raw,
    bronze.raw_sp156.meio_comunicacao_raw,
    bronze.raw_sp156.tema_raw,
    bronze.raw_sp156.servico_raw,
    bronze.raw_sp156.status_raw,
    bronze.raw_sp156.latitude_raw,
    bronze.raw_sp156.longitude_raw,
    bronze.raw_sp156.distrito_raw,
    bronze.raw_sp156.prefeitura_operacional_raw
) IS DISTINCT FROM (
    EXCLUDED.data_abertura_raw,
    EXCLUDED.data_finalizacao_raw,
    EXCLUDED.meio_comunicacao_raw,
    EXCLUDED.tema_raw,
    EXCLUDED.servico_raw,
    EXCLUDED.status_raw,
    EXCLUDED.latitude_raw,
    EXCLUDED.longitude_raw,
    EXCLUDED.distrito_raw,
    EXCLUDED.prefeitura_operacional_raw
)
"""


def normalizar(valor):
    return (
        unicodedata.normalize("NFKD", valor or "")
        .encode("ascii", "ignore")
        .decode()
        .casefold()
        .strip()
    )


def trimestre(nome):
    padrao = r"(\d)\s*[º°o]?\s*TRI\s*(\d{4})"
    resultado = re.search(padrao, nome or "", re.I)

    if not resultado:
        return None

    return int(resultado.group(2)), int(resultado.group(1))


def selecionar_recursos(recursos, ano):
    encontrados = {}

    for recurso in recursos:
        nome = recurso.get("name") or ""
        chave = trimestre(nome)
        url = recurso.get("url") or ""

        if chave is None:
            continue

        if chave[0] != ano or not (1 <= chave[1] <= 4):
            continue

        if "dados do sp156" not in normalizar(nome):
            continue

        if not urlparse(url).path.lower().endswith(".csv"):
            continue

        if chave[1] in encontrados:
            raise ValueError(
                f"Mais de um CSV para {ano} T{chave[1]}"
            )

        encontrados[chave[1]] = {
            "url": url,
            "nome": nome,
            "fonte_arquivo": f"sp156_{ano}_T{chave[1]}",
        }

    return encontrados


def abrir_url(url, contexto, tentativas=3):
    endereco = urlparse(url)

    if (
        endereco.scheme != "https"
        or endereco.hostname != "dados.prefeitura.sp.gov.br"
    ):
        raise ValueError(
            f"Host de fonte não permitido: {endereco.hostname}"
        )

    ultimo_erro = None

    for tentativa in range(tentativas):
        try:
            requisicao = urllib.request.Request(
                url,
                headers={"User-Agent": "fila-justa-ingest/1.0"},
            )

            return urllib.request.urlopen(
                requisicao,
                timeout=180,
                context=contexto,
            )

        except (OSError, TimeoutError) as erro:
            ultimo_erro = erro

            if tentativa < tentativas - 1:
                time.sleep(min(2 ** tentativa, 4))

    raise ultimo_erro


def obter_recursos(contexto):
    with abrir_url(CKAN, contexto) as resposta:
        pacote = json.load(resposta)

    if not pacote.get("success"):
        raise RuntimeError("CKAN retornou erro")

    return pacote["result"]["resources"]



def ler_csv(resposta):
    # Detecta codificação e separador sem carregar
    # o arquivo inteiro na memória.
    buffer = io.BufferedReader(resposta)
    amostra = buffer.peek(65536)

    try:
        texto_amostra = amostra.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        texto_amostra = amostra.decode("latin-1")
        encoding = "latin-1"

    if not texto_amostra.strip():
        raise ValueError("CSV SP156 vazio")

    # Descobre o separador a partir do cabeçalho.
    cabecalho = texto_amostra.splitlines()[0]

    try:
        separador = csv.Sniffer().sniff(
            cabecalho,
            delimiters=",;\t|"
        ).delimiter
    except csv.Error:
        separador = max(
            ",;\t|",
            key=cabecalho.count
        )


        if cabecalho.count(separador) == 0:
            raise ValueError(
                "Colunas SP156 ausentes ou cabeçalho inválido: "
                + repr(cabecalho[:200])
            )


    print(
        f"CSV SP156: codificação={encoding}; "
        f"separador={separador!r}",
        flush=True
    )

    texto = io.TextIOWrapper(
        buffer,
        encoding=encoding,
        newline=""
    )

    leitor = csv.DictReader(
        texto,
        delimiter=separador
    )

    # Remove espaços e eventuais marcas BOM.
    leitor.fieldnames = [
        coluna.strip().lstrip("\ufeff")
        for coluna in (leitor.fieldnames or [])
    ]

    colunas = set(leitor.fieldnames)
    faltando = set(COLUNAS) - colunas

    if faltando:
        raise ValueError(
            f"Colunas SP156 ausentes: {sorted(faltando)}. "
            f"Colunas encontradas: {leitor.fieldnames}"
        )

    for numero, linha in enumerate(leitor, start=2):
        if None in linha:
            raise ValueError(
                f"CSV malformado na linha {numero}"
            )

        yield numero - 1, linha


def transformar(fonte, numero, linha, run_id):
    # LGPD: não armazenar Logradouro, Número, CEP ou Assunto.

    def valor(coluna):
        return (linha.get(coluna) or "").strip() or None

    return (
        fonte,
        numero,
        valor("Data de Abertura"),
        valor("Data de Finalização"),
        valor("Meio de Comunicação"),
        valor("Tema"),
        valor("Serviço"),
        valor("Status"),
        valor("Latitude"),
        valor("Longitude"),
        valor("Distrito"),
        valor("Prefeitura Operacional"),
        "SP156",
        run_id,
    )


def carregar_trimestre(conexao, recurso, contexto, run_id):
    fonte = recurso["fonte_arquivo"]
    total = 0

    # Uma transação por trimestre.
    # Se falhar, os registros desse trimestre são revertidos.
    with conexao.transaction():
        with conexao.cursor() as cursor:
            cursor.execute(SQL_CRIAR_STAGE)

            with abrir_url(recurso["url"], contexto) as resposta:
                comando_copy = (
                    "COPY stage_sp156 ("
                    + ", ".join(CAMPOS)
                    + ") FROM STDIN"
                )

                with cursor.copy(comando_copy) as copia:
                    for numero, linha in ler_csv(resposta):
                        registro = transformar(
                            fonte, numero, linha, run_id
                        )

                        copia.write_row(registro)
                        total += 1

            if total == 0:
                raise ValueError(
                    f"Recurso vazio: {fonte}"
                )

            cursor.execute(
                "SELECT COUNT(*) FROM stage_sp156"
            )

            quantidade_stage = cursor.fetchone()[0]

            if quantidade_stage != total:
                raise RuntimeError(
                    f"Contagem divergente em {fonte}"
                )

            cursor.execute(SQL_UPSERT)
            alteradas = cursor.rowcount

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bronze.raw_sp156
                WHERE fonte_arquivo = %s
                """,
                (fonte,),
            )

            armazenadas = cursor.fetchone()[0]

            if armazenadas != total:
                raise RuntimeError(
                    f"Quantidade inesperada para {fonte}: "
                    f"{armazenadas} != {total}"
                )

    return total, alteradas, armazenadas


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingestão SP156 por ano"
    )

    parser.add_argument(
        "--ano",
        type=int,
        default=2026,
    )

    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Desabilita TLS apenas para diagnóstico",
    )

    parser.add_argument(
        "--run-id",
        default=None,
    )

    args = parser.parse_args(argv)

    contexto = (
        ssl._create_unverified_context()
        if args.insecure
        else ssl.create_default_context()
    )

    if args.insecure:
        print(
            "AVISO: verificação TLS desabilitada",
            file=sys.stderr,
        )

    recursos = selecionar_recursos(
        obter_recursos(contexto),
        args.ano,
    )

    if not recursos:
        raise RuntimeError(
            f"Nenhum trimestre encontrado para {args.ano}"
        )

    faltando = sorted(
        set(range(1, 5)) - set(recursos)
    )

    print(
        f"Ano {args.ano} | "
        f"trimestres disponíveis: {sorted(recursos)} | "
        f"não publicados: {faltando}",
        flush=True,
    )

    dsn = os.environ.get("AIRFLOW_CONN_WAREHOUSE")

    if not dsn:
        raise RuntimeError(
            "AIRFLOW_CONN_WAREHOUSE não configurada"
        )

    run_id = args.run_id or datetime.now(
        timezone.utc
    ).strftime("manual_%Y%m%dT%H%M%SZ")

    with psycopg.connect(dsn) as conexao:
        for tri in sorted(recursos):
            recurso = recursos[tri]

            print(
                f"Iniciando {recurso['nome']}",
                flush=True,
            )

            total, alteradas, armazenadas = (
                carregar_trimestre(
                    conexao,
                    recurso,
                    contexto,
                    run_id,
                )
            )

            print(
                f"T{tri}: fonte={total}, "
                f"inseridas_ou_alteradas={alteradas}, "
                f"bronze={armazenadas}",
                flush=True,
            )

    print(
        "OK: ingestão SP156 concluída",
        flush=True,
    )


if __name__ == "__main__":
    main()
