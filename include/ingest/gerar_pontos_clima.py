"""Gera pontos de consulta para os 96 distritos a partir da malha oficial IBGE 2022.

Uso: python /opt/airflow/include/ingest/gerar_pontos_clima.py
     python /opt/airflow/include/ingest/gerar_pontos_clima.py --arquivo /c/caminho/SP_distritos_CD2022.zip

Saída: include/seeds/pontos_clima_ibge_sp.csv (somente códigos e coordenadas).
Sem dependências de GIS externas; leitura mínima de SHP Polygon + DBF.

limitação: centroide planar calculado em coordenadas geográficas (SIRGAS 2000),
adequado para ponto de consulta climática aproximado, não para medição de área
ou distâncias. Para polígonos côncavos, pode usar ponto interno representativo.
"""
from __future__ import annotations

import argparse
import csv
import io
import math
import struct
import urllib.request
import zipfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
URL_MALHA = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios/malha_com_atributos/distritos/"
    "shp/UF/SP/SP_distritos_CD2022.zip"
)
ARQUIVO_SAIDA = RAIZ / "include/seeds/pontos_clima_ibge_sp.csv"
MAPA = RAIZ / "include/seeds/mapa_distritos_ibge_sp.csv"


def obter_zip(arquivo=None):
    if arquivo:
        print(f"Abrindo malha local: {arquivo}", flush=True)
        return Path(arquivo).read_bytes()
    print(f"Baixando malha oficial do IBGE: {URL_MALHA}", flush=True)
    req = urllib.request.Request(URL_MALHA, headers={"User-Agent": "fila-justa-academico/1.0"})
    with urllib.request.urlopen(req, timeout=180) as resposta:
        return resposta.read()


def ler_campos_dbf(conteudo):
    if len(conteudo) < 33:
        raise ValueError("Arquivo DBF muito pequeno")
    nregistros, tam_cab, tam_reg = struct.unpack_from("<IHH", conteudo, 4)
    campos = []
    pos = 32
    offset = 1
    while pos + 32 <= tam_cab and conteudo[pos] != 0x0D:
        descritor = conteudo[pos:pos + 32]
        nome = descritor[:11].split(b"\0", 1)[0].decode("ascii", "ignore").strip()
        comprimento = descritor[16]
        campos.append((nome, offset, comprimento))
        offset += comprimento
        pos += 32
    if not {"CD_DIST", "CD_MUN"}.issubset({n for n, _, _ in campos}):
        raise ValueError("Malha sem CD_DIST/CD_MUN no DBF")
    if len(conteudo) < tam_cab + nregistros * tam_reg:
        raise ValueError("DBF truncado")
    for i in range(nregistros):
        start = tam_cab + i * tam_reg
        row = conteudo[start:start + tam_reg]
        if row[0:1] == b"*":
            yield i, {}
            continue
        yield i, {
            nome: row[off:off + comp].decode("latin-1").strip()
            for nome, off, comp in campos if nome in ("CD_DIST", "CD_MUN")
        }


def anel_centroide(pontos):
    duplo_area = sx = sy = 0.0
    for a, b in zip(pontos, pontos[1:]):
        cross = a[0] * b[1] - b[0] * a[1]
        duplo_area += cross
        sx += (a[0] + b[0]) * cross
        sy += (a[1] + b[1]) * cross
    if len(pontos) > 2 and pontos[-1] != pontos[0]:
        a, b = pontos[-1], pontos[0]
        cross = a[0] * b[1] - b[0] * a[1]
        duplo_area += cross
        sx += (a[0] + b[0]) * cross
        sy += (a[1] + b[1]) * cross
    return duplo_area, sx, sy


def pertence_anel(x, y, anel):
    dentro = False
    for (xa, ya), (xb, yb) in zip(anel, anel[1:] + anel[:1]):
        if (ya > y) != (yb > y):
            cx = (xb - xa) * (y - ya) / (yb - ya) + xa
            if x < cx:
                dentro = not dentro
    return dentro


def ponto_interno(x, y, aneis):
    return sum(pertence_anel(x, y, a) for a in aneis) % 2 == 1


def centroide_poligono(registro):
    tipo, = struct.unpack_from("<i", registro, 0)
    if tipo not in (5, 15, 25):
        raise ValueError(f"Formato SHP inesperado: {tipo}")
    xmin, ymin, xmax, ymax = struct.unpack_from("<4d", registro, 4)
    n_partes, n_pontos = struct.unpack_from("<2i", registro, 36)
    if not (0 < n_partes < 100000 and 2 < n_pontos < 10000000):
        raise ValueError("Polígono SHP inválido")
    inicio_partes = 44
    inicio_pontos = inicio_partes + n_partes * 4
    if len(registro) < inicio_pontos + n_pontos * 16:
        raise ValueError("Polígono truncado")
    cortes = list(struct.unpack_from(f"<{n_partes}i", registro, inicio_partes)) + [n_pontos]
    pontos = [struct.unpack_from("<2d", registro, inicio_pontos + i * 16) for i in range(n_pontos)]
    aneis = [pontos[cortes[i]:cortes[i + 1]] for i in range(n_partes)]
    soma_a = soma_x = soma_y = 0.0
    for anel in aneis:
        a, sx, sy = anel_centroide(anel)
        soma_a += a
        soma_x += sx
        soma_y += sy
    if abs(soma_a) < 1e-10:
        raise ValueError("Polígono sem área útil")
    x, y = soma_x / (3 * soma_a), soma_y / (3 * soma_a)
    origem = "centroide"
    if not ponto_interno(x, y, aneis):
        # Procura ponto interior determinístico; nunca usa localização inventada.
        encontrados = []
        for n in (12, 40, 100):
            for j in range(n):
                yy = ymin + (j + 0.5) * (ymax - ymin) / n
                for i in range(n):
                    xx = xmin + (i + 0.5) * (xmax - xmin) / n
                    if ponto_interno(xx, yy, aneis):
                        encontrados.append((xx, yy))
            if encontrados:
                break
        if not encontrados:
            raise ValueError("Sem ponto interior para polígono")
        x, y = min(encontrados, key=lambda p: (p[0] - x)**2 + (p[1] - y)**2)
        origem = "ponto_interno"
    if not (-47.3 < x < -46.1 and -24.2 < y < -23.2):
        raise ValueError(f"Coordenada fora de São Paulo: lon={x}, lat={y}; confirmar CRS")
    return y, x, origem


def extrair_pontos(conteudo_zip, codigos):
    with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as z:
        shps = [x for x in z.namelist() if x.lower().endswith(".shp")]
        if len(shps) != 1:
            raise ValueError(f"Esperado 1 SHP, encontrados: {shps}")
        base = shps[0][:-4]
        dbf = next((x for x in z.namelist() if x.lower() == (base + ".dbf").lower()), None)
        prj = next((x for x in z.namelist() if x.lower() == (base + ".prj").lower()), None)
        if not dbf or not prj:
            raise ValueError("Malha incompleta: faltando DBF ou PRJ")
        sistema = z.read(prj).decode("utf-8-sig", "replace")
        if "SIRGAS" not in sistema.upper() or not ("GEOGCS" in sistema.upper() or "GEOGCRS" in sistema.upper()):
            raise ValueError("A malha não declara SIRGAS geográfico; não usar coordenadas sem reprojeção")
        alvo = {i: r["CD_DIST"] for i, r in ler_campos_dbf(z.read(dbf))
                if r.get("CD_MUN") == "3550308" and r.get("CD_DIST") in codigos}
        if set(alvo.values()) != codigos or len(alvo) != len(codigos):
            raise ValueError(f"Códigos de distritos divergentes. Encontrados: {len(alvo)}; esperados: {len(codigos)}")
        pontos = {}
        with z.open(shps[0]) as f:
            if len(f.read(100)) != 100:
                raise ValueError("SHP vazio")
            indice = 0
            while True:
                cab = f.read(8)
                if not cab:
                    break
                if len(cab) != 8:
                    raise ValueError("Cabeçalho SHP truncado")
                _, palavras = struct.unpack(">2i", cab)
                if not 0 <= palavras <= 30000000:
                    raise ValueError("Tamanho inválido do registro SHP")
                corpo = f.read(palavras * 2)
                if len(corpo) != palavras * 2:
                    raise ValueError("Registro SHP truncado")
                if indice in alvo:
                    pontos[alvo[indice]] = centroide_poligono(corpo)
                indice += 1
        if set(pontos) != codigos:
            raise ValueError("Nem todos os distritos foram encontrados na geometria")
        return pontos


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--arquivo", help="ZIP oficial da malha SP 2022 já baixado")
    args = ap.parse_args(argv)
    with MAPA.open(encoding="utf-8-sig", newline="") as f:
        registros = list(csv.DictReader(f))
    if len(registros) != 96 or len({r['area_id'] for r in registros}) != 96:
        raise ValueError("O mapa de distritos precisa ter exatamente 96 códigos distintos")
    codigos = {r['area_id'] for r in registros}
    pontos = extrair_pontos(obter_zip(args.arquivo), codigos)
    ARQUIVO_SAIDA.parent.mkdir(parents=True, exist_ok=True)
    with ARQUIVO_SAIDA.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(("area_id", "latitude", "longitude", "metodo", "fonte"))
        for codigo in sorted(codigos):
            lat, lon, metodo = pontos[codigo]
            w.writerow((codigo, f"{lat:.7f}", f"{lon:.7f}", metodo, "IBGE_SP_distritos_CD2022"))
    print(f"OK: {len(pontos)} pontos de consulta gerados em {ARQUIVO_SAIDA}", flush=True)
    print(f"Centroide: {sum(p[2] == 'centroide' for p in pontos.values())}; ponto interno: {sum(p[2] == 'ponto_interno' for p in pontos.values())}", flush=True)


if __name__ == "__main__":
    main()
