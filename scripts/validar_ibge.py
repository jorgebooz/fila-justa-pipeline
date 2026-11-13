"""Validação reproduzível do básico por distrito (arquivo zip local do IBGE).
Uso: python scripts/validar_ibge.py /caminho/Agregados_por_distritos_basico_BR_20260520.zip
Não lê/escreve dados pessoais nem carrega registros no banco.
"""
import csv
import io
import sys
import zipfile
from pathlib import Path


def validar(caminho):
    with zipfile.ZipFile(caminho) as z:
        with io.TextIOWrapper(z.open("Agregados_por_distritos_basico_BR.csv"), encoding="latin-1", newline="") as f:
            r = csv.DictReader(f, delimiter=";")
            obrigatorias = {"CD_MUN", "CD_DIST", "NM_DIST", "v0001", "v0002", "v0007"}
            faltantes = obrigatorias.difference(r.fieldnames or [])
            if faltantes:
                raise ValueError(f"Colunas ausentes: {sorted(faltantes)}")
            linhas = [linha for linha in r if linha["CD_MUN"] == "3550308"]
    if len(linhas) != 96 or len({l["CD_DIST"] for l in linhas}) != 96:
        raise ValueError(f"Esperados 96 distritos distintos; encontrados {len(linhas)}")
    for linha in linhas:
        if len(linha["CD_DIST"]) != 9 or any(not linha[k].isdigit() for k in ("v0001", "v0002", "v0007")):
            raise ValueError(f"Código ou contagem inválida no distrito {linha['CD_DIST']}")
    print(f"Distritos: {len(linhas)}")
    print(f"População: {sum(int(l['v0001']) for l in linhas)}")
    print(f"Domicílios totais: {sum(int(l['v0002']) for l in linhas)}")
    print("OK: IBGE básico por distrito validado (sem carga no banco)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Uso: python scripts/validar_ibge.py caminho/arquivo.zip")
    validar(Path(sys.argv[1]))
