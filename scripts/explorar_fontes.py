"""Passo 2 (v3): explora as solicitações reais do SP156 e testa o Open-Meteo.

Rode na raiz do projeto, na sua máquina (precisa de internet):

    python scripts/explorar_fontes.py --insecure              # trimestre mais recente, arquivo inteiro
    python scripts/explorar_fontes.py --ano 2026              # todos os trimestres publicados de 2026
    python scripts/explorar_fontes.py --trimestre 2T2026
    python scripts/explorar_fontes.py --amostra               # só as primeiras 40 mil linhas (rápido)

O que ele registra: colunas reais, volume, formato da data, cobertura de latitude/longitude e de
Distrito, valores de Status / Meio de Comunicação / Tema, todos os distritos e prefeituras
operacionais, e, para cada serviço listado em include/seeds/servicos_interesse.csv, quantas
linhas existem e quantas têm coordenada e distrito. Não imprime linhas individuais (o arquivo
tem endereço: Logradouro, Número e CEP). Só usa a biblioteca padrão.

O relatório sai em docs/relatorio_exploracao_fontes.txt e é a base de docs/fontes.md.
Também exporta o catálogo agregado docs/catalogo_servicos_sp156.csv (servico, quantidade),
com totais combinados dos trimestres selecionados. A seleção anual não inventa trimestres ainda não publicados.
"""
import argparse
import csv
import io
import json
import re
import ssl
import unicodedata
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
CKAN = "https://dados.prefeitura.sp.gov.br/api/3/action/package_show?id=dados-do-sp156"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
SEED = RAIZ / "include" / "seeds" / "servicos_interesse.csv"
CATALOGO = RAIZ / "docs" / "catalogo_servicos_sp156.csv"

linhas_relatorio = []
CONTEXTO_SSL = None


def log(*partes):
    texto = " ".join(str(p) for p in partes)
    print(texto)
    linhas_relatorio.append(texto)


def norm(texto):
    sem_acento = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode()
    return " ".join(sem_acento.lower().split())


def abrir(url):
    req = urllib.request.Request(url, headers={"User-Agent": "fila-justa-aluno/1.0"})
    return urllib.request.urlopen(req, timeout=120, context=CONTEXTO_SSL)


def get_bytes(url, limite=None):
    with abrir(url) as resp:
        return resp.status, (resp.read(limite) if limite else resp.read())


def periodo(nome):
    """'Dados do SP156 - 3° TRI 2026' -> (2026, 3)."""
    m = re.search(r"(\d)\s*[º°o]?\s*TRI\s*(\d{4})", nome or "", flags=re.I)
    return (int(m.group(2)), int(m.group(1))) if m else None


def listar_recursos():
    _, corpo = get_bytes(CKAN)
    pacote = json.loads(corpo)["result"]
    log(f"== SP156: {pacote.get('title')} | dataset modificado em: {pacote.get('metadata_modified')}")
    log(f"Licença declarada: {pacote.get('license_title')} ({pacote.get('license_id')})")
    recursos = pacote.get("resources", [])
    log(f"Recursos encontrados: {len(recursos)}")
    return recursos


def candidatos_solicitacoes(recursos):
    saida = []
    for r in recursos:
        nome = (r.get("name") or "").strip()
        url = r.get("url") or ""
        p = periodo(nome)
        if "dados do sp156" not in norm(nome) or p is None or not url.lower().endswith(".csv"):
            continue
        saida.append((p, nome, url, r.get("size")))
    return sorted(saida)


def escolher(cands, trimestre):
    if trimestre:
        m = re.match(r"(\d)T(\d{4})", trimestre.upper())
        alvo = (int(m.group(2)), int(m.group(1)))
        return next((c for c in cands if c[0] == alvo), None)
    return cands[-1] if cands else None


def decodificar_cabecalho(bruto):
    for enc in ("utf-8-sig", "latin-1"):
        try:
            return bruto.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return bruto.decode("utf-8", errors="replace"), "utf-8 (com erros)"


def achar(colunas, *chaves):
    for chave in chaves:
        for c in colunas:
            if chave in norm(c):
                return c
    return None


def carregar_seed():
    mapa = {}
    if not SEED.exists():
        log(f"AVISO: seed {SEED} não encontrada; cobertura por serviço de interesse não será calculada.")
        return mapa
    with open(SEED, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            mapa[norm(r["servico"])] = (r["categoria"], r["incluir"].strip().lower() == "sim", r["servico"])
    return mapa


def topo(contador, n, titulo):
    log(f"\n{titulo}")
    for valor, qtd in contador.most_common(n):
        log(f"  {qtd:>8}  {valor}")


def explorar(url, amostra):
    log(f"\n== Lendo: {url}")
    _, cabeca = get_bytes(url, 200_000)
    texto, enc = decodificar_cabecalho(cabeca.rsplit(b"\n", 1)[0])
    try:
        delim = csv.Sniffer().sniff(texto[:20000], delimiters=";,\t|").delimiter
    except csv.Error:
        delim = ";"
    log(f"Encoding: {enc} | delimitador: {delim!r}")

    resp = abrir(url)
    leitor = csv.DictReader(io.TextIOWrapper(resp, encoding=enc.split()[0], errors="replace", newline=""), delimiter=delim)
    colunas = leitor.fieldnames or []
    log(f"Colunas reais ({len(colunas)}): {colunas}")

    c_serv = achar(colunas, "servi")
    c_lat, c_lon = achar(colunas, "latitude"), achar(colunas, "longitude")
    c_data = achar(colunas, "data de abertura")
    c_fim = achar(colunas, "data de finalizacao")
    c_dist = achar(colunas, "distrito")
    c_pref = achar(colunas, "prefeitura operacional", "subprefeitura")
    c_status, c_meio, c_tema = achar(colunas, "status"), achar(colunas, "meio de comunicacao"), achar(colunas, "tema")
    log(f"Mapeamento -> serviço: {c_serv} | lat: {c_lat} | lon: {c_lon} | abertura: {c_data} | finalização: {c_fim} "
        f"| distrito: {c_dist} | prefeitura operacional: {c_pref} | status: {c_status} | meio: {c_meio} | tema: {c_tema}")

    seed = carregar_seed()
    total = com_coord = com_dist = com_fim = 0
    cont = {k: Counter() for k in ("serv", "dist", "pref", "status", "meio", "tema")}
    dmy = mdy = 0
    primeira = ultima = None
    por_servico = defaultdict(lambda: [0, 0, 0])  # n, com coordenada, com distrito
    limite = 40_000 if amostra else None

    for linha in leitor:
        total += 1
        serv = (linha.get(c_serv) or "").strip() if c_serv else ""
        dist = (linha.get(c_dist) or "").strip() if c_dist else ""
        tem_coord = bool(c_lat and c_lon and (linha.get(c_lat) or "").strip() and (linha.get(c_lon) or "").strip())
        com_coord += tem_coord
        com_dist += bool(dist)
        if c_fim and (linha.get(c_fim) or "").strip():
            com_fim += 1
        cont["serv"][serv] += 1
        for chave, col in (("dist", c_dist), ("pref", c_pref), ("status", c_status), ("meio", c_meio), ("tema", c_tema)):
            if col:
                cont[chave][(linha.get(col) or "").strip()] += 1
        if c_data:
            v = (linha.get(c_data) or "").strip()
            primeira = primeira or v
            ultima = v
            m = re.match(r"^(\d{1,2})/(\d{1,2})/\d{4}", v)
            if m:
                dmy += int(m.group(1)) > 12
                mdy += int(m.group(2)) > 12
        chave_serv = norm(serv)
        if chave_serv in seed:
            s = por_servico[chave_serv]
            s[0] += 1
            s[1] += tem_coord
            s[2] += bool(dist)
        if limite and total >= limite:
            break
    resp.close()

    modo = "AMOSTRA (não representa o trimestre inteiro)" if amostra else "arquivo inteiro"
    log(f"\nModo: {modo} | linhas lidas: {total:,}")
    if total:
        log(f"Com latitude e longitude: {com_coord:,} ({com_coord / total:.1%})")
        log(f"Com Distrito preenchido: {com_dist:,} ({com_dist / total:.1%})")
        log(f"Com Data de Finalização preenchida: {com_fim:,} ({com_fim / total:.1%})")
    log(f"Data de abertura — primeira linha: {primeira} | última linha: {ultima}")
    log(f"Formato da data: linhas que só cabem em DD/MM/AAAA (1º número > 12): {dmy:,} | só em MM/DD/AAAA (2º número > 12): {mdy:,}")
    log(f"Serviços distintos: {len(cont['serv'])} | distritos distintos: {len(cont['dist'])} | prefeituras operacionais: {len(cont['pref'])}")

    # Catálogo combinado será exportado pela função main, após reunir todos os trimestres.

    topo(cont["status"], 20, "Status:")
    topo(cont["meio"], 20, "Meio de Comunicação:")
    topo(cont["tema"], 15, "Tema (top 15):")
    topo(cont["pref"], 50, "Prefeitura Operacional (todas):")
    topo(cont["dist"], 200, "Distrito (todos):")

    if seed:
        log("\nServiços de interesse (seed): linhas | com coordenada | com distrito")
        total_inc = coord_inc = dist_inc = 0
        por_cat = defaultdict(lambda: [0, 0, 0])
        for chave, (cat, incluir, nome) in seed.items():
            n, c, d = por_servico.get(chave, [0, 0, 0])
            log(f"  [{'incluir' if incluir else 'fora   '}] {cat:<18} {n:>7} | {c:>7} | {d:>7}  {nome}")
            if n == 0:
                log("      ^ ATENÇÃO: nenhum registro com esse nome exato neste arquivo")
            if incluir:
                total_inc, coord_inc, dist_inc = total_inc + n, coord_inc + c, dist_inc + d
                por_cat[cat][0] += n
                por_cat[cat][1] += c
                por_cat[cat][2] += d
        log("\nTotal por categoria (somente serviços marcados 'incluir'):")
        for cat, (n, c, d) in por_cat.items():
            log(f"  {cat:<18} {n:>7} linhas | coord {c / n:.1%} | distrito {d / n:.1%}" if n else f"  {cat:<18} 0")
        if total_inc:
            log(f"  {'TOTAL':<18} {total_inc:>7} linhas ({total_inc / total:.1%} do trimestre) "
                f"| coord {coord_inc / total_inc:.1%} | distrito {dist_inc / total_inc:.1%}")

    return cont["serv"], total


def exportar_catalogo(contagem):
    # Apenas agregados, sem endereços ou dados pessoais.
    CATALOGO.parent.mkdir(parents=True, exist_ok=True)
    with CATALOGO.open("w", encoding="utf-8-sig", newline="") as saida:
        escritor = csv.writer(saida)
        escritor.writerow(["servico", "quantidade"])
        for nome, quantidade in sorted(contagem.items(), key=lambda x: (-x[1], norm(x[0]))):
            escritor.writerow([nome, quantidade])
    log(f"Catálogo de serviços exportado: {CATALOGO} ({len(contagem)} serviços)")


def testar_open_meteo():
    log("\n== Open-Meteo: teste de chamada com 2 pontos")
    url = (f"{OPEN_METEO}?latitude=-23.55,-23.60&longitude=-46.63,-46.65"
           "&hourly=precipitation&forecast_days=2&timezone=America%2FSao_Paulo")
    status, corpo = get_bytes(url)
    dados = json.loads(corpo)
    pontos = dados if isinstance(dados, list) else [dados]
    log(f"HTTP {status} | pontos retornados: {len(pontos)}")
    for p in pontos:
        h = p.get("hourly", {})
        log(f"  lat={p.get('latitude')} lon={p.get('longitude')} | horas: {len(h.get('time', []))}"
            f" | unidade: {p.get('hourly_units', {}).get('precipitation')} | primeiras 6 horas: {h.get('precipitation', [])[:6]}")


def main():
    global CONTEXTO_SSL
    ap = argparse.ArgumentParser()
    grupo = ap.add_mutually_exclusive_group()
    grupo.add_argument("--trimestre", help="ex.: 3T2026 (padrão: o mais recente)")
    grupo.add_argument("--ano", type=int, help="ex.: 2026 (todos os trimestres publicados desse ano)")
    ap.add_argument("--amostra", action="store_true", help="lê só uma amostra (rápido)")
    ap.add_argument("--insecure", action="store_true", help="desliga a verificação SSL (use só se necessário)")
    args = ap.parse_args()
    if args.insecure:
        CONTEXTO_SSL = ssl._create_unverified_context()
        log("AVISO: verificação SSL desligada (dado público, sem credenciais).")

    recursos = listar_recursos()
    cands = candidatos_solicitacoes(recursos)
    log(f"CSVs trimestrais de solicitações encontrados: {len(cands)}")
    if args.ano is not None:
        selecionados = [c for c in cands if c[0][0] == args.ano]
        encontrados = {c[0][1] for c in selecionados}
        faltantes = sorted({1, 2, 3, 4} - encontrados)
        log(f"Recorte solicitado: ano completo de {args.ano} (4 trimestres)")
        log(f"Trimestres publicados encontrados: {', '.join(str(i) + 'T' for i in sorted(encontrados)) or 'nenhum'}")
        if faltantes:
            log(f"ATENÇÃO: ano INCOMPLETO na fonte. Faltam: {', '.join(str(i) + 'T' for i in faltantes)}. Não incluir como zero.")
    else:
        escolhido = escolher(cands, args.trimestre)
        selecionados = [escolhido] if escolhido else []

    total_geral = 0
    catalogo_geral = Counter()
    for c in selecionados:
        log(f"\nEscolhido: {c[1]} | tamanho: {c[3]}")
        contagem, total = explorar(c[2], args.amostra)
        catalogo_geral.update(contagem)
        total_geral += total
    if selecionados:
        log(f"\nTOTAL dos arquivos selecionados: {total_geral:,} linhas | {len(catalogo_geral)} serviços distintos")
        exportar_catalogo(catalogo_geral)
    else:
        log("Nenhum CSV de solicitações encontrado para o recorte pedido.")
    testar_open_meteo()

    destino = RAIZ / "docs" / "relatorio_exploracao_fontes.txt"
    destino.write_text("\n".join(linhas_relatorio), encoding="utf-8")
    print(f"\nRelatório salvo em {destino}")


if __name__ == "__main__":
    main()
