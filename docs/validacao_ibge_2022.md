# Validação do IBGE — distritos de São Paulo (Censo 2022)

Fonte inspecionada: arquivo fornecido `Agregados_por_distritos_basico_BR_20260520.zip`,
CSV interno `Agregados_por_distritos_basico_BR.csv`. **Este ZIP não está incluído no pacote do repositório.**

## Estrutura conferida

- Codificação real: **Latin-1**; delimitador: **`;`**; CSV usa aspas em seus campos.
- Colunas relevantes: `CD_MUN` (município), `CD_DIST` (distrito), `NM_DIST` (nome), `v0001` (total de pessoas), `v0002` (total de domicílios), `v0007` (domicílios particulares ocupados).
- Filtro realizado: `CD_MUN = 3550308` (São Paulo).
- Resultado: **96 distritos**, 96 códigos distintos; **11.451.999 pessoas**, **4.996.529 domicílios** no total, somados sobre os 96 distritos.
- Não há valores ausentes/não numéricos em `v0001`, `v0002`, `v0007` nesses 96 registros.
- `CD_DIST` é texto de **9 dígitos** e deve permanecer texto; `v0001`/`v0002` são contagens inteiras.
- Usar `v0002` como **total de domicílios**, não confundir com `v0007` (domicílios particulares ocupados).

Definições das variáveis conferidas com material oficial do Censo 2022; o arquivo básico de 2026 tem ainda `v0008` e `v0009`, não necessários ao MVP. Ver também o dicionário oficial, para qualquer uso futuro dessas colunas.

## Conciliação com SP156 — relatório do 3º trimestre de 2026

A conciliação abaixo usa **exclusivamente** o relatório preservado `docs/relatorio_exploracao_fontes.txt` (484.232 registros do 3º trimestre); não é a cobertura territorial do ano inteiro:

| Tipo de valor em `Distrito` no SP156 | Registros | Tratamento |
|---|---:|---|
| Nome identificado entre os 96 distritos | 301.592 | Normalizar caixa/acentos/espaços e unir pelo nome |
| Código numérico (94 valores distintos) | 1.331 | **Não associar automaticamente** ao sufixo de `CD_DIST`; códigos de origem ainda não confirmados |
| Vazio | 181.309 | Excluir da junção nominal; avaliar geolocalização com malha territorial no futuro |
| **Total** | **484.232** | |

**Validação:** os 96 nomes não vazios e não numéricos presentes no relatório têm correspondência com os 96 nomes do IBGE. Isso não prova que os 1.331 códigos numéricos representam os códigos do IBGE.

## Decisão de integração para versão inicial

- `area_id = CD_DIST`, preservado como texto com 9 dígitos.
- Para `Distrito` com nome: normalizar Unicode/acentos, caixa e espaços, mapear contra `include/seeds/mapa_distritos_ibge_sp.csv`.
- Para `Distrito` numérico ou vazio: registrar motivo e **não adivinhar distrito**; considerar atribuição espacial apenas quando a malha oficial estiver disponível e validada.
- Fonte da equidade: `populacao = v0001` e `domicilios = v0002`.
- **Limitação:** o IBGE é um retrato de 2022 para ponderar ocorrências de 2026. A população pode ter mudado; `necessidade = 1` na primeira versão.
- **Limitação:** ainda falta garantir uma forma pública reproduzível de baixar o arquivo correto durante a ingestão; esta validação usou o arquivo anexado pelo usuário. Nenhuma carga no PostgreSQL foi executada aqui.
