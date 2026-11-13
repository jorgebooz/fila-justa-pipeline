# Validação do catálogo SP156 — recorte 2026

Fonte: `docs/catalogo_servicos_sp156.csv`, exportado pelo script de exploração do projeto, fornecido após execução local. Este relatório não é uma execução de ingestão.

- Serviços distintos (nomes exatos): **880**.
- Soma das contagens do catálogo: **1,525,907** solicitações, conforme consolidação exportada (não é contagem de ocorrências válidas/abertas, nem de clusters).
- Serviços selecionados para seed inicial: **10**, com **228,917** registros no catálogo (**15.0%** das solicitações catalogadas).
- Excluídos da seleção inicial: serviços fora das cinco categorias existentes, ambiguidades ou serviços de avaliação/solicitação que não comprovam o evento (veja `docs/servicos_revisao.csv`).

## Critérios

1. Manter **somente** os nomes literais encontrados no catálogo, sem aproximar strings na origem.
2. Incluir problemas operacionais de infraestrutura/risco urbano compatíveis com `dim_tipo.csv`: `alagamento`, `bueiro_entupido`, `buraco`, `arvore_caida`, `entulho`.
3. Tratar `Inundação` como `alagamento` na versão inicial, explicitando a perda de detalhe. `Drenagem de água da chuva - Bueiros e bocas de lobo` foi agrupado em `bueiro_entupido` por aproximação operacional, **não** como prova de entupimento; confirmar com glossário antes de produção.
4. Não agrupar `Risco iminente de queda de árvore` ou avaliação de poda como `arvore_caida`: exigiria categoria própria ou regra de risco.
5. Somente na silver filtrar `Aberto`/`Em Andamento`, datas, coordenadas, distrito e demais critérios; bronze preserva o conteúdo útil bruto com remoção de endereço/número/CEP por privacidade.

## Volumes por tipo selecionado (antes de filtros de status ou localização)

| Tipo | Quantidade |
|---|---:|
| `entulho` | 122,203 |
| `buraco` | 83,902 |
| `bueiro_entupido` | 18,602 |
| `arvore_caida` | 3,185 |
| `alagamento` | 1,025 |

## Atenções para validação

- O catálogo **não** contém distribuição por trimestre, status ou coordenadas por serviço. Não é possível afirmar quantos registros entrarão na silver ou na fila.
- O catálogo soma os CSVs examinados pelo script; a publicação do **4º trimestre de 2026** deve ser conferida antes de chamar o ano de completo.
- A presença de nomes semelhantes com diferentes caracteres (`Árvore ` vs `Árvore –`) é evidência de inconsistência textual. Evitar normalização destrutiva; comparar Unicode e preservar original para auditoria.
- Antes da ingestão, validar o mapeamento com o glossário oficial e a cobertura por serviço; o modelo dimensional e as fórmulas do score são hipóteses, não validação empírica.
- O catálogo não contém dados pessoais; arquivos brutos com logradouro/número/CEP não devem ser publicados.
