# Passo 5 — Relatos simulados do aplicativo

> Os relatos **não representam cidadãos ou ocorrências reais**. A fonte real de ocorrências continua sendo o SP156. A simulação existe para demonstrar o segundo canal, a deduplicação e a correção de equidade do MVP.

## Fonte territorial

O gerador lê `include/seeds/pontos_clima_ibge_sp.csv`, criado pelo script da malha oficial do IBGE no passo anterior, e confere seus **96 códigos de distrito** com `include/seeds/mapa_distritos_ibge_sp.csv`. Antes de gravar, também compara os 96 códigos com a bronze do IBGE. Os pontos são referências distritais públicas, **não endereços ou posições de ocorrências reais**.

## Parâmetros (hipóteses de simulação)

| Parâmetro | Padrão | Motivo |
|---|---|---|
| Ano | 2026 | Período do SP156 analisado |
| Semente pseudoaleatória | `2026` | Reproduzibilidade |
| Data-base | `2026-11-11T12:00:00-03:00` | Referência temporal fixa para reexecuções idênticas; **não representa a data atual** |
| Distritos | 96 | Todos os distritos do IBGE de São Paulo |
| Propensão a reportar | 32 baixa, 32 média, 32 alta | Diferenças artificiais de reporte entre áreas; faixas distribuídas por embaralhamento determinístico |
| Baixa propensão | 1–2 grupos, 1–2 relatos/grupo | Subnotificação simulada |
| Média propensão | 2–3 grupos, 2–3 relatos/grupo | Faixa intermediária |
| Alta propensão | 3–5 grupos, 4–6 relatos/grupo | Sobre-representação simulada |
| Janela entre grupos | 9 dias | Evita unir incidentes diferentes pela regra de 7 dias da deduplicação |
| Janela dentro do grupo | Até 2 horas | Garante duplicatas intencionais na mesma célula H3 |
| Coordenadas | Um ponto-base IBGE com desvio <= 0,0001° por grupo; **mesmas coordenadas para todos os relatos de um grupo** | Dados sintéticos com sobreposição intencional |
| Tipos | `alagamento`, `bueiro_entupido`, `buraco`, `arvore_caida`, `entulho` | Mesmos tipos de `dim_tipo.csv` |
| Status | `Aberto`, `Em Andamento` | Apenas relatos que caberiam na fila do gestor |

O número de grupos e de relatos é sorteado deterministicamente a partir da semente e do código do distrito. Os IDs têm o formato `APP26-<seed>-<area>-<grupo>-<indice>`; a mesma entrada gera o mesmo ID. O campo `_source` é sempre `APP_SIMULADO`.

## Idempotência

A carga usa `bronze.raw_relatos_app` com chave primária `relato_id`. O `ON CONFLICT ... DO UPDATE ... WHERE ... IS DISTINCT FROM` altera o registro **somente se algum campo operacional mudou**. A segunda execução com os **mesmos parâmetros** deve retornar `inseridas_ou_alteradas=0` e manter a contagem. A carga roda numa transação, para impedir lotes parciais. `--dry-run` gera os mesmos dados, mas não abre conexão ao banco.

> limitação: ao executar com **outra** semente, uma nova série de IDs é adicionada à bronze. Uma nova data-base com a mesma semente atualiza as datas dos IDs anteriores, por desenho. Para auditoria de simulações alternativas, evoluir para `simulation_id` e versões de cenário separadas.

## Limitações e uso para equidade

- A propensão a reportar é **inventada e independente da população**, por escolha pedagógica. Não se trata de um indicador medido nos distritos. Ao misturar SP156 e aplicativo, a distribuição real do SP156 também afeta o fator de equidade; logo, **não está garantido** que a diferença simulada determine o ranking. Verificar o antes/depois no passo 10.
- Os pontos são representativos por distrito. O desvio sintético não foi conferido por teste ponto-em-polígono; portanto, não usar coordenadas simuladas para operação real. Evolução: sorteio espacial somente em interior válido dos polígonos.
- A concentração de relatos no mesmo ponto e na mesma janela foi deliberada para validar clusters H3; fronteiras de células podem separar relatos de fontes diferentes, limitação já prevista no README.
- Nenhum endereço, nome, telefone, texto livre ou identificação pessoal é gerado.
- A data-base é fixa para permitir reprodução, não acompanha a execução diária. Quando este gerador for usado na DAG, a data-base deverá ser um parâmetro versionado da rodada.

## Execução e aceite

```bash
python -m unittest discover -s tests -p 'test_relatos_app_sim.py' -v
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler python -u /opt/airflow/include/ingest/relatos_app_sim.py --dry-run
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler python -u /opt/airflow/include/ingest/relatos_app_sim.py
```

Executar duas vezes com os mesmos parâmetros. Consulta de verificação:

```bash
docker compose exec -T warehouse sh -c 'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS relatos, COUNT(DISTINCT relato_id) AS ids_unicos, COUNT(DISTINCT area_id_raw) AS distritos FROM bronze.raw_relatos_app WHERE _source = '\''APP_SIMULADO'\'';"'
```

Resultado esperado: `relatos = ids_unicos`, `distritos = 96`. O **total exato de relatos** será mostrado pela geração real e não deve ser preenchido com estimativas.
