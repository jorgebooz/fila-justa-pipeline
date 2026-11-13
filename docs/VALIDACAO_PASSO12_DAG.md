# Passo 12 — Orquestração completa (Airflow 3.4)

## Arquivos deste patch

- `dags/fila_justa_pipeline.py`: DAG diária, com disparo manual, retries e tarefas que reutilizam os scripts validados nos passos 4–11.
- `tests/test_dag_pipeline.py`: testes locais da ordem das tarefas, estabilidade da rodada, falhas e bootstrap.
- Este guia.

**Não** sobrescreva `.env`, `docker-compose.yaml`, `Dockerfile.h3`, seeds, conectores nem transformações existentes.

## Dependências

- Docker Compose do projeto com os serviços Airflow 3.4 e `warehouse`.
- Imagem `fila-justa-airflow:3.4.0-h3` construída via override `docker-compose.h3.yaml` (H3, psycopg e bibliotecas do Airflow).
- Código e arquivos já validados dos passos 4–11 (incluindo `gerar_pontos_clima.py`).
- Seeds versionados: `mapa_distritos_ibge_sp.csv`, `servicos_interesse.csv`, `dim_tipo.csv` e `weights.yml`.
- `AIRFLOW_CONN_WAREHOUSE` já fornecida pelo Compose, sem credenciais no repositório.

## Testar localmente

Dentro de `fila-justa-data`, no Git Bash:

```bash
python -m unittest discover -s tests -p "test_dag_pipeline.py" -v
python -m unittest discover -s tests -v
python -m py_compile dags/fila_justa_pipeline.py
```

No Docker (iniciar primeiro, sem `down -v`):

```bash
docker compose -f docker-compose.yaml -f docker-compose.h3.yaml up -d --build
docker compose exec -T airflow-dag-processor airflow dags list-import-errors --local
docker compose exec -T airflow-dag-processor airflow dags list --local
```

Os comandos com `--local` conferem o parsing dentro do Airflow; espere a DAG `fila_justa_pipeline` aparecer. Se o Docker encontrar import errors, capture o traceback antes de tentar rodar a DAG.

Abra `http://localhost:8080`, encontre **fila_justa_pipeline**, habilite-a (se necessário) e use **Trigger** para disparar manualmente. Aguarde o status final da DAG no Airflow; um disparo manual não valida sozinho o agendamento diário.

## O que a DAG faz

1. `preparar_warehouse`: cria os schemas e tabelas em banco vazio. Em banco existente não substitui as views Gold já publicadas.
2. `garantir_pontos_distritais`: valida o seed dos 96 distritos; se ausente, gera a partir da malha IBGE.
3. Quatro ingestões (SP156 2026, IBGE Censo 2022, Open-Meteo, app simulado). SP156 e IBGE podem executar após o banco ficar pronto; clima e app dependem do seed dos pontos.
4. `silver_padronizacao` após todas as ingestões.
5. `silver_clusters`, `gold_areas_equidade`, `silver_clima_48h`, `gold_score_fila`.
6. `validar_37_regras`: falha a tarefa e a DAG quando `qualidade.py` retorna código não zero.
7. `conferir_marts_publicados`: exige que ambas as views mostrem apenas a rodada desta DAG e todos os clusters.

Parâmetros: agendamento `0 6 * * *` em `America/Sao_Paulo`; sem catch-up; até duas novas tentativas por tarefa, cinco minutos de intervalo; uma rodada ativa por vez. Equidade usa a janela `[2026-01-01, 2027-01-01)`; a precipitação usa o snapshot atual, não chuva histórica de 2026.

## Identificador da rodada

Cada execução do Airflow recebe um identificador `fj2026_<hash>` estável em retries. Os scripts de **equidade, score e qualidade** recebem sempre esse mesmo `run_id`. Uma **nova** DAG Run cria outra rodada em `gold.fact_equidade_area` e `gold.fact_score`, de propósito: é histórico/auditoria, não duplicação. A view `gold.mart_fila_priorizacao` expõe só a rodada mais recente.

## Conferência no warehouse após DAG verde

```bash
docker compose exec -T warehouse sh -c \
'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT run_id, COUNT(*) AS clusters,
       COUNT(*) FILTER (WHERE score_0_100 NOT BETWEEN 0 AND 100) AS invalidos
FROM gold.mart_fila_priorizacao
GROUP BY run_id;
"'
```

Resultado esperado com a mesma amostra de dados: **uma única rodada**, **11.163 clusters**, zero scores fora da faixa. O total pode mudar se a prefeitura publicar ou revisar dados, ou se os filtros mudarem. Revise a comparação de equidade da Gold e as contagens por camada.

Faça também uma **segunda DAG Run**. A Bronze e a Silver devem conservar chaves únicas e não produzir duplicações. Haverá outra `run_id` nas tabelas históricas Gold, mas a view deve continuar mostrando uma única rodada. Se uma tarefa falhar, salve o log do Airflow e **não** marque o Passo 12 como concluído.

## Evidências recomendadas

- Print da tela **Graph** com todas as 13 tarefas verdes.
- Log da tarefa `validar_37_regras` mostrando 37/37 e o `run_id`.
- Consultas SQL das duas views após uma DAG Run.
- Segunda execução verde + prova de ausência de duplicados na Bronze/Silver.
- Para o Passo 14: evidência de falha proposital da tarefa de qualidade em ambiente controlado. O comando `--simular-falha` demonstra a regra com ROLLBACK, mas não deixa a tarefa vermelha; para a evidência de falha da DAG, será necessário um cenário de teste seguro e reversível.

## Limitações

- O último trimestre de 2026 só entrará na ingestão após publicação oficial.
- A API climática fornece previsões das próximas 48 horas; não substitui chuva histórica de janeiro a setembro de 2026.
- Falha de conexão à prefeitura, IBGE ou Open-Meteo pode provocar retry e, se persistir, deixar a DAG vermelha.
- O seed dos pontos distritais é gerado no volume local `include/seeds/` e precisa ser persistido na cópia do projeto para evitar novo download da malha.
- O primeiro processamento SP156 pode ser longo, pois lê os três CSVs publicados (mais de 1,5 milhão de registros).
- A execução do banco totalmente vazio **ainda precisa de validação real**; testes unitários não substituem um disparo completo no Docker.
