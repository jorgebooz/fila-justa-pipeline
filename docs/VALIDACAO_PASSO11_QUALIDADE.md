# Passo 11 — Qualidade de dados (Bronze, Silver e Gold)

## Arquivos adicionados

- `include/sql/09_qualidade.sql`: 37 verificações executadas em SQL no PostgreSQL.
- `include/transform/qualidade.py`: executor que **falha com código 1** caso qualquer verificação falhe.
- `tests/test_qualidade.py`: testes unitários sem conexão com o banco.

Nenhum arquivo das etapas anteriores precisa ser substituído.

## 1. Testes locais (Git Bash na raiz do projeto)

```bash
python -m unittest discover -s tests -p 'test_qualidade.py' -v
```

## 2. Rodar no Docker

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/qualidade.py --run-id equidade_2026
```

**Resultado esperado:** todas as 37 linhas começam com `OK:`, seguido de:

```text
QUALIDADE: regras=37 | falhas=0 | run_id=equidade_2026
OK: 37 verificações de qualidade passaram; a DAG pode prosseguir
```

As regras verificam presença de dados em Bronze/Silver/Gold; as chaves de SP156 e do aplicativo; cobertura de cluster, unicidade dos vínculos e consistência por tipo e H3; chaves e datas da Silver, coordenadas no *bounding box* de São Paulo; 96 áreas do IBGE; limites do fator de equidade vindos de `weights.yml`; soma de relatos do fato; score entre 0 e 100 e seus componentes; ranking contíguo sem empates; mart da rodada correta; snapshot climático igual entre Bronze, Silver e Gold, com 96 áreas × 48 horas.

**Atenção:** quando uma nova previsão for ingerida na Bronze, execute a transformação Silver do clima e o score antes da qualidade. O teste foi projetado para **falhar** quando o pipeline estiver parcialmente atualizado.

## 3. Teste negativo reversível (evidência de falha)

O modo abaixo faz `UPDATE` **somente dentro de uma transação não confirmada**, verifica que a regra de soma do score falha e dispara rollback obrigatório. Ele NÃO grava um erro permanente no banco.

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/qualidade.py \
  --run-id equidade_2026 --simular-falha
```

O resultado esperado contém `FALHA ESPERADA` e `ROLLBACK`. Ele serve para demonstrar que o validador detecta uma violação. Para evidenciar **falha da DAG**, no Passo 12 será adicionada uma tarefa de qualidade que chama o comando sem `--simular-falha`; uma execução com violação real deverá ficar vermelha no Airflow. Não use `--simular-falha` na DAG de produção.

## 4. Reutilizar na DAG (Passo 12)

Na etapa depois do score, executar `python -u /opt/airflow/include/transform/qualidade.py --run-id <ID-da-rodada>` por uma tarefa do Airflow. Código de saída diferente de zero faz a tarefa falhar, impedindo publicação parcial. **É necessário preservar o mesmo `run_id` nas etapas de equidade, score e qualidade.**

## Limitações documentadas

- A contagem esperada de áreas é 96 e o horizonte climático é 48 horas porque o MVP é São Paulo.
- A validação de coordenadas é por intervalo de latitude/longitude, não pela geometria municipal.
- O controle de consistência do score foi feito sobre a rodada passada ao script, e as views públicas devem expor essa mesma rodada.
- Verificar apenas a consistência dos dados não prova relevância/justiça social da regra; pesos e fatores seguem sendo hipóteses de negócio.
