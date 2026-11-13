# Conferencia final antes da publicacao - Fila Justa

A documentacao abaixo foi preparada a partir dos arquivos executados (tar.gz compartilhado) e das evidencias registradas em `docs/evidencias/`.

## Ja comprovado

- DAG Airflow `fila_justa_pipeline` com 13 tarefas verdes (imagem do usuario arquivada).
- Suite `python -m unittest discover -s tests -v`: 116 testes, OK.
- Validador SQL: 37 regras, falhas=0, com run_id da fila publicada.
- Teste negativo de qualidade com `ROLLBACK` confirmado fora da DAG.
- Bronze SP156 1.525.907, IBGE 96, App 889; Silver 16.553, Gold 11.163 clusters.
- Marts exibindo somente a rodada do fato corrente, apos correcao da publicacao.

## Obrigatorio conferir antes de afirmar entrega integral

1. **Clone limpo + banco vazio:** rodar a DAG completa a partir de um Postgres novo e registrar log e grafo. Os testes `test_bootstrap_banco_vazio` simulam o comportamento, mas nao substituem esse teste integrado. Nao use `down -v` no ambiente principal.
2. **Falha proposital na DAG:** preservar print de tarefa vermelha que falha pelo validador, obtido em ambiente isolado e restauravel. O `--simular-falha` atual faz rollback, mas nao deixa a DAG vermelha.
3. **Sensibilidade de alpha/M:** executar `docs/analise_sensibilidade.sql` e arquivar a saida; acrescentar ao PDF se a avaliacao exigir os numeros do antes/depois em varios cenarios.
4. **Licencas, links e repositorio:** conferir politica atual de uso de cada fonte; criar o GitHub publico e testar o README seguindo-o como avaliador. Inserir URL publica real no envio (nao foi fornecida aqui).
5. **SP156 2026:** a coleta observada abrange 1T-3T; 4T depende de publicacao oficial. Nao declarar cobertura anual integral.
6. **Captura do Airflow:** a imagem atual mostra 13 tarefas verdes, mas foi recortada, sem URL nem identificador visivel. Preferir outro print com dag_id, data e run_id visiveis.

## Comandos de verificacao sem destruir dados

```bash
python -m unittest discover -s tests -v

docker compose exec -T warehouse sh -c \
'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
< docs/evidencias/consultas_validacao.sql

# Sensibilidade: altera apenas a consulta, nao o warehouse
docker compose exec -T warehouse sh -c \
'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
< docs/analise_sensibilidade.sql | tee docs/evidencias/sensibilidade_resultados.txt
```
