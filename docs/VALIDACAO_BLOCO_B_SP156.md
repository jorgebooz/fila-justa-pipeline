# Bloco B — ingestão real do SP156 (2026)

## Recorte e privacidade

- Usa **todos os CSVs trimestrais de 2026 disponibilizados** pelo catálogo CKAN da Prefeitura de São Paulo, um por trimestre. O 4º trimestre não é presumido publicado.
- Lê arquivos completos (todos os serviços e status); filtros de interesse pertencem à camada silver. Não inventa nem simula registros do SP156.
- `fonte_arquivo = sp156_2026_T{n}` e `numero_linha = linha física de dados` identificam cada registro. A chave única já existia no DDL bronze.
- Não persiste `Logradouro`, `Número`, `CEP` ou `Assunto`: política de minimização de dados. Não salva CSV bruto em disco. `Latitude` e `Longitude` permanecem como necessários para deduplicação e serão tratados como dados operacionais com acesso restrito ao banco.
- Download é por streaming com validação de TLS habilitada. O CSV é lido com detecção inicial de UTF-8 (com ou sem BOM), usando Latin-1 quando a amostra não é UTF-8 válida; a codificação selecionada é registrada no log. Limitação: a detecção usa apenas a amostra inicial; em caso de codificação mista ou byte inválido mais adiante, a carga falhará e fará rollback do trimestre. O script possui `--insecure` somente para diagnóstico, **não recomendado**.
- Cada trimestre é uma transação independente: interrupção no download cancela aquele trimestre, sem deixar meia carga. Trimestres anteriores já concluídos permanecem carregados e podem ser reprocessados.
- Idempotência: `INSERT ... ON CONFLICT(fonte_arquivo, numero_linha) DO UPDATE ... WHERE ... IS DISTINCT FROM ...`; uma execução idêntica não inclui novos registros nem altera `_ingested_at`. Alterações do arquivo publicado atualizam a linha. **Limitação:** se a fonte retirar linhas antigas, o pipeline não as exclui automaticamente; uma reconciliação de snapshots será necessária.
- **Limitação:** a identidade por posição de linha pressupõe ordem estável do CSV publicado. Se a prefeitura reordenar o CSV, o UPSERT atualiza as linhas pela posição, e não por um protocolo de identidade de ocorrência (a fonte examinada não apresenta identificador). É importante registrar data e versão da fonte antes de uso operacional.

## 1. Preparação

Na pasta `fila-justa-data`, com `.env` preservado:

```bash
docker compose ps
python -m unittest discover -s tests -v
```

Confirme que `warehouse` e `airflow-scheduler` estejam ativos. Seu DDL do Bloco A já foi executado; não é necessário recriar o banco. Para verificar a tabela:

```bash
docker compose exec -T warehouse sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) FROM bronze.raw_sp156;"'
```

## 2. Ingerir o ano 2026 (trimestres publicados)

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler python /opt/airflow/include/ingest/sp156.py --ano 2026
```

O `airflow-scheduler` já possui acesso à rede Docker e ao warehouse via `AIRFLOW_CONN_WAREHOUSE`, bem como `psycopg`. **Esta execução é manual**; a DAG integrada será implementada no Bloco C. Não declare a DAG completa como executada.

Saída esperada, sem contagens pré-fixadas:

```text
Ano 2026 | trimestres disponíveis: [...] | não publicados: [...]
Iniciando Dados do SP156 - ...
T1: fonte=..., inseridas_ou_alteradas=..., bronze=...
...
OK: ingestão SP156 concluída
```

Primeira execução pode baixar e processar centenas de MB; o tempo depende da fonte, da rede e do disco. Se falhar por SSL, copie a mensagem completa para diagnóstico (não habilite `--insecure` automaticamente).

## 3. Conferência do conteúdo e idempotência

A tabela possui contagem por arquivo e chave única. Veja o resultado:

```bash
docker compose exec -T warehouse sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT fonte_arquivo, COUNT(*) AS registros, MIN(numero_linha) AS primeira, MAX(numero_linha) AS ultima FROM bronze.raw_sp156 WHERE fonte_arquivo LIKE '\''sp156_2026_T%'\'' GROUP BY fonte_arquivo ORDER BY fonte_arquivo;"'
```

Registre as contagens e **execute o mesmo comando de ingestão novamente**:

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler python /opt/airflow/include/ingest/sp156.py --ano 2026
```

A segunda execução, se os CSVs publicados não mudaram, deve apresentar `inseridas_ou_alteradas=0` em cada trimestre. Rode novamente a consulta de contagens: devem ser iguais.

Verificação de duplicação:

```bash
docker compose exec -T warehouse sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT fonte_arquivo, numero_linha, COUNT(*) FROM bronze.raw_sp156 GROUP BY 1,2 HAVING COUNT(*) > 1 LIMIT 10;"'
```

Resultado esperado: **0 linhas** (também garantido pela `UNIQUE`).

## 4. O que ainda NÃO foi realizado

- Ingestão real executada e observada no Docker do avaliador (precisa rodar os comandos acima).
- Ingestões IBGE, clima e app simulado.
- Transformações silver/gold, DAG end-to-end e testes de qualidade em execução orquestrada.
- Validação do glossário dos serviços e tratamento territorial dos distritos incompletos.
