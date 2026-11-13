# Passo 6 — Padronização Silver (SP156 + APP_SIMULADO)

## O que faz

Transforma a bronze já carregada em `silver.ocorrencia_limpa`, preservando os dados
originais. SQL no PostgreSQL realiza a seleção de serviços, padronização de status,
normalização de distrito (mapa IBGE oficial), leitura segura de datas e coordenadas,
e contagem de descartes. Python é usado apenas para ler seeds e calcular **H3** no nível
configurado por `include/config/weights.yml` (inicialmente 9). Os descartes ficam em
`silver.descarte_ocorrencia`, por `run_id`, canal e motivo.

Regras de entrada: serviços `incluir=sim` no seed e status `Aberto` ou `Em Andamento`.
O canal APP_SIMULADO usa os 5 tipos já padronizados. O 4º trimestre de 2026 só entra
quando houver arquivo público correspondente na bronze.

**Importante:** os totais da Silver **não serão iguais** aos 1.525.907 registros
SP156, porque serviços fora do recorte, chamados finalizados, áreas sem distrito
reconhecido ou coordenadas inválidas são descartados e contabilizados.

## 1. Copiar o patch

Extraia o patch preservando os arquivos existentes. Acrescente, sem substituir
`docker-compose.yaml`:

- `include/sql/04_silver_padronizacao.sql`
- `include/transform/silver.py`
- `tests/test_silver.py`
- `Dockerfile.h3`
- `docker-compose.h3.yaml`
- `docs/VALIDACAO_PASSO6_SILVER.md`

**Não** execute `docker compose down -v`: isso apagaria os volumes persistentes.

## 2. Preparar Airflow com a dependência H3

Na raiz do projeto, no Git Bash (Windows), rode:

```bash
docker compose -f docker-compose.yaml -f docker-compose.h3.yaml up -d --build
```

Esse comando cria a imagem local do Airflow com `h3==4.5.0`, preservando o
warehouse e seus volumes. Alguns containers Airflow podem ser recriados, mas
os dados do Postgres permanecem nos volumes. A estratégia usa override para
não editar o Compose principal.

Confira a instalação:

```bash
docker compose exec -T airflow-scheduler python -c 'import h3; print(h3.versions())'
```

## 3. Testes unitários

```bash
python -m unittest discover -s tests -p 'test_silver.py' -v
```

Os testes unitários não substituem a primeira validação real do SQL no PostgreSQL.

## 4. Primeira transformação (real)

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
  python -u /opt/airflow/include/transform/silver.py --run-id silver_2026
```

O script informa, por canal, quantos registros foram aceitos e quantos foram
descartados por cada motivo. A contagem exata depende dos registros da bronze.
Em caso de erro, toda a transação Silver é revertida.

## 5. Validação no PostgreSQL

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT canal, COUNT(*) AS registros, COUNT(DISTINCT area_id) AS distritos,
       COUNT(DISTINCT h3) AS celulas_h3
FROM silver.ocorrencia_limpa
GROUP BY canal ORDER BY canal;
"'
```

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT canal, motivo, quantidade
FROM silver.descarte_ocorrencia
WHERE run_id = '\''silver_2026'\''
ORDER BY canal, quantidade DESC;
"'
```

Resultado esperado: **889 relatos simulados** na Silver (caso nenhuma coordenada
sintética viole as regras de validação) e um total positivo do SP156.
Os motivos devem justificar a diferença entre a bronze e a silver.

Conferir chaves e H3:

```bash
docker compose exec -T warehouse sh -c \
'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT COUNT(*) AS total,
       COUNT(DISTINCT ocorrencia_id) AS ids_unicos,
       COUNT(*) FILTER (WHERE h3 IS NULL OR area_id IS NULL OR aberta_em IS NULL) AS invalidos
FROM silver.ocorrencia_limpa;
"'
```

`total = ids_unicos` e `invalidos = 0`.

## 6. Reexecutar (idempotência)

Reexecute **o mesmo comando da seção 4**. Na ausência de mudanças na bronze,
esperamos `inseridas_ou_alteradas=0`, `removidas=0` e contagem total igual.

## Regras e limitações

- Datas SP156 são `MM/DD/YYYY`; linhas sem hora recebem **12:00 (-03:00)** para
  manter tipo `TIMESTAMPTZ`. Isso não significa que a ocorrência começou ao meio-dia.
- `Distrito` sem correspondência nos 96 nomes IBGE é descartado e contabilizado.
  Códigos numéricos não são adivinhados; não inferimos área por subprefeitura.
- Validação espacial usa uma *bounding box*, não o polígono real do distrito.
- H3 usa as coordenadas registradas; relato de célula vizinha pode escapar da
  deduplicação (tratada no Passo 7).
- A exclusão de linhas que deixaram de estar elegíveis também remove ligações
  antigas de `silver.cluster_ocorrencia`. Não executamos o Passo 7 nesta fase.
- O identificador SP156 é `fonte_arquivo + numero_linha`, pois a fonte não traz ID
  explícito. Se o portal reordenar linhas, futuras cargas podem identificar
  as ocorrências como outras linhas; evolução: ID oficial estável, caso exista.
- A rotina trabalha sobre todo o snapshot 2026 e não implementa processamento
  incremental nem múltiplos municípios nesta fase.
- Todas as saídas e métricas precisam ser confirmadas no PostgreSQL local, pois
  o script não foi executado contra o seu banco neste pacote.
