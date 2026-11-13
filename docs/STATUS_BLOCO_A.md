# Bloco A — balanço da entrega

- [x] Revisão do relatório local SP156 (484.232 linhas) e do teste Open-Meteo
- [x] Localização oficial de arquivos do IBGE por distrito
- [x] Atualização de `docs/fontes.md` com números reais e pendências
- [x] DDL inicial para bronze/silver/gold e views de contrato
- [x] Configuração dos pesos e seed **hipotético** de categorias
- [ ] Baixar e inspecionar o arquivo básico do IBGE na máquina executora
- [ ] Conferir catálogo real dos serviços SP156 de interesse
- [x] Testar DDL com PostgreSQL via Docker (usuário confirmou execução sem erros e listagem de 14 tabelas + 2 views)
- [ ] Implementar ingestão e transformações (Bloco B)

Arquivos pré-existentes preservados na pasta de trabalho. O ZIP compartilhável não
contém `.env`, logs nem `config/airflow.cfg`; eles continuam disponíveis na cópia local
do usuário. Nenhuma execução do pipeline final foi alegada.

- [x] Recorte do SP156 definido: ano civil 2026 completo, condicionado à publicação do quarto trimestre.
- [ ] Validar resultado da exploração anual (1T a 4T); script atualizado para somar trimestres disponíveis e avisar faltantes.

## Validação do catálogo anual (após execução local)

- Catálogo anual recebido: 880 nomes de serviço e 1.525.907 solicitações somadas.
- Criado `include/seeds/servicos_interesse.csv` com 10 serviços selecionados para 5 categorias do modelo.
- `docs/validacao_catalogo_2026.md` registra quantidades e critérios; `docs/servicos_revisao.csv` preserva candidatos ambíguos.
- A estrutura do PostgreSQL foi verificada pelo usuário (14 tabelas, 2 views); a validação do IBGE e da junção territorial continua pendente.
- A seleção é preliminar: falta conferir glossário e cobertura de status/localização por serviço. Não houve carga de dados.

## Validação IBGE (arquivo recebido)

- O CSV básico foi inspecionado: 96 distritos e colunas de população/domicílios identificadas.
- Criados `docs/validacao_ibge_2022.md`, `include/seeds/mapa_distritos_ibge_sp.csv` e `scripts/validar_ibge.py`.
- Mapeamento por nomes passa no relatório SP156 do 3º trimestre, mas valores numéricos e vazios não podem ser presumidos.
- O Bloco A ainda não valida uma ingestão real nem a cobertura total do ano de 2026.
