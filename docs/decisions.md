# Decisões Arquiteturais e Trade-offs

## ADR-001 — Arquitetura Medallion

**Decisão:** separar o fluxo em Landing, Bronze, Silver e Gold.

**Motivação:** isolar responsabilidades, preservar rastreabilidade, facilitar reprocessamento e disponibilizar dados em níveis crescentes de qualidade.

**Trade-off:** maior número de objetos e etapas, compensado por melhor manutenção e governança.

## ADR-002 — Delta Lake e Unity Catalog

**Decisão:** persistir as camadas processadas como tabelas Delta registradas no Unity Catalog.

**Motivação:** transações ACID, schema management, time travel e organização centralizada dos objetos.

**Trade-off:** maior dependência da plataforma Databricks. O case prioriza a arquitetura solicitada e a execução na Free Edition.

## ADR-003 — CDC com SCD Type 2

**Decisão:** manter histórico para clientes, contas e cartões utilizando SCD2.

**Motivação:** permitir análise histórica e auditoria das alterações cadastrais sem perder o estado anterior.

**Implementação conceitual:**

- chave de negócio da entidade;
- ordenação por timestamp do evento;
- `data_inicio_vigencia` e `data_fim_vigencia`;
- `registro_atual`;
- `registro_excluido`;
- `versao_registro`.

**Trade-off:** aumento do volume e maior complexidade de joins. A Gold simplifica o consumo ao expor apenas a versão atual válida.

## ADR-004 — Chaves substitutas determinísticas

**Decisão:** gerar surrogate keys com SHA-256 a partir das chaves de negócio.

**Motivação:** garantir estabilidade entre reprocessamentos e evitar dependência de sequência/autoincremento.

**Trade-off:** chaves maiores do que inteiros e custo computacional levemente superior, aceitável no contexto do case.

## ADR-005 — Quarentena sem interrupção total

**Decisão:** separar registros inválidos e permitir o processamento dos registros válidos.

**Motivação:** evitar que um pequeno conjunto de erros bloqueie todo o lote, preservando evidência para análise e correção.

**Trade-off:** exige monitoramento da taxa de quarentena e processo operacional de reprocessamento.

## ADR-006 — Schema evolution controlada

**Decisão:** aceitar novas colunas previstas nos cenários sintéticos e utilizar união por nome quando necessário.

**Motivação:** demonstrar resiliência a mudanças compatíveis de contrato.

**Trade-off:** evolução indiscriminada pode degradar qualidade. Em produção, alterações devem ser validadas, versionadas e aprovadas.

## ADR-007 — Gold somente com registros correntes

**Decisão:** dimensões Gold consideram `registro_atual = true` e `registro_excluido = false`.

**Motivação:** oferecer uma visão simples para análise operacional atual.

**Impacto conhecido:** alguns testes de integridade podem gerar `WARNING` quando fatos históricos referenciam membros que não estão na visão corrente da dimensão. Isso não representa perda de transações, pois a reconciliação Silver–Gold permaneceu sem diferença.

## ADR-008 — Notebooks como unidade de entrega

**Decisão:** preservar a implementação principal em notebooks Databricks exportados como **Source**, complementados por módulos Python reutilizáveis (`src/`) e testes automatizados (`tests/`).

**Motivação:** manter compatibilidade direta com o ambiente Databricks, sem abrir mão da reutilização de código e da testabilidade das regras de negócio.

**Trade-off:** parte da lógica permanece distribuída entre notebooks e módulos Python, porém essa abordagem equilibra a experiência de desenvolvimento no Databricks com práticas modernas de engenharia de software.

## ADR-009 — Dados sintéticos reprodutíveis

**Decisão:** utilizar seed fixa e configuração externa para volumes.

**Motivação:** permitir repetibilidade dos cenários e resultados comparáveis.

**Trade-off:** dados sintéticos não representam toda a diversidade e distribuição de um ambiente financeiro real.

## ADR-010 — Persistência Incremental com Delta MERGE

**Decisão**

Substituir a estratégia de persistência baseada em overwrite por Delta MERGE na camada Silver.

**Motivação**

Durante a evolução do case, a estratégia inicial baseada em sobrescrita foi substituída por uma abordagem incremental utilizando Delta MERGE, aproximando a implementação de cenários encontrados em ambientes produtivos.

A nova estratégia permite:

- cargas incrementais;
- reprocessamentos seguros;
- preservação do histórico SCD Type 2;
- redução de escrita desnecessária;
- idempotência.

**Trade-off**

A implementação exige maior complexidade de desenvolvimento quando comparada ao overwrite, porém proporciona maior eficiência operacional, melhor governança e comportamento consistente em reexecuções.

## ADR-011 — Estratégia de Testes Automatizados

**Decisão**

Adicionar uma suíte de testes automatizados para validar as principais regras de negócio implementadas na camada Silver.

A estratégia é composta por duas abordagens complementares:

- testes unitários locais utilizando pytest;
- notebook Databricks para validação integrada das transformações.

**Motivação**

Garantir que futuras alterações não comprometam regras críticas do pipeline.

Entre as regras cobertas estão:

- CDC;
- SCD Type 2;
- deduplicação;
- late arrival;
- delete lógico;
- quarentena;
- integridade referencial;
- idempotência da estratégia de MERGE.

**Resultados**

Execução local:

- 12 testes aprovados.

Execução Databricks:

- 16 validações aprovadas.

**Trade-off**

Maior esforço de manutenção da suíte de testes, compensado pelo aumento da confiabilidade, facilidade de evolução e redução do risco de regressões.
