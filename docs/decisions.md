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

**Decisão:** preservar a implementação principal em notebooks Databricks exportados como Source.

**Motivação:** compatibilidade direta com o ambiente de execução e transparência das etapas do desafio.

**Trade-off:** menor modularização do que um pacote Spark completo. Em produção, regras reutilizáveis seriam extraídas para módulos e implantadas por CI/CD.

## ADR-009 — Dados sintéticos reprodutíveis

**Decisão:** utilizar seed fixa e configuração externa para volumes.

**Motivação:** permitir repetibilidade dos cenários e resultados comparáveis.

**Trade-off:** dados sintéticos não representam toda a diversidade e distribuição de um ambiente financeiro real.
