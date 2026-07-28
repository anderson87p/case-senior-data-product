# Contratos de Dados

## Convenções gerais

- Arquivos de entrada: CSV UTF-8.
- Chaves de negócio não devem ser nulas.
- Datas e timestamps devem ser convertíveis para os tipos esperados.
- Valores monetários devem ser não negativos quando aplicável.
- Operações CDC aceitas: `I`, `U` e `D`.
- Registros incompatíveis são enviados para `case_quarantine` com o motivo da rejeição.
- Colunas de metadados técnicos são preservadas para rastreabilidade.

## Entidades de entrada

### Clientes CDC

| Campo principal | Regra |
|---|---|
| `cliente_id` | Chave de negócio obrigatória |
| `operacao_cdc` | Domínio `I`, `U`, `D` |
| timestamp do evento | Obrigatório para ordenação do CDC |
| atributos cadastrais | Normalizados na Silver |

Saída Silver: `silver_clientes`, com histórico SCD2.

### Contas CDC

| Campo principal | Regra |
|---|---|
| `conta_id` | Chave de negócio obrigatória |
| `cliente_id` | Referência ao cliente |
| `operacao_cdc` | Domínio `I`, `U`, `D` |
| `saldo` | Valor numérico válido |
| `limite_credito` | Valor numérico válido |

Saída Silver: `silver_contas`, com histórico SCD2.

### Cartões CDC

| Campo principal | Regra |
|---|---|
| `cartao_id` | Chave de negócio obrigatória |
| `conta_id` | Referência à conta |
| `operacao_cdc` | Domínio `I`, `U`, `D` |
| `limite_cartao` | Valor numérico válido |
| atributos de status/tipo | Devem respeitar os domínios definidos no notebook de validação |

Saída Silver: `silver_cartoes`, com histórico SCD2.

### Transações

| Campo principal | Regra |
|---|---|
| `transacao_id` | Chave única obrigatória |
| `conta_id` | Referência à conta |
| `cartao_id` | Referência opcional ao cartão, conforme tipo/canal |
| `data_transacao` | Timestamp válido |
| `valor` | Numérico e maior que zero |
| `status` | Domínio controlado |
| `canal` e `tipo_transacao` | Domínios controlados |
| `estabelecimento_id` | Identificador do estabelecimento quando aplicável |

Saída Silver: `silver_transacoes`. Granularidade Gold: uma linha por `transacao_id`.

### Eventos de risco

| Campo principal | Regra |
|---|---|
| `evento_risco_id` | Chave única obrigatória |
| `transacao_id` | Referência à transação |
| `data_evento` | Timestamp válido |
| `nivel_risco` | Domínio controlado, incluindo baixo, médio e alto |

Saída Silver: `silver_eventos_risco`.

### Estornos

| Campo principal | Regra |
|---|---|
| `estorno_id` | Chave única obrigatória |
| `transacao_id` | Referência à transação |
| `data_estorno` | Timestamp válido |
| `valor_estorno` | Numérico válido |
| `motivo_estorno` | Texto normalizado |

Saída Silver: `silver_estornos`.

## Campos técnicos SCD2

Aplicáveis às entidades cadastrais na Silver:

| Campo | Descrição |
|---|---|
| `data_inicio_vigencia` | Início da validade da versão |
| `data_fim_vigencia` | Fim da validade; nulo para versão corrente |
| `registro_atual` | Indica a versão corrente |
| `registro_excluido` | Indica delete lógico |
| `versao_registro` | Número sequencial da versão por chave de negócio |
| timestamp de processamento | Momento de processamento na camada |

## Contratos da Gold

### `gold_dim_cliente`

Uma linha por cliente atual e não excluído. Chave substituta: `cliente_sk`.

### `gold_dim_conta`

Uma linha por conta atual e não excluída. Chaves principais: `conta_sk`, `conta_id`, `cliente_sk`.

### `gold_dim_cartao`

Uma linha por cartão atual e não excluído. Relaciona cartão, conta e cliente.

### `gold_dim_estabelecimento`

Uma linha por estabelecimento derivado das transações, com primeira/última data, quantidade e valor transacionado.

### `gold_fato_transacao`

Granularidade: uma linha por transação distinta. Inclui chaves dimensionais, atributos temporais, métricas financeiras, risco e estorno.

### `gold_cliente_mes`

Granularidade: cliente, ano e mês. Métricas de quantidade, valor, ticket, aprovação, risco e estorno.

### `gold_indicadores_risco`

Granularidade: cliente com eventos de risco. Agrega transações por nível de risco, estornos e indicadores derivados.

### `gold_features_cliente`

Granularidade: cliente atual. Reúne contas, cartões, saldos, limites, comportamento transacional, risco e inatividade para consumo analítico ou Machine Learning.

## Compatibilidade e evolução

Alterações aditivas são toleradas nos cenários de schema evolution. Mudanças destrutivas, alteração de semântica ou tipo incompatível devem gerar uma nova versão do contrato e validação explícita antes da promoção para Silver.
