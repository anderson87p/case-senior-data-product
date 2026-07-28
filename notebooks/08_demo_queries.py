# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 08 — Demonstração Analítica da Camada Gold
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Demonstrar o consumo analítico da camada Gold por meio de consultas SQL, evidenciando os principais indicadores de negócio, Data Products e resultados dos Quality Checks.
# MAGIC
# MAGIC **Tecnologias:** Databricks SQL • Delta Lake • Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Apresentar exemplos de consultas analíticas sobre a camada Gold, demonstrando como os dados podem ser utilizados por áreas de Business Intelligence, Analytics e Ciência de Dados.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Demonstrar KPIs executivos.
# MAGIC - Validar o modelo dimensional.
# MAGIC - Consultar a tabela fato.
# MAGIC - Explorar os Data Products.
# MAGIC - Demonstrar indicadores de risco.
# MAGIC - Consultar Features para Machine Learning.
# MAGIC - Validar os resultados dos Quality Checks.
# MAGIC
# MAGIC ## Consultas demonstradas
# MAGIC
# MAGIC - KPIs Executivos
# MAGIC - Evolução Mensal
# MAGIC - Distribuição por Canal
# MAGIC - Distribuição por Tipo
# MAGIC - Taxa de Aprovação
# MAGIC - Top Clientes
# MAGIC - Segmentação de Clientes
# MAGIC - Indicadores de Risco
# MAGIC - Chargebacks
# MAGIC - Features para Machine Learning
# MAGIC - Quality Checks
# MAGIC
# MAGIC ## Entradas
# MAGIC
# MAGIC Todas as tabelas da camada **Gold**.
# MAGIC
# MAGIC ## Resultado esperado
# MAGIC
# MAGIC Demonstrar que a camada Gold está preparada para consumo analítico por ferramentas de BI, Analytics e modelos de Machine Learning.

# COMMAND ----------

# DBTITLE 1,Configuração do ambiente
# Databricks notebook source

CATALOG = "workspace"
GOLD_SCHEMA = "case_gold"

gold_tables = [
    "gold_dim_cliente",
    "gold_dim_conta",
    "gold_dim_cartao",
    "gold_dim_estabelecimento",
    "gold_fato_transacao",
    "gold_cliente_mes",
    "gold_indicadores_risco",
    "gold_features_cliente",
    "gold_quality_check_results"
]

for table_name in gold_tables:
    full_table_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
    total = spark.table(full_table_name).count()

    print(f"{table_name}: {total} registros")

# COMMAND ----------

# DBTITLE 1,KPIs executivos
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT transacao_id) AS quantidade_transacoes,
# MAGIC     ROUND(SUM(valor), 2) AS valor_total_transacionado,
# MAGIC     ROUND(AVG(valor), 2) AS ticket_medio,
# MAGIC     COUNT(DISTINCT cliente_id) AS clientes_com_movimentacao,
# MAGIC     COUNT(DISTINCT conta_id) AS contas_com_movimentacao,
# MAGIC     COUNT(DISTINCT estabelecimento_id) AS estabelecimentos
# MAGIC FROM workspace.case_gold.gold_fato_transacao;

# COMMAND ----------

# DBTITLE 1,Evolução mensal das transações
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     ano_transacao,
# MAGIC     mes_transacao,
# MAGIC     COUNT(DISTINCT transacao_id) AS quantidade_transacoes,
# MAGIC     ROUND(SUM(valor), 2) AS valor_total_transacionado,
# MAGIC     ROUND(AVG(valor), 2) AS ticket_medio
# MAGIC FROM workspace.case_gold.gold_fato_transacao
# MAGIC GROUP BY
# MAGIC     ano_transacao,
# MAGIC     mes_transacao
# MAGIC ORDER BY
# MAGIC     ano_transacao,
# MAGIC     mes_transacao;

# COMMAND ----------

# DBTITLE 1,Distribuição das transações por canal
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     canal_transacao,
# MAGIC     COUNT(DISTINCT transacao_id) AS quantidade_transacoes,
# MAGIC     ROUND(SUM(valor), 2) AS valor_total_transacionado,
# MAGIC     ROUND(AVG(valor), 2) AS ticket_medio,
# MAGIC     ROUND(
# MAGIC         COUNT(DISTINCT transacao_id) * 100.0 /
# MAGIC         SUM(COUNT(DISTINCT transacao_id)) OVER (),
# MAGIC         2
# MAGIC     ) AS percentual_transacoes
# MAGIC FROM workspace.case_gold.gold_fato_transacao
# MAGIC GROUP BY canal_transacao
# MAGIC ORDER BY quantidade_transacoes DESC;

# COMMAND ----------

# DBTITLE 1,Distribuição das transações por tipo
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     tipo_transacao,
# MAGIC     COUNT(DISTINCT transacao_id) AS quantidade_transacoes,
# MAGIC     ROUND(SUM(valor), 2) AS valor_total_transacionado,
# MAGIC     ROUND(AVG(valor), 2) AS ticket_medio
# MAGIC FROM workspace.case_gold.gold_fato_transacao
# MAGIC GROUP BY tipo_transacao
# MAGIC ORDER BY valor_total_transacionado DESC;

# COMMAND ----------

# DBTITLE 1,Taxa de aprovação das transaçõe
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     tipo_transacao,
# MAGIC     COUNT(DISTINCT transacao_id) AS quantidade_transacoes,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN status_transacao = 'APROVADA' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS quantidade_aprovadas,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN status_transacao <> 'APROVADA' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS quantidade_nao_aprovadas,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN status_transacao = 'APROVADA' THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) * 100.0 / COUNT(*),
# MAGIC         2
# MAGIC     ) AS percentual_aprovacao
# MAGIC
# MAGIC FROM workspace.case_gold.gold_fato_transacao
# MAGIC GROUP BY tipo_transacao
# MAGIC ORDER BY percentual_aprovacao DESC;

# COMMAND ----------

# DBTITLE 1,Top clientes por volume financeiro
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     cliente.cliente_id,
# MAGIC     cliente.nome,
# MAGIC     cliente.segmento_cliente,
# MAGIC     cliente.uf,
# MAGIC     features.quantidade_transacoes,
# MAGIC     ROUND(features.valor_total_transacionado, 2)
# MAGIC         AS valor_total_transacionado,
# MAGIC     ROUND(features.ticket_medio, 2) AS ticket_medio,
# MAGIC     features.quantidade_contas,
# MAGIC     features.quantidade_cartoes,
# MAGIC     features.classificacao_risco_cliente
# MAGIC
# MAGIC FROM workspace.case_gold.gold_features_cliente AS features
# MAGIC
# MAGIC INNER JOIN workspace.case_gold.gold_dim_cliente AS cliente
# MAGIC     ON features.cliente_id = cliente.cliente_id
# MAGIC
# MAGIC ORDER BY features.valor_total_transacionado DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Segmentação de clientes
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     segmento_cliente,
# MAGIC     COUNT(DISTINCT cliente_id) AS quantidade_clientes,
# MAGIC     ROUND(AVG(saldo_total), 2) AS saldo_medio,
# MAGIC     ROUND(AVG(limite_credito_total), 2) AS limite_credito_medio,
# MAGIC     ROUND(AVG(valor_total_transacionado), 2)
# MAGIC         AS movimentacao_media_cliente,
# MAGIC     ROUND(AVG(ticket_medio), 2) AS ticket_medio
# MAGIC FROM workspace.case_gold.gold_features_cliente
# MAGIC GROUP BY segmento_cliente
# MAGIC ORDER BY quantidade_clientes DESC;

# COMMAND ----------

# DBTITLE 1,Indicadores de risco dos cliente
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     classificacao_risco_cliente,
# MAGIC     COUNT(DISTINCT cliente_id) AS quantidade_clientes,
# MAGIC     SUM(quantidade_transacoes) AS quantidade_transacoes,
# MAGIC     SUM(quantidade_eventos_risco) AS quantidade_eventos_risco,
# MAGIC     SUM(quantidade_transacoes_estornadas)
# MAGIC         AS quantidade_transacoes_estornadas,
# MAGIC     ROUND(
# MAGIC         COUNT(DISTINCT cliente_id) * 100.0 /
# MAGIC         SUM(COUNT(DISTINCT cliente_id)) OVER (),
# MAGIC         2
# MAGIC     ) AS percentual_clientes
# MAGIC FROM workspace.case_gold.gold_indicadores_risco
# MAGIC GROUP BY classificacao_risco_cliente
# MAGIC ORDER BY
# MAGIC     CASE classificacao_risco_cliente
# MAGIC         WHEN 'ALTO' THEN 1
# MAGIC         WHEN 'MEDIO' THEN 2
# MAGIC         WHEN 'BAIXO' THEN 3
# MAGIC         ELSE 4
# MAGIC     END;

# COMMAND ----------

# DBTITLE 1,Transações de alto risco
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     fato.transacao_id,
# MAGIC     fato.data_transacao,
# MAGIC     cliente.cliente_id,
# MAGIC     cliente.nome,
# MAGIC     cliente.segmento_cliente,
# MAGIC     fato.conta_id,
# MAGIC     fato.cartao_id,
# MAGIC     fato.tipo_transacao,
# MAGIC     fato.canal_transacao,
# MAGIC     fato.valor,
# MAGIC     fato.quantidade_eventos_risco,
# MAGIC     fato.nivel_risco,
# MAGIC     fato.possui_estorno,
# MAGIC     fato.motivo_estorno
# MAGIC
# MAGIC FROM workspace.case_gold.gold_fato_transacao AS fato
# MAGIC
# MAGIC LEFT JOIN workspace.case_gold.gold_dim_cliente AS cliente
# MAGIC     ON fato.cliente_id = cliente.cliente_id
# MAGIC
# MAGIC WHERE fato.nivel_risco = 'ALTO'
# MAGIC
# MAGIC ORDER BY
# MAGIC     fato.possui_estorno DESC,
# MAGIC     fato.valor DESC
# MAGIC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Análise de estornos (Chargebacks)
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     motivo_estorno,
# MAGIC     COUNT(DISTINCT transacao_id) AS quantidade_transacoes_estornadas,
# MAGIC     ROUND(SUM(valor), 2) AS valor_total_estornado,
# MAGIC     ROUND(AVG(valor), 2) AS ticket_medio_estornado
# MAGIC FROM workspace.case_gold.gold_fato_transacao
# MAGIC WHERE possui_estorno = TRUE
# MAGIC GROUP BY motivo_estorno
# MAGIC ORDER BY valor_total_estornado DESC;

# COMMAND ----------

# DBTITLE 1,Correlação entre risco e estornos
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     nivel_risco,
# MAGIC     COUNT(DISTINCT transacao_id) AS quantidade_transacoes,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN possui_estorno = TRUE THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS quantidade_estornadas,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN possui_estorno = TRUE THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) * 100.0 / COUNT(*),
# MAGIC         2
# MAGIC     ) AS percentual_estorno,
# MAGIC
# MAGIC     ROUND(SUM(valor), 2) AS valor_total_transacionado
# MAGIC
# MAGIC FROM workspace.case_gold.gold_fato_transacao
# MAGIC GROUP BY nivel_risco
# MAGIC ORDER BY
# MAGIC     CASE nivel_risco
# MAGIC         WHEN 'ALTO' THEN 1
# MAGIC         WHEN 'MEDIO' THEN 2
# MAGIC         WHEN 'BAIXO' THEN 3
# MAGIC         WHEN 'SEM_RISCO' THEN 4
# MAGIC         ELSE 5
# MAGIC     END;

# COMMAND ----------

# DBTITLE 1,Análise dos estabelecimentos
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     estabelecimento_id,
# MAGIC     quantidade_transacoes,
# MAGIC     ROUND(valor_total_transacionado, 2) AS valor_total_transacionado,
# MAGIC     primeira_data_transacao,
# MAGIC     ultima_data_transacao
# MAGIC FROM workspace.case_gold.gold_dim_estabelecimento
# MAGIC ORDER BY valor_total_transacionado DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Clientes inativos e Features para Machine Learning
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     cliente_id,
# MAGIC     segmento_cliente,
# MAGIC     status_cliente,
# MAGIC     uf,
# MAGIC     quantidade_contas,
# MAGIC     quantidade_cartoes,
# MAGIC     saldo_total,
# MAGIC     limite_credito_total,
# MAGIC     limite_cartao_total
# MAGIC FROM workspace.case_gold.gold_features_cliente
# MAGIC WHERE quantidade_transacoes = 0
# MAGIC ORDER BY saldo_total DESC;

# COMMAND ----------

# DBTITLE 1,Resumo dos Quality Checks
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     cliente_id,
# MAGIC     segmento_cliente,
# MAGIC     status_cliente,
# MAGIC     uf,
# MAGIC     quantidade_contas,
# MAGIC     quantidade_cartoes,
# MAGIC     saldo_total,
# MAGIC     limite_credito_total,
# MAGIC     limite_cartao_total,
# MAGIC     quantidade_transacoes,
# MAGIC     valor_total_transacionado,
# MAGIC     ticket_medio,
# MAGIC     quantidade_transacoes_estornadas,
# MAGIC     percentual_estorno,
# MAGIC     quantidade_transacoes_pix,
# MAGIC     percentual_pix,
# MAGIC     quantidade_transacoes_alto_risco,
# MAGIC     classificacao_risco_cliente
# MAGIC FROM workspace.case_gold.gold_features_cliente
# MAGIC WHERE quantidade_transacoes > 0;

# COMMAND ----------

# DBTITLE 1,Validações classificadas como WARNING
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     status,
# MAGIC     COUNT(*) AS quantidade_checks
# MAGIC FROM workspace.case_gold.gold_quality_check_results
# MAGIC GROUP BY status
# MAGIC ORDER BY
# MAGIC     CASE status
# MAGIC         WHEN 'FAIL' THEN 1
# MAGIC         WHEN 'WARNING' THEN 2
# MAGIC         WHEN 'PASS' THEN 3
# MAGIC         ELSE 4
# MAGIC     END;

# COMMAND ----------

# DBTITLE 1,Encerramento da demonstração analítica
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     check_name,
# MAGIC     table_name,
# MAGIC     status,
# MAGIC     metric_value,
# MAGIC     expected_value,
# MAGIC     description,
# MAGIC     quality_check_timestamp
# MAGIC FROM workspace.case_gold.gold_quality_check_results
# MAGIC WHERE status IN ('WARNING', 'FAIL')
# MAGIC ORDER BY
# MAGIC     CASE status
# MAGIC         WHEN 'FAIL' THEN 1
# MAGIC         WHEN 'WARNING' THEN 2
# MAGIC         ELSE 3
# MAGIC     END,
# MAGIC     table_name,
# MAGIC     check_name;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resultado
# MAGIC
# MAGIC A demonstração analítica foi concluída com sucesso.
# MAGIC
# MAGIC As consultas executadas evidenciam que a camada Gold disponibiliza dados consistentes e preparados para consumo por diferentes perfis de usuários, incluindo analistas de negócio, cientistas de dados e aplicações analíticas.
# MAGIC
# MAGIC ### Funcionalidades demonstradas
# MAGIC
# MAGIC - KPIs executivos
# MAGIC - Modelo dimensional
# MAGIC - Tabela Fato
# MAGIC - Data Products
# MAGIC - Indicadores de risco
# MAGIC - Features para Machine Learning
# MAGIC - Quality Checks
# MAGIC - Consultas analíticas em SQL
# MAGIC
# MAGIC ### Conclusão
# MAGIC
# MAGIC O pipeline Lakehouse foi implementado de forma completa, contemplando todas as etapas desde a geração dos dados até sua disponibilização para consumo analítico, aplicando boas práticas de Engenharia de Dados com Databricks, Delta Lake e Unity Catalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Projeto concluído
# MAGIC
# MAGIC **Senior Data Engineering Case – Lakehouse Data Platform**