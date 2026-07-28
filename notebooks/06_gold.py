# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 06 — Construção da Camada Gold
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Construir o modelo dimensional da camada Gold, composto por dimensões, tabela fato e Data Products, disponibilizando dados analíticos prontos para consumo por Business Intelligence, Analytics e Machine Learning.
# MAGIC
# MAGIC **Tecnologias:** Databricks • PySpark • Delta Lake • Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Transformar os dados historizados da camada Silver em um modelo dimensional otimizado para consultas analíticas, preservando a consistência dos dados e disponibilizando produtos analíticos de alto valor para o negócio.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Construir as dimensões analíticas.
# MAGIC - Gerar Surrogate Keys determinísticas.
# MAGIC - Construir a tabela Fato de Transações.
# MAGIC - Criar Data Products para Analytics.
# MAGIC - Disponibilizar Features para Machine Learning.
# MAGIC - Persistir as tabelas Gold em Delta Lake.
# MAGIC - Validar a consistência dos dados processados.
# MAGIC - Realizar reconciliação entre Silver e Gold.
# MAGIC
# MAGIC ## Recursos implementados
# MAGIC
# MAGIC - Star Schema
# MAGIC - Modelo Dimensional
# MAGIC - Surrogate Keys (SHA-256)
# MAGIC - Delta Lake
# MAGIC - Data Products
# MAGIC - Features para Machine Learning
# MAGIC - Integridade Referencial
# MAGIC - Reconciliação dos dados
# MAGIC
# MAGIC ## Entradas
# MAGIC
# MAGIC - `workspace.case_silver.silver_clientes`
# MAGIC - `workspace.case_silver.silver_contas`
# MAGIC - `workspace.case_silver.silver_cartoes`
# MAGIC - `workspace.case_silver.silver_transacoes`
# MAGIC - `workspace.case_silver.silver_eventos_risco`
# MAGIC - `workspace.case_silver.silver_estornos`
# MAGIC
# MAGIC ## Saídas
# MAGIC
# MAGIC ### Dimensões
# MAGIC
# MAGIC - `workspace.case_gold.gold_dim_cliente`
# MAGIC - `workspace.case_gold.gold_dim_conta`
# MAGIC - `workspace.case_gold.gold_dim_cartao`
# MAGIC - `workspace.case_gold.gold_dim_estabelecimento`
# MAGIC
# MAGIC ### Tabela Fato
# MAGIC
# MAGIC - `workspace.case_gold.gold_fato_transacao`
# MAGIC
# MAGIC ### Data Products
# MAGIC
# MAGIC - `workspace.case_gold.gold_cliente_mes`
# MAGIC - `workspace.case_gold.gold_indicadores_risco`
# MAGIC - `workspace.case_gold.gold_features_cliente`
# MAGIC
# MAGIC ## Próximo Notebook
# MAGIC
# MAGIC **07_quality_checks**
# MAGIC
# MAGIC Execução das validações finais de qualidade, consistência e integridade das tabelas da camada Gold.

# COMMAND ----------

# DBTITLE 1,Configuração do ambiente
# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SILVER_SCHEMA = "case_silver"
GOLD_SCHEMA = "case_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

print(f"Camada Gold configurada: {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Definição das tabelas e parâmetros da camada Gold
silver_clientes = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_clientes"
)

silver_contas = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_contas"
)

silver_cartoes = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_cartoes"
)

silver_transacoes = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_transacoes"
)

silver_eventos_risco = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_eventos_risco"
)

silver_estornos = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_estornos"
)

print("Tabelas Silver carregadas com sucesso.")

# COMMAND ----------

# DBTITLE 1,Leitura das tabelas da camada Silver
clientes_ativos = (
    silver_clientes
    .filter(
        (F.coalesce(F.col("registro_atual"), F.lit(False)) == True) &
        (F.coalesce(F.col("registro_excluido"), F.lit(False)) == False)
    )
)

contas_ativas = (
    silver_contas
    .filter(
        (F.coalesce(F.col("registro_atual"), F.lit(False)) == True) &
        (F.coalesce(F.col("registro_excluido"), F.lit(False)) == False)
    )
)

cartoes_ativos = (
    silver_cartoes
    .filter(
        (F.coalesce(F.col("registro_atual"), F.lit(False)) == True) &
        (F.coalesce(F.col("registro_excluido"), F.lit(False)) == False)
    )
)

print(f"Clientes ativos: {clientes_ativos.count()}")
print(f"Contas ativas: {contas_ativas.count()}")
print(f"Cartões ativos: {cartoes_ativos.count()}")

# COMMAND ----------

# DBTITLE 1,Definição das funções auxiliares
gold_dim_cliente = (
    clientes_ativos
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("CLIENTE"),
                F.col("cliente_id")
            ),
            256
        ).alias("cliente_sk"),

        F.col("cliente_id"),
        F.col("cpf"),
        F.col("nome"),
        F.col("data_nascimento"),
        F.col("cidade"),
        F.col("uf"),
        F.col("segmento_cliente"),
        F.col("status_cliente"),

        F.current_timestamp().alias("gold_processing_timestamp")
    )
    .dropDuplicates(["cliente_id"])
)

display(gold_dim_cliente.limit(10))

# COMMAND ----------

# DBTITLE 1,Construção da dimensão Cliente
gold_dim_conta = (
    contas_ativas.alias("conta")
    .join(
        gold_dim_cliente
        .select("cliente_id", "cliente_sk")
        .alias("cliente"),
        on="cliente_id",
        how="left"
    )
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("CONTA"),
                F.col("conta.conta_id")
            ),
            256
        ).alias("conta_sk"),

        F.col("conta.conta_id"),
        F.col("cliente_sk"),
        F.col("conta.cliente_id"),
        F.col("conta.agencia"),
        F.col("conta.data_abertura"),
        F.col("conta.status_conta"),
        F.col("conta.tipo_conta"),
        F.col("conta.saldo"),
        F.col("conta.limite_credito"),
        F.col("conta.modalidade_atendimento"),

        F.current_timestamp().alias("gold_processing_timestamp")
    )
    .dropDuplicates(["conta_id"])
)

display(gold_dim_conta.limit(10))

# COMMAND ----------

# DBTITLE 1,Construção da dimensão Conta
gold_dim_cartao = (
    cartoes_ativos.alias("cartao")
    .join(
        gold_dim_conta
        .select(
            "conta_id",
            "conta_sk",
            "cliente_id",
            "cliente_sk"
        )
        .alias("conta"),
        on="conta_id",
        how="left"
    )
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("CARTAO"),
                F.col("cartao.cartao_id")
            ),
            256
        ).alias("cartao_sk"),

        F.col("cartao.cartao_id"),
        F.col("conta.conta_sk"),
        F.col("cartao.conta_id"),
        F.col("conta.cliente_sk"),
        F.col("conta.cliente_id"),
        F.col("cartao.bandeira"),
        F.col("cartao.tipo_cartao"),
        F.col("cartao.status_cartao"),
        F.col("cartao.data_emissao"),
        F.col("cartao.limite_cartao"),
        F.col("cartao.cartao_virtual"),

        F.current_timestamp().alias("gold_processing_timestamp")
    )
    .dropDuplicates(["cartao_id"])
)

display(gold_dim_cartao.limit(10))

# COMMAND ----------

# DBTITLE 1,Construção da dimensão Cartão
dimensoes = {
    "gold_dim_cliente": gold_dim_cliente,
    "gold_dim_conta": gold_dim_conta,
    "gold_dim_cartao": gold_dim_cartao
}

for nome_tabela, dataframe in dimensoes.items():
    total = dataframe.count()

    print("=" * 80)
    print(f"Tabela: {nome_tabela}")
    print(f"Quantidade de registros: {total}")
    print(f"Quantidade de colunas: {len(dataframe.columns)}")

# COMMAND ----------

# DBTITLE 1,Construção da dimensão Estabelecimento
gold_dim_estabelecimento = (
    silver_transacoes
    .filter(F.col("estabelecimento_id").isNotNull())
    .groupBy("estabelecimento_id")
    .agg(
        F.min("data_transacao").alias("primeira_data_transacao"),
        F.max("data_transacao").alias("ultima_data_transacao"),
        F.countDistinct("transacao_id").alias("quantidade_transacoes"),
        F.sum("valor").alias("valor_total_transacionado")
    )
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("ESTABELECIMENTO"),
                F.col("estabelecimento_id")
            ),
            256
        ).alias("estabelecimento_sk"),

        F.col("estabelecimento_id"),
        F.col("primeira_data_transacao"),
        F.col("ultima_data_transacao"),
        F.col("quantidade_transacoes"),
        F.col("valor_total_transacionado"),
        F.current_timestamp().alias("gold_processing_timestamp")
    )
)

display(gold_dim_estabelecimento.limit(10))

# COMMAND ----------

# DBTITLE 1,Construção da Fato de Transações
risco_por_transacao = (
    silver_eventos_risco
    .groupBy("transacao_id")
    .agg(
        F.countDistinct("evento_risco_id").alias("quantidade_eventos_risco"),

        F.max("data_evento").alias("ultima_data_evento_risco"),

        F.max(
            F.when(F.col("nivel_risco") == "ALTO", F.lit(3))
             .when(F.col("nivel_risco") == "MEDIO", F.lit(2))
             .when(F.col("nivel_risco") == "BAIXO", F.lit(1))
             .otherwise(F.lit(0))
        ).alias("nivel_risco_ordem")
    )
    .withColumn(
        "nivel_risco",
        F.when(F.col("nivel_risco_ordem") == 3, F.lit("ALTO"))
         .when(F.col("nivel_risco_ordem") == 2, F.lit("MEDIO"))
         .when(F.col("nivel_risco_ordem") == 1, F.lit("BAIXO"))
         .otherwise(F.lit("SEM_RISCO"))
    )
    .drop("nivel_risco_ordem")
)

display(risco_por_transacao.limit(10))

# COMMAND ----------

# DBTITLE 1,Construção do Data Product Cliente Mês
estorno_por_transacao = (
    silver_estornos
    .groupBy("transacao_id")
    .agg(
        F.countDistinct("estorno_id").alias("quantidade_estornos"),
        F.max("data_estorno").alias("ultima_data_estorno"),
        F.first("motivo_estorno", ignorenulls=True).alias("motivo_estorno")
    )
    .withColumn(
        "possui_estorno",
        F.when(F.col("quantidade_estornos") > 0, F.lit(True))
         .otherwise(F.lit(False))
    )
)

display(estorno_por_transacao.limit(10))

# COMMAND ----------

# DBTITLE 1,Construção do Data Product Indicadores de Risco
gold_fato_transacao = (
    silver_transacoes.alias("transacao")

    .join(
        gold_dim_conta
        .select(
            "conta_id",
            "conta_sk",
            "cliente_id",
            "cliente_sk"
        )
        .alias("conta"),
        on=F.col("transacao.conta_id") == F.col("conta.conta_id"),
        how="left"
    )

    .join(
        gold_dim_cartao
        .select(
            "cartao_id",
            "cartao_sk"
        )
        .alias("cartao"),
        on=F.col("transacao.cartao_id") == F.col("cartao.cartao_id"),
        how="left"
    )

    .join(
        gold_dim_estabelecimento
        .select(
            "estabelecimento_id",
            "estabelecimento_sk"
        )
        .alias("estabelecimento"),
        on=(
            F.col("transacao.estabelecimento_id")
            == F.col("estabelecimento.estabelecimento_id")
        ),
        how="left"
    )

    .join(
        risco_por_transacao.alias("risco"),
        on=F.col("transacao.transacao_id") == F.col("risco.transacao_id"),
        how="left"
    )

    .join(
        estorno_por_transacao.alias("estorno"),
        on=F.col("transacao.transacao_id") == F.col("estorno.transacao_id"),
        how="left"
    )

    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("TRANSACAO"),
                F.col("transacao.transacao_id")
            ),
            256
        ).alias("transacao_sk"),

        F.col("transacao.transacao_id"),

        F.col("conta.cliente_sk"),
        F.col("conta.cliente_id"),
        F.col("conta.conta_sk"),
        F.col("transacao.conta_id"),

        F.col("cartao.cartao_sk"),
        F.col("transacao.cartao_id"),

        F.col("estabelecimento.estabelecimento_sk"),
        F.col("transacao.estabelecimento_id"),

        F.col("transacao.data_transacao"),
        F.to_date("transacao.data_transacao").alias("data_transacao_dia"),
        F.year("transacao.data_transacao").alias("ano_transacao"),
        F.month("transacao.data_transacao").alias("mes_transacao"),

        F.col("transacao.valor"),
        F.col("transacao.tipo_transacao"),
        F.col("transacao.status_transacao"),
        F.col("transacao.canal_transacao"),

        F.coalesce(
            F.col("risco.quantidade_eventos_risco"),
            F.lit(0)
        ).alias("quantidade_eventos_risco"),

        F.coalesce(
            F.col("risco.nivel_risco"),
            F.lit("SEM_RISCO")
        ).alias("nivel_risco"),

        F.col("risco.ultima_data_evento_risco"),

        F.coalesce(
            F.col("estorno.quantidade_estornos"),
            F.lit(0)
        ).alias("quantidade_estornos"),

        F.coalesce(
            F.col("estorno.possui_estorno"),
            F.lit(False)
        ).alias("possui_estorno"),

        F.col("estorno.ultima_data_estorno"),
        F.col("estorno.motivo_estorno"),

        F.col("transacao.data_ingestao_origem"),

        F.current_timestamp().alias("gold_processing_timestamp")
    )
    .dropDuplicates(["transacao_id"])
)

display(gold_fato_transacao.limit(10))

# COMMAND ----------

# DBTITLE 1,Construção do Data Product Features de Cliente
total_silver = silver_transacoes.select("transacao_id").distinct().count()
total_gold = gold_fato_transacao.select("transacao_id").distinct().count()

print("=" * 80)
print("VALIDAÇÃO DA FATO")
print("=" * 80)
print(f"Transações distintas na Silver: {total_silver}")
print(f"Transações distintas na Gold:   {total_gold}")
print(f"Diferença:                      {total_silver - total_gold}")

# COMMAND ----------

# DBTITLE 1,Persistência das dimensões Gold
gold_cliente_mes = (
    gold_fato_transacao
    .filter(F.col("cliente_id").isNotNull())
    .groupBy(
        "cliente_sk",
        "cliente_id",
        "ano_transacao",
        "mes_transacao"
    )
    .agg(
        F.countDistinct("transacao_id").alias("quantidade_transacoes"),

        F.sum("valor").alias("valor_total_transacionado"),

        F.avg("valor").alias("ticket_medio"),

        F.max("valor").alias("maior_valor_transacao"),

        F.min("valor").alias("menor_valor_transacao"),

        F.sum(
            F.when(F.col("possui_estorno") == True, 1).otherwise(0)
        ).alias("quantidade_transacoes_estornadas"),

        F.sum(
            F.when(F.col("nivel_risco") == "ALTO", 1).otherwise(0)
        ).alias("quantidade_transacoes_alto_risco"),

        F.sum(
            F.when(F.col("status_transacao") == "APROVADA", 1).otherwise(0)
        ).alias("quantidade_transacoes_aprovadas"),

        F.sum(
            F.when(F.col("status_transacao") != "APROVADA", 1).otherwise(0)
        ).alias("quantidade_transacoes_nao_aprovadas")
    )
    .withColumn(
        "percentual_estorno",
        F.when(
            F.col("quantidade_transacoes") > 0,
            F.round(
                F.col("quantidade_transacoes_estornadas")
                / F.col("quantidade_transacoes") * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "percentual_alto_risco",
        F.when(
            F.col("quantidade_transacoes") > 0,
            F.round(
                F.col("quantidade_transacoes_alto_risco")
                / F.col("quantidade_transacoes") * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "gold_processing_timestamp",
        F.current_timestamp()
    )
)

display(gold_cliente_mes.limit(10))

# COMMAND ----------

# DBTITLE 1,Persistência da Fato e dos Data Products
gold_indicadores_risco = (
    gold_fato_transacao
    .filter(F.col("cliente_id").isNotNull())
    .groupBy(
        "cliente_sk",
        "cliente_id"
    )
    .agg(
        F.countDistinct("transacao_id").alias("quantidade_transacoes"),

        F.sum("quantidade_eventos_risco").alias("quantidade_eventos_risco"),

        F.sum(
            F.when(F.col("nivel_risco") == "ALTO", 1).otherwise(0)
        ).alias("quantidade_alto_risco"),

        F.sum(
            F.when(F.col("nivel_risco") == "MEDIO", 1).otherwise(0)
        ).alias("quantidade_medio_risco"),

        F.sum(
            F.when(F.col("nivel_risco") == "BAIXO", 1).otherwise(0)
        ).alias("quantidade_baixo_risco"),

        F.sum(
            F.when(F.col("possui_estorno") == True, 1).otherwise(0)
        ).alias("quantidade_transacoes_estornadas"),

        F.sum(
            F.when(
                (F.col("nivel_risco") == "ALTO") &
                (F.col("possui_estorno") == True),
                1
            ).otherwise(0)
        ).alias("quantidade_alto_risco_com_estorno"),

        F.max("ultima_data_evento_risco").alias("ultima_data_evento_risco")
    )
    .withColumn(
        "percentual_transacoes_alto_risco",
        F.when(
            F.col("quantidade_transacoes") > 0,
            F.round(
                F.col("quantidade_alto_risco")
                / F.col("quantidade_transacoes") * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "classificacao_risco_cliente",
        F.when(
            (F.col("quantidade_alto_risco") >= 3) |
            (F.col("quantidade_alto_risco_com_estorno") >= 1),
            F.lit("ALTO")
        )
        .when(
            (F.col("quantidade_medio_risco") >= 2) |
            (F.col("quantidade_alto_risco") >= 1),
            F.lit("MEDIO")
        )
        .otherwise(F.lit("BAIXO"))
    )
    .withColumn(
        "gold_processing_timestamp",
        F.current_timestamp()
    )
)

display(gold_indicadores_risco.limit(10))

# COMMAND ----------

# DBTITLE 1,Validação do modelo dimensional
contas_por_cliente = (
    gold_dim_conta
    .groupBy("cliente_id")
    .agg(
        F.countDistinct("conta_id").alias("quantidade_contas"),
        F.sum("saldo").alias("saldo_total"),
        F.sum("limite_credito").alias("limite_credito_total")
    )
)

cartoes_por_cliente = (
    gold_dim_cartao
    .groupBy("cliente_id")
    .agg(
        F.countDistinct("cartao_id").alias("quantidade_cartoes"),
        F.sum("limite_cartao").alias("limite_cartao_total")
    )
)

transacoes_por_cliente = (
    gold_fato_transacao
    .filter(F.col("cliente_id").isNotNull())
    .groupBy("cliente_id")
    .agg(
        F.countDistinct("transacao_id").alias("quantidade_transacoes"),
        F.sum("valor").alias("valor_total_transacionado"),
        F.avg("valor").alias("ticket_medio"),
        F.max("data_transacao").alias("ultima_data_transacao"),

        F.sum(
            F.when(F.col("possui_estorno") == True, 1).otherwise(0)
        ).alias("quantidade_transacoes_estornadas"),

        F.sum(
            F.when(F.col("tipo_transacao") == "PIX", 1).otherwise(0)
        ).alias("quantidade_transacoes_pix"),

        F.sum(
            F.when(F.col("nivel_risco") == "ALTO", 1).otherwise(0)
        ).alias("quantidade_transacoes_alto_risco")
    )
)

gold_features_cliente = (
    gold_dim_cliente.alias("cliente")

    .join(
        contas_por_cliente.alias("conta"),
        on="cliente_id",
        how="left"
    )

    .join(
        cartoes_por_cliente.alias("cartao"),
        on="cliente_id",
        how="left"
    )

    .join(
        transacoes_por_cliente.alias("transacao"),
        on="cliente_id",
        how="left"
    )

    .join(
        gold_indicadores_risco
        .select(
            "cliente_id",
            "classificacao_risco_cliente"
        )
        .alias("risco"),
        on="cliente_id",
        how="left"
    )

    .select(
        F.col("cliente.cliente_sk"),
        F.col("cliente.cliente_id"),
        F.col("cliente.segmento_cliente"),
        F.col("cliente.status_cliente"),
        F.col("cliente.uf"),

        F.coalesce(F.col("conta.quantidade_contas"), F.lit(0))
        .alias("quantidade_contas"),

        F.coalesce(F.col("cartao.quantidade_cartoes"), F.lit(0))
        .alias("quantidade_cartoes"),

        F.coalesce(F.col("conta.saldo_total"), F.lit(0.0))
        .alias("saldo_total"),

        F.coalesce(F.col("conta.limite_credito_total"), F.lit(0.0))
        .alias("limite_credito_total"),

        F.coalesce(F.col("cartao.limite_cartao_total"), F.lit(0.0))
        .alias("limite_cartao_total"),

        F.coalesce(F.col("transacao.quantidade_transacoes"), F.lit(0))
        .alias("quantidade_transacoes"),

        F.coalesce(F.col("transacao.valor_total_transacionado"), F.lit(0.0))
        .alias("valor_total_transacionado"),

        F.coalesce(F.col("transacao.ticket_medio"), F.lit(0.0))
        .alias("ticket_medio"),

        F.col("transacao.ultima_data_transacao"),

        F.coalesce(
            F.col("transacao.quantidade_transacoes_estornadas"),
            F.lit(0)
        ).alias("quantidade_transacoes_estornadas"),

        F.coalesce(
            F.col("transacao.quantidade_transacoes_pix"),
            F.lit(0)
        ).alias("quantidade_transacoes_pix"),

        F.coalesce(
            F.col("transacao.quantidade_transacoes_alto_risco"),
            F.lit(0)
        ).alias("quantidade_transacoes_alto_risco"),

        F.coalesce(
            F.col("risco.classificacao_risco_cliente"),
            F.lit("SEM_MOVIMENTACAO")
        ).alias("classificacao_risco_cliente"),

        F.current_timestamp().alias("gold_processing_timestamp")
    )

    .withColumn(
        "percentual_estorno",
        F.when(
            F.col("quantidade_transacoes") > 0,
            F.round(
                F.col("quantidade_transacoes_estornadas")
                / F.col("quantidade_transacoes") * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )

    .withColumn(
        "percentual_pix",
        F.when(
            F.col("quantidade_transacoes") > 0,
            F.round(
                F.col("quantidade_transacoes_pix")
                / F.col("quantidade_transacoes") * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )
)

display(gold_features_cliente.limit(10))

# COMMAND ----------

# DBTITLE 1,Reconciliação entre Silver e Gold
gold_tables = {
    "gold_dim_cliente": gold_dim_cliente,
    "gold_dim_conta": gold_dim_conta,
    "gold_dim_cartao": gold_dim_cartao,
    "gold_dim_estabelecimento": gold_dim_estabelecimento,
    "gold_fato_transacao": gold_fato_transacao,
    "gold_cliente_mes": gold_cliente_mes,
    "gold_indicadores_risco": gold_indicadores_risco,
    "gold_features_cliente": gold_features_cliente
}

for table_name, dataframe in gold_tables.items():
    full_table_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    print(f"Tabela persistida: {full_table_name}")

# COMMAND ----------

# DBTITLE 1,Validação final das tabelas da camada Gold
print("=" * 100)
print("VALIDAÇÃO FINAL DA CAMADA GOLD")
print("=" * 100)

for table_name in gold_tables.keys():
    full_table_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
    df_gold = spark.table(full_table_name)

    print(
        f"{table_name}: "
        f"{df_gold.count()} registros | "
        f"{len(df_gold.columns)} colunas"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resultado
# MAGIC
# MAGIC A construção da camada Gold foi concluída com sucesso.
# MAGIC
# MAGIC Os dados da camada Silver foram transformados em um modelo dimensional composto por dimensões, tabela fato e Data Products, disponibilizando informações consistentes e preparadas para análises de negócio.
# MAGIC
# MAGIC ### Componentes construídos
# MAGIC
# MAGIC #### Dimensões
# MAGIC
# MAGIC - Clientes
# MAGIC - Contas
# MAGIC - Cartões
# MAGIC - Estabelecimentos
# MAGIC
# MAGIC #### Tabela Fato
# MAGIC
# MAGIC - Transações
# MAGIC
# MAGIC #### Data Products
# MAGIC
# MAGIC - Cliente Mês
# MAGIC - Indicadores de Risco
# MAGIC - Features de Cliente
# MAGIC
# MAGIC ### Recursos implementados
# MAGIC
# MAGIC - Modelo Dimensional (Star Schema)
# MAGIC - Surrogate Keys determinísticas
# MAGIC - Integridade Referencial
# MAGIC - Data Products Analíticos
# MAGIC - Features para Machine Learning
# MAGIC - Persistência em Delta Lake
# MAGIC - Reconciliação entre Silver e Gold
# MAGIC
# MAGIC Todas as validações de consistência foram concluídas com sucesso, garantindo que a camada Gold esteja preparada para consumo analítico.
# MAGIC
# MAGIC ### Próximo Notebook
# MAGIC
# MAGIC **07_quality_checks**
# MAGIC
# MAGIC Execução das validações de qualidade, integridade e reconciliação das tabelas produzidas na camada Gold.