# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 05 — Processamento da Camada Silver
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Aplicar regras de negócio, processamento de CDC, deduplicação, validações referenciais e reconstrução histórica utilizando SCD Type 2.
# MAGIC
# MAGIC **Tecnologias:** Databricks • PySpark • Delta Lake • Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Transformar os dados da camada Bronze em tabelas Silver consistentes, tipadas, historizadas e preparadas para modelagem analítica.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Deduplicar eventos de CDC.
# MAGIC - Ordenar eventos por chave de negócio e data de alteração.
# MAGIC - Reconstruir o histórico das entidades dimensionais.
# MAGIC - Implementar Slowly Changing Dimension Type 2.
# MAGIC - Identificar registros atuais e excluídos.
# MAGIC - Controlar a versão dos registros.
# MAGIC - Tratar eventos recebidos fora de ordem.
# MAGIC - Validar relacionamentos entre entidades.
# MAGIC - Processar entidades transacionais.
# MAGIC - Persistir as tabelas Silver incrementalmente com Delta MERGE.
# MAGIC
# MAGIC ## Recursos implementados
# MAGIC
# MAGIC - Change Data Capture
# MAGIC - Deduplicação
# MAGIC - SCD Type 2
# MAGIC - Late Arrival
# MAGIC - Validação referencial
# MAGIC - Histórico completo
# MAGIC - Soft Delete
# MAGIC - Versionamento dos registros
# MAGIC
# MAGIC ## Entradas
# MAGIC
# MAGIC - `workspace.case_bronze.bronze_clientes`
# MAGIC - `workspace.case_bronze.bronze_contas`
# MAGIC - `workspace.case_bronze.bronze_cartoes`
# MAGIC - `workspace.case_bronze.bronze_transacoes`
# MAGIC - `workspace.case_bronze.bronze_eventos_risco`
# MAGIC - `workspace.case_bronze.bronze_estornos`
# MAGIC
# MAGIC ## Saídas
# MAGIC
# MAGIC - `workspace.case_silver.silver_clientes`
# MAGIC - `workspace.case_silver.silver_contas`
# MAGIC - `workspace.case_silver.silver_cartoes`
# MAGIC - `workspace.case_silver.silver_transacoes`
# MAGIC - `workspace.case_silver.silver_eventos_risco`
# MAGIC - `workspace.case_silver.silver_estornos`
# MAGIC
# MAGIC ## Próximo Notebook
# MAGIC
# MAGIC **06_gold**
# MAGIC
# MAGIC Construção do modelo dimensional, da tabela fato e dos produtos analíticos da camada Gold.

# COMMAND ----------

# DBTITLE 1,Configuração do ambiente
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = spark.catalog.currentCatalog()

BRONZE_SCHEMA = "case_bronze"
SILVER_SCHEMA = "case_silver"

CDC_CONFIG = {
    "clientes_cdc": {
        "primary_key": "cliente_id",
        "sequence_column": "data_atualizacao",
        "tracked_columns": [
            "cpf",
            "nome",
            "data_nascimento",
            "cidade",
            "uf",
            "status_cliente",
            "segmento_cliente"
        ],
        "target_table": "silver_clientes"
    },

    "contas_cdc": {
        "primary_key": "conta_id",
        "sequence_column": "data_atualizacao",
        "tracked_columns": [
            "cliente_id",
            "tipo_conta",
            "agencia",
            "saldo",
            "limite_credito",
            "status_conta",
            "data_abertura",
            "modalidade_atendimento"
        ],
        "target_table": "silver_contas"
    },

    "cartoes_cdc": {
        "primary_key": "cartao_id",
        "sequence_column": "data_atualizacao",
        "tracked_columns": [
            "conta_id",
            "tipo_cartao",
            "bandeira",
            "limite_cartao",
            "status_cartao",
            "data_emissao",
            "cartao_virtual"
        ],
        "target_table": "silver_cartoes"
    }
}

SILVER_MERGE_CONFIG = {
    "silver_clientes": ["cliente_id", "vigencia_inicio"],
    "silver_contas": ["conta_id", "vigencia_inicio"],
    "silver_cartoes": ["cartao_id", "vigencia_inicio"],
    "silver_transacoes": ["transacao_id"],
    "silver_eventos_risco": ["evento_risco_id"],
    "silver_estornos": ["estorno_id"]
}

# Compatibilidade com Databricks Free Edition / Serverless:
# a configuração global spark.databricks.delta.schema.autoMerge.enabled
# não é disponibilizada nesse ambiente. Quando uma tabela for criada,
# a evolução de schema é solicitada diretamente na operação de escrita
# por meio de .option("mergeSchema", "true").

print(f"Catálogo: {CATALOG}")
print(f"Schema Silver: {SILVER_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Definição das tabelas e entidades
def deduplicate_cdc_events(
    dataframe: DataFrame,
    primary_key: str,
    sequence_column: str
) -> DataFrame:
    """
    Mantém apenas um evento para cada combinação de:
    chave, data de alteração, operação CDC e conteúdo do registro.
    """

    deduplication_window = (
        Window
        .partitionBy(
            primary_key,
            sequence_column,
            "operacao_cdc",
            "_record_hash"
        )
        .orderBy(
            F.col("_processing_date").desc(),
            F.col("_batch_id").desc(),
            F.col("_bronze_ingestion_timestamp").desc()
        )
    )

    return (
        dataframe
        .withColumn(
            "_cdc_duplicate_rank",
            F.row_number().over(deduplication_window)
        )
        .filter(F.col("_cdc_duplicate_rank") == 1)
        .drop("_cdc_duplicate_rank")
    )


def build_merge_condition(
    merge_keys: list[str],
    target_alias: str = "target",
    source_alias: str = "source"
) -> str:
    """Monta uma condição null-safe para Delta MERGE."""

    if not merge_keys:
        raise ValueError(
            "A configuração do MERGE deve possuir ao menos uma chave."
        )

    return " AND ".join(
        f"{target_alias}.`{column}` <=> "
        f"{source_alias}.`{column}`"
        for column in merge_keys
    )


def build_change_condition(
    columns: list[str],
    ignored_columns: list[str] | None = None,
    target_alias: str = "target",
    source_alias: str = "source"
) -> str:
    """
    Compara o conteúdo de origem e destino ignorando colunas voláteis.

    Dessa forma, o mesmo lote reprocessado não gera update apenas porque
    o timestamp técnico de processamento foi recalculado.
    """

    ignored = set(ignored_columns or [])
    comparable_columns = [
        column for column in columns
        if column not in ignored
    ]

    if not comparable_columns:
        return "false"

    return " OR ".join(
        f"NOT ({target_alias}.`{column}` <=> "
        f"{source_alias}.`{column}`)"
        for column in comparable_columns
    )


def validate_unique_merge_keys(
    dataframe: DataFrame,
    merge_keys: list[str],
    table_name: str
) -> None:
    """Evita MERGE ambíguo quando a origem contém chaves duplicadas."""

    duplicated_keys_df = (
        dataframe
        .groupBy(*merge_keys)
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicated_keys_df.limit(1).count() > 0:
        display(duplicated_keys_df.orderBy(F.col("count").desc()))
        raise ValueError(
            f"Origem com chaves de MERGE duplicadas em {table_name}: "
            f"{merge_keys}"
        )


def persist_with_delta_merge(
    dataframe: DataFrame,
    table_name: str,
    merge_keys: list[str]
) -> dict:
    """
    Cria a tabela Delta na carga inicial e aplica MERGE nas demais.

    Compatível com Databricks Free Edition / Serverless: não depende da
    configuração global de autoMerge, que é bloqueada nesse ambiente.

    A implementação evita DataFrame.persist()/cache, pois o compute
    Serverless da Free Edition não oferece suporte ao comando PERSIST TABLE.
    """

    source_df = dataframe

    validate_unique_merge_keys(
        dataframe=source_df,
        merge_keys=merge_keys,
        table_name=table_name
    )

    source_count = source_df.count()
    target_existed = spark.catalog.tableExists(table_name)

    if not target_existed:
        (
            source_df.write
            .format("delta")
            .mode("errorifexists")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
        operation = "INITIAL_LOAD"
    else:
        target_delta = DeltaTable.forName(spark, table_name)
        merge_condition = build_merge_condition(merge_keys)
        change_condition = build_change_condition(
            columns=source_df.columns,
            ignored_columns=["_silver_processing_timestamp"]
        )

        (
            target_delta.alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll(
                condition=change_condition
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        operation = "MERGE"

    target_df = spark.table(table_name)
    target_count = target_df.count()

    duplicated_target_keys = (
        target_df
        .groupBy(*merge_keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicated_target_keys > 0:
        raise ValueError(
            f"Foram encontradas chaves duplicadas no destino "
            f"{table_name}: {merge_keys}"
        )

    return {
        "table_name": table_name,
        "operation": operation,
        "merge_keys": ", ".join(merge_keys),
        "source_count": source_count,
        "target_count": target_count,
        "duplicated_target_keys": duplicated_target_keys,
        "status": "PASS"
    }


# COMMAND ----------

# DBTITLE 1,Leitura das tabelas Bronze
def build_scd2_history(
    entity_name: str
) -> DataFrame:
    """
    Reconstrói o histórico completo SCD Tipo 2 de uma entidade CDC.

    A ordenação usa a data real da alteração, permitindo posicionar
    corretamente eventos atrasados.
    """

    config = CDC_CONFIG[entity_name]

    primary_key = config["primary_key"]
    sequence_column = config["sequence_column"]

    source_table = (
        f"{CATALOG}.{BRONZE_SCHEMA}."
        f"bronze_{entity_name}"
    )

    source_df = spark.table(source_table)

    source_df = deduplicate_cdc_events(
        dataframe=source_df,
        primary_key=primary_key,
        sequence_column=sequence_column
    )

    history_window = (
        Window
        .partitionBy(primary_key)
        .orderBy(
            F.col(sequence_column).asc(),
            F.col("_processing_date").asc(),
            F.col("_batch_id").asc(),
            F.col("_bronze_ingestion_timestamp").asc()
        )
    )

    history_df = (
        source_df
        .withColumn(
            "_next_change_timestamp",
            F.lead(F.col(sequence_column)).over(history_window)
        )
        .withColumn(
            "vigencia_inicio",
            F.col(sequence_column)
        )
        .withColumn(
            "vigencia_fim",
            F.when(
                F.col("_next_change_timestamp").isNotNull(),
                F.expr(
                    "_next_change_timestamp - INTERVAL 1 MICROSECOND"
                )
            ).otherwise(
                F.lit(None).cast("timestamp")
            )
        )
        .withColumn(
            "registro_atual",
            F.col("_next_change_timestamp").isNull()
        )
        .withColumn(
            "registro_excluido",
            F.col("operacao_cdc") == F.lit("D")
        )
        .withColumn(
            "versao_registro",
            F.row_number().over(history_window)
        )
        .withColumn(
            "_silver_processing_timestamp",
            F.current_timestamp()
        )
        .drop("_next_change_timestamp")
    )

    return history_df

# COMMAND ----------

# DBTITLE 1,Definição das funções auxiliares
silver_clientes_df = build_scd2_history(
    entity_name="clientes_cdc"
)

print(
    f"Registros históricos de clientes: "
    f"{silver_clientes_df.count()}"
)

display(
    silver_clientes_df
    .select(
        "cliente_id",
        "nome",
        "cidade",
        "status_cliente",
        "segmento_cliente",
        "operacao_cdc",
        "data_atualizacao",
        "vigencia_inicio",
        "vigencia_fim",
        "versao_registro",
        "registro_atual",
        "registro_excluido",
        "_batch_id"
    )
    .filter(
        F.col("cliente_id").isin(
            "CLI_000001",
            "CLI_000002",
            "CLI_000061"
        )
    )
    .orderBy(
        "cliente_id",
        "versao_registro"
    )
)

# COMMAND ----------

# DBTITLE 1,Deduplicação dos eventos CDC
clientes_current_validation_df = (
    silver_clientes_df
    .filter(F.col("registro_atual"))
    .groupBy("cliente_id")
    .agg(
        F.count("*").alias("quantidade_registros_atuais")
    )
    .filter(
        F.col("quantidade_registros_atuais") != 1
    )
)

invalid_current_customers = clientes_current_validation_df.count()

print(
    "Clientes com quantidade inválida de registros atuais: "
    f"{invalid_current_customers}"
)

display(clientes_current_validation_df)

# COMMAND ----------

# DBTITLE 1,Preparação dos eventos para SCD Type 2
display(
    silver_clientes_df
    .filter(F.col("registro_excluido"))
    .select(
        "cliente_id",
        "nome",
        "operacao_cdc",
        "vigencia_inicio",
        "vigencia_fim",
        "registro_atual",
        "registro_excluido",
        "_batch_id"
    )
    .orderBy(
        "cliente_id",
        "vigencia_inicio"
    )
)

# COMMAND ----------

# DBTITLE 1,Reconstrução do histórico SCD Type 2
silver_contas_df = build_scd2_history(
    entity_name="contas_cdc"
)

silver_cartoes_df = build_scd2_history(
    entity_name="cartoes_cdc"
)

print(
    f"Histórico de clientes: {silver_clientes_df.count()}"
)
print(
    f"Histórico de contas: {silver_contas_df.count()}"
)
print(
    f"Histórico de cartões: {silver_cartoes_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,Processamento da entidade Clientes
scd2_dataframes = {
    "clientes": (
        silver_clientes_df,
        "cliente_id"
    ),
    "contas": (
        silver_contas_df,
        "conta_id"
    ),
    "cartoes": (
        silver_cartoes_df,
        "cartao_id"
    )
}

scd2_validation_rows = []

for entity_name, (
    dataframe,
    primary_key
) in scd2_dataframes.items():

    total_records = dataframe.count()

    distinct_keys = (
        dataframe
        .select(primary_key)
        .distinct()
        .count()
    )

    current_records = (
        dataframe
        .filter(F.col("registro_atual"))
        .count()
    )

    deleted_current_records = (
        dataframe
        .filter(
            F.col("registro_atual")
            & F.col("registro_excluido")
        )
        .count()
    )

    invalid_current_keys = (
        dataframe
        .filter(F.col("registro_atual"))
        .groupBy(primary_key)
        .count()
        .filter(F.col("count") != 1)
        .count()
    )

    scd2_validation_rows.append(
        (
            entity_name,
            total_records,
            distinct_keys,
            current_records,
            deleted_current_records,
            invalid_current_keys
        )
    )

scd2_validation_df = spark.createDataFrame(
    scd2_validation_rows,
    [
        "entidade",
        "registros_historicos",
        "chaves_distintas",
        "registros_atuais",
        "registros_atuais_excluidos",
        "chaves_com_registro_atual_invalido"
    ]
)

display(
    scd2_validation_df.orderBy("entidade")
)

# COMMAND ----------

# DBTITLE 1,Processamento da entidade Contas
scd2_dataframes = {
    "clientes": (
        silver_clientes_df,
        "cliente_id"
    ),
    "contas": (
        silver_contas_df,
        "conta_id"
    ),
    "cartoes": (
        silver_cartoes_df,
        "cartao_id"
    )
}

scd2_validation_rows = []

for entity_name, (
    dataframe,
    primary_key
) in scd2_dataframes.items():

    total_records = dataframe.count()

    distinct_keys = (
        dataframe
        .select(primary_key)
        .distinct()
        .count()
    )

    current_records = (
        dataframe
        .filter(F.col("registro_atual"))
        .count()
    )

    deleted_current_records = (
        dataframe
        .filter(
            F.col("registro_atual")
            & F.col("registro_excluido")
        )
        .count()
    )

    invalid_current_keys = (
        dataframe
        .filter(F.col("registro_atual"))
        .groupBy(primary_key)
        .count()
        .filter(F.col("count") != 1)
        .count()
    )

    scd2_validation_rows.append(
        (
            entity_name,
            total_records,
            distinct_keys,
            current_records,
            deleted_current_records,
            invalid_current_keys
        )
    )

scd2_validation_df = spark.createDataFrame(
    scd2_validation_rows,
    [
        "entidade",
        "registros_historicos",
        "chaves_distintas",
        "registros_atuais",
        "registros_atuais_excluidos",
        "chaves_com_registro_atual_invalido"
    ]
)

display(
    scd2_validation_df.orderBy("entidade")
)

# COMMAND ----------

# DBTITLE 1,Processamento da entidade Cartões
silver_clientes_atual_df = (
    silver_clientes_df
    .filter(
        F.col("registro_atual")
        & ~F.col("registro_excluido")
    )
)

silver_contas_atual_df = (
    silver_contas_df
    .filter(
        F.col("registro_atual")
        & ~F.col("registro_excluido")
    )
)

silver_cartoes_atual_df = (
    silver_cartoes_df
    .filter(
        F.col("registro_atual")
        & ~F.col("registro_excluido")
    )
)

print(
    f"Clientes atuais ativos: "
    f"{silver_clientes_atual_df.count()}"
)

print(
    f"Contas atuais não excluídas: "
    f"{silver_contas_atual_df.count()}"
)

print(
    f"Cartões atuais não excluídos: "
    f"{silver_cartoes_atual_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,Validação referencial de Contas e Clientes
def deduplicate_transactional_entity(
    dataframe: DataFrame,
    primary_key: str,
    event_timestamp_column: str
) -> DataFrame:
    """
    Mantém um registro por chave técnica.

    Em caso de repetição, prioriza:
    1. maior timestamp do evento;
    2. lote mais recente;
    3. maior timestamp de ingestão na Bronze.
    """

    deduplication_window = (
        Window
        .partitionBy(primary_key)
        .orderBy(
            F.col(event_timestamp_column).desc_nulls_last(),
            F.col("_processing_date").desc(),
            F.col("_batch_id").desc(),
            F.col("_bronze_ingestion_timestamp").desc()
        )
    )

    return (
        dataframe
        .withColumn(
            "_silver_deduplication_rank",
            F.row_number().over(deduplication_window)
        )
        .filter(
            F.col("_silver_deduplication_rank") == 1
        )
        .drop("_silver_deduplication_rank")
    )

# COMMAND ----------

# DBTITLE 1,Validação referencial de Cartões e Contas
bronze_transacoes_df = spark.table(
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_transacoes"
)

silver_transacoes_df = deduplicate_transactional_entity(
    dataframe=bronze_transacoes_df,
    primary_key="transacao_id",
    event_timestamp_column="data_transacao"
)

silver_transacoes_df = (
    silver_transacoes_df
    .withColumn(
        "atraso_ingestao_horas",
        F.round(
            (
                F.unix_timestamp("data_ingestao_origem")
                - F.unix_timestamp("data_transacao")
            ) / F.lit(3600),
            2
        )
    )
    .withColumn(
        "late_arrival",
        F.col("atraso_ingestao_horas") > F.lit(24)
    )
    .withColumn(
        "_silver_processing_timestamp",
        F.current_timestamp()
    )
)

print(
    f"Transações Silver: "
    f"{silver_transacoes_df.count()}"
)

print(
    f"Transações late arrival: "
    f"{silver_transacoes_df.filter(F.col('late_arrival')).count()}"
)

# COMMAND ----------

# DBTITLE 1,Tratamento de Late Arrival
contas_referencia_df = (
    silver_contas_df
    .select("conta_id")
    .distinct()
    .withColumn(
        "_conta_encontrada",
        F.lit(True)
    )
)

cartoes_referencia_df = (
    silver_cartoes_df
    .select("cartao_id")
    .distinct()
    .withColumn(
        "_cartao_encontrado",
        F.lit(True)
    )
)

silver_transacoes_df = (
    silver_transacoes_df.alias("trx")
    .join(
        contas_referencia_df.alias("cta"),
        on="conta_id",
        how="left"
    )
    .join(
        cartoes_referencia_df.alias("car"),
        on="cartao_id",
        how="left"
    )
    .withColumn(
        "conta_valida",
        F.coalesce(
            F.col("_conta_encontrada"),
            F.lit(False)
        )
    )
    .withColumn(
        "cartao_valido",
        F.when(
            F.col("cartao_id").isNull(),
            F.lit(True)
        ).otherwise(
            F.coalesce(
                F.col("_cartao_encontrado"),
                F.lit(False)
            )
        )
    )
    .withColumn(
        "referencias_validas",
        F.col("conta_valida")
        & F.col("cartao_valido")
    )
    .drop(
        "_conta_encontrada",
        "_cartao_encontrado"
    )
)

# COMMAND ----------

# DBTITLE 1,Processamento da entidade Transações
silver_transacoes_validas_df = (
    silver_transacoes_df
    .filter(F.col("referencias_validas"))
)

silver_transacoes_orfas_df = (
    silver_transacoes_df
    .filter(~F.col("referencias_validas"))
    .withColumn(
        "_quarantine_reason",
        F.concat_ws(
            " | ",
            F.when(
                ~F.col("conta_valida"),
                F.lit("REFERENCIA_INVALIDA:conta_id")
            ),
            F.when(
                ~F.col("cartao_valido"),
                F.lit("REFERENCIA_INVALIDA:cartao_id")
            )
        )
    )
)

print(
    f"Transações válidas: "
    f"{silver_transacoes_validas_df.count()}"
)

print(
    f"Transações com referência inválida: "
    f"{silver_transacoes_orfas_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,Processamento da entidade Eventos de Risco
bronze_eventos_risco_df = spark.table(
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_eventos_risco"
)

silver_eventos_risco_df = deduplicate_transactional_entity(
    dataframe=bronze_eventos_risco_df,
    primary_key="evento_risco_id",
    event_timestamp_column="data_evento"
)

transacoes_referencia_df = (
    silver_transacoes_validas_df
    .select("transacao_id")
    .distinct()
    .withColumn(
        "_transacao_encontrada",
        F.lit(True)
    )
)

silver_eventos_risco_df = (
    silver_eventos_risco_df
    .join(
        transacoes_referencia_df,
        on="transacao_id",
        how="left"
    )
    .withColumn(
        "transacao_valida",
        F.coalesce(
            F.col("_transacao_encontrada"),
            F.lit(False)
        )
    )
    .withColumn(
        "_silver_processing_timestamp",
        F.current_timestamp()
    )
    .drop("_transacao_encontrada")
)

silver_eventos_risco_validos_df = (
    silver_eventos_risco_df
    .filter(F.col("transacao_valida"))
)

silver_eventos_risco_orfaos_df = (
    silver_eventos_risco_df
    .filter(~F.col("transacao_valida"))
    .withColumn(
        "_quarantine_reason",
        F.lit("REFERENCIA_INVALIDA:transacao_id")
    )
)

print(
    f"Eventos de risco válidos: "
    f"{silver_eventos_risco_validos_df.count()}"
)

print(
    f"Eventos de risco órfãos: "
    f"{silver_eventos_risco_orfaos_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,Processamento da entidade Estornos
bronze_estornos_df = spark.table(
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_estornos"
)

silver_estornos_df = deduplicate_transactional_entity(
    dataframe=bronze_estornos_df,
    primary_key="estorno_id",
    event_timestamp_column="data_estorno"
)

silver_estornos_df = (
    silver_estornos_df
    .join(
        transacoes_referencia_df,
        on="transacao_id",
        how="left"
    )
    .withColumn(
        "transacao_valida",
        F.coalesce(
            F.col("_transacao_encontrada"),
            F.lit(False)
        )
    )
    .withColumn(
        "_silver_processing_timestamp",
        F.current_timestamp()
    )
    .drop("_transacao_encontrada")
)

silver_estornos_validos_df = (
    silver_estornos_df
    .filter(F.col("transacao_valida"))
)

silver_estornos_orfaos_df = (
    silver_estornos_df
    .filter(~F.col("transacao_valida"))
    .withColumn(
        "_quarantine_reason",
        F.lit("REFERENCIA_INVALIDA:transacao_id")
    )
)

print(
    f"Estornos válidos: "
    f"{silver_estornos_validos_df.count()}"
)

print(
    f"Estornos órfãos: "
    f"{silver_estornos_orfaos_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,Persistência incremental das tabelas Silver com Delta MERGE
silver_tables = {
    "silver_clientes": silver_clientes_df,
    "silver_contas": silver_contas_df,
    "silver_cartoes": silver_cartoes_df,
    "silver_transacoes": silver_transacoes_validas_df,
    "silver_eventos_risco": silver_eventos_risco_validos_df,
    "silver_estornos": silver_estornos_validos_df
}

silver_merge_results = []

for table_short_name, dataframe in silver_tables.items():
    table_name = (
        f"{CATALOG}.{SILVER_SCHEMA}."
        f"{table_short_name}"
    )

    merge_keys = SILVER_MERGE_CONFIG[table_short_name]

    merge_result = persist_with_delta_merge(
        dataframe=dataframe,
        table_name=table_name,
        merge_keys=merge_keys
    )

    silver_merge_results.append(merge_result)

    print(
        f"{merge_result['operation']}: {table_name} | "
        f"Origem: {merge_result['source_count']} | "
        f"Destino: {merge_result['target_count']} | "
        f"Status: {merge_result['status']}"
    )

silver_merge_results_df = spark.createDataFrame(
    silver_merge_results
)

display(
    silver_merge_results_df.orderBy("table_name")
)

# COMMAND ----------

# DBTITLE 1,Persistência das tabelas transacionais Silver
QUARANTINE_SCHEMA = "case_quarantine"

silver_quarantine_tables = {
    "quarantine_transacoes_referencia": (
        silver_transacoes_orfas_df
    ),
    "quarantine_eventos_risco_referencia": (
        silver_eventos_risco_orfaos_df
    ),
    "quarantine_estornos_referencia": (
        silver_estornos_orfaos_df
    )
}

for table_short_name, dataframe in silver_quarantine_tables.items():
    table_name = (
        f"{CATALOG}.{QUARANTINE_SCHEMA}."
        f"{table_short_name}"
    )

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table_name)
    )

    print(
        f"Quarentena criada: {table_name} | "
        f"Registros: {dataframe.count()}"
    )

# COMMAND ----------

# DBTITLE 1,Validação das tabelas Silver
silver_summary = []

for table_short_name, dataframe in silver_tables.items():
    silver_summary.append(
        (
            table_short_name,
            dataframe.count(),
            len(dataframe.columns)
        )
    )

silver_summary_df = spark.createDataFrame(
    silver_summary,
    [
        "tabela",
        "quantidade_registros",
        "quantidade_colunas"
    ]
)

display(
    silver_summary_df.orderBy("tabela")
)

# COMMAND ----------

# DBTITLE 1,Validação do histórico SCD Type 2
display(
    silver_transacoes_validas_df
    .filter(F.col("late_arrival"))
    .select(
        "transacao_id",
        "data_transacao",
        "data_ingestao_origem",
        "atraso_ingestao_horas",
        "late_arrival",
        "_batch_id"
    )
    .orderBy(
        F.col("atraso_ingestao_horas").desc()
    )
)

# COMMAND ----------

# DBTITLE 1,Resumo do processamento da camada Silver
display(
    spark.sql(
        f"""
        SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}
        """
    )
    .filter(
        F.col("tableName").startswith("silver_")
    )
    .orderBy("tableName")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resultado
# MAGIC
# MAGIC O processamento da camada Silver foi concluído com sucesso.
# MAGIC
# MAGIC As entidades dimensionais foram historizadas utilizando SCD Type 2, preservando as alterações recebidas por CDC e tratando corretamente eventos recebidos fora de ordem.
# MAGIC
# MAGIC ### Recursos implementados
# MAGIC
# MAGIC - Deduplicação dos eventos CDC
# MAGIC - Reconstrução completa do histórico
# MAGIC - SCD Type 2
# MAGIC - Controle de vigência
# MAGIC - Identificação do registro atual
# MAGIC - Identificação de exclusões lógicas
# MAGIC - Versionamento dos registros
# MAGIC - Tratamento de Late Arrival
# MAGIC - Validações referenciais
# MAGIC - Processamento das entidades transacionais
# MAGIC - Persistência incremental e idempotente com Delta MERGE
# MAGIC
# MAGIC As tabelas Silver estão preparadas para a construção do modelo dimensional e dos produtos analíticos da camada Gold.
# MAGIC
# MAGIC ### Próximo Notebook
# MAGIC
# MAGIC **06_gold**