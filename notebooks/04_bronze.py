# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 04 — Processamento da Camada Bronze
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Transformar os registros validados da camada Landing em tabelas Bronze padronizadas, rastreáveis e persistidas em Delta Lake.
# MAGIC
# MAGIC **Tecnologias:** Databricks • PySpark • Delta Lake • Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Aplicar padronizações técnicas aos dados validados, preservando sua granularidade e histórico de origem para disponibilizá-los à camada Silver.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Ler os registros validados da camada Landing.
# MAGIC - Padronizar nomes e tipos de dados.
# MAGIC - Preservar os campos de CDC.
# MAGIC - Adicionar metadados técnicos de processamento.
# MAGIC - Gerar hashes determinísticos para rastreabilidade.
# MAGIC - Persistir as tabelas Bronze em Delta Lake.
# MAGIC - Validar os registros carregados por entidade.
# MAGIC
# MAGIC ## Entradas
# MAGIC
# MAGIC - Tabelas validadas produzidas pelo notebook `03_validate_landing`.
# MAGIC
# MAGIC ## Saídas
# MAGIC
# MAGIC - `workspace.case_bronze.bronze_clientes`
# MAGIC - `workspace.case_bronze.bronze_contas`
# MAGIC - `workspace.case_bronze.bronze_cartoes`
# MAGIC - `workspace.case_bronze.bronze_transacoes`
# MAGIC - `workspace.case_bronze.bronze_eventos_risco`
# MAGIC - `workspace.case_bronze.bronze_estornos`
# MAGIC
# MAGIC ## Próximo Notebook
# MAGIC
# MAGIC **05_silver**
# MAGIC
# MAGIC Aplicação de regras de negócio, CDC, deduplicação, Late Arrival e reconstrução histórica com SCD Type 2.

# COMMAND ----------

# DBTITLE 1,Configuração do ambiente
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = spark.catalog.currentCatalog()

LANDING_SCHEMA = "case_landing"
BRONZE_SCHEMA = "case_bronze"

ENTITIES = [
    "clientes_cdc",
    "contas_cdc",
    "cartoes_cdc",
    "transacoes",
    "eventos_risco",
    "estornos"
]

print(f"Catálogo: {CATALOG}")
print(f"Schema Bronze: {BRONZE_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Definição das entidades e tabelas Bronze
BRONZE_CONFIG = {
    "clientes_cdc": {
        "date_columns": [
            "data_nascimento"
        ],
        "timestamp_columns": [
            "data_atualizacao"
        ],
        "decimal_columns": {}
    },

    "contas_cdc": {
        "date_columns": [
            "data_abertura"
        ],
        "timestamp_columns": [
            "data_atualizacao"
        ],
        "decimal_columns": {
            "saldo": "decimal(18,2)",
            "limite_credito": "decimal(18,2)"
        }
    },

    "cartoes_cdc": {
        "date_columns": [
            "data_emissao"
        ],
        "timestamp_columns": [
            "data_atualizacao"
        ],
        "decimal_columns": {
            "limite_cartao": "decimal(18,2)"
        }
    },

    "transacoes": {
        "date_columns": [],
        "timestamp_columns": [
            "data_transacao",
            "data_ingestao_origem"
        ],
        "decimal_columns": {
            "valor": "decimal(18,2)"
        }
    },

    "eventos_risco": {
        "date_columns": [],
        "timestamp_columns": [
            "data_evento"
        ],
        "decimal_columns": {
            "score_risco": "decimal(10,4)"
        }
    },

    "estornos": {
        "date_columns": [],
        "timestamp_columns": [
            "data_estorno"
        ],
        "decimal_columns": {
            "valor_estorno": "decimal(18,2)"
        }
    }
}

# COMMAND ----------

# DBTITLE 1,Leitura dos registros validados
def cast_bronze_types(
    dataframe: DataFrame,
    entity_name: str
) -> DataFrame:
    """
    Converte os campos validados para os tipos físicos
    utilizados na camada Bronze.
    """

    config = BRONZE_CONFIG[entity_name]

    result_df = dataframe

    for column_name in config["date_columns"]:
        result_df = result_df.withColumn(
            column_name,
            F.expr(
                f"try_cast(`{column_name}` AS DATE)"
            )
        )

    for column_name in config["timestamp_columns"]:
        result_df = result_df.withColumn(
            column_name,
            F.expr(
                f"try_cast(`{column_name}` AS TIMESTAMP)"
            )
        )

    for column_name, data_type in config["decimal_columns"].items():
        result_df = result_df.withColumn(
            column_name,
            F.expr(
                f"try_cast(`{column_name}` AS {data_type})"
            )
        )

    return result_df

# COMMAND ----------

# DBTITLE 1,Definição das funções de padronização
def add_record_hash(
    dataframe: DataFrame
) -> DataFrame:
    """
    Cria um hash SHA-256 com base apenas nas colunas de negócio.
    """

    business_columns = [
        column_name
        for column_name in dataframe.columns
        if not column_name.startswith("_")
    ]

    hash_payload = F.concat_ws(
        "||",
        *[
            F.coalesce(
                F.col(column_name).cast("string"),
                F.lit("<NULL>")
            )
            for column_name in sorted(business_columns)
        ]
    )

    return dataframe.withColumn(
        "_record_hash",
        F.sha2(hash_payload, 256)
    )

# COMMAND ----------

# DBTITLE 1,Padronização da entidade Clientes
def build_bronze_dataframe(
    entity_name: str
) -> DataFrame:
    """
    Lê a tabela validada da Landing, converte os tipos
    e adiciona metadados técnicos da Bronze.
    """

    source_table = (
        f"{CATALOG}.{LANDING_SCHEMA}."
        f"validated_{entity_name}"
    )

    source_df = spark.table(source_table)

    bronze_df = cast_bronze_types(
        dataframe=source_df,
        entity_name=entity_name
    )

    bronze_df = add_record_hash(bronze_df)

    bronze_df = (
        bronze_df
        .withColumn(
            "_bronze_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_bronze_table_name",
            F.lit(f"bronze_{entity_name}")
        )
        .withColumn(
            "_bronze_schema_version",
            F.coalesce(
                F.col("_schema_version"),
                F.lit("v1")
            )
        )
    )

    return bronze_df

# COMMAND ----------

# DBTITLE 1,Padronização da entidade Contas
bronze_dataframes = {}

for entity_name in ENTITIES:
    print(f"\nPreparando Bronze: {entity_name}")

    bronze_df = build_bronze_dataframe(
        entity_name=entity_name
    )

    bronze_dataframes[entity_name] = bronze_df

    print(
        f"Registros preparados: {bronze_df.count()}"
    )

# COMMAND ----------

# DBTITLE 1,Padronização da entidade Cartões
IMPORTANT_TYPES = {
    "clientes_cdc": {
        "data_nascimento": "date",
        "data_atualizacao": "timestamp"
    },
    "contas_cdc": {
        "data_abertura": "date",
        "data_atualizacao": "timestamp",
        "saldo": "decimal(18,2)",
        "limite_credito": "decimal(18,2)"
    },
    "cartoes_cdc": {
        "data_emissao": "date",
        "data_atualizacao": "timestamp",
        "limite_cartao": "decimal(18,2)"
    },
    "transacoes": {
        "data_transacao": "timestamp",
        "data_ingestao_origem": "timestamp",
        "valor": "decimal(18,2)"
    },
    "eventos_risco": {
        "data_evento": "timestamp",
        "score_risco": "decimal(10,4)"
    },
    "estornos": {
        "data_estorno": "timestamp",
        "valor_estorno": "decimal(18,2)"
    }
}

schema_validation_rows = []

for entity_name, dataframe in bronze_dataframes.items():
    actual_types = dict(dataframe.dtypes)

    for column_name, expected_type in IMPORTANT_TYPES[entity_name].items():
        actual_type = actual_types.get(column_name)

        schema_validation_rows.append(
            (
                entity_name,
                column_name,
                expected_type,
                actual_type,
                actual_type == expected_type
            )
        )

schema_validation_df = spark.createDataFrame(
    schema_validation_rows,
    [
        "entidade",
        "coluna",
        "tipo_esperado",
        "tipo_encontrado",
        "tipo_correto"
    ]
)

display(
    schema_validation_df.orderBy(
        "entidade",
        "coluna"
    )
)

# COMMAND ----------

# DBTITLE 1,Padronização da entidade Transações
TEMPORARY_COLUMNS = [
    "_record_payload_hash",
    "_validation_errors"
]

for entity_name, dataframe in bronze_dataframes.items():
    columns_to_remove = [
        column_name
        for column_name in TEMPORARY_COLUMNS
        if column_name in dataframe.columns
    ]

    bronze_dataframes[entity_name] = dataframe.drop(
        *columns_to_remove
    )

display(
    bronze_dataframes["clientes_cdc"]
    .select(
        "cliente_id",
        "nome",
        "data_nascimento",
        "data_atualizacao",
        "operacao_cdc",
        "_batch_id",
        "_schema_version",
        "_record_hash",
        "_bronze_ingestion_timestamp"
    )
    .orderBy(
        "_batch_id",
        "cliente_id"
    )
    .limit(20)
)

# COMMAND ----------

# DBTITLE 1,Padronização da entidade Eventos de Risco
def save_bronze_table(
    dataframe: DataFrame,
    entity_name: str
) -> str:
    """
    Persiste a entidade na camada Bronze como tabela Delta gerenciada.

    O overwrite torna a execução idempotente para este desafio:
    uma nova execução reconstrói a Bronze com base na Landing validada.
    """

    table_name = (
        f"{CATALOG}.{BRONZE_SCHEMA}."
        f"bronze_{entity_name}"
    )

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table_name)
    )

    print(
        f"Tabela criada: {table_name} | "
        f"Registros: {dataframe.count()}"
    )

    return table_name

# COMMAND ----------

# DBTITLE 1,Padronização da entidade Estornos
bronze_tables = {}

for entity_name, dataframe in bronze_dataframes.items():
    bronze_tables[entity_name] = save_bronze_table(
        dataframe=dataframe,
        entity_name=entity_name
    )

# COMMAND ----------

# DBTITLE 1,Inclusão de metadados técnicos e hashes
display(
    spark.sql(
        f"""
        SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}
        """
    )
    .filter(
        F.col("tableName").startswith("bronze_")
    )
    .orderBy("tableName")
)

# COMMAND ----------

# DBTITLE 1,Persistência das tabelas Bronze
bronze_summary = []

for entity_name, table_name in bronze_tables.items():
    dataframe = spark.table(table_name)

    bronze_summary.append(
        (
            entity_name,
            table_name,
            dataframe.count(),
            dataframe.select("_batch_id").distinct().count(),
            len(dataframe.columns)
        )
    )

bronze_summary_df = spark.createDataFrame(
    bronze_summary,
    [
        "entidade",
        "tabela_bronze",
        "quantidade_registros",
        "quantidade_lotes",
        "quantidade_colunas"
    ]
)

display(
    bronze_summary_df.orderBy("entidade")
)

# COMMAND ----------

# DBTITLE 1,Validação das tabelas persistidas
cdc_entities = [
    "clientes_cdc",
    "contas_cdc",
    "cartoes_cdc"
]

cdc_summary_dataframes = []

for entity_name in cdc_entities:
    dataframe = spark.table(
        bronze_tables[entity_name]
    )

    summary_df = (
        dataframe
        .groupBy(
            "_entity_name",
            "_batch_id",
            "operacao_cdc"
        )
        .agg(
            F.count("*").alias("quantidade_registros")
        )
    )

    cdc_summary_dataframes.append(summary_df)

cdc_summary_df = reduce(
    lambda left_df, right_df: left_df.unionByName(right_df),
    cdc_summary_dataframes
)

display(
    cdc_summary_df.orderBy(
        "_entity_name",
        "_batch_id",
        "operacao_cdc"
    )
)

# COMMAND ----------

# DBTITLE 1,Resumo da carga Bronze
metadata_validation = []

for entity_name, table_name in bronze_tables.items():
    dataframe = spark.table(table_name)

    metadata_validation.append(
        (
            entity_name,
            "_record_hash" in dataframe.columns,
            "_record_payload_hash" in dataframe.columns,
            "_validation_errors" in dataframe.columns,
            "_validation_timestamp" in dataframe.columns
        )
    )

metadata_validation_df = spark.createDataFrame(
    metadata_validation,
    [
        "entidade",
        "possui_record_hash",
        "possui_record_payload_hash",
        "possui_validation_errors",
        "possui_validation_timestamp"
    ]
)

display(
    metadata_validation_df.orderBy("entidade")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resultado
# MAGIC
# MAGIC O processamento da camada Bronze foi concluído com sucesso.
# MAGIC
# MAGIC Os registros validados foram padronizados e persistidos em tabelas Delta, mantendo os metadados necessários para rastreabilidade e processamento incremental.
# MAGIC
# MAGIC ### Recursos implementados
# MAGIC
# MAGIC - Padronização de nomes e tipos
# MAGIC - Preservação dos campos de CDC
# MAGIC - Inclusão de metadados técnicos
# MAGIC - Geração de hashes determinísticos
# MAGIC - Persistência em Delta Lake
# MAGIC - Validação das tabelas Bronze
# MAGIC
# MAGIC ### Próximo Notebook
# MAGIC
# MAGIC **05_silver**
# MAGIC
# MAGIC Processamento de CDC, deduplicação, Late Arrival, validações referenciais e construção do histórico SCD Type 2.