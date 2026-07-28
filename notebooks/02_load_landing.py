# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 02 — Ingestão da Camada Landing
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Ingerir os arquivos sintéticos armazenados no Volume do Unity Catalog, adicionando metadados técnicos e persistindo os dados na camada Landing.
# MAGIC
# MAGIC **Tecnologias:** Databricks • Unity Catalog Volumes • PySpark • Delta Lake
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Realizar a ingestão dos arquivos gerados pelo notebook anterior, preservando os dados em formato próximo ao original e adicionando informações técnicas de rastreabilidade.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Descobrir automaticamente os arquivos disponíveis no Volume.
# MAGIC - Identificar entidades e batches de ingestão.
# MAGIC - Ler arquivos com suporte a evolução de schema.
# MAGIC - Adicionar metadados técnicos dos arquivos de origem.
# MAGIC - Consolidar os arquivos por entidade.
# MAGIC - Persistir os dados em tabelas Delta na camada Landing.
# MAGIC - Disponibilizar os dados para a etapa de validação.
# MAGIC
# MAGIC ## Entradas
# MAGIC
# MAGIC - Arquivos armazenados em:
# MAGIC   - `workspace.case_landing.landing_files`
# MAGIC
# MAGIC ## Saídas
# MAGIC
# MAGIC - `workspace.case_landing.landing_clientes_cdc`
# MAGIC - `workspace.case_landing.landing_contas_cdc`
# MAGIC - `workspace.case_landing.landing_cartoes_cdc`
# MAGIC - `workspace.case_landing.landing_transacoes`
# MAGIC - `workspace.case_landing.landing_eventos_risco`
# MAGIC - `workspace.case_landing.landing_estornos`
# MAGIC
# MAGIC ## Próximo Notebook
# MAGIC
# MAGIC **03_validate_landing**
# MAGIC
# MAGIC Valida regras de qualidade, separa registros válidos e inválidos e direciona inconsistências para a camada Quarantine.

# COMMAND ----------

# DBTITLE 1,Importações e configurações
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = spark.catalog.currentCatalog()

LANDING_SCHEMA = "case_landing"
LANDING_VOLUME = "landing_files"

VOLUME_PATH = (
    f"/Volumes/{CATALOG}/"
    f"{LANDING_SCHEMA}/"
    f"{LANDING_VOLUME}"
)

ENTITIES = [
    "clientes_cdc",
    "contas_cdc",
    "cartoes_cdc",
    "transacoes",
    "eventos_risco",
    "estornos"
]

print(f"Catálogo: {CATALOG}")
print(f"Landing: {VOLUME_PATH}")
print(f"Entidades: {ENTITIES}")

# COMMAND ----------

# DBTITLE 1,Configuração dos caminhos e tabelas
def list_directories(path: str) -> list[str]:
    """
    Retorna apenas os nomes dos subdiretórios existentes no caminho.
    """
    return sorted(
        item.name.rstrip("/")
        for item in dbutils.fs.ls(path)
        if item.isDir()
    )

# COMMAND ----------

# DBTITLE 1,Descoberta dos arquivos no Volume
discovered_entities = list_directories(VOLUME_PATH)

print("Entidades encontradas:")

for entity_name in discovered_entities:
    print(f"- {entity_name}")

# COMMAND ----------

# DBTITLE 1,Identificação das entidades e batches
landing_inventory = []

for entity_name in ENTITIES:
    entity_path = f"{VOLUME_PATH}/{entity_name}"

    date_directories = list_directories(entity_path)

    for date_directory in date_directories:
        if not date_directory.startswith("data="):
            continue

        processing_date = date_directory.replace("data=", "")
        date_path = f"{entity_path}/{date_directory}"

        batch_directories = list_directories(date_path)

        for batch_directory in batch_directories:
            if not batch_directory.startswith("lote="):
                continue

            batch_id = batch_directory.replace("lote=", "")
            batch_path = f"{date_path}/{batch_directory}"

            landing_inventory.append(
                (
                    entity_name,
                    processing_date,
                    batch_id,
                    batch_path
                )
            )

inventory_df = spark.createDataFrame(
    landing_inventory,
    [
        "entity_name",
        "processing_date",
        "batch_id",
        "batch_path"
    ]
)

display(
    inventory_df.orderBy(
        "entity_name",
        "processing_date",
        "batch_id"
    )
)

# COMMAND ----------

# DBTITLE 1,Configuração da ingestão
display(
    inventory_df
    .groupBy("entity_name")
    .agg(
        F.countDistinct("batch_id").alias("quantidade_lotes"),
        F.min("processing_date").alias("primeira_data"),
        F.max("processing_date").alias("ultima_data")
    )
    .orderBy("entity_name")
)

# COMMAND ----------

# DBTITLE 1,Inclusão dos metadados técnicos
def read_landing_batch(
    entity_name: str,
    processing_date: str,
    batch_id: str,
    batch_path: str
) -> DataFrame:
    """
    Lê um lote CSV da Landing, preserva os campos de negócio como texto
    e adiciona metadados técnicos compatíveis com Unity Catalog.
    """

    raw_df = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(batch_path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("_source_file"),
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.col("_metadata.file_size").alias("_source_file_size"),
            F.col("_metadata.file_modification_time").alias(
                "_source_file_modification_timestamp"
            )
        )
    )

    business_columns = [
        column_name
        for column_name in raw_df.columns
        if not column_name.startswith("_source_")
    ]

    raw_df = raw_df.select(
        *[
            F.col(column_name).cast("string").alias(column_name)
            for column_name in business_columns
        ],
        "_source_file",
        "_source_file_path",
        "_source_file_size",
        "_source_file_modification_timestamp"
    )

    return (
        raw_df
        .withColumn("_source_path", F.lit(batch_path))
        .withColumn("_entity_name", F.lit(entity_name))
        .withColumn(
            "_processing_date",
            F.to_date(F.lit(processing_date))
        )
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn(
            "_landing_ingestion_timestamp",
            F.current_timestamp()
        )
    )

# COMMAND ----------

# DBTITLE 1,Leitura e consolidação das entidades
def load_entity_from_landing(
    entity_name: str,
    inventory: list[tuple]
) -> DataFrame:
    """
    Lê todos os lotes de uma entidade e realiza união por nome,
    aceitando colunas introduzidas pela evolução de schema.
    """

    entity_batches = [
        item
        for item in inventory
        if item[0] == entity_name
    ]

    if not entity_batches:
        raise ValueError(
            f"Nenhum lote encontrado para a entidade {entity_name}"
        )

    batch_dataframes = []

    for (
        current_entity,
        processing_date,
        batch_id,
        batch_path
    ) in entity_batches:

        print(
            f"Lendo {current_entity} | "
            f"{batch_id} | {processing_date}"
        )

        batch_df = read_landing_batch(
            entity_name=current_entity,
            processing_date=processing_date,
            batch_id=batch_id,
            batch_path=batch_path
        )

        batch_dataframes.append(batch_df)

    return reduce(
        lambda left_df, right_df: left_df.unionByName(
            right_df,
            allowMissingColumns=True
        ),
        batch_dataframes
    )

# COMMAND ----------

# DBTITLE 1,Persistência da entidade Clientes
landing_dataframes = {}

for entity_name in ENTITIES:
    print(f"\nProcessando entidade: {entity_name}")

    entity_df = load_entity_from_landing(
        entity_name=entity_name,
        inventory=landing_inventory
    )

    landing_dataframes[entity_name] = entity_df

    total_records = entity_df.count()

    print(f"Total bruto: {total_records}")

# COMMAND ----------

# DBTITLE 1,Persistência da entidade Contas
landing_summary_dataframes = []

for entity_name, dataframe in landing_dataframes.items():
    entity_summary = (
        dataframe
        .groupBy(
            "_entity_name",
            "_processing_date",
            "_batch_id"
        )
        .agg(
            F.count("*").alias("quantidade_registros"),
            F.countDistinct("_source_file").alias(
                "quantidade_arquivos"
            )
        )
    )

    landing_summary_dataframes.append(entity_summary)

landing_summary_df = reduce(
    lambda left_df, right_df: left_df.unionByName(right_df),
    landing_summary_dataframes
)

display(
    landing_summary_df.orderBy(
        "_entity_name",
        "_processing_date",
        "_batch_id"
    )
)

# COMMAND ----------

# DBTITLE 1,Persistência da entidade Cartões
EVOLUTION_COLUMNS = {
    "clientes_cdc": "segmento_cliente",
    "contas_cdc": "modalidade_atendimento",
    "cartoes_cdc": "cartao_virtual",
    "transacoes": "canal_transacao",
    "eventos_risco": "modelo_risco_versao",
    "estornos": "origem_solicitacao"
}

for entity_name, evolution_column in EVOLUTION_COLUMNS.items():
    dataframe = landing_dataframes[entity_name]

    landing_dataframes[entity_name] = dataframe.withColumn(
        "_schema_version",
        F.when(
            F.col(evolution_column).isNotNull(),
            F.lit("v2")
        ).otherwise(F.lit("v1"))
    )

# COMMAND ----------

# DBTITLE 1,Persistência da entidade Transações
def save_landing_table(
    dataframe: DataFrame,
    entity_name: str
) -> str:
    """
    Persiste a visão consolidada da Landing em uma tabela Delta.
    O overwrite torna a execução idempotente para este desafio.
    """

    table_name = (
        f"{CATALOG}.{LANDING_SCHEMA}."
        f"landing_{entity_name}"
    )

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table_name)
    )

    total_records = dataframe.count()

    print(
        f"Tabela gravada: {table_name} | "
        f"Registros: {total_records}"
    )

    return table_name

# COMMAND ----------

# DBTITLE 1,Persistência da entidade Eventos de Risco
landing_tables = {}

for entity_name, dataframe in landing_dataframes.items():
    landing_tables[entity_name] = save_landing_table(
        dataframe=dataframe,
        entity_name=entity_name
    )

# COMMAND ----------

# DBTITLE 1,Persistência da entidade Estornos
display(
    spark.sql(
        f"""
        SHOW TABLES IN {CATALOG}.{LANDING_SCHEMA}
        """
    )
    .filter(
        F.col("tableName").startswith("landing_")
    )
    .orderBy("tableName")
)

# COMMAND ----------

# DBTITLE 1,Validação da camada Landing
table_validation = []

for entity_name, table_name in landing_tables.items():
    dataframe = spark.table(table_name)

    table_validation.append(
        (
            entity_name,
            table_name,
            dataframe.count(),
            len(dataframe.columns),
            dataframe.select("_batch_id")
                .distinct()
                .count()
        )
    )

table_validation_df = spark.createDataFrame(
    table_validation,
    [
        "entidade",
        "tabela",
        "quantidade_registros",
        "quantidade_colunas",
        "quantidade_lotes"
    ]
)

display(
    table_validation_df.orderBy("entidade")
)

# COMMAND ----------

# DBTITLE 1,Encerramento
display(
    spark.table(
        f"{CATALOG}.{LANDING_SCHEMA}.landing_clientes_cdc"
    )
    .filter(F.col("segmento_cliente").isNotNull())
    .select(
        "cliente_id",
        "nome",
        "segmento_cliente",
        "_batch_id",
        "_schema_version",
        "_source_file",
        "_source_file_path"
    )
    .orderBy("cliente_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resultado
# MAGIC
# MAGIC Os arquivos disponíveis no Volume foram descobertos e ingeridos com sucesso.
# MAGIC
# MAGIC A camada Landing preserva os dados recebidos e adiciona metadados técnicos para garantir:
# MAGIC
# MAGIC - rastreabilidade;
# MAGIC - auditabilidade;
# MAGIC - identificação de batch;
# MAGIC - identificação da entidade;
# MAGIC - suporte à evolução de schema.
# MAGIC
# MAGIC ### Próximo Notebook
# MAGIC
# MAGIC `03_validate_landing`