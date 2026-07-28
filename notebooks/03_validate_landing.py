# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 03 — Validação da Camada Landing
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Validar os dados da camada Landing, identificando registros válidos e inválidos, aplicando regras de qualidade e direcionando inconsistências para a camada Quarantine.
# MAGIC
# MAGIC **Tecnologias:** Databricks • PySpark • Delta Lake • Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Aplicar regras de qualidade sobre os dados ingeridos na Landing, garantindo que apenas registros válidos avancem para a próxima etapa do pipeline.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Validar campos obrigatórios.
# MAGIC - Validar tipos de dados.
# MAGIC - Validar datas utilizando `try_cast`.
# MAGIC - Identificar registros duplicados dentro do mesmo batch.
# MAGIC - Direcionar registros inválidos para a camada Quarantine.
# MAGIC - Persistir registros validados para consumo da Bronze.
# MAGIC
# MAGIC ## Entradas
# MAGIC
# MAGIC - `landing_clientes_cdc`
# MAGIC - `landing_contas_cdc`
# MAGIC - `landing_cartoes_cdc`
# MAGIC - `landing_transacoes`
# MAGIC - `landing_eventos_risco`
# MAGIC - `landing_estornos`
# MAGIC
# MAGIC ## Saídas
# MAGIC
# MAGIC - `validated_clientes`
# MAGIC - `validated_contas`
# MAGIC - `validated_cartoes`
# MAGIC - `validated_transacoes`
# MAGIC - `validated_eventos_risco`
# MAGIC - `validated_estornos`
# MAGIC
# MAGIC - `quarantine_clientes`
# MAGIC - `quarantine_contas`
# MAGIC - `quarantine_cartoes`
# MAGIC - `quarantine_transacoes`
# MAGIC - `quarantine_eventos_risco`
# MAGIC - `quarantine_estornos`
# MAGIC
# MAGIC ### Observação Técnica
# MAGIC
# MAGIC Este notebook utiliza `try_cast()` para validação de datas e timestamps, garantindo compatibilidade com o ANSI Mode do Databricks e evitando falhas de execução em registros inválidos.
# MAGIC
# MAGIC ## Próximo Notebook
# MAGIC
# MAGIC **04_bronze**
# MAGIC
# MAGIC Transformação dos registros validados para a camada Bronze, realizando padronização de tipos, inclusão de hashes e persistência em Delta Lake.

# COMMAND ----------

# DBTITLE 1,Configuração do ambiente
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

CATALOG = spark.catalog.currentCatalog()

LANDING_SCHEMA = "case_landing"
QUARANTINE_SCHEMA = "case_quarantine"

ENTITIES = [
    "clientes_cdc",
    "contas_cdc",
    "cartoes_cdc",
    "transacoes",
    "eventos_risco",
    "estornos"
]

print(f"Catálogo: {CATALOG}")
print(f"Entidades: {ENTITIES}")

# COMMAND ----------

# DBTITLE 1,Leitura das tabelas Landing
VALIDATION_CONFIG = {
    "clientes_cdc": {
        "primary_key": "cliente_id",
        "required_columns": [
            "cliente_id",
            "cpf",
            "nome",
            "data_nascimento",
            "status_cliente",
            "data_atualizacao",
            "operacao_cdc"
        ],
        "date_columns": [
            "data_nascimento"
        ],
        "timestamp_columns": [
            "data_atualizacao"
        ],
        "numeric_columns": [],
        "domains": {
            "uf": ["BA", "PE", "CE", "SP", "RJ"],
            "status_cliente": ["ATIVO", "INATIVO"],
            "operacao_cdc": ["I", "U", "D"]
        }
    },

    "contas_cdc": {
        "primary_key": "conta_id",
        "required_columns": [
            "conta_id",
            "cliente_id",
            "tipo_conta",
            "saldo",
            "status_conta",
            "data_abertura",
            "data_atualizacao",
            "operacao_cdc"
        ],
        "date_columns": [
            "data_abertura"
        ],
        "timestamp_columns": [
            "data_atualizacao"
        ],
        "numeric_columns": [
            "saldo",
            "limite_credito"
        ],
        "domains": {
            "tipo_conta": [
                "CONTA_CORRENTE",
                "CONTA_POUPANCA",
                "CONTA_PAGAMENTO"
            ],
            "status_conta": [
                "ATIVA",
                "BLOQUEADA",
                "ENCERRADA"
            ],
            "operacao_cdc": ["I", "U", "D"]
        }
    },

    "cartoes_cdc": {
        "primary_key": "cartao_id",
        "required_columns": [
            "cartao_id",
            "conta_id",
            "tipo_cartao",
            "bandeira",
            "limite_cartao",
            "status_cartao",
            "data_emissao",
            "data_atualizacao",
            "operacao_cdc"
        ],
        "date_columns": [
            "data_emissao"
        ],
        "timestamp_columns": [
            "data_atualizacao"
        ],
        "numeric_columns": [
            "limite_cartao"
        ],
        "domains": {
            "tipo_cartao": ["CREDITO", "DEBITO", "MULTIPLO"],
            "bandeira": ["VISA", "MASTERCARD", "ELO"],
            "status_cartao": ["ATIVO", "BLOQUEADO", "CANCELADO"],
            "operacao_cdc": ["I", "U", "D"]
        }
    },

    "transacoes": {
        "primary_key": "transacao_id",
        "required_columns": [
            "transacao_id",
            "conta_id",
            "tipo_transacao",
            "valor",
            "status_transacao",
            "data_transacao",
            "data_ingestao_origem"
        ],
        "date_columns": [],
        "timestamp_columns": [
            "data_transacao",
            "data_ingestao_origem"
        ],
        "numeric_columns": [
            "valor"
        ],
        "domains": {
            "tipo_transacao": [
                "PIX",
                "TED",
                "DOC",
                "COMPRA_CARTAO",
                "SAQUE",
                "PAGAMENTO_BOLETO"
            ],
            "status_transacao": [
                "APROVADA",
                "NEGADA",
                "PENDENTE"
            ]
        }
    },

    "eventos_risco": {
        "primary_key": "evento_risco_id",
        "required_columns": [
            "evento_risco_id",
            "transacao_id",
            "tipo_evento",
            "score_risco",
            "nivel_risco",
            "data_evento"
        ],
        "date_columns": [],
        "timestamp_columns": [
            "data_evento"
        ],
        "numeric_columns": [
            "score_risco"
        ],
        "domains": {
            "tipo_evento": [
                "VALOR_ATIPICO",
                "LOCALIZACAO_SUSPEITA",
                "MULTIPLAS_TENTATIVAS",
                "DISPOSITIVO_NOVO",
                "POSSIVEL_FRAUDE"
            ],
            "nivel_risco": [
                "BAIXO",
                "MEDIO",
                "ALTO",
                "CRITICO"
            ]
        }
    },

    "estornos": {
        "primary_key": "estorno_id",
        "required_columns": [
            "estorno_id",
            "transacao_id",
            "valor_estorno",
            "motivo_estorno",
            "status_estorno",
            "data_estorno"
        ],
        "date_columns": [],
        "timestamp_columns": [
            "data_estorno"
        ],
        "numeric_columns": [
            "valor_estorno"
        ],
        "domains": {
            "motivo_estorno": [
                "FRAUDE",
                "DUPLICIDADE",
                "CLIENTE_NAO_RECONHECE",
                "ERRO_OPERACIONAL",
                "CANCELAMENTO"
            ],
            "status_estorno": [
                "SOLICITADO",
                "APROVADO",
                "NEGADO"
            ]
        }
    }
}

# COMMAND ----------

# DBTITLE 1,Definição das regras de validação
def build_validation_errors(
    dataframe: DataFrame,
    entity_name: str
) -> DataFrame:
    """
    Adiciona um array com os erros de qualidade encontrados.

    TRY_CAST retorna NULL para valores inválidos sem interromper
    a execução no modo ANSI do Databricks.
    """

    config = VALIDATION_CONFIG[entity_name]
    error_expressions = []

    # 1. Campos obrigatórios
    for column_name in config["required_columns"]:
        error_expressions.append(
            F.when(
                F.col(column_name).isNull()
                | (F.trim(F.col(column_name).cast("string")) == ""),
                F.lit(f"CAMPO_OBRIGATORIO:{column_name}")
            )
        )

    # 2. Tipos numéricos
    for column_name in config["numeric_columns"]:
        error_expressions.append(
            F.when(
                F.col(column_name).isNotNull()
                & F.expr(
                    f"try_cast(`{column_name}` AS DOUBLE) IS NULL"
                ),
                F.lit(f"TIPO_NUMERICO_INVALIDO:{column_name}")
            )
        )

    # 3. Datas
    for column_name in config["date_columns"]:
        error_expressions.append(
            F.when(
                F.col(column_name).isNotNull()
                & F.expr(
                    f"try_cast(`{column_name}` AS DATE) IS NULL"
                ),
                F.lit(f"DATA_INVALIDA:{column_name}")
            )
        )

    # 4. Timestamps
    for column_name in config["timestamp_columns"]:
        error_expressions.append(
            F.when(
                F.col(column_name).isNotNull()
                & F.expr(
                    f"try_cast(`{column_name}` AS TIMESTAMP) IS NULL"
                ),
                F.lit(f"TIMESTAMP_INVALIDO:{column_name}")
            )
        )

    # 5. Domínios
    for column_name, allowed_values in config["domains"].items():
        error_expressions.append(
            F.when(
                F.col(column_name).isNotNull()
                & ~F.col(column_name).isin(allowed_values),
                F.lit(f"DOMINIO_INVALIDO:{column_name}")
            )
        )

    return (
        dataframe
        .withColumn(
            "_validation_errors_raw",
            F.array(*error_expressions)
        )
        .withColumn(
            "_validation_errors",
            F.expr(
                """
                filter(
                    _validation_errors_raw,
                    validation_error -> validation_error IS NOT NULL
                )
                """
            )
        )
        .drop("_validation_errors_raw")
    )

# COMMAND ----------

# DBTITLE 1,Aplicação das validações por entidade
def mark_duplicates(
    dataframe: DataFrame,
    primary_key: str
) -> DataFrame:
    """
    Identifica registros repetidos dentro do mesmo lote.

    Alterações CDC da mesma chave em lotes diferentes são esperadas
    e não devem ser classificadas como duplicidade.
    """

    technical_columns = {
        "_source_file",
        "_source_file_path",
        "_source_file_size",
        "_source_file_modification_timestamp",
        "_source_path",
        "_entity_name",
        "_processing_date",
        "_batch_id",
        "_landing_ingestion_timestamp",
        "_schema_version",
        "_validation_errors"
    }

    business_columns = [
        column_name
        for column_name in dataframe.columns
        if column_name not in technical_columns
        and not column_name.startswith("_")
    ]

    payload_expression = F.concat_ws(
        "||",
        *[
            F.coalesce(
                F.col(column_name).cast("string"),
                F.lit("<NULL>")
            )
            for column_name in business_columns
        ]
    )

    dataframe_with_hash = dataframe.withColumn(
        "_record_payload_hash",
        F.sha2(payload_expression, 256)
    )

    duplicate_window = (
        Window
        .partitionBy(
            "_batch_id",
            "_record_payload_hash"
        )
        .orderBy(
            F.col("_source_file").asc(),
            F.col("_landing_ingestion_timestamp").asc()
        )
    )

    return (
        dataframe_with_hash
        .withColumn(
            "_duplicate_rank",
            F.row_number().over(duplicate_window)
        )
        .withColumn(
            "_is_duplicate",
            F.col("_duplicate_rank") > 1
        )
    )

# COMMAND ----------

# DBTITLE 1,Persistência dos registros validados
def validate_entity(
    entity_name: str
) -> tuple[DataFrame, DataFrame]:
    """
    Valida uma entidade da Landing e separa registros válidos
    dos registros enviados para quarentena.
    """

    config = VALIDATION_CONFIG[entity_name]
    primary_key = config["primary_key"]

    table_name = (
        f"{CATALOG}.{LANDING_SCHEMA}."
        f"landing_{entity_name}"
    )

    source_df = spark.table(table_name)

    validated_df = build_validation_errors(
        dataframe=source_df,
        entity_name=entity_name
    )

    validated_df = mark_duplicates(
        dataframe=validated_df,
        primary_key=primary_key
    )

    validated_df = (
        validated_df
        .withColumn(
            "_validation_errors",
            F.when(
                F.col("_is_duplicate"),
                F.array_union(
                    F.col("_validation_errors"),
                    F.array(F.lit("REGISTRO_DUPLICADO"))
                )
            ).otherwise(
                F.col("_validation_errors")
            )
        )
        .withColumn(
            "_is_valid",
            F.size(F.col("_validation_errors")) == 0
        )
        .withColumn(
            "_validation_timestamp",
            F.current_timestamp()
        )
    )

    valid_df = (
        validated_df
        .filter(F.col("_is_valid"))
        .drop(
            "_duplicate_rank",
            "_is_duplicate",
            "_is_valid"
        )
    )

    quarantine_df = (
        validated_df
        .filter(~F.col("_is_valid"))
        .withColumn(
            "_quarantine_reason",
            F.concat_ws(
                " | ",
                F.col("_validation_errors")
            )
        )
        .drop(
            "_duplicate_rank",
            "_is_duplicate",
            "_is_valid"
        )
    )

    return valid_df, quarantine_df

# COMMAND ----------

# DBTITLE 1,Persistência da camada Quarantine
validated_dataframes = {}
quarantine_dataframes = {}

for entity_name in ENTITIES:
    print(f"\nValidando entidade: {entity_name}")

    valid_df, quarantine_df = validate_entity(
        entity_name=entity_name
    )

    validated_dataframes[entity_name] = valid_df
    quarantine_dataframes[entity_name] = quarantine_df

    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()

    print(f"Válidos: {valid_count}")
    print(f"Quarentena: {quarantine_count}")

# COMMAND ----------

# DBTITLE 1,Validação das tabelas persistidas
quality_summary = []

for entity_name in ENTITIES:
    source_count = spark.table(
        f"{CATALOG}.{LANDING_SCHEMA}.landing_{entity_name}"
    ).count()

    valid_count = validated_dataframes[entity_name].count()
    quarantine_count = quarantine_dataframes[entity_name].count()

    valid_percentage = round(
        (valid_count / source_count) * 100,
        2
    ) if source_count > 0 else 0.0

    quality_summary.append(
        (
            entity_name,
            source_count,
            valid_count,
            quarantine_count,
            valid_percentage
        )
    )

quality_summary_df = spark.createDataFrame(
    quality_summary,
    [
        "entidade",
        "registros_origem",
        "registros_validos",
        "registros_quarentena",
        "percentual_validos"
    ]
)

display(
    quality_summary_df.orderBy("entidade")
)

# COMMAND ----------

# DBTITLE 1,Conferência dos registros validados
display(
    quarantine_dataframes["clientes_cdc"]
    .select(
        "cliente_id",
        "cpf",
        "nome",
        "data_nascimento",
        "uf",
        "status_cliente",
        "operacao_cdc",
        "_batch_id",
        "_quarantine_reason"
    )
    .orderBy(
        "_batch_id",
        "cliente_id"
    )
)

# COMMAND ----------

# DBTITLE 1,Conferência dos registros em Quarantine
validated_tables = {}
quarantine_tables = {}

for entity_name in ENTITIES:
    valid_table_name = (
        f"{CATALOG}.{LANDING_SCHEMA}."
        f"validated_{entity_name}"
    )

    quarantine_table_name = (
        f"{CATALOG}.{QUARANTINE_SCHEMA}."
        f"quarantine_{entity_name}"
    )

    (
        validated_dataframes[entity_name]
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(valid_table_name)
    )

    (
        quarantine_dataframes[entity_name]
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(quarantine_table_name)
    )

    validated_tables[entity_name] = valid_table_name
    quarantine_tables[entity_name] = quarantine_table_name

    print(f"Válidos: {valid_table_name}")
    print(f"Quarentena: {quarantine_table_name}")

# COMMAND ----------

# DBTITLE 1,Resumo da validação da camada Landing
display(
    spark.sql(
        f"""
        SHOW TABLES IN {CATALOG}.{QUARANTINE_SCHEMA}
        """
    )
    .filter(
        F.col("tableName").startswith("quarantine_")
    )
    .orderBy("tableName")
)

# COMMAND ----------

# DBTITLE 1,Resumo da persistência das entidades
validation_persistence_summary = []

for entity_name in ENTITIES:
    valid_count = spark.table(
        validated_tables[entity_name]
    ).count()

    quarantine_count = spark.table(
        quarantine_tables[entity_name]
    ).count()

    validation_persistence_summary.append(
        (
            entity_name,
            valid_count,
            quarantine_count
        )
    )

validation_persistence_summary_df = spark.createDataFrame(
    validation_persistence_summary,
    [
        "entidade",
        "registros_validos",
        "registros_quarentena"
    ]
)

display(
    validation_persistence_summary_df.orderBy("entidade")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resultado
# MAGIC
# MAGIC A validação da camada Landing foi concluída com sucesso.
# MAGIC
# MAGIC As regras de qualidade foram aplicadas em todas as entidades do projeto.
# MAGIC
# MAGIC ### Funcionalidades implementadas
# MAGIC
# MAGIC - Validação de campos obrigatórios
# MAGIC - Validação de tipos de dados
# MAGIC - Validação de datas utilizando `try_cast`
# MAGIC - Identificação de registros duplicados
# MAGIC - Separação entre registros válidos e inválidos
# MAGIC - Persistência das tabelas validadas
# MAGIC - Persistência da camada Quarantine
# MAGIC
# MAGIC Os registros válidos estão disponíveis para processamento na camada Bronze.
# MAGIC
# MAGIC ### Próximo Notebook
# MAGIC
# MAGIC **04_bronze**