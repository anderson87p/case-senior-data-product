# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 00 — Setup do Ambiente
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Preparar o ambiente Databricks para execução do pipeline Lakehouse, configurando o Unity Catalog e os schemas utilizados em todas as camadas do projeto.
# MAGIC
# MAGIC **Tecnologias:** Databricks • Unity Catalog • Delta Lake • PySpark
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Preparar o ambiente Databricks para a execução do case, criando e validando os schemas necessários no Unity Catalog e estabelecendo a estrutura da arquitetura Lakehouse que será utilizada ao longo de todo o pipeline de dados.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Configurar o catálogo utilizado pelo projeto.
# MAGIC - Criar os schemas das camadas da arquitetura Lakehouse.
# MAGIC - Validar a disponibilidade das estruturas criadas.
# MAGIC - Garantir que o ambiente esteja preparado para a execução dos notebooks subsequentes.
# MAGIC
# MAGIC ## Camadas Criadas
# MAGIC
# MAGIC - `workspace.case_landing`
# MAGIC - `workspace.case_bronze`
# MAGIC - `workspace.case_silver`
# MAGIC - `workspace.case_gold`
# MAGIC - `workspace.case_quarantine`
# MAGIC - `workspace.case_audit`
# MAGIC
# MAGIC ## Próximo Notebook
# MAGIC
# MAGIC **01_generate_sample_data**
# MAGIC
# MAGIC Geração de dados sintéticos para simular um ambiente bancário, incluindo cenários de CDC, Late Arrival, Schema Evolution e registros inválidos para validação do pipeline.

# COMMAND ----------

# DBTITLE 1,Configurações do projeto
# Configuração inicial do ambiente do projeto
project_name = "case_senior_data_product"

print(f"Projeto: {project_name}")
print(f"Catálogo atual: {spark.catalog.currentCatalog()}")
print(f"Schema atual: {spark.catalog.currentDatabase()}")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Definição das camadas
from datetime import datetime

CATALOG = spark.catalog.currentCatalog()

SCHEMAS = {
    "landing": "case_landing",
    "bronze": "case_bronze",
    "silver": "case_silver",
    "gold": "case_gold",
    "quarantine": "case_quarantine",
    "audit": "case_audit"
}

PROJECT_NAME = "case_senior_data_product"

BATCHES = [
    "batch_001",
    "batch_002",
    "batch_003"
]

print(f"Projeto: {PROJECT_NAME}")
print(f"Catálogo: {CATALOG}")
print(f"Execução: {datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Criação dos schemas
for layer, schema_name in SCHEMAS.items():
    full_schema_name = f"{CATALOG}.{schema_name}"

    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS {full_schema_name}
        COMMENT 'Schema da camada {layer} do projeto {PROJECT_NAME}'
        """
    )

    print(f"Schema disponível: {full_schema_name}")

# COMMAND ----------

# DBTITLE 1,Validação do ambiente
schemas_df = spark.sql(f"SHOW SCHEMAS IN {CATALOG}")

display(
    schemas_df.filter(
        schemas_df.databaseName.startswith("case_")
    )
)

# COMMAND ----------

# DBTITLE 1,Conferência dos schemas
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMAS["audit"]}.pipeline_execution_log (
        execution_id STRING,
        notebook_name STRING,
        pipeline_layer STRING,
        entity_name STRING,
        batch_id STRING,
        execution_status STRING,
        records_read BIGINT,
        records_written BIGINT,
        records_rejected BIGINT,
        execution_start TIMESTAMP,
        execution_end TIMESTAMP,
        error_message STRING
    )
    USING DELTA
    COMMENT 'Log de auditoria das execuções do pipeline'
    """
)

print("Tabela de auditoria criada com sucesso.")

# COMMAND ----------

# DBTITLE 1,Resultado
display(
    spark.sql(
        f"SHOW TABLES IN {CATALOG}.{SCHEMAS['audit']}"
    )
)