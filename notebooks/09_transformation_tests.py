# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Testes de Transformação da Camada Silver
# MAGIC
# MAGIC Testes de integração executados sobre as tabelas Delta produzidas pelo pipeline.
# MAGIC O notebook falha imediatamente quando uma regra crítica não é atendida.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = spark.catalog.currentCatalog()
SILVER_SCHEMA = "case_silver"
QUARANTINE_SCHEMA = "case_quarantine"

results = []


def record_test(name: str, condition: bool, details: str) -> None:
    status = "PASS" if condition else "FAIL"
    results.append((name, status, details))
    if not condition:
        raise AssertionError(f"{name}: {details}")

# COMMAND ----------

# DBTITLE 1,SCD2 — exatamente um registro atual por chave
for table_name, primary_key in {
    "silver_clientes": "cliente_id",
    "silver_contas": "conta_id",
    "silver_cartoes": "cartao_id",
}.items():
    dataframe = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")
    invalid_keys = (
        dataframe.filter(F.col("registro_atual"))
        .groupBy(primary_key)
        .count()
        .filter(F.col("count") != 1)
        .count()
    )
    record_test(
        f"{table_name}: um registro atual por chave",
        invalid_keys == 0,
        f"chaves inválidas={invalid_keys}",
    )

# COMMAND ----------

# DBTITLE 1,SCD2 — vigências não sobrepostas e versões sequenciais
for table_name, primary_key in {
    "silver_clientes": "cliente_id",
    "silver_contas": "conta_id",
    "silver_cartoes": "cartao_id",
}.items():
    dataframe = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")
    window = __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy(primary_key).orderBy("vigencia_inicio")
    invalid_periods = (
        dataframe
        .withColumn("proximo_inicio", F.lead("vigencia_inicio").over(window))
        .withColumn("versao_esperada", F.row_number().over(window))
        .filter(
            (F.col("versao_registro") != F.col("versao_esperada"))
            | (
                F.col("proximo_inicio").isNotNull()
                & (F.col("vigencia_fim") >= F.col("proximo_inicio"))
            )
            | (
                F.col("proximo_inicio").isNull()
                & F.col("vigencia_fim").isNotNull()
            )
        )
        .count()
    )
    record_test(
        f"{table_name}: vigência e versão SCD2",
        invalid_periods == 0,
        f"linhas inválidas={invalid_periods}",
    )

# COMMAND ----------

# DBTITLE 1,CDC — deletes preservados como exclusão lógica
for table_name in ["silver_clientes", "silver_contas", "silver_cartoes"]:
    dataframe = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")
    invalid_deletes = dataframe.filter(
        (F.col("operacao_cdc") == "D") & (~F.col("registro_excluido"))
    ).count()
    record_test(
        f"{table_name}: CDC delete como soft delete",
        invalid_deletes == 0,
        f"deletes inválidos={invalid_deletes}",
    )

# COMMAND ----------

# DBTITLE 1,Deduplicação — chaves transacionais únicas
for table_name, primary_key in {
    "silver_transacoes": "transacao_id",
    "silver_eventos_risco": "evento_risco_id",
    "silver_estornos": "estorno_id",
}.items():
    dataframe = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")
    duplicate_keys = dataframe.groupBy(primary_key).count().filter(F.col("count") > 1).count()
    record_test(
        f"{table_name}: chave técnica única",
        duplicate_keys == 0,
        f"chaves duplicadas={duplicate_keys}",
    )

# COMMAND ----------

# DBTITLE 1,Late arrival — regra superior a 24 horas
transacoes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_transacoes")
invalid_late_arrival = transacoes.filter(
    F.col("late_arrival") != (F.col("atraso_ingestao_horas") > F.lit(24))
).count()
record_test(
    "silver_transacoes: classificação de late arrival",
    invalid_late_arrival == 0,
    f"linhas inválidas={invalid_late_arrival}",
)

# COMMAND ----------

# DBTITLE 1,Quarantine — motivos de referência preenchidos
for table_name in [
    "quarantine_transacoes_referencia",
    "quarantine_eventos_risco_referencia",
    "quarantine_estornos_referencia",
]:
    full_name = f"{CATALOG}.{QUARANTINE_SCHEMA}.{table_name}"
    dataframe = spark.table(full_name)
    invalid_reasons = dataframe.filter(
        F.col("_quarantine_reason").isNull()
        | (F.trim(F.col("_quarantine_reason")) == "")
    ).count()
    record_test(
        f"{table_name}: motivo de quarentena",
        invalid_reasons == 0,
        f"linhas sem motivo={invalid_reasons}",
    )

# COMMAND ----------

# DBTITLE 1,Resumo dos testes
results_df = spark.createDataFrame(results, ["teste", "status", "detalhes"])
display(results_df.orderBy("teste"))

failures = results_df.filter(F.col("status") == "FAIL").count()
assert failures == 0, f"Existem {failures} testes de transformação com falha"

print(f"Testes executados: {len(results)} | PASS: {len(results)} | FAIL: 0")
