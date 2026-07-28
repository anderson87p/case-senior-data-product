# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 07 — Quality Checks da Camada Gold
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Executar validações de qualidade, consistência e integridade dos dados da camada Gold, garantindo que o modelo dimensional e os Data Products atendam aos requisitos de confiabilidade para consumo analítico.
# MAGIC
# MAGIC **Tecnologias:** Databricks • PySpark • Delta Lake • Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Executar um conjunto de validações automatizadas sobre as tabelas da camada Gold, verificando a qualidade dos dados produzidos pelo pipeline antes da disponibilização para consumo analítico.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Validar unicidade das chaves.
# MAGIC - Validar campos obrigatórios.
# MAGIC - Validar integridade referencial.
# MAGIC - Validar consistência dos dados.
# MAGIC - Validar domínios de negócio.
# MAGIC - Realizar reconciliação entre Silver e Gold.
# MAGIC - Consolidar os resultados das validações.
# MAGIC - Persistir os resultados dos Quality Checks.
# MAGIC
# MAGIC ## Tipos de Validação
# MAGIC
# MAGIC - Unicidade
# MAGIC - Campos obrigatórios
# MAGIC - Integridade referencial
# MAGIC - Consistência de valores
# MAGIC - Domínio dos dados
# MAGIC - Reconciliação entre camadas
# MAGIC
# MAGIC ## Entradas
# MAGIC
# MAGIC - `workspace.case_gold.gold_dim_cliente`
# MAGIC - `workspace.case_gold.gold_dim_conta`
# MAGIC - `workspace.case_gold.gold_dim_cartao`
# MAGIC - `workspace.case_gold.gold_dim_estabelecimento`
# MAGIC - `workspace.case_gold.gold_fato_transacao`
# MAGIC - `workspace.case_gold.gold_cliente_mes`
# MAGIC - `workspace.case_gold.gold_indicadores_risco`
# MAGIC - `workspace.case_gold.gold_features_cliente`
# MAGIC
# MAGIC ## Saídas
# MAGIC
# MAGIC - `workspace.case_gold.gold_quality_check_results`
# MAGIC
# MAGIC ## Próximo Notebook
# MAGIC
# MAGIC **08_demo_queries**
# MAGIC
# MAGIC Execução de consultas analíticas para demonstração dos Data Products e validação funcional da camada Gold.

# COMMAND ----------

# DBTITLE 1,Configuração do ambiente
# Databricks notebook source

from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER_SCHEMA = "case_silver"
GOLD_SCHEMA = "case_gold"

print("Ambiente de quality checks configurado.")

# COMMAND ----------

# DBTITLE 1,Leitura das tabelas da camada Gold
silver_transacoes = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_transacoes"
)

gold_dim_cliente = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.gold_dim_cliente"
)

gold_dim_conta = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.gold_dim_conta"
)

gold_dim_cartao = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.gold_dim_cartao"
)

gold_dim_estabelecimento = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.gold_dim_estabelecimento"
)

gold_fato_transacao = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.gold_fato_transacao"
)

gold_cliente_mes = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.gold_cliente_mes"
)

gold_indicadores_risco = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.gold_indicadores_risco"
)

gold_features_cliente = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.gold_features_cliente"
)

print("Tabelas carregadas com sucesso.")

# COMMAND ----------

# DBTITLE 1,Definição das regras de qualidade
quality_results = []

def add_quality_result(
    check_name,
    table_name,
    status,
    metric_value,
    expected_value,
    description
):
    quality_results.append(
        (
            check_name,
            table_name,
            status,
            str(metric_value),
            str(expected_value),
            description
        )
    )


def check_unique_key(df, table_name, key_column):
    total = df.count()
    distinct_total = df.select(key_column).distinct().count()
    duplicates = total - distinct_total

    status = "PASS" if duplicates == 0 else "FAIL"

    add_quality_result(
        check_name=f"unicidade_{key_column}",
        table_name=table_name,
        status=status,
        metric_value=duplicates,
        expected_value=0,
        description=f"Quantidade de registros duplicados para {key_column}"
    )


def check_not_null(df, table_name, column_name):
    null_count = df.filter(F.col(column_name).isNull()).count()

    status = "PASS" if null_count == 0 else "FAIL"

    add_quality_result(
        check_name=f"nao_nulo_{column_name}",
        table_name=table_name,
        status=status,
        metric_value=null_count,
        expected_value=0,
        description=f"Quantidade de valores nulos em {column_name}"
    )

# COMMAND ----------

# DBTITLE 1,Validação de unicidade
check_unique_key(
    gold_dim_cliente,
    "gold_dim_cliente",
    "cliente_id"
)

check_unique_key(
    gold_dim_conta,
    "gold_dim_conta",
    "conta_id"
)

check_unique_key(
    gold_dim_cartao,
    "gold_dim_cartao",
    "cartao_id"
)

check_unique_key(
    gold_dim_estabelecimento,
    "gold_dim_estabelecimento",
    "estabelecimento_id"
)

check_unique_key(
    gold_fato_transacao,
    "gold_fato_transacao",
    "transacao_id"
)

check_unique_key(
    gold_features_cliente,
    "gold_features_cliente",
    "cliente_id"
)

print("Checks de unicidade concluídos.")

# COMMAND ----------

# DBTITLE 1,Validação de campos obrigatórios
required_columns = {
    "gold_dim_cliente": (
        gold_dim_cliente,
        ["cliente_sk", "cliente_id"]
    ),
    "gold_dim_conta": (
        gold_dim_conta,
        ["conta_sk", "conta_id", "cliente_id"]
    ),
    "gold_dim_cartao": (
        gold_dim_cartao,
        ["cartao_sk", "cartao_id", "conta_id"]
    ),
    "gold_dim_estabelecimento": (
        gold_dim_estabelecimento,
        ["estabelecimento_sk", "estabelecimento_id"]
    ),
    "gold_fato_transacao": (
        gold_fato_transacao,
        ["transacao_sk", "transacao_id", "conta_id", "data_transacao", "valor"]
    ),
    "gold_features_cliente": (
        gold_features_cliente,
        ["cliente_sk", "cliente_id"]
    )
}

for table_name, (df, columns) in required_columns.items():
    for column_name in columns:
        check_not_null(df, table_name, column_name)

print("Checks de campos obrigatórios concluídos.")

# COMMAND ----------

# DBTITLE 1,Validação de integridade referencial
silver_transaction_count = (
    silver_transacoes
    .select("transacao_id")
    .distinct()
    .count()
)

gold_transaction_count = (
    gold_fato_transacao
    .select("transacao_id")
    .distinct()
    .count()
)

difference = silver_transaction_count - gold_transaction_count

add_quality_result(
    check_name="reconciliacao_silver_gold",
    table_name="gold_fato_transacao",
    status="PASS" if difference == 0 else "FAIL",
    metric_value=difference,
    expected_value=0,
    description=(
        "Diferença entre transações distintas da Silver "
        "e da Gold"
    )
)

print(f"Silver: {silver_transaction_count}")
print(f"Gold:   {gold_transaction_count}")
print(f"Diferença: {difference}")

# COMMAND ----------

# DBTITLE 1,Validação de consistência dos dados
contas_sem_cliente = (
    gold_dim_conta.alias("conta")
    .join(
        gold_dim_cliente
        .select("cliente_id")
        .alias("cliente"),
        on="cliente_id",
        how="left_anti"
    )
    .count()
)

add_quality_result(
    check_name="integridade_conta_cliente",
    table_name="gold_dim_conta",
    status="PASS" if contas_sem_cliente == 0 else "WARNING",
    metric_value=contas_sem_cliente,
    expected_value=0,
    description=(
        "Contas sem cliente atual correspondente. "
        "WARNING esperado porque a Gold contém apenas registros vigentes, "
        "enquanto o histórico completo permanece na Silver."
    )
)


cartoes_sem_conta = (
    gold_dim_cartao.alias("cartao")
    .join(
        gold_dim_conta
        .select("conta_id")
        .alias("conta"),
        on="conta_id",
        how="left_anti"
    )
    .count()
)

add_quality_result(
    check_name="integridade_cartao_conta",
    table_name="gold_dim_cartao",
    status="PASS" if cartoes_sem_conta == 0 else "WARNING",
    metric_value=cartoes_sem_conta,
    expected_value=0,
    description=(
        "Cartões sem conta atual correspondente. "
        "WARNING esperado porque a Gold contém apenas registros vigentes, "
        "enquanto o histórico completo permanece na Silver."
    )
)


transacoes_sem_conta = (
    gold_fato_transacao
    .filter(F.col("conta_id").isNotNull())
    .join(
        gold_dim_conta.select("conta_id"),
        on="conta_id",
        how="left_anti"
    )
    .count()
)

add_quality_result(
    check_name="integridade_transacao_conta",
    table_name="gold_fato_transacao",
    status="PASS" if transacoes_sem_conta == 0 else "WARNING",
    metric_value=transacoes_sem_conta,
    expected_value=0,
    description=(
        "Transações históricas sem conta vigente correspondente na Gold. "
        "WARNING esperado porque as dimensões Gold representam o estado atual."
    )
)


transacoes_sem_estabelecimento = (
    gold_fato_transacao
    .filter(F.col("estabelecimento_id").isNotNull())
    .join(
        gold_dim_estabelecimento.select("estabelecimento_id"),
        on="estabelecimento_id",
        how="left_anti"
    )
    .count()
)

add_quality_result(
    check_name="integridade_transacao_estabelecimento",
    table_name="gold_fato_transacao",
    status="PASS" if transacoes_sem_estabelecimento == 0 else "FAIL",
    metric_value=transacoes_sem_estabelecimento,
    expected_value=0,
    description="Transações sem estabelecimento correspondente"
)

print("Checks de integridade referencial concluídos.")

# COMMAND ----------

# DBTITLE 1,Validação de domínios
valores_negativos = (
    gold_fato_transacao
    .filter(F.col("valor") < 0)
    .count()
)

add_quality_result(
    check_name="valor_transacao_nao_negativo",
    table_name="gold_fato_transacao",
    status="PASS" if valores_negativos == 0 else "FAIL",
    metric_value=valores_negativos,
    expected_value=0,
    description="Transações com valor negativo"
)


percentuais_invalidos_cliente_mes = (
    gold_cliente_mes
    .filter(
        (F.col("percentual_estorno") < 0) |
        (F.col("percentual_estorno") > 100) |
        (F.col("percentual_alto_risco") < 0) |
        (F.col("percentual_alto_risco") > 100)
    )
    .count()
)

add_quality_result(
    check_name="percentuais_cliente_mes_validos",
    table_name="gold_cliente_mes",
    status="PASS" if percentuais_invalidos_cliente_mes == 0 else "FAIL",
    metric_value=percentuais_invalidos_cliente_mes,
    expected_value=0,
    description="Percentuais fora do intervalo entre 0 e 100"
)


percentuais_invalidos_features = (
    gold_features_cliente
    .filter(
        (F.col("percentual_estorno") < 0) |
        (F.col("percentual_estorno") > 100) |
        (F.col("percentual_pix") < 0) |
        (F.col("percentual_pix") > 100)
    )
    .count()
)

add_quality_result(
    check_name="percentuais_features_validos",
    table_name="gold_features_cliente",
    status="PASS" if percentuais_invalidos_features == 0 else "FAIL",
    metric_value=percentuais_invalidos_features,
    expected_value=0,
    description="Percentuais fora do intervalo entre 0 e 100"
)

print("Checks de consistência concluídos.")

# COMMAND ----------

# DBTITLE 1,Reconciliação entre Silver e Gold
niveis_risco_invalidos = (
    gold_fato_transacao
    .filter(
        ~F.col("nivel_risco").isin(
            "SEM_RISCO",
            "BAIXO",
            "MEDIO",
            "ALTO"
        )
    )
    .count()
)

add_quality_result(
    check_name="dominio_nivel_risco",
    table_name="gold_fato_transacao",
    status="PASS" if niveis_risco_invalidos == 0 else "FAIL",
    metric_value=niveis_risco_invalidos,
    expected_value=0,
    description="Níveis de risco fora do domínio esperado"
)


classificacoes_invalidas = (
    gold_indicadores_risco
    .filter(
        ~F.col("classificacao_risco_cliente").isin(
            "BAIXO",
            "MEDIO",
            "ALTO"
        )
    )
    .count()
)

add_quality_result(
    check_name="dominio_classificacao_risco_cliente",
    table_name="gold_indicadores_risco",
    status="PASS" if classificacoes_invalidas == 0 else "FAIL",
    metric_value=classificacoes_invalidas,
    expected_value=0,
    description="Classificações de risco fora do domínio esperado"
)

print("Checks de domínio concluídos.")

# COMMAND ----------

# DBTITLE 1,Consolidação dos resultados das validações
quality_schema = [
    "check_name",
    "table_name",
    "status",
    "metric_value",
    "expected_value",
    "description"
]

quality_results_df = spark.createDataFrame(
    quality_results,
    quality_schema
).withColumn(
    "quality_check_timestamp",
    F.current_timestamp()
)

display(
    quality_results_df
    .orderBy("status", "table_name", "check_name")
)

# COMMAND ----------

# DBTITLE 1,Persistência dos resultados dos Quality Checks
quality_summary = (
    quality_results_df
    .groupBy("status")
    .agg(
        F.count("*").alias("quantidade_checks")
    )
    .orderBy("status")
)

display(quality_summary)

# COMMAND ----------

# DBTITLE 1,Resumo da execução dos Quality Checks
QUALITY_TABLE = (
    f"{CATALOG}.{GOLD_SCHEMA}.gold_quality_check_results"
)

(
    quality_results_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUALITY_TABLE)
)

print(f"Resultados persistidos em: {QUALITY_TABLE}")

# COMMAND ----------

# DBTITLE 1,Validação final dos Quality Checks
failed_checks = (
    quality_results_df
    .filter(F.col("status") == "FAIL")
    .count()
)

warning_checks = (
    quality_results_df
    .filter(F.col("status") == "WARNING")
    .count()
)

passed_checks = (
    quality_results_df
    .filter(F.col("status") == "PASS")
    .count()
)

print("=" * 80)
print("RESUMO FINAL DOS QUALITY CHECKS")
print("=" * 80)
print(f"Checks aprovados: {passed_checks}")
print(f"Warnings:         {warning_checks}")
print(f"Falhas críticas:  {failed_checks}")

if failed_checks > 0:
    raise Exception(
        f"Quality checks identificaram {failed_checks} falha(s) crítica(s)."
    )

print("Quality checks concluídos sem falhas críticas.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resultado
# MAGIC
# MAGIC Os Quality Checks da camada Gold foram executados com sucesso.
# MAGIC
# MAGIC As validações confirmam a consistência estrutural, referencial e analítica das tabelas produzidas pelo pipeline.
# MAGIC
# MAGIC ### Validações executadas
# MAGIC
# MAGIC - Unicidade das chaves
# MAGIC - Campos obrigatórios
# MAGIC - Integridade referencial
# MAGIC - Consistência dos dados
# MAGIC - Domínios de negócio
# MAGIC - Reconciliação entre Silver e Gold
# MAGIC
# MAGIC ### Resultado consolidado
# MAGIC
# MAGIC - **30 PASS**
# MAGIC - **3 WARNING**
# MAGIC - **0 FAIL**
# MAGIC
# MAGIC ### Observação
# MAGIC
# MAGIC Os registros classificados como **WARNING** são esperados e refletem uma decisão arquitetural do projeto.
# MAGIC
# MAGIC A camada **Silver** preserva o histórico completo das entidades utilizando **SCD Type 2**, enquanto a camada **Gold** disponibiliza apenas os registros vigentes (`registro_atual = true` e `registro_excluido = false`) para otimizar consultas analíticas. Dessa forma, determinadas validações referenciais podem identificar diferenças sem representar inconsistências nos dados.
# MAGIC
# MAGIC Todos os resultados foram persistidos na tabela:
# MAGIC
# MAGIC - `workspace.case_gold.gold_quality_check_results`
# MAGIC
# MAGIC ### Próximo Notebook
# MAGIC
# MAGIC **08_demo_queries**
# MAGIC
# MAGIC Demonstração das consultas analíticas, indicadores de negócio e exemplos de consumo dos Data Products da camada Gold.