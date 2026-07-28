# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 01 — Geração de Dados Sintéticos
# MAGIC
# MAGIC **Projeto:** Senior Data Engineering Case – Lakehouse Data Platform
# MAGIC
# MAGIC **Autor:** Anderson de Alencar Pereira
# MAGIC
# MAGIC **Objetivo:** Gerar dados sintéticos para simular um ambiente bancário, contemplando cenários de ingestão incremental, CDC, Late Arrival, Schema Evolution e registros inválidos para validação do pipeline de dados.
# MAGIC
# MAGIC **Tecnologias:** PySpark • Databricks • Delta Lake • Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo do Notebook
# MAGIC
# MAGIC Gerar arquivos sintéticos que representem diferentes entidades de um ambiente bancário, permitindo validar todas as etapas do pipeline de dados implementado no projeto.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Gerar dados sintéticos para as entidades do domínio bancário.
# MAGIC - Simular múltiplos lotes (batches) de ingestão.
# MAGIC - Simular operações de CDC (Insert, Update e Delete).
# MAGIC - Simular registros com Late Arrival.
# MAGIC - Simular Schema Evolution.
# MAGIC - Gerar registros inválidos para validação da camada de qualidade.
# MAGIC - Persistir os arquivos na camada Landing.
# MAGIC
# MAGIC ## Entidades Geradas
# MAGIC
# MAGIC - Clientes
# MAGIC - Contas
# MAGIC - Cartões
# MAGIC - Transações
# MAGIC - Eventos de Risco
# MAGIC - Estornos
# MAGIC
# MAGIC ## Cenários Simulados
# MAGIC
# MAGIC - CDC (Change Data Capture)
# MAGIC - Late Arrival
# MAGIC - Schema Evolution
# MAGIC - Registros Inválidos
# MAGIC - Múltiplos Batches
# MAGIC
# MAGIC ## Próximo Notebook
# MAGIC
# MAGIC **02_load_landing**
# MAGIC
# MAGIC Realiza a ingestão dos arquivos gerados para a camada Landing, adicionando metadados técnicos e preparando os dados para validação.

# COMMAND ----------

# DBTITLE 1,Configuração do ambiente
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql import types as T
import random
import uuid

random.seed(42)

CATALOG = spark.catalog.currentCatalog()
LANDING_SCHEMA = "case_landing"
LANDING_VOLUME = "landing_files"

VOLUME_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}"

print(f"Catálogo: {CATALOG}")
print(f"Volume da Landing: {VOLUME_PATH}")
print(f"Spark: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Criação do Volume Landing
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS
        {CATALOG}.{LANDING_SCHEMA}.{LANDING_VOLUME}
    COMMENT 'Arquivos de entrada da Landing Zone do case transacional'
    """
)

print(f"Volume criado ou já existente: {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,Configuração dos parâmetros
def generate_cpf(index: int) -> str:
    return f"{index:011d}"


def generate_timestamp(
    start_date: datetime,
    end_date: datetime
) -> datetime:
    seconds = int((end_date - start_date).total_seconds())
    return start_date + timedelta(seconds=random.randint(0, seconds))


def generate_id(prefix: str, index: int) -> str:
    return f"{prefix}_{index:06d}"

# COMMAND ----------

# DBTITLE 1,Geração de Clientes
customer_names = [
    "Ana Souza",
    "Bruno Silva",
    "Carla Santos",
    "Daniel Oliveira",
    "Eduarda Lima",
    "Felipe Costa",
    "Gabriela Rocha",
    "Henrique Alves",
    "Isabela Ferreira",
    "João Carvalho"
]

cities = [
    ("Salvador", "BA"),
    ("Lauro de Freitas", "BA"),
    ("Recife", "PE"),
    ("Fortaleza", "CE"),
    ("São Paulo", "SP"),
    ("Rio de Janeiro", "RJ")
]

customer_rows = []

for index in range(1, 1001):
    city, state = random.choice(cities)
    customer_rows.append(
        {
            "cliente_id": generate_id("CLI", index),
            "cpf": generate_cpf(index),
            "nome": random.choice(customer_names),
            "data_nascimento": (
                datetime(1960, 1, 1)
                + timedelta(days=random.randint(0, 20000))
            ).date(),
            "cidade": city,
            "uf": state,
            "status_cliente": random.choice(["ATIVO", "ATIVO", "ATIVO", "INATIVO"]),
            "data_atualizacao": generate_timestamp(
                datetime(2025, 1, 1),
                datetime(2026, 7, 20)
            ),
            "operacao_cdc": "I"
        }
    )

clientes_batch_001 = spark.createDataFrame(customer_rows)

display(clientes_batch_001.limit(10))
print(f"Clientes gerados: {clientes_batch_001.count()}")

# COMMAND ----------

# DBTITLE 1,Geração de Contas
account_rows = []

account_types = [
    "CONTA_CORRENTE",
    "CONTA_POUPANCA",
    "CONTA_PAGAMENTO"
]

for index in range(1, 1201):
    account_rows.append(
        {
            "conta_id": generate_id("CTA", index),
            "cliente_id": generate_id(
                "CLI",
                random.randint(1, 1000)
            ),
            "tipo_conta": random.choice(account_types),
            "agencia": f"{random.randint(1, 9999):04d}",
            "saldo": round(random.uniform(-5000, 100000), 2),
            "limite_credito": round(random.uniform(0, 30000), 2),
            "status_conta": random.choice(
                ["ATIVA", "ATIVA", "ATIVA", "BLOQUEADA", "ENCERRADA"]
            ),
            "data_abertura": (
                datetime(2015, 1, 1)
                + timedelta(days=random.randint(0, 4000))
            ).date(),
            "data_atualizacao": generate_timestamp(
                datetime(2025, 1, 1),
                datetime(2026, 7, 20)
            ),
            "operacao_cdc": "I"
        }
    )

contas_batch_001 = spark.createDataFrame(account_rows)

display(contas_batch_001.limit(10))
print(f"Contas geradas: {contas_batch_001.count()}")

# COMMAND ----------

# DBTITLE 1, Geração de Cartões
card_rows = []

card_types = ["CREDITO", "DEBITO", "MULTIPLO"]
card_brands = ["VISA", "MASTERCARD", "ELO"]

for index in range(1, 901):
    card_rows.append(
        {
            "cartao_id": generate_id("CAR", index),
            "conta_id": generate_id(
                "CTA",
                random.randint(1, 1200)
            ),
            "tipo_cartao": random.choice(card_types),
            "bandeira": random.choice(card_brands),
            "limite_cartao": round(random.uniform(500, 50000), 2),
            "status_cartao": random.choice(
                ["ATIVO", "ATIVO", "ATIVO", "BLOQUEADO", "CANCELADO"]
            ),
            "data_emissao": (
                datetime(2018, 1, 1)
                + timedelta(days=random.randint(0, 3000))
            ).date(),
            "data_atualizacao": generate_timestamp(
                datetime(2025, 1, 1),
                datetime(2026, 7, 20)
            ),
            "operacao_cdc": "I"
        }
    )

cartoes_batch_001 = spark.createDataFrame(card_rows)

display(cartoes_batch_001.limit(10))
print(f"Cartões gerados: {cartoes_batch_001.count()}")

# COMMAND ----------

# DBTITLE 1,Geração de Transações
transaction_rows = []

transaction_types = [
    "PIX",
    "TED",
    "DOC",
    "COMPRA_CARTAO",
    "SAQUE",
    "PAGAMENTO_BOLETO"
]

transaction_statuses = [
    "APROVADA",
    "APROVADA",
    "APROVADA",
    "NEGADA",
    "PENDENTE"
]

for index in range(1, 15001):
    transaction_date = generate_timestamp(
        datetime(2026, 1, 1),
        datetime(2026, 7, 20)
    )

    transaction_rows.append(
        {
            "transacao_id": generate_id("TRX", index),
            "conta_id": generate_id(
                "CTA",
                random.randint(1, 1200)
            ),
            "cartao_id": (
                generate_id("CAR", random.randint(1, 900))
                if random.random() > 0.35
                else None
            ),
            "tipo_transacao": random.choice(transaction_types),
            "valor": round(random.uniform(5, 20000), 2),
            "status_transacao": random.choice(transaction_statuses),
            "data_transacao": transaction_date,
            "estabelecimento_id": generate_id(
                "EST",
                random.randint(1, 300)
            ),
            "data_ingestao_origem": transaction_date
            + timedelta(minutes=random.randint(1, 180))
        }
    )

transacoes_batch_001 = spark.createDataFrame(transaction_rows)

display(transacoes_batch_001.limit(10))
print(f"Transações geradas: {transacoes_batch_001.count()}")

# COMMAND ----------

# DBTITLE 1,Geração de Eventos de Risco
risk_rows = []

risk_types = [
    "VALOR_ATIPICO",
    "LOCALIZACAO_SUSPEITA",
    "MULTIPLAS_TENTATIVAS",
    "DISPOSITIVO_NOVO",
    "POSSIVEL_FRAUDE"
]

for index in range(1, 701):
    risk_rows.append(
        {
            "evento_risco_id": generate_id("RSK", index),
            "transacao_id": generate_id(
                "TRX",
                random.randint(1, 15000)
            ),
            "tipo_evento": random.choice(risk_types),
            "score_risco": round(random.uniform(0, 1), 4),
            "nivel_risco": random.choice(
                ["BAIXO", "MEDIO", "ALTO", "CRITICO"]
            ),
            "data_evento": generate_timestamp(
                datetime(2026, 1, 1),
                datetime(2026, 7, 20)
            )
        }
    )

eventos_risco_batch_001 = spark.createDataFrame(risk_rows)

display(eventos_risco_batch_001.limit(10))
print(f"Eventos de risco gerados: {eventos_risco_batch_001.count()}")

# COMMAND ----------

# DBTITLE 1,Geração de Estornos
chargeback_rows = []

chargeback_reasons = [
    "FRAUDE",
    "DUPLICIDADE",
    "CLIENTE_NAO_RECONHECE",
    "ERRO_OPERACIONAL",
    "CANCELAMENTO"
]

for index in range(1, 301):
    chargeback_rows.append(
        {
            "estorno_id": generate_id("ESTO", index),
            "transacao_id": generate_id(
                "TRX",
                random.randint(1, 15000)
            ),
            "valor_estorno": round(random.uniform(5, 10000), 2),
            "motivo_estorno": random.choice(chargeback_reasons),
            "status_estorno": random.choice(
                ["SOLICITADO", "APROVADO", "NEGADO"]
            ),
            "data_estorno": generate_timestamp(
                datetime(2026, 1, 1),
                datetime(2026, 7, 20)
            )
        }
    )

estornos_batch_001 = spark.createDataFrame(chargeback_rows)

display(estornos_batch_001.limit(10))
print(f"Estornos gerados: {estornos_batch_001.count()}")

# COMMAND ----------

# DBTITLE 1,Persistência Batch 001
def write_landing_csv(
    dataframe,
    entity_name: str,
    batch_id: str,
    processing_date: str
) -> str:
    output_path = (
        f"{VOLUME_PATH}/"
        f"{entity_name}/"
        f"data={processing_date}/"
        f"lote={batch_id}"
    )

    (
        dataframe
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(output_path)
    )

    print(
        f"{entity_name}: {dataframe.count()} registros "
        f"gravados em {output_path}"
    )

    return output_path

# COMMAND ----------

# DBTITLE 1,Validação Batch 001
PROCESSING_DATE_BATCH_001 = "2026-07-20"

batch_001_dataframes = {
    "clientes_cdc": clientes_batch_001,
    "contas_cdc": contas_batch_001,
    "cartoes_cdc": cartoes_batch_001,
    "transacoes": transacoes_batch_001,
    "eventos_risco": eventos_risco_batch_001,
    "estornos": estornos_batch_001
}

batch_001_paths = {}

for entity_name, dataframe in batch_001_dataframes.items():
    batch_001_paths[entity_name] = write_landing_csv(
        dataframe=dataframe,
        entity_name=entity_name,
        batch_id="batch_001",
        processing_date=PROCESSING_DATE_BATCH_001
    )

# COMMAND ----------

# DBTITLE 1,Preparação Batch 002
display(
    spark.read
    .option("header", True)
    .csv(
        f"{VOLUME_PATH}/clientes_cdc/"
        f"data={PROCESSING_DATE_BATCH_001}/"
        f"lote=batch_001"
    )
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Persistência Batch 002
from pyspark.sql.window import Window
from pyspark.sql import functions as F

BATCH_002 = "batch_002"
PROCESSING_DATE_BATCH_002 = "2026-07-21"

print(f"Lote: {BATCH_002}")
print(f"Data de processamento: {PROCESSING_DATE_BATCH_002}")

# COMMAND ----------

# DBTITLE 1,Validação Batch 002
def add_row_number(dataframe, key_column: str):
    """
    Adiciona numeração determinística com base na chave da entidade.
    Facilita a seleção de registros para UPDATE e DELETE.
    """
    window_spec = Window.orderBy(F.col(key_column))

    return dataframe.withColumn(
        "_row_number",
        F.row_number().over(window_spec)
    )

# COMMAND ----------

# DBTITLE 1,Preparação Batch 003
clientes_numbered = add_row_number(
    clientes_batch_001,
    "cliente_id"
)

# 60 atualizações
clientes_updates = (
    clientes_numbered
    .filter(
        (F.col("_row_number") >= 1)
        & (F.col("_row_number") <= 60)
    )
    .drop("_row_number")
    .withColumn(
        "cidade",
        F.when(
            F.col("cidade") == "Salvador",
            F.lit("Lauro de Freitas")
        ).otherwise(F.lit("Salvador"))
    )
    .withColumn(
        "data_atualizacao",
        F.to_timestamp(F.lit("2026-07-21 09:00:00"))
    )
    .withColumn("operacao_cdc", F.lit("U"))
)

# 10 exclusões — registros diferentes dos updates
clientes_deletes = (
    clientes_numbered
    .filter(
        (F.col("_row_number") >= 61)
        & (F.col("_row_number") <= 70)
    )
    .drop("_row_number")
    .withColumn(
        "data_atualizacao",
        F.to_timestamp(F.lit("2026-07-21 09:30:00"))
    )
    .withColumn("operacao_cdc", F.lit("D"))
)

print(f"Updates de clientes: {clientes_updates.count()}")
print(f"Deletes de clientes: {clientes_deletes.count()}")

# COMMAND ----------

# DBTITLE 1,Aplicação de CDC
new_customer_rows = []

for index in range(1001, 1021):
    city, state = random.choice(cities)

    new_customer_rows.append(
        {
            "cliente_id": generate_id("CLI", index),
            "cpf": generate_cpf(index),
            "nome": random.choice(customer_names),
            "data_nascimento": (
                datetime(1960, 1, 1)
                + timedelta(days=random.randint(0, 20000))
            ).date(),
            "cidade": city,
            "uf": state,
            "status_cliente": "ATIVO",
            "data_atualizacao": generate_timestamp(
                datetime(2026, 7, 21, 8, 0),
                datetime(2026, 7, 21, 18, 0)
            ),
            "operacao_cdc": "I"
        }
    )

clientes_inserts = spark.createDataFrame(
    new_customer_rows,
    schema=clientes_batch_001.schema
)

clientes_batch_002 = (
    clientes_updates
    .unionByName(clientes_inserts)
    .unionByName(clientes_deletes)
)

print(f"Inserts de clientes: {clientes_inserts.count()}")
print(f"Total de clientes no Batch 002: {clientes_batch_002.count()}")

display(
    clientes_batch_002
    .groupBy("operacao_cdc")
    .count()
    .orderBy("operacao_cdc")
)

# COMMAND ----------

# DBTITLE 1,Simulação de Late Arrival
contas_numbered = add_row_number(
    contas_batch_001,
    "conta_id"
)

contas_updates = (
    contas_numbered
    .filter(
        (F.col("_row_number") >= 1)
        & (F.col("_row_number") <= 100)
    )
    .drop("_row_number")
    .withColumn(
        "saldo",
        F.round(F.col("saldo") + F.lit(500.00), 2)
    )
    .withColumn(
        "limite_credito",
        F.round(F.col("limite_credito") + F.lit(1000.00), 2)
    )
    .withColumn(
        "data_atualizacao",
        F.to_timestamp(F.lit("2026-07-21 10:00:00"))
    )
    .withColumn("operacao_cdc", F.lit("U"))
)

contas_deletes = (
    contas_numbered
    .filter(
        (F.col("_row_number") >= 101)
        & (F.col("_row_number") <= 110)
    )
    .drop("_row_number")
    .withColumn("status_conta", F.lit("ENCERRADA"))
    .withColumn(
        "data_atualizacao",
        F.to_timestamp(F.lit("2026-07-21 10:30:00"))
    )
    .withColumn("operacao_cdc", F.lit("D"))
)

print(f"Updates de contas: {contas_updates.count()}")
print(f"Deletes de contas: {contas_deletes.count()}")

# COMMAND ----------

# DBTITLE 1,Simulação de Schema Evolution
new_account_rows = []

for index in range(1201, 1221):
    new_account_rows.append(
        {
            "conta_id": generate_id("CTA", index),
            "cliente_id": generate_id(
                "CLI",
                random.randint(1, 1020)
            ),
            "tipo_conta": random.choice(account_types),
            "agencia": f"{random.randint(1, 9999):04d}",
            "saldo": round(random.uniform(0, 100000), 2),
            "limite_credito": round(random.uniform(0, 30000), 2),
            "status_conta": "ATIVA",
            "data_abertura": datetime(2026, 7, 21).date(),
            "data_atualizacao": generate_timestamp(
                datetime(2026, 7, 21, 8, 0),
                datetime(2026, 7, 21, 18, 0)
            ),
            "operacao_cdc": "I"
        }
    )

contas_inserts = spark.createDataFrame(
    new_account_rows,
    schema=contas_batch_001.schema
)

contas_batch_002 = (
    contas_updates
    .unionByName(contas_inserts)
    .unionByName(contas_deletes)
)

print(f"Inserts de contas: {contas_inserts.count()}")
print(f"Total de contas no Batch 002: {contas_batch_002.count()}")

display(
    contas_batch_002
    .groupBy("operacao_cdc")
    .count()
    .orderBy("operacao_cdc")
)

# COMMAND ----------

# DBTITLE 1,Inserção de Registros Inválidos
cartoes_numbered = add_row_number(
    cartoes_batch_001,
    "cartao_id"
)

cartoes_updates = (
    cartoes_numbered
    .filter(
        (F.col("_row_number") >= 1)
        & (F.col("_row_number") <= 60)
    )
    .drop("_row_number")
    .withColumn(
        "limite_cartao",
        F.round(F.col("limite_cartao") * F.lit(1.10), 2)
    )
    .withColumn(
        "data_atualizacao",
        F.to_timestamp(F.lit("2026-07-21 11:00:00"))
    )
    .withColumn("operacao_cdc", F.lit("U"))
)

cartoes_deletes = (
    cartoes_numbered
    .filter(
        (F.col("_row_number") >= 61)
        & (F.col("_row_number") <= 68)
    )
    .drop("_row_number")
    .withColumn("status_cartao", F.lit("CANCELADO"))
    .withColumn(
        "data_atualizacao",
        F.to_timestamp(F.lit("2026-07-21 11:30:00"))
    )
    .withColumn("operacao_cdc", F.lit("D"))
)

print(f"Updates de cartões: {cartoes_updates.count()}")
print(f"Deletes de cartões: {cartoes_deletes.count()}")

# COMMAND ----------

# DBTITLE 1,Atualização Clientes
new_card_rows = []

for index in range(901, 916):
    new_card_rows.append(
        {
            "cartao_id": generate_id("CAR", index),
            "conta_id": generate_id(
                "CTA",
                random.randint(1, 1220)
            ),
            "tipo_cartao": random.choice(card_types),
            "bandeira": random.choice(card_brands),
            "limite_cartao": round(random.uniform(500, 50000), 2),
            "status_cartao": "ATIVO",
            "data_emissao": datetime(2026, 7, 21).date(),
            "data_atualizacao": generate_timestamp(
                datetime(2026, 7, 21, 8, 0),
                datetime(2026, 7, 21, 18, 0)
            ),
            "operacao_cdc": "I"
        }
    )

cartoes_inserts = spark.createDataFrame(
    new_card_rows,
    schema=cartoes_batch_001.schema
)

cartoes_batch_002 = (
    cartoes_updates
    .unionByName(cartoes_inserts)
    .unionByName(cartoes_deletes)
)

print(f"Inserts de cartões: {cartoes_inserts.count()}")
print(f"Total de cartões no Batch 002: {cartoes_batch_002.count()}")

display(
    cartoes_batch_002
    .groupBy("operacao_cdc")
    .count()
    .orderBy("operacao_cdc")
)

# COMMAND ----------

# DBTITLE 1,Atualização Contas
batch_002_transaction_rows = []

for index in range(15001, 17001):
    transaction_date = generate_timestamp(
        datetime(2026, 7, 21, 0, 0),
        datetime(2026, 7, 21, 23, 59)
    )

    batch_002_transaction_rows.append(
        {
            "transacao_id": generate_id("TRX", index),
            "conta_id": generate_id(
                "CTA",
                random.randint(1, 1220)
            ),
            "cartao_id": (
                generate_id("CAR", random.randint(1, 915))
                if random.random() > 0.35
                else None
            ),
            "tipo_transacao": random.choice(transaction_types),
            "valor": round(random.uniform(5, 20000), 2),
            "status_transacao": random.choice(transaction_statuses),
            "data_transacao": transaction_date,
            "estabelecimento_id": generate_id(
                "EST",
                random.randint(1, 300)
            ),
            "data_ingestao_origem": (
                transaction_date
                + timedelta(minutes=random.randint(1, 180))
            )
        }
    )

transacoes_batch_002 = spark.createDataFrame(
    batch_002_transaction_rows,
    schema=transacoes_batch_001.schema
)

print(f"Transações do Batch 002: {transacoes_batch_002.count()}")

# COMMAND ----------

# DBTITLE 1,Atualização Cartões
batch_002_risk_rows = []

for index in range(701, 751):
    batch_002_risk_rows.append(
        {
            "evento_risco_id": generate_id("RSK", index),
            "transacao_id": generate_id(
                "TRX",
                random.randint(15001, 17000)
            ),
            "tipo_evento": random.choice(risk_types),
            "score_risco": round(random.uniform(0, 1), 4),
            "nivel_risco": random.choice(
                ["BAIXO", "MEDIO", "ALTO", "CRITICO"]
            ),
            "data_evento": generate_timestamp(
                datetime(2026, 7, 21, 0, 0),
                datetime(2026, 7, 21, 23, 59)
            )
        }
    )

eventos_risco_batch_002 = spark.createDataFrame(
    batch_002_risk_rows,
    schema=eventos_risco_batch_001.schema
)

print(
    f"Eventos de risco do Batch 002: "
    f"{eventos_risco_batch_002.count()}"
)

# COMMAND ----------

# DBTITLE 1,Atualização Transações
batch_002_chargeback_rows = []

for index in range(301, 311):
    batch_002_chargeback_rows.append(
        {
            "estorno_id": generate_id("ESTO", index),
            "transacao_id": generate_id(
                "TRX",
                random.randint(15001, 17000)
            ),
            "valor_estorno": round(random.uniform(5, 10000), 2),
            "motivo_estorno": random.choice(chargeback_reasons),
            "status_estorno": random.choice(
                ["SOLICITADO", "APROVADO", "NEGADO"]
            ),
            "data_estorno": generate_timestamp(
                datetime(2026, 7, 21, 0, 0),
                datetime(2026, 7, 21, 23, 59)
            )
        }
    )

estornos_batch_002 = spark.createDataFrame(
    batch_002_chargeback_rows,
    schema=estornos_batch_001.schema
)

print(f"Estornos do Batch 002: {estornos_batch_002.count()}")

# COMMAND ----------

# DBTITLE 1,Atualização Eventos de Risco
batch_002_dataframes = {
    "clientes_cdc": clientes_batch_002,
    "contas_cdc": contas_batch_002,
    "cartoes_cdc": cartoes_batch_002,
    "transacoes": transacoes_batch_002,
    "eventos_risco": eventos_risco_batch_002,
    "estornos": estornos_batch_002
}

batch_002_paths = {}

for entity_name, dataframe in batch_002_dataframes.items():
    batch_002_paths[entity_name] = write_landing_csv(
        dataframe=dataframe,
        entity_name=entity_name,
        batch_id=BATCH_002,
        processing_date=PROCESSING_DATE_BATCH_002
    )

# COMMAND ----------

# DBTITLE 1,Atualização Estornos
batch_002_summary = []

for entity_name, dataframe in batch_002_dataframes.items():
    batch_002_summary.append(
        (
            entity_name,
            BATCH_002,
            dataframe.count()
        )
    )

batch_002_summary_df = spark.createDataFrame(
    batch_002_summary,
    ["entidade", "batch_id", "quantidade_registros"]
)

display(
    batch_002_summary_df.orderBy("entidade")
)

# COMMAND ----------

# DBTITLE 1,Persistência Batch 003
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import types as T

BATCH_003 = "batch_003"
PROCESSING_DATE_BATCH_003 = "2026-07-22"


def to_string_dataframe(dataframe):
    """Converte as colunas para string, preservando o formato bruto da Landing."""
    return dataframe.select(
        *[
            F.col(column_name).cast("string").alias(column_name)
            for column_name in dataframe.columns
        ]
    )


def create_string_dataframe(rows, columns):
    """Cria DataFrame textual permitindo valores inválidos na Landing."""
    schema = T.StructType(
        [
            T.StructField(column_name, T.StringType(), True)
            for column_name in columns
        ]
    )

    normalized_rows = [
        tuple(
            None if row.get(column_name) is None
            else str(row.get(column_name))
            for column_name in columns
        )
        for row in rows
    ]

    return spark.createDataFrame(normalized_rows, schema=schema)


def union_dataframes(dataframes):
    """Une DataFrames pelo nome, aceitando evolução de schema."""
    return reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=True
        ),
        dataframes
    )


print(f"Lote: {BATCH_003}")
print(f"Data de processamento: {PROCESSING_DATE_BATCH_003}")

# COMMAND ----------

# DBTITLE 1,Validação Clientes
customer_columns_v2 = clientes_batch_001.columns + [
    "segmento_cliente"
]

# Dados atrasados: alterações antigas chegando no Batch 003
clientes_late = (
    to_string_dataframe(clientes_batch_001)
    .orderBy("cliente_id")
    .limit(5)
    .withColumn("cidade", F.lit("Feira de Santana"))
    .withColumn("data_atualizacao", F.lit("2026-06-15 10:00:00"))
    .withColumn("operacao_cdc", F.lit("U"))
    .withColumn("segmento_cliente", F.lit(None).cast("string"))
)

# Três registros repetidos duas vezes
clientes_duplicate_source = (
    to_string_dataframe(clientes_batch_002)
    .orderBy("cliente_id")
    .limit(3)
    .withColumn("segmento_cliente", F.lit(None).cast("string"))
)

clientes_duplicates = clientes_duplicate_source.unionByName(
    clientes_duplicate_source
)

invalid_customer_rows = [
    {
        "cliente_id": None,
        "cpf": "11111111111",
        "nome": "Cliente sem ID",
        "data_nascimento": "1980-01-01",
        "cidade": "Salvador",
        "uf": "BA",
        "status_cliente": "ATIVO",
        "data_atualizacao": "2026-07-22 09:00:00",
        "operacao_cdc": "I"
    },
    {
        "cliente_id": "CLI_INV_002",
        "cpf": "123",
        "nome": "CPF inválido",
        "data_nascimento": "1985-02-10",
        "cidade": "Salvador",
        "uf": "BA",
        "status_cliente": "ATIVO",
        "data_atualizacao": "2026-07-22 09:01:00",
        "operacao_cdc": "I"
    },
    {
        "cliente_id": "CLI_INV_003",
        "cpf": "99999999999",
        "nome": None,
        "data_nascimento": "1990-03-10",
        "cidade": "Recife",
        "uf": "PE",
        "status_cliente": "ATIVO",
        "data_atualizacao": "2026-07-22 09:02:00",
        "operacao_cdc": "I"
    },
    {
        "cliente_id": "CLI_INV_004",
        "cpf": "88888888888",
        "nome": "Data inválida",
        "data_nascimento": "31/31/2020",
        "cidade": "Fortaleza",
        "uf": "CE",
        "status_cliente": "ATIVO",
        "data_atualizacao": "2026-07-22 09:03:00",
        "operacao_cdc": "I"
    },
    {
        "cliente_id": "CLI_INV_005",
        "cpf": "77777777777",
        "nome": "UF inválida",
        "data_nascimento": "1975-05-15",
        "cidade": "Cidade desconhecida",
        "uf": "XX",
        "status_cliente": "ATIVO",
        "data_atualizacao": "2026-07-22 09:04:00",
        "operacao_cdc": "I"
    },
    {
        "cliente_id": "CLI_INV_006",
        "cpf": "66666666666",
        "nome": "Status inválido",
        "data_nascimento": "1988-06-12",
        "cidade": "Salvador",
        "uf": "BA",
        "status_cliente": "DESCONHECIDO",
        "data_atualizacao": "2026-07-22 09:05:00",
        "operacao_cdc": "I"
    },
    {
        "cliente_id": "CLI_INV_007",
        "cpf": "55555555555",
        "nome": "Operação inválida",
        "data_nascimento": "1981-07-18",
        "cidade": "Salvador",
        "uf": "BA",
        "status_cliente": "ATIVO",
        "data_atualizacao": "2026-07-22 09:06:00",
        "operacao_cdc": "X"
    },
    {
        "cliente_id": "CLI_INV_008",
        "cpf": "44444444444",
        "nome": "Timestamp inválido",
        "data_nascimento": "1982-08-20",
        "cidade": "Salvador",
        "uf": "BA",
        "status_cliente": "ATIVO",
        "data_atualizacao": "DATA_INVALIDA",
        "operacao_cdc": "I"
    }
]

clientes_invalidos = create_string_dataframe(
    invalid_customer_rows,
    customer_columns_v2
)

evolved_customer_rows = []

for index in range(1021, 1026):
    evolved_customer_rows.append(
        {
            "cliente_id": generate_id("CLI", index),
            "cpf": generate_cpf(index),
            "nome": random.choice(customer_names),
            "data_nascimento": "1985-01-01",
            "cidade": "Salvador",
            "uf": "BA",
            "status_cliente": "ATIVO",
            "data_atualizacao": "2026-07-22 12:00:00",
            "operacao_cdc": "I",
            "segmento_cliente": random.choice(
                ["VAREJO", "ALTA_RENDA", "EMPRESARIAL"]
            )
        }
    )

clientes_evolucao_schema = create_string_dataframe(
    evolved_customer_rows,
    customer_columns_v2
)

clientes_batch_003 = union_dataframes(
    [
        clientes_late,
        clientes_duplicates,
        clientes_invalidos,
        clientes_evolucao_schema
    ]
)

print(f"Clientes Batch 003: {clientes_batch_003.count()}")
display(clientes_batch_003.groupBy("operacao_cdc").count())

# COMMAND ----------

# DBTITLE 1,Validação Contas
account_columns_v2 = contas_batch_001.columns + [
    "modalidade_atendimento"
]

contas_late = (
    to_string_dataframe(contas_batch_001)
    .orderBy("conta_id")
    .limit(5)
    .withColumn("saldo", F.lit("15000.00"))
    .withColumn("data_atualizacao", F.lit("2026-06-10 08:00:00"))
    .withColumn("operacao_cdc", F.lit("U"))
    .withColumn("modalidade_atendimento", F.lit(None).cast("string"))
)

contas_duplicate_source = (
    to_string_dataframe(contas_batch_002)
    .orderBy("conta_id")
    .limit(4)
    .withColumn("modalidade_atendimento", F.lit(None).cast("string"))
)

contas_duplicates = contas_duplicate_source.unionByName(
    contas_duplicate_source
)

invalid_account_rows = []

for index in range(1, 12):
    row = {
        "conta_id": f"CTA_INV_{index:03d}",
        "cliente_id": "CLI_000001",
        "tipo_conta": "CONTA_CORRENTE",
        "agencia": "0001",
        "saldo": "1000.00",
        "limite_credito": "5000.00",
        "status_conta": "ATIVA",
        "data_abertura": "2020-01-01",
        "data_atualizacao": "2026-07-22 10:00:00",
        "operacao_cdc": "I",
        "modalidade_atendimento": None
    }

    issue = index % 6

    if issue == 0:
        row["conta_id"] = None
    elif issue == 1:
        row["cliente_id"] = "CLI_INEXISTENTE"
    elif issue == 2:
        row["saldo"] = "SALDO_INVALIDO"
    elif issue == 3:
        row["limite_credito"] = "LIMITE_INVALIDO"
    elif issue == 4:
        row["status_conta"] = "STATUS_INVALIDO"
    else:
        row["data_abertura"] = "DATA_INVALIDA"

    invalid_account_rows.append(row)

contas_invalidas = create_string_dataframe(
    invalid_account_rows,
    account_columns_v2
)

evolved_account_rows = []

for index in range(1221, 1226):
    evolved_account_rows.append(
        {
            "conta_id": generate_id("CTA", index),
            "cliente_id": generate_id(
                "CLI",
                random.randint(1, 1025)
            ),
            "tipo_conta": random.choice(account_types),
            "agencia": f"{random.randint(1, 9999):04d}",
            "saldo": str(round(random.uniform(0, 50000), 2)),
            "limite_credito": str(round(random.uniform(0, 20000), 2)),
            "status_conta": "ATIVA",
            "data_abertura": "2026-07-22",
            "data_atualizacao": "2026-07-22 12:30:00",
            "operacao_cdc": "I",
            "modalidade_atendimento": random.choice(
                ["DIGITAL", "AGENCIA", "HIBRIDO"]
            )
        }
    )

contas_evolucao_schema = create_string_dataframe(
    evolved_account_rows,
    account_columns_v2
)

contas_batch_003 = union_dataframes(
    [
        contas_late,
        contas_duplicates,
        contas_invalidas,
        contas_evolucao_schema
    ]
)

print(f"Contas Batch 003: {contas_batch_003.count()}")

# COMMAND ----------

# DBTITLE 1,Validação Cartões
card_columns_v2 = cartoes_batch_001.columns + [
    "cartao_virtual"
]

cartoes_late = (
    to_string_dataframe(cartoes_batch_001)
    .orderBy("cartao_id")
    .limit(4)
    .withColumn("limite_cartao", F.lit("12000.00"))
    .withColumn("data_atualizacao", F.lit("2026-06-12 09:00:00"))
    .withColumn("operacao_cdc", F.lit("U"))
    .withColumn("cartao_virtual", F.lit(None).cast("string"))
)

cartoes_duplicate_source = (
    to_string_dataframe(cartoes_batch_002)
    .orderBy("cartao_id")
    .limit(3)
    .withColumn("cartao_virtual", F.lit(None).cast("string"))
)

cartoes_duplicates = cartoes_duplicate_source.unionByName(
    cartoes_duplicate_source
)

invalid_card_rows = []

for index in range(1, 10):
    row = {
        "cartao_id": f"CAR_INV_{index:03d}",
        "conta_id": "CTA_000001",
        "tipo_cartao": "CREDITO",
        "bandeira": "VISA",
        "limite_cartao": "5000.00",
        "status_cartao": "ATIVO",
        "data_emissao": "2022-01-01",
        "data_atualizacao": "2026-07-22 11:00:00",
        "operacao_cdc": "I",
        "cartao_virtual": None
    }

    issue = index % 5

    if issue == 0:
        row["cartao_id"] = None
    elif issue == 1:
        row["conta_id"] = "CTA_INEXISTENTE"
    elif issue == 2:
        row["limite_cartao"] = "LIMITE_INVALIDO"
    elif issue == 3:
        row["bandeira"] = "BANDEIRA_INVALIDA"
    else:
        row["status_cartao"] = "STATUS_INVALIDO"

    invalid_card_rows.append(row)

cartoes_invalidos = create_string_dataframe(
    invalid_card_rows,
    card_columns_v2
)

evolved_card_rows = []

for index in range(916, 920):
    evolved_card_rows.append(
        {
            "cartao_id": generate_id("CAR", index),
            "conta_id": generate_id(
                "CTA",
                random.randint(1, 1225)
            ),
            "tipo_cartao": random.choice(card_types),
            "bandeira": random.choice(card_brands),
            "limite_cartao": str(round(random.uniform(500, 30000), 2)),
            "status_cartao": "ATIVO",
            "data_emissao": "2026-07-22",
            "data_atualizacao": "2026-07-22 13:00:00",
            "operacao_cdc": "I",
            "cartao_virtual": random.choice(["SIM", "NAO"])
        }
    )

cartoes_evolucao_schema = create_string_dataframe(
    evolved_card_rows,
    card_columns_v2
)

cartoes_batch_003 = union_dataframes(
    [
        cartoes_late,
        cartoes_duplicates,
        cartoes_invalidos,
        cartoes_evolucao_schema
    ]
)

print(f"Cartões Batch 003: {cartoes_batch_003.count()}")

# COMMAND ----------

# DBTITLE 1,Validação Transações
transaction_columns_v2 = transacoes_batch_001.columns + [
    "canal_transacao"
]

# 20 transações que ocorreram anteriormente, mas chegaram agora
transacoes_late = (
    to_string_dataframe(transacoes_batch_001)
    .orderBy("transacao_id")
    .limit(20)
    .withColumn("data_transacao", F.lit("2026-06-01 10:00:00"))
    .withColumn("data_ingestao_origem", F.lit("2026-07-22 08:00:00"))
    .withColumn("canal_transacao", F.lit(None).cast("string"))
)

transacoes_duplicate_source = (
    to_string_dataframe(transacoes_batch_002)
    .orderBy("transacao_id")
    .limit(10)
    .withColumn("canal_transacao", F.lit(None).cast("string"))
)

transacoes_duplicates = transacoes_duplicate_source.unionByName(
    transacoes_duplicate_source
)

invalid_transaction_rows = []

for index in range(1, 31):
    row = {
        "transacao_id": f"TRX_INV_{index:04d}",
        "conta_id": "CTA_000001",
        "cartao_id": "CAR_000001",
        "tipo_transacao": "PIX",
        "valor": "100.00",
        "status_transacao": "APROVADA",
        "data_transacao": "2026-07-22 10:00:00",
        "estabelecimento_id": "EST_000001",
        "data_ingestao_origem": "2026-07-22 10:05:00",
        "canal_transacao": None
    }

    issue = index % 7

    if issue == 0:
        row["transacao_id"] = None
    elif issue == 1:
        row["conta_id"] = "CTA_INEXISTENTE"
    elif issue == 2:
        row["cartao_id"] = "CAR_INEXISTENTE"
    elif issue == 3:
        row["valor"] = "VALOR_INVALIDO"
    elif issue == 4:
        row["tipo_transacao"] = "TIPO_INVALIDO"
    elif issue == 5:
        row["status_transacao"] = "STATUS_INVALIDO"
    else:
        row["data_transacao"] = "DATA_INVALIDA"

    invalid_transaction_rows.append(row)

transacoes_invalidas = create_string_dataframe(
    invalid_transaction_rows,
    transaction_columns_v2
)

normal_transaction_rows = []

for index in range(17001, 17301):
    transaction_date = generate_timestamp(
        datetime(2026, 7, 22, 0, 0),
        datetime(2026, 7, 22, 23, 59)
    )

    normal_transaction_rows.append(
        {
            "transacao_id": generate_id("TRX", index),
            "conta_id": generate_id(
                "CTA",
                random.randint(1, 1225)
            ),
            "cartao_id": (
                generate_id("CAR", random.randint(1, 919))
                if random.random() > 0.35
                else None
            ),
            "tipo_transacao": random.choice(transaction_types),
            "valor": str(round(random.uniform(5, 20000), 2)),
            "status_transacao": random.choice(transaction_statuses),
            "data_transacao": str(transaction_date),
            "estabelecimento_id": generate_id(
                "EST",
                random.randint(1, 300)
            ),
            "data_ingestao_origem": str(
                transaction_date
                + timedelta(minutes=random.randint(1, 180))
            ),
            "canal_transacao": random.choice(
                ["APP", "INTERNET_BANKING", "ATM", "AGENCIA"]
            )
        }
    )

transacoes_evolucao_schema = create_string_dataframe(
    normal_transaction_rows,
    transaction_columns_v2
)

transacoes_batch_003 = union_dataframes(
    [
        transacoes_late,
        transacoes_duplicates,
        transacoes_invalidas,
        transacoes_evolucao_schema
    ]
)

print(f"Transações Batch 003: {transacoes_batch_003.count()}")

# COMMAND ----------

# DBTITLE 1,Validação Eventos de Risco
risk_columns_v2 = eventos_risco_batch_001.columns + [
    "modelo_risco_versao"
]

risk_late = (
    to_string_dataframe(eventos_risco_batch_001)
    .orderBy("evento_risco_id")
    .limit(5)
    .withColumn("data_evento", F.lit("2026-06-05 11:00:00"))
    .withColumn("modelo_risco_versao", F.lit(None).cast("string"))
)

risk_duplicate_source = (
    to_string_dataframe(eventos_risco_batch_002)
    .orderBy("evento_risco_id")
    .limit(2)
    .withColumn("modelo_risco_versao", F.lit(None).cast("string"))
)

risk_duplicates = risk_duplicate_source.unionByName(
    risk_duplicate_source
)

invalid_risk_rows = [
    {
        "evento_risco_id": None,
        "transacao_id": "TRX_000001",
        "tipo_evento": "POSSIVEL_FRAUDE",
        "score_risco": "0.90",
        "nivel_risco": "ALTO",
        "data_evento": "2026-07-22 10:00:00"
    },
    {
        "evento_risco_id": "RSK_INV_002",
        "transacao_id": "TRX_INEXISTENTE",
        "tipo_evento": "POSSIVEL_FRAUDE",
        "score_risco": "0.90",
        "nivel_risco": "ALTO",
        "data_evento": "2026-07-22 10:00:00"
    },
    {
        "evento_risco_id": "RSK_INV_003",
        "transacao_id": "TRX_000001",
        "tipo_evento": "POSSIVEL_FRAUDE",
        "score_risco": "SCORE_INVALIDO",
        "nivel_risco": "ALTO",
        "data_evento": "2026-07-22 10:00:00"
    },
    {
        "evento_risco_id": "RSK_INV_004",
        "transacao_id": "TRX_000001",
        "tipo_evento": "TIPO_INVALIDO",
        "score_risco": "0.50",
        "nivel_risco": "ALTO",
        "data_evento": "2026-07-22 10:00:00"
    },
    {
        "evento_risco_id": "RSK_INV_005",
        "transacao_id": "TRX_000001",
        "tipo_evento": "POSSIVEL_FRAUDE",
        "score_risco": "0.70",
        "nivel_risco": "NIVEL_INVALIDO",
        "data_evento": "DATA_INVALIDA"
    }
]

eventos_risco_invalidos = create_string_dataframe(
    invalid_risk_rows,
    risk_columns_v2
)

evolved_risk_rows = []

for index in range(751, 761):
    evolved_risk_rows.append(
        {
            "evento_risco_id": generate_id("RSK", index),
            "transacao_id": generate_id(
                "TRX",
                random.randint(17001, 17300)
            ),
            "tipo_evento": random.choice(risk_types),
            "score_risco": str(round(random.uniform(0, 1), 4)),
            "nivel_risco": random.choice(
                ["BAIXO", "MEDIO", "ALTO", "CRITICO"]
            ),
            "data_evento": "2026-07-22 14:00:00",
            "modelo_risco_versao": "risk_model_v2"
        }
    )

eventos_risco_evolucao = create_string_dataframe(
    evolved_risk_rows,
    risk_columns_v2
)

eventos_risco_batch_003 = union_dataframes(
    [
        risk_late,
        risk_duplicates,
        eventos_risco_invalidos,
        eventos_risco_evolucao
    ]
)

chargeback_columns_v2 = estornos_batch_001.columns + [
    "origem_solicitacao"
]

estornos_late = (
    to_string_dataframe(estornos_batch_001)
    .orderBy("estorno_id")
    .limit(3)
    .withColumn("data_estorno", F.lit("2026-06-08 15:00:00"))
    .withColumn("origem_solicitacao", F.lit(None).cast("string"))
)

estornos_duplicate_source = (
    to_string_dataframe(estornos_batch_002)
    .orderBy("estorno_id")
    .limit(2)
    .withColumn("origem_solicitacao", F.lit(None).cast("string"))
)

estornos_duplicates = estornos_duplicate_source.unionByName(
    estornos_duplicate_source
)

invalid_chargeback_rows = [
    {
        "estorno_id": None,
        "transacao_id": "TRX_000001",
        "valor_estorno": "100.00",
        "motivo_estorno": "FRAUDE",
        "status_estorno": "SOLICITADO",
        "data_estorno": "2026-07-22 12:00:00"
    },
    {
        "estorno_id": "ESTO_INV_002",
        "transacao_id": "TRX_INEXISTENTE",
        "valor_estorno": "100.00",
        "motivo_estorno": "FRAUDE",
        "status_estorno": "SOLICITADO",
        "data_estorno": "2026-07-22 12:00:00"
    },
    {
        "estorno_id": "ESTO_INV_003",
        "transacao_id": "TRX_000001",
        "valor_estorno": "VALOR_INVALIDO",
        "motivo_estorno": "FRAUDE",
        "status_estorno": "SOLICITADO",
        "data_estorno": "2026-07-22 12:00:00"
    },
    {
        "estorno_id": "ESTO_INV_004",
        "transacao_id": "TRX_000001",
        "valor_estorno": "100.00",
        "motivo_estorno": "MOTIVO_INVALIDO",
        "status_estorno": "SOLICITADO",
        "data_estorno": "2026-07-22 12:00:00"
    },
    {
        "estorno_id": "ESTO_INV_005",
        "transacao_id": "TRX_000001",
        "valor_estorno": "100.00",
        "motivo_estorno": "FRAUDE",
        "status_estorno": "STATUS_INVALIDO",
        "data_estorno": "DATA_INVALIDA"
    }
]

estornos_invalidos = create_string_dataframe(
    invalid_chargeback_rows,
    chargeback_columns_v2
)

evolved_chargeback_rows = []

for index in range(311, 316):
    evolved_chargeback_rows.append(
        {
            "estorno_id": generate_id("ESTO", index),
            "transacao_id": generate_id(
                "TRX",
                random.randint(17001, 17300)
            ),
            "valor_estorno": str(round(random.uniform(5, 5000), 2)),
            "motivo_estorno": random.choice(chargeback_reasons),
            "status_estorno": "SOLICITADO",
            "data_estorno": "2026-07-22 15:00:00",
            "origem_solicitacao": random.choice(
                ["APP", "CENTRAL_ATENDIMENTO", "AGENCIA"]
            )
        }
    )

estornos_evolucao = create_string_dataframe(
    evolved_chargeback_rows,
    chargeback_columns_v2
)

estornos_batch_003 = union_dataframes(
    [
        estornos_late,
        estornos_duplicates,
        estornos_invalidos,
        estornos_evolucao
    ]
)

print(f"Eventos de risco Batch 003: {eventos_risco_batch_003.count()}")
print(f"Estornos Batch 003: {estornos_batch_003.count()}")

# COMMAND ----------

# DBTITLE 1,Validação Estornos
batch_003_dataframes = {
    "clientes_cdc": clientes_batch_003,
    "contas_cdc": contas_batch_003,
    "cartoes_cdc": cartoes_batch_003,
    "transacoes": transacoes_batch_003,
    "eventos_risco": eventos_risco_batch_003,
    "estornos": estornos_batch_003
}

batch_003_paths = {}

for entity_name, dataframe in batch_003_dataframes.items():
    batch_003_paths[entity_name] = write_landing_csv(
        dataframe=dataframe,
        entity_name=entity_name,
        batch_id=BATCH_003,
        processing_date=PROCESSING_DATE_BATCH_003
    )

# COMMAND ----------

# DBTITLE 1,Resumo dos Batches
batch_003_summary = [
    (
        entity_name,
        BATCH_003,
        dataframe.count(),
        len(dataframe.columns)
    )
    for entity_name, dataframe in batch_003_dataframes.items()
]

batch_003_summary_df = spark.createDataFrame(
    batch_003_summary,
    [
        "entidade",
        "batch_id",
        "quantidade_registros",
        "quantidade_colunas"
    ]
)

display(batch_003_summary_df.orderBy("entidade"))

# COMMAND ----------

# DBTITLE 1,Encerramento
for entity_name, dataframe in batch_003_dataframes.items():
    print(f"\n{entity_name}")
    print(dataframe.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado
# MAGIC
# MAGIC Foram gerados com sucesso três batches contendo dados sintéticos para todas as entidades do domínio bancário.
# MAGIC
# MAGIC Os arquivos simulam:
# MAGIC
# MAGIC - CDC
# MAGIC - Late Arrival
# MAGIC - Schema Evolution
# MAGIC - Dados inválidos
# MAGIC - Múltiplos lotes de ingestão
# MAGIC
# MAGIC Esses arquivos serão utilizados pelo notebook **02_load_landing** para iniciar o pipeline Lakehouse.