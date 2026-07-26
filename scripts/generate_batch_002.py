from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import yaml
from faker import Faker


CONFIG_PATH = Path("config/data_generation.yaml")
LANDING_PATH = Path("data/landing")
SOURCE_BATCH = "batch_001"
TARGET_BATCH = "batch_002"


def load_config() -> dict[str, Any]:
    """Carrega as configurações gerais do projeto."""

    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def read_source(dataset_name: str) -> pd.DataFrame:
    """Lê um dataset produzido no batch inicial."""

    file_path = (
        LANDING_PATH
        / dataset_name
        / SOURCE_BATCH
        / f"{dataset_name}.csv"
    )

    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo do batch inicial não encontrado: {file_path}"
        )

    return pd.read_csv(file_path)


def save_dataset(
    dataframe: pd.DataFrame,
    dataset_name: str,
    encoding: str,
) -> None:
    """Salva um dataset no batch_002."""

    output_path = LANDING_PATH / dataset_name / TARGET_BATCH
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / f"{dataset_name}.csv"

    dataframe.to_csv(
        file_path,
        index=False,
        encoding=encoding,
    )

    print(
        f"[OK] {len(dataframe):>6} registros -> {file_path}"
    )


def add_cdc_metadata(
    dataframe: pd.DataFrame,
    operation: str,
    event_time: datetime,
    sequence_start: int,
) -> pd.DataFrame:
    """Adiciona metadados de controle CDC."""

    result = dataframe.copy()

    result["cdc_operation"] = operation
    result["cdc_sequence"] = range(
        sequence_start,
        sequence_start + len(result),
    )
    result["cdc_event_at"] = event_time.isoformat()
    result["source_batch"] = TARGET_BATCH

    return result


def generate_clientes_cdc(
    clientes: pd.DataFrame,
    faker: Faker,
    reference_date: datetime,
) -> pd.DataFrame:
    """Gera INSERT, UPDATE e DELETE de clientes."""

    updated = clientes.sample(
        n=60,
        random_state=101,
    ).copy()

    updated["email"] = [
        faker.unique.email()
        for _ in range(len(updated))
    ]

    updated["telefone"] = [
        faker.phone_number()
        for _ in range(len(updated))
    ]

    updated["segmento"] = [
        random.choice(
            [
                "VAREJO",
                "ALTA_RENDA",
                "EMPRESARIAL",
                "COOPERADO",
            ]
        )
        for _ in range(len(updated))
    ]

    updated["updated_at"] = reference_date.isoformat()

    updated = add_cdc_metadata(
        dataframe=updated,
        operation="U",
        event_time=reference_date,
        sequence_start=1,
    )

    deleted = clientes.sample(
        n=10,
        random_state=102,
    ).copy()

    deleted["updated_at"] = reference_date.isoformat()

    deleted = add_cdc_metadata(
        dataframe=deleted,
        operation="D",
        event_time=reference_date,
        sequence_start=1001,
    )

    max_cliente_id = int(clientes["cliente_id"].max())

    inserted_records: list[dict[str, Any]] = []

    for offset in range(1, 21):
        inserted_records.append(
            {
                "cliente_id": max_cliente_id + offset,
                "cpf": faker.unique.cpf(),
                "nome": faker.name(),
                "data_nascimento": faker.date_of_birth(
                    minimum_age=18,
                    maximum_age=80,
                ).isoformat(),
                "email": faker.unique.email(),
                "telefone": faker.phone_number(),
                "cidade": faker.city(),
                "estado": faker.estado_sigla(),
                "segmento": random.choice(
                    [
                        "VAREJO",
                        "ALTA_RENDA",
                        "EMPRESARIAL",
                        "COOPERADO",
                    ]
                ),
                "status_cliente": "ATIVO",
                "data_cadastro": reference_date.isoformat(),
                "updated_at": reference_date.isoformat(),
            }
        )

    inserted = pd.DataFrame(inserted_records)

    inserted = add_cdc_metadata(
        dataframe=inserted,
        operation="I",
        event_time=reference_date,
        sequence_start=2001,
    )

    return pd.concat(
        [inserted, updated, deleted],
        ignore_index=True,
    )


def generate_contas_cdc(
    contas: pd.DataFrame,
    clientes_cdc: pd.DataFrame,
    reference_date: datetime,
) -> pd.DataFrame:
    """Gera INSERT, UPDATE e DELETE de contas."""

    updated = contas.sample(
        n=100,
        random_state=201,
    ).copy()

    updated["saldo"] = (
        updated["saldo"]
        + pd.Series(
            [
                round(random.uniform(-5000, 15000), 2)
                for _ in range(len(updated))
            ],
            index=updated.index,
        )
    ).round(2)

    updated["status_conta"] = [
        random.choices(
            ["ATIVA", "INATIVA", "BLOQUEADA"],
            weights=[82, 8, 10],
            k=1,
        )[0]
        for _ in range(len(updated))
    ]

    updated["updated_at"] = reference_date.isoformat()

    updated = add_cdc_metadata(
        dataframe=updated,
        operation="U",
        event_time=reference_date,
        sequence_start=3001,
    )

    deleted = contas.sample(
        n=10,
        random_state=202,
    ).copy()

    deleted["status_conta"] = "ENCERRADA"
    deleted["updated_at"] = reference_date.isoformat()

    deleted = add_cdc_metadata(
        dataframe=deleted,
        operation="D",
        event_time=reference_date,
        sequence_start=4001,
    )

    inserted_client_ids = clientes_cdc.loc[
        clientes_cdc["cdc_operation"] == "I",
        "cliente_id",
    ].tolist()

    max_conta_id = int(contas["conta_id"].max())

    inserted_records: list[dict[str, Any]] = []

    for offset in range(1, 21):
        inserted_records.append(
            {
                "conta_id": max_conta_id + offset,
                "cliente_id": random.choice(inserted_client_ids),
                "agencia": f"{random.randint(1, 9999):04d}",
                "numero_conta": f"{random.randint(1, 99999999):08d}",
                "tipo_conta": random.choice(
                    ["CORRENTE", "POUPANCA", "PAGAMENTO"]
                ),
                "saldo": round(
                    random.uniform(100, 50000),
                    2,
                ),
                "limite_credito": round(
                    random.uniform(0, 30000),
                    2,
                ),
                "status_conta": "ATIVA",
                "data_abertura": reference_date.date().isoformat(),
                "updated_at": reference_date.isoformat(),
            }
        )

    inserted = pd.DataFrame(inserted_records)

    inserted = add_cdc_metadata(
        dataframe=inserted,
        operation="I",
        event_time=reference_date,
        sequence_start=5001,
    )

    return pd.concat(
        [inserted, updated, deleted],
        ignore_index=True,
    )


def generate_cartoes_cdc(
    cartoes: pd.DataFrame,
    contas_cdc: pd.DataFrame,
    reference_date: datetime,
) -> pd.DataFrame:
    """Gera INSERT, UPDATE e DELETE de cartões."""

    updated = cartoes.sample(
        n=60,
        random_state=301,
    ).copy()

    updated["limite_disponivel"] = [
        round(random.uniform(0, 40000), 2)
        for _ in range(len(updated))
    ]

    updated["status_cartao"] = [
        random.choices(
            ["ATIVO", "BLOQUEADO"],
            weights=[80, 20],
            k=1,
        )[0]
        for _ in range(len(updated))
    ]

    updated["updated_at"] = reference_date.isoformat()

    updated = add_cdc_metadata(
        dataframe=updated,
        operation="U",
        event_time=reference_date,
        sequence_start=6001,
    )

    deleted = cartoes.sample(
        n=8,
        random_state=302,
    ).copy()

    deleted["status_cartao"] = "CANCELADO"
    deleted["updated_at"] = reference_date.isoformat()

    deleted = add_cdc_metadata(
        dataframe=deleted,
        operation="D",
        event_time=reference_date,
        sequence_start=7001,
    )

    inserted_account_ids = contas_cdc.loc[
        contas_cdc["cdc_operation"] == "I",
        "conta_id",
    ].tolist()

    max_cartao_id = int(cartoes["cartao_id"].max())

    inserted_records: list[dict[str, Any]] = []

    for offset in range(1, 16):
        limit_total = round(
            random.uniform(1000, 50000),
            2,
        )

        inserted_records.append(
            {
                "cartao_id": max_cartao_id + offset,
                "conta_id": random.choice(inserted_account_ids),
                "numero_cartao_mascarado": (
                    f"**** **** **** {random.randint(0, 9999):04d}"
                ),
                "bandeira": random.choice(
                    ["VISA", "MASTERCARD", "ELO"]
                ),
                "tipo_cartao": random.choice(
                    ["CREDITO", "DEBITO", "MULTIPLO"]
                ),
                "limite_total": limit_total,
                "limite_disponivel": round(
                    random.uniform(0, limit_total),
                    2,
                ),
                "status_cartao": "ATIVO",
                "data_emissao": reference_date.date().isoformat(),
                "updated_at": reference_date.isoformat(),
            }
        )

    inserted = pd.DataFrame(inserted_records)

    inserted = add_cdc_metadata(
        dataframe=inserted,
        operation="I",
        event_time=reference_date,
        sequence_start=8001,
    )

    return pd.concat(
        [inserted, updated, deleted],
        ignore_index=True,
    )


def generate_new_transacoes(
    faker: Faker,
    contas: pd.DataFrame,
    contas_cdc: pd.DataFrame,
    cartoes: pd.DataFrame,
    cartoes_cdc: pd.DataFrame,
    reference_date: datetime,
    quantity: int = 2000,
) -> pd.DataFrame:
    """Gera novas transações append-only."""

    inserted_accounts = contas_cdc[
        contas_cdc["cdc_operation"] == "I"
    ].drop(
        columns=[
            "cdc_operation",
            "cdc_sequence",
            "cdc_event_at",
            "source_batch",
        ]
    )

    inserted_cards = cartoes_cdc[
        cartoes_cdc["cdc_operation"] == "I"
    ].drop(
        columns=[
            "cdc_operation",
            "cdc_sequence",
            "cdc_event_at",
            "source_batch",
        ]
    )

    all_accounts = pd.concat(
        [contas, inserted_accounts],
        ignore_index=True,
    )

    all_cards = pd.concat(
        [cartoes, inserted_cards],
        ignore_index=True,
    )

    account_ids = all_accounts["conta_id"].tolist()

    cards_by_account = (
        all_cards.groupby("conta_id")["cartao_id"]
        .apply(list)
        .to_dict()
    )

    records: list[dict[str, Any]] = []

    for _ in range(quantity):
        conta_id = random.choice(account_ids)
        transaction_type = random.choice(
            ["PIX", "TED", "CARTAO", "BOLETO", "SAQUE"]
        )

        available_cards = cards_by_account.get(
            conta_id,
            [],
        )

        occurred_at = faker.date_time_between(
            start_date=reference_date - timedelta(days=7),
            end_date=reference_date,
        )

        records.append(
            {
                "transacao_id": str(uuid4()),
                "conta_id": conta_id,
                "cartao_id": (
                    random.choice(available_cards)
                    if transaction_type == "CARTAO"
                    and available_cards
                    else None
                ),
                "tipo_transacao": transaction_type,
                "valor": round(
                    random.uniform(5, 20000),
                    2,
                ),
                "moeda": "BRL",
                "descricao": faker.sentence(nb_words=5),
                "estabelecimento": (
                    faker.company()
                    if transaction_type == "CARTAO"
                    else None
                ),
                "cidade": faker.city(),
                "status_transacao": random.choices(
                    ["APROVADA", "NEGADA", "PENDENTE"],
                    weights=[91, 7, 2],
                    k=1,
                )[0],
                "data_hora_transacao": occurred_at.isoformat(),
                "data_processamento": (
                    occurred_at
                    + timedelta(minutes=random.randint(0, 180))
                ).isoformat(),
                "ingestion_at": reference_date.isoformat(),
                "source_batch": TARGET_BATCH,
            }
        )

    return pd.DataFrame(records)


def generate_new_eventos_risco(
    faker: Faker,
    transacoes: pd.DataFrame,
    quantity: int = 120,
) -> pd.DataFrame:
    """Gera eventos de risco das novas transações."""

    sampled = transacoes.sample(
        n=min(quantity, len(transacoes)),
        random_state=401,
    )

    records: list[dict[str, Any]] = []

    for _, transaction in sampled.iterrows():
        score = round(
            random.uniform(0, 100),
            2,
        )

        transaction_time = datetime.fromisoformat(
            transaction["data_hora_transacao"]
        )

        records.append(
            {
                "evento_risco_id": str(uuid4()),
                "transacao_id": transaction["transacao_id"],
                "score_risco": score,
                "nivel_risco": (
                    "ALTO"
                    if score >= 75
                    else "MEDIO"
                    if score >= 40
                    else "BAIXO"
                ),
                "regra_acionada": random.choice(
                    [
                        "VALOR_ATIPICO",
                        "LOCALIZACAO_DIVERGENTE",
                        "MULTIPLAS_TENTATIVAS",
                        "DISPOSITIVO_DESCONHECIDO",
                    ]
                ),
                "decisao": random.choice(
                    ["APROVAR", "REVISAR", "BLOQUEAR"]
                ),
                "data_hora_evento": (
                    transaction_time
                    + timedelta(seconds=random.randint(1, 120))
                ).isoformat(),
                "detalhes": faker.sentence(nb_words=8),
                "source_batch": TARGET_BATCH,
            }
        )

    return pd.DataFrame(records)


def generate_new_estornos(
    faker: Faker,
    transacoes: pd.DataFrame,
    reference_date: datetime,
    quantity: int = 40,
) -> pd.DataFrame:
    """Gera estornos para novas transações aprovadas."""

    eligible = transacoes[
        transacoes["status_transacao"] == "APROVADA"
    ]

    sampled = eligible.sample(
        n=min(quantity, len(eligible)),
        random_state=501,
    )

    records: list[dict[str, Any]] = []

    for _, transaction in sampled.iterrows():
        transaction_time = datetime.fromisoformat(
            transaction["data_hora_transacao"]
        )

        reversal_time = min(
            transaction_time
            + timedelta(
                days=random.randint(1, 5),
                minutes=random.randint(1, 1440),
            ),
            reference_date,
        )

        records.append(
            {
                "estorno_id": str(uuid4()),
                "transacao_id": transaction["transacao_id"],
                "valor_estorno": transaction["valor"],
                "motivo_estorno": random.choice(
                    [
                        "COMPRA_NAO_RECONHECIDA",
                        "CANCELAMENTO",
                        "DUPLICIDADE",
                        "PRODUTO_NAO_ENTREGUE",
                    ]
                ),
                "status_estorno": random.choice(
                    ["SOLICITADO", "PROCESSADO", "NEGADO"]
                ),
                "data_hora_estorno": reversal_time.isoformat(),
                "observacao": faker.sentence(nb_words=6),
                "source_batch": TARGET_BATCH,
            }
        )

    return pd.DataFrame(records)


def main() -> None:
    config = load_config()

    seed = int(config["project"]["seed"]) + 1
    locale = config["project"]["locale"]
    encoding = config["output"]["encoding"]

    random.seed(seed)
    Faker.seed(seed)

    faker = Faker(locale)

    reference_date = datetime.now().replace(microsecond=0)

    clientes = read_source("clientes_cdc")
    contas = read_source("contas_cdc")
    cartoes = read_source("cartoes_cdc")

    clientes_cdc = generate_clientes_cdc(
        clientes=clientes,
        faker=faker,
        reference_date=reference_date,
    )

    contas_cdc = generate_contas_cdc(
        contas=contas,
        clientes_cdc=clientes_cdc,
        reference_date=reference_date,
    )

    cartoes_cdc = generate_cartoes_cdc(
        cartoes=cartoes,
        contas_cdc=contas_cdc,
        reference_date=reference_date,
    )

    transacoes = generate_new_transacoes(
        faker=faker,
        contas=contas,
        contas_cdc=contas_cdc,
        cartoes=cartoes,
        cartoes_cdc=cartoes_cdc,
        reference_date=reference_date,
    )

    eventos_risco = generate_new_eventos_risco(
        faker=faker,
        transacoes=transacoes,
    )

    estornos = generate_new_estornos(
        faker=faker,
        transacoes=transacoes,
        reference_date=reference_date,
    )

    save_dataset(
        clientes_cdc,
        "clientes_cdc",
        encoding,
    )

    save_dataset(
        contas_cdc,
        "contas_cdc",
        encoding,
    )

    save_dataset(
        cartoes_cdc,
        "cartoes_cdc",
        encoding,
    )

    save_dataset(
        transacoes,
        "transacoes",
        encoding,
    )

    save_dataset(
        eventos_risco,
        "eventos_risco",
        encoding,
    )

    save_dataset(
        estornos,
        "estornos",
        encoding,
    )

    print("\nBatch_002 gerado com sucesso.")


if __name__ == "__main__":
    main()