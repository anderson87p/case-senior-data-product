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
SOURCE_BATCH = "batch_002"
TARGET_BATCH = "batch_003"


def load_config() -> dict[str, Any]:
    """Carrega o arquivo de configuração do projeto."""

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {CONFIG_PATH}"
        )

    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def read_batch(
    dataset_name: str,
    batch_name: str,
) -> pd.DataFrame:
    """Lê um dataset de um batch anterior da camada Landing."""

    file_path = (
        LANDING_PATH
        / dataset_name
        / batch_name
        / f"{dataset_name}.csv"
    )

    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {file_path}"
        )

    return pd.read_csv(file_path)


def save_dataset(
    dataframe: pd.DataFrame,
    dataset_name: str,
    encoding: str,
) -> None:
    """Salva um dataset no batch_003."""

    output_path = (
        LANDING_PATH
        / dataset_name
        / TARGET_BATCH
    )

    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_path = output_path / f"{dataset_name}.csv"

    dataframe.to_csv(
        file_path,
        index=False,
        encoding=encoding,
    )

    print(
        f"[OK] {len(dataframe):>6} registros -> {file_path}"
    )


def generate_clientes_batch_003(
    faker: Faker,
    reference_date: datetime,
) -> pd.DataFrame:
    """
    Gera dados de clientes contendo:

    - atualizações válidas;
    - registros inválidos;
    - duplicidades;
    - evolução de schema.
    """

    batch_002 = read_batch(
        dataset_name="clientes_cdc",
        batch_name=SOURCE_BATCH,
    )

    valid_updates = batch_002.sample(
        n=15,
        random_state=601,
    ).copy()

    valid_updates["cdc_operation"] = "U"
    valid_updates["updated_at"] = reference_date.isoformat()
    valid_updates["cdc_event_at"] = reference_date.isoformat()
    valid_updates["source_batch"] = TARGET_BATCH

    invalid_records = valid_updates.head(5).copy()

    invalid_records.loc[
        invalid_records.index[0],
        "cpf",
    ] = None

    invalid_records.loc[
        invalid_records.index[1],
        "email",
    ] = "email_invalido"

    invalid_records.loc[
        invalid_records.index[2],
        "status_cliente",
    ] = "DESCONHECIDO"

    invalid_records.loc[
        invalid_records.index[3],
        "cliente_id",
    ] = -999

    invalid_records.loc[
        invalid_records.index[4],
        "data_nascimento",
    ] = "2100-01-01"

    duplicate_records = valid_updates.head(3).copy()

    schema_evolution = valid_updates.copy()

    schema_evolution["canal_preferencial"] = [
        random.choice(
            [
                "APP",
                "AGENCIA",
                "WHATSAPP",
                "INTERNET_BANKING",
            ]
        )
        for _ in range(len(schema_evolution))
    ]

    schema_evolution["consentimento_lgpd"] = [
        random.choice([True, False])
        for _ in range(len(schema_evolution))
    ]

    return pd.concat(
        [
            schema_evolution,
            invalid_records,
            duplicate_records,
        ],
        ignore_index=True,
    )


def generate_contas_batch_003(
    reference_date: datetime,
) -> pd.DataFrame:
    """
    Gera dados de contas contendo:

    - atualizações válidas;
    - registros inválidos;
    - duplicidades;
    - evolução de schema.
    """

    batch_002 = read_batch(
        dataset_name="contas_cdc",
        batch_name=SOURCE_BATCH,
    )

    valid_updates = batch_002.sample(
        n=20,
        random_state=602,
    ).copy()

    valid_updates["cdc_operation"] = "U"
    valid_updates["updated_at"] = reference_date.isoformat()
    valid_updates["cdc_event_at"] = reference_date.isoformat()
    valid_updates["source_batch"] = TARGET_BATCH

    invalid_records = valid_updates.head(6).copy()

    # Pandas 3.0 não permite inserir string diretamente
    # em uma coluna originalmente numérica.
    invalid_records["saldo"] = (
        invalid_records["saldo"].astype("object")
    )

    invalid_records.loc[
        invalid_records.index[0],
        "saldo",
    ] = "VALOR_INVALIDO"

    invalid_records.loc[
        invalid_records.index[1],
        "cliente_id",
    ] = 999999

    invalid_records.loc[
        invalid_records.index[2],
        "tipo_conta",
    ] = None

    invalid_records.loc[
        invalid_records.index[3],
        "status_conta",
    ] = "STATUS_INVALIDO"

    invalid_records.loc[
        invalid_records.index[4],
        "limite_credito",
    ] = -5000

    invalid_records.loc[
        invalid_records.index[5],
        "numero_conta",
    ] = None

    duplicate_records = valid_updates.head(4).copy()

    schema_evolution = valid_updates.copy()

    schema_evolution["pix_habilitado"] = [
        random.choice([True, False])
        for _ in range(len(schema_evolution))
    ]

    schema_evolution["categoria_conta"] = [
        random.choice(
            [
                "BASICA",
                "PREMIUM",
                "DIGITAL",
            ]
        )
        for _ in range(len(schema_evolution))
    ]

    return pd.concat(
        [
            schema_evolution,
            invalid_records,
            duplicate_records,
        ],
        ignore_index=True,
    )


def generate_cartoes_batch_003(
    reference_date: datetime,
) -> pd.DataFrame:
    """
    Gera dados de cartões contendo:

    - atualizações válidas;
    - registros inválidos;
    - duplicidades;
    - evolução de schema.
    """

    batch_002 = read_batch(
        dataset_name="cartoes_cdc",
        batch_name=SOURCE_BATCH,
    )

    valid_updates = batch_002.sample(
        n=15,
        random_state=603,
    ).copy()

    valid_updates["cdc_operation"] = "U"
    valid_updates["updated_at"] = reference_date.isoformat()
    valid_updates["cdc_event_at"] = reference_date.isoformat()
    valid_updates["source_batch"] = TARGET_BATCH

    invalid_records = valid_updates.head(5).copy()

    invalid_records.loc[
        invalid_records.index[0],
        "conta_id",
    ] = 999999

    invalid_records.loc[
        invalid_records.index[1],
        "limite_total",
    ] = -1000

    invalid_records.loc[
        invalid_records.index[2],
        "status_cartao",
    ] = "INVALIDO"

    invalid_records.loc[
        invalid_records.index[3],
        "numero_cartao_mascarado",
    ] = None

    invalid_records.loc[
        invalid_records.index[4],
        "tipo_cartao",
    ] = "OUTRO"

    duplicate_records = valid_updates.head(3).copy()

    schema_evolution = valid_updates.copy()

    schema_evolution["cartao_virtual"] = [
        random.choice([True, False])
        for _ in range(len(schema_evolution))
    ]

    schema_evolution["tecnologia_contactless"] = [
        random.choice([True, False])
        for _ in range(len(schema_evolution))
    ]

    return pd.concat(
        [
            schema_evolution,
            invalid_records,
            duplicate_records,
        ],
        ignore_index=True,
    )


def generate_transacoes_batch_003(
    faker: Faker,
    reference_date: datetime,
) -> pd.DataFrame:
    """
    Gera transações contendo:

    - dados atrasados;
    - registros inválidos;
    - duplicidades;
    - novos registros;
    - evolução de schema.
    """

    batch_002 = read_batch(
        dataset_name="transacoes",
        batch_name=SOURCE_BATCH,
    )

    late_arriving = batch_002.sample(
        n=250,
        random_state=604,
    ).copy()

    for index in late_arriving.index:
        original_time = datetime.fromisoformat(
            str(
                late_arriving.loc[
                    index,
                    "data_hora_transacao",
                ]
            )
        )

        late_arriving.loc[
            index,
            "data_hora_transacao",
        ] = (
            original_time
            - timedelta(
                days=random.randint(30, 120)
            )
        ).isoformat()

    late_arriving["ingestion_at"] = (
        reference_date.isoformat()
    )

    late_arriving["source_batch"] = TARGET_BATCH
    late_arriving["late_arriving_flag"] = True

    invalid_records = late_arriving.head(10).copy()

    # Necessário no pandas 3.0 para inserir "ABC"
    # em uma coluna originalmente numérica.
    invalid_records["valor"] = (
        invalid_records["valor"].astype("object")
    )

    invalid_records.loc[
        invalid_records.index[0],
        "valor",
    ] = -100

    invalid_records.loc[
        invalid_records.index[1],
        "valor",
    ] = "ABC"

    invalid_records.loc[
        invalid_records.index[2],
        "conta_id",
    ] = 999999

    invalid_records.loc[
        invalid_records.index[3],
        "tipo_transacao",
    ] = None

    invalid_records.loc[
        invalid_records.index[4],
        "moeda",
    ] = "XXX"

    invalid_records.loc[
        invalid_records.index[5],
        "status_transacao",
    ] = "INVALIDO"

    invalid_records.loc[
        invalid_records.index[6],
        "data_hora_transacao",
    ] = "DATA_INVALIDA"

    invalid_records.loc[
        invalid_records.index[7],
        "transacao_id",
    ] = None

    invalid_records.loc[
        invalid_records.index[8],
        "cidade",
    ] = None

    invalid_records.loc[
        invalid_records.index[9],
        "data_processamento",
    ] = None

    duplicate_records = late_arriving.head(20).copy()

    new_records: list[dict[str, Any]] = []

    for _ in range(100):
        occurred_at = (
            reference_date
            - timedelta(
                days=random.randint(45, 180),
                minutes=random.randint(1, 1440),
            )
        )

        new_records.append(
            {
                "transacao_id": str(uuid4()),
                "conta_id": random.randint(1, 1220),
                "cartao_id": None,
                "tipo_transacao": random.choice(
                    [
                        "PIX",
                        "TED",
                        "BOLETO",
                        "SAQUE",
                    ]
                ),
                "valor": round(
                    random.uniform(10, 10000),
                    2,
                ),
                "moeda": "BRL",
                "descricao": faker.sentence(
                    nb_words=5
                ),
                "estabelecimento": None,
                "cidade": faker.city(),
                "status_transacao": "APROVADA",
                "data_hora_transacao": (
                    occurred_at.isoformat()
                ),
                "data_processamento": (
                    occurred_at
                    + timedelta(
                        minutes=random.randint(1, 180)
                    )
                ).isoformat(),
                "ingestion_at": (
                    reference_date.isoformat()
                ),
                "source_batch": TARGET_BATCH,
                "late_arriving_flag": True,
                "device_id": faker.uuid4(),
                "ip_origem": faker.ipv4_public(),
            }
        )

    new_late_arriving = pd.DataFrame(new_records)

    return pd.concat(
        [
            late_arriving,
            invalid_records,
            duplicate_records,
            new_late_arriving,
        ],
        ignore_index=True,
    )


def generate_eventos_risco_batch_003(
    faker: Faker,
    reference_date: datetime,
) -> pd.DataFrame:
    """
    Gera eventos de risco com registros inválidos
    e evolução de schema.
    """

    del faker
    del reference_date

    batch_002 = read_batch(
        dataset_name="eventos_risco",
        batch_name=SOURCE_BATCH,
    )

    valid_records = batch_002.sample(
        n=40,
        random_state=605,
    ).copy()

    valid_records["source_batch"] = TARGET_BATCH

    invalid_records = valid_records.head(5).copy()

    invalid_records.loc[
        invalid_records.index[0],
        "score_risco",
    ] = 150

    invalid_records.loc[
        invalid_records.index[1],
        "nivel_risco",
    ] = "INVALIDO"

    invalid_records.loc[
        invalid_records.index[2],
        "transacao_id",
    ] = "INEXISTENTE"

    invalid_records.loc[
        invalid_records.index[3],
        "decisao",
    ] = None

    invalid_records.loc[
        invalid_records.index[4],
        "data_hora_evento",
    ] = "DATA_INVALIDA"

    schema_evolution = valid_records.copy()

    schema_evolution["modelo_risco"] = "risk_model_v2"
    schema_evolution["versao_modelo"] = "2.0"

    return pd.concat(
        [
            schema_evolution,
            invalid_records,
        ],
        ignore_index=True,
    )


def generate_estornos_batch_003(
    reference_date: datetime,
) -> pd.DataFrame:
    """
    Gera estornos com registros inválidos
    e evolução de schema.
    """

    del reference_date

    batch_002 = read_batch(
        dataset_name="estornos",
        batch_name=SOURCE_BATCH,
    )

    valid_records = batch_002.sample(
        n=20,
        random_state=606,
    ).copy()

    valid_records["source_batch"] = TARGET_BATCH

    invalid_records = valid_records.head(5).copy()

    invalid_records.loc[
        invalid_records.index[0],
        "valor_estorno",
    ] = -500

    invalid_records.loc[
        invalid_records.index[1],
        "transacao_id",
    ] = "INEXISTENTE"

    invalid_records.loc[
        invalid_records.index[2],
        "status_estorno",
    ] = "INVALIDO"

    invalid_records.loc[
        invalid_records.index[3],
        "data_hora_estorno",
    ] = "DATA_INVALIDA"

    invalid_records.loc[
        invalid_records.index[4],
        "motivo_estorno",
    ] = None

    schema_evolution = valid_records.copy()

    schema_evolution["origem_solicitacao"] = [
        random.choice(
            [
                "APP",
                "AGENCIA",
                "CALL_CENTER",
            ]
        )
        for _ in range(len(schema_evolution))
    ]

    return pd.concat(
        [
            schema_evolution,
            invalid_records,
        ],
        ignore_index=True,
    )


def main() -> None:
    """Executa a geração completa do batch_003."""

    config = load_config()

    seed = int(config["project"]["seed"]) + 2
    locale = config["project"]["locale"]
    encoding = config["output"]["encoding"]

    random.seed(seed)
    Faker.seed(seed)

    faker = Faker(locale)

    reference_date = datetime.now().replace(
        microsecond=0
    )

    clientes = generate_clientes_batch_003(
        faker=faker,
        reference_date=reference_date,
    )

    contas = generate_contas_batch_003(
        reference_date=reference_date,
    )

    cartoes = generate_cartoes_batch_003(
        reference_date=reference_date,
    )

    transacoes = generate_transacoes_batch_003(
        faker=faker,
        reference_date=reference_date,
    )

    eventos_risco = generate_eventos_risco_batch_003(
        faker=faker,
        reference_date=reference_date,
    )

    estornos = generate_estornos_batch_003(
        reference_date=reference_date,
    )

    save_dataset(
        dataframe=clientes,
        dataset_name="clientes_cdc",
        encoding=encoding,
    )

    save_dataset(
        dataframe=contas,
        dataset_name="contas_cdc",
        encoding=encoding,
    )

    save_dataset(
        dataframe=cartoes,
        dataset_name="cartoes_cdc",
        encoding=encoding,
    )

    save_dataset(
        dataframe=transacoes,
        dataset_name="transacoes",
        encoding=encoding,
    )

    save_dataset(
        dataframe=eventos_risco,
        dataset_name="eventos_risco",
        encoding=encoding,
    )

    save_dataset(
        dataframe=estornos,
        dataset_name="estornos",
        encoding=encoding,
    )

    print("\nBatch_003 gerado com sucesso.")


if __name__ == "__main__":
    main()