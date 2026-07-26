from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import yaml
from faker import Faker


def load_config(config_path: Path) -> dict[str, Any]:
    """Carrega as configurações de geração dos dados."""

    if not config_path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def save_dataframe(
    dataframe: pd.DataFrame,
    output_path: Path,
    file_name: str,
    encoding: str,
) -> None:
    """Salva um DataFrame em CSV."""

    output_path.mkdir(parents=True, exist_ok=True)

    destination = output_path / file_name

    dataframe.to_csv(
        destination,
        index=False,
        encoding=encoding,
    )

    print(f"[OK] {len(dataframe):>6} registros -> {destination}")


def generate_clientes(
    faker: Faker,
    quantity: int,
    reference_date: datetime,
) -> pd.DataFrame:
    """Gera dados sintéticos de clientes."""

    records: list[dict[str, Any]] = []

    for index in range(1, quantity + 1):
        birth_date = faker.date_of_birth(minimum_age=18, maximum_age=85)
        created_at = faker.date_time_between(
            start_date="-8y",
            end_date=reference_date,
        )

        records.append(
            {
                "cliente_id": index,
                "cpf": faker.unique.cpf(),
                "nome": faker.name(),
                "data_nascimento": birth_date.isoformat(),
                "email": faker.unique.email(),
                "telefone": faker.phone_number(),
                "cidade": faker.city(),
                "estado": faker.estado_sigla(),
                "segmento": random.choice(
                    ["VAREJO", "ALTA_RENDA", "EMPRESARIAL", "COOPERADO"]
                ),
                "status_cliente": random.choices(
                    ["ATIVO", "INATIVO", "BLOQUEADO"],
                    weights=[90, 7, 3],
                    k=1,
                )[0],
                "data_cadastro": created_at.isoformat(),
                "updated_at": created_at.isoformat(),
            }
        )

    return pd.DataFrame(records)


def generate_contas(
    faker: Faker,
    clientes: pd.DataFrame,
    quantity: int,
    reference_date: datetime,
) -> pd.DataFrame:
    """Gera contas relacionadas aos clientes."""

    cliente_ids = clientes["cliente_id"].tolist()
    records: list[dict[str, Any]] = []

    for index in range(1, quantity + 1):
        opened_at = faker.date_time_between(
            start_date="-7y",
            end_date=reference_date,
        )

        records.append(
            {
                "conta_id": index,
                "cliente_id": random.choice(cliente_ids),
                "agencia": f"{random.randint(1, 9999):04d}",
                "numero_conta": f"{random.randint(1, 99999999):08d}",
                "tipo_conta": random.choice(
                    ["CORRENTE", "POUPANCA", "PAGAMENTO"]
                ),
                "saldo": round(random.uniform(-2500, 150000), 2),
                "limite_credito": round(random.uniform(0, 50000), 2),
                "status_conta": random.choices(
                    ["ATIVA", "INATIVA", "BLOQUEADA", "ENCERRADA"],
                    weights=[88, 5, 4, 3],
                    k=1,
                )[0],
                "data_abertura": opened_at.date().isoformat(),
                "updated_at": opened_at.isoformat(),
            }
        )

    return pd.DataFrame(records)


def generate_cartoes(
    faker: Faker,
    contas: pd.DataFrame,
    quantity: int,
    reference_date: datetime,
) -> pd.DataFrame:
    """Gera cartões relacionados às contas."""

    conta_ids = contas["conta_id"].tolist()
    records: list[dict[str, Any]] = []

    for index in range(1, quantity + 1):
        issued_at = faker.date_time_between(
            start_date="-5y",
            end_date=reference_date,
        )

        records.append(
            {
                "cartao_id": index,
                "conta_id": random.choice(conta_ids),
                "numero_cartao_mascarado": f"**** **** **** {random.randint(0, 9999):04d}",
                "bandeira": random.choice(
                    ["VISA", "MASTERCARD", "ELO"]
                ),
                "tipo_cartao": random.choice(
                    ["CREDITO", "DEBITO", "MULTIPLO"]
                ),
                "limite_total": round(random.uniform(1000, 60000), 2),
                "limite_disponivel": round(random.uniform(0, 40000), 2),
                "status_cartao": random.choices(
                    ["ATIVO", "BLOQUEADO", "CANCELADO"],
                    weights=[91, 6, 3],
                    k=1,
                )[0],
                "data_emissao": issued_at.date().isoformat(),
                "updated_at": issued_at.isoformat(),
            }
        )

    return pd.DataFrame(records)


def generate_transacoes(
    faker: Faker,
    contas: pd.DataFrame,
    cartoes: pd.DataFrame,
    quantity: int,
    reference_date: datetime,
) -> pd.DataFrame:
    """Gera transações financeiras."""

    conta_ids = contas["conta_id"].tolist()
    card_by_account = (
        cartoes.groupby("conta_id")["cartao_id"]
        .apply(list)
        .to_dict()
    )

    records: list[dict[str, Any]] = []

    for _ in range(quantity):
        conta_id = random.choice(conta_ids)
        available_cards = card_by_account.get(conta_id, [])
        channel = random.choice(
            ["PIX", "TED", "CARTAO", "BOLETO", "SAQUE"]
        )

        occurred_at = faker.date_time_between(
            start_date="-180d",
            end_date=reference_date,
        )

        records.append(
            {
                "transacao_id": str(uuid4()),
                "conta_id": conta_id,
                "cartao_id": (
                    random.choice(available_cards)
                    if channel == "CARTAO" and available_cards
                    else None
                ),
                "tipo_transacao": channel,
                "valor": round(random.uniform(5, 15000), 2),
                "moeda": "BRL",
                "descricao": faker.sentence(nb_words=5),
                "estabelecimento": faker.company() if channel == "CARTAO" else None,
                "cidade": faker.city(),
                "status_transacao": random.choices(
                    ["APROVADA", "NEGADA", "PENDENTE"],
                    weights=[92, 6, 2],
                    k=1,
                )[0],
                "data_hora_transacao": occurred_at.isoformat(),
                "data_processamento": (
                    occurred_at + timedelta(minutes=random.randint(0, 180))
                ).isoformat(),
                "ingestion_at": reference_date.isoformat(),
            }
        )

    return pd.DataFrame(records)


def generate_eventos_risco(
    faker: Faker,
    transacoes: pd.DataFrame,
    quantity: int,
) -> pd.DataFrame:
    """Gera eventos de risco relacionados às transações."""

    sampled = transacoes.sample(
        n=min(quantity, len(transacoes)),
        random_state=42,
    )

    records: list[dict[str, Any]] = []

    for _, transaction in sampled.iterrows():
        event_time = datetime.fromisoformat(
            transaction["data_hora_transacao"]
        ) + timedelta(seconds=random.randint(1, 120))

        score = round(random.uniform(0, 100), 2)

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
                "data_hora_evento": event_time.isoformat(),
                "detalhes": faker.sentence(nb_words=8),
            }
        )

    return pd.DataFrame(records)


def generate_estornos(
    faker: Faker,
    transacoes: pd.DataFrame,
    quantity: int,
    reference_date: datetime,
) -> pd.DataFrame:
    """Gera estornos para uma amostra de transações aprovadas."""

    eligible = transacoes[
        transacoes["status_transacao"] == "APROVADA"
    ]

    sampled = eligible.sample(
        n=min(quantity, len(eligible)),
        random_state=84,
    )

    records: list[dict[str, Any]] = []

    for _, transaction in sampled.iterrows():
        transaction_time = datetime.fromisoformat(
            transaction["data_hora_transacao"]
        )

        reversal_time = min(
            transaction_time + timedelta(
                days=random.randint(1, 30),
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
            }
        )

    return pd.DataFrame(records)


def generate_initial_batch(config: dict[str, Any]) -> None:
    """Gera o primeiro lote completo da camada Landing."""

    project_config = config["project"]
    output_config = config["output"]
    volumes = config["volumes"]
    batch_name = config["batches"]["initial"]

    seed = int(project_config["seed"])
    locale = project_config["locale"]

    random.seed(seed)
    Faker.seed(seed)

    faker = Faker(locale)
    reference_date = datetime.now().replace(microsecond=0)

    output_base = Path(output_config["base_path"])
    encoding = output_config["encoding"]

    clientes = generate_clientes(
        faker,
        volumes["clientes"],
        reference_date,
    )

    contas = generate_contas(
        faker,
        clientes,
        volumes["contas"],
        reference_date,
    )

    cartoes = generate_cartoes(
        faker,
        contas,
        volumes["cartoes"],
        reference_date,
    )

    transacoes = generate_transacoes(
        faker,
        contas,
        cartoes,
        volumes["transacoes"],
        reference_date,
    )

    eventos_risco = generate_eventos_risco(
        faker,
        transacoes,
        volumes["eventos_risco"],
    )

    estornos = generate_estornos(
        faker,
        transacoes,
        volumes["estornos"],
        reference_date,
    )

    datasets = {
        "clientes_cdc": clientes,
        "contas_cdc": contas,
        "cartoes_cdc": cartoes,
        "transacoes": transacoes,
        "eventos_risco": eventos_risco,
        "estornos": estornos,
    }

    for dataset_name, dataframe in datasets.items():
        destination = output_base / dataset_name / batch_name

        save_dataframe(
            dataframe=dataframe,
            output_path=destination,
            file_name=f"{dataset_name}.csv",
            encoding=encoding,
        )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera a massa sintética do Data Product."
    )

    parser.add_argument(
        "--config",
        default="config/data_generation.yaml",
        help="Caminho do arquivo YAML de configuração.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    config = load_config(Path(arguments.config))

    generate_initial_batch(config)

    print("\nMassa sintética criada com sucesso.")


if __name__ == "__main__":
    main()