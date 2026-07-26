from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


LANDING_PATH = Path("data/landing")
BRONZE_PATH = Path("data/bronze")
QUARANTINE_PATH = Path("data/quarantine")


DATASET_RULES: dict[str, dict[str, Any]] = {
    "clientes_cdc": {
        "primary_key": "cliente_id",
        "required_columns": [
            "cliente_id",
            "cpf",
            "nome",
            "data_nascimento",
            "email",
            "status_cliente",
        ],
        "allowed_values": {
            "status_cliente": {
                "ATIVO",
                "INATIVO",
                "BLOQUEADO",
            },
            "segmento": {
                "VAREJO",
                "ALTA_RENDA",
                "EMPRESARIAL",
                "COOPERADO",
            },
        },
    },
    "contas_cdc": {
        "primary_key": "conta_id",
        "required_columns": [
            "conta_id",
            "cliente_id",
            "numero_conta",
            "tipo_conta",
            "saldo",
            "status_conta",
        ],
        "allowed_values": {
            "tipo_conta": {
                "CORRENTE",
                "POUPANCA",
                "PAGAMENTO",
            },
            "status_conta": {
                "ATIVA",
                "INATIVA",
                "BLOQUEADA",
                "ENCERRADA",
            },
        },
    },
    "cartoes_cdc": {
        "primary_key": "cartao_id",
        "required_columns": [
            "cartao_id",
            "conta_id",
            "numero_cartao_mascarado",
            "tipo_cartao",
            "limite_total",
            "status_cartao",
        ],
        "allowed_values": {
            "tipo_cartao": {
                "CREDITO",
                "DEBITO",
                "MULTIPLO",
            },
            "status_cartao": {
                "ATIVO",
                "BLOQUEADO",
                "CANCELADO",
            },
            "bandeira": {
                "VISA",
                "MASTERCARD",
                "ELO",
            },
        },
    },
    "transacoes": {
        "primary_key": "transacao_id",
        "required_columns": [
            "transacao_id",
            "conta_id",
            "tipo_transacao",
            "valor",
            "moeda",
            "status_transacao",
            "data_hora_transacao",
        ],
        "allowed_values": {
            "tipo_transacao": {
                "PIX",
                "TED",
                "CARTAO",
                "BOLETO",
                "SAQUE",
            },
            "status_transacao": {
                "APROVADA",
                "NEGADA",
                "PENDENTE",
            },
            "moeda": {
                "BRL",
            },
        },
    },
    "eventos_risco": {
        "primary_key": "evento_risco_id",
        "required_columns": [
            "evento_risco_id",
            "transacao_id",
            "score_risco",
            "nivel_risco",
            "decisao",
            "data_hora_evento",
        ],
        "allowed_values": {
            "nivel_risco": {
                "BAIXO",
                "MEDIO",
                "ALTO",
            },
            "decisao": {
                "APROVAR",
                "REVISAR",
                "BLOQUEAR",
            },
        },
    },
    "estornos": {
        "primary_key": "estorno_id",
        "required_columns": [
            "estorno_id",
            "transacao_id",
            "valor_estorno",
            "motivo_estorno",
            "status_estorno",
            "data_hora_estorno",
        ],
        "allowed_values": {
            "status_estorno": {
                "SOLICITADO",
                "PROCESSADO",
                "NEGADO",
            },
        },
    },
}


def read_dataset_batches(dataset_name: str) -> pd.DataFrame:
    dataset_path = LANDING_PATH / dataset_name

    files = sorted(dataset_path.rglob("*.csv"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado para o dataset: {dataset_name}"
        )

    frames: list[pd.DataFrame] = []

    for file_path in files:
        batch_name = file_path.parent.name

        dataframe = pd.read_csv(
            file_path,
            dtype="object",
        )

        dataframe["_dataset_name"] = dataset_name
        dataframe["_source_batch"] = batch_name
        dataframe["_source_file"] = str(file_path)
        dataframe["_row_number"] = range(
            1,
            len(dataframe) + 1,
        )

        frames.append(dataframe)

    return pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )


def append_error(
    errors: pd.Series,
    condition: pd.Series,
    message: str,
) -> pd.Series:
    current = errors.fillna("")

    updated = current.where(
        ~condition,
        current + message + "; ",
    )

    return updated


def validate_required_columns(
    dataframe: pd.DataFrame,
    required_columns: list[str],
    errors: pd.Series,
) -> pd.Series:
    for column in required_columns:
        if column not in dataframe.columns:
            errors = append_error(
                errors,
                pd.Series(True, index=dataframe.index),
                f"coluna_ausente:{column}",
            )
            continue

        is_null = (
            dataframe[column].isna()
            | dataframe[column].astype(str).str.strip().eq("")
        )

        errors = append_error(
            errors,
            is_null,
            f"campo_obrigatorio_nulo:{column}",
        )

    return errors


def validate_allowed_values(
    dataframe: pd.DataFrame,
    allowed_values: dict[str, set[str]],
    errors: pd.Series,
) -> pd.Series:
    for column, allowed in allowed_values.items():
        if column not in dataframe.columns:
            continue

        values = dataframe[column].astype(str)

        invalid = (
            dataframe[column].notna()
            & ~values.isin(allowed)
        )

        errors = append_error(
            errors,
            invalid,
            f"dominio_invalido:{column}",
        )

    return errors


def validate_email(
    dataframe: pd.DataFrame,
    errors: pd.Series,
) -> pd.Series:
    if "email" not in dataframe.columns:
        return errors

    email_pattern = re.compile(
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    )

    invalid = (
        dataframe["email"].notna()
        & ~dataframe["email"]
        .astype(str)
        .str.match(email_pattern)
    )

    return append_error(
        errors,
        invalid,
        "email_invalido",
    )


def validate_numeric_column(
    dataframe: pd.DataFrame,
    column: str,
    errors: pd.Series,
    minimum: float | None = None,
    maximum: float | None = None,
) -> tuple[pd.Series, pd.Series]:
    if column not in dataframe.columns:
        return errors, pd.Series(
            index=dataframe.index,
            dtype="float64",
        )

    numeric = pd.to_numeric(
        dataframe[column],
        errors="coerce",
    )

    invalid_type = (
        dataframe[column].notna()
        & numeric.isna()
    )

    errors = append_error(
        errors,
        invalid_type,
        f"tipo_numerico_invalido:{column}",
    )

    if minimum is not None:
        below_minimum = numeric.notna() & (numeric < minimum)

        errors = append_error(
            errors,
            below_minimum,
            f"valor_abaixo_minimo:{column}",
        )

    if maximum is not None:
        above_maximum = numeric.notna() & (numeric > maximum)

        errors = append_error(
            errors,
            above_maximum,
            f"valor_acima_maximo:{column}",
        )

    return errors, numeric


def validate_datetime_column(
    dataframe: pd.DataFrame,
    column: str,
    errors: pd.Series,
) -> pd.Series:
    if column not in dataframe.columns:
        return errors

    parsed = pd.to_datetime(
        dataframe[column],
        errors="coerce",
        utc=True,
    )

    invalid = (
        dataframe[column].notna()
        & parsed.isna()
    )

    return append_error(
        errors,
        invalid,
        f"data_invalida:{column}",
    )


def validate_duplicates(
    dataframe: pd.DataFrame,
    primary_key: str,
    errors: pd.Series,
) -> pd.Series:
    if primary_key not in dataframe.columns:
        return errors

    duplicate = dataframe.duplicated(
        subset=[
            primary_key,
            "_source_batch",
        ],
        keep="first",
    )

    return append_error(
        errors,
        duplicate,
        f"duplicidade:{primary_key}",
    )


def validate_clientes(
    dataframe: pd.DataFrame,
    errors: pd.Series,
) -> pd.Series:
    errors = validate_email(
        dataframe,
        errors,
    )

    errors = validate_datetime_column(
        dataframe,
        "data_nascimento",
        errors,
    )

    if "data_nascimento" in dataframe.columns:
        birth_date = pd.to_datetime(
            dataframe["data_nascimento"],
            errors="coerce",
        )

        future_date = (
            birth_date.notna()
            & (birth_date > pd.Timestamp.now())
        )

        errors = append_error(
            errors,
            future_date,
            "data_nascimento_futura",
        )

    errors, cliente_id = validate_numeric_column(
        dataframe,
        "cliente_id",
        errors,
        minimum=1,
    )

    return errors


def validate_contas(
    dataframe: pd.DataFrame,
    errors: pd.Series,
) -> pd.Series:
    errors, _ = validate_numeric_column(
        dataframe,
        "conta_id",
        errors,
        minimum=1,
    )

    errors, _ = validate_numeric_column(
        dataframe,
        "cliente_id",
        errors,
        minimum=1,
    )

    errors, _ = validate_numeric_column(
        dataframe,
        "saldo",
        errors,
    )

    errors, _ = validate_numeric_column(
        dataframe,
        "limite_credito",
        errors,
        minimum=0,
    )

    return errors


def validate_cartoes(
    dataframe: pd.DataFrame,
    errors: pd.Series,
) -> pd.Series:
    errors, _ = validate_numeric_column(
        dataframe,
        "cartao_id",
        errors,
        minimum=1,
    )

    errors, _ = validate_numeric_column(
        dataframe,
        "conta_id",
        errors,
        minimum=1,
    )

    errors, _ = validate_numeric_column(
        dataframe,
        "limite_total",
        errors,
        minimum=0,
    )

    errors, _ = validate_numeric_column(
        dataframe,
        "limite_disponivel",
        errors,
        minimum=0,
    )

    return errors


def validate_transacoes(
    dataframe: pd.DataFrame,
    errors: pd.Series,
) -> pd.Series:
    errors, _ = validate_numeric_column(
        dataframe,
        "conta_id",
        errors,
        minimum=1,
    )

    errors, _ = validate_numeric_column(
        dataframe,
        "valor",
        errors,
        minimum=0.01,
    )

    errors = validate_datetime_column(
        dataframe,
        "data_hora_transacao",
        errors,
    )

    errors = validate_datetime_column(
        dataframe,
        "data_processamento",
        errors,
    )

    return errors


def validate_eventos_risco(
    dataframe: pd.DataFrame,
    errors: pd.Series,
) -> pd.Series:
    errors, _ = validate_numeric_column(
        dataframe,
        "score_risco",
        errors,
        minimum=0,
        maximum=100,
    )

    errors = validate_datetime_column(
        dataframe,
        "data_hora_evento",
        errors,
    )

    return errors


def validate_estornos(
    dataframe: pd.DataFrame,
    errors: pd.Series,
) -> pd.Series:
    errors, _ = validate_numeric_column(
        dataframe,
        "valor_estorno",
        errors,
        minimum=0.01,
    )

    errors = validate_datetime_column(
        dataframe,
        "data_hora_estorno",
        errors,
    )

    return errors


def validate_dataset(
    dataset_name: str,
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rules = DATASET_RULES[dataset_name]

    errors = pd.Series(
        "",
        index=dataframe.index,
        dtype="object",
    )

    errors = validate_required_columns(
        dataframe,
        rules["required_columns"],
        errors,
    )

    errors = validate_allowed_values(
        dataframe,
        rules["allowed_values"],
        errors,
    )

    errors = validate_duplicates(
        dataframe,
        rules["primary_key"],
        errors,
    )

    if dataset_name == "clientes_cdc":
        errors = validate_clientes(
            dataframe,
            errors,
        )

    elif dataset_name == "contas_cdc":
        errors = validate_contas(
            dataframe,
            errors,
        )

    elif dataset_name == "cartoes_cdc":
        errors = validate_cartoes(
            dataframe,
            errors,
        )

    elif dataset_name == "transacoes":
        errors = validate_transacoes(
            dataframe,
            errors,
        )

    elif dataset_name == "eventos_risco":
        errors = validate_eventos_risco(
            dataframe,
            errors,
        )

    elif dataset_name == "estornos":
        errors = validate_estornos(
            dataframe,
            errors,
        )

    result = dataframe.copy()

    result["_validation_errors"] = (
        errors.str.rstrip("; ")
    )

    result["_is_valid"] = (
        result["_validation_errors"].eq("")
    )

    valid = result[
        result["_is_valid"]
    ].copy()

    invalid = result[
        ~result["_is_valid"]
    ].copy()

    return valid, invalid


def save_results(
    dataset_name: str,
    valid: pd.DataFrame,
    invalid: pd.DataFrame,
) -> None:
    bronze_dataset_path = (
        BRONZE_PATH
        / dataset_name
    )

    quarantine_dataset_path = (
        QUARANTINE_PATH
        / dataset_name
    )

    bronze_dataset_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_dataset_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    valid_file = (
        bronze_dataset_path
        / f"{dataset_name}.parquet"
    )

    invalid_file = (
        quarantine_dataset_path
        / f"{dataset_name}_quarantine.csv"
    )

    valid.to_parquet(
        valid_file,
        index=False,
    )

    invalid.to_csv(
        invalid_file,
        index=False,
        encoding="utf-8",
    )

    print(
        f"[OK] {dataset_name:<18} "
        f"válidos={len(valid):>6} "
        f"quarentena={len(invalid):>4}"
    )


def main() -> None:
    QUARANTINE_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    for dataset_name in DATASET_RULES:
        dataframe = read_dataset_batches(
            dataset_name
        )

        valid, invalid = validate_dataset(
            dataset_name,
            dataframe,
        )

        save_results(
            dataset_name,
            valid,
            invalid,
        )

    print(
        "\nValidação da Landing concluída com sucesso."
    )


if __name__ == "__main__":
    main()