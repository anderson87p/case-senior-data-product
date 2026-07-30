from datetime import datetime, timedelta

from src.silver_business_rules import (
    build_scd2_history,
    deduplicate_cdc_events,
    deduplicate_transactional_events,
    is_late_arrival,
    merge_by_keys,
    validate_single_current_record,
    validate_transaction_references,
)


def test_cdc_exact_duplicate_keeps_latest_processing_record() -> None:
    event = {
        "cliente_id": "CLI_001",
        "data_atualizacao": datetime(2026, 1, 1, 10),
        "operacao_cdc": "U",
        "_record_hash": "same-content",
        "_processing_date": datetime(2026, 1, 1, 11),
        "_batch_id": "batch_001",
        "_bronze_ingestion_timestamp": datetime(2026, 1, 1, 11),
    }
    newer_copy = {
        **event,
        "_processing_date": datetime(2026, 1, 2, 11),
        "_batch_id": "batch_002",
        "_bronze_ingestion_timestamp": datetime(2026, 1, 2, 11),
    }

    result = deduplicate_cdc_events(
        [event, newer_copy],
        primary_key="cliente_id",
        sequence_column="data_atualizacao",
    )

    assert len(result) == 1
    assert result[0]["_batch_id"] == "batch_002"


def test_scd2_reconstructs_late_arrival_in_business_order(customer_cdc_events) -> None:
    result = build_scd2_history(
        customer_cdc_events,
        primary_key="cliente_id",
        sequence_column="data_atualizacao",
    )

    assert [row["cidade"] for row in result] == [
        "Salvador",
        "Camaçari",
        "Lauro de Freitas",
    ]
    assert [row["versao_registro"] for row in result] == [1, 2, 3]
    assert result[0]["vigencia_fim"] == result[1]["vigencia_inicio"] - timedelta(microseconds=1)
    assert result[1]["vigencia_fim"] == result[2]["vigencia_inicio"] - timedelta(microseconds=1)


def test_scd2_has_exactly_one_current_record(customer_cdc_events) -> None:
    result = build_scd2_history(
        customer_cdc_events,
        primary_key="cliente_id",
        sequence_column="data_atualizacao",
    )

    assert validate_single_current_record(result, primary_key="cliente_id")
    assert sum(row["registro_atual"] for row in result) == 1
    assert result[-1]["registro_atual"] is True


def test_cdc_delete_is_preserved_as_current_soft_delete(customer_cdc_events) -> None:
    delete_event = {
        **customer_cdc_events[-1],
        "data_atualizacao": datetime(2026, 1, 5, 10),
        "operacao_cdc": "D",
        "_record_hash": "hash-delete",
        "_processing_date": datetime(2026, 1, 5, 11),
        "_batch_id": "batch_004",
        "_bronze_ingestion_timestamp": datetime(2026, 1, 5, 11),
    }
    result = build_scd2_history(
        [*customer_cdc_events, delete_event],
        primary_key="cliente_id",
        sequence_column="data_atualizacao",
    )

    assert result[-1]["registro_atual"] is True
    assert result[-1]["registro_excluido"] is True
    assert result[-1]["operacao_cdc"] == "D"


def test_transactional_deduplication_keeps_latest_event() -> None:
    records = [
        {
            "transacao_id": "TRX_001",
            "data_transacao": datetime(2026, 1, 1, 10),
            "valor": 100,
            "_processing_date": datetime(2026, 1, 1, 11),
            "_batch_id": "batch_001",
            "_bronze_ingestion_timestamp": datetime(2026, 1, 1, 11),
        },
        {
            "transacao_id": "TRX_001",
            "data_transacao": datetime(2026, 1, 1, 12),
            "valor": 120,
            "_processing_date": datetime(2026, 1, 1, 13),
            "_batch_id": "batch_002",
            "_bronze_ingestion_timestamp": datetime(2026, 1, 1, 13),
        },
    ]

    result = deduplicate_transactional_events(
        records,
        primary_key="transacao_id",
        event_timestamp_column="data_transacao",
    )

    assert len(result) == 1
    assert result[0]["valor"] == 120


def test_late_arrival_threshold_is_strictly_greater_than_24_hours() -> None:
    event_time = datetime(2026, 1, 1, 10)

    assert not is_late_arrival(event_time, event_time + timedelta(hours=24))
    assert is_late_arrival(event_time, event_time + timedelta(hours=24, seconds=1))


def test_invalid_transaction_references_are_sent_to_quarantine() -> None:
    transaction = {"conta_id": "CTA_INVALIDA", "cartao_id": "CAR_INVALIDO"}

    valid, reason = validate_transaction_references(
        transaction,
        valid_account_ids={"CTA_001"},
        valid_card_ids={"CAR_001"},
    )

    assert valid is False
    assert reason == "REFERENCIA_INVALIDA:conta_id | REFERENCIA_INVALIDA:cartao_id"


def test_null_card_reference_is_allowed_when_account_is_valid() -> None:
    transaction = {"conta_id": "CTA_001", "cartao_id": None}

    valid, reason = validate_transaction_references(
        transaction,
        valid_account_ids={"CTA_001"},
        valid_card_ids=set(),
    )

    assert valid is True
    assert reason is None


def test_merge_is_idempotent_for_same_source() -> None:
    target = [{"cliente_id": "CLI_001", "vigencia_inicio": "2026-01-01", "nome": "Ana"}]
    source = [
        {"cliente_id": "CLI_001", "vigencia_inicio": "2026-01-01", "nome": "Ana"},
        {"cliente_id": "CLI_001", "vigencia_inicio": "2026-01-02", "nome": "Ana Maria"},
    ]

    first_merge = merge_by_keys(
        target,
        source,
        merge_keys=("cliente_id", "vigencia_inicio"),
    )
    second_merge = merge_by_keys(
        first_merge,
        source,
        merge_keys=("cliente_id", "vigencia_inicio"),
    )

    assert len(first_merge) == 2
    assert second_merge == first_merge
