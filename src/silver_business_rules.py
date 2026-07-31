"""Pure-Python reference rules for the Silver layer.

These functions model the business behavior implemented in the Databricks
Silver notebook. They intentionally avoid Spark dependencies so the core
rules can be exercised by pytest in any Python environment.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Iterable

Record = dict[str, Any]


def _sort_value(value: Any) -> tuple[bool, Any]:
    """Return a stable value that sorts nulls last."""
    return (value is None, value)


def deduplicate_cdc_events(
    records: Iterable[Record],
    *,
    primary_key: str,
    sequence_column: str,
) -> list[Record]:
    """Keep the most recently processed exact CDC event.

    An exact event is identified by business key, event timestamp, CDC
    operation and record hash. Processing metadata is used as tie-breaker.
    """
    selected: dict[tuple[Any, ...], Record] = {}
    for record in records:
        event_key = (
            record[primary_key],
            record[sequence_column],
            record["operacao_cdc"],
            record["_record_hash"],
        )
        rank = (
            record["_processing_date"],
            record["_batch_id"],
            record["_bronze_ingestion_timestamp"],
        )
        current = selected.get(event_key)
        if current is None:
            selected[event_key] = dict(record)
            continue
        current_rank = (
            current["_processing_date"],
            current["_batch_id"],
            current["_bronze_ingestion_timestamp"],
        )
        if rank > current_rank:
            selected[event_key] = dict(record)
    return list(selected.values())


def build_scd2_history(
    records: Iterable[Record],
    *,
    primary_key: str,
    sequence_column: str,
) -> list[Record]:
    """Reconstruct SCD Type 2 history in business-event order.

    Late-arriving events are positioned by their business timestamp rather
    than by batch arrival order. The prior row ends one microsecond before
    the next version starts.
    """
    deduplicated = deduplicate_cdc_events(
        records,
        primary_key=primary_key,
        sequence_column=sequence_column,
    )
    grouped: dict[Any, list[Record]] = defaultdict(list)
    for record in deduplicated:
        grouped[record[primary_key]].append(record)

    history: list[Record] = []
    for business_key, versions in grouped.items():
        versions.sort(
            key=lambda row: (
                row[sequence_column],
                row["_processing_date"],
                row["_batch_id"],
                row["_bronze_ingestion_timestamp"],
            )
        )
        for index, version in enumerate(versions):
            output = dict(version)
            next_version = versions[index + 1] if index + 1 < len(versions) else None
            output["vigencia_inicio"] = version[sequence_column]
            output["vigencia_fim"] = (
                next_version[sequence_column] - timedelta(microseconds=1)
                if next_version is not None
                else None
            )
            output["versao_registro"] = index + 1
            output["registro_atual"] = next_version is None
            output["registro_excluido"] = version["operacao_cdc"] == "D"
            history.append(output)

    return sorted(
        history,
        key=lambda row: (row[primary_key], row["vigencia_inicio"]),
    )


def deduplicate_transactional_events(
    records: Iterable[Record],
    *,
    primary_key: str,
    event_timestamp_column: str,
) -> list[Record]:
    """Keep one transactional event per technical key using Silver priority."""
    grouped: dict[Any, list[Record]] = defaultdict(list)
    for record in records:
        grouped[record[primary_key]].append(record)

    selected: list[Record] = []
    for versions in grouped.values():
        versions.sort(
            key=lambda row: (
                _sort_value(row.get(event_timestamp_column)),
                row["_processing_date"],
                row["_batch_id"],
                row["_bronze_ingestion_timestamp"],
            ),
            reverse=True,
        )
        selected.append(dict(versions[0]))
    return selected


def is_late_arrival(
    event_timestamp: datetime,
    source_ingestion_timestamp: datetime,
    *,
    threshold_hours: int = 24,
) -> bool:
    """Return whether ingestion occurred more than the threshold after the event."""
    return source_ingestion_timestamp - event_timestamp > timedelta(hours=threshold_hours)


def validate_transaction_references(
    transaction: Record,
    *,
    valid_account_ids: set[str],
    valid_card_ids: set[str],
) -> tuple[bool, str | None]:
    """Validate account/card references using the notebook's business rules."""
    account_valid = transaction.get("conta_id") in valid_account_ids
    card_id = transaction.get("cartao_id")
    card_valid = card_id is None or card_id in valid_card_ids

    reasons: list[str] = []
    if not account_valid:
        reasons.append("REFERENCIA_INVALIDA:conta_id")
    if not card_valid:
        reasons.append("REFERENCIA_INVALIDA:cartao_id")
    return account_valid and card_valid, " | ".join(reasons) or None


def validate_single_current_record(
    history: Iterable[Record],
    *,
    primary_key: str,
) -> bool:
    """Return True when every business key has exactly one current version."""
    current_counts: dict[Any, int] = defaultdict(int)
    all_keys: set[Any] = set()
    for record in history:
        key = record[primary_key]
        all_keys.add(key)
        if record.get("registro_atual"):
            current_counts[key] += 1
    return all(current_counts[key] == 1 for key in all_keys)


def merge_by_keys(
    target: Iterable[Record],
    source: Iterable[Record],
    *,
    merge_keys: tuple[str, ...],
) -> list[Record]:
    """Reference upsert used to test idempotency semantics of Delta MERGE."""
    merged = {
        tuple(record.get(key) for key in merge_keys): dict(record)
        for record in target
    }
    for record in source:
        merged[tuple(record.get(key) for key in merge_keys)] = dict(record)
    return list(merged.values())
