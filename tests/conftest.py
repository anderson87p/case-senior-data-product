from datetime import datetime

import pytest


@pytest.fixture
def customer_cdc_events() -> list[dict]:
    """Three versions where the middle event arrived in a later batch."""
    return [
        {
            "cliente_id": "CLI_001",
            "nome": "Ana",
            "cidade": "Salvador",
            "data_atualizacao": datetime(2026, 1, 1, 10, 0),
            "operacao_cdc": "I",
            "_record_hash": "hash-v1",
            "_processing_date": datetime(2026, 1, 1, 11, 0),
            "_batch_id": "batch_001",
            "_bronze_ingestion_timestamp": datetime(2026, 1, 1, 11, 0),
        },
        {
            "cliente_id": "CLI_001",
            "nome": "Ana",
            "cidade": "Lauro de Freitas",
            "data_atualizacao": datetime(2026, 1, 3, 10, 0),
            "operacao_cdc": "U",
            "_record_hash": "hash-v3",
            "_processing_date": datetime(2026, 1, 3, 11, 0),
            "_batch_id": "batch_002",
            "_bronze_ingestion_timestamp": datetime(2026, 1, 3, 11, 0),
        },
        {
            "cliente_id": "CLI_001",
            "nome": "Ana",
            "cidade": "Camaçari",
            "data_atualizacao": datetime(2026, 1, 2, 10, 0),
            "operacao_cdc": "U",
            "_record_hash": "hash-v2",
            "_processing_date": datetime(2026, 1, 4, 11, 0),
            "_batch_id": "batch_003",
            "_bronze_ingestion_timestamp": datetime(2026, 1, 4, 11, 0),
        },
    ]
