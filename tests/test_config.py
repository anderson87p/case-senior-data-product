from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_data_generation_configuration() -> None:
    config_path = ROOT / "config" / "data_generation.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert config["project"]["seed"] == 42
    assert config["output"]["format"] == "csv"
    assert set(config["volumes"]) == {
        "clientes",
        "contas",
        "cartoes",
        "transacoes",
        "eventos_risco",
        "estornos",
    }
    assert set(config["batches"]) == {"initial", "updates", "late_arriving"}
