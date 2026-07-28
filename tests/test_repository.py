from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_required_notebooks_exist() -> None:
    expected = [
        "00_setup_environment.py",
        "01_generate_sample_data.py",
        "02_load_landing.py",
        "03_validate_landing.py",
        "04_bronze.py",
        "05_silver.py",
        "06_gold.py",
        "07_quality_checks.py",
        "08_demo_queries.py",
    ]
    missing = [name for name in expected if not (ROOT / "notebooks" / name).exists()]
    assert not missing, f"Notebooks ausentes: {missing}"


def test_required_documentation_exists() -> None:
    expected = [
        ROOT / "README.md",
        ROOT / "docs" / "architecture.md",
        ROOT / "docs" / "decisions.md",
        ROOT / "docs" / "data_contracts.md",
    ]
    missing = [str(path.relative_to(ROOT)) for path in expected if not path.exists()]
    assert not missing, f"Documentos ausentes: {missing}"
