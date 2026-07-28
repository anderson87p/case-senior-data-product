# Evidências de Execução

Esta pasta contém as principais evidências da execução do pipeline desenvolvido no **Databricks Free Edition**, demonstrando a implementação da arquitetura **Medallion**, a criação das tabelas analíticas e a validação da qualidade dos dados.

## Evidências

| Arquivo | Descrição |
| :--- | :--- |
| `01_unity_catalog.png` | Organização do Unity Catalog com os schemas das camadas **Landing**, **Bronze**, **Silver**, **Gold**, **Quarantine** e **Audit**. |
| `02_landing_tables.png` | Landing Zone contendo os arquivos de entrada e as tabelas validadas da camada Landing. |
| `03_gold_tables.png` | Estrutura das tabelas analíticas da camada Gold, incluindo dimensões, tabela fato e Data Products. |
| `04_quality_checks.png` | Resultado da execução dos Quality Checks, demonstrando **30 validações aprovadas**, **3 warnings** e **0 falhas críticas**. |
| `05_demo_queries.png` | Consultas analíticas e KPIs executados sobre a camada Gold. |
| `06_pipeline.png` | Sequência de execução dos notebooks que compõem o pipeline de dados. |
| `07_gold_data.png` | Consulta realizada na tabela `gold_fato_transacao`, demonstrando os dados carregados na camada Gold. |

## Tecnologias Demonstradas

- Databricks Free Edition
- Unity Catalog
- Delta Lake
- Arquitetura Medallion
- Change Data Capture (CDC)
- Slowly Changing Dimension Type 2 (SCD Type 2)
- Data Quality
- Data Products