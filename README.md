# PaySim Fraud Analytics Platform

An end-to-end Azure data engineering project: mobile-money transactions are ingested, refined through a bronze/silver/gold medallion architecture, screened by a fraud rule, and reported in Power BI.

> **Status: in progress.** Phase 0 (design, local development and EDA) is underway. Azure deployment starts in November 2026.

## Architecture (planned)

- **Batch path:** Azure Data Factory loads daily CSV files into ADLS Gen2
- **Streaming path:** a replay script sends held-back transactions through Azure Event Hubs
- **Processing:** Azure Databricks (Unity Catalog, Delta Lake) refines data from bronze to silver to gold
- **Serving:** gold tables mirrored to Microsoft Fabric; Power BI dashboard in Direct Lake mode
- **Platform:** Key Vault for secrets, Log Analytics for monitoring and alerts, Bicep for infrastructure as code, Azure DevOps for Boards, Repos and CI/CD

## Key findings so far (EDA)

From `notebooks/eda/01_paysim_eda.ipynb` (6.36M transactions):

- Fraud occurs only in TRANSFER and CASH_OUT transactions (0.30% of those types).
- 97.8% of fraudulent transactions send exactly the full account balance; legitimate transactions never do.
- A two-condition rule (full balance, or the 10M cap) reaches 100% precision and 99.6% recall on this simulated data, against 0.19% recall for the dataset's legacy flag.

## Progress

- [x] Architecture design and decision log
- [x] Exploratory data analysis
- [x] Daily batch / streaming split script
- [x] Local Spark and Delta Lake test setup
- [ ] Bronze, silver and gold transforms (local, tested)
- [ ] CI pipeline
- [ ] Azure infrastructure (Bicep)
- [ ] Data Factory ingestion
- [ ] Databricks medallion pipeline
- [ ] Event Hubs streaming
- [ ] Monitoring and alerting
- [ ] Fabric and Power BI dashboard

## Repository layout

| Folder | Contents |
|---|---|
| `data/` | Split script (the dataset itself is not committed) |
| `notebooks/eda/` | Exploratory data analysis |
| `databricks/` | Transform and data quality code, notebooks, asset bundle |
| `tests/` | pytest unit tests |
| `infra/` | Bicep templates |
| `adf/` | Data Factory pipelines |
| `streaming/` | Event Hubs replay script |
| `pipelines/` | Azure Pipelines CI/CD |
| `docs/` | Design notes, monitoring queries, screenshots |

## Running locally

Requires Python 3.12, Java 17 and the PaySim CSV at `data/raw/paysim.csv`.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python data/split_paysim.py
pytest -v
```

## Data

[PaySim synthetic financial dataset](https://www.kaggle.com/datasets/ealaxi/paysim1) (E. A. Lopez-Rojas, Kaggle). It is synthetic, so no personal data is processed. Download it separately; it is excluded from the repository.

## Project management

Work is tracked in Azure Boards and developed in Azure Repos, with every change merged through a reviewed pull request linked to a work item. This GitHub repository mirrors the `main` branch.