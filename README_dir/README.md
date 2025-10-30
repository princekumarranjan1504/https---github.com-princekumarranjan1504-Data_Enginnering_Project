# NeoStats — Server Performance Pipeline (Documentation)

This document provides a concise overview of the Neostars_Project pipeline, how to run it locally, produced artifacts, assumptions, and recommended next steps for scaling.

## Project overview
A batch data pipeline that ingests server metadata and performance logs (Excel), cleans and transforms them into actionable metrics, and produces aggregated outputs suitable for Power BI visualization.

Primary components
- `scripts/ingestion.py` — reads Excel sheets into pandas DataFrames
- `scripts/cleaning.py` — normalizes headers, cleans data, detects anomalies
- `scripts/transformation.py` — computes metrics (CPU, Memory, Disk I/O, Network), aggregates windows, and enriches with metadata
- `notebooks/neo_pipeline.ipynb` — runnable pipeline notebook
- `notebooks/documentation.ipynb` — detailed documentation and guidance
- `output/` — CSV outputs: `structured_data.csv` and `aggregates_<window>.csv`
- `logs/` — runtime logs

## How to run (PowerShell)
1. Install dependencies (minimal):

```powershell
pip install pandas openpyxl
```

2. Run the notebook `notebooks/neo_pipeline.ipynb` in Jupyter, or run the inline script below from the project root (PowerShell):

```powershell
python - <<'PY'
from scripts.ingestion import load_excel
from scripts.cleaning import clean_performance_data
from scripts.transformation import transform_data
from scripts.utils import save_output, setup_logger

setup_logger('logs/pipeline.log')
meta, st1, st2 = load_excel('data/Data Engineering Use Case Dataset.xlsx')
import pandas as pd
perf = pd.concat([st1, st2], ignore_index=True)
cleaned = clean_performance_data(perf)
final = transform_data(cleaned, meta)
save_output(final, 'output/structured_data.csv')
print(final.head())
PY
```

Notes:
- The notebook `notebooks/neo_pipeline.ipynb` performs the same steps with logging and helpful prints.
- Aggregated CSVs (1min, 5min, hourly) are written to `output/` by the transformation step for Power BI.

## Produced artifacts
- `output/structured_data.csv` — cleaned and enriched row-level dataset
- `output/aggregates_1min.csv`, `output/aggregates_5min.csv`, `output/aggregates_hourly.csv` — aggregated time-series views
- `notebooks/documentation.ipynb` — full documentation (architecture, data flow, cleaning/transformation summaries)
- `logs/pipeline.log` — pipeline run logs

## Assumptions
- The Excel file uses sheets: `Server_Metadata`, `Server_Performance_Station1`, `Server_Performance_Station2`.
- Timestamps are present or parsable; when not, rows will be flagged by the cleaner.
- CPU/Memory may be provided either as used/total pairs or pre-computed percentages; the code handles both.
- Disk/network metrics may be present as rates or cumulative counters — transformation attempts to compute rates when counters are present.

## Next steps / recommendations
- Validate the aggregated outputs and anomaly summaries against expected values.
- Convert CSV outputs to Parquet/SQL for scale and better Power BI performance.
- If Azure is available: map ingestion to ADF, storage to ADLS Gen2, transformation to Synapse/Databricks, and publish reports to Power BI Service.
- Add unit tests for the cleaning and transformation functions.

---
Documentation created in `docs/README.md`. Update or expand as needed.