# Auto Loader Bronze Ingestion

This project uses Auto Loader for raw-to-bronze ingestion.

The bronze ingestion notebook should:
- read from the governed landing volume
- use `spark.readStream`
- use `format("cloudFiles")`
- set a schema tracking location
- set a checkpoint location
- append into a bronze Delta table

Why this matters:
- Auto Loader is a realistic Databricks ingestion pattern
- the project teaches why `landing` and `checkpoints` are separate
