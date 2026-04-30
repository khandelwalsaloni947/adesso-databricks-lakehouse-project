# adesso — Week 8 Day 4 Mini Project

## Theme
**adesso Databricks Governance and Lakehouse Implementation Project**

By the end of this mini project, take a moment to review the role and apply here: [Cloud Data Engineer – Databricks at adesso](https://jobs.adesso-group.com/job/Aachen-Cloud-Data-Engineer-Databricks-%28all-genders%29-NW-52070/1145683555/).


This self-learning mini project is inspired by the adesso Cloud Data Engineer Databricks job posting and is designed to help students practice the technical ideas of **Week 8 Day 4**:

- Unity Catalog
- governance
- access control
- governed file locations vs governed tables
- Auto Loader into bronze
- modular transformations with `DataFrame.transform()`
- reusable code in `src/`
- selected UDF usage
- gold tables and views
- practical Jobs/tasks execution
- optional scheduling

## Main scenario
You are working as a junior cloud data engineer on an adesso-style client implementation.  
Your task is to build a governed Databricks project that receives raw files, lands them in a governed Unity Catalog volume, ingests them into bronze using Auto Loader, transforms them into silver using modular Python code, creates governed gold outputs, and applies basic permission and governance inspection.

## Why this project matters
This project mirrors the kind of platform-oriented Databricks work described in the adesso role:
- Unity Catalog
- Delta Lake
- Spark / PySpark
- data platform implementation
- scalable, structured, governed data engineering work

## Learning objectives
By the end of this project, you should be able to:
1. create and use Unity Catalog objects
2. distinguish between volumes and tables
3. ingest files into bronze with Auto Loader
4. structure reusable transformations in `src/`
5. use `sys.path.append(...)` to import project modules
6. apply `df.transform(...)` for modular silver logic
7. use a UDF for one custom business rule
8. build gold tables and views
9. apply and inspect permissions
10. inspect the governed structure in Catalog Explorer
11. run the project through Jobs and tasks
12. optionally add a simple schedule

## Recommended order
Start here:
- `docs/00_how_to_use_this_repo.md`

Then follow the tutorial docs in order.

## Main folders
- `config/` project settings
- `docs/` self-learning tutorial files
- `sample_data/` multi-file raw and reference data
- `sql/` reusable SQL setup and governance files
- `src/` reusable transformation code and UDFs
- `notebooks/` Databricks notebook tasks
- `.github/workflows/` repo structure checks

## Important note
This project is designed for a Databricks Free Edition-friendly classroom setup.  
It uses Unity Catalog volumes instead of direct external storage setup.

## Final expected result
A governed Databricks project with:
- catalog, schemas, volumes, tables, views
- bronze ingestion with Auto Loader
- silver transformations via modular imports and `df.transform()`
- gold outputs
- permissions and governance inspection
- one Job with tasks
