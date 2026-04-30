# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 01 — Setup Unity Catalog Objects
# MAGIC
# MAGIC ## Goal
# MAGIC - create catalog, schemas, and volumes
# MAGIC - define governed structure for the project

# COMMAND ----------

import yaml

config_path = "/Workspace/Repos/Mini Projects/adesso-databricks-lakehouse-project/config/project_config.yml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

catalog_name = config["catalog"]
raw_schema = config["schemas"]["raw"]
refined_schema = config["schemas"]["refined"]
analytics_schema = config["schemas"]["analytics"]
landing_volume = config["volumes"]["landing"]
checkpoint_volume = config["volumes"]["checkpoints"]

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{raw_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{refined_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{analytics_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{raw_schema}.{landing_volume}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{raw_schema}.{checkpoint_volume}")

print("Created or confirmed catalog, schemas, and volumes.")
