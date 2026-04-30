# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Validation and Inspection
# MAGIC
# MAGIC ## Goal
# MAGIC - validate silver data
# MAGIC - inspect market distribution
# MAGIC - optionally set a task value

# COMMAND ----------
import yaml
from pyspark.sql import functions as F

config_path = "/Workspace/Repos/your-user/adesso-week8-day4-mini-project/config/project_config.yml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

catalog_name = config["catalog"]
refined_schema = config["schemas"]["refined"]
silver_sales_table = f"{catalog_name}.{refined_schema}.{config['tables']['silver_sales']}"

silver_df = spark.table(silver_sales_table)

row_count = silver_df.count()
null_market = silver_df.filter(F.col("market").isNull()).count()
null_brand = silver_df.filter(F.col("brand").isNull()).count()
negative_sales = silver_df.filter(F.col("sales_amount") < 0).count()

print("Silver row count:", row_count)
print("Rows with null market:", null_market)
print("Rows with null brand:", null_brand)
print("Rows with negative sales amount:", negative_sales)

display(silver_df.groupBy("market").count().orderBy("market"))

# COMMAND ----------
try:
    dbutils.jobs.taskValues.set(key="silver_row_count", value=row_count)
    print("Task value set: silver_row_count =", row_count)
except Exception as e:
    print("Task value not set. This is normal if the notebook is not running inside a Job.")
    print("Reason:", e)

