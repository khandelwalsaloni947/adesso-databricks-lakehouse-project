# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver Transformations
# MAGIC
# MAGIC ## Goal
# MAGIC - import reusable code from src/
# MAGIC - use df.transform()
# MAGIC - apply one UDF
# MAGIC - write silver Delta table

# COMMAND ----------
import sys
import yaml
from pyspark.sql import functions as F

sys.path.append("/Workspace/Repos/your-user/adesso-week8-day4-mini-project/src")

from transformations.sales_cleaning import standardize_sales_columns, filter_invalid_sales_rows
from transformations.business_rules import add_business_flags
from udfs.market_udfs import classify_market_priority

config_path = "/Workspace/Repos/your-user/adesso-week8-day4-mini-project/config/project_config.yml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

catalog_name = config["catalog"]
raw_schema = config["schemas"]["raw"]
refined_schema = config["schemas"]["refined"]

bronze_sales_table = f"{catalog_name}.{raw_schema}.{config['tables']['bronze_sales']}"
silver_sales_table = f"{catalog_name}.{refined_schema}.{config['tables']['silver_sales']}"
high_value_threshold = config["rules"]["high_value_threshold"]

print("Bronze table:", bronze_sales_table)
print("Silver table:", silver_sales_table)

# COMMAND ----------
bronze_df = spark.table(bronze_sales_table)

silver_df = (
    bronze_df
    .transform(standardize_sales_columns)
    .transform(filter_invalid_sales_rows)
    .transform(lambda df: add_business_flags(df, high_value_threshold))
    .withColumn("market_priority", classify_market_priority(F.col("market"), F.col("sales_amount")))
)

print("Silver row count:", silver_df.count())

# COMMAND ----------
silver_df.write.mode("overwrite").format("delta").saveAsTable(silver_sales_table)
display(spark.table(silver_sales_table))

