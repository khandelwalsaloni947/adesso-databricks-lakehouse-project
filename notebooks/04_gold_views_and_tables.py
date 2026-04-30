# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 04 — Gold Tables and Views
# MAGIC
# MAGIC ## Goal
# MAGIC - create gold business outputs
# MAGIC - create at least one view

# COMMAND ----------

import yaml

config_path = "/Workspace/Repos/Mini Projects/adesso-databricks-lakehouse-project/config/project_config.yml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

catalog_name = config["catalog"]
refined_schema = config["schemas"]["refined"]
analytics_schema = config["schemas"]["analytics"]

silver_sales_table = f"{catalog_name}.{refined_schema}.{config['tables']['silver_sales']}"
gold_market_summary = f"{catalog_name}.{analytics_schema}.{config['tables']['gold_market_summary']}"
gold_brand_summary = f"{catalog_name}.{analytics_schema}.{config['tables']['gold_brand_summary']}"
brand_view = f"{catalog_name}.{analytics_schema}.{config['views']['vw_brand_sales_summary']}"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_market_summary} AS
SELECT
  report_day,
  market,
  ROUND(SUM(sales_amount), 2) AS total_sales,
  SUM(units_sold) AS total_units,
  SUM(is_high_value_market_row) AS high_value_rows
FROM {silver_sales_table}
GROUP BY report_day, market
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_brand_summary} AS
SELECT
  report_day,
  brand,
  ROUND(SUM(sales_amount), 2) AS total_sales,
  SUM(units_sold) AS total_units
FROM {silver_sales_table}
GROUP BY report_day, brand
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {brand_view} AS
SELECT
  report_day,
  brand,
  total_sales,
  total_units
FROM {gold_brand_summary}
""")

print("Created gold table:", gold_market_summary)
print("Created gold table:", gold_brand_summary)
print("Created view:", brand_view)

display(spark.table(gold_market_summary))
display(spark.table(gold_brand_summary))
