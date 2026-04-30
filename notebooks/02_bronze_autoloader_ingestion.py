# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Auto Loader Ingestion
# MAGIC
# MAGIC ## Goal
# MAGIC - copy raw sales files into a governed landing volume
# MAGIC - ingest sales files into a bronze Delta table using Auto Loader

# COMMAND ----------
import yaml
from pyspark.sql import functions as F

config_path = "/Workspace/Repos/your-user/adesso-week8-day4-mini-project/config/project_config.yml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

catalog_name = config["catalog"]
raw_schema = config["schemas"]["raw"]
landing_volume = config["volumes"]["landing"]
checkpoint_volume = config["volumes"]["checkpoints"]

sales_subpath = config["paths"]["sales_subpath"]
schema_subpath = config["paths"]["schema_tracking_subpath"]
checkpoint_subpath = config["paths"]["checkpoints_subpath"]

landing_sales_path = f"/Volumes/{catalog_name}/{raw_schema}/{landing_volume}/{sales_subpath}"
schema_tracking_path = f"/Volumes/{catalog_name}/{raw_schema}/{checkpoint_volume}/{schema_subpath}/sales"
checkpoint_path = f"/Volumes/{catalog_name}/{raw_schema}/{checkpoint_volume}/{checkpoint_subpath}/sales"
bronze_sales_table = f"{catalog_name}.{raw_schema}.{config['tables']['bronze_sales']}"

print("Landing sales path:", landing_sales_path)
print("Schema tracking path:", schema_tracking_path)
print("Checkpoint path:", checkpoint_path)
print("Bronze table:", bronze_sales_table)

# COMMAND ----------
repo_sample_path = "file:/Workspace/Repos/your-user/adesso-week8-day4-mini-project/sample_data"
dbutils.fs.mkdirs(landing_sales_path)

for file_name in ["raw_sales_batch_1.csv", "raw_sales_batch_2.csv", "raw_sales_batch_3.csv", "raw_sales_batch_4.csv"]:
    try:
        dbutils.fs.cp(f"{repo_sample_path}/{file_name}", f"{landing_sales_path}/{file_name}")
    except Exception as e:
        print("Copy skipped or failed:", e)

display(dbutils.fs.ls(landing_sales_path))

# COMMAND ----------
bronze_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", schema_tracking_path)
    .load(landing_sales_path)
)

# COMMAND ----------
query = (
    bronze_stream_df
    .withColumn("ingestion_ts", F.current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(bronze_sales_table)
)

query.awaitTermination()
print("Bronze ingestion complete.")
display(spark.table(bronze_sales_table))

