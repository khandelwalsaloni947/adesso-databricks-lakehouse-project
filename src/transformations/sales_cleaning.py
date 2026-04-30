from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def standardize_sales_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("market", F.upper(F.trim(F.col("market"))))
        .withColumn("brand", F.upper(F.trim(F.col("brand"))))
        .withColumn("product_category", F.upper(F.trim(F.col("product_category"))))
        .withColumn("sales_amount", F.col("sales_amount").cast("double"))
        .withColumn("units_sold", F.col("units_sold").cast("int"))
        .withColumn("inventory_level", F.col("inventory_level").cast("int"))
        .withColumn("report_day", F.to_date("report_date"))
    )

def filter_invalid_sales_rows(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("report_day").isNotNull())
        .filter(F.col("market").isNotNull())
        .filter(F.col("brand").isNotNull())
        .filter(F.col("sales_amount").isNotNull())
        .filter(F.col("units_sold").isNotNull())
        .filter(F.col("sales_amount") >= 0)
        .filter(F.col("units_sold") >= 0)
    )
