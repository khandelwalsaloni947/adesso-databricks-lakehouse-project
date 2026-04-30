from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_business_flags(df: DataFrame, high_value_threshold: float = 10000) -> DataFrame:
    return (
        df
        .withColumn(
            "is_high_value_market_row",
            F.when(F.col("sales_amount") >= high_value_threshold, 1).otherwise(0)
        )
    )
