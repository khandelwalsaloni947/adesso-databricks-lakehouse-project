from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def classify_market_priority(market: str, sales_amount: float) -> str:
    if market is None or sales_amount is None:
        return "UNKNOWN"
    if market == "DE" and sales_amount >= 10000:
        return "CORE"
    if sales_amount >= 8000:
        return "HIGH"
    return "STANDARD"
