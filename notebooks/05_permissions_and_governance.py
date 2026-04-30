# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Permissions and Governance
# MAGIC
# MAGIC ## Goal
# MAGIC - run sample grants
# MAGIC - inspect grants
# MAGIC - connect governance to project structure

# COMMAND ----------
spark.sql("GRANT USE CATALOG ON CATALOG adesso_dev TO `users`")
spark.sql("GRANT USE SCHEMA ON SCHEMA adesso_dev.analytics TO `users`")
spark.sql("GRANT SELECT ON TABLE adesso_dev.analytics.gold_market_summary TO `users`")
spark.sql("GRANT SELECT ON VIEW adesso_dev.analytics.vw_brand_sales_summary TO `users`")
spark.sql("GRANT READ VOLUME ON VOLUME adesso_dev.raw.landing TO `users`")

print("Sample grants executed.")

# COMMAND ----------
display(spark.sql("SHOW GRANTS ON CATALOG adesso_dev"))
display(spark.sql("SHOW GRANTS ON SCHEMA adesso_dev.analytics"))
display(spark.sql("SHOW GRANTS ON TABLE adesso_dev.analytics.gold_market_summary"))
display(spark.sql("SHOW GRANTS ON VIEW adesso_dev.analytics.vw_brand_sales_summary"))
display(spark.sql("SHOW GRANTS ON VOLUME adesso_dev.raw.landing"))

