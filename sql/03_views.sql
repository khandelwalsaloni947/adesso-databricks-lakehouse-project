CREATE OR REPLACE VIEW adesso_dev.analytics.vw_brand_sales_summary AS
SELECT
  report_day,
  brand,
  total_sales,
  total_units
FROM adesso_dev.analytics.gold_brand_summary;
