SELECT COUNT(*) AS silver_row_count
FROM adesso_dev.refined.silver_sales;

SELECT
  SUM(CASE WHEN market IS NULL THEN 1 ELSE 0 END) AS null_market,
  SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) AS null_brand,
  SUM(CASE WHEN sales_amount < 0 THEN 1 ELSE 0 END) AS negative_sales_amount
FROM adesso_dev.refined.silver_sales;

SELECT market, COUNT(*) AS row_count
FROM adesso_dev.refined.silver_sales
GROUP BY market
ORDER BY market;
