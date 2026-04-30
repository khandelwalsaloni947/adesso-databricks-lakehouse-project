-- Example privileges for a hypothetical analyst user or group
GRANT USE CATALOG ON CATALOG adesso_dev TO `users`;
GRANT USE SCHEMA ON SCHEMA adesso_dev.analytics TO `users`;
GRANT SELECT ON TABLE adesso_dev.analytics.gold_market_summary TO `users`;
GRANT SELECT ON VIEW adesso_dev.analytics.vw_brand_sales_summary TO `users`;

GRANT READ VOLUME ON VOLUME adesso_dev.raw.landing TO `users`;
-- Only grant WRITE VOLUME where appropriate in your environment
-- GRANT WRITE VOLUME ON VOLUME adesso_dev.raw.landing TO `users`;

SHOW GRANTS ON CATALOG adesso_dev;
SHOW GRANTS ON SCHEMA adesso_dev.analytics;
SHOW GRANTS ON TABLE adesso_dev.analytics.gold_market_summary;
SHOW GRANTS ON VIEW adesso_dev.analytics.vw_brand_sales_summary;
SHOW GRANTS ON VOLUME adesso_dev.raw.landing;
