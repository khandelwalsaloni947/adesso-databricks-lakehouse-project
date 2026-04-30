CREATE CATALOG IF NOT EXISTS adesso_dev;

CREATE SCHEMA IF NOT EXISTS adesso_dev.raw;
CREATE SCHEMA IF NOT EXISTS adesso_dev.refined;
CREATE SCHEMA IF NOT EXISTS adesso_dev.analytics;

CREATE VOLUME IF NOT EXISTS adesso_dev.raw.landing;
CREATE VOLUME IF NOT EXISTS adesso_dev.raw.checkpoints;
