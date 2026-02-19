-- Setup entrypoint for Snowflake Native App scaffold

-- Application role for consumer access
CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;

CREATE SCHEMA IF NOT EXISTS APP_CORE;
CREATE SCHEMA IF NOT EXISTS APP_DQ;
CREATE SCHEMA IF NOT EXISTS APP_EVENTS;
CREATE SCHEMA IF NOT EXISTS APP_ENGINE;
CREATE SCHEMA IF NOT EXISTS APP_AUDIT;

-- Version tracking
CREATE TABLE IF NOT EXISTS APP_CORE.app_versions (
  version STRING,
  applied_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  applied_by STRING DEFAULT CURRENT_USER()
);

-- Install baseline
INSERT INTO APP_CORE.app_versions(version) SELECT '0.1.0'
WHERE NOT EXISTS (SELECT 1 FROM APP_CORE.app_versions WHERE version = '0.1.0');

-- Streamlit dashboard
CREATE OR REPLACE STREAMLIT APP_CORE.observability_dashboard
  FROM '/streamlit'
  MAIN_FILE = 'Home.py';

GRANT USAGE ON STREAMLIT APP_CORE.observability_dashboard TO APPLICATION ROLE APP_PUBLIC;

-- Load scripted objects
-- In package workflow, execute scripts in order:
-- 1) scripts/001_init.sql
-- 2) scripts/002_cortex_udfs.sql
-- 3) scripts/003_monitoring.sql
-- 4) scripts/004_remediation.sql
