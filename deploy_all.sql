-- deploy_all.sql
-- Single ordered deployment script for SnowSQL/Snowflake CLI.
--
-- Usage example:
--   snowsql -f deploy_all.sql
--
-- Notes:
-- - This script uses SnowSQL meta-commands (`!source`).
-- - Run from repository root so relative paths resolve.
-- - Requires privileges to create/replace schemas, procedures, functions, and tasks.

!set variable_substitution=true

!print ======================================================================
!print STEP 1: Create schemas and version tracking
!print ======================================================================

CREATE SCHEMA IF NOT EXISTS APP_CORE;
CREATE SCHEMA IF NOT EXISTS APP_DQ;
CREATE SCHEMA IF NOT EXISTS APP_EVENTS;
CREATE SCHEMA IF NOT EXISTS APP_ENGINE;
CREATE SCHEMA IF NOT EXISTS APP_AUDIT;

CREATE TABLE IF NOT EXISTS APP_CORE.app_versions (
  version STRING,
  applied_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  applied_by STRING DEFAULT CURRENT_USER()
);

INSERT INTO APP_CORE.app_versions(version) SELECT '0.1.0'
WHERE NOT EXISTS (SELECT 1 FROM APP_CORE.app_versions WHERE version = '0.1.0');

!print ======================================================================
!print STEP 2: Sprint 1 foundation (core + readiness + lifecycle)
!print ======================================================================
!source native_app/scripts/001_init.sql

!print ======================================================================
!print STEP 3: Sprint 2 rule governance + Cortex UDF library
!print ======================================================================
!source native_app/scripts/002_cortex_udfs.sql

!print ======================================================================
!print STEP 4: Sprint 3 profiling + monitoring + incident engine
!print ======================================================================
!source native_app/scripts/003_monitoring.sql

!print ======================================================================
!print STEP 5: Sprint 4 remediation + release controls
!print ======================================================================
!source native_app/scripts/004_remediation.sql

!print ======================================================================
!print STEP 6: Full smoke checks (G1-G12 coverage)
!print ======================================================================
!source tests/sql/smoke_checks.sql

!print ======================================================================
!print STEP 7: Release verification calls
!print ======================================================================

-- Operational health + diagnostics
CALL APP_CORE.sp_healthcheck();
CALL APP_CORE.sp_export_diagnostics(24);

-- Migration safety validation (capture baseline, run upgrade, verify integrity)
CALL APP_CORE.sp_capture_migration_baseline();
CALL APP_CORE.sp_upgrade_app();
CALL APP_CORE.sp_validate_migration_integrity(NULL);

-- Final remediation/incident operational summary
CALL APP_ENGINE.sp_get_remediation_summary();

!print ======================================================================
!print DEPLOYMENT COMPLETE
!print ======================================================================
