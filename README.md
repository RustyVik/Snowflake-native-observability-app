# Snowflake Native Observability App

Cortex-first Snowflake Native App scaffold for Data Quality + Observability parity.

## Scope

- Native App lifecycle (`setup`, `upgrade`, `healthcheck`)
- Cortex-backed DQ UDF library (no non-Cortex fallback)
- Profiling + classification foundations
- Monitoring + anomaly + incident scaffolding
- Streamlit in Snowflake app shell

## Project Structure

- `native_app/manifest.yml` - Native App package manifest
- `native_app/setup.sql` - App setup entrypoint
- `native_app/scripts/` - Versioned SQL objects and procedures
- `native_app/streamlit/` - Streamlit app and page placeholders
- `docs/architecture.md` - Architecture and dependency notes
- `delivery-plan.md` - Sprint 1-4 plan with Cortex dependency gates
- `tests/sql/smoke_checks.sql` - Basic post-deploy checks

## Quick Start (Scaffold)

1. Create app package from `native_app/`
2. Run `setup.sql` in provider packaging workflow
3. Execute scripts in order: `001_init.sql`, `002_cortex_udfs.sql`, `003_monitoring.sql`, `004_remediation.sql`
4. Run lifecycle setup and gate checks:
	- `CALL APP_CORE.sp_setup_app();`
	- `CALL APP_CORE.sp_validate_cortex_access();`
	- `CALL APP_CORE.sp_assert_ready_for_activation();`
	- `CALL APP_CORE.sp_healthcheck();`
5. Validate Sprint 2 rule governance and preview:
	- `CALL APP_DQ.sp_preview_rule_result('udf_dq_email_valid', 'user@example.com');`
	- `CALL APP_DQ.sp_apply_rule_pack('baseline_pack', 'RAW.CUSTOMER');`
6. Validate Sprint 3 profiling/monitoring lifecycle:
	- `CALL APP_ENGINE.sp_profile_dataset('APP_EVENTS', 'TMP_PROFILE_INPUT');`
	- `CALL APP_ENGINE.sp_classify_columns_cortex(NULL);`
	- `CALL APP_ENGINE.sp_inject_synthetic_anomaly('SMOKE_SYNTHETIC', 0.95);`
	- `CALL APP_ENGINE.sp_run_monitoring_cycle('manual_validation_cycle');`
7. Validate Sprint 4 remediation + admin controls:
	- `CALL APP_ENGINE.sp_create_task_for_latest_incident('SMOKE_SYNTHETIC', 'Investigate anomaly', 'oncall_user');`
	- `CALL APP_ENGINE.sp_update_latest_task_for_source('SMOKE_SYNTHETIC', 'IN_PROGRESS', 'Investigation started');`
	- `CALL APP_CORE.sp_export_diagnostics(24);`
	- `CALL APP_CORE.sp_enforce_cortex_execution('model-not-allowlisted', 100, 'manual_check', OBJECT_CONSTRUCT());`
8. Run `tests/sql/smoke_checks.sql`

## Release Operations

- Release runbook: `docs/release-runbook.md`
- Rollback checklist: `docs/rollback-checklist.md`
- Single ordered deployment script: `deploy_all.sql` (SnowSQL/CLI)

## GitHub Actions CD Prerequisites

The workflow at `.github/workflows/cd-deploy.yml` requires these repository **Secrets**:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`

Optional repository **Variables** for environment overrides:

- `SNOWFLAKE_DATABASE_STAGING` (default: `NATIVE_OBS`)
- `SNOWFLAKE_SCHEMA_STAGING` (default: `PROD`)
- `SNOWFLAKE_DATABASE_PROD` (default: `NATIVE_OBS`)
- `SNOWFLAKE_SCHEMA_PROD` (default: `PROD`)

If Variables are not set, workflow defaults are used.

## Notes

This project is now implemented through Sprint 4 baseline and can be extended for production hardening per environment.
