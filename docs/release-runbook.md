# Release Runbook (Sprint 4)

## Preconditions
- All scripts are applied in order: `001_init.sql`, `002_cortex_udfs.sql`, `003_monitoring.sql`, `004_remediation.sql`
- `APP_CORE.sp_healthcheck()` returns `HEALTHY`
- Guardrails and allowlist are configured for target environment

## Release Steps
1. Capture migration baseline:
   - `CALL APP_CORE.sp_capture_migration_baseline();`
2. Run pre-release diagnostics:
   - `CALL APP_CORE.sp_export_diagnostics(24);`
3. Apply upgrade:
   - `CALL APP_CORE.sp_upgrade_app();`
4. Validate migration integrity:
   - `CALL APP_CORE.sp_validate_migration_integrity(NULL);`
5. Run smoke checks:
   - `tests/sql/smoke_checks.sql`

## Post-Release Verification
- Confirm `G10`: P0 UAT scenarios complete successfully
- Confirm `G11`: disallowed model and budget exceed checks are blocked
- Confirm `G12`: migration integrity returns `PASS` with zero row-count regressions

## Operational Handoff
- Share diagnostics output with operations and governance teams
- Confirm monitoring tasks exist and are resumable in target account
- Confirm remediation summary procedure returns expected status counts
