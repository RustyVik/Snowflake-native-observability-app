# Rollback Checklist (Sprint 4)

## Trigger Conditions
- Migration integrity validation returns `FAIL`
- Critical P0 UAT scenario fails with no immediate mitigation
- Guardrail enforcement blocks approved production flows unexpectedly

## Rollback Steps
1. Pause user-facing operations and document incident context.
2. Capture diagnostics before rollback:
   - `CALL APP_CORE.sp_export_diagnostics(6);`
3. Snapshot critical tables (`APP_CORE.app_versions`, `APP_AUDIT.cortex_call_audit`, `APP_ENGINE.incidents`, `APP_ENGINE.remediation_tasks`).
4. Revert app package to previously validated version.
5. Re-run baseline health checks:
   - `CALL APP_CORE.sp_healthcheck();`
6. Execute targeted smoke checks for setup, gating, and incident/remediation lifecycle.

## Validation After Rollback
- Confirm active version in `APP_CORE.app_versions` is expected rollback target.
- Confirm no new row-count regressions in critical tables.
- Confirm `sp_assert_ready_for_activation()` and guardrails behave as expected.

## Communication
- Publish rollback summary with timestamp, cause, and impacted components.
- Record follow-up actions and owner for remediation before next release attempt.
