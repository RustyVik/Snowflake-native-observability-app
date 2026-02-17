-- Smoke checks for scaffolded objects

SHOW SCHEMAS LIKE 'APP_CORE';
SHOW SCHEMAS LIKE 'APP_DQ';
SHOW SCHEMAS LIKE 'APP_EVENTS';
SHOW SCHEMAS LIKE 'APP_ENGINE';
SHOW SCHEMAS LIKE 'APP_AUDIT';

SHOW PROCEDURES LIKE 'SP_VALIDATE_CORTEX_ACCESS' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_SETUP_APP' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_UPGRADE_APP' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_HEALTHCHECK' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_ASSERT_READY_FOR_ACTIVATION' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_RECORD_CORTEX_CALL_AUDIT' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_ENFORCE_CORTEX_EXECUTION' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_EXPORT_DIAGNOSTICS' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_CAPTURE_MIGRATION_BASELINE' IN SCHEMA APP_CORE;
SHOW PROCEDURES LIKE 'SP_VALIDATE_MIGRATION_INTEGRITY' IN SCHEMA APP_CORE;
SHOW FUNCTIONS LIKE 'UDF_DQ_%' IN SCHEMA APP_DQ;
SHOW PROCEDURES LIKE 'SP_PREVIEW_RULE_RESULT' IN SCHEMA APP_DQ;
SHOW PROCEDURES LIKE 'SP_APPLY_RULE_PACK' IN SCHEMA APP_DQ;
SHOW PROCEDURES LIKE 'SP_PROFILE_DATASET' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_CLASSIFY_COLUMNS_CORTEX' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_APPLY_LABEL_OVERRIDE' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_INJECT_SYNTHETIC_ANOMALY' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_RUN_MONITORING_CYCLE' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_CREATE_REMEDIATION_TASK' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_CREATE_TASK_FOR_LATEST_INCIDENT' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_UPDATE_REMEDIATION_STATUS' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_UPDATE_LATEST_TASK_FOR_SOURCE' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_COMPLETE_REMEDIATION_TASK' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_ESCALATE_OVERDUE_TASKS' IN SCHEMA APP_ENGINE;
SHOW PROCEDURES LIKE 'SP_GET_REMEDIATION_SUMMARY' IN SCHEMA APP_ENGINE;
SHOW TASKS LIKE 'TSK_DQ_EVAL' IN SCHEMA APP_ENGINE;
SHOW TASKS LIKE 'TSK_SIGNAL_ROLLUP' IN SCHEMA APP_ENGINE;
SHOW TASKS LIKE 'TSK_INCIDENT_MANAGER' IN SCHEMA APP_ENGINE;

-- G1: install/upgrade smoke pass
CALL APP_CORE.sp_setup_app();
CALL APP_CORE.sp_upgrade_app();
CALL APP_CORE.sp_healthcheck();

-- G2: activation gate callable and returns structured gate payload
CALL APP_CORE.sp_validate_cortex_access();
CALL APP_CORE.sp_assert_ready_for_activation();

-- G3: audit records generated
CALL APP_CORE.sp_record_cortex_call_audit(
	'smoke_check_rule',
	'snowflake-arctic',
	42,
	'SIMULATED',
	OBJECT_CONSTRUCT('source','tests/sql/smoke_checks.sql')
);

SELECT COUNT(*) AS audit_event_count
FROM APP_AUDIT.cortex_call_audit
WHERE rule_name = 'smoke_check_rule'
	AND invoked_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP());

-- G4: all Sprint 2 UDFs include SNOWFLAKE.CORTEX invocation path
SELECT
	POSITION('SNOWFLAKE.CORTEX.' IN GET_DDL('FUNCTION', 'APP_DQ.UDF_DQ_EMAIL_VALID(STRING)')) > 0 AS email_uses_cortex,
	POSITION('SNOWFLAKE.CORTEX.' IN GET_DDL('FUNCTION', 'APP_DQ.UDF_DQ_PHONE_VALID(STRING)')) > 0 AS phone_uses_cortex,
	POSITION('SNOWFLAKE.CORTEX.' IN GET_DDL('FUNCTION', 'APP_DQ.UDF_DQ_NAME_QUALITY(STRING)')) > 0 AS name_uses_cortex,
	POSITION('SNOWFLAKE.CORTEX.' IN GET_DDL('FUNCTION', 'APP_DQ.UDF_DQ_ADDRESS_QUALITY(STRING)')) > 0 AS address_uses_cortex,
	POSITION('SNOWFLAKE.CORTEX.' IN GET_DDL('FUNCTION', 'APP_DQ.UDF_DQ_PII_DETECT(STRING)')) > 0 AS pii_uses_cortex,
	POSITION('SNOWFLAKE.CORTEX.' IN GET_DDL('FUNCTION', 'APP_DQ.UDF_DQ_DOMAIN_CLASSIFY(STRING)')) > 0 AS domain_uses_cortex,
	POSITION('SNOWFLAKE.CORTEX.' IN GET_DDL('FUNCTION', 'APP_DQ.UDF_DQ_REASON_CODE(STRING)')) > 0 AS reason_uses_cortex;

-- G5: standardized payload contract
CALL APP_DQ.sp_preview_rule_result('udf_dq_email_valid', 'user@example.com');
SELECT
	payload:"status" IS NOT NULL AS has_status,
	payload:"confidence" IS NOT NULL AS has_confidence,
	payload:"reason_code" IS NOT NULL AS has_reason_code,
	payload:"model" IS NOT NULL AS has_model
FROM (
	SELECT APP_DQ.udf_dq_phone_valid('+1-415-555-1212') AS payload
);

-- G6: promotion requires approved version
UPDATE APP_DQ.rule_approvals
SET approval_status = 'PENDING', approved_by = NULL, approved_at = NULL
WHERE rule_name = 'udf_dq_email_valid' AND version = 'v1';

CALL APP_DQ.sp_apply_rule_pack('baseline_pack', 'RAW.CUSTOMER');

UPDATE APP_DQ.rule_approvals
SET approval_status = 'APPROVED', approved_by = 'SMOKE_TEST', approved_at = CURRENT_TIMESTAMP()
WHERE rule_name = 'udf_dq_email_valid' AND version = 'v1';

CALL APP_DQ.sp_apply_rule_pack('baseline_pack', 'RAW.CUSTOMER');

SELECT status, promoted_rule_count, promoted_at
FROM APP_DQ.rule_promotion_audit
WHERE pack_name = 'baseline_pack'
ORDER BY promoted_at DESC
LIMIT 5;

-- Sprint 3 setup data for profiling/classification
CREATE OR REPLACE TEMP TABLE APP_EVENTS.tmp_profile_input (
	customer_id NUMBER,
	customer_name STRING,
	customer_email STRING,
	spend_amount FLOAT
);

INSERT INTO APP_EVENTS.tmp_profile_input(customer_id, customer_name, customer_email, spend_amount)
VALUES
	(1, 'Alice Smith', 'alice@example.com', 120.50),
	(2, 'Bob Stone', 'bob@example.com', 95.10),
	(3, NULL, 'charlie@example.com', 201.20);

CALL APP_ENGINE.sp_profile_dataset('APP_EVENTS', 'TMP_PROFILE_INPUT');
CALL APP_ENGINE.sp_classify_columns_cortex(NULL);
CALL APP_ENGINE.sp_apply_label_override(NULL, 'CUSTOMER_EMAIL', 'CONTACT_EMAIL_CONFIRMED', 'smoke_check_override');

-- G7: scheduled runs pass for 3 cycles (validated via monitoring cycle run history)
CALL APP_ENGINE.sp_run_monitoring_cycle('cycle_1');
CALL APP_ENGINE.sp_run_monitoring_cycle('cycle_2');
CALL APP_ENGINE.sp_run_monitoring_cycle('cycle_3');

SELECT COUNT(*) AS completed_cycles
FROM APP_ENGINE.task_run_history
WHERE run_status = 'SUCCESS'
	AND run_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP());

-- G8: synthetic anomaly opens and closes incidents
CALL APP_ENGINE.sp_inject_synthetic_anomaly('SMOKE_SYNTHETIC', 0.95);
CALL APP_ENGINE.sp_detect_anomalies();
CALL APP_ENGINE.sp_open_close_incidents();
CALL APP_ENGINE.sp_resolve_latest_alert('SMOKE_SYNTHETIC');
CALL APP_ENGINE.sp_open_close_incidents();

SELECT status, COUNT(*) AS incident_count
FROM APP_ENGINE.incidents
WHERE payload:"source"::STRING = 'SMOKE_SYNTHETIC'
GROUP BY status;

-- G9: confidence + override traceability
SELECT profile_run_id, column_name, inferred_label, effective_label, confidence, override_applied, override_reason
FROM APP_ENGINE.classification_results
WHERE profile_run_id = (SELECT profile_run_id FROM APP_ENGINE.profile_runs ORDER BY started_at DESC LIMIT 1)
ORDER BY column_name;

SELECT profile_run_id, column_name, old_label, override_label, reason, overridden_by, overridden_at
FROM APP_ENGINE.label_overrides
WHERE profile_run_id = (SELECT profile_run_id FROM APP_ENGINE.profile_runs ORDER BY started_at DESC LIMIT 1)
ORDER BY overridden_at DESC;

SELECT incident_id, old_status, new_status, changed_at
FROM APP_ENGINE.incident_history
WHERE changed_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY changed_at DESC;

-- G10: UAT P0 scenarios (anomaly -> incident -> remediation -> closure)
CALL APP_ENGINE.sp_inject_synthetic_anomaly('UAT_P0', 0.99);
CALL APP_ENGINE.sp_detect_anomalies();
CALL APP_ENGINE.sp_open_close_incidents();
CALL APP_ENGINE.sp_create_task_for_latest_incident('UAT_P0', 'Investigate P0 anomaly', 'p0_oncall');
CALL APP_ENGINE.sp_update_latest_task_for_source('UAT_P0', 'IN_PROGRESS', 'P0 investigation started');
CALL APP_ENGINE.sp_update_latest_task_for_source('UAT_P0', 'COMPLETED', 'P0 remediation completed');
CALL APP_ENGINE.sp_resolve_latest_alert('UAT_P0');
CALL APP_ENGINE.sp_open_close_incidents();

SELECT status, COUNT(*) AS count_by_status
FROM APP_ENGINE.remediation_tasks
WHERE incident_id IN (
	SELECT incident_id
	FROM APP_ENGINE.incidents
	WHERE payload:"source"::STRING = 'UAT_P0'
)
GROUP BY status;

CALL APP_ENGINE.sp_get_remediation_summary();

-- G11: budget/allowlist controls enforce execution limits
CALL APP_CORE.sp_enforce_cortex_execution(
	'model-not-allowlisted',
	100,
	'smoke_guardrail_model_check',
	OBJECT_CONSTRUCT('source', 'tests/sql/smoke_checks.sql')
);

SET old_daily_budget = (
	SELECT threshold
	FROM APP_CORE.cost_guardrails
	WHERE guardrail_name = 'daily_token_budget'
);

UPDATE APP_CORE.cost_guardrails
SET threshold = 1
WHERE guardrail_name = 'daily_token_budget';

CALL APP_CORE.sp_enforce_cortex_execution(
	'snowflake-arctic',
	100,
	'smoke_guardrail_budget_check',
	OBJECT_CONSTRUCT('source', 'tests/sql/smoke_checks.sql')
);

UPDATE APP_CORE.cost_guardrails
SET threshold = $old_daily_budget
WHERE guardrail_name = 'daily_token_budget';

CALL APP_CORE.sp_export_diagnostics(24);

-- G12: upgrade migration has zero data loss
CALL APP_CORE.sp_capture_migration_baseline();
CALL APP_CORE.sp_upgrade_app();
CALL APP_CORE.sp_validate_migration_integrity(NULL);
