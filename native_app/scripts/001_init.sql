-- 001_init.sql

USE SCHEMA APP_CORE;

CREATE TABLE IF NOT EXISTS app_config (
  key STRING,
  value VARIANT,
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_by STRING DEFAULT CURRENT_USER()
);

CREATE TABLE IF NOT EXISTS model_allowlist (
  model_name STRING PRIMARY KEY,
  is_enabled BOOLEAN DEFAULT TRUE,
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS cost_guardrails (
  guardrail_name STRING PRIMARY KEY,
  threshold NUMBER,
  unit STRING,
  is_enabled BOOLEAN DEFAULT TRUE,
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO app_config tgt
USING (
  SELECT 'app_status' AS key, PARSE_JSON('"INSTALLING"') AS value
  UNION ALL
  SELECT 'activation_required_checks', PARSE_JSON('["cortex_access","allowlist","guardrails"]')
  UNION ALL
  SELECT 'enforce_activation_gate', PARSE_JSON('true')
) src
ON tgt.key = src.key
WHEN MATCHED THEN UPDATE SET value = src.value, updated_at = CURRENT_TIMESTAMP(), updated_by = CURRENT_USER()
WHEN NOT MATCHED THEN INSERT (key, value) VALUES (src.key, src.value);

MERGE INTO model_allowlist tgt
USING (
  SELECT 'snowflake-arctic' AS model_name, TRUE AS is_enabled
  UNION ALL
  SELECT 'llama3.1-70b', TRUE
) src
ON tgt.model_name = src.model_name
WHEN MATCHED THEN UPDATE SET is_enabled = src.is_enabled, updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (model_name, is_enabled) VALUES (src.model_name, src.is_enabled);

MERGE INTO cost_guardrails tgt
USING (
  SELECT 'daily_token_budget' AS guardrail_name, 1000000 AS threshold, 'TOKENS' AS unit, TRUE AS is_enabled
  UNION ALL
  SELECT 'monthly_token_budget', 30000000, 'TOKENS', TRUE
) src
ON tgt.guardrail_name = src.guardrail_name
WHEN MATCHED THEN UPDATE SET threshold = src.threshold, unit = src.unit, is_enabled = src.is_enabled, updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (guardrail_name, threshold, unit, is_enabled)
VALUES (src.guardrail_name, src.threshold, src.unit, src.is_enabled);

USE SCHEMA APP_AUDIT;

CREATE TABLE IF NOT EXISTS cortex_call_audit (
  event_id STRING DEFAULT UUID_STRING(),
  rule_name STRING,
  model_name STRING,
  call_status STRING,
  invoked_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  invoked_by STRING DEFAULT CURRENT_USER(),
  token_estimate NUMBER,
  metadata VARIANT
);

CREATE TABLE IF NOT EXISTS migration_validation_audit (
  run_id STRING,
  phase STRING,
  table_name STRING,
  row_count NUMBER,
  captured_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  captured_by STRING DEFAULT CURRENT_USER()
);

USE SCHEMA APP_CORE;

CREATE OR REPLACE PROCEDURE sp_validate_cortex_access()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  allowlist_enabled_count NUMBER DEFAULT 0;
  guardrail_enabled_count NUMBER DEFAULT 0;
  enforce_gate BOOLEAN DEFAULT TRUE;
  all_checks_pass BOOLEAN DEFAULT FALSE;
  status STRING;
  result VARIANT;
BEGIN
  SELECT COUNT(*)
    INTO :allowlist_enabled_count
  FROM APP_CORE.model_allowlist
  WHERE is_enabled = TRUE;

  SELECT COUNT(*)
    INTO :guardrail_enabled_count
  FROM APP_CORE.cost_guardrails
  WHERE is_enabled = TRUE;

  SELECT COALESCE(TRY_TO_BOOLEAN(value), TRUE)
    INTO :enforce_gate
  FROM APP_CORE.app_config
  WHERE key = 'enforce_activation_gate';

  all_checks_pass := allowlist_enabled_count > 0 AND guardrail_enabled_count > 0;
  status := IFF(all_checks_pass, 'PASS', 'FAIL');

  result := OBJECT_CONSTRUCT(
    'status', status,
    'enforce_activation_gate', enforce_gate,
    'checks', ARRAY_CONSTRUCT(
      OBJECT_CONSTRUCT(
        'name','model_allowlist_enabled',
        'status', IFF(allowlist_enabled_count > 0, 'PASS', 'FAIL'),
        'details', OBJECT_CONSTRUCT('enabled_models', allowlist_enabled_count)
      ),
      OBJECT_CONSTRUCT(
        'name','guardrails_enabled',
        'status', IFF(guardrail_enabled_count > 0, 'PASS', 'FAIL'),
        'details', OBJECT_CONSTRUCT('enabled_guardrails', guardrail_enabled_count)
      )
    )
  );
  RETURN result;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_assert_ready_for_activation()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  gate_result VARIANT;
  gate_status STRING;
  enforce_gate BOOLEAN;
BEGIN
  gate_result := (CALL APP_CORE.sp_validate_cortex_access());
  gate_status := gate_result:"status"::STRING;
  enforce_gate := COALESCE(gate_result:"enforce_activation_gate"::BOOLEAN, TRUE);

  IF enforce_gate AND gate_status <> 'PASS' THEN
    RETURN OBJECT_CONSTRUCT(
      'status', 'BLOCKED',
      'reason', 'CORTEX_PREREQUISITES_FAILED',
      'gate_result', gate_result
    );
  END IF;

  RETURN OBJECT_CONSTRUCT(
    'status', 'READY',
    'gate_result', gate_result
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_record_cortex_call_audit(
  rule_name STRING,
  model_name STRING,
  token_estimate NUMBER,
  call_status STRING,
  metadata VARIANT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  new_event_id STRING;
BEGIN
  new_event_id := UUID_STRING();

  INSERT INTO APP_AUDIT.cortex_call_audit (
    event_id,
    rule_name,
    model_name,
    call_status,
    token_estimate,
    metadata
  )
  VALUES (
    :new_event_id,
    :rule_name,
    :model_name,
    COALESCE(:call_status, 'SIMULATED'),
    :token_estimate,
    COALESCE(:metadata, OBJECT_CONSTRUCT('source','sp_record_cortex_call_audit'))
  );

  RETURN :new_event_id;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_healthcheck()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  gate_result VARIANT;
  schema_count NUMBER DEFAULT 0;
  audit_events_24h NUMBER DEFAULT 0;
BEGIN
  gate_result := (CALL APP_CORE.sp_validate_cortex_access());

  SELECT COUNT(*)
    INTO :schema_count
  FROM INFORMATION_SCHEMA.SCHEMATA
  WHERE SCHEMA_NAME IN ('APP_CORE', 'APP_DQ', 'APP_EVENTS', 'APP_ENGINE', 'APP_AUDIT');

  SELECT COUNT(*)
    INTO :audit_events_24h
  FROM APP_AUDIT.cortex_call_audit
  WHERE invoked_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

  RETURN OBJECT_CONSTRUCT(
    'status', IFF(schema_count = 5 AND gate_result:"status"::STRING = 'PASS', 'HEALTHY', 'DEGRADED'),
    'schemas_present', schema_count,
    'cortex_gate', gate_result,
    'audit_events_24h', audit_events_24h,
    'checked_at', CURRENT_TIMESTAMP()
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_setup_app()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  activation_result VARIANT;
BEGIN
  MERGE INTO APP_CORE.app_config tgt
  USING (
    SELECT 'app_status' AS key, PARSE_JSON('"SETUP_COMPLETE"') AS value
  ) src
  ON tgt.key = src.key
  WHEN MATCHED THEN UPDATE SET value = src.value, updated_at = CURRENT_TIMESTAMP(), updated_by = CURRENT_USER()
  WHEN NOT MATCHED THEN INSERT (key, value) VALUES (src.key, src.value);

  activation_result := (CALL APP_CORE.sp_assert_ready_for_activation());

  RETURN OBJECT_CONSTRUCT(
    'status', IFF(activation_result:"status"::STRING = 'READY', 'SUCCESS', 'PARTIAL'),
    'activation', activation_result,
    'next_step', 'Run APP_CORE.sp_healthcheck() and tests/sql/smoke_checks.sql'
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_upgrade_app()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  next_version STRING DEFAULT '0.1.1';
  health VARIANT;
BEGIN
  INSERT INTO APP_CORE.app_versions(version)
  SELECT :next_version
  WHERE NOT EXISTS (SELECT 1 FROM APP_CORE.app_versions WHERE version = :next_version);

  MERGE INTO APP_CORE.app_config tgt
  USING (
    SELECT 'app_status' AS key, PARSE_JSON('"UPGRADE_COMPLETE"') AS value
  ) src
  ON tgt.key = src.key
  WHEN MATCHED THEN UPDATE SET value = src.value, updated_at = CURRENT_TIMESTAMP(), updated_by = CURRENT_USER()
  WHEN NOT MATCHED THEN INSERT (key, value) VALUES (src.key, src.value);

  health := (CALL APP_CORE.sp_healthcheck());

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'version', next_version,
    'health', health
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_enforce_cortex_execution(
  model_name STRING,
  token_estimate NUMBER,
  action_name STRING,
  metadata VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  normalized_model STRING;
  estimated_tokens NUMBER;
  is_allowed BOOLEAN DEFAULT FALSE;
  daily_budget NUMBER DEFAULT NULL;
  monthly_budget NUMBER DEFAULT NULL;
  consumed_today NUMBER DEFAULT 0;
  consumed_month NUMBER DEFAULT 0;
  blocked_reason STRING DEFAULT NULL;
  status STRING DEFAULT 'ALLOW';
BEGIN
  normalized_model := LOWER(COALESCE(:model_name, ''));
  estimated_tokens := COALESCE(:token_estimate, 0);

  SELECT COUNT(*) > 0
    INTO :is_allowed
  FROM APP_CORE.model_allowlist
  WHERE LOWER(model_name) = :normalized_model
    AND is_enabled = TRUE;

  SELECT MAX(threshold)
    INTO :daily_budget
  FROM APP_CORE.cost_guardrails
  WHERE guardrail_name = 'daily_token_budget'
    AND is_enabled = TRUE;

  SELECT MAX(threshold)
    INTO :monthly_budget
  FROM APP_CORE.cost_guardrails
  WHERE guardrail_name = 'monthly_token_budget'
    AND is_enabled = TRUE;

  SELECT COALESCE(SUM(token_estimate), 0)
    INTO :consumed_today
  FROM APP_AUDIT.cortex_call_audit
  WHERE DATE(invoked_at) = CURRENT_DATE()
    AND COALESCE(call_status, 'UNKNOWN') NOT LIKE 'BLOCKED%';

  SELECT COALESCE(SUM(token_estimate), 0)
    INTO :consumed_month
  FROM APP_AUDIT.cortex_call_audit
  WHERE DATE_TRUNC('month', invoked_at) = DATE_TRUNC('month', CURRENT_DATE())
    AND COALESCE(call_status, 'UNKNOWN') NOT LIKE 'BLOCKED%';

  IF NOT is_allowed THEN
    status := 'BLOCKED';
    blocked_reason := 'MODEL_NOT_ALLOWLISTED';
  ELSEIF daily_budget IS NOT NULL AND (consumed_today + estimated_tokens) > daily_budget THEN
    status := 'BLOCKED';
    blocked_reason := 'DAILY_BUDGET_EXCEEDED';
  ELSEIF monthly_budget IS NOT NULL AND (consumed_month + estimated_tokens) > monthly_budget THEN
    status := 'BLOCKED';
    blocked_reason := 'MONTHLY_BUDGET_EXCEEDED';
  END IF;

  IF status = 'BLOCKED' THEN
    INSERT INTO APP_AUDIT.cortex_call_audit(
      rule_name,
      model_name,
      call_status,
      token_estimate,
      metadata
    )
    VALUES (
      COALESCE(:action_name, 'unknown_action'),
      :model_name,
      CONCAT('BLOCKED_', blocked_reason),
      :estimated_tokens,
      OBJECT_INSERT(COALESCE(:metadata, OBJECT_CONSTRUCT()), 'blocked_reason', blocked_reason, TRUE)
    );

    RETURN OBJECT_CONSTRUCT(
      'status', 'BLOCKED',
      'reason', blocked_reason,
      'model', :model_name,
      'consumed_today', consumed_today,
      'consumed_month', consumed_month,
      'daily_budget', daily_budget,
      'monthly_budget', monthly_budget
    );
  END IF;

  RETURN OBJECT_CONSTRUCT(
    'status', 'ALLOW',
    'model', :model_name,
    'consumed_today', consumed_today,
    'consumed_month', consumed_month,
    'daily_budget', daily_budget,
    'monthly_budget', monthly_budget,
    'remaining_daily', IFF(daily_budget IS NULL, NULL, daily_budget - consumed_today),
    'remaining_monthly', IFF(monthly_budget IS NULL, NULL, monthly_budget - consumed_month)
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_export_diagnostics(hours_back NUMBER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  lookback NUMBER DEFAULT COALESCE(:hours_back, 24);
  total_calls NUMBER DEFAULT 0;
  blocked_calls NUMBER DEFAULT 0;
  total_tokens NUMBER DEFAULT 0;
  model_summary VARIANT;
BEGIN
  SELECT COUNT(*),
         COUNT_IF(COALESCE(call_status, 'UNKNOWN') LIKE 'BLOCKED%'),
         COALESCE(SUM(token_estimate), 0)
    INTO :total_calls, :blocked_calls, :total_tokens
  FROM APP_AUDIT.cortex_call_audit
  WHERE invoked_at >= DATEADD('hour', -lookback, CURRENT_TIMESTAMP());

  SELECT COALESCE(
           ARRAY_AGG(OBJECT_CONSTRUCT('model', model_name, 'calls', calls, 'tokens', tokens)),
           ARRAY_CONSTRUCT()
         )
    INTO :model_summary
  FROM (
    SELECT model_name,
           COUNT(*) AS calls,
           COALESCE(SUM(token_estimate), 0) AS tokens
    FROM APP_AUDIT.cortex_call_audit
    WHERE invoked_at >= DATEADD('hour', -lookback, CURRENT_TIMESTAMP())
    GROUP BY model_name
    ORDER BY calls DESC
    LIMIT 20
  );

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'hours_back', lookback,
    'total_calls', total_calls,
    'blocked_calls', blocked_calls,
    'total_tokens', total_tokens,
    'models', model_summary
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_capture_migration_baseline()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  run_id STRING DEFAULT UUID_STRING();
BEGIN
  INSERT INTO APP_AUDIT.migration_validation_audit(run_id, phase, table_name, row_count)
  SELECT :run_id, 'PRE_UPGRADE', 'APP_CORE.APP_VERSIONS', COUNT(*) FROM APP_CORE.app_versions
  UNION ALL
  SELECT :run_id, 'PRE_UPGRADE', 'APP_AUDIT.CORTEX_CALL_AUDIT', COUNT(*) FROM APP_AUDIT.cortex_call_audit
  UNION ALL
  SELECT :run_id, 'PRE_UPGRADE', 'APP_DQ.DQ_RULE_LIBRARY', COUNT(*) FROM APP_DQ.dq_rule_library
  UNION ALL
  SELECT :run_id, 'PRE_UPGRADE', 'APP_ENGINE.INCIDENTS', COUNT(*) FROM APP_ENGINE.incidents;

  RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'baseline_run_id', run_id);
END;
$$;

CREATE OR REPLACE PROCEDURE sp_validate_migration_integrity(baseline_run_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  baseline_id STRING;
  validation_run_id STRING DEFAULT UUID_STRING();
  regression_count NUMBER DEFAULT 0;
BEGIN
  baseline_id := COALESCE(
    :baseline_run_id,
    (
      SELECT run_id
      FROM APP_AUDIT.migration_validation_audit
      WHERE phase = 'PRE_UPGRADE'
      ORDER BY captured_at DESC
      LIMIT 1
    )
  );

  IF baseline_id IS NULL THEN
    RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'reason', 'NO_BASELINE_FOUND');
  END IF;

  INSERT INTO APP_AUDIT.migration_validation_audit(run_id, phase, table_name, row_count)
  SELECT :validation_run_id, 'POST_UPGRADE', 'APP_CORE.APP_VERSIONS', COUNT(*) FROM APP_CORE.app_versions
  UNION ALL
  SELECT :validation_run_id, 'POST_UPGRADE', 'APP_AUDIT.CORTEX_CALL_AUDIT', COUNT(*) FROM APP_AUDIT.cortex_call_audit
  UNION ALL
  SELECT :validation_run_id, 'POST_UPGRADE', 'APP_DQ.DQ_RULE_LIBRARY', COUNT(*) FROM APP_DQ.dq_rule_library
  UNION ALL
  SELECT :validation_run_id, 'POST_UPGRADE', 'APP_ENGINE.INCIDENTS', COUNT(*) FROM APP_ENGINE.incidents;

  SELECT COUNT(*)
    INTO :regression_count
  FROM APP_AUDIT.migration_validation_audit pre
  JOIN APP_AUDIT.migration_validation_audit post
    ON pre.table_name = post.table_name
  WHERE pre.run_id = :baseline_id
    AND pre.phase = 'PRE_UPGRADE'
    AND post.run_id = :validation_run_id
    AND post.phase = 'POST_UPGRADE'
    AND post.row_count < pre.row_count;

  RETURN OBJECT_CONSTRUCT(
    'status', IFF(regression_count = 0, 'PASS', 'FAIL'),
    'baseline_run_id', baseline_id,
    'validation_run_id', validation_run_id,
    'row_count_regressions', regression_count
  );
END;
$$;
