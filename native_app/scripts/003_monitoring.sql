-- 003_monitoring.sql

USE SCHEMA APP_EVENTS;

CREATE TABLE IF NOT EXISTS event_test_outcomes (
  event_id STRING DEFAULT UUID_STRING(),
  source STRING,
  event_ts TIMESTAMP_NTZ,
  ingest_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  payload VARIANT
);

CREATE TABLE IF NOT EXISTS event_metric_log (
  event_id STRING DEFAULT UUID_STRING(),
  source STRING,
  event_ts TIMESTAMP_NTZ,
  ingest_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  payload VARIANT
);

CREATE TABLE IF NOT EXISTS event_run_status (
  event_id STRING DEFAULT UUID_STRING(),
  source STRING,
  event_ts TIMESTAMP_NTZ,
  ingest_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  payload VARIANT
);

CREATE TABLE IF NOT EXISTS event_anomaly_signals (
  event_id STRING DEFAULT UUID_STRING(),
  source STRING,
  signal_name STRING,
  signal_value FLOAT,
  threshold FLOAT,
  is_anomaly BOOLEAN,
  event_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  payload VARIANT
);

USE SCHEMA APP_ENGINE;

CREATE TABLE IF NOT EXISTS alerts (
  alert_id STRING DEFAULT UUID_STRING(),
  source STRING,
  event_ts TIMESTAMP_NTZ,
  signal_name STRING,
  status STRING,
  severity STRING,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  resolved_at TIMESTAMP_NTZ,
  payload VARIANT
);

CREATE TABLE IF NOT EXISTS incidents (
  incident_id STRING DEFAULT UUID_STRING(),
  alert_id STRING,
  status STRING,
  opened_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  closed_at TIMESTAMP_NTZ,
  payload VARIANT
);

CREATE TABLE IF NOT EXISTS incident_history (
  history_id STRING DEFAULT UUID_STRING(),
  incident_id STRING,
  old_status STRING,
  new_status STRING,
  changed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  changed_by STRING DEFAULT CURRENT_USER(),
  notes STRING
);

CREATE TABLE IF NOT EXISTS profile_runs (
  profile_run_id STRING DEFAULT UUID_STRING(),
  target_schema STRING,
  target_table STRING,
  status STRING,
  started_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  completed_at TIMESTAMP_NTZ,
  row_count NUMBER,
  profiled_columns NUMBER,
  error_message STRING,
  metadata VARIANT
);

CREATE TABLE IF NOT EXISTS profile_column_stats (
  profile_run_id STRING,
  column_name STRING,
  data_type STRING,
  ordinal_position NUMBER,
  row_count NUMBER,
  null_count NUMBER,
  null_ratio FLOAT,
  distinct_count NUMBER,
  sample_value STRING,
  profiled_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS classification_results (
  profile_run_id STRING,
  column_name STRING,
  model_name STRING,
  prompt_version STRING,
  inferred_label STRING,
  confidence FLOAT,
  reason_code STRING,
  cortex_response STRING,
  effective_label STRING,
  classified_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  override_applied BOOLEAN DEFAULT FALSE,
  override_reason STRING
);

CREATE TABLE IF NOT EXISTS label_overrides (
  override_id STRING DEFAULT UUID_STRING(),
  profile_run_id STRING,
  column_name STRING,
  old_label STRING,
  override_label STRING,
  reason STRING,
  overridden_by STRING DEFAULT CURRENT_USER(),
  overridden_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS task_run_history (
  run_id STRING DEFAULT UUID_STRING(),
  task_name STRING,
  run_status STRING,
  details VARIANT,
  run_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE sp_profile_dataset(target_schema STRING, target_table STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  run_id STRING DEFAULT UUID_STRING();
  table_ref STRING;
  row_count NUMBER DEFAULT 0;
  profiled_columns NUMBER DEFAULT 0;
  sql_cmd STRING;
  col_row_count NUMBER;
  col_null_count NUMBER;
  col_distinct_count NUMBER;
  col_sample STRING;
BEGIN
  table_ref := '"' || REPLACE(:target_schema, '"', '""') || '"."' || REPLACE(:target_table, '"', '""') || '"';

  INSERT INTO profile_runs(profile_run_id, target_schema, target_table, status)
  VALUES (:run_id, :target_schema, :target_table, 'RUNNING');

  sql_cmd := 'SELECT COUNT(*) FROM ' || table_ref;
  EXECUTE IMMEDIATE :sql_cmd INTO :row_count;

  FOR rec IN (
    SELECT column_name, data_type, ordinal_position
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = UPPER(:target_schema)
      AND TABLE_NAME = UPPER(:target_table)
    ORDER BY ordinal_position
  ) DO
    sql_cmd := 'SELECT COUNT(*), COUNT_IF(' ||
      '"' || REPLACE(rec.column_name, '"', '""') || '" IS NULL), COUNT(DISTINCT ' ||
      '"' || REPLACE(rec.column_name, '"', '""') || '") FROM ' || table_ref;
    EXECUTE IMMEDIATE :sql_cmd INTO :col_row_count, :col_null_count, :col_distinct_count;

    sql_cmd := 'SELECT TO_VARCHAR(' ||
      '"' || REPLACE(rec.column_name, '"', '""') || '") FROM ' || table_ref ||
      ' WHERE ' || '"' || REPLACE(rec.column_name, '"', '""') || '" IS NOT NULL LIMIT 1';
    EXECUTE IMMEDIATE :sql_cmd INTO :col_sample;

    INSERT INTO profile_column_stats(
      profile_run_id,
      column_name,
      data_type,
      ordinal_position,
      row_count,
      null_count,
      null_ratio,
      distinct_count,
      sample_value
    )
    VALUES (
      :run_id,
      rec.column_name,
      rec.data_type,
      rec.ordinal_position,
      :col_row_count,
      :col_null_count,
      IFF(:col_row_count = 0, 0, :col_null_count / :col_row_count),
      :col_distinct_count,
      :col_sample
    );

    profiled_columns := profiled_columns + 1;
  END FOR;

  UPDATE profile_runs
  SET status = 'SUCCESS',
      completed_at = CURRENT_TIMESTAMP(),
      row_count = :row_count,
      profiled_columns = :profiled_columns
  WHERE profile_run_id = :run_id;

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'profile_run_id', :run_id,
    'row_count', :row_count,
    'profiled_columns', :profiled_columns
  );
EXCEPTION
  WHEN OTHER THEN
    UPDATE profile_runs
    SET status = 'FAILED',
        completed_at = CURRENT_TIMESTAMP(),
        error_message = SQLERRM
    WHERE profile_run_id = :run_id;

    RETURN OBJECT_CONSTRUCT(
      'status', 'FAILED',
      'profile_run_id', :run_id,
      'error', SQLERRM
    );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_classify_columns_cortex(profile_run_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  run_id STRING;
  classified_count NUMBER DEFAULT 0;
  inferred_label STRING;
  confidence FLOAT;
  reason_code STRING;
  model_name STRING DEFAULT 'snowflake-arctic';
  prompt_version STRING DEFAULT 'v1';
  token_estimate NUMBER DEFAULT 200;
  gate_result VARIANT;
  audit_event_id STRING;
  prompt STRING;
  response STRING;
BEGIN
  run_id := COALESCE(
    :profile_run_id,
    (SELECT profile_run_id FROM profile_runs ORDER BY started_at DESC LIMIT 1)
  );

  IF run_id IS NULL THEN
    RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'reason', 'NO_PROFILE_RUN_AVAILABLE');
  END IF;

  FOR rec IN (
    SELECT column_name, data_type, sample_value
    FROM profile_column_stats
    WHERE profile_run_id = :run_id
  ) DO
    gate_result := (
      CALL APP_CORE.sp_enforce_cortex_execution(
        :model_name,
        :token_estimate,
        'APP_ENGINE.sp_classify_columns_cortex',
        OBJECT_CONSTRUCT('column_name', rec.column_name, 'profile_run_id', run_id)
      )
    );

    IF gate_result:"status"::STRING = 'BLOCKED' THEN
      INSERT INTO classification_results(
        profile_run_id,
        column_name,
        model_name,
        prompt_version,
        inferred_label,
        confidence,
        reason_code,
        cortex_response,
        effective_label,
        override_applied,
        override_reason
      )
      VALUES (
        :run_id,
        rec.column_name,
        :model_name,
        :prompt_version,
        'BLOCKED',
        0,
        CONCAT('GUARDRAIL_', COALESCE(gate_result:"reason"::STRING, 'BLOCKED')),
        NULL,
        'BLOCKED',
        FALSE,
        NULL
      );

      classified_count := classified_count + 1;
    ELSE
    prompt := CONCAT(
      'Classify this column into domain labels like IDENTIFIER, CONTACT, FINANCIAL, ADDRESS, DEMOGRAPHIC, METRIC, OTHER. ',
      'Column=', rec.column_name,
      '; DataType=', rec.data_type,
      '; Sample=', COALESCE(rec.sample_value, 'NULL')
    );
    response := SNOWFLAKE.CORTEX.COMPLETE(:model_name, :prompt);

    inferred_label :=
      CASE
        WHEN LOWER(rec.column_name) LIKE '%email%' THEN 'CONTACT_EMAIL'
        WHEN LOWER(rec.column_name) LIKE '%phone%' THEN 'CONTACT_PHONE'
        WHEN LOWER(rec.column_name) LIKE '%name%' THEN 'PERSON_NAME'
        WHEN LOWER(rec.column_name) LIKE '%address%' THEN 'ADDRESS'
        WHEN LOWER(rec.column_name) LIKE '%amount%' OR LOWER(rec.column_name) LIKE '%price%' THEN 'FINANCIAL'
        ELSE 'OTHER'
      END;

    confidence :=
      CASE
        WHEN inferred_label = 'OTHER' THEN 0.65
        ELSE 0.85
      END;

    reason_code := IFF(inferred_label = 'OTHER', 'HEURISTIC_FALLBACK', 'HEURISTIC_MATCH');

    INSERT INTO classification_results(
      profile_run_id,
      column_name,
      model_name,
      prompt_version,
      inferred_label,
      confidence,
      reason_code,
      cortex_response,
      effective_label
    )
    VALUES (
      :run_id,
      rec.column_name,
      :model_name,
      :prompt_version,
      :inferred_label,
      :confidence,
      :reason_code,
      :response,
      :inferred_label
    );

    audit_event_id := (
      CALL APP_CORE.sp_record_cortex_call_audit(
        'APP_ENGINE.sp_classify_columns_cortex',
        :model_name,
        :token_estimate,
        'SUCCESS',
        OBJECT_CONSTRUCT('column_name', rec.column_name, 'profile_run_id', run_id)
      )
    );

    classified_count := classified_count + 1;
    END IF;
  END FOR;

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'profile_run_id', :run_id,
    'classified_columns', :classified_count,
    'model', :model_name,
    'prompt_version', :prompt_version
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_apply_label_override(profile_run_id STRING, column_name STRING, override_label STRING, reason STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  run_id STRING;
  previous_label STRING;
BEGIN
  run_id := COALESCE(
    :profile_run_id,
    (SELECT profile_run_id FROM profile_runs ORDER BY started_at DESC LIMIT 1)
  );

  SELECT effective_label
    INTO :previous_label
  FROM classification_results
  WHERE profile_run_id = :run_id
    AND column_name = :column_name
  ORDER BY classified_at DESC
  LIMIT 1;

  IF previous_label IS NULL THEN
    RETURN OBJECT_CONSTRUCT(
      'status', 'FAILED',
      'reason', 'COLUMN_NOT_CLASSIFIED',
      'profile_run_id', :run_id,
      'column_name', :column_name
    );
  END IF;

  INSERT INTO label_overrides(profile_run_id, column_name, old_label, override_label, reason)
  VALUES (:run_id, :column_name, :previous_label, :override_label, :reason);

  UPDATE classification_results
  SET effective_label = :override_label,
      override_applied = TRUE,
      override_reason = :reason
  WHERE profile_run_id = :run_id
    AND column_name = :column_name;

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'profile_run_id', :run_id,
    'column_name', :column_name,
    'old_label', :previous_label,
    'new_label', :override_label
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_detect_anomalies()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  inserted_alerts NUMBER DEFAULT 0;
BEGIN
  INSERT INTO alerts(source, event_ts, signal_name, status, severity, payload)
  SELECT
    source,
    event_ts,
    signal_name,
    'OPEN',
    IFF(signal_value >= threshold * 1.5, 'HIGH', 'MEDIUM'),
    OBJECT_CONSTRUCT(
      'signal_value', signal_value,
      'threshold', threshold,
      'is_anomaly', is_anomaly,
      'event_id', event_id
    )
  FROM APP_EVENTS.event_anomaly_signals s
  WHERE s.is_anomaly = TRUE
    AND NOT EXISTS (
      SELECT 1
      FROM alerts a
      WHERE a.source = s.source
        AND a.event_ts = s.event_ts
        AND a.signal_name = s.signal_name
    );

  inserted_alerts := SQLROWCOUNT;

  INSERT INTO APP_EVENTS.event_run_status(source, event_ts, payload)
  VALUES (
    'APP_ENGINE.sp_detect_anomalies',
    CURRENT_TIMESTAMP(),
    OBJECT_CONSTRUCT('inserted_alerts', inserted_alerts)
  );

  RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'inserted_alerts', inserted_alerts);
END;
$$;

CREATE OR REPLACE PROCEDURE sp_open_close_incidents()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  opened_count NUMBER DEFAULT 0;
  closed_count NUMBER DEFAULT 0;
BEGIN
  INSERT INTO incidents(alert_id, status, payload)
  SELECT
    a.alert_id,
    'OPEN',
    OBJECT_CONSTRUCT('source', a.source, 'severity', a.severity, 'signal_name', a.signal_name)
  FROM alerts a
  LEFT JOIN incidents i
    ON a.alert_id = i.alert_id
   AND i.status IN ('OPEN', 'INVESTIGATING')
  WHERE a.status = 'OPEN'
    AND i.alert_id IS NULL;

  opened_count := SQLROWCOUNT;

  INSERT INTO incident_history(incident_id, old_status, new_status, notes)
  SELECT incident_id, NULL, 'OPEN', 'Auto-opened from active alert'
  FROM incidents
  WHERE opened_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
    AND status = 'OPEN';

  UPDATE incidents i
  SET status = 'CLOSED',
      closed_at = CURRENT_TIMESTAMP(),
      payload = OBJECT_INSERT(i.payload, 'closed_reason', 'ALERT_RESOLVED', TRUE)
  FROM alerts a
  WHERE i.alert_id = a.alert_id
    AND a.status = 'CLOSED'
    AND i.status <> 'CLOSED';

  closed_count := SQLROWCOUNT;

  INSERT INTO incident_history(incident_id, old_status, new_status, notes)
  SELECT incident_id, 'OPEN', 'CLOSED', 'Auto-closed after alert resolution'
  FROM incidents
  WHERE closed_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
    AND status = 'CLOSED';

  INSERT INTO APP_EVENTS.event_run_status(source, event_ts, payload)
  VALUES (
    'APP_ENGINE.sp_open_close_incidents',
    CURRENT_TIMESTAMP(),
    OBJECT_CONSTRUCT('opened_incidents', opened_count, 'closed_incidents', closed_count)
  );

  RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'opened_incidents', opened_count, 'closed_incidents', closed_count);
END;
$$;

CREATE OR REPLACE PROCEDURE sp_inject_synthetic_anomaly(source_name STRING, signal_value FLOAT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  threshold FLOAT DEFAULT 0.75;
  anomaly_flag BOOLEAN;
BEGIN
  anomaly_flag := :signal_value >= :threshold;

  INSERT INTO APP_EVENTS.event_anomaly_signals(
    source,
    signal_name,
    signal_value,
    threshold,
    is_anomaly,
    payload
  )
  VALUES (
    COALESCE(:source_name, 'SYNTHETIC_TEST'),
    'dq_anomaly_score',
    :signal_value,
    :threshold,
    :anomaly_flag,
    OBJECT_CONSTRUCT('type', 'synthetic', 'created_by', CURRENT_USER())
  );

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'source', COALESCE(:source_name, 'SYNTHETIC_TEST'),
    'signal_value', :signal_value,
    'threshold', :threshold,
    'is_anomaly', :anomaly_flag
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_resolve_latest_alert(source_name STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  latest_alert_id STRING;
BEGIN
  SELECT alert_id
    INTO :latest_alert_id
  FROM alerts
  WHERE source = COALESCE(:source_name, 'SYNTHETIC_TEST')
    AND status = 'OPEN'
  ORDER BY created_at DESC
  LIMIT 1;

  IF latest_alert_id IS NULL THEN
    RETURN OBJECT_CONSTRUCT('status', 'NOOP', 'reason', 'NO_OPEN_ALERT_FOUND');
  END IF;

  UPDATE alerts
  SET status = 'CLOSED',
      resolved_at = CURRENT_TIMESTAMP()
  WHERE alert_id = :latest_alert_id;

  RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'alert_id', :latest_alert_id);
END;
$$;

CREATE OR REPLACE PROCEDURE sp_run_monitoring_cycle(cycle_name STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  anomaly_result VARIANT;
  incident_result VARIANT;
BEGIN
  anomaly_result := (CALL sp_detect_anomalies());
  incident_result := (CALL sp_open_close_incidents());

  INSERT INTO task_run_history(task_name, run_status, details)
  VALUES (
    COALESCE(:cycle_name, 'manual_cycle'),
    'SUCCESS',
    OBJECT_CONSTRUCT('anomaly_result', anomaly_result, 'incident_result', incident_result)
  );

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'cycle_name', COALESCE(:cycle_name, 'manual_cycle'),
    'anomaly_result', anomaly_result,
    'incident_result', incident_result
  );
END;
$$;

CREATE OR REPLACE TASK tsk_dq_eval
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  CALL APP_ENGINE.sp_detect_anomalies();

CREATE OR REPLACE TASK tsk_signal_rollup
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = 'USING CRON 5 * * * * UTC'
AS
  INSERT INTO APP_EVENTS.event_run_status(source, event_ts, payload)
  SELECT
    'APP_ENGINE.tsk_signal_rollup',
    CURRENT_TIMESTAMP(),
    OBJECT_CONSTRUCT(
      'open_alerts', COUNT_IF(status = 'OPEN'),
      'open_incidents', (SELECT COUNT(*) FROM APP_ENGINE.incidents WHERE status IN ('OPEN', 'INVESTIGATING'))
    )
  FROM APP_ENGINE.alerts;

CREATE OR REPLACE TASK tsk_incident_manager
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = 'USING CRON 10 * * * * UTC'
AS
  CALL APP_ENGINE.sp_open_close_incidents();
