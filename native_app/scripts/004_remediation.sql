-- 004_remediation.sql

USE SCHEMA APP_ENGINE;

CREATE TABLE IF NOT EXISTS remediation_tasks (
  task_id STRING DEFAULT UUID_STRING(),
  incident_id STRING,
  title STRING,
  description STRING,
  status STRING,
  priority STRING,
  owner STRING,
  sla_hours NUMBER,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  assigned_at TIMESTAMP_NTZ,
  started_at TIMESTAMP_NTZ,
  completed_at TIMESTAMP_NTZ,
  escalated_at TIMESTAMP_NTZ,
  due_at TIMESTAMP_NTZ,
  payload VARIANT
);

CREATE TABLE IF NOT EXISTS task_status_history (
  history_id STRING DEFAULT UUID_STRING(),
  task_id STRING,
  old_status STRING,
  new_status STRING,
  notes STRING,
  changed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  changed_by STRING DEFAULT CURRENT_USER()
);

CREATE TABLE IF NOT EXISTS remediation_comments (
  comment_id STRING DEFAULT UUID_STRING(),
  task_id STRING,
  comment_text STRING,
  commented_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  commented_by STRING DEFAULT CURRENT_USER()
);

CREATE OR REPLACE PROCEDURE sp_create_remediation_task(incident_id STRING, title STRING, owner STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  new_id STRING;
  incident_exists BOOLEAN DEFAULT FALSE;
BEGIN
  SELECT COUNT(*) > 0
    INTO :incident_exists
  FROM APP_ENGINE.incidents
  WHERE incident_id = :incident_id;

  IF NOT incident_exists THEN
    RETURN OBJECT_CONSTRUCT(
      'status', 'FAILED',
      'reason', 'INCIDENT_NOT_FOUND',
      'incident_id', :incident_id
    );
  END IF;

  new_id := UUID_STRING();
  INSERT INTO remediation_tasks(
    task_id,
    incident_id,
    title,
    description,
    status,
    priority,
    owner,
    sla_hours,
    assigned_at,
    due_at,
    payload
  )
  VALUES (
    :new_id,
    :incident_id,
    :title,
    'Auto-created from incident workflow',
    'OPEN',
    'MEDIUM',
    :owner,
    24,
    CURRENT_TIMESTAMP(),
    DATEADD('hour', 24, CURRENT_TIMESTAMP()),
    OBJECT_CONSTRUCT('source', 'sp_create_remediation_task')
  );

  INSERT INTO task_status_history(task_id, old_status, new_status, notes)
  VALUES (:new_id, NULL, 'OPEN', 'Task created');

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'task_id', :new_id,
    'incident_id', :incident_id,
    'owner', :owner
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_create_task_for_latest_incident(source_name STRING, title STRING, owner STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  latest_incident_id STRING;
  create_result VARIANT;
BEGIN
  SELECT i.incident_id
    INTO :latest_incident_id
  FROM APP_ENGINE.incidents i
  WHERE i.status IN ('OPEN', 'INVESTIGATING')
    AND i.payload:"source"::STRING = COALESCE(:source_name, 'SMOKE_SYNTHETIC')
  ORDER BY i.opened_at DESC
  LIMIT 1;

  IF latest_incident_id IS NULL THEN
    RETURN OBJECT_CONSTRUCT(
      'status', 'FAILED',
      'reason', 'NO_OPEN_INCIDENT_FOR_SOURCE',
      'source', COALESCE(:source_name, 'SMOKE_SYNTHETIC')
    );
  END IF;

  create_result := (CALL sp_create_remediation_task(:latest_incident_id, :title, :owner));

  RETURN OBJECT_INSERT(create_result, 'incident_id', :latest_incident_id, TRUE);
END;
$$;

CREATE OR REPLACE PROCEDURE sp_reassign_remediation_task(task_id STRING, new_owner STRING, reason STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  updated_rows NUMBER DEFAULT 0;
BEGIN
  UPDATE remediation_tasks
  SET owner = :new_owner,
      assigned_at = CURRENT_TIMESTAMP(),
      payload = OBJECT_INSERT(COALESCE(payload, OBJECT_CONSTRUCT()), 'reassign_reason', :reason, TRUE)
  WHERE task_id = :task_id;

  updated_rows := SQLROWCOUNT;

  IF updated_rows = 0 THEN
    RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'reason', 'TASK_NOT_FOUND', 'task_id', :task_id);
  END IF;

  INSERT INTO remediation_comments(task_id, comment_text)
  VALUES (:task_id, CONCAT('Task reassigned to ', :new_owner, '. Reason: ', COALESCE(:reason, 'N/A')));

  RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'task_id', :task_id, 'new_owner', :new_owner);
END;
$$;

CREATE OR REPLACE PROCEDURE sp_update_remediation_status(task_id STRING, new_status STRING, notes STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  old_status STRING;
  normalized_status STRING;
  is_valid_transition BOOLEAN DEFAULT FALSE;
BEGIN
  normalized_status := UPPER(COALESCE(:new_status, ''));

  SELECT status
    INTO :old_status
  FROM remediation_tasks
  WHERE task_id = :task_id;

  IF old_status IS NULL THEN
    RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'reason', 'TASK_NOT_FOUND', 'task_id', :task_id);
  END IF;

  is_valid_transition :=
      (old_status = 'OPEN' AND normalized_status IN ('IN_PROGRESS', 'BLOCKED', 'CANCELLED'))
   OR (old_status = 'IN_PROGRESS' AND normalized_status IN ('BLOCKED', 'COMPLETED', 'CANCELLED'))
   OR (old_status = 'BLOCKED' AND normalized_status IN ('IN_PROGRESS', 'CANCELLED'))
   OR (old_status IN ('CANCELLED', 'COMPLETED') AND normalized_status = old_status)
   OR (old_status = normalized_status);

  IF NOT is_valid_transition THEN
    RETURN OBJECT_CONSTRUCT(
      'status', 'FAILED',
      'reason', 'INVALID_STATUS_TRANSITION',
      'old_status', old_status,
      'new_status', normalized_status
    );
  END IF;

  UPDATE remediation_tasks
  SET status = :normalized_status,
      started_at = IFF(old_status = 'OPEN' AND normalized_status = 'IN_PROGRESS', CURRENT_TIMESTAMP(), started_at),
      completed_at = IFF(normalized_status = 'COMPLETED', CURRENT_TIMESTAMP(), completed_at)
  WHERE task_id = :task_id;

  INSERT INTO task_status_history(task_id, old_status, new_status, notes)
  VALUES (:task_id, :old_status, :normalized_status, :notes);

  IF notes IS NOT NULL THEN
    INSERT INTO remediation_comments(task_id, comment_text)
    VALUES (:task_id, :notes);
  END IF;

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'task_id', :task_id,
    'old_status', old_status,
    'new_status', normalized_status
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_complete_remediation_task(task_id STRING, resolution_notes STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  status_result VARIANT;
BEGIN
  status_result := (CALL sp_update_remediation_status(:task_id, 'COMPLETED', :resolution_notes));

  IF status_result:"status"::STRING <> 'SUCCESS' THEN
    RETURN status_result;
  END IF;

  UPDATE remediation_tasks
  SET payload = OBJECT_INSERT(COALESCE(payload, OBJECT_CONSTRUCT()), 'resolution_notes', COALESCE(:resolution_notes, ''), TRUE)
  WHERE task_id = :task_id;

  RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'task_id', :task_id, 'resolution_notes', :resolution_notes);
END;
$$;

CREATE OR REPLACE PROCEDURE sp_escalate_overdue_tasks()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  escalated_count NUMBER DEFAULT 0;
BEGIN
  UPDATE remediation_tasks
  SET status = IFF(status = 'OPEN', 'BLOCKED', status),
      priority = 'HIGH',
      escalated_at = CURRENT_TIMESTAMP(),
      payload = OBJECT_INSERT(COALESCE(payload, OBJECT_CONSTRUCT()), 'escalation_reason', 'SLA_OVERDUE', TRUE)
  WHERE status IN ('OPEN', 'IN_PROGRESS', 'BLOCKED')
    AND due_at IS NOT NULL
    AND due_at < CURRENT_TIMESTAMP();

  escalated_count := SQLROWCOUNT;

  INSERT INTO task_status_history(task_id, old_status, new_status, notes)
  SELECT task_id, status, status, 'Escalated due to SLA overdue'
  FROM remediation_tasks
  WHERE escalated_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP());

  RETURN OBJECT_CONSTRUCT('status', 'SUCCESS', 'escalated_tasks', escalated_count);
END;
$$;

CREATE OR REPLACE PROCEDURE sp_get_remediation_summary()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  open_count NUMBER DEFAULT 0;
  in_progress_count NUMBER DEFAULT 0;
  blocked_count NUMBER DEFAULT 0;
  completed_count NUMBER DEFAULT 0;
  overdue_count NUMBER DEFAULT 0;
BEGIN
  SELECT COUNT_IF(status = 'OPEN'),
         COUNT_IF(status = 'IN_PROGRESS'),
         COUNT_IF(status = 'BLOCKED'),
         COUNT_IF(status = 'COMPLETED')
    INTO :open_count, :in_progress_count, :blocked_count, :completed_count
  FROM remediation_tasks;

  SELECT COUNT(*)
    INTO :overdue_count
  FROM remediation_tasks
  WHERE status IN ('OPEN', 'IN_PROGRESS', 'BLOCKED')
    AND due_at < CURRENT_TIMESTAMP();

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'open', open_count,
    'in_progress', in_progress_count,
    'blocked', blocked_count,
    'completed', completed_count,
    'overdue', overdue_count
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_update_latest_task_for_source(source_name STRING, new_status STRING, notes STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  latest_task_id STRING;
  result VARIANT;
BEGIN
  SELECT rt.task_id
    INTO :latest_task_id
  FROM APP_ENGINE.remediation_tasks rt
  JOIN APP_ENGINE.incidents i
    ON rt.incident_id = i.incident_id
  WHERE i.payload:"source"::STRING = COALESCE(:source_name, 'SMOKE_SYNTHETIC')
  ORDER BY rt.created_at DESC
  LIMIT 1;

  IF latest_task_id IS NULL THEN
    RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'reason', 'NO_TASK_FOUND_FOR_SOURCE', 'source', :source_name);
  END IF;

  result := (CALL sp_update_remediation_status(:latest_task_id, :new_status, :notes));
  RETURN OBJECT_INSERT(result, 'task_id', :latest_task_id, TRUE);
END;
$$;
