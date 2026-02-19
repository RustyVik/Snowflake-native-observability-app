-- 002_cortex_udfs.sql
-- Cortex-only DQ UDF scaffolding

USE SCHEMA APP_DQ;

CREATE TABLE IF NOT EXISTS dq_rule_library (
  rule_name STRING PRIMARY KEY,
  rule_type STRING,
  description STRING,
  cortex_model STRING,
  prompt_template STRING,
  prompt_version STRING,
  pass_threshold FLOAT,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dq_rule_pack (
  pack_name STRING,
  rule_name STRING,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  created_by STRING DEFAULT CURRENT_USER(),
  PRIMARY KEY (pack_name, rule_name)
);

CREATE TABLE IF NOT EXISTS dq_rule_assignment (
  assignment_id STRING DEFAULT UUID_STRING(),
  pack_name STRING,
  rule_name STRING,
  target_object STRING,
  assigned_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  assigned_by STRING DEFAULT CURRENT_USER(),
  is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS rule_versions (
  rule_name STRING,
  version STRING,
  model_name STRING,
  prompt_template STRING,
  prompt_version STRING,
  threshold FLOAT,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  created_by STRING DEFAULT CURRENT_USER(),
  PRIMARY KEY (rule_name, version)
);

CREATE TABLE IF NOT EXISTS rule_approvals (
  rule_name STRING,
  version STRING,
  approval_status STRING,
  approved_by STRING,
  approved_at TIMESTAMP_NTZ,
  notes STRING,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (rule_name, version)
);

CREATE TABLE IF NOT EXISTS rule_promotion_audit (
  event_id STRING DEFAULT UUID_STRING(),
  pack_name STRING,
  target_object STRING,
  promoted_rule_count NUMBER,
  status STRING,
  promoted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  promoted_by STRING DEFAULT CURRENT_USER(),
  details VARIANT
);

MERGE INTO dq_rule_library tgt
USING (
  SELECT 'udf_dq_email_valid' AS rule_name, 'VALIDATION' AS rule_type, 'Validate email structure and quality' AS description, 'snowflake-arctic' AS cortex_model, 'Classify email quality for: {input}. Return concise rationale.' AS prompt_template, 'v1' AS prompt_version, 0.80 AS pass_threshold, TRUE AS is_active
  UNION ALL
  SELECT 'udf_dq_phone_valid', 'VALIDATION', 'Validate phone number plausibility', 'snowflake-arctic', 'Validate phone quality for: {input}. Return concise rationale.', 'v1', 0.75, TRUE
  UNION ALL
  SELECT 'udf_dq_name_quality', 'QUALITY', 'Assess personal name quality', 'snowflake-arctic', 'Assess name quality for: {input}. Return concise rationale.', 'v1', 0.70, TRUE
  UNION ALL
  SELECT 'udf_dq_address_quality', 'QUALITY', 'Assess address completeness and plausibility', 'snowflake-arctic', 'Assess address quality for: {input}. Return concise rationale.', 'v1', 0.70, TRUE
  UNION ALL
  SELECT 'udf_dq_pii_detect', 'CLASSIFICATION', 'Detect if input appears to contain PII', 'snowflake-arctic', 'Classify whether this contains PII: {input}. Return concise rationale.', 'v1', 0.80, TRUE
  UNION ALL
  SELECT 'udf_dq_domain_classify', 'CLASSIFICATION', 'Classify business data domain', 'snowflake-arctic', 'Classify business domain for: {input}. Return concise rationale.', 'v1', 0.65, TRUE
  UNION ALL
  SELECT 'udf_dq_reason_code', 'EXPLANATION', 'Generate normalized reason code for rule result', 'snowflake-arctic', 'Provide normalized reason code for this value: {input}.', 'v1', 0.60, TRUE
) src
ON tgt.rule_name = src.rule_name
WHEN MATCHED THEN UPDATE SET
  rule_type = src.rule_type,
  description = src.description,
  cortex_model = src.cortex_model,
  prompt_template = src.prompt_template,
  prompt_version = src.prompt_version,
  pass_threshold = src.pass_threshold,
  is_active = src.is_active,
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  rule_name,
  rule_type,
  description,
  cortex_model,
  prompt_template,
  prompt_version,
  pass_threshold,
  is_active
)
VALUES (
  src.rule_name,
  src.rule_type,
  src.description,
  src.cortex_model,
  src.prompt_template,
  src.prompt_version,
  src.pass_threshold,
  src.is_active
);

MERGE INTO rule_versions tgt
USING (
  SELECT rule_name, 'v1' AS version, cortex_model AS model_name, prompt_template, prompt_version, pass_threshold AS threshold, TRUE AS is_active
  FROM dq_rule_library
) src
ON tgt.rule_name = src.rule_name AND tgt.version = src.version
WHEN MATCHED THEN UPDATE SET
  model_name = src.model_name,
  prompt_template = src.prompt_template,
  prompt_version = src.prompt_version,
  threshold = src.threshold,
  is_active = src.is_active
WHEN NOT MATCHED THEN INSERT (
  rule_name,
  version,
  model_name,
  prompt_template,
  prompt_version,
  threshold,
  is_active
)
VALUES (
  src.rule_name,
  src.version,
  src.model_name,
  src.prompt_template,
  src.prompt_version,
  src.threshold,
  src.is_active
);

MERGE INTO rule_approvals tgt
USING (
  SELECT rule_name, 'v1' AS version, 'APPROVED' AS approval_status, 'SYSTEM' AS approved_by, CURRENT_TIMESTAMP() AS approved_at, 'Seed approval for Sprint 2 baseline' AS notes
  FROM dq_rule_library
) src
ON tgt.rule_name = src.rule_name AND tgt.version = src.version
WHEN MATCHED THEN UPDATE SET
  approval_status = src.approval_status,
  approved_by = src.approved_by,
  approved_at = src.approved_at,
  notes = src.notes
WHEN NOT MATCHED THEN INSERT (
  rule_name,
  version,
  approval_status,
  approved_by,
  approved_at,
  notes
)
VALUES (
  src.rule_name,
  src.version,
  src.approval_status,
  src.approved_by,
  src.approved_at,
  src.notes
);

MERGE INTO dq_rule_pack tgt
USING (
  SELECT 'baseline_pack' AS pack_name, rule_name, TRUE AS is_active
  FROM dq_rule_library
) src
ON tgt.pack_name = src.pack_name AND tgt.rule_name = src.rule_name
WHEN MATCHED THEN UPDATE SET is_active = src.is_active
WHEN NOT MATCHED THEN INSERT (pack_name, rule_name, is_active) VALUES (src.pack_name, src.rule_name, src.is_active);

CREATE OR REPLACE FUNCTION udf_dq_email_valid(input_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  WITH ctx AS (
    SELECT 'snowflake-arctic' AS model_name,
           CONCAT('Validate whether this is a valid email address and explain briefly: ', COALESCE(input_value, 'NULL')) AS prompt
  ),
  cortex AS (
    SELECT model_name, SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', prompt) AS raw_response
    FROM ctx
  )
  SELECT TO_VARIANT(OBJECT_CONSTRUCT(
    'status', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'), 'PASS', 'FAIL'),
    'confidence', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'), 0.95, 0.60),
    'reason_code', IFF(input_value IS NULL OR TRIM(input_value) = '', 'INPUT_EMPTY', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'), 'EMAIL_VALID', 'EMAIL_INVALID_FORMAT')),
    'model', model_name,
    'explanation', raw_response
  ))
  FROM cortex
$$;

CREATE OR REPLACE FUNCTION udf_dq_phone_valid(input_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  WITH ctx AS (
    SELECT 'snowflake-arctic' AS model_name,
           CONCAT('Validate whether this appears to be a valid phone number and explain briefly: ', COALESCE(input_value, 'NULL')) AS prompt
  ),
  cortex AS (
    SELECT model_name, SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', prompt) AS raw_response
    FROM ctx
  )
  SELECT TO_VARIANT(OBJECT_CONSTRUCT(
    'status', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '^[+]?[(]?[0-9]{1,4}[)]?[-\\s./0-9]*$') AND LENGTH(REGEXP_REPLACE(COALESCE(input_value,''), '[^0-9]', '')) BETWEEN 7 AND 15, 'PASS', 'FAIL'),
    'confidence', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '^[+]?[(]?[0-9]{1,4}[)]?[-\\s./0-9]*$') AND LENGTH(REGEXP_REPLACE(COALESCE(input_value,''), '[^0-9]', '')) BETWEEN 7 AND 15, 0.90, 0.55),
    'reason_code', IFF(input_value IS NULL OR TRIM(input_value) = '', 'INPUT_EMPTY', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '^[+]?[(]?[0-9]{1,4}[)]?[-\\s./0-9]*$') AND LENGTH(REGEXP_REPLACE(COALESCE(input_value,''), '[^0-9]', '')) BETWEEN 7 AND 15, 'PHONE_VALID', 'PHONE_INVALID_FORMAT')),
    'model', model_name,
    'explanation', raw_response
  ))
  FROM cortex
$$;

CREATE OR REPLACE FUNCTION udf_dq_name_quality(input_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  WITH ctx AS (
    SELECT 'snowflake-arctic' AS model_name,
           CONCAT('Assess quality of this personal name and explain briefly: ', COALESCE(input_value, 'NULL')) AS prompt
  ),
  cortex AS (
    SELECT model_name, SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', prompt) AS raw_response
    FROM ctx
  )
  SELECT TO_VARIANT(OBJECT_CONSTRUCT(
    'status', IFF(input_value IS NOT NULL AND LENGTH(TRIM(input_value)) >= 2, 'PASS', 'FAIL'),
    'confidence', IFF(input_value IS NOT NULL AND LENGTH(TRIM(input_value)) >= 2, 0.80, 0.50),
    'reason_code', IFF(input_value IS NULL OR TRIM(input_value) = '', 'INPUT_EMPTY', IFF(LENGTH(TRIM(input_value)) < 2, 'NAME_TOO_SHORT', 'NAME_QUALITY_OK')),
    'model', model_name,
    'explanation', raw_response
  ))
  FROM cortex
$$;

CREATE OR REPLACE FUNCTION udf_dq_address_quality(input_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  WITH ctx AS (
    SELECT 'snowflake-arctic' AS model_name,
           CONCAT('Assess quality and completeness of this address and explain briefly: ', COALESCE(input_value, 'NULL')) AS prompt
  ),
  cortex AS (
    SELECT model_name, SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', prompt) AS raw_response
    FROM ctx
  )
  SELECT TO_VARIANT(OBJECT_CONSTRUCT(
    'status', IFF(input_value IS NOT NULL AND LENGTH(TRIM(input_value)) >= 8, 'PASS', 'FAIL'),
    'confidence', IFF(input_value IS NOT NULL AND LENGTH(TRIM(input_value)) >= 8, 0.78, 0.52),
    'reason_code', IFF(input_value IS NULL OR TRIM(input_value) = '', 'INPUT_EMPTY', IFF(LENGTH(TRIM(input_value)) < 8, 'ADDRESS_INCOMPLETE', 'ADDRESS_QUALITY_OK')),
    'model', model_name,
    'explanation', raw_response
  ))
  FROM cortex
$$;

CREATE OR REPLACE FUNCTION udf_dq_pii_detect(input_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  WITH ctx AS (
    SELECT 'snowflake-arctic' AS model_name,
           CONCAT('Classify whether this text contains personally identifiable information and explain briefly: ', COALESCE(input_value, 'NULL')) AS prompt
  ),
  cortex AS (
    SELECT model_name, SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', prompt) AS raw_response
    FROM ctx
  )
  SELECT TO_VARIANT(OBJECT_CONSTRUCT(
    'status', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}') OR REGEXP_LIKE(COALESCE(input_value, ''), '[0-9]{3}-[0-9]{2}-[0-9]{4}'), 'FAIL', 'PASS'),
    'confidence', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}') OR REGEXP_LIKE(COALESCE(input_value, ''), '[0-9]{3}-[0-9]{2}-[0-9]{4}'), 0.88, 0.68),
    'reason_code', IFF(input_value IS NULL OR TRIM(input_value) = '', 'INPUT_EMPTY', IFF(REGEXP_LIKE(COALESCE(input_value, ''), '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}') OR REGEXP_LIKE(COALESCE(input_value, ''), '[0-9]{3}-[0-9]{2}-[0-9]{4}'), 'PII_DETECTED', 'PII_NOT_DETECTED')),
    'model', model_name,
    'explanation', raw_response
  ))
  FROM cortex
$$;

CREATE OR REPLACE FUNCTION udf_dq_domain_classify(input_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  WITH ctx AS (
    SELECT 'snowflake-arctic' AS model_name,
           CONCAT('Classify this value into a business domain such as customer, finance, product, operations. Value: ', COALESCE(input_value, 'NULL')) AS prompt
  ),
  cortex AS (
    SELECT model_name, SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', prompt) AS raw_response
    FROM ctx
  )
  SELECT TO_VARIANT(OBJECT_CONSTRUCT(
    'status', 'PASS',
    'confidence', 0.70,
    'reason_code', 'DOMAIN_CLASSIFIED',
    'model', model_name,
    'explanation', raw_response
  ))
  FROM cortex
$$;

CREATE OR REPLACE FUNCTION udf_dq_reason_code(input_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  WITH ctx AS (
    SELECT 'snowflake-arctic' AS model_name,
           CONCAT('Return a normalized reason code for this value quality assessment: ', COALESCE(input_value, 'NULL')) AS prompt
  ),
  cortex AS (
    SELECT model_name, SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', prompt) AS raw_response
    FROM ctx
  )
  SELECT TO_VARIANT(OBJECT_CONSTRUCT(
    'status', 'PASS',
    'confidence', 0.65,
    'reason_code', IFF(input_value IS NULL OR TRIM(input_value) = '', 'INPUT_EMPTY', 'REASON_CODE_GENERATED'),
    'model', model_name,
    'explanation', raw_response
  ))
  FROM cortex
$$;

CREATE OR REPLACE PROCEDURE sp_preview_rule_result(rule_name STRING, input_value STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  normalized_rule STRING;
  model_name STRING;
  token_estimate NUMBER DEFAULT 250;
  gate_result VARIANT;
  gate_status STRING;
  audit_event_id STRING;
  result VARIANT;
BEGIN
  normalized_rule := LOWER(COALESCE(:rule_name, ''));

  SELECT cortex_model
    INTO :model_name
  FROM APP_DQ.dq_rule_library
  WHERE LOWER(rule_name) = :normalized_rule
    AND is_active = TRUE
  LIMIT 1;

  IF (:model_name IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'status', 'ERROR',
      'message', CONCAT('Unknown or inactive rule: ', COALESCE(:rule_name, 'NULL'))
    );
  END IF;

  gate_result := (
    CALL APP_CORE.sp_enforce_cortex_execution(
      :model_name,
      :token_estimate,
      :rule_name,
      OBJECT_CONSTRUCT('source', 'APP_DQ.sp_preview_rule_result')
    )
  );

  gate_status := COALESCE(:gate_result:"status"::STRING, 'UNKNOWN');

  IF (:gate_status = 'BLOCKED') THEN
    RETURN OBJECT_CONSTRUCT(
      'status', 'BLOCKED',
      'rule_name', :rule_name,
      'gate', :gate_result
    );
  END IF;

  SELECT CASE
    WHEN :normalized_rule = 'udf_dq_email_valid' THEN APP_DQ.udf_dq_email_valid(:input_value)
    WHEN :normalized_rule = 'udf_dq_phone_valid' THEN APP_DQ.udf_dq_phone_valid(:input_value)
    WHEN :normalized_rule = 'udf_dq_name_quality' THEN APP_DQ.udf_dq_name_quality(:input_value)
    WHEN :normalized_rule = 'udf_dq_address_quality' THEN APP_DQ.udf_dq_address_quality(:input_value)
    WHEN :normalized_rule = 'udf_dq_pii_detect' THEN APP_DQ.udf_dq_pii_detect(:input_value)
    WHEN :normalized_rule = 'udf_dq_domain_classify' THEN APP_DQ.udf_dq_domain_classify(:input_value)
    WHEN :normalized_rule = 'udf_dq_reason_code' THEN APP_DQ.udf_dq_reason_code(:input_value)
    ELSE NULL
  END
  INTO :result;

  IF (:result IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'status', 'ERROR',
      'message', CONCAT('Unknown rule: ', COALESCE(:rule_name, 'NULL')),
      'supported_rules', ARRAY_CONSTRUCT(
        'udf_dq_email_valid',
        'udf_dq_phone_valid',
        'udf_dq_name_quality',
        'udf_dq_address_quality',
        'udf_dq_pii_detect',
        'udf_dq_domain_classify',
        'udf_dq_reason_code'
      )
    );
  END IF;

  audit_event_id := (
    CALL APP_CORE.sp_record_cortex_call_audit(
      :rule_name,
      :model_name,
      :token_estimate,
      'SUCCESS',
      OBJECT_CONSTRUCT('source', 'APP_DQ.sp_preview_rule_result')
    )
  );

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'rule_name', :rule_name,
    'result', :result,
    'audit_event_id', :audit_event_id
  );
END;
$$;

CREATE OR REPLACE PROCEDURE sp_apply_rule_pack(pack_name STRING, target_object STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  unresolved_count NUMBER DEFAULT 0;
  eligible_count NUMBER DEFAULT 0;
  inserted_count NUMBER DEFAULT 0;
BEGIN
  SELECT COUNT(*)
    INTO :unresolved_count
  FROM APP_DQ.dq_rule_pack rp
  LEFT JOIN APP_DQ.rule_versions rv
    ON rp.rule_name = rv.rule_name
   AND rv.is_active = TRUE
  LEFT JOIN APP_DQ.rule_approvals ra
    ON rv.rule_name = ra.rule_name
   AND rv.version = ra.version
  WHERE rp.pack_name = :pack_name
    AND rp.is_active = TRUE
    AND (
      rv.version IS NULL
      OR COALESCE(ra.approval_status, 'PENDING') <> 'APPROVED'
    );

  IF (:unresolved_count > 0) THEN
    INSERT INTO APP_DQ.rule_promotion_audit(pack_name, target_object, promoted_rule_count, status, details)
    SELECT
      :pack_name,
      :target_object,
      0,
      'BLOCKED',
      OBJECT_CONSTRUCT('reason', 'UNAPPROVED_OR_MISSING_VERSION', 'unresolved_count', :unresolved_count);

    RETURN OBJECT_CONSTRUCT(
      'status', 'BLOCKED',
      'reason', 'UNAPPROVED_OR_MISSING_VERSION',
      'unresolved_count', :unresolved_count
    );
  END IF;

  SELECT COUNT(*)
    INTO :eligible_count
  FROM APP_DQ.dq_rule_pack
  WHERE pack_name = :pack_name
    AND is_active = TRUE;

  INSERT INTO APP_DQ.dq_rule_assignment (pack_name, rule_name, target_object, is_active)
  SELECT :pack_name, rp.rule_name, :target_object, TRUE
  FROM APP_DQ.dq_rule_pack rp
  WHERE rp.pack_name = :pack_name
    AND rp.is_active = TRUE;

  inserted_count := SQLROWCOUNT;

  INSERT INTO APP_DQ.rule_promotion_audit(pack_name, target_object, promoted_rule_count, status, details)
  SELECT
    :pack_name,
    :target_object,
    :inserted_count,
    'SUCCESS',
    OBJECT_CONSTRUCT('eligible_count', :eligible_count);

  RETURN OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'pack_name', :pack_name,
    'target_object', :target_object,
    'eligible_count', eligible_count,
    'promoted_count', inserted_count
  );
END;
$$;
