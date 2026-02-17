# Delivery Plan: Cortex-Only Snowflake Native App

## Sprint 1 (Weeks 1-2): Foundation + Cortex Readiness

### Deliverables
- Native app schemas: `APP_CORE`, `APP_DQ`, `APP_EVENTS`, `APP_ENGINE`, `APP_AUDIT`
- Lifecycle procedures: `sp_setup_app()`, `sp_upgrade_app()`, `sp_healthcheck()`
- Cortex gate: `sp_validate_cortex_access()`
- Governance tables: `app_versions`, `app_config`, `model_allowlist`, `cost_guardrails`, `cortex_call_audit`

### Exit Gates
- `G1` install/upgrade smoke pass
- `G2` Cortex gate blocks activation when prerequisites fail
- `G3` Cortex audit records generated

### Execution Plan

#### Sprint Objective
Stand up the Native App foundation and prove Cortex readiness controls end-to-end so Sprint 2 can safely build the UDF library on top of governed base objects.

#### Workstream Breakdown
- **W1. Packaging + Core Schema Baseline**
  - Validate and finalize app packaging entrypoints: `native_app/manifest.yml`, `native_app/setup.sql`
  - Execute/install baseline schemas and version tracking from `setup.sql`
  - Verify required schemas exist: `APP_CORE`, `APP_DQ`, `APP_EVENTS`, `APP_ENGINE`, `APP_AUDIT`
  - Seed initial `APP_CORE.app_versions` version record (`0.1.0`)

- **W2. Governance + Cortex Gate + Audit**
  - Implement/validate governance objects in `native_app/scripts/001_init.sql`:
    - `APP_CORE.app_config`
    - `APP_CORE.model_allowlist`
    - `APP_CORE.cost_guardrails`
    - `APP_AUDIT.cortex_call_audit`
  - Harden and validate `APP_CORE.sp_validate_cortex_access()` output contract (`status`, `checks[]`)
  - Add activation precheck path that enforces Cortex gate pass before enabling downstream rule execution
  - Produce at least one audited Cortex-call-shaped event in `APP_AUDIT.cortex_call_audit` as evidence for `G3`

- **W3. Lifecycle Procedures + Verification Harness**
  - Implement lifecycle procedures in `APP_CORE`:
    - `sp_setup_app()`
    - `sp_upgrade_app()`
    - `sp_healthcheck()`
  - Ensure idempotent setup/upgrade behavior and deterministic status payloads
  - Run and record SQL smoke checks in `tests/sql/smoke_checks.sql`
  - Publish Sprint 1 validation notes (gate status, evidence queries, known gaps)

#### Day-by-Day Suggested Sequence (10 Working Days)
- **Day 1-2:** Packaging + setup sequence dry run (`setup.sql`, script order verification)
- **Day 3-4:** Governance tables + default seed strategy (`app_config`, allowlist, guardrails)
- **Day 5:** Cortex gate logic finalization + negative test scenarios
- **Day 6-7:** Lifecycle procedure implementation (`setup`, `upgrade`, `healthcheck`)
- **Day 8:** Audit event generation + validation query pack
- **Day 9:** Full smoke pass + defect fixes
- **Day 10:** Gate review (`G1-G3`) + Sprint 2 handoff readiness

#### Dependencies
- Snowflake account has Cortex features enabled for target region/account
- Required app roles/privileges available for schema/procedure creation
- Native App packaging pipeline can execute `setup.sql` and ordered scripts

#### Risks and Mitigations
- **Risk:** Cortex unavailable or misconfigured in consumer account  
  **Mitigation:** fail-fast in `sp_validate_cortex_access()` and block activation path (`G2`)
- **Risk:** Setup/upgrade non-idempotent behavior causes install drift  
  **Mitigation:** use `IF NOT EXISTS` patterns + repeat-run smoke checks
- **Risk:** Missing governance seed values slows Sprint 2 UDF rollout  
  **Mitigation:** provide baseline rows and documented defaults in Sprint 1 closeout

#### Definition of Done (Sprint 1)
- `G1`: Install + upgrade + smoke checks pass with no blocking defects
- `G2`: Cortex gate returns `FAIL` for prerequisite violations and blocks activation
- `G3`: Audit evidence exists in `APP_AUDIT.cortex_call_audit` and is queryable
- Lifecycle procedures are callable and return stable status payloads
- Sprint 2 handoff checklist published with verified base objects and open issues

## Sprint 2 (Weeks 3-4): Cortex UDF Library + Rule Governance

### Deliverables
- Cortex-backed UDFs:
  - `udf_dq_email_valid`
  - `udf_dq_phone_valid`
  - `udf_dq_name_quality`
  - `udf_dq_address_quality`
  - `udf_dq_pii_detect`
  - `udf_dq_domain_classify`
  - `udf_dq_reason_code`
- Rule metadata: `dq_rule_library`, `dq_rule_pack`, `dq_rule_assignment`, `rule_versions`, `rule_approvals`
- Rule APIs: `sp_apply_rule_pack()`, `sp_preview_rule_result()`

### Exit Gates
- `G4` all UDFs call `SNOWFLAKE.CORTEX.*`
- `G5` standardized output payload (`status`, `confidence`, `reason_code`, `model`)
- `G6` version/approval required for promotion

### Execution Plan

#### Sprint Objective
Implement the Cortex-only DQ UDF library and rule governance controls so rule promotion is auditable, versioned, and safe for production rollout.

#### Workstream Breakdown
- **W1. Cortex UDF Core Implementation**
  - Replace placeholder bodies in `native_app/scripts/002_cortex_udfs.sql` for:
    - `udf_dq_email_valid`
    - `udf_dq_phone_valid`
    - `udf_dq_name_quality`
    - `udf_dq_address_quality`
    - `udf_dq_pii_detect`
    - `udf_dq_domain_classify`
    - `udf_dq_reason_code`
  - Ensure every UDF routes through `SNOWFLAKE.CORTEX.*` (no non-Cortex fallback path)
  - Standardize response contract for every UDF:
    - `status`
    - `confidence`
    - `reason_code`
    - `model`

- **W2. Rule Governance Data Model**
  - Create/extend rule governance tables in `APP_DQ`:
    - `dq_rule_library`
    - `dq_rule_pack`
    - `dq_rule_assignment`
    - `rule_versions`
    - `rule_approvals`
  - Define required metadata columns:
    - `rule_name`, `rule_type`, `model_name`, `prompt_template`, `prompt_version`
    - `threshold`, `is_active`, `version`, `approval_status`, `approved_by`, `approved_at`
  - Seed baseline rule metadata aligned to the 7 Sprint 2 UDFs

- **W3. Promotion APIs + Control Enforcement**
  - Implement `sp_preview_rule_result()` for dry-run validation against sample values
  - Implement `sp_apply_rule_pack()` for controlled assignment/promotion
  - Enforce `G6` policy in procedures:
    - promotion requires approved version
    - block unapproved/inactive rules
    - persist promotion audit metadata (who, when, version)

- **W4. Validation + Evidence Pack**
  - Extend `tests/sql/smoke_checks.sql` (or add Sprint 2 checks) for:
    - Cortex invocation path validation (`G4`)
    - payload schema conformance (`G5`)
    - promotion guard behavior (`G6`)
  - Add representative positive + negative test cases per UDF class
  - Produce evidence queries and release notes for Sprint 2 gate review

#### Day-by-Day Suggested Sequence (10 Working Days)
- **Day 1:** finalize UDF response contract and model/prompt conventions
- **Day 2-4:** implement all 7 Cortex-backed UDFs and unit-level SQL validations
- **Day 5-6:** build rule governance tables + seed baseline rule/version records
- **Day 7:** implement `sp_preview_rule_result()` and dry-run output checks
- **Day 8:** implement `sp_apply_rule_pack()` with approval/version enforcement
- **Day 9:** execute full Sprint 2 smoke and negative-path tests
- **Day 10:** gate review (`G4-G6`) + Sprint 3 handoff package

#### Dependencies
- Sprint 1 procedures/tables operational (`sp_validate_cortex_access`, `cortex_call_audit`, guardrails)
- Approved Cortex models available in `APP_CORE.model_allowlist`
- Prompt templates and thresholds finalized by data quality stakeholders

#### Risks and Mitigations
- **Risk:** Inconsistent UDF payloads break downstream consumers  
  **Mitigation:** enforce common output object schema in all UDF definitions + contract tests
- **Risk:** Rule promotion bypasses approvals under manual SQL changes  
  **Mitigation:** centralize promotion in `sp_apply_rule_pack()` and reject unapproved versions
- **Risk:** Cortex model/prompt drift reduces classification quality  
  **Mitigation:** persist `model` and `prompt_version` in outputs and approval metadata for traceability

#### Definition of Done (Sprint 2)
- `G4`: all published Sprint 2 UDFs contain direct `SNOWFLAKE.CORTEX.*` invocation paths
- `G5`: every UDF returns standardized payload with `status`, `confidence`, `reason_code`, `model`
- `G6`: promotion to active assignment requires approved `rule_versions` + `rule_approvals`
- Rule APIs (`sp_preview_rule_result`, `sp_apply_rule_pack`) are callable and policy-compliant
- Sprint 3 handoff includes validated rule catalog, promotion audit evidence, and known limitations

## Sprint 3 (Weeks 5-6): Profiling + Monitoring + Incidents

### Deliverables
- Profiling/classification: `sp_profile_dataset()`, `sp_classify_columns_cortex()`
- Tables: `profile_runs`, `profile_column_stats`, `classification_results`, `label_overrides`
- Monitoring plane: event tables, streams, dynamic tables
- Incident engine: `alerts`, `incidents`, `incident_history`
- Tasks: `tsk_dq_eval`, `tsk_signal_rollup`, `tsk_incident_manager`

### Exit Gates
- `G7` scheduled runs pass for 3 cycles
- `G8` synthetic anomaly opens/closes incidents
- `G9` confidence + override traceability proven

### Execution Plan

#### Sprint Objective
Operationalize continuous data quality observability by implementing dataset profiling/classification, scheduled monitoring pipelines, and incident lifecycle automation with traceable confidence and override lineage.

#### Workstream Breakdown
- **W1. Profiling + Classification Foundation**
  - Implement `sp_profile_dataset()` and `sp_classify_columns_cortex()` in `APP_ENGINE`
  - Create/extend tables:
    - `profile_runs`
    - `profile_column_stats`
    - `classification_results`
    - `label_overrides`
  - Persist per-column confidence, model name, prompt/version metadata for downstream explainability

- **W2. Monitoring Plane Assembly**
  - Expand `APP_EVENTS` ingestion/event capture for profiling and DQ signal outputs
  - Build derived monitoring layer in `APP_ENGINE` (streams + dynamic tables/views)
  - Implement/complete anomaly pipeline procedures:
    - `sp_detect_anomalies()`
    - `sp_open_close_incidents()`

- **W3. Incident Lifecycle + State Transitions**
  - Normalize incident entities and history tracking:
    - `alerts`
    - `incidents`
    - `incident_history`
  - Enforce state transitions (`OPEN -> INVESTIGATING -> RESOLVED/CLOSED`) with timestamps and actor metadata
  - Link anomaly evidence + impacted assets into incident payload schema

- **W4. Scheduling + Reliability + Gate Evidence**
  - Create/activate tasks:
    - `tsk_dq_eval`
    - `tsk_signal_rollup`
    - `tsk_incident_manager`
  - Validate task dependencies and idempotent rerun behavior
  - Execute synthetic anomaly scenarios to prove auto-open/auto-close behavior
  - Produce evidence queries/reports for `G7–G9`

#### Day-by-Day Suggested Sequence (10 Working Days)
- **Day 1-2:** implement profiling/classification procedures and result tables
- **Day 3:** add override model (`label_overrides`) and confidence lineage fields
- **Day 4-5:** implement anomaly derivation and monitoring rollups
- **Day 6-7:** complete incident lifecycle logic + history tracking
- **Day 8:** wire/activate scheduled tasks and dependency order
- **Day 9:** run 3-cycle schedule validation and synthetic anomaly tests
- **Day 10:** gate review (`G7-G9`) + Sprint 4 handoff package

#### Dependencies
- Sprint 1 foundation and Sprint 2 rule governance/UDF contracts are active
- Cortex model allowlist and guardrails remain enabled in `APP_CORE`
- Event-producing upstream sources available for realistic monitoring signals

#### Risks and Mitigations
- **Risk:** Low-signal datasets produce unstable anomaly behavior  
  **Mitigation:** add minimum sample thresholds + fallback severity policy in anomaly logic
- **Risk:** Task schedule overlap causes duplicate alerts/incidents  
  **Mitigation:** enforce idempotent keys and last-processed watermark patterns
- **Risk:** Override decisions lose lineage to original classifications  
  **Mitigation:** store both original and effective labels with override actor/time/reason

#### Definition of Done (Sprint 3)
- `G7`: scheduled task chain executes successfully for 3 consecutive cycles
- `G8`: synthetic anomalies produce incident open and close transitions automatically
- `G9`: confidence values and manual override lineage are queryable and auditable
- Incident history is complete and reconstructs full lifecycle per incident
- Sprint 4 handoff includes validated monitoring/incident operational runbook

## Sprint 4 (Weeks 7-8): Remediation + UX + Release

### Deliverables
- Remediation workflow tables and procedures
- Streamlit pages: Setup, Rule Library, Monitoring, Incidents, Remediation, Admin
- Cost controls and diagnostics export
- Release runbook + rollback checklist

### Exit Gates
- `G10` UAT passes P0 scenarios
- `G11` budget/allowlist controls enforce execution limits
- `G12` upgrade migration has zero data loss

### Execution Plan

#### Sprint Objective
Finalize production readiness by implementing remediation workflows, completing operational UX, enforcing cost controls, and executing a release/rollback-ready handoff with validated upgrade safety.

#### Workstream Breakdown
- **W1. Remediation Workflow Completion**
  - Finalize remediation entities and status lifecycle for assignment, SLA, escalation, and closure
  - Implement/update procedures for remediation creation, reassignment, status transitions, and completion evidence
  - Ensure remediation links cleanly to incidents/alerts with traceable ownership and timestamps

- **W2. UX Finalization (Streamlit)**
  - Complete pages for Setup, Rule Library, Monitoring, Incidents, Remediation, and Admin
  - Add query-backed views for key operational actions (run checks, approve/publish rules, open/close incidents, manage tasks)
  - Validate UX pathways against P0 scenarios used in UAT

- **W3. Governance + Cost Controls**
  - Enforce budget/allowlist guardrails in execution paths that call Cortex features
  - Add diagnostics exports for usage, guardrail breaches, and model invocation summaries
  - Verify controls block disallowed models and over-budget execution attempts

- **W4. Release Hardening + Migration Safety**
  - Execute end-to-end upgrade path and migration checks from prior app version(s)
  - Validate rollback playbook with explicit pre/post checks and data integrity snapshots
  - Publish release runbook with deployment, verification, rollback, and support triage steps

#### Day-by-Day Suggested Sequence (10 Working Days)
- **Day 1-2:** remedation workflow completion + procedure hardening
- **Day 3-4:** Streamlit page completion and operational wiring
- **Day 5:** cost guardrail and allowlist enforcement verification
- **Day 6:** diagnostics export and admin controls finalization
- **Day 7-8:** UAT execution on P0 scenario suite and defect closure
- **Day 9:** upgrade/rollback rehearsal with migration integrity checks
- **Day 10:** gate review (`G10-G12`) and production release sign-off

#### Dependencies
- Sprint 1-3 foundations stable and deployed in target test environment
- UAT participants and P0 scenario definitions available before week start
- Access to representative upgrade baseline datasets for migration validation

#### Risks and Mitigations
- **Risk:** UAT uncovers late UX/operational gaps  
  **Mitigation:** prioritize P0-critical actions first and reserve explicit defect-burn window (Day 7-8)
- **Risk:** guardrails configured but not enforced at runtime  
  **Mitigation:** run negative-path tests for budget exceed and disallowed model attempts (`G11`)
- **Risk:** upgrade scripts preserve schema but lose payload fidelity  
  **Mitigation:** perform pre/post row-count and checksum-style comparisons for critical tables (`G12`)

#### Definition of Done (Sprint 4)
- `G10`: UAT passes all P0 scenarios with documented evidence
- `G11`: budget and model allowlist controls actively block violating execution
- `G12`: upgrade migration demonstrates zero data loss on validation dataset
- Release runbook and rollback checklist are complete, reviewed, and executable by ops
- App is release-ready with no open blocking defects

## Non-Negotiable Constraint
All published DQ UDFs must be Cortex-backed and must not include non-Cortex fallback paths.
