# Architecture Notes

## Runtime Model
- Native App objects are created in consumer account schemas.
- Streamlit is embedded for operational UX.
- Tasks + Streams + Dynamic Tables drive continuous monitoring.

## Cortex-Only Rule Strategy
- Every DQ UDF routes validation/classification decisions through `SNOWFLAKE.CORTEX.*`.
- Rule metadata stores model, prompt template/version, threshold, confidence.
- Every invocation is audited for governance and cost control.

## Main Data Flows
1. Ingestion events -> `APP_EVENTS` append-only tables
2. Signal derivation -> `APP_ENGINE` dynamic tables/views
3. Alerts/incidents -> incident lifecycle procedures
4. Remediation -> task assignment and SLA tracking
