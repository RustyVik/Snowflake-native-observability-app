import streamlit as st

st.header('Admin')

st.write('Review guardrails, diagnostics export, and upgrade migration integrity checks.')

st.code(
    """
CALL APP_CORE.sp_export_diagnostics(24);
CALL APP_CORE.sp_capture_migration_baseline();
CALL APP_CORE.sp_upgrade_app();
CALL APP_CORE.sp_validate_migration_integrity(NULL);
""".strip(),
    language='sql'
)
