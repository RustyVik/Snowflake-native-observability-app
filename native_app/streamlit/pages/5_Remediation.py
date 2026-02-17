import streamlit as st

st.header('Remediation')

st.write('Manage remediation assignments, lifecycle transitions, escalations, and completion evidence.')

st.code(
    """
CALL APP_ENGINE.sp_create_task_for_latest_incident('SMOKE_SYNTHETIC', 'Investigate anomaly root cause', 'oncall_user');
CALL APP_ENGINE.sp_update_remediation_status('<task_id>', 'IN_PROGRESS', 'Investigation started');
CALL APP_ENGINE.sp_complete_remediation_task('<task_id>', 'Issue resolved and validated');
CALL APP_ENGINE.sp_get_remediation_summary();
""".strip(),
    language='sql'
)
