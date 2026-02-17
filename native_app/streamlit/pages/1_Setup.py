import streamlit as st

st.header('Setup & Cortex Readiness')

st.write('Run these Sprint 1 commands from your deployment workflow:')

st.code(
	"""
CALL APP_CORE.sp_setup_app();
CALL APP_CORE.sp_validate_cortex_access();
CALL APP_CORE.sp_assert_ready_for_activation();
CALL APP_CORE.sp_healthcheck();
""".strip(),
	language='sql'
)

st.markdown('''
Exit gates:
- `G1`: setup/upgrade/healthcheck pass
- `G2`: activation guard blocks when Cortex prerequisites fail
- `G3`: Cortex call audit evidence exists
''')
