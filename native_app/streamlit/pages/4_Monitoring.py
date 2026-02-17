import streamlit as st

st.header('Monitoring')

st.write('Run monitoring cycles, inspect anomaly signals, and verify task-chain health.')

st.code(
    """
CALL APP_ENGINE.sp_run_monitoring_cycle('manual_monitoring_cycle');
CALL APP_ENGINE.sp_detect_anomalies();
CALL APP_ENGINE.sp_open_close_incidents();
""".strip(),
    language='sql'
)
