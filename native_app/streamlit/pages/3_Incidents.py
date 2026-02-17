import streamlit as st

st.header('Incidents')

st.write('Track anomaly alerts, incident lifecycle transitions, and monitoring-cycle health.')

st.code(
	"""
CALL APP_ENGINE.sp_inject_synthetic_anomaly('SMOKE_SYNTHETIC', 0.95);
CALL APP_ENGINE.sp_detect_anomalies();
CALL APP_ENGINE.sp_open_close_incidents();
CALL APP_ENGINE.sp_run_monitoring_cycle('manual_validation_cycle');
""".strip(),
	language='sql'
)
