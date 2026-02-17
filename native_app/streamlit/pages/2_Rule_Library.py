import streamlit as st

st.header('Cortex Rule Library')

st.write('Manage Cortex-backed DQ rules, versions, approvals, and pack promotion.')

st.code(
	"""
CALL APP_DQ.sp_preview_rule_result('udf_dq_email_valid', 'user@example.com');
CALL APP_DQ.sp_apply_rule_pack('baseline_pack', 'RAW.CUSTOMER');
""".strip(),
	language='sql'
)
