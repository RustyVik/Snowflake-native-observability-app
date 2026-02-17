import streamlit as st

st.set_page_config(page_title='Snowflake Native Observability App', layout='wide')

st.title('Snowflake Native Observability App')
st.caption('Cortex-first Data Quality + Observability scaffold')

st.markdown('''
Use the sidebar pages to:
- Validate Cortex readiness
- Manage rule packs
- Monitor anomalies and incidents
- Track remediation tasks
''')
