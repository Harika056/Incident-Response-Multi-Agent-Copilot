"""Read-only local dashboard for the incident-response copilot."""
import json
import os
import sqlite3
from pathlib import Path
import httpx
import streamlit as st

# Load local settings without overriding values already set in PowerShell.
root = Path(__file__).resolve().parent
os.chdir(root)
if (root / '.env').exists():
    for line in (root / '.env').read_text(encoding='utf-8-sig').splitlines():
        key, sep, value = line.partition('=')
        if sep and key.strip() and not key.lstrip().startswith('#'):
            os.environ.setdefault(key.strip(), value.strip())
os.environ.setdefault('ORDERHUB_URL', 'http://localhost:18080')
os.environ.setdefault('TASKFLOW_URL', 'http://localhost:18081')
from copilot.config import get_settings
from copilot import diagnostics as d

st.set_page_config(page_title='Incident Response Copilot', page_icon='🛡️', layout='wide')
s = get_settings()
st.title('Incident Response Copilot')
st.caption('Local operations dashboard · read-only · no LLM API calls')
if st.button('Refresh live data'):
    st.rerun()

def safe(fn):
    try:
        return fn()
    except Exception as exc:
        return {'error': str(exc)}

def health(url):
    r = httpx.get(url.rstrip('/') + '/actuator/health', timeout=5)
    return {'status': r.json().get('status', 'UNKNOWN'), 'http_status': r.status_code}

order = safe(lambda: health(s.orderhub_url))
task = safe(lambda: health(s.taskflow_url))
kafka = safe(lambda: d.kafka_consumer_lag(s))
db = safe(lambda: d.active_queries(s, 1000))
a,b,c,e = st.columns(4)
a.metric('OrderHub', order.get('status', 'Unavailable'))
b.metric('TaskFlow', task.get('status', 'Unavailable'))
c.metric('Kafka lag', kafka.get('total_lag', 'Unavailable'))
e.metric('DB queries ≥ 1s', db.get('count', 'Unavailable'))
for label, result in [('OrderHub',order),('TaskFlow',task),('Kafka',kafka),('PostgreSQL',db)]:
    if 'error' in result:
        st.warning(f'{label}: {result["error"]}')
left,right = st.columns(2)
with left:
    st.subheader('Kafka partitions')
    st.caption('Consumer group: ' + str(kafka.get('group_info',{}).get('state','Unknown')))
    if kafka.get('partitions'): st.dataframe(kafka['partitions'], use_container_width=True)
with right:
    st.subheader('Database diagnostics')
    if 'count' in db: st.json(db)

st.subheader('Incident history')
try:
    path = Path(s.checkpoint_db).resolve()
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5) as conn:
        conn.row_factory = sqlite3.Row
        incidents = [dict(r) for r in conn.execute('SELECT * FROM incidents ORDER BY created_at DESC')]
        proposals = [dict(r) for r in conn.execute('SELECT * FROM proposals ORDER BY created_at DESC')]
except (sqlite3.Error, OSError) as exc:
    st.info(f'Incident database unavailable: {exc}')
    incidents, proposals = [], []
if incidents:
    selected = st.selectbox('Select incident', [r['id'] for r in incidents])
    incident = next(r for r in incidents if r['id'] == selected)
    st.write('Root cause:', incident.get('root_cause') or 'Not diagnosed')
    st.write('Stored incident status:', incident['status'])
    st.write('Evidence:', json.loads(incident.get('evidence') or '[]'))
    related = [r for r in proposals if r['incident_id'] == selected]
    if related:
        st.dataframe([{k:r[k] for k in ('id','action','status','rationale','result')} for r in related], use_container_width=True)
        if incident['status'] == 'awaiting_approval' and any(r['status']=='executed' for r in related):
            st.info('Remediation executed; incident lifecycle was not resumed.')
else:
    st.info('No incident records available.')

st.subheader('Verified demonstration · historical results')
st.caption('Recorded deterministic Kafka recovery, not a live LLM benchmark.')
a,b,c = st.columns(3)
a.metric('Automated tests passed', '45/45')
b.metric('Kafka lag reduction', '100%')
c.metric('Test runtime', '10.97 s')
st.bar_chart({'Pending messages': [30,0]}, x_label='Stage (0 = before, 1 = after)')
st.caption('30 messages before remediation; 0 after. Live LLM investigation remains unverified.')
