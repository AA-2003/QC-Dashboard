import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from sqlalchemy import create_engine

from utils.customCss import apply_custom_css
from utils.sidebar import render_sidebar
from utils.logger import log_event

# CONFIG
CONFIG = {
    "queues": ["5100", "5200", "5300", '5600'],
}


@st.cache_data(ttl=600, show_spinner=False)
def execute_query(query_formatted, engine_string) -> pd.DataFrame:
    """Execute a SQL query and return the result as a DataFrame."""
    try:
        engine = create_engine(engine_string)
        return pd.read_sql_query(query_formatted, engine)
    except Exception as e:
        print(f"Error executing query: {e}")
        log_event(user=st.session_state.userdata['name'], event_type='error', message=f"Error executing query: {e}\nQuery: {query_formatted}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def main():
    """
    """
    st.set_page_config(
        page_title="surveys",
        layout="wide",
        initial_sidebar_state="auto"
    )
    apply_custom_css()

    st.title("نظرسنجی‌ها")

    render_sidebar()

    if not st.session_state.get('logged_in', False):
        st.warning("لطفاً برای دسترسی به این صفحه وارد شوید.")
        return
    else:
        role = st.session_state.userdata['role']
        switcher = {
            'Admin': load_admin,
            'QC': load_admin,
            'Team Manager': load_team_manager,
            'Supervisor': load_supervisor,
            'Expert': load_expert
        }
        # Get the function from switcher dictionary
        func = switcher.get(role, lambda: st.error("نقش کاربری نامعتبر است."))
        # Execute the function
        func()

def load_admin():
    """Load admin specific content."""

    users = st.session_state.users

    teams = list(set(st.session_state.users['team'].apply(
        lambda x: [y.strip() for y in x.split('|')]).explode().unique().tolist() + ['All']))
    shifts = list(set(st.session_state.users['shift'].apply(
        lambda x: [y.strip() for y in x.split('|')]).explode().unique().tolist() + ['All']))
    experts = list(set(st.session_state.users[
        st.session_state.users['role'].str.contains('Expert|Supervisor')
    ]['name'].tolist() + ['All']))

    with st.form("filter_form"):
        #  filters
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            start_date = st.date_input("Start date", value=pd.to_datetime("today") - pd.Timedelta(days=0))
        with col2:
            end_date = st.date_input("End date", value=pd.to_datetime("today"))
        with col3:
            team = st.selectbox("تیم", options=teams, index=teams.index('All'))
        with col4:
            shift = st.selectbox("شیفت", options=shifts, index=shifts.index('All'))
        with col5:
            expert = st.selectbox("کارشناس", options=experts, index=experts.index('All'))

        submitted = st.form_submit_button("اعمال فیلترها")

    if submitted:
        log_event(
            st.session_state.userdata['name'],
            "apply_filters",
            f"""User {st.session_state.userdata['name']} applied filters on Admin page.
            start_date: {start_date}, end_date: {end_date}, team: {team}, shift: {shift}, expert: {expert}."""
        )

        # apply filters
        filtered_members = st.session_state.users.copy()
        if team != 'All':
            filtered_members = filtered_members[
                filtered_members['team'] == team
            ]
        if shift != 'All':
            filtered_members = filtered_members[
                filtered_members['shift'].apply(lambda x: shift in [y.strip() for y in x.split('|')])
            ]
        if expert != 'All':
            filtered_members = filtered_members[
                filtered_members['name'] == expert
            ]

        voip_names = [item for sublist in filtered_members['voip_name'].apply(lambda x: [y.strip() for y in x.split('|')] if x != '-' else []).tolist() for item in sublist]
        voip_ids = [item for sublist in filtered_members['voip_id'].apply(lambda x: [y.strip() for y in str(x).split('|')] if x != '-' else []).tolist() for item in sublist]
        filtered_members_voip_ = set(voip_names + voip_ids)

        if not filtered_members_voip_:
            st.warning("هیچ کارشناس فیلتر شده‌ای برای نمایش وجود ندارد.")
            return
        
        internal_numbers_voip = st.session_state.internal_numbers

        query = """
SELECT
    callid, time, data2 as phone_number, agent, event
FROM queue_log
WHERE
    DATE(time) BETWEEN '{start_date}' AND '{end_date}'
    AND queuename IN {queues}
    AND callid IN(
        SELECT DISTINCT(callid) FROM queue_log
        WHERE data2 not IN {internal_numbers_voip}
        AND agent IN {filtered_members_voip_}
        AND event IN ('ABANDON', 'RINGNOANSWER', 'EXITWITHTIMEOUT', 'RINGCANCELED')
    )
        """
        query_formatted = query.format(
            queues=tuple(CONFIG['queues']),
            start_date=start_date,
            end_date=end_date,
            internal_numbers_voip=tuple(internal_numbers_voip),
            filtered_members_voip_=tuple(filtered_members_voip_),
        )

        engine_string = f"mysql+pymysql://{st.secrets['VOIP_DB']['user']}:{st.secrets['VOIP_DB']['password']}@{st.secrets['VOIP_DB']['host']}/{st.secrets['VOIP_DB']['database']}"
        calls_df = execute_query(query_formatted, engine_string)


        st.write(calls_df)


def load_team_manager():
    st.write("Team Manager content goes here.")

def load_supervisor():
    st.write("Supervisor content goes here.")

def load_expert():
    st.write("Expert content goes here.")

main()  