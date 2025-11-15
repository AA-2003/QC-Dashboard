from multiprocessing import queues
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
        page_title="صفحه ۱",
        layout="wide",
        initial_sidebar_state="auto"
    )
    apply_custom_css()

    st.title("صفحه ۱")

    render_sidebar()

    if not st.session_state.get('logged_in', False):
        st.warning("لطفاً برای دسترسی به این صفحه وارد شوید.")
        return
    else:
        role = st.session_state.userdata['role']
        switcher = {
            'admin': load_admin,
            'qc': load_admin,
            'team manger': load_team_manager,
            'expert': load_expert
        }
        # Get the function from switcher dictionary
        func = switcher.get(role, lambda: st.error("نقش کاربری نامعتبر است."))
        # Execute the function
        func()

def load_admin():
    """Load admin specific content."""

    users = st.session_state.users


    teams = list(set(st.session_state.users['team'].apply(
        lambda x: [y.strip() for y in x.split('|')]).explode().unique().tolist() + ['all']))
    shifts = list(set(st.session_state.users['shift'].apply(
        lambda x: [y.strip() for y in x.split('|')]).explode().unique().tolist() + ['all']))
    experts = list(set(st.session_state.users[
        st.session_state.users['role'].str.contains('Expert|Supervisor')
    ]['name'].tolist() + ['all']))

    

    with st.form("filter_form"):
        #  filters
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            start_date = st.date_input("Start date", value=pd.to_datetime("today") - pd.Timedelta(days=7))
        with col2:
            end_date = st.date_input("End date", value=pd.to_datetime("today"))
        with col3:
            team = st.selectbox("تیم", options=teams, index=teams.index('all'))
        with col4:
            shift = st.selectbox("شیفت", options=shifts, index=shifts.index('all'))
        with col5:
            expert = st.selectbox("کارشناس", options=experts, index=experts.index('all'))

        submitted = st.form_submit_button("اعمال فیلترها")

    if submitted:
        log_event(st.session_state.userdata['name'], "apply_filters", f"""User {st.session_state.userdata['name']} applied filters on Admin page.
start_date: {start_date}, end_date: {end_date}, team: {team}, shift: {shift}, expert: {expert}.""")
        
        # apply filters
        filtered_members = st.session_state.users.copy()
        if team != 'all':
            filtered_members = filtered_members[
                filtered_members['team'] == team
            ]
        if shift != 'all':
            filtered_members = filtered_members[
                filtered_members['shift'].apply(lambda x: shift in [y.strip() for y in x.split('|')])
            ]
        if expert != 'all':
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

        # ==========================
        # ======== survays =========
        # ==========================
        
        if len(filtered_members_voip_) == 1:
            filtered_members_voip_ = str(list(filtered_members_voip_)[0])
        
        # query
        query = """
SELECT
    timestamp, agent_id, queue_number, caller_id as phone_number, unique_id as callid, agent_rate as rate
FROM smart_survey
WHERE
    DATE(timestamp) BETWEEN '{start_date}' AND '{end_date}'
    AND queue_number IN {queues}
    AND agent_id IN {filtered_members_voip_}
        """
        query_formatted = query.format(
            queues=tuple(CONFIG['queues']),
            start_date=start_date,
            end_date=end_date,
            internal_numbers_voip=tuple(internal_numbers_voip),
            filtered_members_voip_=tuple(filtered_members_voip_),
        )
        engine_string = f"mysql+pymysql://{st.secrets['VOIP_DB']['user']}:{st.secrets['VOIP_DB']['password']}@{st.secrets['VOIP_DB']['host']}/{st.secrets['VOIP_DB']['database']}"
        surveys_df = execute_query(query_formatted, engine_string)

        st.subheader("نظرسنجی‌ها")
        if surveys_df.empty:
            st.warning("هیچ داده‌ای برای نمایش نظرسنجی‌ها وجود ندارد با فیلترهای انتخاب شده.")
            return
        
        # Create a mapping dictionary, handling duplicate voip_ids by keeping first occurrence
        users['voip_id'] = users['voip_id'].astype(str)
        surveys_df['agent_id'] = surveys_df['agent_id'].astype(str)
        voip_to_name = users.drop_duplicates(subset=['voip_id']).set_index('voip_id')['name'].to_dict()
        surveys_df['agent_name'] = surveys_df['agent_id'].astype(str).map(voip_to_name)

        st.metric("تعداد کل نظرسنجی‌ها", surveys_df.shape[0])    
        st.metric("میانگین رضایت‌مندی", round((surveys_df['rate'].mean()), 2))
        
        st.header("میانگین رضایت‌مندی بر اساس کارشناس")
        avg_rate_df = surveys_df.groupby('agent_name').agg(
            average_rate=('rate', 'mean'),
            total_surveys=('callid', 'nunique')
        ).reset_index().sort_values(by='average_rate', ascending=False)
        
        # normalize 
        z = 1.96  # 95% confidence

        avg_rate_df['normalized_rate'] = (avg_rate_df['average_rate'] - z * (
            avg_rate_df['average_rate'].std() / np.sqrt(avg_rate_df['total_surveys']))).round(2)
        fig = px.bar(
            avg_rate_df.sort_values(by='normalized_rate', ascending=False).head(7),
            x='agent_name',
            y='normalized_rate',
            hover_data=['normalized_rate', 'total_surveys'],
            labels={'agent_name': 'کارشناس', 'normalized_rate': 'امتیاز'},
            title='میانگین رضایت‌مندی بر اساس کارشناس'
        )
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("میانگین رضایت‌مندی بر اساس کارشناس"):
            st.dataframe(avg_rate_df.sort_values(by='normalized_rate', ascending=False).reset_index(drop=True))
        

        with st.expander("نمایش داده‌های خام نظرسنجی‌ها"):
            st.dataframe(surveys_df.sort_values(by='timestamp', ascending=False).reset_index(drop=True))

def load_team_manager():
    st.write("Team Manager content goes here.")

    # manager_teams = [x.strip() for x in st.session_state.userdata['team'].split('|')]

    # manager_members = st.session_state.users[
    #     st.session_state.users['team'].apply(lambda x: any(team in [y.strip() for y in x.split('|')] for team in manager_teams))
    # ]['name'].unique().tolist()

    # shifts = st.session_state.users[
    #     st.session_state.users['team'].apply(lambda x: any(team in [y.strip() for y in x.split('|')] for team in manager_teams))
    # ]['shift'].apply(lambda x: [y.strip() for y in x.split('|')]).explode().unique().tolist()


    # #  filters
    # col1, col2, col3, col4, col5 = st.columns(5)
    # with col1:
    #     start_date = st.date_input("Start date", value=pd.to_datetime("today") - pd.Timedelta(days=1))
    # with col2:
    #     end_date = st.date_input("End date", value=pd.to_datetime("today"))
    # with col3:
    #     team = st.selectbox("تیم", options=['All'] + manager_teams)
    # with col4:
    #     shift = st.selectbox("شیفت", options=['All'] + shifts)
    # with col5:
    #     expert = st.selectbox("کارشناس", options=["All"] + manager_members)


def load_supervisor():
    st.write("Supervisor content goes here.")

def load_expert():
    st.write("Expert content goes here.")

main()  