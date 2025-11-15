import streamlit as st
import pandas as pd
import plotly.express as px

from utils.customCss import apply_custom_css
from utils.sidebar import render_sidebar
from utils.voipConnect import VoipDBConnection
from utils.logger import log_event

voip_conn = VoipDBConnection(host=st.secrets['VOIP_DB']['host'], user=st.secrets["VOIP_DB"]['user'], password=st.secrets["VOIP_DB"]['password'], database='smartPBX')

# CONFIG
CONFIG = {
    "queues": ["5100", "5200", "5300", '5600'],
}

@st.cache_data(ttl=600, show_spinner=False)
def execute_query(query: str, queues: list, start_date: pd.Timestamp, end_date: pd.Timestamp, internal_numbers_voip: tuple, filtered_members_voip_: tuple) -> pd.DataFrame:
    """Execute a SQL query and return the result as a DataFrame."""

    with voip_conn as conn:
        query_formatted = query.format(
            queues=tuple(queues),
            start_date=start_date,
            end_date=end_date,
            internal_numbers_voip=tuple(internal_numbers_voip),
            filtered_members_voip_=tuple(filtered_members_voip_),
        )
        return pd.read_sql_query(query_formatted, conn)

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
                filtered_members['team'].apply(lambda x: team in [y.strip() for y in x.split('|')])
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
        
        # expert exit and login in day
        # user voip_ids
        voip_ids_map = [
            f'Local/{voip_id}@from-queue' for voip_id in voip_ids
        ]
        if len(voip_ids_map) == 1:
            voip_ids_map = str(voip_ids_map[0])
        st.write(voip_ids_map)
         # query    
        login_logout_query = """
        SELECT
            time, agent, event 
        FROM queue_log
        WHERE agent IN {filtered_members_voip_}
            AND queuename IN {queues}
            AND DATE(time) BETWEEN '{start_date}' AND '{end_date}'
            AND (event = 'ADDMEMBER' OR event = 'REMOVEMEMBER')
        """

        login_logout_df = execute_query(
            login_logout_query,
            CONFIG['queues'],
            start_date,
            end_date,
            internal_numbers_voip,
            filtered_members_voip_=voip_ids_map
        )

        with st.expander("ورود و خروج کارشناسان"):
            for voip_id in login_logout_df['agent'].unique():
                member_name = filtered_members[
                    filtered_members['voip_id'].apply(lambda x: voip_id.replace('Local/', '').replace('@from-queue', '') in [y.strip() for y in str(x).split('|')])
                ]['name'].values
                if member_name.size > 0:
                    st.subheader(f"کارشناس: {member_name[0]} ({voip_id.replace('Local/', '').replace('@from-queue', '')})")
                member_events = login_logout_df[login_logout_df['agent'] == voip_id].sort_values(by='time', ascending=False).reset_index(drop=True)
                st.dataframe(member_events)

        # convert rate 
        # query = """
        # SELECT
        #     callid, time, data2 as phone_number, agent, event,

        #     CAST(time AS DATE) AS call_date,

        #     -- shift
        #     CASE
        #         WHEN EXTRACT(HOUR FROM time) BETWEEN 8  AND 16 THEN 'Morning Shift'   -- 08:00–16:59
        #         WHEN EXTRACT(HOUR FROM time) BETWEEN 17 AND 23 THEN 'Night Shift'     -- 17:00–23:59    
        #         ELSE
        #         'Midnight Shift'                                             -- 00:00–07:59
        #         END AS shift,
        #     -- team
        #     CASE 
        #         WHEN queuename = '5100' THEN 'sales'
        #         WHEN queuename = '5200' THEN 'support'
        #         ELSE 'others'
        #     END AS team,

        #     COUNT(DISTINCT data2) AS unique_calls
        # FROM queue_log
        # WHERE
        #     DATE(time) BETWEEN '{start_date}' AND '{end_date}'
        #     AND (queuename = '5100' OR queuename = '5200')
        #     AND callid NOT IN(
        #         SELECT callid FROM queue_log
        #         WHERE data IN {internal_numbers_voip}
        #     )
        # """
        # if team != 'all' or shift != 'all' or expert != 'all':
        #     query += """AND agent IN {filtered_members_voip_}"""

        # calls_df = execute_query(query, start_date, end_date, internal_numbers_voip, filtered_members_voip_)
        # st.dataframe(calls_df)




def load_qc_manager():
    st.write("QC Manager content goes here.")

    #  filters
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        start_date = st.date_input("Start date", value=pd.to_datetime("today") - pd.Timedelta(days=1))
    with col2:
        end_date = st.date_input("End date", value=pd.to_datetime("today"))
    with col3:
        team = st.selectbox("تیم", options=st.session_state.users['team']
                            .apply(lambda x: x.split('|')).explode().unique().tolist())
    with col4:
        shift = st.selectbox("شیفت", options=['همه'] + [x for x in st.session_state.users['shift'
                                        ].apply(lambda x: x.split('|')).explode().unique().tolist() if x != '-'])
    with col5:
        expert = st.selectbox("کارشناس", options=["همه"] + st.session_state.users[
            st.session_state.users['role'].str.contains('expert')
        ]['name'].tolist())

    
def load_team_manager():
    st.write("Team Manager content goes here.")

    manager_teams = [x.strip() for x in st.session_state.userdata['team'].split('|')]

    manager_members = st.session_state.users[
        st.session_state.users['team'].apply(lambda x: any(team in [y.strip() for y in x.split('|')] for team in manager_teams))
    ]['name'].unique().tolist()

    shifts = st.session_state.users[
        st.session_state.users['team'].apply(lambda x: any(team in [y.strip() for y in x.split('|')] for team in manager_teams))
    ]['shift'].apply(lambda x: [y.strip() for y in x.split('|')]).explode().unique().tolist()


    #  filters
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        start_date = st.date_input("Start date", value=pd.to_datetime("today") - pd.Timedelta(days=1))
    with col2:
        end_date = st.date_input("End date", value=pd.to_datetime("today"))
    with col3:
        team = st.selectbox("تیم", options=['all'] + manager_teams)
    with col4:
        shift = st.selectbox("شیفت", options=['all'] + shifts)
    with col5:
        expert = st.selectbox("کارشناس", options=["all"] + manager_members)

def load_expert():
    st.write("Expert content goes here.")

        #  filters
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start date", value=pd.to_datetime("today") - pd.Timedelta(days=1))
    with col2:
        end_date = st.date_input("End date", value=pd.to_datetime("today"))

main()  