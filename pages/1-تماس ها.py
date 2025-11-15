from multiprocessing import queues
import streamlit as st
import pandas as pd
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
        # ====call count per day====
        # ==========================
        
        if len(filtered_members_voip_) == 1:
            filtered_members_voip_ = str(list(filtered_members_voip_)[0])
        
        # query
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
        AND event not IN ('DID', '', '', '')
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


        # plotting
        if calls_df.empty:
            st.warning("هیچ داده‌ای برای نمایش وجود ندارد با فیلترهای انتخاب شده.")
            return
        
        calls_df['call_date'] = pd.to_datetime(calls_df['time']).dt.date
        call_counts_agg = calls_df[calls_df['event'] == 'ENTERQUEUE'].groupby(
            ['call_date']).agg(total_calls=('phone_number', 'nunique')).reset_index()
        call_counts_agg = call_counts_agg.set_index('call_date')

        all_dates = pd.date_range(start=start_date, end=end_date)
        call_counts_agg = call_counts_agg.reindex(all_dates, fill_value=0)
        call_counts_agg.index.name = 'call_date'

        fig = px.line(
            call_counts_agg, x=call_counts_agg.index,
            y='total_calls', title='تعداد تماس‌های یکتا در روز',
            markers=True, 
            line_shape='spline',
            template='plotly_white'
            )
        fig.update_layout(
            xaxis_title='تاریخ', 
            yaxis_title='تعداد تماس‌های یکتا',
            title_x=0.85,
            xaxis={'side': 'bottom'},
            yaxis={'side': 'right'},
            font=dict(family="IranSans", size=14),
        )
        st.plotly_chart(fig, width="stretch")
        
        with st.expander("جزئیات تماس ها"):
            st.dataframe(calls_df.sort_values(by='time', ascending=False))

        # ==========================
        # = avg duration to answer =
        # ==========================

        st.header("مدت زمان متوسط پاسخگویی برای هر کارشناس")

        avg_answer_df = calls_df.groupby('callid')

        avg_answer_data = []

        for grouped_callid, group in avg_answer_df:
            enterqueue_times = group[group['event'] == 'ENTERQUEUE']['time'].sort_values().tolist()
            leavequeue_times = group[group['event'] == 'CONNECT']['time'].sort_values().tolist()
            agent = group[group['event'] == 'CONNECT']['agent'].sort_values().tolist()
            if enterqueue_times and leavequeue_times:
                enter_time = pd.to_datetime(enterqueue_times[0])
                leave_time = pd.to_datetime(leavequeue_times[0])
                duration = (leave_time - enter_time).total_seconds()
                avg_answer_data.append({
                    'callid': grouped_callid,
                    'answer_duration_seconds': duration,
                    'agent': agent[0]
                })
        avg_answer_df_final = pd.DataFrame(avg_answer_data)
        
        if avg_answer_df_final.empty:
            st.warning("هیچ داده‌ای برای نمایش مدت زمان متوسط پاسخگویی وجود ندارد با فیلترهای انتخاب شده.")
            return

        st.dataframe(avg_answer_df_final.groupby('agent').agg(
            avg_answer_duration_seconds=('answer_duration_seconds', 'mean'),
            total_answered_calls=('callid', 'nunique')
        ).reset_index().sort_values(by='avg_answer_duration_seconds').reset_index(drop=True))

        # ==========================
        # ==== avg talking time ====
        # ==========================
        st.header("مدت زمان متوسط مکالمه برای هر کارشناس")

        avg_talking_df = calls_df.groupby('callid')
        avg_talking_data = []
        for grouped_callid, group in avg_talking_df:
            connect_times = group[group['event'] == 'CONNECT']['time'].sort_values().tolist()
            disconnect_times = group[group['event'].isin(['COMPLETECALLER', 'COMPLETEAGENT'])]['time'].sort_values().tolist()
            agent = group[group['event'] == 'CONNECT']['agent'].sort_values().tolist()
            if connect_times and disconnect_times:
                connect_time = pd.to_datetime(connect_times[0])
                disconnect_time = pd.to_datetime(disconnect_times[0])
                duration = (disconnect_time - connect_time).total_seconds()
                avg_talking_data.append({
                    'callid': grouped_callid,
                    'talking_duration_seconds': duration,
                    'agent': agent[0]
                })
        
        avg_talking_df_final = pd.DataFrame(avg_talking_data)
        if avg_talking_df_final.empty:
            st.warning("هیچ داده‌ای برای نمایش مدت زمان متوسط مکالمه وجود ندارد با فیلترهای انتخاب شده.")
            return
        st.dataframe(avg_talking_df_final.groupby('agent').agg(
            avg_talking_duration_seconds=('talking_duration_seconds', 'mean'),
            total_answered_calls=('callid', 'nunique')
        ).reset_index().sort_values(by='avg_talking_duration_seconds').reset_index(drop=True))


        # ==========================
        # ======== survays =========
        # ==========================




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