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
            start_date = st.date_input("Start date", value=pd.to_datetime("today") - pd.Timedelta(days=7))
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
        log_event(st.session_state.userdata['name'], "apply_filters", f"""User {st.session_state.userdata['name']} applied filters on Admin page.
start_date: {start_date}, end_date: {end_date}, team: {team}, shift: {shift}, expert: {expert}.""")
        
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

        if len(filtered_members_voip_) == 1:
            filtered_members_voip_ = str(list(filtered_members_voip_)[0])
        
        # ========================== 
        # ====call count per day====
        # ==========================
        
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
        AND event not IN ('DID', '')
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

        cols = st.columns(2)
        with cols[0]:
            st.metric("تعداد کل تماس‌ها", call_counts_agg['total_calls'].sum())
        with cols[1]:
            st.metric("میانگین تماس‌ها در روز", round(call_counts_agg['total_calls'].mean(), 2))

        all_dates = pd.date_range(start=start_date, end=end_date)
        call_counts_agg = call_counts_agg.reindex(all_dates, fill_value=0)
        call_counts_agg.index.name = 'call_date'

        fig = px.line(
            call_counts_agg, x=call_counts_agg.index,
            y='total_calls', title='تعداد تماس‌ها در روز',
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
        
        # st.write(calls_df['event'].unique())

        # ============================
        # == avg duration to answer ==
        # ============================

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
        ).reset_index().sort_values(by='total_answered_calls').reset_index(drop=True))
    

        # =========================
        # === lead calls count ===
        # =========================
        # each call more than 60 seconds of duration(bettween connect evenv and COMPLETECALLERor COMPLETEAGENT) is lead call
        st.header("تعداد تماس های لید(بیشتر از ۶۰ ثانیه)")
        
        lead_data = []

        for callid in calls_df['callid'].unique():
            callid_rows = calls_df[calls_df['callid'] == callid]
            enter_queue_row = callid_rows[callid_rows['event'] == 'ENTERQUEUE']
            connect_time_row = callid_rows[callid_rows['event'] == 'CONNECT']
            disconnect_time_row = callid_rows[callid_rows['event'].isin(['COMPLETECALLER', 'COMPLETEAGENT'])]
            if not connect_time_row.empty and not disconnect_time_row.empty:
                connect_time = pd.to_datetime(connect_time_row.iloc[0]['time'])
                disconnect_time = pd.to_datetime(disconnect_time_row.iloc[0]['time'])
                duration = (disconnect_time - connect_time).total_seconds()
                phone = enter_queue_row.iloc[0]['phone_number']
                if duration >= 60:
                    lead_data.append({
                        'callid': callid,
                        'date': connect_time.date(),
                        'agent': connect_time_row.iloc[0]['agent'],
                        'duration_seconds': duration,
                        'phone_number': phone
                    })
        lead_df = pd.DataFrame(lead_data)
        if lead_df.empty:
            st.warning("هیچ داده‌ای برای نمایش تعداد تماس‌های لید وجود ندارد با فیلترهای انتخاب شده.")
            return
        
        cols = st.columns(2)
        with cols[0]:
            st.metric("تعداد کل تماس‌های لید", lead_df['phone_number'].nunique())
        with cols[1]:
            st.metric('میانگین تعداد تماس‌های لید در روز', round(lead_df['phone_number'].nunique() / ((end_date - start_date).days + 1), 2))
        
        with st.expander("جزئیات تماس های لید"):
            st.dataframe(lead_df.sort_values(by='duration_seconds', ascending=False).reset_index(drop=True))

        # lead per day
        fig = px.bar(
            lead_df.groupby('date').agg(total_lead_calls=('phone_number', 'nunique')).reset_index(),
            x='date',
            y='total_lead_calls',
            title='تعداد تماس‌های لید در روز'
        )
        fig.update_layout(
            xaxis_title='تاریخ',
            yaxis_title='تعداد تماس‌های لید',
            title_x=0.85,
            xaxis={'side': 'bottom'},
            yaxis={'side': 'right'},
            font=dict(family="IranSans", size=14),
        )
        st.plotly_chart(fig, use_container_width=True)

            

def load_team_manager():
    st.write("Team Manager content goes here.")

def load_supervisor():
    st.write("Supervisor content goes here.")

def load_expert():
    st.write("Expert content goes here.")



main()