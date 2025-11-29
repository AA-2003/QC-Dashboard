import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

from utils.customCss import apply_custom_css
from utils.sidebar import render_sidebar
from utils.logger import log_event
from utils.auth import authenticate

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
    main function to render the calls page
    """
    st.set_page_config(
        page_title="تماس ها",
        layout="wide",
        initial_sidebar_state="auto"
    )
    apply_custom_css()

    st.title("تماس ها")
    
    render_sidebar()

    if not st.session_state.get('logged_in', False):
        st.warning("لطفاً برای دسترسی به این صفحه وارد شوید.")
        authenticate()
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
        call_count_per_day_query = """
    SELECT
        DATE(time) as call_date,
        COUNT(DISTINCT data2) as total_calls
    FROM queue_log
    WHERE
        DATE(time) BETWEEN '{start_date}' AND '{end_date}'
        AND queuename IN {queues}
        AND event = 'ENTERQUEUE'
        AND callid IN(
            SELECT DISTINCT(callid) FROM queue_log
            WHERE data2 NOT IN {internal_numbers_voip}
            AND agent IN {filtered_members_voip_}
            AND event not IN ('DID', '')
        )
    GROUP BY DATE(time)
    ORDER BY call_date
"""
        call_count_per_day_query = call_count_per_day_query.format(
            queues=tuple(CONFIG['queues']),
            start_date=start_date,
            end_date=end_date,
            internal_numbers_voip=tuple(internal_numbers_voip),
            filtered_members_voip_=tuple(filtered_members_voip_),
        )

        engine_string = f"mysql+pymysql://{st.secrets['VOIP_DB']['user']}:{st.secrets['VOIP_DB']['password']}@{st.secrets['VOIP_DB']['host']}/{st.secrets['VOIP_DB']['database']}"
        call_count_per_day_df = execute_query(call_count_per_day_query, engine_string)
        
        if call_count_per_day_df.empty:
            st.warning("هیچ داده‌ای برای نمایش وجود ندارد با فیلترهای انتخاب شده.")
            return
        
        # plotting
        st.header("تعداد تماس‌ها")
        
        cols = st.columns(2)
        with cols[0]:
            st.metric(
                label="تعداد تماس‌های یکتا",
                value=call_count_per_day_df['total_calls'].sum(),
            )
        with cols[1]:
            st.metric(
                label="میانگین تماس‌های یکتا در روز",
                value=round(call_count_per_day_df['total_calls'].mean(), 0),
            )

        all_dates = pd.date_range(start=start_date, end=end_date)
        call_count_per_day_df = call_count_per_day_df.set_index('call_date').reindex(all_dates, fill_value=0)
        call_count_per_day_df.index.name = 'call_date'
        fig = px.line(
            call_count_per_day_df, x=call_count_per_day_df.index,
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
        st.plotly_chart(fig, use_container_width=True)

        # ============================
        # == avg duration to answer ==
        # ============================

        avg_duration_query = """
    SELECT
        q.callid,
        MIN(CASE WHEN q.event = 'ENTERQUEUE' THEN q.time END) AS enter_time,
        MIN(CASE WHEN q.event = 'CONNECT' THEN q.time END) AS connect_time,
        MIN(CASE WHEN q.event = 'CONNECT' THEN q.agent END) AS agent
    FROM queue_log q
    WHERE
        DATE(q.time) BETWEEN '{start_date}' AND '{end_date}'
        AND q.queuename IN {queues}
        AND q.callid IN(
            SELECT DISTINCT(callid) FROM queue_log
            WHERE data2 NOT IN {internal_numbers_voip}
            AND agent IN {filtered_members_voip_}
            AND event not IN ('DID', '')
        )
    GROUP BY q.callid
    HAVING enter_time IS NOT NULL AND connect_time IS NOT NULL
"""
        avg_duration_query = avg_duration_query.format(
            queues=tuple(CONFIG['queues']),
            start_date=start_date,
            end_date=end_date,
            internal_numbers_voip=tuple(internal_numbers_voip),
            filtered_members_voip_=tuple(filtered_members_voip_),
        )
        avg_duration_df = execute_query(avg_duration_query, engine_string)  
        if avg_duration_df.empty:
            st.warning("هیچ داده‌ای برای نمایش مدت زمان متوسط پاسخگویی وجود ندارد با فیلترهای انتخاب شده.")
            return
        
        # plotting
        st.header("مدت زمان متوسط پاسخگویی برای هر کارشناس")
        avg_duration_df['enter_time'] = pd.to_datetime(avg_duration_df['enter_time'])
        avg_duration_df['connect_time'] = pd.to_datetime(avg_duration_df['connect_time'])
        avg_duration_df['answer_duration_seconds'] = (avg_duration_df['connect_time'] - avg_duration_df['enter_time']).dt.total_seconds()
        st.dataframe(avg_duration_df.groupby('agent').agg(
            avg_answer_duration_seconds=('answer_duration_seconds', 'mean'),
            total_answered_calls=('callid', 'nunique')
        ).reset_index().sort_values(by='avg_answer_duration_seconds').reset_index(drop=True))

        # ==========================
        # ==== avg talking time ====
        # ==========================
        avg_talking_query = """
    SELECT
        q.callid,
        MIN(CASE WHEN q.event = 'CONNECT' THEN q.time END) AS connect_time,
        MIN(CASE WHEN q.event IN ('COMPLETECALLER', 'COMPLETEAGENT') THEN q.time END) AS disconnect_time,
        MIN(CASE WHEN q.event = 'CONNECT' THEN q.agent END) AS agent
    FROM queue_log q
    WHERE
        DATE(q.time) BETWEEN '{start_date}' AND '{end_date}'
        AND q.queuename IN {queues}
        AND q.callid IN(
            SELECT DISTINCT(callid) FROM queue_log
            WHERE data2 NOT IN {internal_numbers_voip}
            AND agent IN {filtered_members_voip_}
            AND event not IN ('DID', '')   
        )
    GROUP BY q.callid
    HAVING connect_time IS NOT NULL AND disconnect_time IS NOT NULL
"""
        avg_talking_query = avg_talking_query.format(
            queues=tuple(CONFIG['queues']),
            start_date=start_date,
            end_date=end_date,
            internal_numbers_voip=tuple(internal_numbers_voip),
            filtered_members_voip_=tuple(filtered_members_voip_),
        )
        avg_talking_df = execute_query(avg_talking_query, engine_string)  
        if avg_talking_df.empty:
            st.warning("هیچ داده‌ای برای نمایش مدت زمان متوسط مکالمه وجود ندارد با فیلترهای انتخاب شده.")
            return
        # plotting
        st.header("مدت زمان متوسط مکالمه برای هر کارشناس")
        avg_talking_df['connect_time'] = pd.to_datetime(avg_talking_df['connect_time'])
        avg_talking_df['disconnect_time'] = pd.to_datetime(avg_talking_df['disconnect_time'])
        avg_talking_df['talking_duration_seconds'] = (avg_talking_df['disconnect_time'] - avg_talking_df['connect_time']).dt.total_seconds()
        st.dataframe(avg_talking_df.groupby('agent').agg(
            avg_talking_duration_seconds=('talking_duration_seconds', 'mean'),
            total_answered_calls=('callid', 'nunique')
        ).reset_index().sort_values(by='total_answered_calls').reset_index(drop=True))

        # =========================
        # === lead calls count ====
        # =========================
        leads_query = """
    SELECT
        callid,
        MIN(CASE WHEN event = 'ENTERQUEUE' THEN data2 END) AS phone_number,
        MIN(CASE WHEN event = 'CONNECT' THEN time END) AS connect_time,
        MIN(CASE WHEN event IN ('COMPLETECALLER', 'COMPLETEAGENT') THEN time END) AS disconnect_time,
        MIN(CASE WHEN event = 'CONNECT' THEN agent END) AS agent
    FROM queue_log
    WHERE
        DATE(time) BETWEEN '{start_date}' AND '{end_date}'
        AND queuename IN {queues}
        AND callid IN(
            SELECT DISTINCT(callid) FROM queue_log
            WHERE data2 NOT IN {internal_numbers_voip}
            AND agent IN {filtered_members_voip_}
            AND event not IN ('DID', '')   
        )
    GROUP BY callid
    HAVING connect_time IS NOT NULL AND disconnect_time IS NOT NULL"""

        leads_query = leads_query.format(
            queues=tuple(CONFIG['queues']),
            start_date=start_date,
            end_date=end_date,
            internal_numbers_voip=tuple(internal_numbers_voip),
            filtered_members_voip_=tuple(filtered_members_voip_),
        )
        leads_df = execute_query(leads_query, engine_string)  
        if leads_df.empty:
            st.warning("هیچ داده‌ای برای نمایش تماس‌های لید وجود ندارد با فیلترهای انتخاب شده.")
            return
        leads_df['connect_time'] = pd.to_datetime(leads_df['connect_time'])
        leads_df['disconnect_time'] = pd.to_datetime(leads_df['disconnect_time'])
        leads_df['talking_duration_seconds'] = (leads_df['disconnect_time'] - leads_df['connect_time']).dt.total_seconds()

        # if talking duration is more than 60 seconds, mark it as valid lead call
        leads_df['is_valid_lead_call'] = leads_df['talking_duration_seconds'] > 60

        filtered_leads_df = leads_df[leads_df['is_valid_lead_call']]

        # metrics
        cols = st.columns(2)
        with cols[0]:
            st.metric(
                label="تعداد تماس‌های لید",
                value=filtered_leads_df['phone_number'].nunique(),
            )
        with cols[1]:
            st.metric(
                label="میانگین تعداد تماس‌های لید در روز",
                value=round(filtered_leads_df['phone_number'].nunique() / (end_date - start_date).days, 0),
            )
        
        st.header("تعداد تماس های لید به ازای هر روز")
        leads_count_per_day_df = filtered_leads_df.groupby(filtered_leads_df['connect_time'].dt.date).agg(
            total_lead_calls=('phone_number', 'nunique')
        ).reset_index().rename(columns={'connect_time': 'date'})

        fig = px.line(
            leads_count_per_day_df, x='date', y='total_lead_calls',
            title='تعداد تماس‌های لید در روز',
            markers=True, 
            line_shape='spline',
            template='plotly_white'
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

        with st.expander("نمایش جزئیات تماس‌های لید"):
            st.dataframe(leads_df.sort_values(by='connect_time', ascending=False).reset_index(drop=True))



def load_team_manager():
    st.write("Team Manager content goes here.")

def load_supervisor():
    st.write("Supervisor content goes here.")

def load_expert():
    st.write("Expert content goes here.")


main()