import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

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
        page_title="in&out",
        layout="wide",
        initial_sidebar_state="auto"
    )
    apply_custom_css()

    st.title("ورود و خروج کارشناسان")

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
        log_event(st.session_state.userdata['name'], "apply_filters", f"""User {st.session_state.userdata['name']} applied filters on Admin page.
start_date: {start_date}, end_date: {end_date}, team: {team}, shift: {shift}, expert: {expert}.""")
        
        # apply filters
        filtered_members = st.session_state.users.copy()
        if team != 'All':
            filtered_members = filtered_members[
                filtered_members['team'].apply(lambda x: team in [y.strip() for y in x.split('|')])
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
        
        # expert exit and login in day
        # user voip_ids
        voip_ids_map = [
            f'Local/{voip_id}@from-queue' for voip_id in voip_ids
        ]

        # query    
        login_logout_query = """
        SELECT
            time, agent, event, queuename 
        FROM queue_log
        WHERE agent IN {filtered_members_voip_}
            AND queuename IN {queues}
            AND DATE(time) BETWEEN '{start_date}' AND '{end_date}'
            AND (event = 'ADDMEMBER' OR event = 'REMOVEMEMBER')
        """

        query_formatted = login_logout_query.format(
            queues=tuple(CONFIG['queues']),
            start_date=start_date,
            end_date=end_date,
            internal_numbers_voip=tuple(internal_numbers_voip),
            filtered_members_voip_=tuple(voip_ids_map) if len(voip_ids_map) > 1 else f"('{voip_ids_map[0]}')",
        )

        engine_string = f"mysql+pymysql://{st.secrets['VOIP_DB']['user']}:{st.secrets['VOIP_DB']['password']}@{st.secrets['VOIP_DB']['host']}/{st.secrets['VOIP_DB']['database']}"
        login_logout_df = execute_query(query_formatted, engine_string)

        # =======================
        # === experts in line ===
        # =======================
        st.subheader("کارشناسان در خط")
        if login_logout_df.empty:
            st.warning("هیچ داده‌ای  با فیلترهای انتخاب شده برای نمایش ورود و خروج کارشناسان وجود ندارد.")
            return
        
        in_line_experts = set()

        login_logout_df['time'] = pd.to_datetime(login_logout_df['time'])
        today_login_logouts = login_logout_df[
            login_logout_df['time'].dt.date == pd.to_datetime("today").date()
        ]

        for voip_id in today_login_logouts['agent'].unique():
            member_name = filtered_members[
                filtered_members['voip_id'].apply(lambda x: voip_id.replace('Local/', '').replace('@from-queue', '') in [y.strip() for y in str(x).split('|')])
            ]['name'].values
            
            if member_name.size > 0:
                member_events = login_logout_df[login_logout_df['agent'] == voip_id].sort_values(by='time', ascending=False).reset_index(drop=True)
                if not member_events.empty and member_events.iloc[0]['event'] == 'ADDMEMBER':
                    in_line_experts.add(f"{member_name[0]} ({voip_id.replace('Local/', '').replace('@from-queue', '')})")

        # display results in good way
        if in_line_experts:
            st.dataframe(pd.DataFrame(list(in_line_experts), columns=['کارشناسان در خط']))
        else:
            st.write("هیچ کارشناس فعالی در خط وجود ندارد.")

        
        with st.expander("ورود و خروج کارشناسان"):
            for voip_id in login_logout_df.sort_values(by='time', ascending=False)['agent'].unique():
                member_name = filtered_members[
                    filtered_members['voip_id'].apply(lambda x: voip_id.replace('Local/', '').replace('@from-queue', '') in [y.strip() for y in str(x).split('|')])
                ]['name'].values
                if member_name.size > 0:
                    st.subheader(f"کارشناس: {member_name[0]} ({voip_id.replace('Local/', '').replace('@from-queue', '')})")
                member_events = login_logout_df[login_logout_df['agent'] == voip_id].sort_values(by='time', ascending=False).reset_index(drop=True)
                st.dataframe(member_events.sort_values(by='time', ascending=False).reset_index(drop=True))


        # ===========================
        # === out time per expert ===
        # ===========================

        st.subheader("مدت زمان خارج از خط هر کارشناس")

        out_time_results = []
        for voip_id in login_logout_df['agent'].unique():
            member_name = filtered_members[
                filtered_members['voip_id'].apply(lambda x: voip_id.replace('Local/', '').replace('@from-queue', '') in [y.strip() for y in str(x).split('|')])
            ]['name'].values
            
            if member_name.size > 0:
                member_events = login_logout_df[login_logout_df['agent'] == voip_id].sort_values(by='time').reset_index(drop=True)
                total_out_time = pd.Timedelta(0)
                # check each queue name seperately
                for queuename in member_events['queuename'].unique():
                    queuename_events = member_events[member_events['queuename'] == queuename].reset_index(drop=True).sort_values(by='time')
                    for i in range(len(queuename_events)):
                        if queuename_events.iloc[i]['event'] == 'REMOVEMEMBER':
                            out_time = queuename_events.iloc[i]['time']
                            # Find the next ADDMEMBER event
                            in_time = None
                            for j in range(i+1, len(queuename_events)):
                                if queuename_events.iloc[j]['event'] == 'ADDMEMBER':
                                    in_time = queuename_events.iloc[j]['time']
                                    break
                            if in_time is None:
                                in_time = pd.to_datetime("today")
                            # less than 4 hours check
                            if (in_time - out_time) < pd.Timedelta(hours=4): 
                                total_out_time += (in_time - out_time)

                out_time_results.append({
                    'member_name': member_name[0],
                    'voip_id': voip_id,
                    # TypeError: isoformat() takes exactly 0 positional arguments (1 given)
                    # i want in this foramt "HH:MM:SS"
                    'total_out_time': total_out_time.components.hours * 3600 + total_out_time.components.minutes * 60 + total_out_time.components.seconds if total_out_time >= pd.Timedelta(0) else pd.Timedelta(0)
                })
        out_time_results_df  = pd.DataFrame({
            'نام کارشناس': [res['member_name'] for res in out_time_results],
            'VOIP ID': [res['voip_id'] for res in out_time_results],
            'مدت زمان خارج از خط (ثانیه)': [res['total_out_time'] for res in out_time_results],
        })
        
        out_time_results_df['مدت زمان خارج از خط'] = out_time_results_df['مدت زمان خارج از خط (ثانیه)'].apply(
            lambda x: str(pd.Timedelta(seconds=x))
        )
        st.dataframe(out_time_results_df.sort_values(by='مدت زمان خارج از خط (ثانیه)', ascending=False).reset_index(drop=True))
            
def load_team_manager():
    st.write("Team Manager content goes here.")

def load_supervisor():
    st.write("Supervisor content goes here.")

def load_expert():
    st.write("Expert content goes here.")

main()