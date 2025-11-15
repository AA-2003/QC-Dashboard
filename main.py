import streamlit as st
import pandas as pd

from utils.customCss import apply_custom_css
from utils.sheetConnect import load_sheet
from utils.sidebar import render_sidebar
from utils.dataPreprocess import preprocess_internal_number

def main():
    """
    """
    st.set_page_config(
        page_title="داشبورد تضمین کیفیت (QC)",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    apply_custom_css()
    st.title("داشبورد QC")

    # extract internal numbers
    if 'internal_numbers' not in st.session_state:
        internal_numbers_df = load_sheet(key='INTERNAL NUMBERS', sheet_name='Numbers')
        internal_numbers = [preprocess_internal_number(num) for num in internal_numbers_df['Number'].tolist()]
        st.session_state.internal_numbers = internal_numbers

    # extract users
    if 'users' not in st.session_state:
        users = load_sheet(key='MAIN_SPREADSHEET_ID', sheet_name='Users')
        st.session_state.users = users
    
    render_sidebar()

    # check if logged in
    if not st.session_state.get('logged_in', False):
        st.warning("لطفاً برای دسترسی به داشبورد وارد شوید.")
        return
    
    else:
        st.success(f"شما با موفقیت وارد شدید. خوش آمدید، {st.session_state.userdata['name']}!")
        # role = st.session_state.userdata['role']

        # if role in ['admin', 'qc manager']:
        #     #  filters
        #     col1, col2, col3, col4, col5 = st.columns(5)
        #     with col1:
        #         start_date = st.date_input("Start date", value=pd.to_datetime("2022-01-01"))
        #     with col2:
        #         end_date = st.date_input("End date", value=pd.to_datetime("today"))
        #     with col3:
        #         team = st.selectbox("تیم", options=st.session_state.users['team']
        #                             .apply(lambda x: x.split('|')).explode().unique().tolist())
        #     with col4:
        #         shift = st.selectbox("شیفت", options=['همه'] + [x for x in st.session_state.users['shift'
        #                                         ].apply(lambda x: x.split('|')).explode().unique().tolist() if x != '-'])
        #     with col5:
        #         expert = st.selectbox("کارشناس", options=["همه"] + st.session_state.users[
        #             st.session_state.users['role'].str.contains('expert')
        #         ]['name'].tolist())
        
        # elif role == 'team manger':
        #     pass  # to be implemented
        # elif role == 'expert':
        #     pass  # to be implemented

if __name__ == "__main__":
    main()
