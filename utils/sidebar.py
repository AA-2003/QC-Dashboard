import streamlit as st

from utils.auth import authenticate

def refresh_data():
    users = st.session_state.get('users', None)
    internal_numbers = st.session_state.get('internal_numbers', None)
    st.cache_data.clear()
    st.session_state.users  = users
    st.session_state.internal_numbers = internal_numbers
    st.session_state.refresh_trigger = True


def render_sidebar():

    with st.sidebar:
        authenticate()

        if st.session_state.get('logged_in', False):
            st.button("رفرش داده‌ها", on_click=refresh_data)
        
            if st.session_state.get('refresh_trigger', False):
                st.session_state.refresh_trigger = False
                st.rerun()