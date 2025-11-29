import streamlit as st

from utils.customCss import apply_custom_css
from utils.sheetConnect import load_sheet
from utils.sidebar import render_sidebar
from utils.dataPreprocess import preprocess_internal_number
from utils.auth import authenticate

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

    # extract users
    if 'users' not in st.session_state:
        users = load_sheet(key='MAIN_SPREADSHEET_ID', sheet_name='Users')
        st.session_state.users = users

    # extract internal numbers
    if 'internal_numbers' not in st.session_state:
        internal_numbers_df = load_sheet(key='INTERNAL NUMBERS', sheet_name='Numbers')
        internal_numbers = [preprocess_internal_number(num) for num in internal_numbers_df['Number'].tolist()]
        st.session_state.internal_numbers = internal_numbers
    
    render_sidebar()

    # check if logged in
    if not st.session_state.get('logged_in', False):
        st.warning("لطفاً برای دسترسی به داشبورد وارد شوید.")
        authenticate()
        return
    else:
        st.success(f"شما با موفقیت وارد شدید. خوش آمدید، {st.session_state.userdata['name']}!")

        # button to go to different pages
        pages = ["1-تماس ها", "2-ورود و خروج", "3-نظرسنجی ها", "4-میس کال ها"]

        for page in pages:
            if st.button(page, width='stretch'):
                st.switch_page(f"pages/{page}.py")
if __name__ == "__main__":
    main()
