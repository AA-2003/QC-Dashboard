import streamlit as st



def apply_custom_css():
    st.markdown(
        """
        <style>
        /* استفاده از فونت Tahoma برای همه‌ی متن‌ها و کوچکتر کردن اندازه فونت کلی */
        html, body, div, input, textarea, label, select, button, p, h1, h2, h3, h4, h5, h6 {
            font-family: Tahoma, sans-serif !important;
            font-size: 16px !important;
        }
        h1 { font-size: 1.6em !important; }
        h2 { font-size: 1.3em !important; }
        h3 { font-size: 1.1em !important; }
        h4, h5, h6 { font-size: 1em !important; }

        /* تنظیم جهت راست‌به‌چپ برای فارسی */
        .main, .block-container, [data-testid="stSidebar"] {
            direction: rtl !important;
            text-align: right !important;
        }

        /* اجبار راست‌چین برای فرم‌ها و ویجت‌ها */
        input, textarea, select,
        .stSelectbox > div, .stTextInput > div,
        .stTextArea > div, .stDateInput > div {
            direction: rtl !important;
            text-align: right !important;
        }

        /* راست‌چین کردن متن Markdown */
        .stMarkdown {
            direction: rtl !important;
            text-align: right !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )