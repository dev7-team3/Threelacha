import streamlit as st

def price_card(content, bg_color):
    st.markdown(
        f"""
        <div style="
            background-color: {bg_color};
            padding: 16px;
            border-radius: 12px;
            height: 90px;
            display: flex;
            align-items: center;
            font-weight: 600;
        ">
            {content}
        </div>
        """,
        unsafe_allow_html=True
    )
