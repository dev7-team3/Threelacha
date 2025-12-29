import streamlit as st

def render_season_selector():
    st.subheader("제철 식재료")
    st.button("🌿 제철 식재료 보기", use_container_width=True)
    return st.selectbox("과일 선택", ["무화과", "귤", "사과", "배"])
