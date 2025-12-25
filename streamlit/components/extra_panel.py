import streamlit as st

def render_extra_panel(popular_items):
    st.markdown("### 포기하지 않은 나노바나나")
    st.image("assets/nano_nugul.jpg", use_container_width=True)
    st.info("오늘은 양배추가 너무 비싸서 상추를 공략!")

    st.divider()
    st.markdown("### 요즘 인기 있는 식재료")

    for i, item in enumerate(popular_items, start=1):
        st.write(f"{i}️ {item}")

