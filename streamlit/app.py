import streamlit as st
from components.price_cards import price_card
from components.extra_panel import render_extra_panel
from components.season_selector import render_season_selector
from data.sample_data import get_price_summary, get_popular_items


def load_css():
    with open("styles.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()

summary = get_price_summary()
popular_items = get_popular_items()


st.set_page_config(
    page_title="농산물 가격 대시보드",
    layout="wide"
)

if "page" not in st.session_state:
    st.session_state.page = "main"

# -------------------------
# 사이드바 (좌측 탭)
# -------------------------
with st.sidebar:
    st.title("메뉴")

    if st.button("🧺 오늘의 식재료", use_container_width=True):
        st.session_state.page = "main"

    if st.button("🌱 친환경 정보", use_container_width=True):
        st.session_state.page = "eco"

    if st.button("🏪 유통업체별", use_container_width=True):
        st.session_state.page = "dist"

    st.divider()

    st.caption("필터 영역 (추후 추가)")

# -------------------------
# 메인 콘텐츠
# -------------------------
if st.session_state.page == "main":
    st.title("오늘 눈여겨볼 만한 식재료들")
    st.divider()

    center, right = st.columns([3, 1])

    
    # -------------------------
    # 중앙 영역
    # -------------------------
    with center:
        c1, c2, c3 = st.columns(3)

        with c1:
            st.subheader("가장 싸요")
            price_card(price_summary["cheap"])

        with c2:
            st.subheader("가장 비싸요")
            price_card(price_summary["expensive"])

        with c3:
            st.subheader("이건 어때요")
            price_card(price_summary["suggest"])

        st.divider()

        bottom_left, bottom_right = st.columns([1, 2])

        with bottom_left:
            render_season_selector()

        with bottom_right:
            st.info("※ 이 영역에 지도 / 차트가 들어갈 예정입니다.")


    # -------------------------
    # 우측 영역 (추가 기능)
    # -------------------------
    with right:
        render_extra_panel(popular_items)


# =================================================
# 친환경 페이지
# =================================================
elif st.session_state.page == "eco":

    st.title("친환경 살펴보기")
    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("생협이 더 저렴해요!")
        st.info("참깨 500g\n전통시장 15,548원 / 대형마트 23,717원")
        st.info("굴 1kg\n전통시장 20,056원 / 대형마트 27,706원")

    with col2:
        st.subheader("대형마트가 더 저렴해요!")
        st.info("배추 10개\n전통시장 34,384원 / 대형마트 27,165원")
        st.info("사과 10개\n전통시장 29,636원 / 대형마트 27,511원")

    st.divider()
    st.subheader("친환경 농산물 소비 추이 (예시)")
    st.line_chart({
        "2019": [62, 45, 28, 31, 60, 80],
        "2020": [72, 50, 30, 36, 75, 85],
    })


# =================================================
# 유통업체 페이지
# =================================================
elif st.session_state.page == "dist":

    st.title("일반 농수산물 살펴보기")
    st.divider()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("전통시장")
        st.info("굴 1kg · 21,000원")

    with col2:
        st.subheader("대형마트")
        st.info("굴 1kg · 20,000원")

    with col3:
        st.subheader("온라인")
        st.info("굴 1kg · 18,000원")

    st.divider()
    st.subheader("전통시장 지역별 가격 비교")
    st.write("※ 지도 시각화는 추후 추가 예정")