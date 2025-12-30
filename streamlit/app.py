import os
from pathlib import Path

import streamlit as st

from components.channel_cards import render_channel_comparison_sections
from components.eco_page import render_eco_content
from components.extra_panel import render_extra_panel
from components.price_cards import price_card
from components.region_map import render_selected_item_region_map
from components.season_selector import render_season_selector
from data.athena_connection import get_athena_config
from data.queries.channel_queries import get_channel_comparison_query
from data.sample_data import get_price_summary, get_popular_items
from data.trino_connection import execute_query, get_trino_connection


def load_css():
    base_path = Path(__file__).parent
    with open(base_path / "styles.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


load_css()

summary = get_price_summary()
popular_items = get_popular_items()
conn = get_trino_connection()


st.set_page_config(page_title="농산물 가격 대시보드", layout="wide")

if "page" not in st.session_state:
    st.session_state.page = "main"

# 세션 상태 초기화
if "show_region_map" not in st.session_state:
    st.session_state.show_region_map = False
if "selected_item_nm" not in st.session_state:
    st.session_state.selected_item_nm = None
if "selected_kind_nm" not in st.session_state:
    st.session_state.selected_kind_nm = None

# -------------------------
# 사이드바 (좌측 탭)
# -------------------------
with st.sidebar:
    st.title("메뉴")

    if st.button("🧺 오늘의 식재료", use_container_width=True):
        st.session_state.page = "main"

    if st.button("🌱 친환경 정보", use_container_width=True):
        st.session_state.page = "eco"

    if st.button("🏪 유통업체별 정보", use_container_width=True):
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
            price_card(summary["cheap"], "#eaf2fb")

        with c2:
            st.subheader("가장 비싸요")
            price_card(summary["expensive"], "#fff8e1")

        with c3:
            st.subheader("이건 어때요")
            price_card(summary["suggest"], "#eaf7ea")

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

    # Athena 연결 사용 - 항상 최신 데이터 자동 조회
    use_athena_data = st.checkbox("Athena 데이터베이스 연결 사용", value=True)

    render_eco_content(use_athena_data)


# =================================================
# 유통업체 페이지
# =================================================
elif st.session_state.page == "dist":
    st.title("일반 농수산물 살펴보기")
    st.divider()

    # 데이터 소스 선택 (샘플 데이터 vs 실제 데이터)
    use_real_data = st.checkbox("실제 데이터베이스 연결 사용", value=False)

    if use_real_data:
        # 실제 Trino 데이터베이스 연결 사용
        try:
            # 날짜 필터 추가
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                date_filter = st.date_input("날짜 선택", value=None, key="dist_date")
            with col2:
                category_filter = st.selectbox(
                    "카테고리 선택",
                    ["전체", "식량작물", "채소류", "특용작물", "과일류", "축산물", "수산물"],
                    key="dist_category",
                )
            with col3:
                # 버튼을 아래로 정렬하기 위한 빈 공간 추가
                st.markdown("<br>", unsafe_allow_html=True)
                query_button = st.button(
                    "데이터 조회", type="primary", key="dist_query_button", use_container_width=True
                )

            # 유통 vs 전통 비교 쿼리 생성
            comparison_query = get_channel_comparison_query(
                date_filter=date_filter, category_filter=category_filter, limit=None
            )

            if query_button:
                with st.spinner("데이터를 불러오는 중..."):
                    try:
                        df_comparison = execute_query(comparison_query, conn)

                        if len(df_comparison) > 0:
                            # 세션 상태에 쿼리 결과 저장
                            st.session_state.df_comparison = df_comparison
                            st.session_state.query_date_filter = date_filter
                            st.session_state.query_category_filter = category_filter

                            # 요약 통계
                            st.subheader("📈 요약 통계")
                            summary_col1, summary_col2, summary_col3 = st.columns(3)

                            with summary_col1:
                                avg_yutong = df_comparison["유통_평균가격"].mean()
                                st.metric("유통 평균 가격", f"{avg_yutong:,.0f}원")

                            with summary_col2:
                                avg_jeontong = df_comparison["전통_평균가격"].mean()
                                st.metric("전통 평균 가격", f"{avg_jeontong:,.0f}원")

                            with summary_col3:
                                avg_diff = df_comparison["가격차이"].mean()
                                st.metric("평균 가격 차이", f"{avg_diff:,.0f}원")

                            st.divider()

                            render_channel_comparison_sections(df_comparison)

                            # 선택된 품목이 있으면 지역별 지도 표시
                            render_selected_item_region_map(
                                conn,
                                date_filter=st.session_state.get("query_date_filter"),
                                category_filter=st.session_state.get("query_category_filter"),
                            )

                            st.divider()
                            st.subheader("📊 유통 vs 전통 가격 비교")
                            st.dataframe(df_comparison, use_container_width=True)
                        else:
                            st.info("조회된 데이터가 없습니다.")

                    except Exception as e:
                        st.error(f"데이터 조회 중 오류 발생: {str(e)}")
                        st.info("💡 Trino 서버가 실행 중인지 확인하세요. (docker-compose up -d trino)")

            # 쿼리 버튼이 눌러지지 않았지만 이전에 조회한 데이터가 있고 지도 표시 요청이 있는 경우
            elif "df_comparison" in st.session_state and len(st.session_state.df_comparison) > 0:
                df_comparison = st.session_state.df_comparison

                # 요약 통계
                st.subheader("📈 요약 통계")
                summary_col1, summary_col2, summary_col3 = st.columns(3)

                with summary_col1:
                    avg_yutong = df_comparison["유통_평균가격"].mean()
                    st.metric("유통 평균 가격", f"{avg_yutong:,.0f}원")

                with summary_col2:
                    avg_jeontong = df_comparison["전통_평균가격"].mean()
                    st.metric("전통 평균 가격", f"{avg_jeontong:,.0f}원")

                with summary_col3:
                    avg_diff = df_comparison["가격차이"].mean()
                    st.metric("평균 가격 차이", f"{avg_diff:,.0f}원")

                st.divider()

                render_channel_comparison_sections(df_comparison)

                # 선택된 품목이 있으면 지역별 지도 표시
                render_selected_item_region_map(
                    conn,
                    date_filter=st.session_state.get("query_date_filter"),
                    category_filter=st.session_state.get("query_category_filter"),
                )

                st.divider()
                st.subheader("📊 유통 vs 전통 가격 비교")
                st.dataframe(df_comparison, use_container_width=True)

        except Exception as e:
            st.error(f"연결 오류: {str(e)}")
            st.info("""
            **연결 설정 확인:**
            - Trino 서버가 실행 중인지 확인: `docker-compose ps trino`
            - 환경 변수 설정 확인:
              - `TRINO_HOST`: 기본값 `localhost` (Docker 외부에서 접속 시)
              - `TRINO_PORT`: 기본값 `8082` (Docker 외부에서 접속 시)
            """)

    else:
        # 샘플 데이터 사용 (기존 코드)
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

# 사이드바 하단에 연결 정보 표시
with st.sidebar:
    st.markdown("---")
    st.markdown("### 연결 정보")

    # 현재 페이지에 따라 다른 연결 정보 표시
    if st.session_state.page == "eco":
        database, workgroup = get_athena_config()
        st.info(f"""
        **Athena 설정:**
        - Database: {database}
        - WorkGroup: {workgroup}
        - Region: {os.getenv("AWS_REGION", "ap-northeast-2")}
        """)
    else:
        st.info(f"""
        **Trino 설정:**
        - Host: {os.getenv("TRINO_HOST", "localhost")}
        - Port: {os.getenv("TRINO_PORT", "8082")}
        - Catalog: hive
        - Schema: gold
        """)
