# -*- coding: utf-8 -*-
import os
from pathlib import Path
import pandas as pd
import json

# visualization
import altair as alt
import streamlit as st
from streamlit_folium import st_folium

# components
from components.channel_cards import render_channel_comparison_sections
from components.eco_page import render_eco_content
from components.extra_panel import render_extra_panel
from components.region_map import render_selected_item_region_map
    # price
from components.price_cards import render_price_drop_cards, render_price_rise_cards
from components.price_graph import render_price_region_donut
    # season
from components.season_cards import render_region_price_comparison, render_region_all_items_chart
from components.season_map import create_season_price_map

# data & queries
from data.athena_connection import get_athena_config
from data.queries.channel_queries import get_channel_comparison_query
from data.sample_data import get_price_summary, get_popular_items
from data.trino_connection import execute_query, get_trino_connection
from data.queries.meta_queries import get_update_status_query
from data.queries.price_queries import (
    get_country_list,
    get_price_drop_top3_query,
    get_price_rise_top3_query,
    get_price_region_rate_query
)
from data.queries.season_queries import (
    get_season,
    get_season_item_list,
    get_season_region_price_query,
    get_region_all_items_price_query
)

def load_css():
    base_path = Path(__file__).parent
    with open(base_path / "styles.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# 초기 설정
st.set_page_config(page_title="농산물 가격 대시보드", layout="wide")
load_css()

# DB 연결
conn = get_trino_connection()

# ================= 확인 필요 ==================
summary = get_price_summary()
popular_items = get_popular_items()
# ============================================

# 메타 정보 조회
status_df = pd.read_sql(get_update_status_query(), conn)
update_status = status_df.iloc[0]

# 세션 상태 초기화
if "page" not in st.session_state:
    st.session_state.page = "main"
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
    # -------------------------
    # header
    # -------------------------
    header_container = st.container()
    with header_container:
        header_left, header_right = st.columns([3, 2])
        with header_left:
            st.title("오늘의 지역별 농산물 가격 동향 한눈에 보기")
        with header_right:
            m1, m2, m3 = st.columns(3)
            m1.metric(
                label="📅 최신 업데이트",
                value=str(update_status["latest_date"])
            )
            m2.metric(
                label="📦 업데이트 품목 수",
                value=f"{int(update_status['row_count']):,}"
            )
            m3.metric(
                label="🌍 업데이트 지역 수",
                value=int(update_status["country_count"])
            )
    st.divider()

    # -------------------------
    # [part 1: price] sub-title
    # -------------------------
    st.subheader("🌱 오늘 눈여겨볼 만한 식재료들")
    st.markdown(
    """
    <div class="callout">
        <div class="callout-title">💡 어떻게 보면 좋을까요?</div>
        지역을 선택하면 <b>전일 대비 가격 변동이 가장 큰</b> 농수산물 TOP 3를 확인할 수 있어요.<br>
        이를 통해 오늘 해당 지역의 <b>이상 가격 징후</b>가 있는 품목을 빠르게 파악할 수 있습니다.<br>
        해당 지역에서 전체 품목 중 <b>상승·하락·유지 비율</b>을 도넛 차트를 통해 한눈에 볼 수 있습니다.
    </div>
    """,
    unsafe_allow_html=True
    )

    # -------------------------
    # [part 1: price] 지역 선택
    # -------------------------
    country_list_df = get_country_list(conn)
    country_list = country_list_df['country_nm'].drop_duplicates().sort_values().tolist()

    if 'country' not in st.session_state:
        st.session_state.country = country_list[0]

    country = st.selectbox(
        "지역 선택", 
        country_list,
        index=country_list.index(st.session_state.country),
        key='country'
    )
    # st.markdown(f"선택된 지역: **{country}**")  # 선택 확인용


    c1, c2, c3 = st.columns(3)

    # -------------------------
    # [part 1: price] charts
    # -------------------------
    with c1:
        st.subheader("📉 전일 대비 가격 하락 TOP 3")
        drop_query = get_price_drop_top3_query(country_filter=country)
        print(drop_query) # debug
        cheep_df = pd.read_sql(drop_query, conn)
        render_price_drop_cards(cheep_df)

    with c2:
        st.subheader("📈 전일 대비 가격 상승 TOP 3")
        rise_query = get_price_rise_top3_query(country_filter=country) #, limit=3)
        rise_df = pd.read_sql(rise_query, conn)
        render_price_rise_cards(rise_df)

    with c3:
        st.subheader("📊 상승/하락/유지 품목 비율")
        summary_query = get_price_region_rate_query(country_filter=country)
        summary_df = pd.read_sql(summary_query, conn)
        render_price_region_donut(summary_df, country)

    st.divider()

    # --------------------------
    # [PART 2: season] sub-title
    # --------------------------
    season_nm_query = get_season()
    season_nm = pd.read_sql(season_nm_query, conn)

    season = season_nm["season"].iloc[0]
    st.markdown(
        f"""
        <h3>❄️ <span style="color:#1f77b4">{season}</span> 제철 식자재 가격 지도 톺아보기</h3>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div class="callout">
            <div class="callout-title">🧭 이렇게 활용해보세요</div>
            💡 제철 식자재 가격을 지역별로 살펴보세요.<br><br>
            <b>현재 월을 기준</b>으로 해당 제철의 식자재 리스트를 확인할 수 있습니다<br>
            제철 농수산물을 선택하면 <b>지역별 가격 수준</b>을 색상으로 확인할 수 있어요.<br><br>
            특정 지역을 클릭하면
            <ul>
                <li>해당 지역의 <b>전년 동일 대비 가격 변화</b>를 확인할 수 있어요.</li>
                <li>해당 지역의 <b>다른 제철 농수산물 가격</b> 현황도 함께 확인할 수 있어요.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True
    )

    # -----------------------
    # 1️⃣ RDS 연결용
    # -----------------------
    # season_query = query함수(
    #     filter 뭐시기, conn = conn
    # )
    # season_df = conn.execute_query(season_query)

    # render_season_cards(season_df)

    # -----------------------------
    # [PART 2: season] select item
    # -----------------------------
    item_query = get_season_item_list()
    item_df = pd.read_sql(item_query, conn)
    item_list = item_df["item_kind"].dropna().tolist()

    if not item_list:
        st.warning("선택 가능한 제철 품목이 없습니다.")
        st.stop()

    if "selected_item" not in st.session_state:
        st.session_state.selected_item = item_list[0]

    bottom_left, bottom_right = st.columns([1, 1])

    with bottom_left:
#        st.subheader("🔎 필터")
        selected_item_kind = st.selectbox(
            f"{season} 제철 농수산물 선택",
            item_list,
            index=item_list.index(st.session_state.selected_item),
            key="selected_item"
        )

    # -----------------------------
    # [PART 2: season] query to df
    # -----------------------------
    season_query = get_season_region_price_query(item_kind_filter=selected_item_kind)
    season_df = pd.read_sql(season_query, conn)

    # 디버깅용 저장
    season_df.to_csv("season_df_debug.csv", index=False, encoding="utf-8-sig")

    if season_df.empty:
        st.error("제철 데이터가 없습니다.")
        st.stop()

    # 결측치 처리
    season_df['prev_1y_pr'] = season_df['prev_1y_pr'].fillna(0)
    season_df['base_pr'] = season_df['base_pr'].fillna(0)

    # ---------------------------
    # [PART 2: season] geo json
    # ---------------------------
    @st.cache_resource
    def load_geojson():
        path = Path("assets/retail_regions.json")
        with path.open(encoding="utf-8") as f:
            return json.load(f)

    merged_geojson = load_geojson()
    season_map = create_season_price_map(
        merged_geojson,
        season_df,
        season_df,
        selected_item_kind
    )

    with bottom_left:
        unit = None
        if "product_cls_unit" in season_df.columns:
            unit_row = season_df.loc[
                season_df["item_kind"] == selected_item_kind, "product_cls_unit"
            ]
            if not unit_row.empty:
                unit = unit_row.iloc[0]

        if unit:
            st.markdown(
                f"<h4>🗺️ <span style='color:#0095fa'>{selected_item_kind}({unit})</span> 지역별 가격 분포</h4>",
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                f"<h4>🗺️ <span style='color:#0095fa'>{selected_item_kind}</span> 지역별 가격 분포</h4>",
                unsafe_allow_html=True
            )

        _map_state = st_folium(
            season_map,
            width=1000,
            height=650,
            key="season_map",
            returned_objects=["last_active_drawing"]
        )

    clicked_region = None
    if _map_state and _map_state.get("last_active_drawing"):
        clicked_region = _map_state["last_active_drawing"]["properties"]["CITY_AB_NM"]

    # 기본값 설정
    if not clicked_region:
        clicked_region = "서울"

    with bottom_right:
        if clicked_region:
            region_df = season_df[season_df["country_nm"] == clicked_region]
            render_region_price_comparison(region_df, clicked_region, selected_item_kind)
        
        region_all_query = get_region_all_items_price_query(clicked_region)
        region_all_df = pd.read_sql(region_all_query, conn)
        render_region_all_items_chart(region_all_df, clicked_region)
    # -------------------------
    # 우측 영역 (추가 기능)
    # -------------------------
    # with right:
    #     render_extra_panel(popular_items)

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
