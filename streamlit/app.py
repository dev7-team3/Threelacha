import os
from pathlib import Path

import streamlit as st

from components.channel_cards import render_channel_comparison_sections
from components.extra_panel import render_extra_panel
from components.price_cards import price_card
from components.region_map import render_selected_item_region_map
from components.season_selector import render_season_selector
from data.athena_connection import execute_athena_query, get_athena_config
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

    if use_athena_data:
        try:
            # 카테고리 필터만 추가 (날짜 필터 제거)
            category_filter = st.selectbox(
                "카테고리 선택 (선택사항)",
                ["전체", "식량작물", "채소류", "특용작물", "과일류", "축산물", "수산물"],
                key="eco_category",
            )

            # team3_gold.api13_price_statistics_by_category 테이블에서 최신 데이터 조회
            # 각 품목별로 가장 저렴한 market_category 찾기
            latest_data_query = """
            WITH latest_date AS (
                SELECT MAX(res_dt) as max_date
                FROM team3_gold.api13_price_statistics_by_category
            ),
            item_prices AS (
                SELECT 
                    item_nm,
                    item_cd,
                    market_category,
                    avg_price,
                    min_price,
                    max_price,
                    record_count
                FROM team3_gold.api13_price_statistics_by_category
                CROSS JOIN latest_date
                WHERE res_dt = latest_date.max_date
            ),
            cheapest_market AS (
                SELECT 
                    ip1.item_nm,
                    ip1.item_cd,
                    ip1.market_category as cheapest_category,
                    ip1.avg_price as cheapest_price,
                    ip1.min_price,
                    ip1.max_price,
                    ip1.record_count
                FROM item_prices ip1
                WHERE ip1.avg_price = (
                    SELECT MIN(ip2.avg_price)
                    FROM item_prices ip2
                    WHERE ip2.item_nm = ip1.item_nm 
                      AND ip2.item_cd = ip1.item_cd
                )
            ),
            all_markets AS (
                SELECT 
                    item_nm,
                    item_cd,
                    market_category,
                    avg_price
                FROM item_prices
            )
            SELECT 
                cm.item_nm,
                cm.item_cd,
                cm.cheapest_category,
                cm.cheapest_price,
                cm.min_price,
                cm.max_price,
                cm.record_count,
                COUNT(DISTINCT am.market_category) as total_market_count
            FROM cheapest_market cm
            LEFT JOIN all_markets am ON am.item_nm = cm.item_nm AND am.item_cd = cm.item_cd
            GROUP BY 
                cm.item_nm, 
                cm.item_cd, 
                cm.cheapest_category, 
                cm.cheapest_price,
                cm.min_price,
                cm.max_price,
                cm.record_count
            ORDER BY cm.item_nm, cm.cheapest_price
            """

            with st.spinner("Athena에서 최신 데이터를 불러오는 중..."):
                try:
                    # Athena 쿼리 실행
                    df_comparison = execute_athena_query(latest_data_query)

                    if len(df_comparison) > 0:
                        # 세션 상태에 쿼리 결과 저장
                        st.session_state.eco_df_comparison = df_comparison
                        st.session_state.eco_query_category_filter = category_filter

                        # 최신 데이터 날짜 표시
                        latest_date_query = (
                            "SELECT MAX(res_dt) as latest_date FROM team3_gold.api13_price_statistics_by_category"
                        )
                        latest_date_df = execute_athena_query(latest_date_query)
                        latest_date = latest_date_df.iloc[0]["latest_date"] if len(latest_date_df) > 0 else "N/A"
                        st.info(f"📅 최신 데이터 날짜: {latest_date}")

                        # 요약 통계
                        st.subheader("📈 요약 통계")
                        summary_col1, summary_col2, summary_col3 = st.columns(3)

                        with summary_col1:
                            total_items = len(df_comparison)
                            st.metric("총 품목 수", f"{total_items:,}개")

                        with summary_col2:
                            avg_cheapest = df_comparison["cheapest_price"].mean()
                            st.metric("평균 최저가", f"{avg_cheapest:,.0f}원")

                        with summary_col3:
                            # 가장 저렴한 market_category 분포
                            category_counts = df_comparison["cheapest_category"].value_counts()
                            most_common_category = category_counts.index[0] if len(category_counts) > 0 else "N/A"
                            st.metric("가장 저렴한 곳", most_common_category)

                        st.divider()

                        # 카테고리별로 가장 저렴한 품목 그룹화
                        st.subheader("💰 품목별 가장 저렴한 구매처")

                        # market_category별로 그룹화
                        for category in df_comparison["cheapest_category"].unique():
                            category_items = df_comparison[df_comparison["cheapest_category"] == category].head(20)

                            if len(category_items) > 0:
                                # 카테고리별 헤더
                                category_emoji = {
                                    "대형마트": "🏪",
                                    "생협": "🌱",
                                    "SSM": "🏬",
                                    "전문점": "🏪",
                                    "백화점": "🏢",
                                    "전통시장": "🏮",
                                }
                                emoji = category_emoji.get(category, "📍")

                                st.markdown(f"### {emoji} {category}에서 가장 저렴한 품목")

                                # 카드 형태로 표시
                                cols = st.columns(3)
                                for idx, (_, row) in enumerate(category_items.iterrows()):
                                    col_idx = idx % 3
                                    with cols[col_idx]:
                                        st.markdown(
                                            f"""
                                            <div style="
                                                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                                                padding: 15px;
                                                border-radius: 10px;
                                                margin-bottom: 10px;
                                                color: white;
                                                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                                            ">
                                                <h4 style="margin: 0 0 10px 0; color: white;">{row.get("item_nm", "N/A")}</h4>
                                                <p style="margin: 5px 0; font-size: 24px; font-weight: bold;">
                                                    {row["cheapest_price"]:,.0f}원
                                                </p>
                                                <p style="margin: 5px 0; font-size: 12px; opacity: 0.9;">
                                                    {category} · 최저: {row["min_price"]:,.0f}원 · 최고: {row["max_price"]:,.0f}원
                                                </p>
                                            </div>
                                            """,
                                            unsafe_allow_html=True,
                                        )

                                if len(category_items) < len(
                                    df_comparison[df_comparison["cheapest_category"] == category]
                                ):
                                    st.caption(
                                        f"총 {len(df_comparison[df_comparison['cheapest_category'] == category])}개 중 상위 20개만 표시"
                                    )

                                st.divider()

                        st.subheader("📊 전체 데이터")
                        st.dataframe(df_comparison, use_container_width=True)
                    else:
                        st.info("조회된 데이터가 없습니다.")

                except Exception as e:
                    st.error(f"데이터 조회 중 오류 발생: {str(e)}")
                    st.info("💡 Athena 연결 설정을 확인하세요.")

        except Exception as e:
            st.error(f"Athena 연결 오류: {str(e)}")
            st.info("""
            **Athena 연결 설정 확인:**
            - AWS 자격 증명이 설정되어 있는지 확인
            - 환경 변수 설정 확인:
              - `AWS_ACCESS_KEY_ID`: AWS Access Key
              - `AWS_SECRET_ACCESS_KEY`: AWS Secret Key
              - `AWS_REGION`: 기본값 `ap-northeast-2`
              - `ATHENA_DATABASE`: 기본값 `team3_silver`
              - `ATHENA_WORKGROUP`: 기본값 `team3-wg`
            """)

    else:
        # 샘플 데이터 사용 (기존 코드)
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
