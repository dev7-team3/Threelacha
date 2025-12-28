import os
from pathlib import Path

import pandas as pd
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
            latest_data_query = """
            WITH latest_date AS (
                SELECT MAX(res_dt) as max_date
                FROM team3_gold.api13_price_statistics_by_category
            )
            SELECT 
                res_dt,
                item_cd,
                item_nm,
                market_category,
                record_count,
                avg_price,
                min_price,
                max_price
            FROM team3_gold.api13_price_statistics_by_category
            CROSS JOIN latest_date
            WHERE res_dt = latest_date.max_date
            ORDER BY item_nm, market_category, avg_price
            """

            with st.spinner("Athena에서 최신 데이터를 불러오는 중..."):
                try:
                    # Athena 쿼리 실행
                    df_data = execute_athena_query(latest_data_query)

                    if len(df_data) > 0:
                        # 최신 데이터 날짜 표시
                        latest_date = df_data["res_dt"].iloc[0] if "res_dt" in df_data.columns else "N/A"
                        st.info(f"📅 최신 데이터 날짜: {latest_date}")

                        # 요약 통계
                        st.subheader("📈 요약 통계")
                        summary_col1, summary_col2, summary_col3 = st.columns(3)

                        with summary_col1:
                            total_records = len(df_data)
                            st.metric("총 레코드 수", f"{total_records:,}개")

                        with summary_col2:
                            unique_items = df_data["item_nm"].nunique() if "item_nm" in df_data.columns else 0
                            st.metric("고유 품목 수", f"{unique_items:,}개")

                        with summary_col3:
                            avg_price = df_data["avg_price"].mean() if "avg_price" in df_data.columns else 0
                            st.metric("평균 가격", f"{avg_price:,.0f}원")

                        st.divider()

                        # market_category를 피봇으로 변환
                        st.subheader("📊 마트별 가격 비교 (피봇 테이블)")

                        try:
                            # 피봇 테이블 생성: res_dt, item_cd, item_nm을 행으로, market_category를 열로, avg_price를 값으로
                            df_pivot = df_data.pivot_table(
                                index=["res_dt", "item_cd", "item_nm"],
                                columns="market_category",
                                values="avg_price",
                                aggfunc="first",  # 중복이 있을 경우 첫 번째 값 사용
                            ).reset_index()

                            # 컬럼명 정리 (market_category가 컬럼명이 됨)
                            df_pivot.columns.name = None

                            # avg_price의 최대값과 최소값의 차이를 계산하는 컬럼 추가
                            # market_category 컬럼들만 선택 (res_dt, item_cd, item_nm 제외)
                            price_columns = [
                                col for col in df_pivot.columns if col not in ["res_dt", "item_cd", "item_nm"]
                            ]

                            if price_columns:
                                # 각 행별로 가격 컬럼들의 최대값과 최소값 계산 (NaN 제외)
                                df_pivot["가격차이"] = df_pivot[price_columns].max(axis=1, skipna=True) - df_pivot[
                                    price_columns
                                ].min(axis=1, skipna=True)

                                # 가격차이 컬럼을 마지막에 배치하기 위해 컬럼 순서 재정렬
                                other_columns = [col for col in df_pivot.columns if col != "가격차이"]
                                df_pivot = df_pivot[[*other_columns, "가격차이"]]

                            st.dataframe(df_pivot, use_container_width=True)

                            # 가격차이가 큰 상위 5개 품목 그래프
                            if "가격차이" in df_pivot.columns:
                                st.divider()
                                st.subheader("📊 가격차이가 큰 상위 5개 품목")

                                # 가격차이 기준으로 내림차순 정렬하고 상위 5개 선택
                                top_5_items = df_pivot.nlargest(5, "가격차이")

                                # 각 품목별로 그래프 생성
                                for _, row in top_5_items.iterrows():
                                    item_nm = row["item_nm"]
                                    price_diff = row["가격차이"]

                                    st.markdown(f"### {item_nm} (가격차이: {price_diff:,.0f}원)")

                                    # market_category별 가격 데이터 추출
                                    price_data = {}
                                    for col in df_pivot.columns:
                                        if col not in ["res_dt", "item_cd", "item_nm", "가격차이"]:
                                            price_value = row[col]
                                            if pd.notna(price_value):
                                                price_data[col] = price_value

                                    if price_data:
                                        # 막대 그래프로 표시
                                        price_df = pd.DataFrame(
                                            list(price_data.items()), columns=["구매처", "평균가격"]
                                        )
                                        price_df = price_df.sort_values("평균가격")

                                        st.bar_chart(price_df.set_index("구매처"))

                                        # 데이터 테이블도 함께 표시
                                        st.dataframe(
                                            price_df,
                                            use_container_width=True,
                                            hide_index=True,
                                        )

                                    st.markdown("<br>", unsafe_allow_html=True)

                            # 원본 데이터도 탭으로 제공
                            with st.expander("📋 원본 데이터 보기"):
                                st.dataframe(df_data, use_container_width=True)

                        except Exception as pivot_error:
                            st.error(f"피봇 테이블 생성 중 오류: {str(pivot_error)}")
                            st.info("원본 데이터를 표시합니다.")
                            st.dataframe(df_data, use_container_width=True)
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
