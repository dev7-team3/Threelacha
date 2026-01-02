import pandas as pd
import streamlit as st

from data.athena_connection import execute_athena_query
from data.queries.eco_queries import get_latest_price_statistics_query


def render_eco_content(use_athena_data: bool):
    """친환경 페이지 콘텐츠 렌더링"""
    if use_athena_data:
        try:
            # 카테고리 필터만 추가 (날짜 필터 제거)
            category_filter = st.selectbox(
                "카테고리 선택 (선택사항)",
                ["전체", "식량작물", "채소류", "특용작물", "과일류", "축산물", "수산물"],
                key="eco_category",
            )

            # 최신 데이터 조회 쿼리 생성
            latest_data_query = get_latest_price_statistics_query()

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
