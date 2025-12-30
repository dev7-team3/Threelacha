"""지역별 지도 시각화 컴포넌트"""

import folium
import pandas as pd
from typing import Optional
import streamlit as st
from streamlit_folium import st_folium
from data.trino_connection import execute_query
from data.queries.region_queries import get_region_stats_query


# 한국 주요 도시 좌표
REGION_COORDINATES = {
    "서울": [37.5665, 126.9780],
    "부산": [35.1796, 129.0756],
    "대구": [35.8714, 128.6014],
    "인천": [37.4563, 126.7052],
    "광주": [35.1595, 126.8526],
    "대전": [36.3504, 127.3845],
    "울산": [35.5384, 129.3114],
    "세종": [36.4800, 127.2890],
    "수원": [37.2636, 127.0286],
    "성남": [37.4201, 127.1267],
    "고양": [37.6584, 126.8320],
    "용인": [37.2411, 127.1776],
    "청주": [36.6424, 127.4890],
    "천안": [36.8151, 127.1139],
    "전주": [35.8242, 127.1480],
    "포항": [36.0322, 129.3650],
    "창원": [35.2279, 128.6819],
    "김해": [35.2284, 128.8893],
    "목포": [34.8118, 126.3922],
    "여수": [34.7604, 127.6622],
}


def create_region_map(
    region_data: pd.DataFrame,
    price_column: str = "평균가격",
    region_column: str = "country_nm",
    selected_item: Optional[str] = None,
) -> folium.Map:
    """지역별 가격 데이터를 지도에 표시합니다.

    Args:
        region_data: 지역별 가격 데이터 (country_nm, 평균가격 등 포함)
        price_column: 가격 컬럼명
        region_column: 지역명 컬럼명
        selected_item: 선택된 품목명 (선택사항)

    Returns:
        folium.Map: 지도 객체
    """
    # 한국 중심 지도 생성 - CartoDB positron 타일 사용 (깔끔한 스타일)
    m = folium.Map(
        location=[36.5, 127.5],  # 한국 중심 좌표
        zoom_start=7,
        tiles="cartodbpositrononlylabels",
    )

    # 데이터가 없으면 기본 지도만 반환
    if region_data.empty:
        return m

    # 가격 범위 계산 (색상 구분을 위해)
    if price_column in region_data.columns:
        min_price = region_data[price_column].min()
        max_price = region_data[price_column].max()
        price_range = max_price - min_price if max_price > min_price else 1
    else:
        min_price = 0
        max_price = 1
        price_range = 1

    # 색상 함수 (가격이 낮을수록 초록색, 높을수록 빨간색)
    def get_color(price: float) -> str:
        if pd.isna(price):
            return "gray"
        normalized = (price - min_price) / price_range
        if normalized < 0.33:
            return "green"  # 저렴
        elif normalized < 0.67:
            return "orange"  # 중간
        else:
            return "red"  # 비쌈

    # 각 지역에 마커 추가
    for _, row in region_data.iterrows():
        region_name = row[region_column]
        price = row.get(price_column, 0)

        # 좌표 가져오기
        if region_name in REGION_COORDINATES:
            coords = REGION_COORDINATES[region_name]
        else:
            continue  # 좌표가 없으면 스킵

        # 팝업 텍스트 생성 (간소화)
        popup_text = f"""
        <div style="font-family: Arial; width: 180px;">
            <h4 style="margin: 5px 0; font-size: 16px;">{region_name}</h4>
            <p style="margin: 5px 0; font-size: 14px;"><b>가격:</b> {price:,.0f}원</p>
        """

        if selected_item:
            popup_text += f"<p style='margin: 5px 0; font-size: 12px; color: #666;'><b>품목:</b> {selected_item}</p>"

        popup_text += "</div>"

        # 마커 추가 (더 깔끔한 스타일)
        folium.CircleMarker(
            location=coords,
            radius=12 + (price / max_price * 15) if max_price > 0 else 12,  # 가격에 비례한 크기
            popup=folium.Popup(popup_text, max_width=200),
            tooltip=f"{region_name}: {price:,.0f}원",
            color="white",
            weight=1.5,
            fill=True,
            fillColor=get_color(price),
            fillOpacity=0.8,
        ).add_to(m)

    # 범례 추가 (간소화된 스타일)
    legend_html = f"""
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 180px; 
                background-color: white; border: 1px solid #ccc; border-radius: 5px;
                z-index:9999; font-size: 12px; padding: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
    <h4 style="margin: 0 0 8px 0; font-size: 14px; font-weight: bold;">가격 범례</h4>
    <p style="margin: 4px 0;"><span style="display: inline-block; width: 12px; height: 12px; background-color: green; border-radius: 50%; margin-right: 6px;"></span> 저렴</p>
    <p style="margin: 4px 0;"><span style="display: inline-block; width: 12px; height: 12px; background-color: orange; border-radius: 50%; margin-right: 6px;"></span> 중간</p>
    <p style="margin: 4px 0;"><span style="display: inline-block; width: 12px; height: 12px; background-color: red; border-radius: 50%; margin-right: 6px;"></span> 비쌈</p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


def render_region_map(
    region_data: pd.DataFrame,
    price_column: str = "평균가격",
    region_column: str = "country_nm",
    selected_item: Optional[str] = None,
    height: int = 500,
):
    """Streamlit에서 지역별 지도를 렌더링합니다.

    Args:
        region_data: 지역별 가격 데이터
        price_column: 가격 컬럼명
        region_column: 지역명 컬럼명
        selected_item: 선택된 품목명
        height: 지도 높이 (픽셀)
    """
    if region_data.empty:
        st.info("지도에 표시할 데이터가 없습니다.")
        return

    # 지도 생성
    m = create_region_map(region_data, price_column, region_column, selected_item)

    # Streamlit에 지도 표시
    st_folium(m, width=700, height=height, returned_objects=[])


def render_selected_item_region_map(conn, date_filter=None, category_filter=None):
    """선택된 품목의 지역별 지도를 표시하는 함수.

    Args:
        conn: Trino 연결 객체
        date_filter: 날짜 필터
        category_filter: 카테고리 필터
    """
    if not (
        st.session_state.get("show_region_map", False)
        and "selected_item_nm" in st.session_state
        and "selected_kind_nm" in st.session_state
    ):
        return

    st.divider()
    st.subheader(f"🗺️ {st.session_state.selected_item_nm}({st.session_state.selected_kind_nm}) 지역별 가격 지도")

    # 지역별 데이터 조회
    region_stats_query = get_region_stats_query(date_filter=date_filter, category_filter=category_filter)

    with st.spinner("지역별 데이터를 불러오는 중..."):
        try:
            df_region = execute_query(region_stats_query, conn)

            if len(df_region) > 0:
                # 선택된 품목 필터링
                df_filtered = df_region[
                    (df_region["item_nm"] == st.session_state.selected_item_nm)
                    & (df_region["kind_nm"] == st.session_state.selected_kind_nm)
                ]

                if len(df_filtered) > 0:
                    # 지역별 평균 가격으로 그룹화
                    df_region_agg = df_filtered.groupby("country_nm").agg({"평균가격": "mean"}).reset_index()

                    # 지도 표시
                    render_region_map(
                        df_region_agg,
                        price_column="평균가격",
                        region_column="country_nm",
                        selected_item=f"{st.session_state.selected_item_nm}({st.session_state.selected_kind_nm})",
                    )

                    # 데이터 테이블도 함께 표시
                    st.dataframe(
                        df_filtered[["country_nm", "평균가격", "최저가격", "최고가격"]], use_container_width=True
                    )

                    # 닫기 버튼
                    if st.button("지도 닫기", key="close_map_btn"):
                        st.session_state.show_region_map = False
                        st.session_state.selected_item_nm = None
                        st.session_state.selected_kind_nm = None
                        st.rerun()
                else:
                    st.info(
                        f"{st.session_state.selected_item_nm}({st.session_state.selected_kind_nm})에 대한 지역별 데이터가 없습니다."
                    )
                    if st.button("지도 닫기", key="close_map_btn_no_data"):
                        st.session_state.show_region_map = False
                        st.session_state.selected_item_nm = None
                        st.session_state.selected_kind_nm = None
                        st.rerun()
            else:
                st.info("지역별 데이터가 없습니다.")
        except Exception as e:
            st.error(f"지역별 데이터 조회 중 오류: {str(e)}")
