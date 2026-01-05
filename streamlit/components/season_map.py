import folium
import branca.colormap as cm
from folium import Element
import pandas as pd
import copy
from folium.features import GeoJsonTooltip
import streamlit as st

def create_season_price_map(
    geojson_data: dict,
    region_price_df: pd.DataFrame,
    popup_df: pd.DataFrame,
    selected_item: str,
):
    m = folium.Map(
        location=[35.5, 129.5],
        zoom_start=7,
        min_zoom=7,
        max_zoom=8,
        tiles="Esri.WorldGrayCanvas"
    )

    # 1) 키 정규화: 문자열/공백 통일
    region_price_df = region_price_df.copy()
    region_price_df["country_nm"] = (
        region_price_df["country_nm"].astype(str).str.strip()
    )
    popup_df = popup_df.copy()
    popup_df["country_nm"] = popup_df["country_nm"].astype(str).str.strip()

    price_map = region_price_df.set_index("country_nm").to_dict("index")
    popup_map = popup_df.set_index("country_nm").to_dict("index")

    # 2) GeoJSON 딥카피 후 주입
    geojson_enriched = copy.deepcopy(geojson_data)

    # st.write(geojson_enriched["features"][0]["properties"])

    # 3) 색상 스케일 (랭킹 기반)
    vmin = region_price_df["base_pr"].min()
    vmax = region_price_df["base_pr"].max()
    colormap = cm.LinearColormap(
        colors=["#2c7bb6", "#abd9e9", "#fdae61", "#d7191c"],  # Blue→Red
        vmin=vmin,
        vmax=vmax,
    )#.to_step(10)

    legend = Element(f"""
    <div style="
        position: fixed;
        bottom: 20px;
        right: 20px;
        z-index: 9999;
        background: rgba(255,255,255,0.9);
        padding: 10px 14px;
        border-radius: 6px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.2);
        font-size: 12px;
    ">

        <!-- 제목 -->
        <div style="
            text-align:right;
            margin-bottom:6px;
            color: #000;
        ">
            <b>{selected_item}</br>가격 (원)</b>
        </div>

        <!-- 범례 본체 -->
        <div style="position: relative; height: 160px;">

            <!-- max 값 (왼쪽) -->
            <div style="
                position: absolute;
                top: -2px;
                right: 26px;
                white-space: nowrap;
                color: #d7191c;
            ">
                {vmax:,.0f}
            </div>

            <!-- min 값 (왼쪽) -->
            <div style="
                position: absolute;
                bottom: -2px;
                right: 26px;
                white-space: nowrap;
                color: #2c7bb6;
            ">
                {vmin:,.0f}
            </div>

            <!-- 컬러바 -->
            <div style="
                position: absolute;
                right: 0;
                width: 18px;
                height: 160px;
                background: linear-gradient(
                    to top,
                    #2c7bb6,
                    #abd9e9,
                    #fdae61,
                    #d7191c
                );
            "></div>

        </div>
    </div>
    """)

    m.get_root().html.add_child(legend)

    def style_function(feature):
        props = feature.get("properties", {})
        region = str(props.get("CITY_AB_NM", "")).strip()
        if region in price_map:
            price = price_map[region]["base_pr"]
            return {
                "fillColor": colormap(price),
                "color": "#ECBA82",
                "weight": 1.2,
                "fillOpacity": 0.8,
            }
        return {
            "fillColor": "#eeeeee",
            "color": "#cccccc",
            "weight": 0.5,
            "fillOpacity": 0.3,
        }

    def popup_function(feature):
        props = feature.get("properties", {})
        region = str(props.get("CITY_AB_NM", "")).strip()
        if region not in popup_map:
            return folium.Popup("추가 제철 품목 없음", max_width=300)
        items = popup_map[region]["other_items"]
        items_html = "<br>".join(items)
        return folium.Popup(
            f"""
            <b>{region}의 다른 제철 품목</b><br><br>
            {items_html}
            """,
            max_width=300
        )
    
    
    for feat in geojson_enriched["features"]:
        props = feat["properties"]
        region = str(props.get("CITY_AB_NM", "")).strip()

        if region in price_map:
            # 값 주입
            props["price"] = price_map[region]["base_pr"]
            props["yoy_pct"] = price_map[region]["yoy_pct"]
            props["price_rank"] = price_map[region]["price_rank"]
            props["unit"] = price_map[region].get("product_cls_unit")  # 단위 추가

            # 안전한 포맷팅 처리
            price_val = props.get("price")
            yoy_val = props.get("yoy_pct")
            rank_val = props.get("price_rank")
            unit_val = props.get("unit")

            price_str = f"{int(price_val):,}원" if price_val is not None else "데이터 없음"
            if yoy_val is None or pd.isna(yoy_val):
                yoy_str = "데이터 없음"
            else:
                yoy_str = f"{yoy_val:+.1f}%"
            rank_str = f"{rank_val}" if rank_val is not None else "데이터 없음"
            unit_str = f"({unit_val})" if unit_val else ""

            tooltip_html = f"""
            <b>{region}</b><br>
            품목: {selected_item}{unit_str}<br>
            가격: {price_str}<br>
            전년 대비: {yoy_str}만큼 비싸졌어요<br>
            전국에서 {rank_str}번째로 싸요
            """

            folium.GeoJson(
                feat,
                style_function=style_function,
                tooltip=folium.Tooltip(tooltip_html, sticky=False),
                popup=popup_function,
            ).add_to(m)
        else:
            # 데이터 없는 지역은 Tooltip 없이 추가
            folium.GeoJson(
                feat,
                style_function=style_function,
                popup=popup_function,
            ).add_to(m)

    return m
