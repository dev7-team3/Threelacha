import streamlit as st
import altair as alt
import pandas as pd

def render_region_price_comparison(region_df: pd.DataFrame, clicked_region: str, selected_item_kind: str):
    """
    특정 지역의 선택된 품목 가격 비교 (오늘 vs 1년 전) 바차트 렌더링
    """
    if region_df.empty:
        st.info("데이터가 없습니다.")
        return

    chart_data = region_df.melt(
        id_vars=["item_kind"],
        value_vars=["base_pr", "prev_1y_pr"],
        var_name="가격구분",
        value_name="가격"
    )
    chart_data["가격구분"] = chart_data["가격구분"].replace({
        "base_pr": "오늘 가격",
        "prev_1y_pr": "1년 전 가격"
    })

    chart = alt.Chart(chart_data).mark_bar().encode(
        x=alt.X("item_kind:N", title="품목", axis=alt.Axis(titleAngle=0, labelAngle=0, labelLimit=0)),
        xOffset="가격구분",
        y=alt.Y("가격:Q", title="가격(원)", axis=alt.Axis(titleAngle=0, titleAnchor="end")),
        color=alt.Color("가격구분:N", title="구분",
                        scale=alt.Scale(domain=["오늘 가격", "1년 전 가격"],
                                        range=["#1f77b4", "#e6f4f1"])),
#                                        range=["#1f77b4", "#ff7f0e"])),
        tooltip=["item_kind", "가격구분", "가격"]
    )

    text = alt.Chart(chart_data).mark_text(
        dy=-10,
        align="center",
        fontSize=20,
    ).encode(
        x=alt.X("item_kind:N"),
        xOffset="가격구분",
        y="가격:Q",
        text=alt.Text("가격:Q", format=","),
        color=alt.Color("가격구분:N",
                        scale=alt.Scale(domain=["오늘 가격", "1년 전 가격"],
                                        range=["#1f77b4", "#e6f4f1"]))
#                                        range=["#2c7744", "#5a3f37"]))
    )

    st.markdown(
        f"<h6 style='text-align:center;'>전년 동일 대비 <span style='color:#0095fa'>{clicked_region}</span> 지역 "
        f"<span style='color:#0095fa'>{selected_item_kind}</span> 가격 비교</h6>",
        unsafe_allow_html=True
    )

    final_chart = (chart + text).properties(width=400, height=400)
    st.altair_chart(final_chart, use_container_width=True)


def render_region_all_items_chart(region_all_df: pd.DataFrame, clicked_region: str):
    """
    특정 지역의 전체 제철 식재료 가격 현황 바차트 렌더링
    """
    if region_all_df.empty:
        st.info("데이터가 없습니다.")
        return

    chart = alt.Chart(region_all_df).mark_bar().encode(
        y=alt.Y("item_kind:N", title="품목"),
        x=alt.X("base_pr:Q", title="가격(원)"),
        color=alt.Color(
            "national_rank:Q",#O,
            title="전국 순위",
#            scale=alt.Scale(scheme="blues", reverse=True),
            scale=alt.Scale(
                domain=[region_all_df["national_rank"].min(),
                        region_all_df["national_rank"].max()],
#                        range=["#cdc45c", "#204E4A"] # 그라데이션
                        range=["#8bade1", "#00264e"] # 그라데이션
            ),
            legend=None
        ),
        tooltip=[
            alt.Tooltip("item_kind:N", title="식재료명"),
            alt.Tooltip("base_pr:Q", title="오늘 가격"),
            alt.Tooltip("national_rank:N", title="오늘 전국 순위")
        ]
    ).properties(
        width=600,
        height=400,
#        title=f"{clicked_region} 지역 전체 제철 식재료 가격 현황"
    )

    text = alt.Chart(region_all_df).mark_text(
        align="left",
        baseline="middle",
        dx=3
    ).encode(
        y="item_kind:N",
        x="base_pr:Q",
        text=alt.Text("base_pr:Q", format=","),
        color=alt.Color(
            "national_rank:Q",
            scale=alt.Scale(
                domain=[region_all_df["national_rank"].min(),
                        region_all_df["national_rank"].max()],
                range=["#8bade1", "#973e72"]
            ),
            legend=None
        )
    )

    st.markdown(
        f"<h6 style='text-align:center;'><span style='color:#0095fa'>{clicked_region}</span> 지역 "
        f"전체 제철 식재료 가격 현황</h6>",
        unsafe_allow_html=True
    )

    final_chart = chart + text
    st.altair_chart(final_chart, use_container_width=True)
