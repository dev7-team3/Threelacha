import streamlit as st
import pandas as pd
import altair as alt

def render_price_region_donut(summary_df: pd.DataFrame, country: str):
    """
    지역별 상승/하락/유지 품목 비율 도넛 차트 렌더링
    summary_df: get_price_region_rate_query 결과 DataFrame (rise_count, drop_count, keep_count 포함)
    """
    if summary_df.empty:
        st.info("데이터가 없습니다.")
        return

    donut_df = pd.DataFrame({
        "status": ["상승", "하락", "유지"],
        "count": [
            int(summary_df["rise_count"].iloc[0]),
            int(summary_df["drop_count"].iloc[0]),
            int(summary_df["keep_count"].iloc[0])
        ]
    })

    # 퍼센트 계산
    total = donut_df["count"].sum()
    donut_df["percent"] = (donut_df["count"] / total * 100).round(1).astype(str) + "%"


    chart = alt.Chart(donut_df).mark_arc(innerRadius=60).encode(
        theta="count:Q",
        color=alt.Color(
            "status:N",
            scale=alt.Scale(domain=["상승", "하락", "유지"], range=["#16a34a", "#ef4444", "#f6edd9"]),
            # legend=None
        ),
        tooltip=[
            alt.Tooltip("status:N", title="구분"),
            alt.Tooltip("count:Q", title="품목 수"),
            alt.Tooltip("percent:N", title="비율")
        ]
    ).properties(
        width=300,
        height=300,
#        title=f"{country} 지역 상승/하락 품목 비율"
    )

    st.markdown(
        f"<h6 style='text-align:center;'><span style='color:#2E933C'>{country}</span> 지역 상승/하락 품목 비율</h6>",
        unsafe_allow_html=True
    )

    st.altair_chart(chart, use_container_width=True)
