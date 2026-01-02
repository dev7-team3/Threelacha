import streamlit as st
import streamlit.components.v1 as components

def render_price_card(row, direction="drop"):
    """
    통합 가격 카드 렌더링
    direction: "drop" -> 빨간색 ▼, "rise" -> 초록색 ▲
    """
    pct = row["prev_1d_dir_pct"]
    
    prev_price = f"{int(row['prev_1d_pr']):,}"
    base_price = f"{int(row['base_pr']):,}"
    
    if direction == "rise":
        arrow = "▲"
        color = "#16a34a"  # 초록
        box_bg = "#dcfce7"
        box_border = "#16a34a"
    else:
        arrow = "▼"
        color = "#ef4444"  # 빨강
        box_bg = "#ffffff"
        box_border = "#6b7280"
    
    components.html(
        f"""
        <div style="
            display: flex;
            justify-content: space-between;
            align-items: stretch;
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 12px;
            background-color: {box_bg};
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        ">
            <!-- left: 상품 정보 + 전일/금일 가격 -->
            <div style="display: flex; flex-direction: column; justify-content: center;">
                <div style="font-size: 14px; color: #6b7280;">
                    {row["item_nm"]} ({row["kind_nm"]}) 
                    <span style="font-size:12px; color:#9ca3af;">{row["product_cls_unit"]}</span>
                </div>
                
                <div style="margin-top: 10px; font-size: 13px; display: flex; flex-direction: column; gap: 4px;">
                    <!-- 전일 -->
                    <div style="display: flex; align-items: center; gap: 4px;">
                        <span style="
                            border: 1px solid #d1d5db;
                            border-radius: 4px;
                            padding: 2px 6px;
                            font-size: 12px;
                            color: #6b7280;
                        ">전일</span>
                        <strong>{prev_price}</strong>원
                    </div>
                    
                    <!-- 금일 -->
                    <div style="display: flex; align-items: center; gap: 4px;">
                        <span style="
                            border: 1px solid #6b7280;
                            background-color: #6b7280;
                            color: white;
                            border-radius: 4px;
                            padding: 2px 6px;
                            font-size: 12px;
                        ">금일</span>
                        <strong>{base_price}</strong>원
                    </div>
                </div>
            </div>

            <!-- 오른쪽: 화살표 + % -->
            <div style="
                display: flex;
                flex-direction: row;       /* 한 줄로 */
                align-items: center;       /* 세로 중앙 정렬 */
                font-size: 20px;
                font-weight: 700;
                color: {color};
                padding-left: 12px;
            ">
                {arrow} {pct:.1f}%
            </div>
        </div>
        """,
        height=130
    )

def render_price_drop_cards(df):
    if df.empty:
        st.info("가격 하락 데이터가 없습니다.")
        return
    for _, row in df.iterrows():
        render_price_card(row, direction="drop")

def render_price_rise_cards(df):
    if df.empty:
        st.info("가격 상승 데이터가 없습니다.")
        return
    for _, row in df.iterrows():
        render_price_card(row, direction="rise")