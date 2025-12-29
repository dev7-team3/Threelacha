"""채널별 가격 비교 카드 컴포넌트"""
import streamlit as st
import pandas as pd


def render_channel_comparison_header(title: str, gradient_colors: str):
    """채널 비교 섹션 헤더를 렌더링합니다.
    
    Args:
        title: 헤더 제목
        gradient_colors: 그라데이션 색상 (CSS gradient 형식)
    """
    st.markdown(
        f"""
        <div style="background: linear-gradient(135deg, {gradient_colors}); 
                    padding: 20px; border-radius: 15px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
            <h2 style="color: white; margin: 0; text-align: center; font-size: 24px;">
                {title}
            </h2>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_price_comparison_card(
    item_nm: str,
    kind_nm: str,
    cheaper_channel: str,
    cheaper_price: float,
    other_channel: str,
    other_price: float,
    price_diff: float,
    border_color: str
):
    """가격 비교 카드를 렌더링합니다. 카드 전체가 클릭 가능합니다.
    
    Args:
        item_nm: 품목명
        kind_nm: 품종명
        cheaper_channel: 더 저렴한 채널명
        cheaper_price: 더 저렴한 가격
        other_channel: 다른 채널명
        other_price: 다른 채널 가격
        price_diff: 가격 차이
        border_color: 카드 테두리 색상
    """
    # 버튼 키 생성
    button_key = f"card_{item_nm}_{kind_nm}".replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
    
    # 카드 내용을 간단한 텍스트로 구성 (버튼 label로 사용)
    button_label = f"{item_nm}({kind_nm})"
    
    # 카드처럼 보이는 버튼 생성
    button_clicked = st.button(
        button_label,
        key=button_key,
        use_container_width=True,
        type="secondary"
    )
    
    # 버튼 아래에 카드 정보 표시
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"<div style='text-align: center; color: #666; font-size: 14px; margin-bottom: 5px;'>{other_channel}</div>", unsafe_allow_html=True)
        st.markdown(f"<div style='text-align: center; color: #333; font-size: 18px; font-weight: 500;'>{other_price:,.0f}원</div>", unsafe_allow_html=True)
    with col2:
        st.markdown(f"<div style='text-align: center; color: #666; font-size: 14px; margin-bottom: 5px;'>{cheaper_channel}</div>", unsafe_allow_html=True)
        st.markdown(f"<div style='text-align: center; color: #28a745; font-size: 20px; font-weight: bold;'>{cheaper_price:,.0f}원</div>", unsafe_allow_html=True)
    with col3:
        st.markdown(f"<div style='text-align: center; color: #666; font-size: 14px; margin-bottom: 5px;'>가격 차이</div>", unsafe_allow_html=True)
        st.markdown(f"<div style='text-align: center; color: #4A90E2; font-size: 18px; font-weight: bold;'>↓ {price_diff:,.0f}원</div>", unsafe_allow_html=True)
    
    # 버튼을 카드처럼 스타일링하는 CSS
    st.markdown(
        f"""
        <style>
        button[kind="secondary"][data-testid="baseButton-secondary"]:has([key="{button_key}"]) {{
            background: white !important;
            border: none !important;
            border-left: 5px solid {border_color} !important;
            border-radius: 10px !important;
            padding: 20px !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
            margin-bottom: 10px !important;
            text-align: left !important;
            height: auto !important;
            min-height: auto !important;
            transition: all 0.3s ease !important;
            cursor: pointer !important;
            width: 100% !important;
        }}
        button[kind="secondary"][data-testid="baseButton-secondary"]:has([key="{button_key}"]):hover {{
            box-shadow: 0 4px 8px rgba(0,0,0,0.15) !important;
            transform: translateY(-2px) !important;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    # 버튼 클릭 시 처리
    if button_clicked:
        st.session_state.selected_item_nm = item_nm
        st.session_state.selected_kind_nm = kind_nm
        st.session_state.show_region_map = True
        st.rerun()


def render_yutong_cheaper_section(df_comparison: pd.DataFrame):
    """대형마트가 더 저렴한 품목 섹션을 렌더링합니다.
    
    Args:
        df_comparison: 비교 데이터프레임 (가격차이 컬럼 포함)
    """
    # 대형마트가 더 저렴한 품목 (가격차이 > 0: 유통이 더 비싸므로 전통이 더 저렴)
    # 실제로는 전통시장이 더 저렴한 경우
    jeontong_cheaper = df_comparison[df_comparison["가격차이"] < 0].copy()
    jeontong_cheaper = jeontong_cheaper.sort_values("가격차이").head(3)
    
    render_channel_comparison_header(
        "대형마트가 더 저렴해요!",
        "#667eea 0%, #764ba2 100%"
    )
    
    if len(jeontong_cheaper) > 0:
        for _, row in jeontong_cheaper.iterrows():
            price_diff = abs(row["가격차이"])
            jeontong_price = row["전통_평균가격"]
            yutong_price = row["유통_평균가격"]
            
            render_price_comparison_card(
                item_nm=row["item_nm"],
                kind_nm=row["kind_nm"],
                cheaper_channel="대형마트",
                cheaper_price=yutong_price,
                other_channel="전통시장",
                other_price=jeontong_price,
                price_diff=price_diff,
                border_color="#667eea"
            )
    else:
        st.info("대형마트가 더 저렴한 품목이 없습니다.")


def render_jeontong_cheaper_section(df_comparison: pd.DataFrame):
    """전통시장이 더 저렴한 품목 섹션을 렌더링합니다.
    
    Args:
        df_comparison: 비교 데이터프레임 (가격차이 컬럼 포함)
    """
    # 전통시장이 더 저렴한 품목 (가격차이 > 0: 유통이 더 비싸므로 전통이 더 저렴)
    yutong_cheaper = df_comparison[df_comparison["가격차이"] > 0].copy()
    yutong_cheaper = yutong_cheaper.sort_values("가격차이", ascending=False).head(3)
    
    render_channel_comparison_header(
        "전통시장이 더 저렴해요!",
        "#28a745 0%, #20c997 100%"
    )
    
    if len(yutong_cheaper) > 0:
        for _, row in yutong_cheaper.iterrows():
            price_diff = abs(row["가격차이"])
            jeontong_price = row["전통_평균가격"]
            yutong_price = row["유통_평균가격"]
            
            render_price_comparison_card(
                item_nm=row["item_nm"],
                kind_nm=row["kind_nm"],
                cheaper_channel="전통시장",
                cheaper_price=jeontong_price,
                other_channel="대형마트",
                other_price=yutong_price,
                price_diff=price_diff,
                border_color="#28a745"
            )
    else:
        st.info("전통시장이 더 저렴한 품목이 없습니다.")


def render_channel_comparison_sections(df_comparison: pd.DataFrame):
    """채널 비교 섹션 전체를 렌더링합니다.
    
    Args:
        df_comparison: 비교 데이터프레임
    """
    col1, col2 = st.columns(2)
    
    with col1:
        render_yutong_cheaper_section(df_comparison)
    
    with col2:
        render_jeontong_cheaper_section(df_comparison)

