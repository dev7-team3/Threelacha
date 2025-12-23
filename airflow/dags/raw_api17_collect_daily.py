from datetime import datetime, timedelta
import json
import os
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
import dotenv
import requests

dotenv.load_dotenv()

API_KEY = os.getenv("CERT_KEY")
ID = os.getenv("CERT_ID")


REQUEST_URL = "https://www.kamis.or.kr/service/price/xml.do?action=periodRetailProductList"

with Path.open(Path(__file__).parent.parent / "plugins" / "param_tree.json", "r", encoding="utf-8") as f:
    params_tree = json.load(f)

with Path.open(Path(__file__).parent.parent / "plugins" / "country_code.json", "r", encoding="utf-8") as f:
    country_code_mapping = json.load(f)


def set_category_product_variety_retail_code() -> dict:
    """카테고리, 품목, 품종, 판매코드 정보를 설정

    Yields:
        Iterator[dict]: 카테고리, 품목, 품종, 판매코드 정보
    """
    for category in params_tree:
        for product in params_tree[category]["products"]:
            for variety in params_tree[category]["products"][product]["varieties"]:
                for retail_code in params_tree[category]["products"][product]["varieties"][variety]["retail_codes"]:
                    yield {
                        "item_category_code": category,
                        "item_code": product,
                        "kind_code": variety,
                        "product_rank_code": retail_code,
                    }


def group_data_by_date(data: dict) -> dict[str, list] | None:
    """
    날짜별로 데이터를 그룹화

    Args:
        data (dict): API 응답 데이터

    Returns:
        dict[str, list] | None: 날짜별 데이터. 오류 발생 시 None 반환
    """
    grouped = {}
    for item in data["data"]["item"]:
        # yyyy: "2025", regday: "12/17" -> "20251217"
        yyyy = item.get("yyyy", "")
        regday = item.get("regday", "")

        if yyyy and regday:
            # "12/17" -> "1217" (MM/DD -> MMDD)
            month_day = regday.replace("/", "-")
            date_str = f"{yyyy}-{month_day}"  # "2025-12-17"

            if date_str not in grouped:
                grouped[date_str] = []
            grouped[date_str].append(item)

    return grouped


def get_data(
    country_code: str,
    item_category_code: str,
    item_code: str,
    kind_code: str,
    product_rank_code: str,
    start_day: str,
    end_day: str,
) -> dict | None:
    """API를 호출하여 데이터를 가져옴

    Args:
        country_code (str): 도시 코드
        item_category_code (str): 카테고리 코드
        item_code (str): 품목 코드
        kind_code (str): 품종 코드
        product_rank_code (str): 판매코드
        start_day (str): 시작 날짜
        end_day (str): 종료 날짜

    Returns:
        dict | None: 날짜별 데이터. 오류 발생 시 None 반환
    """
    url = f"{REQUEST_URL}&p_cert_key={API_KEY}&p_cert_id={ID}&p_returntype=json&p_startday={start_day}&p_endday={end_day}&p_countrycode={country_code}&p_convert_kg_yn=N&p_itemcategorycode={item_category_code}&p_itemcode={item_code}&p_kindcode={kind_code}&p_productrankcode={product_rank_code}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return group_data_by_date(data)
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error: {e}") from e


def upload_data_to_s3(
    country_code: str,
    date_data: dict[str, list],
    category_info: dict,
) -> str:
    """날짜별 데이터를 S3에 업로드

    Args:
        country_code (str): 도시 코드
        date_data (dict[str, list]): 날짜별 데이터
        category_info (dict): 카테고리, 품목, 품종, 판매코드 정보
    """
    # 경로 구성: raw/api-17/dt=YYYYMMDD/product_cls=01/country=1101/category=100/item=111/kind=01/product_rank=04/data.json
    product_cls = "01"
    item_category_code = category_info["item_category_code"]
    item_code = category_info["item_code"]
    kind_code = category_info["kind_code"]
    product_rank_code = category_info["product_rank_code"]

    hook = S3Hook(aws_conn_id="s3_conn")

    for date_str, data in date_data.items():
        key = (
            f"raw/api-17/dt={date_str}/"
            f"product_cls={product_cls}/country={country_code}/category={item_category_code}/"
            f"item={item_code}/kind={kind_code}/product_rank={product_rank_code}/data.json"
        )

        json_data = json.dumps(data, ensure_ascii=False)

        bucket_name = "team3-batch"

        hook.load_string(
            string_data=json_data,
            key=key,
            bucket_name=bucket_name,
            replace=True,
        )


def get_data_by_country_code(country_code: str, **context) -> dict:
    """도시 코드에 해당하는 데이터를 가져옴

    Args:
        country_code (str): 도시 코드
        **context: 컨텍스트

    Returns:
        dict: 날짜별 데이터
    """
    logical_date = context.get("logical_date") or context.get("data_interval_start")

    if logical_date is None:
        raise ValueError("logical_date 또는 data_interval_start를 찾을 수 없습니다.")

    start_day = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
    end_day = logical_date.strftime("%Y-%m-%d")

    for category in set_category_product_variety_retail_code():
        data_from_api = get_data(
            country_code=country_code,
            item_category_code=category["item_category_code"],
            item_code=category["item_code"],
            kind_code=category["kind_code"],
            product_rank_code=category["product_rank_code"],
            start_day=start_day,
            end_day=end_day,
        )

        if data_from_api is None:
            continue

        upload_data_to_s3(
            country_code=country_code,
            date_data=data_from_api,
            category_info=category,
        )


with DAG(
    dag_id="raw_api17_collect_daily",
    start_date=datetime(2025, 12, 10),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "owner": "jiyeon_kim",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["api_ingestion"],
) as dag:
    for country_code in country_code_mapping.values():
        get_data_by_region_task = PythonOperator(
            task_id=f"get_data_by_region_{country_code}",
            python_callable=get_data_by_country_code,
            op_kwargs={
                "country_code": country_code,
            },
        )
