"""
KAMIS API10 데이터 수집 DAG

지역별 소매 가격 데이터를 수집하여 S3에 저장합니다.

작업 흐름:
    1. fetch_api: API 호출 및 응답 검증
    2. build_path: 메타데이터 추출 및 S3 경로 생성
    3. save_to_s3: JSON 데이터를 S3에 업로드
"""

import logging
from typing import Dict, Optional

from airflow.sdk import dag, task
from api_caller_utils import call_kamis_api, validate_api10_response
from metadata_loader_utils import generate_api10_params
import pendulum
from s3_uploader_utils import build_s3_path, upload_json_to_s3

API10_ACTION = "dailyCountyList"


@dag(
    dag_id="raw_api10_collect_daily",
    description="KAMIS API10 지역별 소매 가격 데이터 수집",
    schedule=None,  # KST 11:00
    start_date=pendulum.datetime(2025, 12, 23),
    catchup=False,
    tags=["KAMIS", "api-10", "raw", "daily"],
    default_args={
        "owner": "dahye",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(hours=1),
    },
)
def extract_and_load_kamis_api10():
    """KAMIS API10 데이터 수집 파이프라인"""

    @task(max_active_tis_per_dag=3)
    def fetch_api(
        req: Dict[str, str],
    ) -> Optional[Dict]:
        """
        Task 1: API 호출 및 응답 검증

        Args:
            req (Dict[str, str]): API 요청 파라미터 (country_code 포함)

        Returns:
            Optional[Dict]: API 응답 JSON (성공 시), 실패 시 None

        Raises:
            AirflowSkipException: 응답 검증 실패 시 스킵 처리
        """
        logger = logging.getLogger("airflow.task")

        logger.info(f"[API10 호출 시작] 지역={req['country_code']}")

        params = {
            "p_countycode": req["country_code"],
        }

        response = call_kamis_api(action=API10_ACTION, params=params)
        validate_api10_response(response)

        response["_country_code"] = req["country_code"]

        return response

    @task
    def build_path(response: Dict) -> Optional[Dict]:
        """
        Task 2: S3 경로 생성

        Args:
            response (Dict): API 응답 JSON (fetch_api 출력)

        Returns:
            Optional[Dict]: {"response": 응답 JSON, "s3_key": 생성된 S3 경로}
                            실패 시 None
        """
        logger = logging.getLogger("airflow.task")

        try:
            dt = response["condition"][0][0]
            country_code = response.get("_country_code", "")
        except Exception:
            logger.warning("⚠️ API10: condition 블록에서 날짜 추출 실패")
            return None

        s3_key = build_s3_path(
            api_number="10",
            dt=dt,
            product_cls="01",
            country=country_code,
            dt_normalized=True,
        )

        logger.info(f"📁 S3 경로 생성: {s3_key}")

        return {"response": response, "s3_key": s3_key}

    @task
    def save_to_s3(data: Optional[Dict]) -> Optional[str]:
        """
        Task 3: S3 업로드

        Args:
            data (Optional[Dict]): build_path 출력
                - response: API 응답 JSON
                - s3_key: S3 저장 경로

        Returns:
            Optional[str]: 업로드된 S3 키, 실패 시 None
        """
        logger = logging.getLogger("airflow.task")  # ✅ 추가

        if not data:
            logger.warning("⚠️ API10: 저장할 데이터 없음")
            return None

        return upload_json_to_s3(
            data=data["response"],
            s3_key=data["s3_key"],
        )

    # ========================================
    # Task 체이닝
    # ========================================
    requests = list(generate_api10_params())
    logging.getLogger("airflow.task").info(f"API10 요청 파라미터 수: {len(requests)}")

    api_responses = fetch_api.expand(req=requests)
    s3path_with_res = build_path.expand(response=api_responses)
    save_to_s3.expand(data=s3path_with_res)


# ============================================================
# DAG 인스턴스 생성
# ============================================================

extract_and_load_kamis_api10()
