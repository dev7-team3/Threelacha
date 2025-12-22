# 농수산물 api를 사용한 데이터 파이프라인 및 대시보드

## Airflow · MinIO · Trino · dbt 데이터 파이프라인 실행 가이드

이 문서는 **로컬 Docker 환경에서 Airflow–MinIO–Trino–dbt 기반 데이터 파이프라인을 실행하는 방법**을 설명합니다.
최초 1회 설정 이후에는 DAG 실행만으로 전체 파이프라인을 재현할 수 있습니다.

---

### 0. Airflow 메타데이터 DB 볼륨 생성 및 Docker Compose 구동

Airflow 메타데이터(DB) 유실을 방지하기 위해 **external volume**을 먼저 생성합니다.

```bash
docker volume create airflow-metadata-db
docker-compose up -d
```

* `airflow-metadata-db` : Airflow Connection, Variable, DAG 메타데이터 유지용
* 이후 `docker-compose down -v`를 실행해도 메타데이터는 유지됩니다.

---

### 1. Airflow에 MinIO Connection 등록 및 연결 확인

#### 1-1. Airflow Web UI → Admin → Connections

아래 정보를 입력합니다.

* **Connection ID** : `minio_conn` (이름은 변경 가능)
* **Connection Type** : `Amazon Web Services`
* **AWS Access Key ID** : MinIO Access Key
* **AWS Secret Access Key** : MinIO Secret Key

##### Extra 필드 (JSON)

```json
{
  "endpoint_url": "http://minio:9000"
}
```

> ⚠️ `endpoint_url` 은 **컨테이너 내부 기준 주소**입니다 (`localhost` 아님)

---

#### 1-2. MinIO 연결 테스트 DAG 실행

* DAG: `test_minio_connection.py`
* 수행 내용

  * Airflow ↔ MinIO 연결 테스트
  * `team3-batch` bucket 자동 생성

정상 실행 시:

* Airflow 로그에 bucket 목록 출력
* MinIO Console에서 `team3-batch` 확인 가능

---

### 2. Airflow를 통한 데이터 수집 (Raw → Silver)

* DAG: `raw_api1_collect_daily.py`
* DAG: `silver_api1_transform.py`

수행 내용:

* KAMIS API 호출 (일 단위)
* Raw 데이터 MinIO 저장
* Silver 레이어용 정제 데이터 생성

---

### 3. Airflow에 Trino Connection 등록 및 연결 확인

#### 3-1. Airflow Web UI → Admin → Connections

아래 정보를 입력합니다.

* **Connection ID** : `trino_conn`
* **Host** : `trino`
  → Docker Compose 기준 Trino 컨테이너 이름
* **Login** : `admin`
  → Trino는 인증을 사용하지 않지만, 필수 입력 필드
* **Port** : `8080`
* **Schema** : `hive`
  → Trino Catalog 이름 (`trino/catalog/hive.properties`)

---

#### 3-2. Trino 연결 테스트 DAG 실행

* DAG: `test_trino_conn.py`
* 수행 내용

  * Airflow ↔ Trino SQL 실행 테스트
  * Hive Metastore 연동 확인

---

### 4. Silver 스키마 Hive Metastore 등록

* DAG: `make_silver_schema_ap1.py`

수행 내용:

* Trino를 통해 Hive Metastore에 Silver 스키마 등록
* MinIO 경로 기반 External Table 생성

---

### 5. dbt를 이용한 Gold 데이터마트 생성

* DAG: `run_dbt_transform_gold.py`

수행 내용:

* dbt 모델 실행
* Silver → Gold 데이터마트 변환
* Trino + Hive Metastore 기반 분석 테이블 생성

---

### 전체 파이프라인 요약

```text
KAMIS API
   ↓
Airflow (Raw 수집)
   ↓
MinIO (Raw)
   ↓
Airflow (Silver 변환)
   ↓
MinIO (Silver)
   ↓
Trino + Hive Metastore
   ↓
dbt (Gold Data Mart)
```

---

### 참고 사항

* Airflow Connection 정보는 **airflow-metadata-db 볼륨**에 저장됩니다.
* MinIO / Trino / dbt 관련 자격 정보는 `.env` 파일로 관리합니다.
* `dbt/target`, `minio-data`, `logs` 등은 Git에 포함되지 않습니다.

---

### Troubleshooting

* MinIO 연결 실패 시: `endpoint_url` 이 `http://minio:9000` 인지 확인
* Trino 연결 실패 시: `schema = hive` 입력 여부 확인
* DAG가 보이지 않을 경우: Airflow Scheduler / Dag Processor 상태 확인
