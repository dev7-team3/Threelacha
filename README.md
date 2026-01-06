# Local Development Environment for AWS-like Data Pipelines

본 레포지토리는 **AWS 기반 데이터 파이프라인을 로컬 환경에서 최대한 유사하게 재현**하기 위해 구성된 개발용 데이터 플랫폼입니다.<br>
실제 AWS 비용을 사용하지 않고도, S3–Glue–Athena–DBT-Airflow 중심의 아키텍처를 그대로 로컬에서 검증하는 것을 목표로 합니다.<p>
이와 같은 로컬 AWS 유사 환경을 선제적으로 구성한 배경은 다음과 같습니다.
- 첫째, Glue Data Catalog와 Athena 등 비교적 생소한 AWS 서비스 도입에 앞서, 로컬에서 유사한 아키텍처를 직접 구성함으로써 전체 데이터 흐름과 인프라 구조에 대한 이해도를 높이고 설계 단계에서의 오류를 사전에 차단하기 위함입니다.
- 둘째, 데이터 수집부터 정제·집계·마트 생성까지의 전 과정을 로컬 환경에서 단계적으로 프로토타입 구현함으로써, 비즈니스 로직과 데이터 처리 흐름을 가시화하고 핵심 파이프라인 로직을 조기에 검증하기 위함입니다.
- 마지막으로, 프로젝트 종료 이후 클라우드 자원 반납 상황을 고려하여, 언제든 재구축이 가능한 Portable한 개발 환경을 마련함으로써 운영 지속성과 복구 가용성을 확보하고자 하였습니다.

---
## ✨ 핵심 특징

- AWS 서비스와 1:1 대응되는 로컬 구성
- .env 기반 환경변수로 Local ↔ AWS 이관 시 코드 수정 최소화
- Raw → Silver → Gold Lakehouse 아키텍처 구현
- Airflow + dbt 기반 ELT 오케스트레이션

---
## ☁️ AWS 대응 구조

| AWS                   | Local 구성                        |
| ----------------------| -------------------------------- |
| airflow on EC2 / MWAA | Apache Airflow (Celery Executor) |
| dbt on EC2            | dbt Container                    |
| S3                    | MinIO                            |
| Glue Data Catalog     | Hive Metastore                   |
| Athena                | Trino                            |

---
## 🚀 설정 및 실행방법
### 1) Git Clone
```
git clone git@github.com:dev7-team3/Threelacha.git
cd Threelacha
```
### 2) 환경 변수 설정
```.env.example``` 파일을 복사하여 ```.env``` 파일을 생성합니다.
```
cp .env.example .env
```
필요에 따라 ```.env``` 파일의 아래 항목들을 환경에 맞게 수정합니다.<br>
```CERT_KEY``` 및 ```CERT_ID``` 항목을 제외하고는, 변경하지 않아도 ```docker-compse.yaml``` 설정된 기본값으로 실행가능합니다.
```
AIRFLOW_DB_USER=<airflow_user>
AIRFLOW_DB_PASSWORD=<airflow_password>
AIRFLOW_DB_NAME=<airflow_db>

_AIRFLOW_WWW_USER_USERNAME=<admin_user>
_AIRFLOW_WWW_USER_PASSWORD=<admin_password>

_MINIO_WWW_USER_USERNAME=<minio_admin>
_MINIO_WWW_USER_PASSWORD=<minio_password>

CERT_KEY=<YOUR_KAMIS_API_KEY>
CERT_ID=<YOUR_KAMIS_API_ID>
```
### 3) Docker Compose 실행
```
docker compose up -d
```
### 4) 서비스 접속 확인
| 서비스              | 주소                                             |
| ---------------- | ---------------------------------------------- |
| Airflow Web UI   | [http://localhost:8080](http://localhost:8080) |
| MinIO Console    | [http://localhost:9001](http://localhost:9001) |
| Trino            | [http://localhost:8082](http://localhost:8082) |
| Jupyter Notebook | [http://localhost:8888](http://localhost:8888) |

---
## 📁 디렉토리 구조
```
THREELACHA/
├── airflow/                   
│   ├── dags/                   # Airflow DAG 파일 저장소
│   ├── plugins/                # Airflow 커스텀 플러그인
│   ├── logs/                   # Airflow 실행 로그
│   ├── config/                 # airflow.cfg 및 설정 파일
│   └── Dockerfile              # [Build] Airflow 커스텀 이미지 (trino provider 설치)
│
├── dbt/                        
│   ├── Threelacha/             # 메인 dbt 프로젝트 폴더
│   │   ├── models/             # dbt models (gold)
│   │   ├── seeds/              # Seed data
│   │   ├── tests/              # dbt tests
│   │   ├── logs/               # dbt 실행 로그
│   │   └── dbt_project.yml     # dbt 프로젝트 설정
│   ├── profiles.yml            # trino / Postgres 연결 설정
│   └── Dockerfile              # [Build] dbt 커스텀 이미지 (trino adapter 설치)
│
├── hive/                       
│   ├── core-site.xml           # S3A / MinIO 설정
│   ├── entrypoint.sh           # Docker image 빌드 시 필요 파일 (메타스토어 DB 초기화 여부 검증 스크립트 쉘)
│   └── Dockerfile              # [Build] hive 커스텀 이미지 (S3A 라이브러리, PostgreSQL JDBC 설치)
│
├── trino/                      
│   └── catalog/
│       └── hive.properties     # Hive Metastore 및 MinIO 커넥터 설정
│
├── minio-data/                 # MinIO 로컬 데이터 볼륨
│   └── team3-batch/            # Raw / Silver / Gold data buckets
│
├── notebooks/                  # 데이터 분석용 Jupyter Notebook
│
├── streamlit/                  # 데이터 시각화 및 대시보드 어플리케이션
│   ├── app.py                  # 메인 실행 파일
│   ├── components/             # 시각화 컴포넌트 모듈
│   ├── data/                   # 대시보드용 데이터
│   └── styles.css              # UI 스타일 정의
│
├── docker-compose.yaml         
├── .env.example                # 환경 변수 설정 샘플
├── pyproject.toml              # 프로젝트 메타데이터 및 의존성 정의
├── uv.lock                     # 패키지 버전 고정 파일 (uv)
├── README.md
└── LICENSE
```
---
## ⚙️ 주요 서비스 설명
### 1. Apache Airflow (Celery Executor)
- Scheduler / Worker / Triggerer / API Server 분리 구성
- Redis + PostgreSQL 기반 Celery Executor
- dbt 컨테이너 실행을 위한 Docker Socket 연동
- .env 기반으로 Local / AWS 커넥션 분기 처리

📌 Web UI: http://localhost:8080

### 2. MinIO (S3 대체)
- AWS S3와 동일한 API 제공
- Raw / Silver / Gold 레이어 데이터 저장
- Trino, Jupyter(Spark), Airflow에서 동일한 방식으로 접근
📌 API: 9000
📌 Console: http://localhost:9001

### 3. Hive Metastore (Glue Catalog 대체)
- PostgreSQL 기반 메타스토어
- Trino / Spark / dbt에서 공통 메타데이터 사용
- Glue Data Catalog와 거의 동일한 역할 수행
📌 Metastore Thrift: 9083

### 4. Trino (Athena 대체)
- Hive Metastore + MinIO 연동
- Interactive SQL Query Engine
- dbt, BI, 분석 쿼리 용도
📌 포트: Web UI / API: http://localhost:8082

### 5. dbt
- 컨테이너 기반 dbt 실행
- Silver → Gold 데이터 변환
- Airflow DAG에서 트리거 가능

### 6. Jupyter (Spark / 분석)
- PySpark Notebook 환경
- 실행 시, 토큰입력 비활성화 설정
- MinIO 연동 설정 포함
- 데이터 검증 및 탐색 용도
📌 Notebook: http://localhost:8888

---
## 🔄 MinIO Client (mc) – AWS S3 이관용 유틸리티
본 레포의 ```docker-compose.yaml```에 정의된 서비스에는 ```minio/mc``` 컨테이너가 포함되어 있으며,<br>
이는 로컬 MinIO(S3 호환)에서 실제 AWS S3로 데이터를 이관하기 위한 전용 도구입니다.
- Airflow/Spark와 분리된 데이터 이관 전용 컨테이너
- AWS CLI를 호스트에 설치하지 않고도 이관 가능
- mc mirror --dry-run을 통한 사전 검증 후 안전한 이관

### 사용 예시
```
# 컨테이너 접속
docker exec -it minio-mc sh

# Local MinIO 등록
mc alias set minio http://minio:9000 admin adminadmin

# AWS S3 등록
mc alias set s3 https://s3.ap-northeast-2.amazonaws.com \
  $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY

# minio 폴더 확인
mc ls minio

# mc mirror dry-run : 실질적인 이관없이 변경사항만 확인하기
mc mirror --dry-run minio/threelacha/raw/api-13 s3/team3-batch/raw/api-13

# 데이터 이관
mc mirror minio/threelacha/raw/api-13 s3/team3-batch/raw/api-13

```