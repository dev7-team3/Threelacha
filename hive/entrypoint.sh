#!/bin/bash
set -e

export HIVE_CONF_DIR=${HIVE_CONF_DIR:-/opt/hive/conf}
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Xmx1G ${SERVICE_OPTS}"

echo "=================================================="
echo "Checking Metastore DB schema..."
echo "=================================================="

# PostgreSQL 연결 대기 (nc 대신 간단한 방법)
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if timeout 1 bash -c "</dev/tcp/metastore-db/5432" 2>/dev/null; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    echo "⏳ Waiting... ($i/30)"
    sleep 2
done

# 스키마 존재 여부 확인
echo "Checking if schema exists..."

SCHEMA_VERSION=$(/opt/hive/bin/schematool -dbType ${DB_DRIVER} -info 2>&1 | grep "Metastore schema version" || echo "")

if [[ -n "${SCHEMA_VERSION}" ]]; then
    echo "✅ Schema already exists:"
    echo "   ${SCHEMA_VERSION}"
    echo "   Skipping initialization..."
else
    echo "🔧 Schema not found. Initializing..."
    /opt/hive/bin/schematool -dbType ${DB_DRIVER} -initSchema
    
    if [ $? -eq 0 ]; then
        echo "✅ Schema initialization completed successfully!"
    else
        echo "❌ Schema initialization failed!"
        exit 1
    fi
fi

echo "=================================================="
echo "🚀 Starting Hive Metastore Service..."
echo "=================================================="

exec /opt/hive/bin/hive --service metastore