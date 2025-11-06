#!/usr/bin/env bash
# 03_submit_spark.sh
# Submits the Spark Structured Streaming job via spark-submit. Waits for Spark master.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SPARK_MASTER=${SPARK_MASTER:-spark://spark-master:7077}

echo "Waiting for Spark master UI..."
until curl -sSf http://spark-master:8080 || [ $? -eq 0 ]; do
  echo "Waiting for Spark master..."; sleep 3
done

echo "Submitting Spark job to ${SPARK_MASTER}"
spark-submit --master ${SPARK_MASTER} \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:log4j.properties" \
  ${ROOT_DIR}/spark/spark_streaming.py &

echo "Spark job submitted"
