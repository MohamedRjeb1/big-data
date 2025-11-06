#!/usr/bin/env bash
# 04_run_mapreduce.sh
# Runs a Hadoop Streaming job to compute daily stats and then imports results into MongoDB using mongoimport.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

HADOOP_CONTAINER=$(docker ps --filter "ancestor=bde2020/hadoop-namenode" -q | head -n1)
if [ -z "$HADOOP_CONTAINER" ]; then
  echo "Hadoop namenode container not found via docker ps. Ensure docker-compose is up."; exit 1
fi

echo "Running Hadoop Streaming job"
docker exec -i $HADOOP_CONTAINER bash -lc "hdfs dfs -rm -r -f /crypto/output || true"
docker exec -i $HADOOP_CONTAINER bash -lc "hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=1 \
  -input /crypto/raw -output /crypto/output \
  -mapper 'python3 /mapper.py' -reducer 'python3 /reducer.py' \
  -file /mapper.py -file /reducer.py"

echo "Copying output to local"
docker exec -i $HADOOP_CONTAINER bash -lc "hdfs dfs -cat /crypto/output/part-*" > ${ROOT_DIR}/hadoop_output.csv

echo "Importing into MongoDB (collection daily_stats)"
mongoimport --host mongo --db crypto --collection daily_stats --type csv --headerline --file ${ROOT_DIR}/hadoop_output.csv || true

echo "MapReduce and import finished. Output saved to ${ROOT_DIR}/hadoop_output.csv"
