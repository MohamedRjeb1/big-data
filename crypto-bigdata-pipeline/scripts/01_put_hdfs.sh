#!/usr/bin/env bash
# 01_put_hdfs.sh
# Put local data/crypto.csv into HDFS at /crypto/raw/crypto.csv. Waits for HDFS namenode UI availability.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
HDFS_URI="hdfs://hadoop-namenode:8020"

echo "Waiting for HDFS namenode web UI..."
until curl -sSf http://hadoop-namenode:9870 || [ $? -eq 0 ]; do
  echo "Waiting for HDFS..."; sleep 3
done

echo "Creating HDFS directories and putting crypto.csv"
docker exec -i $(docker ps --filter "ancestor=bde2020/hadoop-namenode" -q | head -n1) bash -lc "hdfs dfs -mkdir -p /crypto/raw || true"
docker exec -i $(docker ps --filter "ancestor=bde2020/hadoop-namenode" -q | head -n1) bash -lc "hdfs dfs -put -f /data/crypto.csv /crypto/raw/"

echo "Done. Use 'hdfs dfs -ls /crypto/raw' to verify." 
