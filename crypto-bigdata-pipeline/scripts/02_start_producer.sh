#!/usr/bin/env bash
# 02_start_producer.sh
# Starts the Python Kafka producer inside the producer folder (local execution) or in a container.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-kafka:9092}
RATE=${1:-1}
LOOP_FLAG=${2:-"--loop"}

echo "Ensure Kafka is reachable at ${KAFKA_BOOTSTRAP}"
until nc -z $(echo ${KAFKA_BOOTSTRAP} | cut -d: -f1) $(echo ${KAFKA_BOOTSTRAP} | cut -d: -f2) ; do
  echo "Waiting for Kafka..."; sleep 2
done

echo "Starting producer with rate ${RATE} msgs/sec"
cd "${ROOT_DIR}/producer"
python -m pip install -r requirements.txt >/dev/null 2>&1 || true
nohup python producer.py --rate ${RATE} --loop --bootstrap-server ${KAFKA_BOOTSTRAP} &>/tmp/producer.log &
echo "Producer started (logs -> /tmp/producer.log)"
