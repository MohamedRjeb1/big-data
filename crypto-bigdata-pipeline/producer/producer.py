"""
producer.py
Simple Kafka producer that reads ./data/crypto.csv line-by-line and publishes JSON messages
to topic `crypto-topic`. Supports rate limiting (--rate), replay loop (--loop), and configurable
bootstrap server via env var KAFKA_BOOTSTRAP (default localhost:9092).
"""
import os
import time
import json
import argparse
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'crypto.csv')

def parse_row(line):
    parts = [p.strip() for p in line.split(',')]
    if len(parts) != 7:
        raise ValueError('Bad CSV row length')
    timestamp, open_p, high, low, close, volume, symbol = parts
    return {
        'timestamp': timestamp,
        'open': float(open_p),
        'high': float(high),
        'low': float(low),
        'close': float(close),
        'volume': float(volume),
        'symbol': symbol
    }

def get_producer(bootstrap_servers):
    return KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         retries=5)

def run(rate, loop, bootstrap, topic='crypto-topic'):
    producer = get_producer([bootstrap])
    logging.info('Producer connected to %s', bootstrap)
    while True:
        with open(DATA_PATH, 'r', encoding='utf-8') as fh:
            header = fh.readline()
            for line in fh:
                line=line.strip()
                if not line:
                    continue
                try:
                    msg = parse_row(line)
                except Exception as e:
                    logging.exception('Malformed line, skipping: %s', line)
                    continue
                try:
                    producer.send(topic, msg)
                except KafkaError:
                    logging.exception('Failed to send to Kafka, will retry')
                    time.sleep(1)
                if rate>0:
                    time.sleep(1.0/float(rate))
        if not loop:
            break
        logging.info('Replay loop enabled - restarting dataset')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rate', type=float, default=1.0, help='messages per second')
    parser.add_argument('--loop', action='store_true', help='replay data infinitely')
    parser.add_argument('--bootstrap-server', default=os.environ.get('KAFKA_BOOTSTRAP','localhost:9092'))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        run(args.rate, args.loop, args.bootstrap_server)
    except KeyboardInterrupt:
        logging.info('Producer stopped by user')
