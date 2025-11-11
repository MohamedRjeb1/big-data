
"""
producer.py - envoie en continu les lignes de data/crypto.csv vers le topic kafka `crypto-topic`.
- utilise le timestamp courant (int epoch)
- n'envoie pas la colonne 'target'
- logue "message numero N" pour chaque message envoyé
"""

import os
import time
import json
import argparse
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'crypto.csv')

def parse_row_keep_all_but_target(line):
    parts = [p.strip() for p in line.split(',')]
    if len(parts) == 10:
        # timestamp from CSV ignored (we will overwrite), keep other fields except target
        _, asset_id, count, open_p, high, low, close, volume, vwap, _ = parts
        return {
            # timestamp will be set by caller (current epoch)
            'Asset_ID': asset_id,
            'count': float(count) if count != '' else None,
            'open': float(open_p) if open_p != '' else None,
            'high': float(high) if high != '' else None,
            'low': float(low) if low != '' else None,
            'close': float(close) if close != '' else None,
            'volume': float(volume) if volume != '' else None,
            'vwap': float(vwap) if vwap != '' else None,
            'symbol': str(asset_id)
        }
    elif len(parts) == 7:
        _, open_p, high, low, close, volume, symbol = parts
        return {
            'open': float(open_p) if open_p != '' else None,
            'high': float(high) if high != '' else None,
            'low': float(low) if low != '' else None,
            'close': float(close) if close != '' else None,
            'volume': float(volume) if volume != '' else None,
            'symbol': symbol
        }
    else:
        raise ValueError(f'Bad CSV row length: {len(parts)}')

def get_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )

def run(rate, bootstrap, topic='crypto-topic'):
    producer = get_producer([bootstrap])
    logging.info('Producer connected to %s', bootstrap)
    msg_counter = 0

    while True:
        with open(DATA_PATH, 'r', encoding='utf-8') as fh:
            header = fh.readline()
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = parse_row_keep_all_but_target(line)
                except Exception:
                    logging.exception('Malformed line, skipping: %s', line)
                    continue

                # overwrite timestamp with current epoch
                payload['timestamp'] = int(time.time())

                try:
                    producer.send(topic, payload)
                except KafkaError:
                    logging.exception('Failed to send to Kafka, will retry briefly')
                    time.sleep(1)
                    continue

                msg_counter += 1
                logging.info('message numero %d', msg_counter)

                if rate > 0:
                    time.sleep(1.0 / float(rate))

        logging.info('End of file reached - replaying (continuous mode)')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rate', type=float, default=1.0, help='messages per second')
    parser.add_argument('--bootstrap-server', default=os.environ.get('KAFKA_BOOTSTRAP','kafka:9092'))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        run(args.rate, args.bootstrap_server)
    except KeyboardInterrupt:
        logging.info('Producer stopped by user')
