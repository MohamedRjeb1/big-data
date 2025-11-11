import json
import os
import sys
from kafka import KafkaConsumer

BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
TOPIC = os.environ.get('KAFKA_TOPIC', 'crypto-topic')
# If CONSUME_MAX <= 0 then consume indefinitely
MAX_ENV = int(os.environ.get('CONSUME_MAX', '10'))
MAX = None if MAX_ENV <= 0 else MAX_ENV
# If CONSUMER_TIMEOUT_MS <= 0 then block indefinitely (do not pass consumer_timeout_ms)
_timeout_env = int(os.environ.get('CONSUMER_TIMEOUT_MS', '0'))
TIMEOUT = None if _timeout_env <= 0 else _timeout_env

print(f"consumer starting: bootstrap={BOOTSTRAP} topic={TOPIC} max={MAX}")

# Build kwargs so we only set consumer_timeout_ms when a positive timeout is requested.
kwargs = dict(
    bootstrap_servers=[BOOTSTRAP],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)
if TIMEOUT is not None:
    kwargs['consumer_timeout_ms'] = TIMEOUT
consumer = KafkaConsumer(TOPIC, **kwargs)

count = 0
try:
    for msg in consumer:
        print(json.dumps(msg.value, ensure_ascii=False))
        count += 1
        if MAX is not None and count >= MAX:
            break
except Exception as e:
    print('consumer error:', e, file=sys.stderr)
    raise
finally:
    consumer.close()
    print('done, read', count)
        if MAX is not None and count >= MAX:
