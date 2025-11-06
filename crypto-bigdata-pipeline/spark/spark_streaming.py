"""
spark_streaming.py
PySpark Structured Streaming job that reads from Kafka topic `crypto-topic`, parses JSON messages
using an explicit schema, computes 1-minute tumbling window aggregations with watermarking,
and writes results to MongoDB using a foreachBatch upsert (pymongo) to guarantee portability.

Config via environment variables:
  - KAFKA_BOOTSTRAP (default: kafka:9092)
  - MONGO_URI (default: mongodb://mongo:27017)
  - CHECKPOINT_DIR (default: ./checkpoints)

Run via spark-submit. The script uses spark session created by spark-submit.
"""
import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, window

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://mongo:27017')
KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
CHECKPOINT = os.environ.get('CHECKPOINT_DIR', '/tmp/spark-checkpoints')

logging.basicConfig(level=logging.INFO)

schema = StructType([
    StructField('timestamp', StringType(), True),
    StructField('open', DoubleType(), True),
    StructField('high', DoubleType(), True),
    StructField('low', DoubleType(), True),
    StructField('close', DoubleType(), True),
    StructField('volume', DoubleType(), True),
    StructField('symbol', StringType(), True),
])

def upsert_to_mongo(batch_df, batch_id):
    # This function is executed on the driver for each microbatch
    import pymongo
    docs = [row.asDict() for row in batch_df.collect()]
    if not docs:
        return
    client = pymongo.MongoClient(MONGO_URI)
    db = client['crypto']
    coll = db['realtime']
    # Upsert by symbol + window start
    operations = []
    for d in docs:
        key = {'symbol': d.get('symbol'), 'window_start': d.get('window').get('start')}
        ops = {
            '$set': {
                'symbol': d.get('symbol'),
                'window_start': d.get('window').get('start'),
                'window_end': d.get('window').get('end'),
                'avg_price': d.get('avg_price'),
                'max_price': d.get('max_price'),
                'min_price': d.get('min_price'),
                'sum_volume': d.get('sum_volume'),
                'count_trades': d.get('count_trades')
            }
        }
        operations.append(pymongo.UpdateOne(key, ops, upsert=True))
    if operations:
        coll.bulk_write(operations, ordered=False)
    client.close()

def main():
    spark = SparkSession.builder.appName('crypto-streaming') \
        .config('spark.mongodb.output.uri', MONGO_URI) \
        .getOrCreate()

    df = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', 'crypto-topic') \
        .option('startingOffsets', 'latest') \
        .load()

    # value is bytes -> string
    parsed = df.select(from_json(col('value').cast('string'), schema).alias('data')) \
        .selectExpr(
            "to_timestamp(data.timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp",
            'data.open as open', 'data.high as high', 'data.low as low', 'data.close as close',
            'data.volume as volume', 'data.symbol as symbol')

    # windowed aggregations (1 minute tumbling)
    agg = parsed.withWatermark('timestamp', '2 minutes') \
        .groupBy(window(col('timestamp'), '1 minute'), col('symbol')) \
        .agg(
            {'close': 'avg', 'close': 'max', 'close': 'min', 'volume': 'sum', '*': 'count'}
        )

    # Rename columns to useful names
    result = agg.select(
        col('symbol'),
        col('window'),
        col('avg(close)').alias('avg_price'),
        col('max(close)').alias('max_price'),
        col('min(close)').alias('min_price'),
        col('sum(volume)').alias('sum_volume'),
        col('count(1)').alias('count_trades')
    )

    # Write to Mongo via foreachBatch upsert
    query = result.writeStream \
        .foreachBatch(lambda df, epoch_id: upsert_to_mongo(df, epoch_id)) \
        .option('checkpointLocation', CHECKPOINT) \
        .outputMode('update') \
        .start()

    query.awaitTermination()

if __name__ == '__main__':
    main()
