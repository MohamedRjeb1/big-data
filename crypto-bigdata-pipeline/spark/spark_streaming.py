import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType
from pyspark.sql.functions import (
    from_json, col, window, avg, max as spark_max, min as spark_min, sum as spark_sum, count,
    to_timestamp, from_unixtime, coalesce, current_timestamp
)

MONGO_URI = os.environ.get('MONGO_URI','mongodb://mongo:27017')
KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP','kafka:9092')
CHECKPOINT = os.environ.get('CHECKPOINT_DIR','/tmp/spark-checkpoints')
INFLUX_HOST = os.environ.get('INFLUX_HOST', 'http://influxdb:8086')
INFLUX_DB = os.environ.get('INFLUX_DB', 'metrics')
KAFKA_STARTING_OFFSETS = os.environ.get('KAFKA_STARTING_OFFSETS', 'latest')

# Use a flexible map schema because incoming CSV-derived JSON may have varying
# field names/casing (e.g., 'Close' vs 'close', 'Asset_ID' vs 'asset_id', 'target').
map_schema = MapType(StringType(), StringType())


def upsert_to_mongo(batch_df, batch_id):
    """More robust upsert: avoid extra DataFrame actions (count/collect) and
    build bulk ops while iterating rows. Log minimal info to avoid long blocking.
    """
    client = None
    try:
        import pymongo
        from pymongo import UpdateOne
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client['crypto']
        coll = db['realtime']

        ops = []
        processed = 0
        last_sample = None
        influx_lines = []

        for row in batch_df.toLocalIterator():
            try:
                d = row.asDict()
                w = d.get('window')
                if not w:
                    continue
                start = getattr(w, 'start', None) if not isinstance(w, dict) else w.get('start')
                if start is None:
                    continue
                try:
                    start_key = start.isoformat()
                except Exception:
                    start_key = str(start)
                end = getattr(w, 'end', None) if not isinstance(w, dict) else w.get('end')
                try:
                    end_key = end.isoformat() if end is not None else None
                except Exception:
                    end_key = str(end) if end is not None else None

                key = {'symbol': d.get('symbol'), 'window_start': start_key}
                update = {'$set':{
                    'symbol': d.get('symbol'),
                    'window_start': start_key,
                    'window_end': end_key,
                    'avg_price': d.get('avg_price'),
                    'max_price': d.get('max_price'),
                    'min_price': d.get('min_price'),
                    'sum_volume': d.get('sum_volume'),
                    'count_trades': d.get('count_trades')
                }}
                ops.append(UpdateOne(key, update, upsert=True))

                # prepare InfluxDB line protocol for this aggregated row
                try:
                    # build measurement: crypto_agg, tag=symbol, fields=metrics
                    symbol = d.get('symbol') or 'None'
                    # ensure numeric fields are JSON-serializable numbers or null
                    avg_p = d.get('avg_price')
                    max_p = d.get('max_price')
                    min_p = d.get('min_price')
                    sum_v = d.get('sum_volume')
                    cnt = d.get('count_trades')
                    # convert start_key ISO to epoch ns for Influx timestamp
                    ts_ns = None
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(start_key)
                        ts_ns = int(dt.timestamp() * 1e9)
                    except Exception:
                        ts_ns = None

                    # build field set, only include numeric fields if not None
                    fields = []
                    if avg_p is not None:
                        fields.append(f"avg_price={float(avg_p)}")
                    if max_p is not None:
                        fields.append(f"max_price={float(max_p)}")
                    if min_p is not None:
                        fields.append(f"min_price={float(min_p)}")
                    if sum_v is not None:
                        fields.append(f"sum_volume={float(sum_v)}")
                    if cnt is not None:
                        # Influx int fields must have trailing i
                        try:
                            ival = int(cnt)
                            fields.append(f"count_trades={ival}i")
                        except Exception:
                            pass

                    if fields:
                        # escape symbol tag
                        sym_tag = str(symbol).replace(' ', '\\ ').replace(',', '\\,')
                        line = f"crypto_agg,symbol={sym_tag} {','.join(fields)}"
                        if ts_ns is not None:
                            line = f"{line} {ts_ns}"
                        influx_lines.append(line)
                except Exception:
                    # don't let influx issues block Mongo upserts
                    pass

                processed += 1
                if last_sample is None:
                    last_sample = {'symbol': d.get('symbol'), 'window_start': start_key}
                if len(ops) >= 500:
                    coll.bulk_write(ops, ordered=False)
                    ops = []
            except Exception as e:
                logging.getLogger('spark_upsert').exception('row processing error: %s', e)

        if ops:
            coll.bulk_write(ops, ordered=False)

        # send metrics to InfluxDB (best-effort)
        if influx_lines:
            try:
                # ensure DB exists (best-effort)
                import urllib.request, urllib.parse
                q = urllib.parse.urlencode({'q': f'CREATE DATABASE {INFLUX_DB}'})
                urllib.request.urlopen(f"{INFLUX_HOST}/query?{q}", timeout=5)
            except Exception:
                pass
            try:
                import urllib.request
                data = "\n".join(influx_lines).encode('utf-8')
                write_url = f"{INFLUX_HOST}/write?db={INFLUX_DB}"
                req = urllib.request.Request(write_url, data=data, method='POST')
                req.add_header('Content-Type', 'text/plain; charset=utf-8')
                urllib.request.urlopen(req, timeout=5)
            except Exception:
                logging.getLogger('spark_upsert').exception('influx write failed')

        print(f"[upsert_to_mongo] batch_id={batch_id} processed={processed} sample={last_sample}")
    except Exception as e:
        logging.getLogger('spark_upsert').exception('upsert error: %s', e)
    finally:
        try:
            if client:
                client.close()
        except:
            pass


def main():
    spark = SparkSession.builder.appName('crypto-streaming-agg') \
        .config('spark.mongodb.output.uri', MONGO_URI) \
        .getOrCreate()

    # normalize starting offsets (allow 'latest' or 'earliest', or full JSON)
    starting_offsets = KAFKA_STARTING_OFFSETS
    try:
        starting_offsets = str(starting_offsets).lower()
        if starting_offsets not in ('latest', 'earliest'):
            # leave as-is (could be JSON string for specific partitions)
            starting_offsets = os.environ.get('KAFKA_STARTING_OFFSETS', 'latest')
    except Exception:
        starting_offsets = 'latest'

    print(f"[spark_streaming] using KAFKA_STARTING_OFFSETS={starting_offsets}")

    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP)
        .option('subscribe', 'crypto-topic')
        .option('startingOffsets', starting_offsets)
        .option('failOnDataLoss','false')
        .load()
    )

    # include the Kafka key (if any) as a fallback for symbol
    parsed = df.select(
        from_json(col('value').cast('string'), map_schema).alias('data'),
        col('key').cast('string').alias('k')
    )

    # event timestamp comes from producer's 'timestamp' field (ISO or epoch),
    # fallback to current timestamp if missing or unparsable.
    event_ts = coalesce(
        # epoch seconds numeric string
        from_unixtime(col('data').getItem('timestamp').cast('long')).cast('timestamp'),
        # ISO timestamp
        to_timestamp(col('data').getItem('timestamp')),
        # fallback to current processing time
        current_timestamp()
    ).alias('event_ts')

    # Extract numeric fields with fallbacks for common casings/variants.
    close_col = coalesce(
        col('data').getItem('close').cast('double'),
        col('data').getItem('Close').cast('double'),
        col('data').getItem('close_price').cast('double'),
        col('data').getItem('ClosePrice').cast('double')
    ).alias('close')

    volume_col = coalesce(
        col('data').getItem('volume').cast('double'),
        col('data').getItem('Volume').cast('double'),
        col('data').getItem('vol').cast('double'),
        col('data').getItem('Volume_(BTC)').cast('double')
    ).alias('volume')

    # Use 'target' as the symbol if 'symbol' is not present (per user request).
    # use data symbol first, then kafka key, then other fallbacks
    symbol_col = coalesce(
        col('data').getItem('symbol'),
        col('k'),
        col('data').getItem('target'),
        col('data').getItem('asset_id'),
        col('data').getItem('Asset_ID')
    ).alias('symbol')

    selected = parsed.select(
        event_ts,
        close_col,
        volume_col,
        symbol_col
    )

    agg = selected.withWatermark('event_ts','2 minutes') \
        .groupBy(window(col('event_ts'),'1 minute'), col('symbol')) \
        .agg(
            avg(col('close')).alias('avg_price'),
            spark_max(col('close')).alias('max_price'),
            spark_min(col('close')).alias('min_price'),
            spark_sum(col('volume')).alias('sum_volume'),
            count('*').alias('count_trades')
        )

    query = agg.writeStream \
        .foreachBatch(lambda df, eid: upsert_to_mongo(df, eid)) \
        .option('checkpointLocation', CHECKPOINT) \
        .outputMode('append') \
        .start()

    query.awaitTermination()

if __name__ == '__main__':
    main()
