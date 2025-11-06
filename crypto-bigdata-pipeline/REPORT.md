# REPORT: crypto-bigdata-pipeline

Architecture overview
- Lambda-style hybrid pipeline: streaming ingestion via Kafka + Spark Structured Streaming for near-real-time processing, and daily batch via Hadoop Streaming (MapReduce) for historical aggregations. MongoDB is the serving layer for both realtime and batch outputs. A lightweight Node.js API provides access for the React dashboard.

Design choices
- Kafka for ingestion: standard, durable streaming backbone. Chosen kafka-python for simplicity on the producer side.
- Spark Structured Streaming: windowed aggregations with watermarking provide robust event-time semantics.
- Hadoop Streaming (Python mapper/reducer): simple, portable batch job to compute daily per-symbol metrics without Java.
- MongoDB: document store for fast reads by the dashboard and easy upserts.

Metrics & measurement (suggested)
- Ingestion latency: measure time from producer send to document available in Mongo — record timestamps in messages.
- Throughput: messages/sec produced and processed by Spark. Tune by increasing partitions and Spark executors.

Recommendations & tuning
- Kafka: add partitions per-topic based on expected concurrency; set replication.factor >=2 for production.
- Spark: tune spark.sql.shuffle.partitions (default 200) to match data volume; allocate executors and memory via --conf or yarn settings.
- HDFS/Hadoop: ensure replication factor and block size match dataset size; use compaction for small files.

Improvements & production concerns
- Exactly-once semantics: use Kafka transactions + idempotent sinks or Delta Lake for transactional writes.
- Security: enable TLS for Kafka and MongoDB, and authentication for all services. Do not store secrets in code; use a secret manager.
- Observability: add metrics (Prometheus), logging pipeline, and alerting.
