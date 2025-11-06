# Validation checklist

1) Start services
- docker-compose -f docker/docker-compose.yml up -d
- Verify: ZK (2181), Kafka (9092), HDFS UI (9870), Spark UI (8080), Mongo (27017), backend (3000), frontend (3001)

2) Put CSV in HDFS
- ./scripts/01_put_hdfs.sh
- Verify with: docker exec -it <namenode> hdfs dfs -ls /crypto/raw

3) Start Spark streaming job
- ./scripts/03_submit_spark.sh
- Check Spark UI and ensure job is running

4) Start producer
- ./scripts/02_start_producer.sh 1 --loop
- Use kafka-console-consumer to confirm messages

5) Query backend
- curl http://localhost:3000/health
- curl http://localhost:3000/api/realtime

6) Run MapReduce batch
- ./scripts/04_run_mapreduce.sh
- Confirm resulting CSV and that it imports to mongo: db.daily_stats.find().limit(5)

7) Open frontend
- Open http://localhost:3001 in browser (or adjust ports)
