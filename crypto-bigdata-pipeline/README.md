# crypto-bigdata-pipeline — Guide d'installation et de vérification

Ce dépôt contient une démo end-to-end d'une pipeline Big Data pour données crypto :
- Producteur Python qui lit `./data/crypto.csv` et publie ligne-par-ligne sur Kafka
- Kafka (Confluent) pour la simulation de streaming
- Spark Structured Streaming (job PySpark prêt) pour agréger et écrire dans MongoDB
- Hadoop Streaming (mapper/reducer Python) pour calcul batch sur HDFS
- MongoDB comme couche de stockage/serving
- Backend Node.js (Express) + frontend React minimal
- Orchestration via Docker Compose

Ce README explique comment démarrer, vérifier l'état et dépanner pas à pas (PowerShell sous Windows).

## Prérequis
- Windows avec Docker Desktop (WSL2 recommandé)
- PowerShell (vous utilisez `powershell.exe`, OK)
- Python 3.10+ (pour le producer) et venv

Fichiers clés :
- `docker/docker-compose.yml` — orchestration
- `producer/producer.py` — producteur Kafka (utiliser venv)
- `spark/spark_streaming.py` — job PySpark (spark-submit)
- `hadoop/mapper.py`, `hadoop/reducer.py` — Hadoop streaming
- `mongo/init-mongo.js` — script d'initialisation Mongo (création collections/indexes)
- `backend/` et `frontend/` — API et UI

## Étapes d'installation (rapide)
1. Récupérer le repo et se placer dans le dossier :

```powershell
cd C:\Users\moham\big_data\crypto-bigdata-pipeline
```

2. Démarrer la stack Docker (Zookeeper, Kafka, HDFS, Mongo, backend, frontend)

```powershell
docker-compose -f docker\docker-compose.yml up -d
```

3. Vérifier que les conteneurs sont up

```powershell
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
```

Ports exposés importants :
- Kafka: 9092 (localhost)
- MongoDB: 27017
- Backend API: 3000
- Frontend: 3001

## Charger le CSV dans HDFS
Le CSV d'exemple est `data/crypto.csv`. Pour le copier dans HDFS (namenode container) :

```powershell
# copier dans le conteneur NameNode (remplacez <namenode> si nécessaire)
docker cp .\data\crypto.csv docker-hadoop-namenode-1:/tmp/crypto.csv

# utiliser la binaire hdfs depuis le namenode
docker exec -it docker-hadoop-namenode-1 bash -lc \
  "/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /crypto/raw && /opt/hadoop-3.2.1/bin/hdfs dfs -put -f /tmp/crypto.csv /crypto/raw/"
```

Vérifier :

```powershell
docker exec -it docker-hadoop-namenode-1 bash -lc "/opt/hadoop-3.2.1/bin/hdfs dfs -ls /crypto/raw"
```

## Démarrer le producer (sur l'hôte, venv conseillé)

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r producer\requirements.txt
python producer\producer.py --rate 1 --loop --bootstrap-server localhost:9092
```

Le producer lit le CSV et boucle (option `--loop`). Les logs montrent les connexions et envois.

## Vérifier Kafka / consumer
Ouvrir un autre terminal PowerShell et lancer un consumer dans le conteneur Kafka (attention au `--` en PowerShell).

```powershell
docker exec -it docker-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-topic --from-beginning --timeout-ms 10000
```

Vérifier la liste des topics :

```powershell
docker exec -it docker-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

Vérifier offsets (latest) :

```powershell
docker exec -it docker-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic crypto-topic --time -1
```

## MongoDB — état et initialisation
Le compose contient un healthcheck qui utilise `mongosh`. Si Mongo est `unhealthy` :

1. Voir les logs :

```powershell
docker logs docker-mongo-1 --tail 200
```

2. Vérifier le résultat du healthcheck (et le log du health) :

```powershell
docker inspect --format='{{json .State.Health}}' docker-mongo-1
docker inspect --format='{{json .State.Health.Log}}' docker-mongo-1
```

3. Exécuter le ping de test depuis l'intérieur du conteneur (doit renvoyer `{ ok: 1 }`) :

```powershell
docker exec -it docker-mongo-1 bash -lc "mongosh --quiet --eval 'db.adminCommand({ping:1})'"
```

4. Si l'init n'a pas été appliquée, exécutez le script idempotent :

```powershell
docker cp .\mongo\init-mongo.js docker-mongo-1:/tmp/init-mongo.js
docker exec -it docker-mongo-1 bash -lc "mongosh /tmp/init-mongo.js"
```

5. Vérifier les collections (commande PowerShell safe) :

```powershell
docker exec -it docker-mongo-1 bash -lc "mongosh --quiet --eval \"use crypto; print('realtime:', db.realtime.count()); print('daily:', db.daily_stats.count());\""
```

Note : l'image `mongo:6.0` n'inclut pas l'ancien `mongo` shell ; le compose utilise `mongosh`.

## Vérifier backend & frontend
- Backend (Express) disponible sur http://localhost:3000
- Frontend disponible sur http://localhost:3001

Tester les endpoints :

```powershell
Invoke-RestMethod http://localhost:3000/api/realtime | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:3000/api/daily | ConvertTo-Json -Depth 5
```

Si ces endpoints renvoient des tableaux vides ou counts = 0, c'est normal tant que Spark n'a pas ingéré de messages.

## Lancer le job Spark (streaming)
Le service Spark est commenté par défaut dans `docker-compose.yml` (compatibilité d'image). Pour exécuter le job :

1) Option A — utiliser un Spark local/remote : installez PySpark et exécutez :

```powershell
# Exemple local (nécessite PySpark et pymongo installés)
python -m pip install pyspark pymongo
python spark\spark_streaming.py --bootstrap-server localhost:9092 --mongo-uri mongodb://mongo:27017
```

2) Option B — activer Spark dans docker-compose puis soumettre :
 - Décommenter/ajouter les services `spark-master` et `spark-worker` dans `docker-compose.yml` (utiliser images compatibles), puis :

```powershell
docker exec -it <spark-master-container> bash -lc \
  "/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/spark/spark_streaming.py --bootstrap-server kafka:9092 --mongo-uri mongodb://mongo:27017"
```

Contactez-moi si vous voulez que j'ajoute les services Spark dans `docker-compose.yml` et fournis les commandes exactes.

## Executer le MapReduce Hadoop (batch)
Le script `scripts/04_run_mapreduce.sh` lance un job Hadoop Streaming qui lit `/crypto/raw/crypto.csv` et produit un CSV de stats. Il importe ensuite le résultat dans Mongo via `mongoimport`.

Exécuter le script depuis l'hôte (bash) :

```powershell
bash scripts/04_run_mapreduce.sh
```

## Tests rapides & dépannage
- Insérer un document manuel pour tester backend/frontend :

```powershell
docker exec -it docker-mongo-1 bash -lc "mongosh --quiet --eval \"use crypto; db.realtime.insertOne({timestamp: new Date(), symbol:'BTCUSDT', close:60000, volume:1});\""
```

- Recréer Mongo proprement (supprime les données si vous supprimez le volume) :

```powershell
docker-compose -f docker\docker-compose.yml down
docker rm -f docker-mongo-1
docker volume ls
docker volume rm <mongo_volume_name>
docker-compose -f docker\docker-compose.yml up -d mongo
```

## Checklist de validation (à cocher)
- [ ] `docker-compose up -d` démarre tous les services
- [ ] `producer.py` se connecte à `localhost:9092` et envoie des messages
- [ ] `kafka-console-consumer` montre les messages (topic `crypto-topic`)
- [ ] `docker inspect ... docker-mongo-1` montre `Status: healthy`
- [ ] `mongosh /tmp/init-mongo.js` crée `crypto.realtime` et `crypto.daily_stats`
- [ ] Backend `GET /api/realtime` et `GET /api/daily` répondent
- [ ] Spark job écrit dans `crypto.realtime` (après lancement)

## Support & contributions
Ouvrez une issue ou partagez ici les logs/outputs si vous voulez que j'aide à diagnostiquer. Si vous modifiez `docker-compose.yml`, testez chaque changement isolément.

---
Si vous voulez, je peux :
- ajouter et tester les services Spark dans `docker-compose.yml`, ou
- fournir un script PowerShell automatisé pour toutes les vérifications.

Merci.# crypto-bigdata-pipeline

This repository provides a full end-to-end demo Big Data pipeline that:
- Ingests a static CSV dataset (./data/crypto.csv)
- Simulates streaming via a Kafka producer (producer/producer.py)
- Processes streaming data with Spark Structured Streaming (spark/spark_streaming.py)
- Runs a daily batch MapReduce using Hadoop Streaming (hadoop/mapper.py, reducer.py)
- Persists realtime results and daily aggregates in MongoDB
- Exposes a simple Node.js API (backend/) and a minimal React dashboard (frontend/)
- Orchestrated with Docker Compose (docker/docker-compose.yml)

Prerequisites
- Docker & Docker Compose installed
- Recommended: >=8GB RAM available to Docker

Quick start
1. Start core services:

   Open a shell in the project root and run:

   docker-compose -f docker/docker-compose.yml up -d

2. Put the sample CSV into HDFS (script waits for HDFS):

   ./scripts/01_put_hdfs.sh

3. Start the Spark streaming job:

   ./scripts/03_submit_spark.sh

4. Start the producer (locally):

   ./scripts/02_start_producer.sh 1 --loop

5. Run the Hadoop MapReduce batch (after data landed in HDFS):

   ./scripts/04_run_mapreduce.sh

6. Start backend and frontend (if not running via Docker Compose):

   npm --prefix backend install && npm --prefix backend start
   npm --prefix frontend install && npm --prefix frontend start

Verification and debug commands
- Kafka console consumer (inside kafka container):
  docker exec -it $(docker ps --filter "ancestor=bitnami/kafka" -q | head -n1) /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto-topic --from-beginning

- Check HDFS files:
  docker exec -it $(docker ps --filter "ancestor=bde2020/hadoop-namenode" -q | head -n1) bash -lc "hdfs dfs -ls /crypto/raw"

- Mongo queries (mongo shell):
  mongo --host localhost --eval "db.getSiblingDB('crypto').realtime.find().limit(5).pretty()"

- Backend health:
  curl http://localhost:3000/health

Notes & assumptions
- The dataset expected schema: timestamp,open,high,low,close,volume,symbol (timestamp format YYYY-MM-DD HH:MM:SS)
- Environment variables used: KAFKA_BOOTSTRAP, MONGO_URI, CHECKPOINT_DIR, SPARK_MASTER
- All scripts attempt simple service checks before proceeding; adjust container names if you customized images.

Files of interest
- docker/docker-compose.yml - orchestration
- producer/producer.py - Kafka producer
- spark/spark_streaming.py - Structured Streaming job
- hadoop/mapper.py & reducer.py - Hadoop streaming MapReduce
- backend/index.js - Node API
- frontend/public - minimal React UI (served unbundled)

If anything fails, check container logs (docker-compose logs -f) and share the output.
