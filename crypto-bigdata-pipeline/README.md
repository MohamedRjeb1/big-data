# Pipeline de Données Crypto en Temps Réel

Ce projet met en œuvre une pipeline de données de bout en bout pour traiter des données de cryptomonnaies en temps réel.

**Architecture :**
CSV -> Python Producer -> Kafka -> Spark Structured Streaming -> MongoDB / InfluxDB -> Grafana

## Prérequis

- Docker et Docker Compose installés sur votre machine.

## Guide d'Exécution de A à Z

Suivez ces étapes dans l'ordre pour lancer toute la pipeline.

### Étape 1 : Construire et Démarrer tous les services

Ouvrez un terminal à la racine du projet et exécutez la commande suivante. Cela construira les images Docker (si nécessaire) et démarrera tous les conteneurs (Kafka, Zookeeper, Spark, MongoDB, etc.).

```bash
docker compose up --build -d
```

Attendez environ 1 à 2 minutes que tous les services soient pleinement opérationnels.

### Étape 2 : Lancer le Producteur de Données

Ouvrez un **nouveau terminal** et lancez le script Python qui envoie les données du fichier CSV vers Kafka.

```bash
docker compose exec producer python producer.py
```

Ce terminal affichera les messages envoyés à Kafka. Laissez-le tourner.

### Étape 3 : Lancer le Job Spark Streaming

Ouvrez un **troisième terminal** pour soumettre le job Spark qui lira les données de Kafka, les agrégera et les écrira dans MongoDB.

```bash
docker compose exec spark spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  /home/jovyan/work/spark_streaming.py
```

Ce terminal affichera les logs de Spark. Laissez-le également tourner.

### Étape 4 : Surveillance et Visualisation

Votre pipeline est maintenant active ! Voici comment observer les résultats :

1.  **Consulter les données dans MongoDB :**
    Pour voir les dernières données agrégées écrites dans MongoDB, exécutez cette commande dans un autre terminal :
    ```bash
    docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('crypto').realtime.find().sort({_id:-1}).limit(5).toArray()"
    ```

2.  **Visualiser avec Grafana :**
    -   Ouvrez votre navigateur et allez sur [http://localhost:3000](http://localhost:3000).
    -   **Login :** `admin`
    -   **Password :** `admin`
    -   Vous pouvez ensuite configurer une source de données MongoDB ou InfluxDB pour créer des tableaux de bord.

### Étape 5 : Arrêter la Pipeline

Une fois que vous avez terminé, vous pouvez arrêter tous les services proprement. Retournez au premier terminal (ou ouvrez-en un nouveau) et exécutez :

```bash
docker compose down
```

Cela arrêtera et supprimera tous les conteneurs liés au projet.
