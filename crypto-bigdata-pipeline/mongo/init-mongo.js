// init-mongo.js
// Creates database, collections and indexes for the pipeline. Run with mongo shell or mount into
// the mongo container's /docker-entrypoint-initdb.d/ if desired.

db = db.getSiblingDB('crypto');
db.createCollection('realtime');
db.createCollection('daily_stats');
db.realtime.createIndex({symbol:1, window_start:1});
db.daily_stats.createIndex({symbol:1, date:1});

print('Mongo initializationdocker logs docker-mongo-1 --tail 200 script executed');
