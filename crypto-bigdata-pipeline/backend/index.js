// index.js
// Minimal Express backend exposing endpoints to read realtime and daily aggregated data from MongoDB.
// Configuration via environment variables: MONGO_URI, PORT

const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');
const morgan = require('morgan');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://mongo:27017';
const PORT = process.env.PORT || 3000;

const app = express();
app.use(cors());
app.use(morgan('dev'));

let dbClient;
let db;

async function initDb() {
  dbClient = new MongoClient(MONGO_URI);
  await dbClient.connect();
  db = dbClient.db('crypto');
}

app.get('/health', (req, res) => res.json({status: 'ok'}));

// GET /api/realtime/:symbol?limit=10
app.get('/api/realtime/:symbol?', async (req, res) => {
  try {
    const symbol = req.params.symbol;
    const limit = parseInt(req.query.limit || '10', 10);
    const query = symbol ? {symbol: symbol} : {};
    const docs = await db.collection('realtime').find(query).sort({window_start:-1}).limit(limit).toArray();
    res.json(docs);
  } catch (err) {
    console.error(err);
    res.status(500).json({error: 'internal'})
  }
});

// GET /api/daily/:symbol?
app.get('/api/daily/:symbol?', async (req, res) => {
  try {
    const symbol = req.params.symbol;
    const query = symbol ? {symbol: symbol} : {};
    const docs = await db.collection('daily_stats').find(query).sort({date:-1}).limit(100).toArray();
    res.json(docs);
  } catch (err) {
    console.error(err);
    res.status(500).json({error: 'internal'})
  }
});

app.listen(PORT, async () => {
  try {
    await initDb();
    console.log(`Backend listening on port ${PORT}`);
  } catch (err) {
    console.error('Failed to connect to Mongo', err);
    process.exit(1);
  }
});
