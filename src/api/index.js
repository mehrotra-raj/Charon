const express = require('express');
const CharonClient = require("../client/index.js")
const logger = require('../utils/logger');

const app = express();
app.use(express.json());

// Simple API key authentication middleware
const API_KEY = process.env.CHARON_API_KEY || 'default-key'; 
app.use((req, res, next) => {
  const key = req.headers['x-api-key'];
  if (!key || key !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized: Invalid API key' });
  }
  next();
});

// Request logging middleware
app.use((req, res, next) => {
  logger.info({ method: req.method, url: req.url, ip: req.ip });
  next();
});

const client = new CharonClient({ redisUrl: process.env.REDIS_URL || 'redis://localhost:6379' });

// Validation helper (basic, consider joi for production)
function validateEnqueue(req) {
  const { queue, type, payload } = req.body;
  if (!queue || typeof queue !== 'string') throw new Error('Invalid queue: must be a string');
  if (!type || typeof type !== 'string') throw new Error('Invalid type: must be a string');
  if (!payload || typeof payload !== 'object') throw new Error('Invalid payload: must be an object');
}

// Enqueue job
app.post('/enqueue', async (req, res) => {
  try {
    validateEnqueue(req);
    const { queue, type, payload, options = {} } = req.body;
    const job = await client.enqueue(queue, type, payload, options);
    res.status(201).json({ jobId: job.id, status: 'enqueued' });
  } catch (err) {
    logger.error({ error: err.message }, 'Enqueue failed');
    res.status(400).json({ error: err.message });
  }
});

// Get job status
app.get('/jobs/:id', async (req, res) => {
  try {
    const jobId = req.params.id;
    const jobData = await client.redis.hgetall(`job:${jobId}`);
    if (!jobData) {
      return res.status(404).json({ error: 'Job not found' });
    }
    const job = {
      ...jobData,
      payload: JSON.parse(jobData.payload || '{}'),
      attempts: parseInt(jobData.attempts || 0),
      maxAttempts: parseInt(jobData.maxAttempts || 3),
      priority: parseInt(jobData.priority || 10),
      createdAt: parseInt(jobData.createdAt || 0),
    };
    res.json(job);
  } catch (err) {
    logger.error(err);
    res.status(500).json({ error: 'Failed to fetch job' });
  }
});

// List queues with stats
app.get('/queues', async (req, res) => {
  try {
    const keys = await client.redis.keys('queue:*');
    const queues = [];
    for (const key of keys) {
      const queueName = key.replace('queue:', '');
      const length = await client.redis.zcard(key);
      const deadLength = await client.redis.llen(`dead:${queueName}`);
      queues.push({ name: queueName, activeJobs: length, deadJobs: deadLength });
    }
    res.json({ queues });
  } catch (err) {
    logger.error(err);
    res.status(500).json({ error: 'Failed to fetch queues' });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: Date.now() });
});

app.listen(3000, () => logger.info('API server running on port 3000'));