const Redis = require("ioredis");
const { v4: uuidv4 } = require("uuid");
const logger = require("../utils/logger");

class CharonClient {
  constructor(config) {
    this.redis = new Redis(config.redisUrl);
  }
  async enqueue(queueName, type, payload, options = {}) {
    const job = {
      id: uuidv4(),
      type,
      payload: JSON.stringify(payload),
      status: "pending",
      attempts: 0,
      maxAttempts: options.maxAttempts ?? 3,
      priority: options.priority ?? 10,
      createdAt: Date.now(),
    };
    await this.redis.hset(`job:${job.id}`, job);
    await this.redis.zadd(`queue:${queueName}`, job.priority, job.id);
    logger.info(
      { jobId: job.id, queue: queueName, type, priority: job.priority },
      "Job enqueued",
    );
    return job;
  }
}

module.exports = CharonClient;
