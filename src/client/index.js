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
    await this.redis.sadd(`queues`, queueName)
    logger.info(
      { jobId: job.id, queue: queueName, type, priority: job.priority },
      "Job enqueued",
    );
    return job;
  }
  async getJob(jobId) {
    return await this.redis.hgetall(`job:${jobId}`);
  }
  async getQueues() {
    const names = await this.redis.smembers('queues');
    const results = await Promise.all(names.map(async (name) => ({
      name,
      waiting: await this.redis.zcard(`queue:${name}`),
      active: await this.redis.zcard(`active:${name}`),
      completed: await this.redis.zcard(`completed:${name}`),
      failed: await this.redis.llen(`dead:${name}`),
    })));
    return results;
  }
}

module.exports = CharonClient;
