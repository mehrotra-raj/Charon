class JobManager {
  constructor(redis, handlers) {
    this.redis = redis;
    this.handlers = handlers;
  }

  normalizeJob(jobId, jobData) {
    return {
      id: jobId,
      ...jobData,
      payload: JSON.parse(jobData.payload || '{}'),
      attempts: parseInt(jobData.attempts || 0, 10),
      maxAttempts: parseInt(jobData.maxAttempts || 3, 10),
      priority: parseInt(jobData.priority || 10, 10),
      createdAt: parseInt(jobData.createdAt || 0, 10),
    };
  }

  async getJob(jobId) {
    const jobData = await this.redis.hgetall(`job:${jobId}`);
    if (!jobData || Object.keys(jobData).length === 0) return null;
    return this.normalizeJob(jobId, jobData);
  }

  async processJob(job) {
    const handler = this.handlers.get(job.type);
    if (!handler) {
      throw new Error(`No handler registered for job type: ${job.type}`);
    }
    await handler(job);
  }

  async markRunning(jobId) {
    await this.redis.hset(
      `job:${jobId}`,
      'status',
      'running',
      'startedAt',
      Date.now(),
    );
  }

  async markCompleted(jobId, queueName) {
    const completedAt = Date.now()
    await this.redis.hset(
      `job:${jobId}`,
      'status',
      'completed',
      'completedAt',
      completedAt,
    );
    if (queueName) {
      await this.redis.zrem(`active:${queueName}`, jobId);
      await this.redis.zadd(`completed:${queueName}`, completedAt, jobId)
    }
  }

  async markPending(jobId, attempts) {
    await this.redis.hset(
      `job:${jobId}`,
      'status',
      'pending',
      'attempts',
      attempts,
    );
  }

  async moveToDeadLetter(job, queueName) {
    await this.redis.lpush(`dead:${queueName}`, JSON.stringify(job));
    await this.redis.hset(
      `job:${job.id}`,
      'status',
      'dead',
      'failedAt',
      Date.now(),
    );
    if (queueName) {
      await this.redis.zrem(`active:${queueName}`, job.id);
    }
  }

  async retryJob(job, queueName, delayMs = 0) {
    await this.markPending(job.id, job.attempts);
    if (delayMs > 0) {
      const runAt = Date.now() + delayMs;
      await this.redis.zadd(`delayed:${queueName}`, runAt, job.id);
    } else {
      await this.redis.zadd(`queue:${queueName}`, job.priority, job.id);
    }
    if (queueName) {
      await this.redis.zrem(`active:${queueName}`, job.id);
    }
  }

}

module.exports = JobManager;
