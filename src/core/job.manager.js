class JobManager {
  constructor(redis, handlers) {
    this.redis = redis;
    this.handlers = handlers;
  }

  normalizeJob(jobId, jobData) {
    let payload = {};
    try {
      payload = JSON.parse(jobData.payload || '{}');
    } catch {
      payload = { _corruptedPayload: jobData.payload };
    }

    return {
      id: jobId,
      ...jobData,
      payload,
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
      Date.now()
    );
  }

  async markCompleted(jobId, queueName) {
    const completedAt = Date.now();
    const pipeline = this.redis.pipeline();

    pipeline.hset(
      `job:${jobId}`,
      'status',
      'completed',
      'completedAt',
      completedAt
    );

    if (queueName) {
      pipeline.zrem(`active:${queueName}`, jobId);
      pipeline.zadd(`completed:${queueName}`, completedAt, jobId);
    }

    await pipeline.exec();
  }

  async markPending(jobId, attempts) {
    await this.redis.hset(
      `job:${jobId}`,
      'status',
      'pending',
      'attempts',
      attempts
    );
  }

  async moveToDeadLetter(job, queueName) {
    const pipeline = this.redis.pipeline();

    pipeline.hset(
      `job:${job.id}`,
      'status',
      'dead',
      'failedAt',
      Date.now()
    );

    if (queueName) {
      pipeline.lpush(`dead:${queueName}`, JSON.stringify(job));
      pipeline.zrem(`active:${queueName}`, job.id);
    }

    await pipeline.exec();
  }

  async retryJob(job, queueName, delayMs = 0) {
    const pipeline = this.redis.pipeline();

    pipeline.hset(
      `job:${job.id}`,
      'status',
      'pending',
      'attempts',
      job.attempts
    );

    if (queueName) {
      if (delayMs > 0) {
        const runAt = Date.now() + delayMs;
        pipeline.zadd(`delayed:${queueName}`, runAt, job.id);
      } else {
        pipeline.zadd(`queue:${queueName}`, job.priority, job.id);
      }
      pipeline.zrem(`active:${queueName}`, job.id);
    }

    await pipeline.exec();
  }
}

module.exports = JobManager; 