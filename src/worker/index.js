const Redis = require("ioredis");
const { v4: uuidv4 } = require("uuid");
const logger = require("../utils/logger");

class CharonWorker {
  constructor(config) {
    this.redis = new Redis(config.redisUrl);
    this.queue = config.queue;
    this.concurrency = config.concurrency ?? 3;
    this.handlers = new Map();
    this.shouldStop = false;
    this.activeWorkers = 0;
    this.workerId = uuidv4();
    logger.info(
      { workerId: this.workerId, queue: this.queue ?? null },
      "Worker ID",
    );
  }
  register(jobType, handler) {
    this.handlers.set(jobType, handler);
  }
  async acquireLock(jobId) {
    const result = await this.redis.set(
      `lock:${jobId}`,
      this.workerId,
      "NX",
      "EX",
      30,
    );
    return result === "OK";
  }
  async releaseLock(jobId) {
    const currentOwner = await this.redis.get(`lock:${jobId}`);
    if (currentOwner === this.workerId) {
      await this.redis.del(`lock:${jobId}`);
    }
  }
  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
  async getJob(jobId) {
  const jobData = await this.redis.hgetall(`job:${jobId}`)
  if (!jobData || Object.keys(jobData).length === 0) return null
  return {
    ...jobData,
    payload: JSON.parse(jobData.payload || '{}'),
    attempts: parseInt(jobData.attempts || 0),
    maxAttempts: parseInt(jobData.maxAttempts || 3),
    priority: parseInt(jobData.priority || 10),
    createdAt: parseInt(jobData.createdAt || 0),
  }
}

async getQueues() {
  const keys = await this.redis.keys('queue:*')
  const queues = []
  for (const key of keys) {
    const queueName = key.replace('queue:', '')
    const activeJobs = await this.redis.zcard(key)
    const deadJobs = await this.redis.llen(`dead:${queueName}`)
    queues.push({ name: queueName, activeJobs, deadJobs })
  }
  return queues
}
  async processJob(job) {
    const handler = this.handlers.get(job.type);
    if (!handler)
      throw new Error(`No handler registered for job type: ${job.type}`);
    await handler(job);
  }

  async startWorker(queueName) {
    logger.info(
      { queue: queueName },
      "Worker started, listening on queue",
    );
    while (!this.shouldStop) {
      //async loop to make it asynchronouse or else it will keep runnign infintely
      //blocking every other task
      const result = await this.redis.zpopmin(`queue:${queueName}`, 1);

      if (!result || result.length == 0) {
        await this.sleep(500);
        continue;
      }

      const jobId = result[0];
      const jobData = await this.redis.hgetall(`job:${jobId}`);
      if (!jobData) {
        await this.sleep(500);
        continue;
      }
      const job = {
        ...jobData,
        payload: JSON.parse(jobData.payload),
        attempts: parseInt(jobData.attempts),
        maxAttempts: parseInt(jobData.maxAttempts),
        priority: parseInt(jobData.priority),
        createdAt: parseInt(jobData.createdAt),
      };

      const locked = await this.acquireLock(jobId);
      if (!locked) {
        // put it back into the sorted set with its original priority
        const priority = result[1];
        await this.redis.zadd(`queue:${queueName}`, priority, jobId);
        logger.info(
          { jobId, queue: queueName },
          "Job already locked, putting back",
        );
        continue;
      }
      this.activeWorkers++;
      try {
          await this.redis.hset(`job:${jobId}`, "status", "running", "startedAt", Date.now());
        await this.processJob(job);
        logger.info(
          { jobId, queue: queueName, duration_ms: Date.now() - job.createdAt },
          'job completed',
        );
        await this.redis.hset(`job:${jobId}`, "status", "completed", "completedAt", Date.now());
        await this.redis.del(`job:${jobId}`);
        await this.releaseLock(jobId);
      } catch (err) {
        await this.releaseLock(jobId);
        job.attempts += 1;
        logger.error(
          {
            jobId,
            queue: queueName,
            attempt: job.attempts,
            maxAttempts: job.maxAttempts,
          },
          "Job failed",
        );
        if (job.attempts < job.maxAttempts) {
          const delay =
            1000 * Math.pow(2, job.attempts) + Math.floor(Math.random() * 1000); //exponential backoff
          logger.info({ jobId, queue: queueName, delay }, "Retrying job");
          await this.sleep(delay);
          await this.redis.hset(`job:${jobId}`, "status", "pending", "attempts", job.attempts);
          await this.redis.zadd(`queue:${queueName}`, job.priority, jobId);
        } else {
          logger.info(
            { jobId, queue: queueName, attempts: job.attempts },
            "Job exhausted all retries, moving to DLQ",
          );
          //adds to dead-letter queue in redis
          await this.redis.lpush(`dead:${queueName}`, JSON.stringify(job));
          //added it in a different dead email queue;
          await this.redis.hset(`job:${jobId}`, "status", "dead", "failedAt", Date.now());
        }
      }

      this.activeWorkers--;
      if (this.shouldStop && this.activeWorkers === 0) {
        logger.info(
          { queue: this.queue },
          "All jobs finished, exiting now",
        );
        process.exit(0);
      }
    }
  }
  async start() {
    // spawn the worker pool
    logger.info(
      { queue: this.queue, concurrency: this.concurrency },
      "Starting workers",
    );
    process.on("SIGINT", () => {
      logger.info(
        { queue: this.queue },
        "Shutdown signal received",
      );
      this.shouldStop = true;
      if (this.activeWorkers === 0) process.exit(0);
    });

    for (let i = 0; i < this.concurrency; i++) {
      this.startWorker(this.queue);
    }
  }
}

module.exports = CharonWorker;
