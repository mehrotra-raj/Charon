const Redis = require("ioredis");
const { v4: uuidv4 } = require("uuid");
const logger = require("../utils/logger");
const LockManager = require("../core/lock.manager");
const JobManager = require("../core/job.manager");
class CharonWorker {
  constructor(config) {
    this.redis = new Redis(config.redisUrl);
    this.queue = config.queue;
    this.concurrency = config.concurrency ?? 3;
    this.handlers = new Map();
    this.shouldStop = false;
    this.activeWorkers = 0;
    this.workerId = uuidv4();
    this.lockManager = new LockManager(this.redis, this.workerId);
    this.jobManager = new JobManager(this.redis, this.handlers);
    logger.info(
      { workerId: this.workerId, queue: this.queue ?? null },
      "Worker ID",
    );
  }
  register(jobType, handler) {
    this.handlers.set(jobType, handler);
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async getQueues() {
    const keys = await this.redis.keys("queue:*");
    const queues = [];
    for (const key of keys) {
      const queueName = key.replace("queue:", "");
      const activeJobs = await this.redis.zcard(key);
      const deadJobs = await this.redis.llen(`dead:${queueName}`);
      queues.push({ name: queueName, activeJobs, deadJobs });
    }
    return queues;
  }

  async startWorker(queueName) {
    logger.info({ queue: queueName }, "Worker started, listening on queue");
    while (!this.shouldStop) {
      //async loop to make it asynchronouse or else it will keep runnign infintely
      //blocking every other task
      const result = await this.redis.zpopmin(`queue:${queueName}`, 1);

      if (!result || result.length == 0) {
        await this.sleep(500);
        continue;
      }

      const jobId = result[0];
      const job = await this.jobManager.getJob(jobId);
      if (!job) {
        await this.sleep(500);
        continue;
      }

      const locked = await this.lockManager.acquireLock(jobId);
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
        await this.jobManager.markRunning(jobId);
        await this.jobManager.processJob(job);
        logger.info(
          { jobId, queue: queueName, duration_ms: Date.now() - job.createdAt },
          "job completed",
        );
        await this.jobManager.markCompleted(jobId);
        await this.lockManager.releaseLock(jobId);
      } catch (err) {
        await this.lockManager.releaseLock(jobId);
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
          await this.jobManager.retryJob(job, queueName);
        } else {
          logger.info(
            { jobId, queue: queueName, attempts: job.attempts },
            "Job exhausted all retries, moving to DLQ",
          );
          await this.jobManager.moveToDeadLetter(job, queueName);
        }
      }

      this.activeWorkers--;
      if (this.shouldStop && this.activeWorkers === 0) {
        logger.info({ queue: this.queue }, "All jobs finished, exiting now");
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
      logger.info({ queue: this.queue }, "Shutdown signal received");
      this.shouldStop = true;
      if (this.activeWorkers === 0) process.exit(0);
    });

    for (let i = 0; i < this.concurrency; i++) {
      this.startWorker(this.queue);
    }
  }
}

module.exports = CharonWorker;
