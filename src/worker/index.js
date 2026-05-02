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
    this.redis.defineCommand("popAndLock", {
      numberOfKeys: 2,
      lua: `
      local result = redis.call('ZPOPMIN', KEYS[1], 1)
      if #result == 0 then return nil end
      local jobId = result[1]
      local priority = result[2]
      local lockKey = KEYS[2] .. jobId
      local locked = redis.call('SET', lockKey, ARGV[1], 'NX', 'PX', ARGV[2])
      if locked == false then
        redis.call('ZADD', KEYS[1], priority, jobId)
        return nil
      end
      return jobId
    `,
    });
    this.redis.defineCommand("moveDelayedJobs", {
      numberOfKeys: 3,
      lua: `
      local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
      if #jobs > 0 then
        for i, jobId in ipairs(jobs) do
          local priority = redis.call('HGET', KEYS[3] .. jobId, 'priority')
          if not priority then priority = 10 end
          redis.call('ZREM', KEYS[1], jobId)
          redis.call('ZADD', KEYS[2], priority, jobId)
        end
      end
      return #jobs
      `
    });
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

  async pollDelayedJobs(queueName) {
    logger.info({ queue: queueName }, "Delayed jobs poller started");
    while (!this.shouldStop) {
      try {
        const moved = await this.redis.moveDelayedJobs(
          `delayed:${queueName}`,
          `queue:${queueName}`,
          `job:`,
          Date.now()
        );
        if (moved > 0) {
          logger.info({ queue: queueName, count: moved }, "Moved delayed jobs to active queue");
        }
      } catch (err) {
        logger.error({ queue: queueName, err: err.message }, "Error polling delayed jobs");
      }
      await this.sleep(1000);
    }
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
      const jobId = await this.redis.popAndLock(
        `queue:${queueName}`,  // KEYS[1]
        `lock:`,               // KEYS[2]
        this.workerId,         // ARGV[1]
        30000                  // ARGV[2] — lock TTL in ms
      );
      if (!jobId) {
        await this.sleep(500);
        continue;
      }
      const job = await this.jobManager.getJob(jobId);
      if (!job) {
        await this.lockManager.releaseLock(jobId);
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
          logger.info({ jobId, queue: queueName, delay }, "Scheduling delayed retry");
          await this.jobManager.retryJob(job, queueName, delay);
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
    this.pollDelayedJobs(this.queue);
  }
}

module.exports = CharonWorker;
