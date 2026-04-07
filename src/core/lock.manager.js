class LockManager {
  constructor(redis, workerId) {
    this.redis = redis;
    this.workerId = workerId;
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
}

module.exports = LockManager;