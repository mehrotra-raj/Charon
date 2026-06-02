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
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    await this.redis.eval(script, 1, `lock:${jobId}`, this.workerId);
  }
}

module.exports = LockManager;