const Redis = require("ioredis");

const redis = new Redis({
  host: process.env.REDIS_URL || "redis://127.0.0.1:6379"
});

redis.on("connect", () => console.log("Redis connected"));
redis.on("error", () => console.log("Redis error"));

module.exports = redis;
