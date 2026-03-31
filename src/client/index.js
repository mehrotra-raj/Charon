const redis = require('../utils/redis');
const { v4: uuidv4 } = require('uuid')

async function enqueue(queueName, type, payload, priority = 10) {
    const job = {
        id : uuidv4(),
        type,
        payload:JSON.stringify(payload),
        status: "pending",
        attempts: 0,
        maxAttempts: 3,
        priority: priority,
        createdAt: Date.now()
    }   
    await redis.hset(`job:${job.id}`, job);
    await redis.zadd(`queue:${queueName}`, priority, job.id);
    console.log(`Job enqueued: ${job.id} | priority: ${priority}`);
    return job;
}

module.exports = {enqueue}