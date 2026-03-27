const redis = require('../utils/redis');
const { v4: uuidv4 } = require('uuid')

async function enqueue(queueName, type, payload) {
    const job = {
        id : uuidv4(),
        type,
        payload,
        status: "pending",
        attempts: 0,
        maxAttempts: 3,
        createdAt: Date.now()
    }
    await redis.lpush(`queue:${queueName}`, JSON.stringify(job));
    console.log(`Job enqueued: ${job.id}`)
    return job;
}

module.exports = {enqueue}