const redis = require("../utils/redis")
const { v4: uuidv4 } = require('uuid')
const workerId = uuidv4();
console.log(`Worker ID: ${workerId}`);

async function acquireLock(jobId) {
        const result = await redis.set(
            `lock:${jobId}`,
            workerId,
            'NX',
            'EX',
            30
        )
        return result === 'OK'
}
async function releaseLock(jobId) {
    const currentOwner = await redis.get(`lock:${jobId}`);
    if (currentOwner === workerId) {
        await redis.del(`lock:${jobId}`);
    }
}   

let isProcessing = false;
let shouldStop = false;
function sleep (ms) {
        return  new Promise(resolve => setTimeout(resolve, ms) );
}
async function processJob(job) {
    console.log(`Processing job; ${job.id} | ${job.type}`);
    console.log(`Payload :`,JSON.stringify(job.payload));
    // simulate random failure
    if (Math.random() < 0.5) {
    throw new Error('Something went wrong processing the job')
    }
    
    await sleep(1000);
    console.log(`Job completed ${job.id}`);

}
async function startWorker(queueName) {
    console.log(`Worker started, listening on queue:${queueName}`);
    while(!shouldStop) {
        //async loop to make it asynchronouse or else it will keep runnign infintely
        //blocking every other task
        const result = await redis.zpopmin(`queue:${queueName}`, 1);

        if(!result || result.length == 0) {
            await sleep(500);
            continue;
        }
        
        const jobId = result[0];
        const jobData = await redis.hgetall(`job:${jobId}`);
        if(!jobData) {
            await sleep(500);
            continue;
        }
        const job = {
            ...jobData,
            payload: JSON.parse(jobData.payload),
            attempts: parseInt(jobData.attempts),
            maxAttempts: parseInt(jobData.maxAttempts),
            createdAt: parseInt(jobData.createdAt)
        }
        
        const locked = await acquireLock(jobId)
        if (!locked) {
            // put it back into the sorted set with its original priority
            const priority = result[1]
            await redis.zadd(`queue:${queueName}`, priority, jobId)
            console.log(`Job ${jobId} already locked, putting back`)
            continue
        }
        isProcessing = true;
        try {
            await processJob(job);
            await redis.del(`job:${jobId}`)
            await releaseLock(jobId)
        } catch (err) {
             await releaseLock(jobId)
            job.attempts += 1
            console.error(`Job failed: ${job.id} | attempt ${job.attempts}/${job.maxAttempts}`)
            if(job.attempts < job.maxAttempts) {
                const delay = 1000 * Math.pow(2, job.attempts) + Math.floor(Math.random() * 1000); //exponential backoff
                console.log(`Retrying job: ${job.id} in ${delay} ms`);
                await sleep(delay)
                await redis.hset(`job:${jobId}`, 'attempts', job.attempts);
                await redis.zadd(`queue:${queueName}`, job.attempts, jobId);
            } else {
                console.log(`Job exhausted all retries, moving to DLQ: ${job.id}`)
                //adds to dead-letter queue in redis
               await redis.lpush(`dead:${queueName}`, JSON.stringify(job))
                //added it in a different dead email queue; 
                await redis.hset(`job:${jobId}`, 'status', 'dead');
            }
        }
        
        isProcessing = false;
    }

console.log('Worker stopped cleanly') 
process.exit(0);
}
process.on("SIGINT", () => {
    console.log('Shutdown signal received, finishing current job...');
    shouldStop = true;
    if (!isProcessing) {
        console.log('No job in progress, exiting now')
        process.exit(0)
    }
})

startWorker('email');