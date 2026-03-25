const redis = require("../utils/redis")

async function processJob(job) {
    console.log(`Processing job; ${job.id} | ${job.type}`);
    console.log(`Payload :`,JSON.stringify(job.payload));
    
    await new Promise(resolve => setTimeout(resolve, 1000) );
    console.log(`Job completed ${job.id}`);

}
async function startWorker(queueName) {
    console.log(`Worker started, listening on queue:${queueName}`);
    while(true) {
        //async loop to make it asynchronouse or else it will keep runnign infintely
        //blocking every other task
        const result = await redis.brpop(`queue:${queueName}`, 0)
        const job = JSON.parse(result[1]);
        await processJob(job)
    }
}

startWorker('email');