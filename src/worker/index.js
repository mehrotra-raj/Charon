const redis = require("../utils/redis")
let isProcessing = false;
let shouldStop = false;
async function processJob(job) {
    console.log(`Processing job; ${job.id} | ${job.type}`);
    console.log(`Payload :`,JSON.stringify(job.payload));
    await new Promise(resolve => setTimeout(resolve, 1000) );
    console.log(`Job completed ${job.id}`);

}
async function startWorker(queueName) {
    console.log(`Worker started, listening on queue:${queueName}`);
    while(!shouldStop) {
        //async loop to make it asynchronouse or else it will keep runnign infintely
        //blocking every other task
        const result = await redis.brpop(`queue:${queueName}`, 0)
        if(!result) {
            continue;
        }
        isProcessing = true
        const job = JSON.parse(result[1]);
        await processJob(job)
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