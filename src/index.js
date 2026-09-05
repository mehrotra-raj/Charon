require('dotenv').config();
const CharonWorker = require('./worker')
/*
This page starts the worker, its starts a generic worker
earlier, we had started 2 workers for email and three workers for 
payment
But to scale each of them independently and thus make it truly distrubuted
I moved the handlers to src/handlers and created a generic process here
This worker will start based for whichever jobs its provided!
*/

const queue = process.env.QUEUE || 'email';

const queueConfig = {
    email : {
        concurrency : 3,
        handlerFile: './handlers/email',
    },
    payments : {
        concurrency : 2,
        handlerFile: './handlers/payment',
    }
}[queue];


if (!queueConfig) {
  throw new Error(
    `Unsupported QUEUE: ${queue}. Use "email" or "payments".`
  );
}


const worker = new CharonWorker({
    queue,
    concurrency : Number(
        process.env.CONCURRENCY || queueConfig.concurrency
    ),
    redisUrl : process.env.REDIS_URL || 'redis://localhost:6379',
});

const registerHandlers = require(queueConfig.handlerFile)
registerHandlers(worker)

worker.start();