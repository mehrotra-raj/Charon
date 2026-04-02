const CharonWorker = require('./worker')
const logger = require("./utils/logger")
const worker = new CharonWorker({
  queue: 'email',
  concurrency: 3,
  redisUrl: 'redis://localhost:6379'
})

worker.register('welcome-email', async (job) => {
  logger.info(`Sending welcome email to ${job.payload.email}`)
  await worker.sleep(500)
  logger.info(`Email sent to ${job.payload.email}`)
})

worker.start()