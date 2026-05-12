const CharonWorker = require('./worker')
const logger = require("./utils/logger")

const emailWorker = new CharonWorker({
  queue: 'email',
  concurrency: 3,
  redisUrl: 'redis://localhost:6379'
})

emailWorker.register('welcome-email', async (job) => {
  logger.info(`Sending welcome email to ${job.payload.email}`)
  await emailWorker.sleep(500)
  logger.info(`Email sent to ${job.payload.email}`)
})

const paymentsWorker = new CharonWorker({
  queue: 'payments',
  concurrency: 2,
  redisUrl: 'redis://localhost:6379'
})

paymentsWorker.register('process-payment', async (job) => {
  logger.info(`Processing payment for userId ${job.payload.userId}`)
  await paymentsWorker.sleep(500)
  logger.info(`Payment processed for userId ${job.payload.userId}`)
})

emailWorker.start()
paymentsWorker.start()