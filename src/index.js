const CharonWorker = require('./worker')

const worker = new CharonWorker({
  queue: 'email',
  concurrency: 3,
  redisUrl: 'redis://localhost:6379'
})

worker.register('welcome-email', async (job) => {
  console.log(`Sending welcome email to ${job.payload.email}`)
  await worker.sleep(500)
  console.log(`Email sent to ${job.payload.email}`)
})

worker.start()