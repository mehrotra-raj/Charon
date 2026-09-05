const logger = require('../utils/logger')

module.exports = function registerHandlers(worker) {
	worker.register('process-payment', async (job) => {
		logger.info(`Processing payment for userId ${job.payload.userId}`)
		await worker.sleep(500)
		logger.info(`Payment processed for userId ${job.payload.userId}`)
	})
}
