const logger = require('../utils/logger')

module.exports = function registerHandlers(worker) {
	worker.register('welcome-email', async (job) => {
		logger.info(`Sending welcome email to ${job.payload.email}`)
		await worker.sleep(500)
		logger.info(`Email sent to ${job.payload.email}`)
	})
}
