require("dotenv").config();
const CharonClient = require('./client')

const client = new CharonClient({ 
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379'
})

async function main() {
  await client.enqueue('email', 'welcome-email', { email: 'raj@example.com', userId: '123' }, { priority: 1 })
  await client.enqueue('email', 'welcome-email', { email: 'test@example.com', userId: '456' }, { priority: 10 })
  await client.enqueue('email', 'welcome-email', { email: 'vip@example.com', userId: '789' }, { priority: 1 })
  await client.enqueue(
    'payments',
    'process-payment',
    { userId: '456', amount: 2500, currency: 'USD' },
    { priority: 8 }
  );

  await client.enqueue(
    'payments',
    'process-payment',
    { userId: '789', amount: 500, currency: 'EUR' },
    { priority: 3 }
  );
  process.exit(0)
}

main()