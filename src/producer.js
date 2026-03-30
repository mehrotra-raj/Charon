const {enqueue} = require("./client")

async function main() {
    await enqueue('email', 'welcome-email', { email: 'raj@example.com', userId: '123' }, 1)
    await enqueue('email', 'welcome-email', { email: 'test@example.com', userId: '456' }, 10)
    await enqueue('email', 'welcome-email', { email: 'vip@example.com', userId: '789' }, 1)
    await enqueue('email', 'welcome-email', { email: 'alpha@example.com', userId: '101' }, 5)
    await enqueue('email', 'welcome-email', { email: 'beta@example.com', userId: '102' }, 2)
    await enqueue('email', 'welcome-email', { email: 'gamma@example.com', userId: '103' }, 8)
    await enqueue('email', 'welcome-email', { email: 'delta@example.com', userId: '104' }, 3)
    await enqueue('email', 'welcome-email', { email: 'epsilon@example.com', userId: '105' }, 7)
    await enqueue('email', 'welcome-email', { email: 'zeta@example.com', userId: '106' }, 1)
    process.exit(0);
}
main();