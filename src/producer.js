const {enqueue} = require("./client")

async function main() {
    await enqueue('email', 'welcome-email', { email: 'raj@example.com', userId: '123' }, 1)
    await enqueue('email', 'welcome-email', { email: 'test@example.com', userId: '456' }, 10)
    await enqueue('email', 'welcome-email', { email: 'vip@example.com', userId: '789' }, 1)
    process.exit(0);
}
main();