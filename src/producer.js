const {enqueue} = require("./client")

async function main() {
    await enqueue('email', 'welcome-email', {
        email: "rajmhr078@gmail.com",
        userId: '123'
    })
    await enqueue('email', 'welcome-email', {
        email: "test@example.com",
        userId: '456'
    })
    process.exit(0);
}
main();