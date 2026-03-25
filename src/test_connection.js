const redis = require("./utils/redis")

async function tester() {
    await redis.set('test', 'charon is alive');
    const val = await redis.get('test');
    console.log("Fetched from redis ->", val);
    process.exit(0);
}
tester();