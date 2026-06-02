// src/shutdown.js
class shutdownCoordinator {
    constructor(gracePeriodMs = 30000) { // 30 seconds default
        this.workers = new Set();
        this.gracePeriodMs = gracePeriodMs;
        this._registerSignalHandler();
    }

    register(worker) {
        this.workers.add(worker);
    }

    _registerSignalHandler() {
        process.once('SIGINT', async () => {
            console.log(`Shutdown signal received. Waiting up to ${this.gracePeriodMs / 1000}s for workers to drain...`);

            // Start a forceful exit timer
            const forceExitTimer = setTimeout(() => {
                console.error("Grace period expired! Forcefully killing stuck workers.");
                process.exit(1);
            }, this.gracePeriodMs);

            // Wait for workers to cleanly stop
            await Promise.all([...this.workers].map(worker => worker.stop()));

            // If they finish in time, clear the timer and exit cleanly
            clearTimeout(forceExitTimer);
            console.log("All workers drained cleanly. Exiting.");
            process.exit(0);
        });
    }
}

// Export a single instance (Singleton pattern) so all files share the same coordinator
module.exports = new shutdownCoordinator();
