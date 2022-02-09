const coordinator = require('./coordinator/index');

coordinator.start();

process.on("SIGINT", () => {
    coordinator.stop();
})