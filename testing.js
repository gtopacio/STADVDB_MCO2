const detector = require('./detector/index');

detector.start();

process.on('SIGINT', () => {
    detector.stop();
})