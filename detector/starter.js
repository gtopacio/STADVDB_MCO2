const detector = require('./index');

detector.start();

function stop(){
    detector.stop();
}

process.on("SIGINT", stop);