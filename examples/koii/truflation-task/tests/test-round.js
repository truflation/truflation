const dotenv = require('dotenv');
require('dotenv').config();
const  Kayak = require('../kayak-task');
const { coreLogic } = require('../coreLogic');

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function executeTasks() {
    for (let i = 0; i < 10; i++) {
        let delay = 60000;
        let round = i;
        await coreLogic.task(round);

        await sleep(delay);

        console.log('stopping crawler at round', round);
    }
}

executeTasks();
