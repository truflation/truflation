const { coreLogic } = require('../coreLogic');
const task = require('../task');
const index = require('../index');

async function test_coreLogic() {
  const round = 1;
  await coreLogic.task(round);
  // const submission = await coreLogic.submitTask(round);
  // const submission = "bafybeiait5nqvamkh32zpd447f5zox6fluczzm5cm4jricszkdmnkyizgy";
  // console.log('Receive test submission', submission);
  // const audit = await task.audit.validateNode(submission, round);
  // console.log('Audit test submission', audit);
}

test_coreLogic();
