const task = require('./task');
const { namespaceWrapper } = require('./_koiiNode/koiiNode');

class CoreLogic {
  async task(round) {
    const result = await task.submission.task(round);
    return result;
  }

  async submitTask(round) {
    const submission = await task.submission.submitTask(round);
    return submission;
  }

  async auditTask(round) {
    await task.audit.auditTask(round);
  }

  async selectAndGenerateDistributionList(
    round,
    isPreviousRoundFailed = false,
  ) {
    await namespaceWrapper.selectAndGenerateDistributionList(
      task.distribution.submitDistributionList,
      round,
      isPreviousRoundFailed,
    );
  }

  async auditDistribution(round) {
    await task.distribution.auditDistribution(round);
  }
}
const coreLogic = new CoreLogic();

module.exports = { coreLogic };
