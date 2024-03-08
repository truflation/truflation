const { namespaceWrapper } = require('../_koiiNode/koiiNode');
const KayakTask = require('../kayak-task');
class Submission {
  constructor() {
    this.kayakTask = null;
  }
  async task(round) {
    // Write the logic to do the work required for submitting the values and optionally store the result in levelDB

    // Below is just a sample of work that a task can do
    // https://www.kayak.com/Cheap-New-York-Car-Rentals.15830.cars.ksp
    // we will work to create a proof that can be submitted to K2 to claim rewards
    let proof_cid;

    // in order for this proof to withstand scrutiny (see validateNode, below, for audit flow) the proof must be generated from a full round of valid work

    // the following function starts the crawler if not already started, or otherwise fetches a submission CID for a particular round
    round = await namespaceWrapper.getRound();
    if (!this.kayakTask || !this.kayakTask.isRunning) {
      try {
        this.kayakTask = await new KayakTask(
          namespaceWrapper.getRound,
          round,
        );
        console.log('started a new crawler at round', round);
      } catch (e) {
        console.log('error starting crawler', e);
      }
    } else {
      console.log('crawler already running at round', round);
    }
  }

  async submitTask(roundNumber) {
    console.log('submitTask called with round', roundNumber);
    try {
      console.log('inside try');
      if (!this.kayakTask || !this.kayakTask.isRunning){
        return;
      }
      console.log(
        await namespaceWrapper.getSlot(),
        'current slot while calling submit',
      );
      const submission = await this.fetchSubmission(roundNumber);
      if(submission){
      console.log('SUBMISSION', submission);
      await namespaceWrapper.checkSubmissionAndUpdateRound(
        submission,
        roundNumber,
      );
      console.log('after the submission call');
      }
      else {
        console.log('no submission call made as submission is null');
      }
      return submission;
    } catch (error) {
      console.log('error in submission', error);
    }
  }

  async fetchSubmission(round) {
    // Write the logic to fetch the submission values here and return the cid string

    // fetching round number to store work accordingly

    console.log('IN FETCH SUBMISSION');

    // The code below shows how you can fetch your stored value from level DB

    const cid = await this.kayakTask.getRoundCID(round);// retrieves the value
    console.log('VALUE', cid);
    return cid;
  }
}
const submission = new Submission();
module.exports = { submission };
