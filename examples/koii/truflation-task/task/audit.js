const { namespaceWrapper } = require('../_koiiNode/koiiNode');
const { KoiiStorageClient } = require('@_koii/storage-task-sdk');
class Audit {
  async validateNode(submission_value, round) {
    // Write your logic for the validation of submission value here and return a boolean value in response

    // The sample logic can be something like mentioned below to validate the submission
    let vote = true;
    console.log('SUBMISSION VALUE', submission_value, round);
    try {
      let data = await getJSONFromCID(submission_value, 'data.json'); // check this
      console.log('DATA', data);
      for(let i = 0; i < data.length; i++) {
        // if(data[i].round !== round) {
        //   vote = false;
        //   break;
        // }
        console.log('DATA', data[i].data);
      }
      if (data) vote = true;
    } catch (e) {
      console.error(e);
      vote = false;
    }
    return vote;
  }

  async auditTask(roundNumber) {
    console.log('auditTask called with round', roundNumber);
    console.log(
      await namespaceWrapper.getSlot(),
      'current slot while calling auditTask',
    );
    await namespaceWrapper.validateAndVoteOnNodes(
      this.validateNode,
      roundNumber,
    );
  }
}

const getJSONFromCID = async (cid, fileName, retries = 3) => {
  const client = new KoiiStorageClient(undefined, undefined, false);

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const blob = await client.getFile(cid, fileName);
      const text = await blob.text(); // Convert Blob to text
      const data = JSON.parse(text); // Parse text to JSON
      return data;
    } catch (error) {
      console.log(
        `Attempt ${attempt}: Error fetching file from Koii IPFS: ${error.message}`,
      );
      if (attempt === retries) {
        throw new Error(`Failed to fetch file after ${retries} attempts`);
      }
      // Optionally, you can add a delay between retries
      await new Promise(resolve => setTimeout(resolve, 3000)); // 3-second delay
    }
  }

  return null;
};
const audit = new Audit();
module.exports = { audit };
