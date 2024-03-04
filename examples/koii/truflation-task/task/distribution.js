const { Web3Storage, File } = require('web3.storage');
const { Connection, PublicKey } = require('@_koi/web3.js');
const { namespaceWrapper, TASK_ID } = require('../_koiiNode/koiiNode');
const { SpheronClient, ProtocolEnum } = require('@spheron/storage');
const axios = require('axios');
const { writeFileSync } = require('fs');
class Distribution {
  async submitDistributionList(round) {
    // This function just upload your generated distribution List and do the transaction for that

    console.log('SubmitDistributionList called');

    try {
      const distributionList = await this.generateDistributionList(round);

      const decider = await namespaceWrapper.uploadDistributionList(
        distributionList,
        round,
      );
      console.log('DECIDER', decider);

      if (decider) {
        const response =
          await namespaceWrapper.distributionListSubmissionOnChain(round);
        console.log('RESPONSE FROM DISTRIBUTION LIST', response);
      }
    } catch (err) {
      console.log('ERROR IN SUBMIT DISTRIBUTION', err);
    }
  }

  async auditDistribution(roundNumber) {
    console.log('auditDistribution called with round', roundNumber);
    await namespaceWrapper.validateAndVoteOnDistributionList(
      this.validateDistribution,
      roundNumber,
    );
  }

  async computeAverages(round) {
    const db = await namespaceWrapper.getDb();
    const searchPattern = `scrape:${round}:`;

    // Construct the regular expression dynamically
    const regexPattern = new RegExp(`^${searchPattern}`);
    const itemListRaw = await db.find({ id: regexPattern });
    console.log('itemListRaw', itemListRaw);
    // Object to store averages
    const averages = {};

    // Calculate averages
    for (const item of itemListRaw) {
      const locationSummary = item.data.locationSummary;

      for (const entry of locationSummary) {
        const { location, data } = entry;

        for (const type in data) {
          if (!averages[location]) {
            averages[location] = {};
          }

          if (!averages[location][type]) {
            averages[location][type] = { count: 1, sum: data[type] };
          } else {
            averages[location][type].count++;
            averages[location][type].sum += data[type];
          }
        }
      }
    }

    // Calculate averages and update the original data
    for (const location in averages) {
      const types = averages[location];

      for (const type in types) {
        types[type] = types[type].sum / types[type].count;
      }
    }

    // Print the averages
    console.log(averages);
    return averages;
  }

  async getPreviousSubmissionWallet() {
    console.log('getPreviousSubmissionWallet called');
    const storageWallet = await namespaceWrapper.getStorageWallet();
    console.log('storageWallet', storageWallet.publicKey.toBase58());
    const unInitializedRounds = [];
    const failedRounds = [];
    try {
      const taskState = await namespaceWrapper.getTaskState();
      console.log('taskState', taskState);
      let distributionSubmissionList;
      let lastDistributionRound;
      let rounds = Object.keys(taskState.distribution_rewards_submission);
      console.log('rounds', rounds);
      while (!distributionSubmissionList) {
        let latestRound = Math.max(...rounds.map(Number));
        console.log('latestRound', latestRound);
        if (latestRound === -Infinity) {
          return {
            curr_storage_wallet: '',
            curr_distribution_round: '',
            uninitializedRounds: unInitializedRounds,
            failedRounds: failedRounds,
          };
        }
        if (
          taskState.distributions_audit_record[latestRound] ===
          'PayoutSuccessful'
        ) {
          console.log('Its inside the payout successful case');
          distributionSubmissionList =
            taskState.distribution_rewards_submission[latestRound];
          lastDistributionRound = latestRound;
          break;
        } else {
          if (
            taskState.distributions_audit_record[latestRound] === 'PayoutFailed'
          ) {
            failedRounds.push(latestRound);
          } else if (
            taskState.distributions_audit_record[latestRound] ===
            'Uninitialized'
          ) {
            unInitializedRounds.push(latestRound);
          }
          const index = rounds.indexOf(String(latestRound));
          console.log('index', index);
          if (index > -1) {
            rounds.splice(index, 1);
            console.log('rounds after getting spliced', rounds);
          }
        }
      }
      const distributionKeys = Object.keys(distributionSubmissionList);
      let latestSubmittedSlot = 0;
      let finalDistributionAccount = '';
      for (let index = 0; index < distributionKeys.length; index++) {
        const submissionSlot =
          distributionSubmissionList[distributionKeys[index]].slot;
        if (submissionSlot > latestSubmittedSlot) {
          console.log('submissionSlot', submissionSlot);
          console.log('latestSubmittedSlot', latestSubmittedSlot);
          console.log('distributionKeys[index]', distributionKeys[index]);
          latestSubmittedSlot = submissionSlot;
          finalDistributionAccount = distributionKeys[index];
        }
      }
      if (finalDistributionAccount === '') {
        return {
          curr_storage_wallet: '',
          curr_distribution_round: '',
          uninitializedRounds: unInitializedRounds,
          failedRounds: failedRounds,
        };
      }
      const distributionList = await namespaceWrapper.getDistributionList(
        finalDistributionAccount,
        lastDistributionRound,
      );
      console.log('distributionList', distributionList);
      let parsed = JSON.parse(distributionList);
      parsed = JSON.parse(parsed);
      const keys = Object.keys(parsed);
      for (let index = 0; index < keys.length; index++) {
        const key = keys[index];
        if (parsed[key] == 0) {
          console.log('key in parsed distribution list that equals 0', key);
          return {
            curr_storage_wallet: key,
            curr_distribution_round: lastDistributionRound,
            uninitializedRounds: unInitializedRounds,
            failedRounds: failedRounds,
          };
        }
      }
      return {
        curr_storage_wallet: '',
        curr_distribution_round: '',
        uninitializedRounds: unInitializedRounds,
        failedRounds: failedRounds,
      };
    } catch (error) {
      console.log('ERROR IN GETTING PREVIOUS SUBMISSION WALLET', error);
      return {
        curr_storage_wallet: '',
        curr_distribution_round: '',
        uninitializedRounds: unInitializedRounds,
        failedRounds: failedRounds,
      };
    }
  }

  async generateDistributionList(round, _dummyTaskState, isAuditing = false) {
    try {
      console.log('GenerateDistributionList called');
      console.log('I am selected node');

      // Write the logic to generate the distribution list here by introducing the rules of your choice

      /*  **** SAMPLE LOGIC FOR GENERATING DISTRIBUTION LIST ******/

      let distributionList = {};
      let distributionCandidates = [];
      let unfinishedRoundsData = {};
      if (!isAuditing) {
        const storageWallet = await namespaceWrapper.getStorageWallet();
        const averageData = await this.computeAverages(round);
        const getPreviousSubmissionData =
          await this.getPreviousSubmissionWallet();
        let prevData;
        if (getPreviousSubmissionData.curr_storage_wallet === '') {
          prevData = '';
        } else {
          const uploadableRound = this.getUploadingRound(
            getPreviousSubmissionData.curr_distribution_round,
          );
          console.log('getPreviousSubmissionData', getPreviousSubmissionData);
          prevData = await namespaceWrapper.getDistributionList(
            getPreviousSubmissionData.curr_storage_wallet,
            uploadableRound,
          );
          prevData = JSON.parse(prevData);
          prevData = JSON.parse(prevData);
          console.log('prevData', prevData);
          prevData = prevData['avgData'];
          unfinishedRoundsData = await this.getUnfinishedRoundsData(prevData);
        }

        console.log('getPreviousSubmissionWallet', getPreviousSubmissionData);
        const ipfsUploadableData = {};
        ipfsUploadableData['prevIpfsCid'] = prevData;
        ipfsUploadableData['curr_data'] = {};
        ipfsUploadableData['curr_data'][round] = averageData;
        console.log(
          'unfinishedRoundsData Keys',
          Object.keys(unfinishedRoundsData).length,
        );
        console.log('unfinishedRoundsData', unfinishedRoundsData);
        if (Object.keys(unfinishedRoundsData).length !== 0) {
          const unfinishedRoundKeys = Object.keys(unfinishedRoundsData);
          for (
            let unfinishedRoundIndex = 0;
            unfinishedRoundIndex < unfinishedRoundKeys.length;
            unfinishedRoundIndex++
          ) {
            console.log('unfinishedRoundIndex', unfinishedRoundIndex);
            const unfinishedRound = unfinishedRoundKeys[unfinishedRoundIndex];
            console.log(
              'ipfsUploadableData[curr_data][unfinishedRound]',
              ipfsUploadableData['curr_data'][unfinishedRound],
            );
            console.log(
              'unfinishedRoundsData[unfinishedRound]',
              unfinishedRoundsData[unfinishedRound],
            );
            if (!ipfsUploadableData['curr_data'][unfinishedRound]) {
              ipfsUploadableData['curr_data'][unfinishedRound] =
                unfinishedRoundsData[unfinishedRound];
            }
          }
        }
        ipfsUploadableData['uninitializedRounds'] =
          getPreviousSubmissionData.uninitializedRounds;
        const cid = await this.uploadToIPFS(ipfsUploadableData);
        const uploadableData = {};
        uploadableData['avgData'] = cid;
        // uploadableData['prevRoundStorageWallet'] = getPreviousSubmissionData.curr_storage_wallet;
        let uploadableRound = this.getUploadingRound(round);
        console.log('uploadableRound', uploadableRound);
        await namespaceWrapper.uploadCustomData(
          uploadableData,
          uploadableRound,
        );
        // adding the storage wallet to the distribution list
        distributionList[storageWallet.publicKey.toBase58()] = 0;
      }

      let taskAccountDataJSON = await namespaceWrapper.getTaskState();
      if (taskAccountDataJSON == null) taskAccountDataJSON = _dummyTaskState;
      const submissions = taskAccountDataJSON.submissions[round];
      const submissions_audit_trigger =
        taskAccountDataJSON.submissions_audit_trigger[round];
      if (submissions == null) {
        console.log(`No submisssions found in round ${round}`);
        return distributionList;
      } else {
        const keys = Object.keys(submissions);
        const values = Object.values(submissions);
        const size = values.length;
        console.log('Submissions from last round: ', keys, values, size);

        // Logic for slashing the stake of the candidate who has been audited and found to be false
        for (let i = 0; i < size; i++) {
          const candidatePublicKey = keys[i];
          if (
            submissions_audit_trigger &&
            submissions_audit_trigger[candidatePublicKey]
          ) {
            console.log(
              'distributions_audit_trigger votes ',
              submissions_audit_trigger[candidatePublicKey].votes,
            );
            const votes = submissions_audit_trigger[candidatePublicKey].votes;
            if (votes.length === 0) {
              // slash 70% of the stake as still the audit is triggered but no votes are casted
              // Note that the votes are on the basis of the submission value
              // to do so we need to fetch the stakes of the candidate from the task state
              const stake_list = taskAccountDataJSON.stake_list;
              const candidateStake = stake_list[candidatePublicKey];
              const slashedStake = candidateStake * 0.7;
              distributionList[candidatePublicKey] = -slashedStake;
              console.log('Candidate Stake', candidateStake);
            } else {
              let numOfVotes = 0;
              for (let index = 0; index < votes.length; index++) {
                if (votes[index].is_valid) numOfVotes++;
                else numOfVotes--;
              }

              if (numOfVotes < 0) {
                // slash 70% of the stake as the number of false votes are more than the number of true votes
                // Note that the votes are on the basis of the submission value
                // to do so we need to fetch the stakes of the candidate from the task state
                const stake_list = taskAccountDataJSON.stake_list;
                const candidateStake = stake_list[candidatePublicKey];
                const slashedStake = candidateStake * 0.7;
                distributionList[candidatePublicKey] = -slashedStake;
                console.log('Candidate Stake', candidateStake);
              }

              if (numOfVotes > 0) {
                distributionCandidates.push(candidatePublicKey);
              }
            }
          } else {
            distributionCandidates.push(candidatePublicKey);
          }
        }
      }

      // now distribute the rewards based on the valid submissions
      // Here it is assumed that all the nodes doing valid submission gets the same reward

      const reward = Math.floor(
        taskAccountDataJSON.bounty_amount_per_round /
          distributionCandidates.length,
      );
      console.log('REWARD RECEIVED BY EACH NODE', reward);
      for (let i = 0; i < distributionCandidates.length; i++) {
        distributionList[distributionCandidates[i]] = reward;
      }

      // console.log('Distribution List', distributionList);
      return distributionList;
    } catch (err) {
      console.log('ERROR IN GENERATING DISTRIBUTION LIST', err);
      return {};
    }
  }

  validateDistribution = async (
    distributionListSubmitter,
    round,
    _dummyDistributionList,
    _dummyTaskState,
  ) => {
    // Write your logic for the validation of submission value here and return a boolean value in response
    // this logic can be same as generation of distribution list function and based on the comparision will final object , decision can be made

    // try {
    //   console.log('Distribution list Submitter', distributionListSubmitter);
    //   const rawDistributionList = await namespaceWrapper.getDistributionList(
    //     distributionListSubmitter,
    //     round,
    //   );
    //   let fetchedDistributionList;
    //   if (rawDistributionList == null) {
    //     fetchedDistributionList = _dummyDistributionList;
    //   } else {
    //     fetchedDistributionList = JSON.parse(rawDistributionList);
    //   }
    //   // const returnedList = await namespaceWrapper.getAverageDataFromPubKey(pubKeyReturned, round);
    //   console.log('FETCHED DISTRIBUTION LIST', fetchedDistributionList);
    //   const generateDistributionList = await this.generateDistributionList(
    //     round,
    //     _dummyTaskState,
    //     true,
    //   );

    //   // compare distribution list

    //   const parsed = fetchedDistributionList;
    //   console.log(
    //     'compare distribution list',
    //     parsed,
    //     generateDistributionList,
    //   );
    //   const result = await this.shallowEqual(parsed, generateDistributionList);
    //   console.log('RESULT', result);
    //   return result;
    // } catch (err) {
    //   console.log('ERROR IN VALIDATING DISTRIBUTION', err);
    //   return false;
    // }
    return true;
  };

  async shallowEqual(parsed, generateDistributionList) {
    if (typeof parsed === 'string') {
      parsed = JSON.parse(parsed);
    }

    // Normalize key quote usage for generateDistributionList
    generateDistributionList = JSON.parse(
      JSON.stringify(generateDistributionList),
    );

    const keys1 = Object.keys(parsed);
    const keys2 = Object.keys(generateDistributionList);
    if (keys1.length !== keys2.length) {
      return false;
    }

    for (let key of keys1) {
      if (parsed[key] !== generateDistributionList[key]) {
        return false;
      }
    }
    return true;
  }

  async uploadToIPFS(data) {
    const client = new SpheronClient({
      token: process.env.Spheron_Storage,
    });
    const listFilePath = await this.makeFileObjects(data);
    // const cid = await client.put(files);
    // console.log('stored files with cid:', cid);
    // return cid;
    console.log(
      '***************STORING FILES***************',
      listFilePath,
    );

    let currentlyUploaded = 0;

    const { cid } = await client.upload(listFilePath, {
      protocol: ProtocolEnum.IPFS,
      name: 'test',
      onUploadInitiated: uploadId => {
        console.log(`Upload with id ${uploadId} started...`);
      },
      onChunkUploaded: (uploadedSize, totalSize) => {
        currentlyUploaded += uploadedSize;
        console.log(`Uploaded ${currentlyUploaded} of ${totalSize} Bytes.`);
      },
    });

    console.log(`CID: ${cid}`);
    return cid;
  }

  async makeFileObjects(obj) {
    // const buffer = Buffer.from(JSON.stringify(obj));
    // const files = [new File([buffer], 'data.json')];
    // return files;
    try {
      const dataString = JSON.stringify(obj);
  
      // await namespaceWrapper.fs('writeFile', 'data.json', dataString);
  
      const path = await namespaceWrapper.getBasePath();
      console.log('path', path);
      const filePath = path + '/data.json';
      writeFileSync(filePath, dataString);
      return filePath;
    } catch (error) {
      console.log('error', error);
    }
  }

  getUploadingRound(round) {
    if (round === '0') {
      return 0;
    }
    round = parseInt(round);
    return Math.abs(round) % 10;
  }

  async getUnfinishedRoundsData(prevData) {
    try {
      const prev_round_data = await axios.get(
        // `https://${prevData}.ipfs.w3s.link/data.json`,
        `https://${prevData}.ipfs.dweb.link/data.json`,
      );
      console.log('prev round data', prev_round_data.data);
      if (
        !prev_round_data ||
        !prev_round_data.data ||
        !prev_round_data.data.uninitializedRounds ||
        prev_round_data.data.uninitializedRounds.length === 0
      ) {
        console.log('No unfinished rounds data found');
        return {};
      }
      const unfinishedRounds = prev_round_data.data.uninitializedRounds;
      console.log('unfinishedRounds', unfinishedRounds);
      const finishedRoundData = {};
      const taskState = await namespaceWrapper.getTaskState();

      for (let index = 0; index < unfinishedRounds.length; index++) {
        let element = unfinishedRounds[index];
        console.log('unfinished business for element ', element);
        if (
          taskState.distributions_audit_record[element] === 'PayoutSuccessful'
        ) {
          console.log('Its inside the payout successful case');
          const distributionSubmissions =
            taskState.distribution_rewards_submission[element];
          const latestDistributionSubmission =
            this.findLatestDistributionSubmission(distributionSubmissions);
          console.log(
            'latestDistributionSubmission',
            latestDistributionSubmission,
          );
          if (!latestDistributionSubmission) {
            continue;
          }
          const parsed = await this.fetchDataFromAccount(
            latestDistributionSubmission,
            element,
          );
          console.log('parsed', parsed);
          const storageWalletAccount = this.findStorageWalletAccount(parsed);
          console.log('storageWalletAccount', storageWalletAccount);
          const uploadableRound = this.getUploadingRound(element);
          console.log('uploadableRound', uploadableRound);
          const origData = await this.fetchDataFromAccount(
            storageWalletAccount,
            uploadableRound,
          );
          console.log('origData', origData);
          finishedRoundData[element] = origData;
        }
      }
      console.log('finishedRoundData', finishedRoundData);
      return finishedRoundData;
    } catch (error) {
      console.log('ERROR IN GETTING UNFINISHED ROUNDS DATA', error);
      return {};
    }
  }

  async fetchDataFromAccount(account, round) {
    const rpcUrl = await namespaceWrapper.getRpcUrl();
    const connection = new Connection(rpcUrl, 'confirmed');
    const storageWalletAccountInfo = await connection.getAccountInfo(
      new PublicKey(account),
    );
    const storageWalletAccountData = JSON.parse(
      storageWalletAccountInfo.data + '',
    );
    if (storageWalletAccountData[round]) {
      const bufferAccountData = Buffer.from(
        storageWalletAccountData[round][TASK_ID],
      );

      let origData = this.extractOrigDataFromBuffer(bufferAccountData);
      origData = JSON.parse(origData);
      origData = JSON.parse(origData);
      return origData;
    } else return null;
  }

  extractOrigDataFromBuffer(bufferData) {
    const index = bufferData.indexOf(0x00);
    const slicedBuffer = bufferData.slice(0, index);
    return JSON.stringify(new TextDecoder().decode(slicedBuffer));
  }

  findLatestDistributionSubmission(distributionSubmissions) {
    let latestSlot = -Infinity;
    let latestSubmissionKey = null;

    for (const key in distributionSubmissions) {
      const slot = distributionSubmissions[key].slot;
      if (slot > latestSlot) {
        latestSlot = slot;
        latestSubmissionKey = key;
      }
    }
    return latestSubmissionKey;
  }

  findStorageWalletAccount(parsed) {
    for (const key in parsed) {
      if (parsed[key] === 0) {
        return key;
      }
    }
    return null;
  }
}

const distribution = new Distribution();
module.exports = {
  distribution,
};
