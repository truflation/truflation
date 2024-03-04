const { default: axios } = require('axios');

const {
  Connection,
  PublicKey,
  Keypair,
  SystemProgram,
  Transaction,
  LAMPORTS_PER_SOL,
  TransactionInstruction,
  sendAndConfirmTransaction,
} = require('@_koi/web3.js');
const fs = require('fs');
const Datastore = require('nedb-promises');
const fsPromises = require('fs/promises');
const bs58 = require('bs58');
const BufferLayout = require('@solana/buffer-layout');
const nacl = require('tweetnacl');
const makeChunks = require('../helpers/makeChunks');
const encodeData = require('../helpers/encodeData');
const DEFAULT_PROGRAM_ID = 'Koiitask22222222222222222222222222222222222';
const CHUNKNUM_BYTE_COUNT = 2; // u16 int, which chunk
const CHUNKSIZE_BYTE_COUNT = 2; // u16 int, how big are chunks
const TASK_INSTRUCTION_LAYOUTS = Object.freeze({
  UploadDistributionList: {
    index: 11,
    layout: BufferLayout.struct([
      BufferLayout.u8('instruction'),
      BufferLayout.blob(512, 'instruction_data'),
      BufferLayout.ns64('round'),
    ]),
  },
});

/****************************************** init.js ***********************************/

const express = require('express');
// Only used for testing purposes, in production the env will be injected by tasknode
require('dotenv').config();
const bodyParser = require('body-parser');
/**
 * This will be the name of the current task as coming from the task node running this task.
 */
const TASK_NAME = process.argv[2] || 'Local';
/**
 * This will be the id of the current task as coming from the task node running this task.
 */
const TASK_ID = process.argv[3];
/**
 * This will be the PORT on which the this task is expected to run the express server coming from the task node running this task.
 * As all communication via the task node and this task will be done on this port.
 */
const EXPRESS_PORT = process.argv[4] || 10000;

const LogLevel = {
  Log: 'log',
  Warn: 'warn',
  Error: 'error',
};

// Not used anymore
// const NODE_MODE = process.argv[5];

/**
 * This will be the main account public key in string format of the task node running this task.
 */
const MAIN_ACCOUNT_PUBKEY = process.argv[6];
/**
 * This will be the secret used by the task to authenticate with task node running this task.
 */
const SECRET_KEY = process.argv[7];
/**
 * This will be K2 url being used by the task node, possible values are 'https://k2-testnet.koii.live' | 'https://k2-devnet.koii.live' | 'http://localhost:8899'
 */
const K2_NODE_URL = process.argv[8] || 'https://k2-testnet.koii.live';
/**
 * This will be public task node endpoint (Or local if it doesn't have any) of the task node running this task.
 */
const SERVICE_URL = process.argv[9];
/**
 * This will be stake of the task node running this task, can be double checked with the task state and staking public key.
 */
const STAKE = Number(process.argv[10]);
/**
 * This will be the port used by task node as the express server port, so it can be used by the task for the communication with the task node
 */
const TASK_NODE_PORT = Number(process.argv[11]);

const app = express();

console.log('SETTING UP EXPRESS');

app.use(bodyParser.urlencoded({ extended: false }));

app.use(bodyParser.json());

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader(
    'Access-Control-Allow-Methods',
    'GET, POST, PUT, PATCH, DELETE',
  );
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Allow-Credentials', false);
  if (req.method === 'OPTIONS')
    // if is preflight(OPTIONS) then response status 204(NO CONTENT)
    return res.send(204);
  next();
});

app.get('/', (req, res) => {
  res.send('Hello World!');
});

const _server = app.listen(EXPRESS_PORT, () => {
  console.log(`${TASK_NAME} listening on port ${EXPRESS_PORT}`);
});

/****************************************** NamespaceWrapper.js ***********************************/

const taskNodeAdministered = !!TASK_ID;
const BASE_ROOT_URL = `http://localhost:${TASK_NODE_PORT}/namespace-wrapper`;

class NamespaceWrapper {
  #db;
  #testingMainSystemAccount;
  #testingStakingSystemAccount;
  #testingTaskState;
  #testingDistributionList;

  constructor() {
    console.log('taskNodeAdministered', taskNodeAdministered);
    if (taskNodeAdministered) {
      this.initializeDB();
    } else {
      this.#db = Datastore.create('./localKOIIDB.db');
      this.defaultTaskSetup();
    }
  }

  async initializeDB() {
    if (this.#db) return;
    try {
      if (taskNodeAdministered) {
        const path = await this.getTaskLevelDBPath();
        // console.log('path', path);
        // if(this.fs.existsSync(path))
        //   this.#db = new Datastore({ filename: path, autoload: true });
        this.#db = Datastore.create(path);
      } else {
        this.#db = Datastore.create('./localKOIIDB.db');
      }
    } catch (e) {
      this.#db = Datastore.create(`../namespace/${TASK_ID}/KOIILevelDB.db`);
    }
  }

  async getDb() {
    if (this.#db) return this.#db;
    await this.initializeDB();
    return this.#db;
  }
  /**
   * Namespace wrapper of storeGetAsync
   * @param {string} key // Path to get
   */
  async storeGet(key) {
    try {
      await this.initializeDB();
      const resp = await this.#db.findOne({ key: key });
      if (resp) {
        return resp[key];
      } else {
        return null;
      }
    } catch (e) {
      console.error(e);
      return null;
    }
  }
  /**
   * Namespace wrapper over storeSetAsync
   * @param {string} key Path to set
   * @param {*} value Data to set
   */
  async storeSet(key, value) {
    try {
      await this.initializeDB();
      await this.#db.update(
        { key: key },
        { [key]: value, key },
        { upsert: true },
      );
    } catch (e) {
      console.error(e);
      return undefined;
    }
  }

  /**
   * Namespace wrapper over fsPromises methods
   * @param {*} method The fsPromise method to call
   * @param {*} path Path for the express call
   * @param  {...any} args Remaining parameters for the FS call
   */
  async fs(method, path, ...args) {
    if (taskNodeAdministered) {
      return await genericHandler('fs', method, path, ...args);
    } else {
      return fsPromises[method](`${path}`, ...args);
    }
  }
  async fsStaking(method, path, ...args) {
    if (taskNodeAdministered) {
      return await genericHandler('fsStaking', method, path, ...args);
    } else {
      return fsPromises[method](`${path}`, ...args);
    }
  }

  async fsWriteStream(imagepath) {
    if (taskNodeAdministered) {
      return await genericHandler('fsWriteStream', imagepath);
    } else {
      const writer = createWriteStream(imagepath);
      return writer;
    }
  }
  async fsReadStream(imagepath) {
    if (taskNodeAdministered) {
      return await genericHandler('fsReadStream', imagepath);
    } else {
      const file = readFileSync(imagepath);
      return file;
    }
  }

  /**
   * Namespace wrapper for getting current slots
   */
  async getSlot() {
    if (taskNodeAdministered) {
      return await genericHandler('getCurrentSlot');
    } else {
      return 100;
    }
  }

  async payloadSigning(body) {
    if (taskNodeAdministered) {
      return await genericHandler('signData', body);
    } else {
      const msg = new TextEncoder().encode(JSON.stringify(body));
      const signedMessage = nacl.sign(
        msg,
        this.#testingMainSystemAccount.secretKey,
      );
      return await this.bs58Encode(signedMessage);
    }
  }

  async bs58Encode(data) {
    return bs58.encode(
      Buffer.from(data.buffer, data.byteOffset, data.byteLength),
    );
  }

  async bs58Decode(data) {
    return new Uint8Array(bs58.decode(data));
  }

  decodePayload(payload) {
    return new TextDecoder().decode(payload);
  }

  /**
   * Namespace wrapper of storeGetAsync
   * @param {string} signedMessage r // Path to get
   */

  async verifySignature(signedMessage, pubKey) {
    if (taskNodeAdministered) {
      return await genericHandler('verifySignedData', signedMessage, pubKey);
    } else {
      try {
        const payload = nacl.sign.open(
          await this.bs58Decode(signedMessage),
          await this.bs58Decode(pubKey),
        );
        if (!payload) return { error: 'Invalid signature' };
        return { data: this.decodePayload(payload) };
      } catch (e) {
        console.error(e);
        return { error: `Verification failed: ${e}` };
      }
    }
  }

  // async submissionOnChain(submitterKeypair, submission) {
  //   return await genericHandler(
  //     'submissionOnChain',
  //     submitterKeypair,
  //     submission,
  //   );
  // }

  async stakeOnChain(
    taskStateInfoPublicKey,
    stakingAccKeypair,
    stakePotAccount,
    stakeAmount,
  ) {
    if (taskNodeAdministered) {
      return await genericHandler(
        'stakeOnChain',
        taskStateInfoPublicKey,
        stakingAccKeypair,
        stakePotAccount,
        stakeAmount,
      );
    } else {
      this.#testingTaskState.stake_list[
        this.#testingStakingSystemAccount.publicKey.toBase58()
      ] = stakeAmount;
    }
  }
  async claimReward(stakePotAccount, beneficiaryAccount, claimerKeypair) {
    if (!taskNodeAdministered) {
      console.log('Cannot call sendTransaction in testing mode');
      return;
    }
    return await genericHandler(
      'claimReward',
      stakePotAccount,
      beneficiaryAccount,
      claimerKeypair,
    );
  }
  async sendTransaction(serviceNodeAccount, beneficiaryAccount, amount) {
    if (!taskNodeAdministered) {
      console.log('Cannot call sendTransaction in testing mode');
      return;
    }
    return await genericHandler(
      'sendTransaction',
      serviceNodeAccount,
      beneficiaryAccount,
      amount,
    );
  }

  async getSubmitterAccount() {
    if (taskNodeAdministered) {
      const submitterAccountResp = await genericHandler('getSubmitterAccount');
      return Keypair.fromSecretKey(
        Uint8Array.from(Object.values(submitterAccountResp._keypair.secretKey)),
      );
    } else {
      return this.#testingStakingSystemAccount;
    }
  }

  /**
   * sendAndConfirmTransaction wrapper that injects mainSystemWallet as the first signer for paying the tx fees
   * @param {connection} method // Receive method ["get", "post", "put", "delete"]
   * @param {transaction} path // Endpoint path appended to namespace
   * @param {Function} callback // Callback function on traffic receive
   */
  async sendAndConfirmTransactionWrapper(transaction, signers) {
    if (!taskNodeAdministered) {
      console.log('Cannot call sendTransaction in testing mode');
      return;
    }
    const blockhash = (await connection.getRecentBlockhash('finalized'))
      .blockhash;
    transaction.recentBlockhash = blockhash;
    transaction.feePayer = new PublicKey(MAIN_ACCOUNT_PUBKEY);
    return await genericHandler(
      'sendAndConfirmTransactionWrapper',
      transaction.serialize({
        requireAllSignatures: false,
        verifySignatures: false,
      }),
      signers,
    );
  }

  // async signArweave(transaction) {
  //   let tx = await genericHandler('signArweave', transaction.toJSON());
  //   return arweave.transactions.fromRaw(tx);
  // }
  // async signEth(transaction) {
  //   return await genericHandler('signEth', transaction);
  // }
  async getTaskState() {
    if (taskNodeAdministered) {
      const response = await genericHandler('getTaskState');
      if (response.error) {
        return null;
      }
      return response;
    } else {
      return this.#testingTaskState;
    }
  }

  async logMessage(level, message) {
    switch (level) {
      case LogLevel.Log:
        console.log(message);
        break;
      case LogLevel.Warn:
        console.warn(message);
        break;
      case LogLevel.Error:
        console.error(message);
        break;
      default:
        console.log(
          `Invalid log level: ${level}. The log levels can be log, warn or error`,
        );
        return false;
    }
    return true;
  }

  /**
   * This logger function is used to log the task erros , warnings and logs on desktop-node
   * @param {level} enum // Receive method ["Log", "Warn", "Error"]
   enum LogLevel {
   Log = 'log',
   Warn = 'warn',
   Error = 'error',
   }
   * @param {message} string // log, error or warning message
   * @returns {boolean} // true if the message is logged successfully otherwise false
   */

  async logger(level, message) {
    if (taskNodeAdministered) {
      return await genericHandler('logger', level, message);
    } else {
      return await this.logMessage(level, message);
    }
  }

  async auditSubmission(candidatePubkey, isValid, voterKeypair, round) {
    if (taskNodeAdministered) {
      return await genericHandler(
        'auditSubmission',
        candidatePubkey,
        isValid,
        round,
      );
    } else {
      if (
        this.#testingTaskState.submissions_audit_trigger[round] &&
        this.#testingTaskState.submissions_audit_trigger[round][candidatePubkey]
      ) {
        this.#testingTaskState.submissions_audit_trigger[round][
          candidatePubkey
        ].votes.push({
          is_valid: isValid,
          voter: voterKeypair.pubKey.toBase58(),
          slot: 100,
        });
      } else {
        this.#testingTaskState.submissions_audit_trigger[round] = {
          [candidatePubkey]: {
            trigger_by: this.#testingStakingSystemAccount.publicKey.toBase58(),
            slot: 100,
            votes: [],
          },
        };
      }
    }
  }

  async distributionListAuditSubmission(
    candidatePubkey,
    isValid,
    voterKeypair,
    round,
  ) {
    if (taskNodeAdministered) {
      return await genericHandler(
        'distributionListAuditSubmission',
        candidatePubkey,
        isValid,
        round,
      );
    } else {
      if (
        this.#testingTaskState.distributions_audit_trigger[round] &&
        this.#testingTaskState.distributions_audit_trigger[round][
          candidatePubkey
        ]
      ) {
        this.#testingTaskState.distributions_audit_trigger[round][
          candidatePubkey
        ].votes.push({
          is_valid: isValid,
          voter: voterKeypair.pubKey.toBase58(),
          slot: 100,
        });
      } else {
        this.#testingTaskState.distributions_audit_trigger[round] = {
          [candidatePubkey]: {
            trigger_by: this.#testingStakingSystemAccount.publicKey.toBase58(),
            slot: 100,
            votes: [],
          },
        };
      }
    }
  }

  async getRound() {
    if (taskNodeAdministered) {
      return await genericHandler('getRound');
    } else {
      return 1;
    }
  }

  async nodeSelectionDistributionList() {
    if (taskNodeAdministered) {
      return await genericHandler('nodeSelectionDistributionList');
    } else {
      return this.#testingStakingSystemAccount.publicKey.toBase58();
    }
  }

  async payoutTrigger() {
    if (taskNodeAdministered) {
      return await genericHandler('payloadTrigger');
    } else {
      console.log(
        'Payout Trigger only handles possitive flows (Without audits)',
      );
      let round = 1;
      const submissionValAcc =
        this.#testingDistributionList[round][
          this.#testingStakingSystemAccount.toBase58()
        ].submission_value;
      this.#testingTaskState.available_balances =
        this.#testingDistributionList[round][submissionValAcc];
    }
  }

  async uploadDistributionList(distributionList, round) {
    if (taskNodeAdministered) {
      return await genericHandler(
        'uploadDistributionList',
        distributionList,
        round,
      );
    } else {
      if (!this.#testingDistributionList[round])
        this.#testingDistributionList[round] = {};

      this.#testingDistributionList[round][
        this.#testingStakingSystemAccount.publicKey.toBase58()
      ] = Buffer.from(JSON.stringify(distributionList));
      return true;
    }
  }

  async createDataStorageWallet() {
    const newAccount = new Keypair();
    // console.log('newAccount', newAccount.publicKey.toBase58());
    const createSubmitterAccTransaction = new Transaction().add(
      SystemProgram.createAccount({
        fromPubkey: new PublicKey(MAIN_ACCOUNT_PUBKEY),
        newAccountPubkey: newAccount.publicKey,
        lamports:
          0.15 * LAMPORTS_PER_SOL +
          (await connection.getMinimumBalanceForRentExemption(100)) +
          10000, //Adding 10,000 extra lamports for padding
        space: 100,
        programId: new PublicKey('Koiitask22222222222222222222222222222222222'),
      }),
    );
    const confirmation = await this.sendAndConfirmTransactionWrapper(
      createSubmitterAccTransaction,
      [newAccount],
    );
    console.log('confirmation', confirmation);
    fs.writeFileSync(
      (await this.getBasePath()) + '/dataStorageWallet.json',
      JSON.stringify(Array.from(newAccount.secretKey)),
    );
    return newAccount;
  }

  async getStorageWallet() {
    const filePath = (await this.getBasePath()) + '/dataStorageWallet.json';

    if (fs.existsSync(filePath)) {
      const dataStorageWallet = await fs.readFileSync(
        (await this.getBasePath()) + '/dataStorageWallet.json',
      );
      return Keypair.fromSecretKey(
        Uint8Array.from(JSON.parse(dataStorageWallet)),
      );
    } else {
      const newAccount = await this.createDataStorageWallet();
      return newAccount;
    }
  }

  // async uploadCustomData(customData, round) {
  //   const TASK_INSTRUCTION_LAYOUTS = Object.freeze({
  //     UploadDistributionList: {
  //       //Upload Distribution complex flow, seperate script
  //       index: 11,
  //       layout: BufferLayout.struct([
  //         BufferLayout.u8('instruction'),
  //         BufferLayout.blob(512, 'instruction_data'),
  //         BufferLayout.ns64('round'),
  //       ]),
  //     },
  //   });
  //   const CHUNKNUM_BYTE_COUNT = 2; // u16 int, which chunk
  //   const CHUNKSIZE_BYTE_COUNT = 2; // u16 int, how big are chunks
  //   const mainAccountKey = await this.getMainAccountPubkey();
  //   if (!mainAccountKey) {
  //     throw new Error('Main system account not found');
  //   }

  //   console.log('MAIN SYSTEM PUBKEY', mainAccountKey);

  //   console.log('DISTRIBUTION OBJECT', customData);

  //   console.log('Round value passed', round);

  //   const buf = Buffer.from(JSON.stringify(customData));
  //   const data = buf;
  //   console.log('ProgramId to use:', SystemProgram.programId);

  //   console.log('Data contains', data.length, 'bytes');

  //   const storageWalletAccount = await this.getStorageWallet();

  //   // split our data across a number of calls

  //   const remainingByteCount = 512;

  //   const dataByteCount =
  //     remainingByteCount - (CHUNKNUM_BYTE_COUNT + CHUNKSIZE_BYTE_COUNT);

  //   const numBytes = data.length;
  //   const chunks = makeChunks.makeChunks(data, dataByteCount);
  //   console.log('CHUNKS LENGTH', chunks.length);

  //   console.log('Data will be uploaded over', chunks.length, 'calls');

  //   const { feeCalculator } = await connection.getRecentBlockhash();
  //   const costOfAccountRent =
  //     await connection.getMinimumBalanceForRentExemption(numBytes);

  //   const costOfAccountTran = 2 * feeCalculator.lamportsPerSignature;
  //   let costOfAccountUpload = 0;

  //   for (let i = 0; i < chunks.length; i++) {
  //     const chunkCost = 2 * feeCalculator.lamportsPerSignature; // two signers, the payer and the new account
  //     costOfAccountUpload += chunkCost;
  //   }

  //   const total = costOfAccountRent + costOfAccountTran + costOfAccountUpload;

  //   console.log(
  //     'EST FINAL TOTAL -- ',
  //     total,
  //     'lamports (',
  //     total / LAMPORTS_PER_SOL,
  //     ')',
  //   );

  //   const bal = await connection.getBalance(new PublicKey(mainAccountKey));

  //   if (bal < total) {
  //     console.error(
  //       'WARNING, your balance',
  //       bal,
  //       'is less than the estimate',
  //       total,
  //     );
  //     return null;
  //   }

  //   await this.sleep(1000); //To make sure the above account is created before uploading data

  //   const balBeforeUpload = await connection.getBalance(new PublicKey(mainAccountKey));

  //     // const encodedData =  encodeData.encodeData(chunks[0]);
  //     // const instruction = new TransactionInstruction({
  //     //   keys: [
  //     //     { pubkey: storageWalletAccount.publicKey, isSigner: false, isWritable: true },
  //     //   ],
  //     //   programId: SystemProgram.programId,
  //     //   data: chunks[0],
  //     // });

  //     // const transaction = new Transaction().add(instruction);
  //     // const txnSignature = await this.sendAndConfirmTransactionWrapper(
  //     //   transaction,
  //     //   [storageWalletAccount],
  //     // );
  //     const len =  chunks[0].length;
  //     console.log('chunk length', len);
  //     if (len < 512) {
  //       // Instruction data must be of 512 bytes so padding with null buffer
  //       const rem = 512 - len;
  //       const a = Buffer.from(new Array(rem).fill(0));
  //       chunks[0] = Buffer.concat([ chunks[0], a]);
  //     }
  //     console.log('After the padding',chunks[0].length);
  //     const encodedTransactionInstruction = encodeData.encodeData(
  //       TASK_INSTRUCTION_LAYOUTS.UploadDistributionList,

  //       {
  //         instruction_data: chunks[0],
  //         round: round,
  //       },
  //     );

  //     const instruction = new TransactionInstruction({
  //       keys: [
  //         {
  //           pubkey: new PublicKey('4WQ8CTgEknyRZKRUjgBVtNpv3EteqDA3W3PLncZGXQZP') /* */,
  //           isSigner: false,
  //           isWritable: false,
  //         },
  //         {
  //           pubkey: storageWalletAccount.publicKey,
  //           isSigner: true,
  //           isWritable: true,
  //         },
  //       ], // add acc here after pk
  //       programId: new PublicKey('Koiitask22222222222222222222222222222222222'),
  //       data: encodedTransactionInstruction,
  //     });

  //     const transaction = new Transaction().add(instruction);
  //     console.log("transaction given", transaction);
  //     const mainStorageWallet = await fs.readFileSync(
  //       '/Users/diszsid/.config/koii/id.json',
  //     );
  //     const mainKeyPair =  Keypair.fromSecretKey(
  //       Uint8Array.from(JSON.parse(mainStorageWallet)),
  //     );

  //     const txnSignature = await sendAndConfirmTransaction(connection, transaction, [
  //       mainKeyPair, storageWalletAccount,
  //     ]);
  //     console.log('txnSignature', txnSignature);
  //     //console.log('CALLED AFTER TRANSACTION');
  //     await this.sleep(100);

  //   const balAfterUpload = await connection.getBalance(new PublicKey(mainAccountKey));
  //   const costOfUpload = balBeforeUpload - balAfterUpload;
  //   console.log(
  //     'Total cost was',
  //     costOfUpload,
  //     'lamports (',
  //     costOfUpload / LAMPORTS_PER_SOL,
  //     ')',
  //   );
  //   // TODO: Verify Retry Failed Transactions (logic changed)

  //   console.log('ROUND VY', round);
  //   await this.sleep(10000);
  //   const isDistributionListValid = await this.fetchUploadedstorage(
  //   );
  //   // if (!isDistributionListValid) {
  //   //   console.log('Distribution List is not uploaded successfully');
  //   //   for (let i = 0; i < 5; i++) {
  //   //     console.log(`Retrying upload distribution list for ${i} time(s) `);
  //   //     const result = await this.retryFailedTransactions(
  //   //       dataByteCount,
  //   //       chunks,
  //   //       this.mainSystemAccount,
  //   //       distributionListAccount,
  //   //       '' + round,
  //   //     );
  //   //     if (result == true) {
  //   //       await sleep(10000);
  //   //       const isValid = await this.fetchUploadedstorage(
  //   //       );
  //   //       if (isValid == true) break;
  //   //     }
  //   //   }
  //   // }
  //   // const result = await this.fetchUploadedstorage(
  //   // );
  //   console.log('result', isDistributionListValid);
  //   return isDistributionListValid;
  // }

  async uploadCustomData(distributionListDataBlob, round) {
    const distributionAccountKeyPair = await this.getStorageWallet();
    console.log('DISTRIBUTION OBJECT', distributionListDataBlob);

    console.log('Round value passed', round);

    const buf = Buffer.from(JSON.stringify(distributionListDataBlob));
    const data = buf;
    console.log('ProgramId to use:', DEFAULT_PROGRAM_ID);

    console.log('Data contains', data.length, 'bytes');

    const distributionListAccount = distributionAccountKeyPair;

    // split our data across a number of calls

    const remainingByteCount = 512;

    const dataByteCount =
      remainingByteCount - (CHUNKNUM_BYTE_COUNT + CHUNKSIZE_BYTE_COUNT);

    const numBytes = data.length;
    const chunks = makeChunks.makeChunks(data, dataByteCount);
    console.log('CHUNKS LENGTH', chunks.length);

    console.log('Data will be uploaded over', chunks.length, 'calls');

    const { feeCalculator } = await connection.getRecentBlockhash();
    const costOfAccountRent =
      await connection.getMinimumBalanceForRentExemption(numBytes);

    const costOfAccountTran = 2 * feeCalculator.lamportsPerSignature;
    let costOfAccountUpload = 0;

    for (let i = 0; i < chunks.length; i++) {
      const chunkCost = 2 * feeCalculator.lamportsPerSignature; // two signers, the payer and the new account
      costOfAccountUpload += chunkCost;
    }
    console.log({ costOfAccountUpload });
    const total = costOfAccountRent + costOfAccountTran + costOfAccountUpload;

    console.log(
      'EST FINAL TOTAL -- ',
      total,
      'lamports (',
      total / LAMPORTS_PER_SOL,
      ')',
    );

    const bal = await connection.getBalance(new PublicKey(MAIN_ACCOUNT_PUBKEY));

    if (bal < total) {
      console.error(
        'WARNING, your balance',
        bal,
        'is less than the estimate',
        total,
      );
      return null;
    }

    await this.sleep(1000); //To make sure the above account is created before uploading data

    const balBeforeUpload = await connection.getBalance(
      new PublicKey(MAIN_ACCOUNT_PUBKEY),
    );

    for (let i = 0; i < chunks.length; i++) {
      const data = chunks[i];
      const dataStructureU16 = BufferLayout.u16();
      const buffChunkNum = Buffer.alloc(CHUNKNUM_BYTE_COUNT);
      dataStructureU16.encode(i, buffChunkNum);

      const buffChunkSize = Buffer.alloc(CHUNKSIZE_BYTE_COUNT);
      dataStructureU16.encode(dataByteCount, buffChunkSize);

      let instruction_data = Buffer.concat([buffChunkNum, buffChunkSize, data]);
      const len = instruction_data.length;
      if (len < 512) {
        // Instruction data must be of 512 bytes so padding with null buffer
        const rem = 512 - len;
        const a = Buffer.from(new Array(rem).fill(0));
        instruction_data = Buffer.concat([instruction_data, a]);
      }
      const encodedTransactionInstruction = encodeData.encodeData(
        TASK_INSTRUCTION_LAYOUTS.UploadDistributionList,

        {
          instruction_data: instruction_data,
          round: round,
        },
      );

      const instruction = new TransactionInstruction({
        keys: [
          {
            pubkey: new PublicKey(TASK_ID) /* */,
            isSigner: false,
            isWritable: false,
          },
          {
            pubkey: distributionListAccount.publicKey,
            isSigner: true,
            isWritable: true,
          },
        ], // add acc here after pk
        programId: new PublicKey(DEFAULT_PROGRAM_ID),
        data: encodedTransactionInstruction,
      });

      const transaction = new Transaction().add(instruction);
      await this.sendAndConfirmTransactionWrapper(transaction, [
        distributionListAccount,
      ]);
      //console.log('CALLED AFTER TRANSACTION');
      await this.sleep(100);
    }

    const balAfterUpload = await connection.getBalance(
      new PublicKey(MAIN_ACCOUNT_PUBKEY),
    );
    const costOfUpload = balBeforeUpload - balAfterUpload;
    console.log(
      'Total cost was',
      costOfUpload,
      'lamports (',
      costOfUpload / LAMPORTS_PER_SOL,
      ')',
    );
    // TODO: Verify Retry Failed Transactions (logic changed)

    console.log('ROUND VY', round);
    await this.sleep(10000);
    return await this.validateUpload(distributionListAccount.publicKey, round);
  }

  async validateUpload(storageWalletKey, round) {
    let accountInfo = await connection.getAccountInfo(
      new PublicKey(storageWalletKey),
    );
    if (!accountInfo) {
      console.log(`${publicKey} doesn't contain any distribution list data`);
      return false;
    }
    const d = JSON.parse(accountInfo.data + '');
    console.log({ d });
    const i = Buffer.from(d[round][TASK_ID]).indexOf(0x00);
    const t = Buffer.from(d[round][TASK_ID]).slice(0, i);

    const origData = new TextDecoder().decode(t);
    try {
      const b = JSON.parse(origData);
      console.log('Data received from K2', b);
    } catch (err) {
      console.log('ERROR', err);
      return false;
    }
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async fetchUploadedstorage() {
    let accountInfo = null;
    try {
      accountInfo = await connection.getAccountInfo(
        new PublicKey(await this.getStorageWallet()),
      );
      if (!accountInfo) {
        console.log(`${publicKey} doesn't contain any distribution list data`);
        return false;
      }
      const d = JSON.parse(accountInfo.data + '');
      console.log({ d });
      return true;
    } catch (e) {
      console.error(e);
      return false;
    }
  }

  async distributionListSubmissionOnChain(round) {
    if (taskNodeAdministered) {
      return await genericHandler('distributionListSubmissionOnChain', round);
    } else {
      if (!this.#testingTaskState.distribution_rewards_submission[round])
        this.#testingTaskState.distribution_rewards_submission[round] = {};

      this.#testingTaskState.distribution_rewards_submission[round][
        this.#testingStakingSystemAccount.publicKey.toBase58()
      ] = {
        submission_value:
          this.#testingStakingSystemAccount.publicKey.toBase58(),
        slot: 200,
        round: 1,
      };
    }
  }

  async checkSubmissionAndUpdateRound(submissionValue = 'default', round) {
    if (taskNodeAdministered) {
      return await genericHandler(
        'checkSubmissionAndUpdateRound',
        submissionValue,
        round,
      );
    } else {
      if (!this.#testingTaskState.submissions[round])
        this.#testingTaskState.submissions[round] = {};
      this.#testingTaskState.submissions[round][
        this.#testingStakingSystemAccount.publicKey.toBase58()
      ] = {
        submission_value: submissionValue,
        slot: 100,
        round: 1,
      };
    }
  }
  async getProgramAccounts() {
    if (taskNodeAdministered) {
      return await genericHandler('getProgramAccounts');
    } else {
      console.log('Cannot call getProgramAccounts in testing mode');
    }
  }
  async defaultTaskSetup() {
    if (taskNodeAdministered) {
      return await genericHandler('defaultTaskSetup');
    } else {
      if (this.#testingTaskState) return;
      this.#testingMainSystemAccount = new Keypair();
      this.#testingStakingSystemAccount = new Keypair();
      this.#testingDistributionList = {};
      this.#testingTaskState = {
        task_name: 'DummyTestState',
        task_description: 'Dummy Task state for testing flow',
        submissions: {},
        submissions_audit_trigger: {},
        total_bounty_amount: 10000000000,
        bounty_amount_per_round: 1000000000,
        total_stake_amount: 50000000000,
        minimum_stake_amount: 5000000000,
        available_balances: {},
        stake_list: {},
        round_time: 600,
        starting_slot: 0,
        audit_window: 200,
        submission_window: 200,
        distribution_rewards_submission: {},
        distributions_audit_trigger: {},
      };
    }
  }
  async getRpcUrl() {
    if (taskNodeAdministered) {
      return await genericHandler('getRpcUrl');
    } else {
      console.log('Cannot call getNodes in testing mode');
    }
  }
  async getNodes(url) {
    if (taskNodeAdministered) {
      return await genericHandler('getNodes', url);
    } else {
      console.log('Cannot call getNodes in testing mode');
    }
  }

  // Wrapper for selection of node to prepare a distribution list

  async nodeSelectionDistributionList(round) {
    return await genericHandler('nodeSelectionDistributionList', round);
  }

  async getDistributionList(publicKey, round) {
    if (taskNodeAdministered) {
      const response = await genericHandler(
        'getDistributionList',
        publicKey,
        round,
      );
      if (response.error) {
        return null;
      }
      return response;
    } else {
      const submissionValAcc =
        this.#testingTaskState.distribution_rewards_submission[round][
          this.#testingStakingSystemAccount.publicKey.toBase58()
        ].submission_value;
      return this.#testingDistributionList[round][submissionValAcc];
    }
  }

  async validateAndVoteOnNodes(validate, round) {
    console.log('******/  IN VOTING /******');
    const taskAccountDataJSON = await this.getTaskState();

    // console.log(
    //   `Fetching the submissions of round ${round}`,
    //   taskAccountDataJSON.submissions[round],
    // );
    const submissions = taskAccountDataJSON.submissions[round];
    if (submissions == null) {
      console.log(`No submisssions found in round ${round}`);
      return `No submisssions found in round ${round}`;
    } else {
      const keys = Object.keys(submissions);
      const values = Object.values(submissions);
      const size = values.length;
      const numberOfChecks = Math.min(5, size);

      let uniqueIndices = new Set();

      // Populate uniqueIndices with unique random numbers
      while (uniqueIndices.size < numberOfChecks) {
        const randomIndex = Math.floor(Math.random() * size);
        uniqueIndices.add(randomIndex);
      }

      let isValid;
      const submitterAccountKeyPair = await this.getSubmitterAccount();
      const submitterPubkey = submitterAccountKeyPair.publicKey.toBase58();
      for (let index of uniqueIndices) {
        let candidatePublicKey = keys[index];
        console.log('FOR CANDIDATE KEY', candidatePublicKey);
        let candidateKeyPairPublicKey = new PublicKey(keys[index]);
        if (candidatePublicKey == submitterPubkey) {
          console.log('YOU CANNOT VOTE ON YOUR OWN SUBMISSIONS');
        } else {
          try {
            // console.log(
            //   'SUBMISSION VALUE TO CHECK',
            //   values[index].submission_value,
            // );
            isValid = await validate(values[index].submission_value, round);
            console.log(`Voting ${isValid} to ${candidatePublicKey}`);

            if (isValid) {
              // check for the submissions_audit_trigger , if it exists then vote true on that otherwise do nothing
              const submissions_audit_trigger =
                taskAccountDataJSON.submissions_audit_trigger[round];
              console.log('SUBMIT AUDIT TRIGGER', submissions_audit_trigger);
              // console.log(
              //   "CANDIDATE PUBKEY CHECK IN AUDIT TRIGGER",
              //   submissions_audit_trigger[candidatePublicKey]
              // );
              if (
                submissions_audit_trigger &&
                submissions_audit_trigger[candidatePublicKey]
              ) {
                console.log('VOTING TRUE ON AUDIT');
                const response = await this.auditSubmission(
                  candidateKeyPairPublicKey,
                  isValid,
                  submitterAccountKeyPair,
                  round,
                );
                console.log('RESPONSE FROM AUDIT FUNCTION', response);
              }
            } else if (isValid == false) {
              // Call auditSubmission function and isValid is passed as false
              console.log('RAISING AUDIT / VOTING FALSE');
              const response = await this.auditSubmission(
                candidateKeyPairPublicKey,
                isValid,
                submitterAccountKeyPair,
                round,
              );
              console.log('RESPONSE FROM AUDIT FUNCTION', response);
            }
          } catch (err) {
            console.log('ERROR IN ELSE CONDITION', err);
          }
        }
      }
    }
  }

  async validateAndVoteOnDistributionList(validateDistribution, round) {
    // await this.checkVoteStatus();
    console.log('******/  IN VOTING OF DISTRIBUTION LIST /******');
    try {
      const taskAccountDataJSON = await this.getTaskState();
      console.log(
        `Fetching the Distribution submissions of round ${round}`,
        taskAccountDataJSON.distribution_rewards_submission[round],
      );
      const submissions =
        taskAccountDataJSON.distribution_rewards_submission[round];
      if (submissions == null) {
        console.log(`No submisssions found in round ${round}`);
        return `No submisssions found in round ${round}`;
      } else {
        const keys = Object.keys(submissions);
        const values = Object.values(submissions);
        const size = values.length;
        console.log(
          'Distribution Submissions from last round: ',
          keys,
          values,
          size,
        );
        let isValid;
        const submitterAccountKeyPair = await this.getSubmitterAccount();
        const submitterPubkey = submitterAccountKeyPair.publicKey.toBase58();

        for (let i = 0; i < size; i++) {
          let candidatePublicKey = keys[i];
          console.log('FOR CANDIDATE KEY', candidatePublicKey);
          let candidateKeyPairPublicKey = new PublicKey(keys[i]);
          if (candidatePublicKey == submitterPubkey) {
            console.log('YOU CANNOT VOTE ON YOUR OWN DISTRIBUTION SUBMISSIONS');
          } else {
            try {
              console.log(
                'DISTRIBUTION SUBMISSION VALUE TO CHECK',
                values[i].submission_value,
              );
              isValid = await validateDistribution(
                values[i].submission_value,
                round,
              );
              console.log(`Voting ${isValid} to ${candidatePublicKey}`);

              if (isValid) {
                // check for the submissions_audit_trigger , if it exists then vote true on that otherwise do nothing
                const distributions_audit_trigger =
                  taskAccountDataJSON.distributions_audit_trigger[round];
                console.log(
                  'SUBMIT DISTRIBUTION AUDIT TRIGGER',
                  distributions_audit_trigger,
                );
                // console.log(
                //   "CANDIDATE PUBKEY CHECK IN AUDIT TRIGGER",
                //   distributions_audit_trigger[candidatePublicKey]
                // );
                if (
                  distributions_audit_trigger &&
                  distributions_audit_trigger[candidatePublicKey]
                ) {
                  console.log('VOTING TRUE ON DISTRIBUTION AUDIT');
                  const response = await this.distributionListAuditSubmission(
                    candidateKeyPairPublicKey,
                    isValid,
                    submitterAccountKeyPair,
                    round,
                  );
                  console.log(
                    'RESPONSE FROM DISTRIBUTION AUDIT FUNCTION',
                    response,
                  );
                }
              } else if (isValid == false) {
                // Call auditSubmission function and isValid is passed as false
                console.log('RAISING AUDIT / VOTING FALSE ON DISTRIBUTION');
                const response = await this.distributionListAuditSubmission(
                  candidateKeyPairPublicKey,
                  isValid,
                  submitterAccountKeyPair,
                  round,
                );
                console.log(
                  'RESPONSE FROM DISTRIBUTION AUDIT FUNCTION',
                  response,
                );
              }
            } catch (err) {
              console.log('ERROR IN ELSE CONDITION FOR DISTRIBUTION', err);
            }
          }
        }
      }
    } catch (error) {
      console.log('ERROR IN validateAndVoteOnDistributionList', err);
    }
  }
  async getTaskLevelDBPath() {
    if (taskNodeAdministered) {
      return await genericHandler('getTaskLevelDBPath');
    } else {
      return './KOIIDB';
    }
  }
  async getBasePath() {
    if (taskNodeAdministered) {
      const basePath = (await namespaceWrapper.getTaskLevelDBPath()).replace(
        '/KOIIDB',
        '',
      );
      return basePath;
    } else {
      return './';
    }
  }
  getMainAccountPubkey() {
    if (taskNodeAdministered) {
      return MAIN_ACCOUNT_PUBKEY;
    } else {
      return this.#testingMainSystemAccount.publicKey.toBase58();
    }
  }
}

async function genericHandler(...args) {
  try {
    let response = await axios.post(BASE_ROOT_URL, {
      args,
      taskId: TASK_ID,
      secret: SECRET_KEY,
    });
    if (response.status == 200) return response.data.response;
    else {
      console.error(response.status, response.data);
      return null;
    }
  } catch (err) {
    console.error(`Error in genericHandler: "${args[0]}"`, err.message);
    console.error(err?.response?.data);
    return { error: err };
  }
}
let connection;
const namespaceWrapper = new NamespaceWrapper();
if (taskNodeAdministered) {
  namespaceWrapper.getRpcUrl().then(rpcUrl => {
    console.log(rpcUrl, 'RPC URL');
    connection = new Connection(rpcUrl, 'confirmed');
  });
}
module.exports = {
  namespaceWrapper,
  taskNodeAdministered, // Boolean flag indicating that the task is being ran in active mode (Task node supervised), or development (testing) mode
  app, // The initialized express app to be used to register endpoints
  TASK_ID, // This will be the PORT on which the this task is expected to run the express server coming from the task node running this task. As all communication via the task node and this task will be done on this port.
  MAIN_ACCOUNT_PUBKEY, // This will be the secret used to authenticate with task node running this task.
  SECRET_KEY, // This will be the secret used by the task to authenticate with task node running this task.
  K2_NODE_URL, // This will be K2 url being used by the task node, possible values are 'https://k2-testnet.koii.live' | 'https://k2-devnet.koii.live' | 'http://localhost:8899'
  SERVICE_URL, // This will be public task node endpoint (Or local if it doesn't have any) of the task node running this task.
  STAKE, // This will be stake of the task node running this task, can be double checked with the task state and staking public key.
  TASK_NODE_PORT, // This will be the port used by task node as the express server port, so it can be used by the task for the communication with the task node
  _server, // Express server object
};
