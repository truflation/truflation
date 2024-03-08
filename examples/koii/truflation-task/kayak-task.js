const Kayak = require('./adapters/kayak/kayak.js');
const db = require('./helpers/db');
const { Web3Storage } = require('web3.storage');
const Data = require('./model/data');
const dotenv = require('dotenv');
const { default: axios } = require('axios');
dotenv.config();
const cron = require('node-cron');
const moment = require('moment');

/**
 * KayakTask is a class that handles the Kayak crawler and validator
 *
 * @description KayakTask is a class that handles the Kayak crawler and validator
 *              In this task, the crawler asynchronously populates a database, which is later
 *              read by the validator. The validator then uses the database to prepare a submission CID
 *              for the current round, and submits that for rewards.
 *
 *              Four main functions control this process:
 *              @crawl crawls Kayak and populates the database
 *              @validate verifies the submissions of other nodes
 *              @getRoundCID returns the submission for a given round
 *              @stop stops the crawler
 *
 * @param {function} getRound - a function that returns the current round
 * @param {number} round - the current round
 * @param {string} searchTerm - the search term to use for the crawler
 * @param {string} adapter - the adapter to use for the crawler
 * @param {string} db - the database to use for the crawler
 *
 * @returns {KayakTask} - a KayakTask object
 *
 */

class KayakTask {
  constructor(getRound, round) {
    this.round = round;
    this.lastRoundCheck = Date.now();
    this.isRunning = false;
    this.adapter = null;
    this.setAdapter = async () => {
      this.adapter = new Kayak(this.db);
      await this.adapter.negotiateSession();
    };
    this.cronJob = null;
    this.timeoutInterval = null;
    this.updateRound = async () => {
      // if it has been more than 1 minute since the last round check, check the round and update this.round
      if (Date.now() - this.lastRoundCheck > 60000) {
        this.round = await getRound();
        this.lastRoundCheck = Date.now();
      }
      return this.round;
    };
    this.start();
  }

  /**
   * strat
   * @description starts the crawler
   *
   * @returns {void}
   *
   */
  async start() {
    await this.setAdapter();

    this.isRunning = true;
    console.log('starting crawler');
    const locationURLs = (
      await this.adapter.locationDb.getItem({ id: 'locationData' })
    )[0].data;

    console.log('locationURLs', locationURLs);
    this.startCronOnEvenMinute(locationURLs);
  }

  /**
   * crawlLocations
   * @description crawls Kayak and populates the database
   * @param {*} locationURLs
   * @returns
   */
  async crawlLocations(locationURLs, currentMinute) {
    let locationSummary = [];
    // console.log('crawlMinute', currentMinute);
    for (let i = 0; i < locationURLs.length; i++) {
      let url = locationURLs[i].href;
      console.log('starting crawl for', url);
      await this.sleep(2000);
      const dataForLocation = await this.adapter.crawl(url);
      const locationSet = {
        location: locationURLs[i].text,
        data: dataForLocation,
      };
      locationSummary.push(locationSet);
    }
    const oneCrawlData = {
      timestamp: new Date().getTime(),
      locationSummary: locationSummary,
    };

    // console.log('oneCrawlData', oneCrawlData);
    await this.adapter.locationDb.storeOneScrape(oneCrawlData);
    console.log('storing scrape on IPFS');
    await this.adapter.storeScrapeOnIPFS(oneCrawlData);
    // const testing = await this.adapter.locationDb.getListing({round: 574});
    // console.log("Testing the fetch of ******",testing);
    // await this.adapter.cids.putItem(oneCrawlData);
  }

  /**
   * stop
   * @description stops the crawler
   *
   * @returns {void}
   */
  async stop() {
    this.isRunning = false;
    this.adapter.stop();
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * getRoundCID
   * @param {*} roundID
   * @returns
   */
  async getRoundCID(roundID) {
    console.log('starting submission prep for ');
    let result = await this.adapter.getSubmissionCID(roundID);
    console.log('returning round CID', result, 'for round', roundID);
    return result;
  }

  /**
   * getJSONofCID
   * @description gets the JSON of a CID
   * @param {*} cid
   * @returns
   */
  async getJSONofCID(cid) {
    return await getJSONFromCID(cid);
  }

  /**
   * validate
   * @description validates a round of results from another node against the Kayak API
   * @param {*} proofCid
   * @returns
   */
  async validate(proofCid) {
    // in order to validate, we need to take the proofCid
    // and go get the results from web3.storage

    let data = await getJSONFromCID(proofCid); // check this
    // console.log(`validate got results for CID: ${ proofCid } for round ${ roundID }`, data, typeof(data), data[0]);

    // the data submitted should be an array of additional CIDs for individual tweets, so we'll try to parse it

    let proofThreshold = 4; // an arbitrary number of records to check

    for (let i = 0; i < proofThreshold; i++) {
      let randomIndex = Math.floor(Math.random() * data.length);
      let item = data[randomIndex];
      let result = await getJSONFromCID(item.cid);

      // then, we need to compare the CID result to the actual result on Kayak
      // i.e.
      console.log('item was', item);
      if (item.id) {
        // TODO - revise this check to make sure it handles issues with type conversions
        console.log('ipfs', item);
        let ipfsCheck = await this.getJSONofCID(item.cid);
        console.log('ipfsCheck', ipfsCheck);
        if (ipfsCheck.id) {
          console.log('ipfs check passed');
        }
        return true;
      } else {
        console.log('invalid item id', item.id);
        return false;
      }
    }

    // if none of the random checks fail, return true
    return true;
  }

  scheduleCron(locationURLs) {
    if (this.cronJob) {
      this.cronJob.stop();
      clearTimeout(this.timeoutInterval);
    }

    this.cronJob = cron.schedule(
      '*/10 * * * *',
      async () => {
        const currentMinute = parseInt(moment.utc().format('m'), 10);
        if (currentMinute % 10 === 0) {
          // Your desired cron task to be executed here
          console.log('Cron job started!');
          await this.crawlLocations(locationURLs, currentMinute);
        }
      },
      {
        timezone: 'UTC',
      },
    );
  }

  startCronOnEvenMinute(locationURLs) {
    const currentMinute = parseInt(moment().format('m'), 10);
    const cronMinute = Math.ceil(currentMinute / 10) * 10; // Round up the current minute to the nearest multiple of 10
    const nextCronTime = moment()
      .minute(cronMinute)
      .add(cronMinute <= currentMinute ? 10 : 0, 'minutes'); // If current minute is already a multiple of 10, add 10 minutes

    const waitTime = nextCronTime.diff(moment(), 'milliseconds');
    console.log(
      `Waiting ${waitTime} milliseconds for next even minute to start cron job`,
    );
    this.timeoutInterval = setTimeout(() => {
      this.scheduleCron(locationURLs);
    }, waitTime);
  }
}

module.exports = KayakTask;

/**
 * getJSONFromCID
 * @description gets the JSON from a CID
 * @param {*} cid
 * @returns promise<JSON>
 */
const getJSONFromCID = async cid => {
  return new Promise((resolve, reject) => {
    let url = `https://${cid}.ipfs.dweb.link/data.json`;
    // console.log('making call to ', url)
    axios.get(url).then(response => {
      if (response.status !== 200) {
        console.log('error', response);
        reject(response);
      } else {
        // console.log('response', response)
        resolve(response.data);
      }
    });
  });
};
