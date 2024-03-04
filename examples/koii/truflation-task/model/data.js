const { namespaceWrapper } = require('../_koiiNode/koiiNode');

/**
 * Data class
 *
 * @param {string} name - the name of the database
 * @param {object} data - the initial data to be stored in the database
 *
 * @returns {Data} - a Data object
 *
 */

class Data {
  constructor(name, data) {
    this.name = name;
    this.data = data;
    this.dbprefix = `${name} + ":"`;
    this.fullList = [];
    this.lastUpdate = Date.now();
  }

  /**
   * initializeData
   * @returns {void}
   */
  async initializeData() {
    if (this.db) return;
    const db = await namespaceWrapper.getDb();
    this.db = db;
  }

  async getRound() {
    const round = await namespaceWrapper.getRound();
    return round;
  }

  /**
   * create
   * @param {*} item
   * @returns {void}
   */
  async create(item) {
    try {
      const existingItem = await this.getItem(item);
      // console.log('get item', existingItem);

      if (existingItem) {
        if (
          !existingItem[0].timestamp ||
          (item.timestamp && item.timestamp > existingItem[0].timestamp)
        ) {
          // Remove the old item with the same ID
          await this.db.remove({ id: item.id }, {});
          // console.log('Old item removed');
          this.db.compactDatafile();
        } else {
          console.log('New item has a lower or equal timestamp; ignoring');
          return undefined;
        }
      }

      await this.db.insert(item);
      // console.log('Item inserted', item);
    } catch (e) {
      console.error(e);
      return undefined;
    }
  }

  /**
   * getItem
   * @param {*} item
   * @returns
   * @description gets an item from the database by ID (CID)
   */
  async getItem(item) {
    // console.log('trying to retrieve with ID', item.id);
    try {
      const resp = await this.db.find({ id: item.id });
      // console.log('resp is ', resp);
      if (resp.length !== 0) {
        return resp;
      } else {
        return null;
      }
    } catch (e) {
      console.error(e);
      return null;
    }
  }

  async storeOneScrape(data) {
    const round = await this.getRound();
    const item = {
      id: 'scrape:' + round + ':' + data.timestamp,
      data: data,
    };
    await this.create(item);
    const searchPattern = `scrape:${round}:`;

    // Construct the regular expression dynamically
    const regexPattern = new RegExp(`^${searchPattern}`);
    const itemListRaw = await this.db.find({ id: regexPattern });
    console.log('itemListRaw', itemListRaw);
  }

  /**
   * getList
   * @param {*} options
   * @returns
   * @description gets a list of items from the database by ID (CID)
   * or by round
   */
  async getList(options) {
    // doesn't support options or rounds yet?
    let itemListRaw;
    console.log('has round', options.round);
    const searchPattern = `scrapeCid:${options.round}:`;

    // Construct the regular expression dynamically
    const regexPattern = new RegExp(`^${searchPattern}`);
    itemListRaw = await this.db.find({ id: regexPattern });
    console.log('itemListRaw', itemListRaw);
    return itemListRaw;
  }
}

module.exports = Data;
