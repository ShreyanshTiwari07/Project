const { MongoClient } = require('mongodb');
const { mongoConfig } = require('./config');

let client;
let db;

/**
 * Connects to the MongoDB database.
 * @returns {Promise<void>} - A promise that resolves when the connection is established.
 */
async function connect() {
  try {
    client = await MongoClient.connect(mongoConfig.mongoUrl);
    db = client.db(mongoConfig.dbName);
    console.log('Db: Connected to MongoDB...');
  } catch (error) {
    console.error('Error occurred while connecting to the database:', error);
    throw error;
  }
}

/**
 * Closes the connection to the MongoDB database.
 */
function close() {
  if (client) {
    client.close();
    client = null;
    db = null;
    console.log('Db: Disconnected from MongoDB...');
  }
}
async function dropCollection(collectionName) {
  try {
    const collections = await db.listCollections().toArray();
    const collectionExists = collections.some(collection => collection.name === collectionName);

    if (collectionExists) {
      const collection = db.collection(collectionName);
      await collection.drop();
      console.log(`Collection ${collectionName} dropped successfully.`);
    } else {
      console.log(`Collection ${collectionName} does not exist.`);
    }
  } catch (error) {
    console.error(`Error dropping collection ${collectionName}:`, error);
    throw error;
  }
}

/**
 * Retrieves the last processed block number for a specific date.
 * @param {string} date - The date for which to retrieve the last processed block number.
 * @returns {Promise<number>} - A promise that resolves with the last processed block number.
 */
async function getLastProcessedEndBlockNumber(date) {
  try {
    const collection = db.collection('lastProcessedBlocks');
    const lastProcessedBlock = await collection.findOne({ date });
    return lastProcessedBlock ? lastProcessedBlock.rangeEnd : 0;
  } catch (error) {
    console.error(`Error retrieving last processed block number for date ${date}:`, error);
    return 0;
  }
}
/**
 * Retrieves the last processed block number for a specific date.
 * @param {string} date - The date for which to retrieve the last processed block number.
 * @returns {Promise<number>} - A promise that resolves with the last processed block number.
 */
async function getLastProcessedStartBlockNumber(date) {
  try {
    const collection = db.collection('lastProcessedBlocks');
    const lastProcessedBlock = await collection.findOne({ date });
    return lastProcessedBlock ? lastProcessedBlock.rangeStart : 0;
  } catch (error) {
    console.error(`Error retrieving last processed block number for date ${date}:`, error);
    return 0;
  }
}

/**
 * Updates the last processed block number for a specific date.
 * @param {string} date - The date for which to update the last processed block number.
 * @param {number} lastProcessedBlock - The last processed block number.
 * @returns {Promise<void>} - A promise that resolves when the last processed block number is updated.
 */
async function updateLastProcessedBlockNumber(date, lastProcessedStartBlock, lastProcessedEndBlock) {
  try {
    const collection = db.collection('lastProcessedBlocks');
    const existingDocument = await collection.findOne({ date });

    if (existingDocument) {
      await collection.updateOne(
        { date },
        { $set: { rangeStart: lastProcessedStartBlock, rangeEnd: lastProcessedEndBlock } }
      );
    } else {
      await collection.insertOne({
        date,
        rangeStart: lastProcessedStartBlock,
        rangeEnd: lastProcessedEndBlock
      });
    }
  } catch (error) {
    console.error(`Error updating last processed block number for date ${date}:`, error);
    throw error;
  }
}



/**
 * Retrieves the last processed date.
 * @returns {Promise<string|null>} - A promise that resolves with the last processed date if found, or null if not found.
 */
async function getLastProcessedDate() {
  try {
    const collection = db.collection('lastProcessedBlocks');
    const lastProcessedBlock = await collection.findOne({}, { sort: { date: -1 } });
    return lastProcessedBlock ? lastProcessedBlock.date : null;
  } catch (error) {
    console.error('Error occurred while retrieving last processed date:', error);
    return null;
  }
}

/**
 * Performs bulk write operations for transactions.
 * @param {Array} operations - The array of bulk write operations.
 * @param {string} collectionName - The name of the collection to perform the operations on.
 * @returns {Promise<void>} - A promise that resolves when the bulk write operations are executed.
 */
async function bulkWriteTransactions(operations, collectionName) {
  try {
    const collection = db.collection(collectionName);
    const result = await collection.bulkWrite(operations);
    // console.log('Bulk write operations completed:', result);
  } catch (error) {
    console.error('Error occurred during bulk write operations:', error);
    throw error;
  }
}

/**
 * Retrieves a transaction by transaction hash.
 * @param {string} transactionHash - The transaction hash.
 * @param {string} collectionName - The name of the collection to perform the operation on.
 * @returns {Promise<Object|null>} - A promise that resolves with the transaction object if found, or null if not found.
 */
async function getTransaction(transactionHash, collectionName) {
  try {
    const collection = db.collection(collectionName);
    const transaction = await collection.findOne({ transactionHash });
    return transaction;
  } catch (error) {
    console.error('Error occurred while retrieving transaction:', error);
    throw error;
  }
}

/**
 * Updates transactions in bulk by transaction hash.
 * @param {Array} updateOperations - The array of update operations.
 * @param {string} collectionName - The name of the collection to perform the operations on.
 * @returns {Promise<void>} - A promise that resolves when the bulk update operations are executed.
 */
async function bulkUpdateTransactions(updateOperations, collectionName) {
  try {
    const collection = db.collection(collectionName);
    const bulkWriteOperations = updateOperations.map(({ transactionHash, ms, tokenId }) => ({
      updateOne: {
        filter: { transactionHash },
        update: { $set: { ms }, $addToSet: { tokenId } },
      },
    }));
    await collection.bulkWrite(bulkWriteOperations);
  } catch (error) {
    console.error('Error occurred during bulk update operations:', error);
    throw error;
  }
}

/**
 * Inserts transactions in bulk.
 * @param {Array} insertDocuments - The array of documents to insert.
 * @param {string} collectionName - The name of the collection to perform the operations on.
 * @returns {Promise<void>} - A promise that resolves when the bulk insert operations are executed.
 */
async function bulkInsertTransactions(insertDocuments, collectionName) {
  try {
    const collection = db.collection(collectionName);
    const bulkWriteOperations = insertDocuments.map((document) => ({
      insertOne: {
        document,
      },
    }));
    await collection.bulkWrite(bulkWriteOperations);
  } catch (error) {
    console.error('Error occurred during bulk insert operations:', error);
    throw error;
  }
}

module.exports = {
  connect,
  close,
  bulkWriteTransactions,
  getTransaction,
  bulkUpdateTransactions,
  bulkInsertTransactions,
  getLastProcessedEndBlockNumber,
  getLastProcessedStartBlockNumber,
  updateLastProcessedBlockNumber,
  getLastProcessedDate,
  dropCollection
};