const EthDater = require('./BlockByDate');
const { Web3 } = require('web3');
const fs = require('fs');
const { MongoClient } = require('mongodb');
const Promise = require('bluebird');

//Custom Classes 
const { getWeb3Instance } = require('./web3Instance');
const logger = require('./log.js');
const marketplaceEventMappings = require('./marketplaceEventMappings.js');
const db = require('./database.js');
const TransactionHelper = require('./TransactionHelper');



// Maximum batch size for bulk write operations
const MAX_BATCH_SIZE = 3000;

// Example usage
logger.setDebugFlag(true);


// Instantiate Web3 using HTTP provider
//const web3Http = getWeb3Instance('http');
// Use `web3Http` for HTTP-related functionality

// Instantiate Web3 using WebSocket (WSS) provider
const web3 = getWeb3Instance('ws');
// Use `web3Ws` for WebSocket-related functionality



/**
 * Scans and calculates the total count of ERC721 and ERC1155 transactions within the specified block range using parallelized requests.
 * @param {BigInt} startBlockNumber - The start block number.
 * @param {BigInt} endBlockNumber - The end block number.
 * @returns {Promise<number>} - A promise that resolves with the total count of NFT transactions.
 */
async function scanForERC721AndERC1155Transactions(startBlockNumber, endBlockNumber) {
  logger.log(`Checking transaction count from block ${startBlockNumber} to ${endBlockNumber}`, 'info');

  // Convert startBlockNumber and endBlockNumber to numbers
  const startBlock = Number(startBlockNumber);
  const endBlock = Number(endBlockNumber);

  const blockNumbers = Array.from({ length: endBlock - startBlock + 1 }, (_, i) => i + startBlock);
  logger.log(`Total block numbers: ${blockNumbers.length}`, 'info');

  const blockSplitRange = 100;
  logger.log(`Block split range: ${blockSplitRange}`, 'info');
  const concurrencyCount = 5;
  logger.log(`Concurrency count: ${concurrencyCount}`, 'info');

  let totalErc721Transactions = 0;
  let totalErc1155TransactionsSingle = 0;
  let totalErc1155TransactionsBatch = 0;

  try {
    // Split blockNumbers into smaller ranges of 100 blocks or less
    const blockRanges = splitIntoRanges(blockNumbers, blockSplitRange);

    // Process block ranges in parallel with a concurrency limit
    await Promise.map(blockRanges, async (range) => {
      const bulkOperations = []; // Create a new bulkOperations array for each block range

      for (const blockNumber of range) {
        totalErc721Transactions += await processBlockForErc721Transactions(blockNumber, true, bulkOperations);
        totalErc1155TransactionsSingle += await processBlockForErc1155SingleTransfer(blockNumber, true, bulkOperations);
        totalErc1155TransactionsBatch += await processBlockForErc1155BatchTransfer(blockNumber, true, bulkOperations);
      }

      // Execute the final bulk write operation for the current block range's operations
      if (bulkOperations.length > 0) {
        await db.bulkWriteTransactions(bulkOperations);
        bulkOperations.length = 0; // Reset bulkOperations to zero
      }
    }, { concurrency: concurrencyCount });

    logger.log(`Total ERC721 transactions: ${totalErc721Transactions}`, 'info');
    logger.log(`Total ERC1155 batch transfer transactions: ${totalErc1155TransactionsBatch}`, 'info');
    logger.log(`Total ERC1155 Single transfer transactions: ${totalErc1155TransactionsSingle}`, 'info');

    const totalRawTransactions = totalErc721Transactions + totalErc1155TransactionsSingle + totalErc1155TransactionsBatch;
    logger.log(`Total NFT transactions: ${totalRawTransactions}`, 'info');
    return totalRawTransactions;
  } catch (error) {
    logger.log(`Error fetching transactions: ${error}`, 'error');
    console.log(`Error fetching transactions: `, error);
    throw error;
  }
}

/**
 * Splits an array into smaller ranges of specified size.
 * @param {Array} array - The array to split.
 * @param {number} size - The maximum size of each range.
 * @returns {Array} Array of ranges.
 */
function splitIntoRanges(array, size) {
  const ranges = [];
  for (let i = 0; i < array.length; i += size) {
    ranges.push(array.slice(i, i + size));
  }
  return ranges;
}
/**
 * Processes a single block to filter ERC1155 single transfer events and returns the count.
 * @param {number} blockNumber - The block number to process.
 * @param {boolean} writeToDatabase - Flag to indicate whether to write to the database or not.
 * @returns {Promise<number>} - A promise that resolves with the count of ERC1155 single transfer transactions.
 */
async function processBlockForErc1155SingleTransfer(blockNumber,writeToDatabase,bulkOperations) {
  let erc1155SingleTransferCount = 0;
 
  const options1155Single = {
    topics: [web3.utils.sha3('TransferSingle(address,address,address,uint256,uint256)')],
    fromBlock: web3.utils.toHex(blockNumber),
    toBlock: web3.utils.toHex(blockNumber)
  };

  try {
    const events = await web3.eth.getPastLogs(options1155Single);

    for (const event of events) {
      try {
        const transaction = web3.eth.abi.decodeLog(
          [
            { type: 'address', name: 'operator', indexed: true },
            { type: 'address', name: 'from', indexed: true },
            { type: 'address', name: 'to', indexed: true },
            { type: 'uint256', name: 'id' },
            { type: 'uint256', name: 'value' }
          ],
          event.data,
          event.topics.slice(1)
        );


	    const tokenType = 'ERC1155';
		const transferType = 'single';
		const contract = event.address;
		
		const rawDataObject = {
			"transactionHash": event.transactionHash,
			"blockNumber": event.blockNumber,
			"contract": event.address,
			"from": transaction.from,
			"to": transaction.to,
			"tokenId": [Number(transaction.id)],
            "amount": [Number(transaction.value)],
			"tokentype": 'ERC1155',
			"transfertype": 'single'
		};
		
		erc1155SingleTransferCount++;
		await processTransactionForDatabase(writeToDatabase, rawDataObject, bulkOperations);
       
      } catch (error) {
        console.error('Error decoding log:', error);
      }
    }
  } catch (error) {
    console.error('Error fetching logs:', error);
  }
  
  // Process any remaining bulk write operations
  if (writeToDatabase && bulkOperations.length > 0) {
   // console.time(`Final Batch Write ${bulkOperations.length}`);
    await db.bulkWriteTransactions(bulkOperations);
    //console.timeEnd(`Final Batch Write ${bulkOperations.length}`);
  }
  
  return erc1155SingleTransferCount;
}

/**
 * Processes a single block to filter ERC1155 batch transfer events and returns the count.
 * @param {number} blockNumber - The block number to process.
 * @param {boolean} writeToDatabase - Flag to indicate whether to write to the database or not.
 * @returns {Promise<number>} - A promise that resolves with the count of ERC1155 batch transfer transactions.
 */
async function processBlockForErc1155BatchTransfer(blockNumber,writeToDatabase,bulkOperations) {
  let erc1155BatchTransferCount = 0;
  
  const options1155Batch = {
    topics: [web3.utils.sha3('TransferBatch(address,address,address,uint256[],uint256[])')],
    fromBlock: web3.utils.toHex(blockNumber),
    toBlock: web3.utils.toHex(blockNumber)
  };

  try {
    const events = await web3.eth.getPastLogs(options1155Batch);

    for (const event of events) {
      try {
        const transaction = web3.eth.abi.decodeLog(
          [
            { type: 'address', name: 'operator', indexed: true },
            { type: 'address', name: 'from', indexed: true },
            { type: 'address', name: 'to', indexed: true },
            { type: 'uint256[]', name: 'ids' },
            { type: 'uint256[]', name: 'values' }
          ],
          event.data,
          event.topics.slice(1)
        );

		
		
		const rawDataObject = {
			"transactionHash": event.transactionHash,
			"blockNumber": event.blockNumber,
			"contract": event.address,
			"from": transaction.from,
			"to": transaction.to,
			"tokenId": transaction.ids.map(Number),
			"amount": transaction.values.map(Number),
			"tokentype": 'ERC1155',
			"transfertype": 'batch'
		};
		erc1155BatchTransferCount++;
		await processTransactionForDatabase(writeToDatabase, rawDataObject, bulkOperations);
		
      } catch (error) {
        console.error('Error decoding log:', error);
      }
    }
  } catch (error) {
    console.error('Error fetching logs:', error);
  }

  
  return erc1155BatchTransferCount;
}



/**
 * Processes a single block to filter ERC721 transfer events and returns the count.
 * Accumulates the data objects for bulk write in the provided array if the flag is true.
 * @param {number} blockNumber - The block number to process.
 * @param {boolean} writeToDatabase - Flag to indicate whether to write to the database or not.
 * @param {Array} bulkOperations - The array to accumulate bulk write operations.
 * @returns {Promise<number>} - A promise that resolves with the count of ERC721 transactions.
 */
async function processBlockForErc721Transactions(blockNumber, writeToDatabase, bulkOperations) {
  try {
    let erc721TransactionCount = 0;

    // Retrieve ERC721 transfer events for the specified block number
    const options = {
      topics: [
        web3.utils.sha3('Transfer(address,address,uint256)')
      ],
      fromBlock: web3.utils.toHex(blockNumber),
      toBlock: web3.utils.toHex(blockNumber)
    };

    const events = await web3.eth.getPastLogs(options);

    // Iterate over the events
    for (const event of events) {
      // Check if the event has the expected number of topics
      if (event.topics.length === 4) {
        // Decode the event log data to extract relevant information
        const transaction = web3.eth.abi.decodeLog(
          [
            { type: 'address', name: 'from', indexed: true },
            { type: 'address', name: 'to', indexed: true },
            { type: 'uint256', name: 'tokenId', indexed: true }
          ],
          event.data,
          [event.topics[1], event.topics[2], event.topics[3]]
        );

        // Increment the ERC721 transaction count
        erc721TransactionCount++;

        const rawDataObject = {
          transactionHash: event.transactionHash,
          blockNumber: event.blockNumber,
          contract: event.address,
          from: transaction.from,
          to: transaction.to,
          tokenId: [Number(transaction.tokenId)],
          amount: [],
          tokentype: 'ERC721',
          transfertype: 'Single'
        };
	   //console.log('Raw Data:', rawDataObject); // Log the raw data object for debugging
       await processTransactionForDatabase(writeToDatabase, rawDataObject, bulkOperations);
      }
    }

    return erc721TransactionCount;
  } catch (error) {
    logger.log(`Error processing block for ERC721 transactions: ${error}`, 'error');
    throw error;
  }
}


/**
 * Processes the transaction data for database update or insertion.
 * @param {boolean} writeToDatabase - Indicates whether to write the transaction to the database.
 * @param {Object} rawDataObject - The raw transaction data.
 * @param {BulkWriteOperation[]} bulkOperations - The array to store all bulk write operations.
 * @returns {Promise<void>} - A promise that resolves when the transaction is processed.
 */
async function processTransactionForDatabase(writeToDatabase, rawDataObject, bulkOperations) {
  //logger.log('Entering processTransactionForDatabase', 'info');

  try {
    if (writeToDatabase) {
      //logger.log('Processing transaction for database update or insertion', 'info');
      const processedData = await TransactionHelper.getRawDataObject(rawDataObject);

      // Process the transaction for database update or insertion
      const updateOperation  = prepareBulkWriteOperation(processedData);
      bulkOperations.push(updateOperation );

      // Write bulk operations when the batch size is reached
      if (bulkOperations.length >= MAX_BATCH_SIZE) {
        try {
          //logger.log('Writing bulk transactions to the database', 'info');
          await db.bulkWriteTransactions(bulkOperations);
        } catch (error) {
          logger.log(`Error writing bulk transactions to the database: ${error}`, 'error');
          throw error;
        }
        logger.log('Bulk transactions written to the database', 'info');
        bulkOperations.length = 0; // Reset bulkOperations to zero
      }
    }
  } catch (error) {
    logger.log(`Error processing transaction for database: ${error}`, 'error');
    throw error;
  }

  //logger.log('Exiting processTransactionForDatabase', 'info');
}



/**
 * Processes a transaction for database update or insertion based on its existence in the database.
 * @param {Object} processedData - The processed data object for the transaction.
 * @returns {Object} - The bulk write operation for the transaction.
 */
function prepareBulkWriteOperation(processedData) {
  const { transactionHash, tokenId, ms, amount, ...rest } = processedData;

  // Construct the update operation for the data object
  const updateOperation = {
    updateOne: {
      // Specify the filter to find the document to be updated
      filter: { transactionHash },
      // Specify the modifications to be applied to the document
      update: {
        $push: {
          tokenId: { $each: tokenId },
          ms: { $each: ms },
          amount: { $each: amount }
        },
        $setOnInsert: { transactionHash, duplicate: true, ...rest }
      },
      // Enable upsert to create a new document if no match is found
      upsert: true
    }
  };

  return updateOperation;
}



/**
 * Processes a transaction for database update or insertion based on its existence in the database.
 * @param {Object} processedData - The processed data object for the transaction.
 * @returns {Object} - The bulk write operation for the transaction.
 */
function prepareBulkWriteOperation1(processedData) {
  // Construct the update operation for the data object
  const updateOperation = {
    updateOne: {
      // Specify the filter to find the document to be updated
      filter: { transactionHash: processedData.transactionHash },
      // Specify the modifications to be applied to the document
      update: {
        // Add each element of the amount array to the existing amount field
        $push: { amount: { $each: processedData.amount } },
        // Add each element of the tokenId array to the existing tokenId field
        $push: { tokenId: { $each: processedData.tokenId } },
        // Add each element of the ms array to the existing ms field
        $push: { ms: { $each: processedData.ms } },
        // Set the transactionHash field if the document is being inserted as a new entry
        $setOnInsert: { transactionHash: processedData.transactionHash }
      },
      // Enable upsert to create a new document if no match is found
      upsert: true
    }
  };

  return updateOperation;
}



/**
 * Retrieves transactions from the specified block range.
 * @param {number} startBlockNumber - Start block number.
 * @param {number} endBlockNumber - End block number.
 * @returns {Array} - Array of transactions.
 */
async function checkTransactionCount(startBlockNumber, endBlockNumber) {
  logger.log(`Checking transaction count from block ${startBlockNumber} to ${endBlockNumber}`, 'info', 'checkTransactionCount');

  // Calculate the total number of blocks in the range
  const totalBlocks = endBlockNumber - startBlockNumber + 1;
  logger.log(`Total number of blocks: ${totalBlocks}`, 'info', 'checkTransactionCount');

  // Initialize an empty array to store the transactions
  let transactions = [];
  let totalErc721Transactions = 0;
  let totalErc1155TransactionsSingle = 0;
  let totalErc1155TransactionsBatch = 0;

  // Iterate over the block numbers within the specified range
  for (let blockNumber = startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
    // Retrieve the block information for the current block number
    let block = await web3.eth.getBlock(blockNumber);

    try {
      // Process the block to extract ERC721 transactions
      //logger.log(`Processing block ${blockNumber}...`, 'info', 'checkTransactionCount');
      let erc721TransactionCount = await processBlockForErc721Transactions(blockNumber,false);
	  let erc1155SingleTransactionCount = await processBlockForErc1155SingleTransfer(blockNumber,false);
      let erc1155BatchTransactionCount = await processBlockForErc1155BatchTransfer(blockNumber,false);	  	  
      //logger.log(`Block ${blockNumber} processing completed successfully`, 'info', 'checkTransactionCount');

      // Increment the total ERC721 and ERC1155 transaction count
      totalErc721Transactions += erc721TransactionCount;
	  totalErc1155TransactionsSingle += erc1155SingleTransactionCount;
	  totalErc1155TransactionsBatch += erc1155BatchTransactionCount;
    } catch (error) {
      console.log(`Error occurred during block ${blockNumber} processing: ${error}`, 'error', 'checkTransactionCount');
    }

    // Check if the block exists and contains transactions
    if (block != null && block.transactions != null && block.transactions.length !== 0) {
      // Concatenate the transactions to the existing array
      transactions = transactions.concat(block.transactions);
    }
  }

  logger.log(`Found ${transactions.length} transactions in the specified block range`, 'info', 'checkTransactionCount');
  logger.log(`Total ERC721 transactions: ${totalErc721Transactions}`, 'info', 'checkTransactionCount');
  logger.log(`Total ERC1155 Single Transfer transactions: ${totalErc1155TransactionsSingle}`, 'info', 'checkTransactionCount');
  logger.log(`Total ERC1155 Batch Transfer transactions: ${totalErc1155TransactionsBatch}`, 'info', 'checkTransactionCount');
  const totalRawTransactions = totalErc721Transactions + totalErc1155TransactionsSingle + totalErc1155TransactionsBatch;
  logger.log(`Total NFT transactions: ${totalRawTransactions}`, 'info','checkTransactionCount');

  return transactions;
}

// Main function
async function main() {
  try {
	console.log('Final Script.....');  
    // Connect to MongoDB
    await db.connect();
    console.log('Db: Connected Mongodb...');
    // Instantiate Web3
    //global.web3 = instantiateWeb3ViaWebsocket();
    //console.log('Web3: Connected RPC...');
    
	
    const dater = new EthDater(web3);

    // Get the target date 23 June and 29th May
	//MM-DD-YYYY
    const targetDate = new Date('06-23-2023');

    // Set the start date at 12:00 AM in UTC
    const startDate = new Date(Date.UTC(targetDate.getUTCFullYear(), targetDate.getUTCMonth(), targetDate.getUTCDate(), 0, 0, 0));

    // Add hours or minutes to the start date to get the end date
    const hoursToAdd = 23;
	console.log("Number of Hours: ", hoursToAdd);
    const minutesToAdd = 59;
	console.log("Number of Minutes: ", minutesToAdd);
    const secondsToAdd = 59;
	console.log("Number of Seconds: ", secondsToAdd);
   
    const endDate = new Date(startDate.getTime() + (hoursToAdd * 60 * 60 * 1000) + (minutesToAdd * 60 * 1000) + (secondsToAdd * 1000));

    // Format the start and end dates in UTC format
    const formattedStartDate = startDate.toISOString();
    const formattedEndDate = endDate.toISOString();

    console.log("Start Date:", formattedStartDate);
    console.log("End Date:", formattedEndDate);

    // Getting block by date:
    console.time('blockStartDate');
    let blockStartDate = await dater.getDate(formattedStartDate);
    //console.timeEnd('blockStartDate');
    console.log('Start Block:', blockStartDate.block);

    console.time('blockEndDate');
    let blockEndDate = await dater.getDate(formattedEndDate);
    //console.timeEnd('blockEndDate');
    console.log('End Block:', blockEndDate.block);
	
    
	
	console.log("");
    console.log('Fetching transactions in parallel...');
    console.time('Parallel Processing Scan ERC721 and ERC1155');
    // Get the transactions within the specified block range with a concurrency limit of 5
    const rawTransactionCnt = await scanForERC721AndERC1155Transactions(blockStartDate.block, blockEndDate.block);
    console.log('Total ERC721 and ERC1155 Transactions in Parallel:', rawTransactionCnt);
    console.timeEnd('Parallel Processing Scan ERC721 and ERC1155');
	
	/*
	console.log("");
    console.log('Sequence Processing.....');		
	console.time('Sequence Processing');
	const transactionsInSeq = await checkTransactionCount(blockStartDate.block,blockEndDate.block);
	console.log('Total Transactions In sequence Process:', transactionsInSeq.length);
    console.timeEnd('Sequence Processing');   
	console.log("");
	*/
	
  } catch (error) {
    console.error('An error occurred:', error);
    process.exit(1);
  } finally {
    // Dissconect to MongoDB
    await db.close();
    console.log('Db: Disconect Mongodb...');
    
  }
      process.exit(); // Exit the Node.js process

}
// Run the main function
main();

