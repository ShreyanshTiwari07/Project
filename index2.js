const EthDater = require('./BlockByDate');
const { Web3 } = require('web3');
const fs = require('fs');
const { MongoClient } = require('mongodb');
const Promise = require('bluebird');

//Custom Classes
const { getWeb3Instance, connectWeb3, disconnectWeb3 } = require('./web3Instance');
const logger = require('./log.js');
const marketplaceEventMappings = require('./marketplaceEventMappings.js');
const db = require('./database.js');
const TransactionHelper = require('./TransactionHelper');

let collectionName='';

// Load ERC721 token ABI from file
const tokenABIPathERC721 = './abi/ERC721.json';

// Load ERC token ABI from file
const tokenABIPathERC1155 = './abi/ERC1155.json';

// Maximum batch size for bulk write operations
const MAX_BATCH_SIZE = 300;

// Example usage
logger.setDebugFlag(true);


// Instantiate Web3 using HTTP provider
//const web3Http = getWeb3Instance('http');
// Use `web3Http` for HTTP-related functionality

// Instantiate Web3 using WebSocket (WSS) provider
const web3 = instantiateWeb3ViaWebsocket();
// Use `web3Ws` for WebSocket-related functionality


/**
 * Retrieves ERC1155 single transfer events within the specified block range.
 * @param {number} fromBlock - The starting block number.
 * @param {number} toBlock - The ending block number.
 * @returns {Promise<Object[]>} - A promise that resolves with an array of ERC1155 single transfer events.
 */
async function getERC1155SingleTransferEvents(fromBlock, toBlock) {
  const options = {
    topics: [
      web3.utils.sha3('TransferSingle(address,address,address,uint256,uint256)')
    ],
    fromBlock: web3.utils.toHex(fromBlock),
    toBlock: web3.utils.toHex(toBlock)
  };

  try {
    //console.time('ERC1155 Single Transfer Events'); // Start the timer
    const events = await web3.eth.getPastLogs(options);
    //console.timeEnd('ERC1155 Single Transfer Events'); // End the timer
    //console.log('Total ERC1155 Single Transfer Events:', events.length);
    return events;
  } catch (error) {
    console.error('Error retrieving ERC1155 single transfer events:', error);
    return [];
  }
}

/**
 * Retrieves the unique raw data objects from an array of events.
 * @param {Object[]} events - The array of event objects containing transaction details.
 * @returns {Object[]} - The array of unique raw data objects extracted from the events.
 */
async function getUniqueRawDataObjectsFromEventsERC1155Single(events) {
  const finalDataObjects = {};

  for (const event of events) {
    try {
      // Check if the event has the expected number of topics
      if (event.topics.length === 4) {
        // Decode the event log data to extract relevant information
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

        /*
        1. "transactionHash": transactionHash,
        2. "blockNumber": blockNumberK,
        3. "from": fromK,
        4. "to": toK,
        5. "gas": gas,
        6. "gasPrice": gasPrice,
        7. "value": valueK,
        8. "tokenId": tokenId,
        9. "amount": amount,
        10. "ms": ms,
        11. "contract": contract,
        12. "tokentype": tokentype,
        13. "transferType": transfertype,
        14. "time": time,
        15. "collectionName": collectionName,
        16. "symbol": symbol,
        17. "marketPlace": marketPlace,
        18. "pushedToDb": pushedToDb
        */

        const transactionHash = event.transactionHash;
        const existingRawDataObject = finalDataObjects[transactionHash];

        // Get the current timestamp
        const now = new Date();
        const pushedToDb = now.toUTCString().substring(0, now.toUTCString().indexOf('GMT')) + 'UTC';

        let tokenId = [Number(transaction.id)];
        let amount = [Number(transaction.value)];
		
        if (existingRawDataObject) {          
		  existingRawDataObject.tokenId.push(...tokenId);
          existingRawDataObject.amount.push(...amount);
          const msk = await TransactionHelper.getTransactionType(
            existingRawDataObject.from,
            existingRawDataObject.to,
            existingRawDataObject.marketValueType,
            existingRawDataObject.contract
          );

          // Check if msk has values
          if (msk.length > 0) {
            existingRawDataObject.ms.push(...msk);
          }
        } else {
          const blockNumber = event.blockNumber;
          const contract = event.address;
          const tokentype = 'ERC1155';
          const transferType = 'Single';

          
          const processedTransaction = await TransactionHelper.getTransactionFromRawTransaction(transactionHash);
          const from = processedTransaction.from;
          const to = processedTransaction.to;
          const gas = Number(processedTransaction.gas);
          const gasPrice = Number(processedTransaction.gasPrice);
          let value = '0';
          if (processedTransaction.value) {
            value = web3.utils.fromWei(processedTransaction.value, 'ether');
          }
          const time = await TransactionHelper.getFormattedBlockTimestamp(blockNumber);
          const marktAndValuType = await TransactionHelper.getMarketplaceAndValue(transactionHash);
          const marketPlace = marktAndValuType.marketplace;
          const marketValueType = marktAndValuType.value;
          const ms = await TransactionHelper.getTransactionType(from, to, marketValueType, contract);

          let tokenDetails = null;
          tokenDetails = await TransactionHelper.getTokenDetails(tokenABIPathERC721, contract);
          let collectionName = '';
          let symbol = '';

          if (tokenDetails) {
            collectionName = tokenDetails.name || '';
            symbol = tokenDetails.symbol || '';
          }

          const dataObject = {
            "transactionHash": transactionHash,
            "blockNumber": blockNumber,
            "from": from,
            "to": to,
            "gas": gas,
            "gasPrice": gasPrice,
            "value": value,
            "tokenId": tokenId,
            "amount":amount,
            "ms": ms,
            "contract": contract,
            "tokentype": tokentype,
            "transferType": transferType,
            "time": time,
            "collectionName": collectionName,
            "symbol": symbol,
            "marketPlace": marketPlace,
            "pushedToDb": pushedToDb,
            "marketValuType": marketValueType
          };
          finalDataObjects[transactionHash] = dataObject;
        }
      }
    } catch (error) {
      console.error(pushedToDb + ': An error occurred while processing an event:', error);
    }
  }

  return Object.values(finalDataObjects);
}






/**
 * Retrieves ERC1155 batch transfer events within the specified block range.
 * @param {number} fromBlock - The starting block number.
 * @param {number} toBlock - The ending block number.
 * @returns {Promise<Object[]>} - A promise that resolves with an array of ERC1155 batch transfer events.
 */
async function getERC1155BatchTransferEvents(fromBlock, toBlock) {
  const options = {
    topics: [
      web3.utils.sha3('TransferBatch(address,address,address,uint256[],uint256[])')
    ],
    fromBlock: web3.utils.toHex(fromBlock),
    toBlock: web3.utils.toHex(toBlock)
  };

  try {
    //console.time('ERC1155 Batch Transfer Events'); // Start the timer
    const events = await web3.eth.getPastLogs(options);
    //console.timeEnd('ERC1155 Batch Transfer Events'); // End the timer
    //console.log('Total ERC1155 Batch Transfer Events:', events.length);
    return events;
  } catch (error) {
    console.error('Error retrieving ERC1155 batch transfer events:', error);
    return [];
  }
}


/**
 * Retrieves the unique raw data objects from an array of events.
 * @param {Object[]} events - The array of event objects containing transaction details.
 * @returns {Object[]} - The array of unique raw data objects extracted from the events.
 */
async function getUniqueRawDataObjectsFromEventsERC1155Batch(events) {
  const finalDataObjects = {};

  for (const event of events) {
    try {
      // Check if the event has the expected number of topics
      if (event.topics.length === 4) {
        // Decode the event log data to extract relevant information
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

        /*
        1. "transactionHash": transactionHash,
        2. "blockNumber": blockNumberK,
        3. "from": fromK,
        4. "to": toK,
        5. "gas": gas,
        6. "gasPrice": gasPrice,
        7. "value": valueK,
        8. "tokenId": tokenId,
        9. "amount": amount,
        10. "ms": ms,
        11. "contract": contract,
        12. "tokentype": tokentype,
        13. "transferType": transfertype,
        14. "time": time,
        15. "collectionName": collectionName,
        16. "symbol": symbol,
        17. "marketPlace": marketPlace,
        18. "pushedToDb": pushedToDb
        */

        const transactionHash = event.transactionHash;
        const existingRawDataObject = finalDataObjects[transactionHash];

        // Get the current timestamp
        const now = new Date();
        const pushedToDb = now.toUTCString().substring(0, now.toUTCString().indexOf('GMT')) + 'UTC';

        let tokenId = transaction.ids.map(Number);
        let amount = transaction.values.map(Number);
		
        if (existingRawDataObject) {          
		  existingRawDataObject.tokenId.push(...tokenId);
          existingRawDataObject.amount.push(...amount);
          const msk = await TransactionHelper.getTransactionType(
            existingRawDataObject.from,
            existingRawDataObject.to,
            existingRawDataObject.marketValueType,
            existingRawDataObject.contract
          );

          // Check if msk has values
          if (msk.length > 0) {
            existingRawDataObject.ms.push(...msk);
          }
        } else {
          const blockNumber = event.blockNumber;
          const contract = event.address;
          const tokentype = 'ERC1155';
          const transferType = 'batch';

          
          const processedTransaction = await TransactionHelper.getTransactionFromRawTransaction(transactionHash);
          const from = processedTransaction.from;
          const to = processedTransaction.to;
          const gas = Number(processedTransaction.gas);
          const gasPrice = Number(processedTransaction.gasPrice);
          let value = '0';
          if (processedTransaction.value) {
            value = web3.utils.fromWei(processedTransaction.value, 'ether');
          }
          const time = await TransactionHelper.getFormattedBlockTimestamp(blockNumber);
          const marktAndValuType = await TransactionHelper.getMarketplaceAndValue(transactionHash);
          const marketPlace = marktAndValuType.marketplace;
          const marketValueType = marktAndValuType.value;
          const ms = await TransactionHelper.getTransactionType(from, to, marketValueType, contract);

          let tokenDetails = null;
          tokenDetails = await TransactionHelper.getTokenDetails(tokenABIPathERC721, contract);
          let collectionName = '';
          let symbol = '';

          if (tokenDetails) {
            collectionName = tokenDetails.name || '';
            symbol = tokenDetails.symbol || '';
          }

          const dataObject = {
            "transactionHash": transactionHash,
            "blockNumber": blockNumber,
            "from": from,
            "to": to,
            "gas": gas,
            "gasPrice": gasPrice,
            "value": value,
            "tokenId": tokenId,
            "amount":amount,
            "ms": ms,
            "contract": contract,
            "tokentype": tokentype,
            "transferType": transferType,
            "time": time,
            "collectionName": collectionName,
            "symbol": symbol,
            "marketPlace": marketPlace,
            "pushedToDb": pushedToDb,
            "marketValuType": marketValueType
          };
          finalDataObjects[transactionHash] = dataObject;
        }
      }
    } catch (error) {
      console.error('An error occurred while processing an event:', error);
    }
  }

  return Object.values(finalDataObjects);
}




/**
 * Retrieves ERC721 transfer events within the specified block range.
 * @param {number} fromBlock - The starting block number.
 * @param {number} toBlock - The ending block number.
 * @returns {Promise<Object[]>} - A promise that resolves with an array of ERC721 transfer events.
 */
async function getERC721TransferEvents(fromBlock, toBlock) {
  const options = {
    topics: [
      web3.utils.sha3('Transfer(address,address,uint256)')
    ],
    fromBlock: web3.utils.toHex(fromBlock),
    toBlock: web3.utils.toHex(toBlock)
  };

  try {
    //console.time('ERC721 Transfer Events'); // Start the timer
    const events = await web3.eth.getPastLogs(options);
    //console.timeEnd('ERC721 Transfer Events'); // End the timer
    //onsole.log('Total ERC721 Transfer Events:', events.length);
    return events;
  } catch (error) {
    console.error('Error retrieving ERC721 transfer events:', error);
    return [];
  }
}





/**
 * Retrieves the unique raw data objects from an array of events.
 * @param {Object[]} events - The array of event objects containing transaction details.
 * @returns {Object[]} - The array of unique raw data objects extracted from the events.
 */
async function getUniqueRawDataObjectsFromEventsERC721(events) {
  const finalDataObjects = {};

  for (const event of events) {
    try {
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

        /*
        1. "transactionHash": transactionHash,
        2. "blockNumber": blockNumberK,
        3. "from": fromK,
        4. "to": toK,
        5. "gas": gas,
        6. "gasPrice": gasPrice,
        7. "value": valueK,
        8. "tokenId": tokenId,
        9. "amount": amount,
        10. "ms": ms,
        11. "contract": contract,
        12. "tokentype": tokentype,
        13. "transferType": transfertype,
        14. "time": time,
        15. "collectionName": collectionName,
        16. "symbol": symbol,
        17. "marketPlace": marketPlace,
        18. "pushedToDb": pushedToDb
        */

        const transactionHash = event.transactionHash;
        const existingRawDataObject = finalDataObjects[transactionHash];

        // Get the current timestamp
        const now = new Date();
        const pushedToDb = now.toUTCString().substring(0, now.toUTCString().indexOf('GMT')) + 'UTC';

        let tokenId = Number(transaction.tokenId);
        let amount = [];
        if (existingRawDataObject) {
          existingRawDataObject.tokenId.push(tokenId);

          const msk = await TransactionHelper.getTransactionType(
            existingRawDataObject.from,
            existingRawDataObject.to,
            existingRawDataObject.marketValueType,
            existingRawDataObject.contract
          );

          // Check if msk has values
          if (msk.length > 0) {
            existingRawDataObject.ms.push(...msk);
          }
        } else {
          const blockNumber = event.blockNumber;
          const contract = event.address;
          const tokentype = 'ERC721';
          const transferType = 'Single';

          
          const processedTransaction = await TransactionHelper.getTransactionFromRawTransaction(transactionHash);
          const from = processedTransaction.from;
          const to = processedTransaction.to;
          const gas = Number(processedTransaction.gas);
          const gasPrice = Number(processedTransaction.gasPrice);
          let value = '0';
          if (processedTransaction.value) {
            value = web3.utils.fromWei(processedTransaction.value, 'ether');
          }
          const time = await TransactionHelper.getFormattedBlockTimestamp(blockNumber);
          const marktAndValuType = await TransactionHelper.getMarketplaceAndValue(transactionHash);
          const marketPlace = marktAndValuType.marketplace;
          const marketValueType = marktAndValuType.value;
          const ms = await TransactionHelper.getTransactionType(from, to, marketValueType, contract);

          let tokenDetails = null;
          tokenDetails = await TransactionHelper.getTokenDetails(tokenABIPathERC721, contract);
          let collectionName = '';
          let symbol = '';

          if (tokenDetails) {
            collectionName = tokenDetails.name || '';
            symbol = tokenDetails.symbol || '';
          }

          const dataObject = {
            "transactionHash": transactionHash,
            "blockNumber": blockNumber,
            "from": from,
            "to": to,
            "gas": gas,
            "gasPrice": gasPrice,
            "value": value,
            "tokenId": [tokenId],
            "amount": amount,
            "ms": ms,
            "contract": contract,
            "tokentype": tokentype,
            "transferType": transferType,
            "time": time,
            "collectionName": collectionName,
            "symbol": symbol,
            "marketPlace": marketPlace,
            "pushedToDb": pushedToDb,
            "marketValuType": marketValueType
          };
          finalDataObjects[transactionHash] = dataObject;
        }
      }
    } catch (error) {
      console.error('An error occurred while processing an event:', error);
    }
  }

  return Object.values(finalDataObjects);
}



/**
 * Processes the unique raw data objects and generates the final data object.
 * @param {Object[]} uniqueRawDataObjects - The array of unique raw data objects.
 * @returns {Object} - The final data object generated from the unique raw data objects.
 */
async function getFinalDataObject(uniqueRawDataObjects) {
  // Initialize the final data object
  const finalDataArray = [];

  // Process each unique raw data object
  for (const rawDataObject of uniqueRawDataObjects) {
    const procObj = await TransactionHelper.getFinalDataObject(rawDataObject);

// Check if procObj is not null
if (procObj !== null) {
  // Add the data object to the final data array
  finalDataArray.push(procObj);
}



  }

  // Return the final data object
  return finalDataArray;
}


/**
 * Processes a block range to retrieve ERC721 and ERC1155 transfer events and returns the total count of events.
 * @param {number} fromBlock - The starting block number.
 * @param {number} toBlock - The ending block number.
 * @param {boolean} writeToDatabase - Flag indicating whether to write data to the database.
 * @returns {Promise<number>} - A promise that resolves with the total count of events.
 */
async function processBlockRange(fromBlock, toBlock, writeToDatabase) {
  try {
    let totalEvents = 0;
    let erc721Count = 0;
    let erc1155BatchCount = 0;
    let erc1155SingleCount = 0;
    const bulkOperations = []; // Create a new bulkOperations array for each block range

    // Retrieve ERC721 transfer events
    const erc721Events = await getERC721TransferEvents(fromBlock, toBlock);

    if (erc721Events && erc721Events.length) {
      // Process each ERC721 event and get the raw data object
      const erc721DataObjects = await getUniqueRawDataObjectsFromEventsERC721(erc721Events);

      if (erc721DataObjects && erc721DataObjects.length) {
      // Update the total count and erc721Count
      totalEvents += erc721DataObjects.length;
      erc721Count += erc721DataObjects.length;
	  }

      // Write ERC721 data to the database
      if (writeToDatabase && erc721DataObjects && erc721DataObjects.length) {

        await db.bulkInsertTransactions(erc721DataObjects,collectionName);
      }
    }

    // Retrieve ERC1155 batch transfer events
    const erc1155BatchEvents = await getERC1155BatchTransferEvents(fromBlock, toBlock);

    if (erc1155BatchEvents && erc1155BatchEvents.length) {
      // Process each ERC1155 batch event and get the raw data object
      const erc1155BatchDataObjects = await getUniqueRawDataObjectsFromEventsERC1155Batch(erc1155BatchEvents);

	  if (erc1155BatchDataObjects && erc1155BatchDataObjects.length) {
      // Update the total count and erc1155BatchCount
      totalEvents += erc1155BatchDataObjects.length;
      erc1155BatchCount += erc1155BatchDataObjects.length;
	  }

      // Write ERC1155 batch data to the database
      if (writeToDatabase && erc1155BatchDataObjects && erc1155BatchDataObjects.length) {
        await db.bulkInsertTransactions(erc1155BatchDataObjects,collectionName);
      }
    }

    // Retrieve ERC1155 single transfer events
    const erc1155SingleEvents = await getERC1155SingleTransferEvents(fromBlock, toBlock);

    if (erc1155SingleEvents && erc1155SingleEvents.length) {
      // Process each ERC1155 single event and get the raw data object
      const erc1155SingleDataObjects = await getUniqueRawDataObjectsFromEventsERC1155Single(erc1155SingleEvents);

      if (erc1155SingleDataObjects && erc1155SingleDataObjects.length) {
      // Update the total count and erc1155SingleCount
      totalEvents += erc1155SingleDataObjects.length;
      erc1155SingleCount += erc1155SingleDataObjects.length;
	  }

      // Write ERC1155 single data to the database
      if (writeToDatabase && erc1155SingleDataObjects && erc1155SingleDataObjects.length) {
        await db.bulkInsertTransactions(erc1155SingleDataObjects,collectionName);
      }
    }

    console.log(`Total ERC721 events: ${erc721Count}`);
    console.log(`Total ERC1155 Batch events: ${erc1155BatchCount}`);
    console.log(`Total ERC1155 Single events: ${erc1155SingleCount}`);

    return totalEvents;
  } catch (error) {
    console.error('Error processing block range:', error);
    return 0; // Return 0 in case of an error
  }
}




/**
 * Splits the start and end block numbers into smaller ranges of the specified size.
 * @param {number} startBlockNumber - The start block number.
 * @param {number} endBlockNumber - The end block number.
 * @param {number} rangeSize - The size of each range.
 * @returns {Array} - An array containing the ranges of block numbers.
 */
function splitIntoRanges(startBlockNumber, endBlockNumber, rangeSize) {
  const ranges = [];

  let currentBlockNumber = startBlockNumber;

  while (currentBlockNumber <= endBlockNumber) {
    const endRangeBlockNumber = Math.min(currentBlockNumber + rangeSize - 1, endBlockNumber);
    ranges.push([currentBlockNumber, endRangeBlockNumber]);
    currentBlockNumber += rangeSize;
  }

  return ranges;
}

/**
 * Main function to process blocks for a specified number of days.
 * @returns {Promise<void>} - A promise that resolves when the processing is complete.
 */
async function main() {
  try {
	console.log("");
	  
	console.log("********************   START   **************************");
	console.log("");
    

    // Connect to MongoDB
    await db.connect();
    console.log('Db: Connected Mongodb...');

    const dater = new EthDater(web3);

    const year = 2023; // Specify the year
    const month = 3; // Specify the month (1-12)
    const date = 25; // Specify the date (1-31)

    const startDate = new Date(year, month - 1, date);
    // Get the target start date //MM-DD-YYYY
    //const startDate = new Date('04-06-2023');
    const numberOfDays = 24; // Specify the number of days (positive or negative)

    // Define the duration to add for each day
    const hoursToAdd = 23;
    const minutesToAdd = 59;
    const secondsToAdd = 59;

    // Calculate the start date after considering the number of days
    const targetStartDate = new Date(startDate);
    targetStartDate.setDate(targetStartDate.getDate() - numberOfDays);

    // Loop through each day based on the number of days in reverse order
    let currentDate = new Date(targetStartDate);

    for (let i = 0; i <= numberOfDays; i++) {
      
      console.log("");
      console.log("-----------------------------------------");

      // Set the start date at 12:00 AM in UTC
      const startOfDay = new Date(Date.UTC(currentDate.getUTCFullYear(), currentDate.getUTCMonth(), currentDate.getUTCDate(), 0, 0, 0));

      const options = { day: "2-digit", month: "short", year: "numeric" };
      const formattedDate = startOfDay.toLocaleDateString("en-US", options)
        .replace(/\b\w/g, (match) => match.toUpperCase())
        .replace(/ /g, '_');

      console.log(formattedDate); // Output: 07_MAY_2023

      collectionName = `${formattedDate}_Collection`; // Format the collection name

      // Add hours, minutes, and seconds to the start date to get the end date
      const endOfDay = new Date(startOfDay.getTime());
      endOfDay.setUTCHours(startOfDay.getUTCHours() + hoursToAdd);
      endOfDay.setUTCMinutes(startOfDay.getUTCMinutes() + minutesToAdd);
      endOfDay.setUTCSeconds(startOfDay.getUTCSeconds() + secondsToAdd);

      console.log("Start Date:", startOfDay.toISOString());
      console.log("End Date:", endOfDay.toISOString());

      // Getting block by date
      let blockStartDate = await dater.getDate(startOfDay.toISOString());
      let blockEndDate = await dater.getDate(endOfDay.toISOString());
      console.log('Start Block:', blockStartDate.block);
      console.log('End Block:', blockEndDate.block);

      let startBlockNumber = blockStartDate.block;
      let endBlockNumber = blockEndDate.block;

      // Calculate the total number of blocks in the range
      const totalBlocks = endBlockNumber - startBlockNumber + 1;
      logger.log(`Total number of blocks: ${totalBlocks}`, 'info');

      const lastProcessedDate = await db.getLastProcessedDate();
      
      
	  console.log("Start Date:", startOfDay);
	  console.log("Last Processed Date:", lastProcessedDate);
	  const isDate1GreaterThanDate2 = startOfDay >= lastProcessedDate;
	  console.log("Is Startdate >= lastProcessedDate: " + isDate1GreaterThanDate2);


	  let isProcessDate = false;
	  if(lastProcessedDate === null)
	  {
		  isProcessDate = true;
	  }	  
      else if (lastProcessedDate && startOfDay >= lastProcessedDate) {
        const lastProcessedStartBlock = await db.getLastProcessedStartBlockNumber(lastProcessedDate);
        const lastProcessedEndBlock = await db.getLastProcessedEndBlockNumber(lastProcessedDate);
        console.log("lastProcessedStartBlock", lastProcessedStartBlock);
		console.log("lastProcessedEndBlock", lastProcessedEndBlock);
		console.log("startBlockNumber", startBlockNumber);
		console.log("endBlockNumber", endBlockNumber);
        if (
          lastProcessedStartBlock !== null &&
          lastProcessedEndBlock !== null &&
          lastProcessedStartBlock <= startBlockNumber &&
          lastProcessedEndBlock >= endBlockNumber - 1
        ) {
          console.log(`Data for ${formattedDate} already processed. Skipping...`);
          
          //console.log("Data processed for " + formattedDate);
          console.log("-----------------------------------------");
          console.log("");
          // Move to the next day
          currentDate.setDate(currentDate.getDate() + 1);
          continue;
        } else if (
          lastProcessedStartBlock !== null &&
          lastProcessedEndBlock !== null
        ) {
          console.log(`Partial run detected for ${formattedDate}. Restarting from the last processed batch...`);

          // Delete the collectionName from database
          const collectionExists = await db.dropCollection(collectionName);
		  isProcessDate = true;
          
        }
      }
      
      if(isProcessDate)
	  {
	  console.time('Process');
      // Split blocks into ranges
      const blockRanges = splitIntoRanges(startBlockNumber, endBlockNumber, 100);

      // Initialize total events count
      let totalEvents = 0;

      // Process each block range concurrently
      await Promise.map(blockRanges, async (range, i) => {
        const [rangeStart, rangeEnd] = range;
        console.log("");
        console.log(`Batch ${i + 1} of ${blockRanges.length}`);
        logger.log(`Processing block range from ${rangeStart} to ${rangeEnd}`, 'info');

        const eventsCount = await processBlockRange(rangeStart, rangeEnd, true);
        totalEvents += eventsCount;

        console.log("");
      }, { concurrency: 1 }); // Adjust the concurrency limit as needed

      logger.log(`Total number of events: ${totalEvents}`, 'info');

      // Refresh the system before moving to the next day of data extraction
      await refreshSystem();
      console.timeEnd('Process');
      console.log("Data processed for " + formattedDate); // Output: 07_MAY_2023
      console.log("-----------------------------------------");
      console.log("");
      
	  }
	  else {
		  console.log(`Data for ${formattedDate} already processed. Skipping...`);
	  }
	  // Move to the next day
      currentDate.setDate(currentDate.getDate() + 1);

      // Update the last processed date, start block, and end block in the database
      await db.updateLastProcessedBlockNumber(startOfDay, startBlockNumber, endBlockNumber);
    }

  } catch (error) {
    console.error('An error occurred:', error);
    process.exit(1);
  } finally {
    // Disconnect from MongoDB
    await db.close();
    console.log('Db: Disconnect Mongodb...');
	console.log("********************   END   **************************");
	console.log("");
	console.log("");
  }

  process.exit(); // Exit the Node.js process
}






async function main1() {
  try {
    console.log('Final Script.....');

    // Connect to MongoDB
    await db.connect();
    console.log('Db: Connected Mongodb...');

    const dater = new EthDater(web3);


	const year = 2023; // Specify the year
	const month = 7; // Specify the month (1-12)
	const date = 1; // Specify the date (1-31)

	const startDate = new Date(year, month - 1, date);
    // Get the target start date //MM-DD-YYYY
    //const startDate = new Date('04-06-2023');
    const numberOfDays = 5; // Specify the number of days (positive or negative)

    // Define the duration to add for each day
    const hoursToAdd = 0;
    const minutesToAdd = 0;
    const secondsToAdd = 59;

    // Calculate the start date after considering the number of days
    const targetStartDate = new Date(startDate);
    targetStartDate.setDate(targetStartDate.getDate() - numberOfDays);

    // Loop through each day based on the number of days in reverse order
    let currentDate = new Date(targetStartDate);



    for (let i = 0; i <= numberOfDays; i++) {
	  console.time('Process');
	  console.log("");
	  console.log("-----------------------------------------");
      // Set the start date at 12:00 AM in UTC
      const startOfDay = new Date(Date.UTC(currentDate.getUTCFullYear(), currentDate.getUTCMonth(), currentDate.getUTCDate(), 0, 0, 0));

	  const options = { day: "2-digit", month: "short", year: "numeric" };
			const formattedDate = startOfDay.toLocaleDateString("en-US", options)
  .replace(/\b\w/g, (match) => match.toUpperCase())
  .replace(/ /g, '_');

	  console.log(formattedDate); // Output: 07_MAY_2023



	  collectionName = `${formattedDate}_Collection`; // Format the collection name

      // Add hours, minutes, and seconds to the start date to get the end date
      const endOfDay = new Date(startOfDay.getTime());
      endOfDay.setUTCHours(startOfDay.getUTCHours() + hoursToAdd);
      endOfDay.setUTCMinutes(startOfDay.getUTCMinutes() + minutesToAdd);
      endOfDay.setUTCSeconds(startOfDay.getUTCSeconds() + secondsToAdd);

      // Format the start and end dates in UTC format
      const formattedStartDate = startOfDay.toISOString();
      const formattedEndDate = endOfDay.toISOString();

      console.log("Start Date:", formattedStartDate);
      console.log("End Date:", formattedEndDate);

      // Getting block by date
      let blockStartDate = await dater.getDate(formattedStartDate);
      let blockEndDate = await dater.getDate(formattedEndDate);
      console.log('Start Block:', blockStartDate.block);
      console.log('End Block:', blockEndDate.block);



      const startBlockNumber = blockStartDate.block;
      const endBlockNumber = blockEndDate.block;

      // Calculate the total number of blocks in the range
      const totalBlocks = endBlockNumber - startBlockNumber + 1;
      logger.log(`Total number of blocks: ${totalBlocks}`, 'info');

      // Split blocks into ranges
      const blockRanges = splitIntoRanges(startBlockNumber, endBlockNumber, 25);

      // Initialize total events count
      let totalEvents = 0;

      // Process each block range concurrently
	  await Promise.map(blockRanges, async (range, i) => {
	  const [rangeStart, rangeEnd] = range;
	  console.log("");
	  console.log(`Batch ${i + 1} of ${blockRanges.length}`);
	  logger.log(`Processing block range from ${rangeStart} to ${rangeEnd}`, 'info');

	  const eventsCount = await processBlockRange(rangeStart, rangeEnd, true);
	  totalEvents += eventsCount;




	  console.log("");
	  }, { concurrency: 1 }); // Adjust the concurrency limit as needed


      logger.log(`Total number of events: ${totalEvents}`, 'info');


      // Refresh the system before moving to the next day of data extraction
      await refreshSystem();
      console.timeEnd('Process');
	   console.log("Data processsed for " + formattedDate); // Output: 07_MAY_2023

	  console.log("-----------------------------------------");
      console.log("");
      // Move to the next day
      currentDate.setDate(currentDate.getDate() + 1);
    }

  } catch (error) {
    console.error('An error occurred:', error);
    process.exit(1);
  } finally {
    // Disconnect from MongoDB
    await db.close();
    console.log('Db: Disconnect Mongodb...');
  }

  process.exit(); // Exit the Node.js process
}
async function refreshSystem() {
  // Disconnect from Web3 and reconnect
  disconnectWeb3();
  connectWeb3('ws');

  // Wait for some time to allow the system to reset
  await sleep(5000); // Adjust the delay time as needed

  // Reconnect to Web3
  const web3 = getWeb3Instance('ws');
  await web3.eth.net.isListening();
  console.log('Web3: Connected RPC...');
}

async function refreshSystem2() {
  // Disconnect from Web3 and reconnect
  web3.currentProvider.disconnect();
  web3.setProvider(instantiateWeb3ViaWebsocket());

  // Wait for some time to allow the system to reset
  await sleep(5000); // Adjust the delay time as needed

  // Reconnect to Web3
  await web3.eth.net.isListening();
  console.log('Web3: Connected RPC...');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function instantiateWeb3ViaWebsocket() {
  const providerType = 'ws'; // Specify the provider type (either 'http' or 'ws')
  const web3 = getWeb3Instance(providerType);
  return web3;
}

// Run the main function
main();

