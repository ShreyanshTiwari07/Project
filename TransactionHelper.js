const { getWeb3Instance } = require('./web3Instance');
const web3 = getWeb3Instance('ws');
const marketplaceEventMappings = require('./marketplaceEventMappings');
const fs = require('fs');


// Load ERC721 token ABI from file
const tokenABIPathERC721 = './abi/ERC721.json';

// Load ERC token ABI from file
const tokenABIPathERC1155 = './abi/ERC1155.json';

class TransactionHelper {


/**
 * Retrieves the raw data object from the transaction.
 * @param {Object} rawDataObject - The raw data object containing transaction details.
 * @returns {Object} - The raw data object.
 */
static async getFinalDataObject(rawDataObject) {
  try {

	// Destructure the properties from the rawDataObject
    const {
      transactionHash,
      blockNumber,
      contract,
      from,
      to,
      tokenId,
      amount,
      tokentype,
      transfertype,
	  ms	  
    } = rawDataObject;	  
    
	const blockNumberK = Number(blockNumber);
   

	// Get the transaction details
    const transaction = await this.getTransactionFromRawTransaction(transactionHash);
	const fromK= transaction.from;
    const toK= transaction.to;	
	let valueK = '0';
    if (transaction.value) {
      valueK = web3.utils.fromWei(transaction.value, 'ether');
    }    
	
	const gas = Number(transaction.gas);	
	const gasPrice = Number(transaction.gasPrice);
    // Get the current timestamp
    const now = new Date();
    const pushedToDb = now.toUTCString().substring(0, now.toUTCString().indexOf('GMT')) + 'UTC';
	
	// Get the marketplace and value types
    const marktAndValuType = await this.getMarketplaceAndValue(transactionHash);
    const marketPlace = marktAndValuType.marketplace;
    const msk = this.getTransactionType(from,to, marktAndValuType.value, contract);
// Check if msk has values
if (msk.length > 0) {
  rawDataObject.ms.push(...msk);
}
	
    let tokenDetails = null;
	tokenDetails = await this.getTokenDetails(tokenABIPathERC721, contract);
	let collectionName = '';
	let symbol = '';

if (tokenDetails) {
  collectionName = tokenDetails.name || '';
  symbol = tokenDetails.symbol || '';
}
	
	
	
    const time = await this.getFormattedBlockTimestamp(blockNumber);
	
    const rawData = {
        "transactionHash": transactionHash,//1*
        "blockNumber": blockNumberK,//2 *
        "from": fromK,//3 *
        "to": toK,//4*
        "gas": gas,//5*
        "gasPrice": gasPrice,//6*
        "value": valueK,
        "tokenId": tokenId,//8  *** check it based on ERC 721 and ERC 1155
        "amount": amount,//9 *** check based on ERC 721 and ERC 1155
        "ms": ms,//10 *
        "contract": contract,//11 *
        "tokentype": tokentype,//12 *
		"transferType":transfertype,
        "time": time,//13 *
		"collectionName": collectionName,//14
		"symbol": symbol,//15
		"marketPlace": marketPlace,
		"pushedToDb": pushedToDb,
        "tokenDetails":tokenDetails		
    }   
	//console.log('Raw Data:', rawData); // Log the raw data object for debugging

    return rawData;
  } catch (error) {
    // Handle the error here, log it, or throw a custom error
    console.error('Error in getRawDataObject:', error);
    throw error;
  }
}
  

 
 
 /**
 * Retrieves the raw data object from the transaction.
 * @param {Object} rawDataObject - The raw data object containing transaction details.
 * @returns {Object|null} - The raw data object or null on error.
 */
static async getFinalDataObject(rawDataObject) {
  try {
    // Destructure the properties from the rawDataObject
    const {
      transactionHash,
      blockNumber,
      contract,
      from,
      to,
      tokenId,
      amount,
      tokentype,
      transfertype,
	  ms
    } = rawDataObject;

    const blockNumberK = Number(blockNumber);
    
	let marketPlace = '';
    //let ms = [];
	let tokenDetails = null;
	let collectionName = '';
    let symbol = '';
	
	let time = await this.getFormattedBlockTimestamp(blockNumber);
	
    // Get the transaction details
	const transactionReceipt = await web3.eth.getTransactionReceipt(transactionHash);
    const fromK = transactionReceipt.from;
    const toK = transactionReceipt.to;
    let valueK = '0';
    if (transactionReceipt.value) {
      valueK = web3.utils.fromWei(transactionReceipt.value, 'ether');
    }

    const gas = Number(transactionReceipt.gas);
    const gasPrice = Number(transactionReceipt.gasPrice);
    // Get the current timestamp
    const now = new Date();
    const pushedToDb = now.toUTCString().substring(0, now.toUTCString().indexOf('GMT')) + 'UTC';


	
	
    // Get the marketplace and value types
    const marktAndValuType = await this.getMarketplaceAndValue(transactionReceipt);
    marketPlace = marktAndValuType.marketplace;
    let msk = this.getTransactionType(from, to, marktAndValuType.value, contract);

// Check if msk has values
if (msk.length > 0) {
  rawDataObject.ms.push(...msk);
}
 
    	
    tokenDetails = await this.getTokenDetails(tokenABIPathERC721, contract);
    if (tokenDetails) {
      collectionName = tokenDetails.name || '';
      symbol = tokenDetails.symbol || '';
    }
   
	
	
	
    const Data = {
      "transactionHash": transactionHash,
      "blockNumber": blockNumberK,
      "from": fromK,
      "to": toK,
      "gas": gas,
      "gasPrice": gasPrice,
      "value": valueK,
      "tokenId": tokenId,
      "amount": amount,
      "ms": ms,
      "contract": contract,
      "tokentype": tokentype,
      "transferType": transfertype,
      "time": time,
      "collectionName": collectionName,
      "symbol": symbol,
      "marketPlace": marketPlace,
      "pushedToDb": pushedToDb,
      "tokenDetails": tokenDetails
    };

    return Data;
  } catch (error) {
    // Log the error and return null or an empty object
    console.error('Error in getRawDataObject:', error);
    return null; // or return {} for an empty object
  }
}






static async getTransactionFromRawTransaction(rawTransaction)
{
	 try {
      
      if (!rawTransaction) {
		  console.error('Error occurred while retrieving transaction');
        return null;
      }
	  const transaction = await web3.eth.getTransaction(rawTransaction);

      //const transaction = await web3.eth.getTransaction(rawTransaction);
      return transaction;
    } catch (error) {
      console.error('Error occured while getting transaction:', error);
      
    }
	
}
  
  /**
   * Retrieves the transaction timestamp.
   * @param {string} transactionHash - The hash of the transaction.
   * @returns {Promise<number|null>} - A promise that resolves with the timestamp of the transaction, or null if not found.
   */
  static async getTransactionTimestamp(transaction) {
    try {
      //const transaction = await web3.eth.getTransaction(transactionHash);
      if (!transaction || !transaction.blockNumber) {
		  console.error('Error occurred while retrieving transaction timestamp');
        return null;
      }
      const block = await web3.eth.getBlock(transaction.blockNumber);
      return block.timestamp;
    } catch (error) {
      console.error('Error occurred while retrieving transaction timestamp:', error);
      throw error;
    }
  }

 /**
 * Get the formatted timestamp for a given block number in the format: "Tue, 06 Jun 2023 00:00:23 UTC".
 * @param {number} blockNumber - The block number.
 * @returns {Promise<string>} - A promise that resolves with the formatted timestamp.
 */
static async getFormattedBlockTimestamp(blockNumber) {
  const block = await web3.eth.getBlock(blockNumber);
  const timestamp = Number(block.timestamp); 
  const date = new Date(timestamp * 1000);// Convert to milliseconds
  const options = { weekday: 'short', day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'UTC' };
  const formattedTimestamp = date.toLocaleString('en-US', options);
  return formattedTimestamp;
}

  
/**
 * Retrieves the marketplace and value based on a given transaction hash.
 * @param {string} transactionHash - The transaction hash to lookup in the marketplace event mappings.
 * @returns {Object} - An object containing the marketplace and value.
 *                    If the transaction hash is not found in the mappings, empty strings are returned.
 */
static async getMarketplaceAndValue(transactionHash) {
  try {
	const transactionReceipt = await web3.eth.getTransactionReceipt(transactionHash);
    var events = transactionReceipt.logs;
	for(var i=0; i< events.length;i++){
		var event = events[i];
		const mapping = marketplaceEventMappings[event.topics[0]];
		if (mapping) {
			const { marketplace, value } = mapping;
			return { marketplace, value };
		} else {
			// Transaction hash not found in mappings
			return { marketplace: '', value: '' };
		}	
	}	
    
  } catch (error) {
    // Handle the error here, log it, or throw a custom error
    console.error('Error retrieving marketplace and value:', error);
    throw error;
  }
}

/**
 * Retrieves the marketplace and value based on a given transaction hash.
 * @param {string} transactionHash - The transaction hash to lookup in the marketplace event mappings.
 * @returns {Object} - An object containing the marketplace and value.
 *                    If the transaction hash is not found in the mappings, empty strings are returned.
 */
static async getMarketplaceAndValue1(transactionReceipt) {
  try {
	//const transactionReceipt = await web3.eth.getTransactionReceipt(transactionHash);
    var events = transactionReceipt.logs;
	for(var i=0; i< events.length;i++){
		var event = events[i];
		const mapping = marketplaceEventMappings[event.topics[0]];
		if (mapping) {
			const { marketplace, value } = mapping;
			return { marketplace, value };
		} else {
			// Transaction hash not found in mappings
			return { marketplace: '', value: '' };
		}	
	}	
    
  } catch (error) {
    // Handle the error here, log it, or throw a custom error
    console.error('Error retrieving marketplace and value:', error);
    throw error;
  }
}



/**
 * Determines the mint, sale, or transfer type of a transaction.
 * @param {Object} transaction - The transaction object.
 * @param {string} value - The value indicating the type of transaction.
 * @param {string} [contract] - The contract address (optional).
 * @returns {Array<string>} - An array containing the mint, sale, or transfer type.
 */
static getTransactionType(from,to, value, contract) {
  //const { from, to } = transaction;
  const mintOrSaleOrTransfer = [];

  const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';

  if (from === ZERO_ADDRESS) {
    mintOrSaleOrTransfer.push('Mint');
  } else if (to === ZERO_ADDRESS) {
    mintOrSaleOrTransfer.push('Burn');
  } else if (value === 'Sale') {
    mintOrSaleOrTransfer.push('Sale');
  } else if (value === 'Bid Won') {
    mintOrSaleOrTransfer.push('Bid Won');
  } else {
    mintOrSaleOrTransfer.push('Transfer/Sale');
  }
  
  return mintOrSaleOrTransfer;
}


/**
 * Retrieves token details from a contract using its ABI.
 * @param {string} tokenABIPath - The file path of the token ABI.
 * @param {string} contract - The contract address.
 * @returns {Promise<{ name: string, symbol: string }>} - A promise that resolves to an object containing the token name and symbol.
 */
static async getTokenDetails(tokenABIPath, contract) {
  //console.log('Entering getTokenDetails method.');

  let name = '';
  let symbol = '';

  try {
    const tokenABI = JSON.parse(fs.readFileSync(tokenABIPath, 'utf8'));
    const instance = new web3.eth.Contract(tokenABI, contract);
    name = await instance.methods.name().call() || '';
    symbol = await instance.methods.symbol().call() || '';

    //console.log('Token name:', name);
    //console.log('Token symbol:', symbol);
  } catch (error) {
    //console.error('Error retrieving token details:', error);
  }

  //console.log('Exiting getTokenDetails method.');

  return {
    name,
    symbol
  };
}





}

module.exports = TransactionHelper;
