const { Web3 } = require('web3');
const { rpcConfig } = require('./config');

let web3Provider; // Declare the web3Provider variable

/**
 * Connects to the specified Web3 provider based on the provided provider type.
 *
 * @param {string} providerType - The type of Web3 provider (either 'http' or 'ws').
 */
function connectWeb3(providerType) {
  const { RPC_URL_HTTP_ENDPOINT, RPC_URL_WSS_ENDPOINT } = rpcConfig;

  if (providerType === 'http') {
    // Connect using HTTP provider
    web3Provider = new Web3.providers.HttpProvider(RPC_URL_HTTP_ENDPOINT);
  } else if (providerType === 'ws') {
    // Connect using WebSocket provider
    web3Provider = new Web3.providers.WebsocketProvider(
      RPC_URL_WSS_ENDPOINT,
      {
        clientConfig: {
          keepalive: true,
          keepaliveInterval: 60000,
        },
        reconnect: {
          auto: true,
          delay: 5000,
          maxAttempts: 5,
          onTimeout: false,
        },
		timeout: 5000, // 5 seconds in milliseconds
      }
    );

    // Event listeners for WebSocket provider
    web3Provider.on('connect', () => {
      console.log('Websocket connected.');
    });

    web3Provider.on('close', (event) => {
      console.log(event);
      console.log('Websocket closed.');
    });

    web3Provider.on('error', (error) => {
      console.error(error);
    });
  }
}

/**
 * Disconnects the current Web3 provider.
 */
function disconnectWeb3() {
  if (web3Provider) {
    if (web3Provider.disconnect) {
      web3Provider.disconnect();
    } else if (web3Provider.connection && web3Provider.connection.close) {
      web3Provider.connection.close();
    }
    console.log('Web3 provider disconnected.');
  }
}

/**
 * Returns an instance of Web3 connected to the specified provider.
 *
 * @param {string} providerType - The type of Web3 provider (either 'http' or 'ws').
 * @returns {Web3} - An instance of Web3 connected to the specified provider.
 */
function getWeb3Instance(providerType) {
  if (!web3Provider) {
    connectWeb3(providerType);
  }

  const web3 = new Web3(web3Provider);
  return web3;
}

module.exports = { getWeb3Instance, connectWeb3, disconnectWeb3 };
