//DO: 'https://dofktr:dofktr2023@fktrnode1.nfthing.com'
//AWS: 'https://fktr@nfthing.com:fktr@2023@fktrnode.nfthing.com'
//personal: mongodb+srv://naga:nagafktr2023@cluster0.98zwq6r.mongodb.net/?retryWrites=true&w=majority
//AWS MONGODB: mongodb://Kushal:Kushal_FKTR@13.233.203.198:27017/
//DigitalOceanMongoDb:   mongodb+srv://doadmin:5Q3D1R2e49o7cp6v@fktrnode2023mongodb-07e504fb.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=fktrnode2023mongodb
//End Point DO: ws://127.0.0.1:3334

const rpcConfig = {
  RPC_URL_HTTP_ENDPOINT: 'https://dofktr:dofktr2023@fktrnode1.nfthing.com',
  RPC_URL_WSS_ENDPOINT: 'ws://127.0.0.1:3334'
};


const mongoConfig = {
  mongoUrl: 'mongodb+srv://doadmin:5Q3D1R2e49o7cp6v@fktrnode2023mongodb-07e504fb.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=fktrnode2023mongodb',
  dbName: 'transactionDbNaga',
  collectionName: 'transaction5Days'
};

// Number of hours to fetch ERC721 transactions from
const hours = 1;

module.exports = {
  rpcConfig,
  mongoConfig,
  hours
};
