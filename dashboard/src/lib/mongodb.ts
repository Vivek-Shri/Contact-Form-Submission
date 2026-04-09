import { MongoClient } from 'mongodb';

if (!process.env.MONGODB_URI) {
  // Set default URI similar to the Python backend
  process.env.MONGODB_URI = 'mongodb://127.0.0.1:27017/outreach';
}

const uri = process.env.NODE_ENV === 'development' ? 'mongodb://127.0.0.1:27017/outreach' : (process.env.MONGODB_URI as string);
const options = {};

let client;
let clientPromise: Promise<MongoClient>;

if (process.env.NODE_ENV === 'development') {
  let globalWithMongo = global as typeof globalThis & {
    _mongoClientPromise?: Promise<MongoClient>
  }
  if (!globalWithMongo._mongoClientPromise) {
    client = new MongoClient(uri, options);
    globalWithMongo._mongoClientPromise = client.connect();
  }
  clientPromise = globalWithMongo._mongoClientPromise;
} else {
  client = new MongoClient(uri, options);
  clientPromise = client.connect();
}

export default clientPromise;
