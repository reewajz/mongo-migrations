const { exec } = require('child_process');
const { promisify } = require('util');
const fs = require('fs');
const path = require('path');

const execAsync = promisify(exec);

async function runMongoImport(uri, dbName, collectionName, filePath) {
  const command = `mongoimport --uri "${uri}" --db ${dbName} --collection ${collectionName} --file ${filePath} --jsonArray`;
  try {
    const { stdout, stderr } = await execAsync(command);
    console.log('stdout output:', stdout);
    if (stderr) {
      console.error('stderr output:', stderr);
    }
  } catch (error) {
    console.error('Error running mongoimport:', error);
  }
}

async function importFromSplittedExport(uri, dbName, collectionName, folderPath) {
  const files = fs.readdirSync(folderPath).filter(file => file.endsWith('.json'));

  const importPromises = files.map(file => {
    const filePath = path.join(folderPath, file);
    console.log(`Importing ${file} to ${collectionName}...`);
    return runMongoImport(uri, dbName, collectionName, filePath);
  });

  await Promise.all(importPromises);
}


const uri = 'mongodb://localhost:27017';
const dbName = 'dbname';
const collectionName = 'collname';
const folderPath = './splittedExport';

importFromSplittedExport(uri, dbName, collectionName, folderPath).catch(console.error);