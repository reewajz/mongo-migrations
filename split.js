const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream');
const { promisify } = require('util');
const JSONStream = require('JSONStream');
const { assoc, pipe, path: ramdaPath, omit, mergeAll } = require('ramda');


const pipelineAsync = promisify(pipeline);

const CHUNK_SIZE = 100 * 1024 * 1024; // 100MB in bytes
const inputDir = './singleExport';
const outputDir = './splittedExport';
const JSONPath = 'json.path.here';

function transformDocument(data, type) {
  // some transform logic here
  return data;
}

async function splitJsonFile(inputFilePath, outputDir, chunkSize) {
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
  }

  const fileStream = fs.createReadStream(inputFilePath, { encoding: 'utf-8' });
  const jsonStream = JSONStream.parse(JSONPath);
  let chunkIndex = 0;
  let currentChunkSize = 0;
  let currentChunk = [];

  jsonStream.on('data', (data) => {
    const transformedData = transformDocument(data, 'SKU');
    const jsonData = JSON.stringify(transformedData);
    currentChunkSize += Buffer.byteLength(jsonData, 'utf-8');

    if (currentChunkSize >= chunkSize) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const chunkFilePath = path.join(outputDir, `chunk_${chunkIndex}_${timestamp}.json`);
      fs.writeFileSync(chunkFilePath, JSON.stringify(currentChunk, null, 2));
      console.log(`Created chunk file: ${chunkFilePath}`);
      chunkIndex++;
      currentChunkSize = Buffer.byteLength(jsonData, 'utf-8');
      currentChunk = [transformedData];
    } else {
      currentChunk.push(transformedData);
    }
  });

  jsonStream.on('end', () => {
    if (currentChunk.length > 0) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const chunkFilePath = path.join(outputDir, `chunk_${chunkIndex}${timestamp}.json`);
      fs.writeFileSync(chunkFilePath, JSON.stringify(currentChunk, null, 2));
      console.log(`Created chunk file: ${chunkFilePath}`);
    }
  });

  await pipelineAsync(fileStream, jsonStream);
}

async function processAllFiles(inputDir, outputDir, chunkSize) {
  const files = fs.readdirSync(inputDir).filter(file => file.endsWith('.json'));

  for (const file of files) {
    const inputFilePath = path.join(inputDir, file);
    console.log(`Processing file: ${inputFilePath}`);
    await splitJsonFile(inputFilePath, outputDir, chunkSize);
  }
}

processAllFiles(inputDir, outputDir, CHUNK_SIZE).catch(console.error);