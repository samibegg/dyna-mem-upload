const express = require('express');
const multer = require('multer');
const path = require('path');
const axios = require('axios');
const app = express();
const port = 3000;
const fs = require('fs');
const readline = require('readline');
const csv = require('csv-parser');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient;
const csvParser = require('csv-parser');
const stream = require('stream');
require('dotenv').config();

const uri = process.env.TRADEDB_URI;
const databaseName = 'trades'; // Replace with your db name

// Set up multer for memory storage (instead of storing on disk)
const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

// Set EJS as the view engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, '../views')); // Use path.join for correct path

// Render the file upload form
app.get('/', (req, res) => {
  res.render('index');
});

// Handle file upload and CSV parsing
app.post('/upload', upload.single('csvfile'), (req, res) => {
  if (!req.file) {
    return res.status(400).send('No file uploaded');
  }

  // Convert the buffer to a stream
  const bufferStream = new stream.PassThrough();
  bufferStream.end(req.file.buffer); // Pass the file buffer to the stream

  const accountTradeHistory = [];
  let isAccountTradeHistoryFound = false;
  let accountTradeHistoryHeaders = null;
  let inAccountTradeHistorySection = false;
  let doneAccountTradeHistorySection = false;

  const futuresOptions = [];
  let isFuturesOptionsFound = false;
  let futuresOptionsHeaders = null;
  let inFuturesOptionsSection = false;
  let doneFuturesOptionsSection = false;


  // Parse the CSV data from the buffer stream
  bufferStream
    .pipe(csvParser({ headers: false })) // Disable automatic header parsing
    .on('data', (row) => {
      // Check if we've found the "Account Trade History" line
      if (!isAccountTradeHistoryFound) {
        if (row['0'] === 'Account Trade History') {
          isAccountTradeHistoryFound = true; // We found the start of the section
        }
        return; 
      }

      // The line directly after "Account Trade History" is the header row
      if (!accountTradeHistoryHeaders) {
        // Use this row as the headers
        accountTradeHistoryHeaders = Object.values(row);
        inAccountTradeHistorySection = true;
        console.log('AT Headers encountered.');
        return; 
      }

      if (isAccountTradeHistoryFound && accountTradeHistoryHeaders && !doneAccountTradeHistorySection) {
      // Check if the row is blank (all fields are empty)
        if (Object.values(row).every(value => value.trim() === '')) {
            console.log('AT Blank line encountered.');
            inAccountTradeHistorySection = false;
            doneAccountTradeHistorySection = true;
            return;
        }
        if (inAccountTradeHistorySection) {
        // Process the data rows after headers are found
            const dataRow = {};
      		accountTradeHistoryHeaders.forEach((header, index) => {
               // Map the data to the corresponding header
        	   dataRow[header] = row[`${index}`] || null; 
            });
            accountTradeHistory.push(dataRow);
        }
      }
      
      // Check if we've found the "Futures Options" line
      if (!isFuturesOptionsFound) {
        if (row['0'] === 'Futures Options') {
          isFuturesOptionsFound = true; // We found the start of the section
        }
        return; 
      }

      // The line directly after "Futures Options" is the header row
      if (!futuresOptionsHeaders) {
        // Use this row as the headers
        futuresOptionsHeaders = Object.values(row); 
        inFuturesOptionsSection = true;
        console.log('FO Headers encountered.');
        return; 
      }

      if (isFuturesOptionsFound && futuresOptionsHeaders && !doneFuturesOptionsSection) {
      // Check if the row is blank (all fields are empty)
        if (Object.values(row).every(value => value.trim() === '')) {
            console.log('FO Blank line encountered.');
            inFuturesOptionsSection = false;
            doneFuturesOptionsSection = true;
            return;
        }
        if (inFuturesOptionsSection) {
        // Process the data rows after headers are found
            const dataRow = {};
      		futuresOptionsHeaders.forEach((header, index) => {
               // Map the data to the corresponding header
        	   dataRow[header] = row[`${index}`] || null; 
            });
            futuresOptions.push(dataRow);
        }
      }

    })
    .on('end', async () => {
        console.log('CSV file processed, updating JSON...');

        // Iterate through the array and update specific fields
        let contractPrice;
        let execTime;
        let execDate;
        let spread;
        let orderId;

        accountTradeHistory.forEach(contract => {
          if (contract["Exec Time"] === null) {
            // these are not the first record in the trade ...
        } else {
            // The first record in the trade contains Exec Time and Spread, used for the remaining records in the trade, if any
            execDate = contract["Exec Time"].split(" ")[0];
            execTime = contract["Exec Time"].split(" ")[1];
            spread = contract.Spread;
            // add a new field with a random string to be orderId
            orderId = generateRandomString();
        }
            // Copied field from the first record in the trade
            contract["Exec Time"] = execTime;
            contract["Exec Date"] = execDate;
            contract.Spread = spread;
            contract.orderId = orderId;
        });

		// Insert raw data
  		await storeToDB(accountTradeHistory, false, 'raw_contracts');
  		await storeToDB(futuresOptions, false, 'raw_futures_options');

  		await storeToDB('../defaults/Expiration_Calendar.json', true, 'expirations_calendar');
  		await storeToDB('../defaults/US_XCME_daily.json', true, 'us_xcme_daily');
 		await storeToDB('../defaults/Market_Perspectives.json', true, 'market_perspectives');

		// Execute transformations
  		await runAggregation('raw_contracts', '../aggregations/processed_contracts.json'); 
		await runAggregation('processed_contracts', '../aggregations/option_lifecycle.json'); 
  		await runAggregation('processed_contracts', '../aggregations/option_legs.json'); 
  		await runAggregation('raw_contracts', '../aggregations/orders.json'); 

  		await mergeRelatedOrders();

  		await runAggregation('trade_journeys', '../aggregations/trade_journeys.json'); 
  		await runAggregation('trade_journeys', '../aggregations/calculate_exercise_value.json'); 
  		await runAggregation('trade_journeys', '../aggregations/trade_journey_w_exercise.json'); 

        await runAggregation('raw_futures_options', '../aggregations/processed_futures_options.json'); 
  
       // Send back the JSON response
       // res.json(accountTradeHistory);
       // res.json(futuresOptions);
   })
    .on('error', (err) => {
      console.error('CSV Parsing Error:', err);
      res.status(500).send('Error processing the file');
    });
});

function generateRandomString(length = 20) {
  const characters = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

// Start the server
app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});

async function storeToDB(jsonObj, isFile, collectionName){
  const client = new MongoClient(uri, {useUnifiedTopology: true});

  try {
    await client.connect();
    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    if (isFile) {
      const data = JSON.parse(fs.readFileSync(jsonObj));
      await collection.insertMany(data);
      console.log('JSON File loaded successfully');
    } else {
      await collection.insertMany(jsonObj);
      console.log('JSON Array loaded successfully');
    }      
  } catch (err) {
    console.error('Error loading JSON:', err);
  } finally {
    client.close();
  }

}

app.get('/reset', (req, res) => {
  resetDB();
  res.render('index', { list: ["Collections removed successfully"] });
});

async function mergeRelatedOrders() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    const db = client.db(databaseName);
    const ordersCollection = db.collection('orders');  // Replace with your collection name
    const mergedCollection = db.collection('trade_journeys');  // New collection for merged results

    // Step 1: Fetch all documents and their related order arrays
    const orders = await ordersCollection.find().toArray();

    // Step 2: Union-Find Data Structure to handle merging
    class UnionFind {
      constructor(n) {
        this.parent = Array(n).fill(null).map((_, i) => i); // Initially, each document is its own parent
      }

      find(x) {
        if (this.parent[x] !== x) {
          this.parent[x] = this.find(this.parent[x]); // Path compression
        }
        return this.parent[x];
      }

      union(x, y) {
        const rootX = this.find(x);
        const rootY = this.find(y);

        if (rootX !== rootY) {
          this.parent[rootX] = rootY;  // Merge the two groups
        }
      }
    }

    // Step 3: Initialize Union-Find structure for each document
    const uf = new UnionFind(orders.length);

    // Step 4: Compare documents and union those with common strings
    for (let i = 0; i < orders.length; i++) {
      for (let j = i + 1; j < orders.length; j++) {
        // Check if there's any common string between the two arrays
        const hasCommon = orders[i].related_orders_by_code.some(code =>
          orders[j].related_orders_by_code.includes(code)
        );

        if (hasCommon) {
          // Union the documents if they share a common string
          uf.union(i, j);
        }
      }
    }

    // Step 5: Group documents by their root parent
    const groups = {};

    for (let i = 0; i < orders.length; i++) {
      const root = uf.find(i);
      if (!groups[root]) {
        groups[root] = new Set();
      }
      orders[i].related_orders_by_code.forEach(code => groups[root].add(code));
    }

    // Step 6: Prepare the merged documents for insertion
    const mergedDocs = Object.values(groups).map(group => ({
      related_orders_by_code: [...group], // Convert Set to array
    }));

    // Step 7: Insert the merged documents into the new collection
    await mergedCollection.insertMany(mergedDocs);

    console.log('Merged documents inserted successfully');
  } catch (err) {
    console.error('Error merging orders:', err);
  } finally {
    await client.close();
  }
}

async function runAggregation(collectionName, aggFileName) {
  const client = new MongoClient(uri);

  try {
    // Connect to the MongoDB server
    await client.connect();

    // Access the desired database and collection
    const db = client.db(databaseName);
    const collection = db.collection(collectionName); 

    // Synchronously read the file
    const data = fs.readFileSync(aggFileName, 'utf8');
    
    // Define the aggregation pipeline
    const pipeline = JSON.parse(data);
    //console.log("PIPELINE: "+pipeline); // Output the contents of the file

    // Perform the aggregation
    const result = await collection.aggregate(pipeline).toArray();

    // Log the result
    console.log('Aggregation ' + aggFileName + ' completed successfully');
    console.log(result);
  } catch (err) {
    console.error('Error during aggregation:', err);
  } finally {
    // Close the connection
    await client.close();
  }
}

async function resetDB(){  
  const client = new MongoClient(uri, {useUnifiedTopology: true});

  try {
    await client.connect();
    const db = client.db(databaseName);

 	await db.collection("orders").drop();
 	await db.collection("raw_contracts").drop();
 	await db.collection("processed_contracts").drop();
 	await db.collection("option_legs").drop();
  	await db.collection("option_lifecycle").drop();
 	await db.collection("expirations_calendar").drop();
 	await db.collection("us_xcme_daily").drop();
 	await db.collection("market_perspectives").drop();
 	await db.collection("trade_journeys").drop();
 	await db.collection("calculate_exercise_value").drop();
 	await db.collection("raw_futures_options").drop();
 	await db.collection("processed_futures_options").drop();

    console.log('Collections removed successfully');
  } catch (err) {
    console.error('Error loading JSON files:', err);
  } finally {
    client.close();
  }
  
}

async function splitCsv(csvFilePath, sectionName) {
    let inSection = false;

	try {
	  // Create a readable stream for the input file
	  const inputStream = fs.createReadStream('uploads/'+csvFilePath, 'utf8');

	  // Create a writable stream for the output file
	  const outputStream = fs.createWriteStream('uploads/'+sectionName+'.csv');

	  // Create a readline interface
	  const rl = readline.createInterface({
  	    input: inputStream,
        output: process.stdout,
        terminal: false // This prevents readline from printing input
      });

	  // Define the string condition
	  const condition = sectionName; 

	  // Read the file line by line
	  rl.on('line', (line) => {
  	    // Write all lines in the section 
  	    if (inSection) {
          // Write the line to the output file
          outputStream.write(line + '\n');
        }
  	    // Check if the line contains the section heading
  	    if (line.includes(condition)) {
          // Flag that we are in the section
          inSection = true;
        }
  	    // Check for blank line (end of section) 
  	    if (line.trim() === '') {
          // Flag that we are done with section
          inSection = false;
        }
      });

	  // Handle the end of the input file
	  rl.on('close', () => {
	    outputStream.end(); // Close the output stream
	  });

	  console.log('CSV saved to '+sectionName+'.csv');
	  
	} catch (err) {
  	  console.error('Error occurred:', err);
	}
}

async function csvToJson(csvFilePath, jsonFilePath) {
    const results = [];
    let currentSection = [];
    let headers = null;
    let inSection = false;

    const stream = fs.createReadStream('uploads/'+csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
            // If it's an empty row or a section break
            if (Object.keys(row).length === 0 || Object.values(row).every(cell => cell === '')) {
                if (currentSection.length > 0) {
                    results.push({ Contracts: currentSection });
                    currentSection = [];
                }
                inSection = false;
            } else {
                // If headers haven't been set yet, assume the first row is the header
                if (!headers) {
                    headers = Object.keys(row);
                }
                // Map the row data to a JSON object
                const rowData = headers.reduce((acc, header) => {
                    acc[header] = row[header];
                    return acc;
                }, {});
                currentSection.push(rowData);
                inSection = true;
            }
        })
        .on('end', () => {
            // Push the last section if any data remains
            if (currentSection.length > 0) {
                results.push({ Contracts: currentSection });
            }

            // Write the JSON to a file
            fs.writeFileSync('uploads/'+jsonFilePath, JSON.stringify(results, null, 4), 'utf-8');
        });

        console.log('JSON saved to '+jsonFilePath);
}

module.exports = app;

