const express = require('express');
const mysql = require('mysql2/promise');
const https = require('https');
const csv = require('csv-parser');
const { performance } = require('perf_hooks');
// const util = require('util');

const PORT = 8080;

const helpers = require('./main.js');

const app = express();

let con;
// let rollback;
// let commit;
// let beginTransaction;
// let query;

async function dbConnector () {
  con = await mysql.createConnection({
    host: 'localhost',
    user: 'dbadmin',
    password: 'dbadmin',
    database: 'hack',
  });
  // con.connect(function (err) {
  //   if (err) throw err;
  console.log(`Connected to the MySQL database`);
  // });
  // query = util.promisify(con.query).bind(con);
  // beginTransaction = util.promisify(con.beginTransaction).bind(con);
  // commit = util.promisify(con.commit).bind(con);
  // rollback = util.promisify(con.rollback).bind(con);
}

dbConnector();

const BATCH_SIZE = 1000;

// Function to insert batch data into MySQL
async function insertBatch(con, records) {
    if (records.length === 0) return;

    // Generate query placeholders for the batch
    const placeholders = records.map(() => '(?, ?, ?, ?, ?, ?)').join(',');
    const query = `INSERT INTO sensor_data (id, type, subtype, reading, location, timestamp) VALUES ${placeholders}`;
    const params = records.flat();  // Flatten the array of arrays

    // Execute the batch insert
    try {
        await con.execute(query, params);
        console.log(`Inserted ${records.length} records`);
    } catch (error) {
        console.error('Error inserting batch:', error);
        throw error;  // Re-throw the error to roll back the transaction
    }
}


async function streamCSVAndInsert(url) {
    // let count = 1;
    let batch = [];
    await con.beginTransaction();
    return new Promise((resolve, reject) => {
        https.get(url, (response) => {
            try {

                response.pipe(csv()).on('data', async (row) => {
                    // Insert each row into the database
                    const { id, type, subtype, reading, location, timestamp } = row;
    
                    // console.log('Parsed row:', row);  // Log each parsed row for debugging
    
                    // Push the row data into the batch
                    batch.push([id, type, subtype, reading, location, timestamp]);
    
                    // When the batch reaches the BATCH_SIZE, insert into MySQL
                    if (batch.length === BATCH_SIZE) {
                        // console.log('Batch size reached, inserting into MySQL...');
                        await insertBatch(con, batch);
                        batch = [];  // Clear the batch after insertion
                    }
                })
                .on('end', async () => {
                    // Insert any remaining records in the last batch
                    if (batch.length > 0) {
                        // console.log('Inserting remaining batch into MySQL...');
                        await insertBatch(con, batch);
                    }
    
                    // Commit the transaction after all batches are inserted
                    await con.commit();
                    console.log('All data inserted successfully!');
                    resolve();
                })
                .on('error', async (error) => {
                    console.error('Error processing CSV:', error);
                    await con.rollback();  // Roll back the transaction on error
                    reject(error);
                });
            } catch (error) {
                
            }
        });
    });
}

app.get('/test', (req, res) => {
    return res.status(200).json({message: 'Server is running'});
});

app.post('/injestion_old', async (req, res) => {
    try {
        let url = req.query.url;
        console.log('Streaming and inserting CSV data...');
        const start = performance.now();
        await streamCSVAndInsert(url);
        const end = performance.now();
        console.log(`Data inserted successfully. Time taken: ${(end-start)/1000}`);
        return res.status(200).json({message: 'Data inserted successfully'});
    } catch (error) {
        console.error(error);
        return res.status(500).json({error: 'Internal server error'});
    }
});

app.post('/injestion', async (req, res) => {
  try {
      let url = req.query.url;
      console.log('Streaming and inserting CSV data...');
      const start = performance.now();
      await helpers.processCsvData(con, url);
      const end = performance.now();
      console.log(`Data inserted successfully. Time taken: ${(end-start)/1000}`);
      return res.status(200).json({message: 'Data inserted successfully'});
  } catch (error) {
      console.error(error);
      return res.status(500).json({error: 'Internal server error'});
  }
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});