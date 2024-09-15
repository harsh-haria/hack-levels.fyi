const express = require('express');
const mysql = require('mysql2/promise');
const https = require('https');
const csv = require('csv-parser');
const { performance } = require('perf_hooks');
const util = require('util');

const PORT = 8080;

const helpers = require('./main.js');

const app = express();

let con;
// let rollback;
// let commit;
// let beginTransaction;
// let query;

async function dbConnector() {
  con = await mysql.createConnection({
    host: 'mysql-db',
    user: 'dbadmin',
    password: 'dbadmin',
    database: 'hack',
  });
  // con.connect(function (err) {
  //   if (err) throw err;
  console.log(`Connected to the MySQL database`);
  // });
  query = util.promisify(con.query).bind(con);
  beginTransaction = util.promisify(con.beginTransaction).bind(con);
  commit = util.promisify(con.commit).bind(con);
  rollback = util.promisify(con.rollback).bind(con);
}

dbConnector();

async function streamCSVAndInsert(url) {
  let count = 1;
  return new Promise((resolve, reject) => {

    https.get(url, (response) => {
      response.pipe(csv()).on('data', async (row) => {
        // Insert each row into the database
        const sql = `INSERT INTO sensor_data (id, type, subtype, reading, location, timestamp) VALUES (?, ?, ?, ?, ?, ?)`;
        try {
          console.log(`Inserting row(${count++})`);
          await query(sql, [row.id, row.type, row.subtype, row.reading, row.location, row.timestamp]);
        } catch (error) {
          console.error('Error inserting row:', error);
          reject(error);  // Reject if any insert fails
        }
      })
        .on('end', () => {
          console.log('CSV file successfully processed');

          resolve();  // Resolve when the stream ends
        })
        .on('error', (error) => {
          console.error('Error streaming CSV:', error);

          reject(error);  // Handle errors during the stream
        });
    });
  });
}

app.get('/test', (req, res) => {
  return res.status(200).json({ message: 'Server is running' });
});

app.post('/injestion_old', async (req, res) => {
  try {
    let url = req.query.url;
    console.log('Streaming and inserting CSV data...');
    const start = performance.now();
    beginTransaction();
    await streamCSVAndInsert(url);
    commit();
    const end = performance.now();
    console.log(`Data inserted successfully. Time taken: ${(end - start) / 1000}`);
    return res.status(200).json({ message: 'Data inserted successfully' });
  } catch (error) {
    rollback();
    console.error(error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/injestion', async (req, res) => {
  try {
    let url = req.query.url;
    console.log('Streaming and inserting CSV data...');
    const start = performance.now();
    await helpers.processCsvData(con, url);
    const end = performance.now();
    console.log(`Data inserted successfully. Time taken: ${(end - start) / 1000}`);
    return res.status(200).json({ message: 'Data inserted successfully' });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

// Main function to process the downloaded file
async function processCsvFile(filePath, csvUrl) {
  const connection = await mysql.createConnection(dbConfig);
  let batch = [];

  try {
    await connection.beginTransaction();

    const response = await axios.get(csvUrl, { responseType: 'stream' });

    // return fs.createReadStream(filePath)
    return fastcsv.parseStream(response.data, { headers: true })
      .on('data', async (row) => {
        const { id, type, subtype, reading, location, timestamp } = row;
        batch.push([id, type, subtype, reading, location, timestamp]);

        if (batch.length === BATCH_SIZE) {
          await insertBatch(connection, batch);
          batch = [];
        }
      })
      .on('end', async () => {
        if (batch.length > 0) {
          await insertBatch(connection, batch);
        }
        await connection.commit();
        console.log('All data inserted and commited successfully!');
      })
      .on('error', async (error) => {
        console.error('Error processing CSV:', error);
        await connection.rollback();
      });
  } catch (error) {
    console.error('Transaction error:', error);
    await connection.rollback();
  }
}

async function getFilteredRecords(filters, connection) {
  try {



    // Start building the query
    let whereClauses = [];
    let values = [];

    // Dynamically add conditions based on the presence of filters
    if (filters.id && filters.id.length > 0) {
      whereClauses.push('id IN (?)');
      values.push(filters.id);
    }

    if (filters.type && filters.type.length > 0) {
      whereClauses.push('type IN (?)');
      values.push(filters.type);
    }

    if (filters.subtype && filters.subtype.length > 0) {
      whereClauses.push('subtype IN (?)');
      values.push(filters.subtype);
    }

    if (filters.location && filters.location.length > 0) {
      whereClauses.push('location IN (?)');
      values.push(filters.location);
    }

    // Construct the WHERE clause, if there are conditions
    let where = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';

    // Create the SQL query
    const query = `
      WITH ordered_data AS (
        SELECT reading,
              ROW_NUMBER() OVER (ORDER BY reading) AS row_num,
              COUNT(*) OVER () AS total_rows
        FROM sensor_data
        ${where}
        )
        SELECT 
            COUNT(*) AS count,
            AVG(reading) AS median
        FROM (
            SELECT reading
            FROM ordered_data
            WHERE row_num IN (
                (total_rows + 1) / 2,  -- Odd case
                (total_rows + 2) / 2   -- Even case, handles the middle two rows
            )
        ) AS median_data;

    `;

    // Execute the query
    const [rows] = await connection.execute(query, values);

    // Send the result
    return rows[0];
  } catch (error) {
    console.error('Error executing query', error);
    return;
  }
}


async function getFilteredData(filters, connection) {
  // Destructure filters
  const { id, type, subtype, location } = filters;

  // Construct the base SQL query with placeholders
  let baseQuery = `
        SELECT COUNT(*) AS rowCount, 
        IFNULL(
            (
                SELECT AVG(reading) FROM (
                    SELECT reading 
                    FROM sensor_data 
                    WHERE 1=1
                    ${id.length ? ' AND id IN (?)' : ''}
                    ${type.length ? ' AND type IN (?)' : ''}
                    ${subtype.length ? ' AND subtype IN (?)' : ''}
                    ${location.length ? ' AND location IN (?)' : ''}
                    ORDER BY reading
                    LIMIT 1 OFFSET (
                        SELECT FLOOR((COUNT(*) - 1) / 2) FROM sensor_data 
                        WHERE 1=1
                        ${id.length ? ' AND id IN (?)' : ''}
                        ${type.length ? ' AND type IN (?)' : ''}
                        ${subtype.length ? ' AND subtype IN (?)' : ''}
                        ${location.length ? ' AND location IN (?)' : ''}
                    )
                ) AS median
            ), 0
        ) AS median
        FROM sensor_data 
        WHERE 1=1
        ${id.length ? ' AND id IN (?)' : ''}
        ${type.length ? ' AND type IN (?)' : ''}
        ${subtype.length ? ' AND subtype IN (?)' : ''}
        ${location.length ? ' AND location IN (?)' : ''}
    `;

  // Build filter values array
  let filterValues = [];
  if (id.length) filterValues.push(id);
  if (type.length) filterValues.push(type);
  if (subtype.length) filterValues.push(subtype);
  if (location.length) filterValues.push(location);

  try {

    // Execute the query
    console.log(baseQuery);
    const [rows] = await connection.execute(baseQuery, filterValues.concat(filterValues, filterValues, filterValues));

    await connection.end(); // Close the connection

    return rows[0]; // Return the row count and median result

  } catch (error) {
    console.error("Error fetching data: ", error);
    throw error; // Handle or throw errors as necessary
  }
}

// Batch size for inserts
const BATCH_SIZE = 1000;


// Function to insert batch data into MySQL
async function insertBatch(connection, records) {
  if (records.length === 0) return;

  const placeholders = records.map(() => '(?, ?, ?, ?, ?, ?)').join(',');
  const query = `INSERT INTO sensor_data (id, type, subtype, reading, location, timestamp) VALUES ${placeholders}`;
  const params = records.flat();

  try {
    await connection.execute(query, params);
  } catch (error) {
    console.error('Error inserting batch:', error);
    await connection.rollback();
    throw error;
  }
}