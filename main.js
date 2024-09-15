// const mysql = require('mysql2/promise');
const axios = require('axios');
const csvParser = require('csv-parser');
const csv = require('csv-parser');
const { Transform } = require('stream');

// Database configuration (use your details)
// const dbConfig = {
//     host: 'localhost',
//     user: 'your_db_user',
//     password: 'your_db_password',
//     database: 'your_database_name',
// };

// Batch size for inserts
const BATCH_SIZE = 1000;

// Function to insert batch data into MySQL
async function insertBatch(connection, records) {
    if (records.length === 0) return;

    // Generate query placeholders for the batch
    const placeholders = records.map(() => '(?, ?, ?, ?, ?, ?)').join(',');
    const query = `INSERT INTO sensor_data (id, type, subtype, reading, location, timestamp) VALUES ${placeholders}`;
    const params = records.flat();  // Flatten the array of arrays

    // Execute the batch insert
    try {
        await connection.execute(query, params);
    } catch (error) {
        console.error('Error inserting batch:', error);
        throw error;  // Re-throw the error to roll back the transaction
    }
}

// Main function to handle the CSV download, parsing, and insertion
async function processCsvData(connection, csvUrl) {
    // Create a connection to the MySQL database
    // const connection = await mysql.createConnection(dbConfig);

    try {
        // Start a transaction
        await connection.beginTransaction();

        // Array to hold the current batch of records
        let batch = [];

        // Fetch the CSV from the provided URL and process it in a stream
        // const response = await axios({
        //     method: 'get',
        //     url: csvUrl,
        //     responseType: 'stream',
        // });
        const response = await axios.get(csvUrl, { responseType: 'stream' });

        // Create a transform stream to handle CSV rows as they are read
        response.data
            .pipe(csv())
            .on('data', async (row) => {
                // Assuming the CSV has columns: id, type, subtype, reading, location, timestamp
                const { id, type, subtype, reading, location, timestamp } = row;

                console.log('Parsed row:', row);  // Log each parsed row for debugging

                // Push the row data into the batch
                batch.push([id, type, subtype, reading, location, timestamp]);

                // When the batch reaches the BATCH_SIZE, insert into MySQL
                if (batch.length === BATCH_SIZE) {
                    console.log('Batch size reached, inserting into MySQL...');
                    await insertBatch(connection, batch);
                    batch = [];  // Clear the batch after insertion
                }
            })
            .on('end', async () => {
                // Insert any remaining records in the last batch
                if (batch.length > 0) {
                    console.log('Inserting remaining batch into MySQL...');
                    await insertBatch(connection, batch);
                }

                // Commit the transaction after all batches are inserted
                await connection.commit();
                console.log('All data inserted successfully!');
            })
            .on('error', async (error) => {
                console.error('Error processing CSV:', error);
                await connection.rollback();  // Roll back the transaction on error
            });

    } catch (error) {
        // Roll back the transaction if an error occurs during any part of the process
        if (connection) {
            console.error('Error during the transaction, rolling back...', error);
            await connection.rollback();
        }
    } finally {
        // Close the database connection
        if (connection) {
            await connection.end();
            console.log('Database connection closed.');
        }
    }
}

// Example URL for the CSV file (Replace with actual URL)

// Call the main function to process the CSV
// processCsvData(csvUrl).catch((err) => console.error('Error in processing:', err));
module.exports = {
    insertBatch,
    processCsvData
};