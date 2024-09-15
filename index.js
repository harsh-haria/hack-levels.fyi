const fs = require('fs');
const axios = require('axios');
const mysql = require('mysql2/promise');
const express = require('express');
const redis = require('redis');

const PORT = process.env.PORT || 3000;

const app = express();

const filePath = './downloaded_file.csv';

const redisClient = redis.createClient({
    host: 'redis',
    port: 6379
});

redisClient.on('error', (err) => console.log('Redis Client Error', err));

// Database configuration
const dbConfig = {
    host: 'mysql-db',
    // host: "localhost", //dev
    user: 'dbadmin',
    password: 'dbadmin',
    database: 'hack',
    infileStreamFactory: path => fs.createReadStream(path)
};

// Function to download the file first
async function downloadFile(csvUrl, filePath) {
    const response = await axios({
        url: csvUrl,
        method: 'GET',
        responseType: 'stream',
    });

    const writer = fs.createWriteStream(filePath);

    return new Promise((resolve, reject) => {
        response.data.pipe(writer);
        writer.on('finish', resolve);
        writer.on('error', reject);
    });
}

async function loadCsvIntoDatabase(filePath, connection) {
    try {
        const query = `
            LOAD DATA LOCAL INFILE ?
            INTO TABLE sensor_data
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS
            (id, type, subtype, reading, location, timestamp);
        `;
        await connection.query(query, [filePath]);
    } catch (error) {
        console.error(error);
        console.log('bulk csv upload doesnt work');
        throw error;
    }
}

app.post('/injestion', async (req, res) => {
    try {

        // FIRE SET GLOBAL local_infile=ON;
        // INCRASER THE SIZE OF INNODB BUFFER

        let url = req.query.url;
        const connection = await mysql.createConnection(dbConfig);
        console.log('Streaming and inserting CSV data...');

        const start = performance.now();

        await downloadFile(url, filePath);

        await loadCsvIntoDatabase(filePath, connection);

        const end = performance.now();

        try {
            // Flush all keys in the currently selected database
            const succeeded = await redisClient.flushAll();
            console.log("Redis flushed:", succeeded); // Should print 'OK' if successful
        } catch (err) {
            console.error('Failed to flush Redis:', err);
        }

        console.log(`Data inserted successfully. Time taken: ${(end - start) / 1000}`);
        return res.status(200).json({ message: 'Data inserted successfully' });
    } catch (error) {
        // await conenction.rollback();
        console.log('rollback');
        console.error(error);
        return res.status(500).json({ error: 'Internal server error' });
    }
});


async function getFilteredData(filters, connection) {
    const { id, type, subtype, location } = filters;

    // Build the base WHERE clause and prepare the filters
    let whereClause = '';
    let whereClauseBase = ' WHERE 1=1 ';
    let whereClauseAddition = '';
    let filterValues = [];

    if (id && id.length) {
        let questions = `(` + id.map(() => '?').join(',') + ')';
        whereClauseAddition += ` AND id IN ${questions}`;
        filterValues.push(id);
    }

    if (type && type.length) {
        let questions = `(` + type.map(() => '?').join(',') + ')';
        whereClauseAddition += ` AND type IN ${questions}`;
        filterValues.push(type);
    }

    if (subtype && subtype.length) {
        let questions = `(` + subtype.map(() => '?').join(',') + ')';
        whereClauseAddition += ` AND subtype IN ${questions}`;
        filterValues.push(subtype);
    }

    if (location && location.length) {
        let questions = `(` + location.map(() => '?').join(',') + ')';
        whereClauseAddition += ` AND location IN ${questions}`;
        filterValues.push(location);
    }

    if (whereClauseAddition) {
        whereClause = whereClauseBase + whereClauseAddition;
    }

    const countQuery = `SELECT COUNT(*) AS rowCount FROM sensor_data ${whereClause}`;

    // Query to get the median
    let medianQuery = `
        WITH ordered_readings AS (
            SELECT reading, ROW_NUMBER() OVER (ORDER BY reading) AS row_num,
                COUNT(*) OVER() AS total_rows
            FROM sensor_data
            ${whereClause}
        )
        SELECT AVG(reading) AS median
        FROM ordered_readings
        WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2));

    `;

    try {

        filterValues = filterValues.flat();

        // Execute both queries in parallel
        const [[countResult], [medianResult]] = await Promise.all([
            connection.execute(countQuery, filterValues),
            connection.execute(medianQuery, filterValues)
        ]);

        await connection.end();

        return {
            count: countResult[0].rowCount,
            median: medianResult[0].median
        };

    } catch (error) {
        console.error("Error fetching data: ", error);
        throw error;
    }
}


app.get('/median', async (req, res) => {
    try {
        let input = req.query.filters || '{}';
        const filters = JSON.parse(input);
        const filterKey = `median:${input}`;

        let cachedData;
        try {
            cachedData = await redisClient.get(filterKey);
            if (cachedData) {
                console.log('Returning cached data.');
                return res.status(200).json(JSON.parse(cachedData));
            }
        } catch (redisErr) {
            console.error('Redis GET error:', redisErr);
        }

        const connection = await mysql.createConnection(dbConfig);
        let data = await getFilteredData(filters, connection);
        await connection.end();

        try {
            await redisClient.set(filterKey, JSON.stringify(data));
        } catch (redisErr) {
            console.error('Redis SET error:', redisErr);
        }

        return res.status(200).json(data);
    } catch (error) {
        console.error(error);
        return res.status(500).send();
    }
});


app.listen(PORT, () => {
    redisClient.connect()
        .then(() => console.log('Connected to Redis'))
        .catch(err => console.log('Failed to connect to Redis', err));
    console.log(`Server is running on port ${PORT}`);
});