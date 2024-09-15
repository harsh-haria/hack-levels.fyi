const fs = require('fs');
const axios = require('axios');
const mysql = require('mysql2/promise');
const express = require('express');
const redis = require('redis');
const cluster = require('cluster');
const os = require('os');

var availableCpus = os.cpus().length;

const PORT = process.env.PORT || 3000;

const filePath = './downloaded_file.csv';

if (cluster.isMaster) {
    for (let i = 0; i < availableCpus; i++) {
        cluster.fork();
    }
    cluster.on('online', function (worker) {
        console.log(`worker: ${worker.process.pid} is online`);
    })

    cluster.on('exit', (worker, code, signal) => {
        if (code !== 0 && !worker.exitedAfterDisconnect) {
            console.log(`Starting new worker: ${worker.process.pid}, signal: ${signal}`);
            cluster.fork();
        }
    })
}
else {
    const app = express();

    const redisClient = redis.createClient({
        socket: {
            host: 'redis',
            port: 6379
        }
    });

    redisClient.on('error', (err) => console.log('Redis Client Error', err));

    // const dbConfig = {
    //     host: 'mysql-db',
    //     // host: "localhost", //dev
    //     user: 'dbadmin',
    //     password: 'dbadmin',
    //     database: 'hack',
    //     infileStreamFactory: path => fs.createReadStream(path)
    // };

    const pool = mysql.createPool({
        host: 'mysql-db', //prod
        // host: "localhost", //dev
        user: 'dbadmin',
        password: 'dbadmin',
        database: 'hack',
        waitForConnections: true,
        connectionLimit: 3,
        queueLimit: 0,
        infileStreamFactory: path => fs.createReadStream(path),
    });

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
            await connection.beginTransaction();
            await connection.query(query, [filePath]);
            await connection.commit();
            return;
        } catch (error) {
            console.error(error);
            console.log('bulk csv upload doesnt work');
            throw error;
        }
    }

    app.post('/injestion', async (req, res) => {
        let connection;
        try {
            let url = req.query.url;
            connection = await pool.getConnection();

            const start = performance.now();

            console.log('downloading CSV...');
            await downloadFile(url, filePath);

            console.log('Inserting records now...');
            await loadCsvIntoDatabase(filePath, connection);

            const end = performance.now();

            console.log(`Data inserted successfully. Time taken: ${(end - start) / 1000}`);

            try {
                const succeeded = await redisClient.flushAll();
                console.log("Redis flushed:", succeeded);
            } catch (err) {
                console.error('Failed to flush Redis:', err);
            }

            return res.status(200).json({ message: 'Data inserted successfully' });
        } catch (error) {
            if (connection) await connection.rollback();
            console.error(error);
            return res.status(500).json({ error: 'Internal server error' });
        }
        finally {
            if (connection) connection.release();
        }
    });


    async function getFilteredData(filters, connection) {
        const { id, type, subtype, location } = filters;

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

            const [[countResult], [medianResult]] = await Promise.all([
                connection.execute(countQuery, filterValues),
                connection.execute(medianQuery, filterValues)
            ]);


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
        let connection;
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

            connection = await pool.getConnection();
            let data = await getFilteredData(filters, connection);

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
        finally {
            if (connection) connection.release();
        }

    });

    async function testDatabaseConnectionWithRetry(retries = 12, delay = 5000) {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const connection = await pool.getConnection();
                console.log('Successfully connected to the database');
                connection.release();
                return true;
            } catch (error) {
                console.error(`Failed to connect to the database (Attempt ${attempt} of ${retries}):`, error.message);
                if (attempt < retries) {
                    console.log(`Retrying in ${delay / 1000} seconds...`);
                    await new Promise(resolve => setTimeout(resolve, delay));
                } else {
                    console.error('All retry attempts failed. Could not connect to the database.');
                    return false;
                }
            }
        }
    }


    async function startServer() {
        const connected = await testDatabaseConnectionWithRetry();
        if (connected) {
            app.listen(PORT, () => {
                redisClient.connect()
                    .then(() => console.log('Connected to Redis'))
                    .catch(err => console.log('Failed to connect to Redis', err));
                console.log(`Server is running on port ${PORT}`);
            });
        } else {
            console.error('Unable to start server due to database connection failure.');
        }
    }

    startServer();
}