const { v4: uuidv4 } = require('uuid');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Helper function to generate a random integer between min and max (inclusive)
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Helper function to generate a random timestamp between 2001 and 2005
function getRandomTimestamp() {
    const start = new Date(2001, 0, 1).getTime();
    const end = new Date(2005, 11, 31).getTime();
    let date = new Date(getRandomInt(start, end)).toISOString();
    date = date.replace('T', ' ');
    date = date.replace('Z', '');
    return date;
}

// Pre-generate the sets with the required cardinalities
const types = Array.from({ length: 1000 }, () => uuidv4());
const subtypes = Array.from({ length: 1000 }, () => uuidv4());
const locations = Array.from({ length: 100000 }, () => uuidv4());

const csvWriter = createCsvWriter({
    path: 'test_data_10M.csv',
    header: [
        { id: 'id', title: 'id' },
        { id: 'type', title: 'type' },
        { id: 'subtype', title: 'subtype' },
        { id: 'reading', title: 'reading' },
        { id: 'location', title: 'location' },
        { id: 'timestamp', title: 'timestamp' }
    ]
});

// Generate 1M records
async function generateCSVData() {
    const records = [];

    for (let i = 0; i < 10000000; i++) {
        records.push({
            id: uuidv4(),
            type: types[getRandomInt(0, types.length - 1)],
            subtype: subtypes[getRandomInt(0, subtypes.length - 1)],
            reading: getRandomInt(1, 1_000_000_000), // Reading between 1 and 1 billion
            location: locations[getRandomInt(0, locations.length - 1)],
            timestamp: getRandomTimestamp() // Random timestamp between 2001 and 2005
        });

        // Writing data in chunks to avoid memory overflow
        if (records.length === 10000) {
            await csvWriter.writeRecords(records);
            records.length = 0; // Clear the array
        }
    }

    // Write the remaining records
    if (records.length) {
        await csvWriter.writeRecords(records);
    }

    console.log('CSV file created successfully!');
}

generateCSVData().catch((err) => {
    console.error('Error generating CSV:', err);
});
