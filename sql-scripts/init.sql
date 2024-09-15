CREATE DATABASE IF NOT EXISTS hack;

USE hack;

CREATE TABLE IF NOT EXISTS sensor_data (
    id VARCHAR(40),
    type VARCHAR(40),
    subtype VARCHAR(40),
    reading INT,
    location VARCHAR(40),
    timestamp TIMESTAMP
);

CREATE INDEX idx_id ON sensor_data (id);

CREATE INDEX idx_type ON sensor_data (type);

CREATE INDEX idx_subtype ON sensor_data (subtype);

CREATE INDEX idx_location ON sensor_data (location);