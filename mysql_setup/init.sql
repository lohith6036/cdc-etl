CREATE DATABASE IF NOT EXISTS cdc_demo;
USE cdc_demo;

-- Create a sample customers table
CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);

-- For testing initial load (Batch 1)
INSERT INTO customers (id, name, email) VALUES
(1, 'Alice Smith', 'alice@example.com'),
(2, 'Bob Jones', 'bob@example.com'),
(3, 'Charlie Brown', 'charlie@example.com')
ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email);
