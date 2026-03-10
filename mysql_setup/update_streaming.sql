USE cdc_demo;

-- Insert a new record
INSERT INTO customers (id, name, email) VALUES (8, 'Frank White', 'frank@example.com');

-- Update an existing record
UPDATE customers SET email = 'awsmith_updated@example.com' WHERE id = 1;

-- Hard delete a record (to test Debezium 'd' op)
DELETE FROM customers WHERE id = 4;
