USE cdc_demo;

-- Insert a new record
INSERT INTO customers (id, name, email) VALUES (4, 'David Evans', 'david@example.com');

-- Update an existing record
UPDATE customers SET name = 'Alice W. Smith', email = 'awsmith@example.com' WHERE id = 1;

-- Soft delete a record
UPDATE customers SET is_deleted = TRUE WHERE id = 2;
