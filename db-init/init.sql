-- Create the table for our incoming transactions
CREATE TABLE IF NOT EXISTS transactions (
    signature VARCHAR(150) PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    fee_payer VARCHAR(150) NOT NULL,
    fee BIGINT NOT NULL,
    slot VARCHAR(50),
    -- We use JSONB to store nested array data efficiently, a great feature of Postgres
    instructions JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);