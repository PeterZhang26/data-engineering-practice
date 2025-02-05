DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;

CREATE TABLE accounts (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address_1 VARCHAR(100),
    address_2 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    join_date DATE
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_code VARCHAR(50),
    product_description VARCHAR(100)
);

CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    transaction_date DATE,
    product_id INT REFERENCES products(product_id),
    product_code VARCHAR(50),
    product_description VARCHAR(100),
    quantity INT,
    account_id INT REFERENCES accounts(customer_id)
);

CREATE INDEX idx_transactions_product ON transactions(product_id);
CREATE INDEX idx_transactions_account ON transactions(account_id);