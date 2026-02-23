-- Seed data for the jaffle-shop-classic example.
-- Pre-creates the raw data tables that the original jaffle-shop loads via dbt seeds.

CREATE TABLE IF NOT EXISTS pg_matviews (
    schemaname VARCHAR,
    matviewname VARCHAR,
    matviewowner VARCHAR,
    tablespace VARCHAR,
    hasindexes BOOLEAN,
    ispopulated BOOLEAN,
    definition VARCHAR
);

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.raw_customers (
    id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT
);

INSERT INTO raw.raw_customers VALUES
    (1, 'Michael', 'P.'),
    (2, 'Shawn', 'M.'),
    (3, 'Kathleen', 'P.'),
    (4, 'Jimmy', 'C.'),
    (5, 'Katherine', 'R.'),
    (6, 'Sarah', 'R.'),
    (7, 'Martin', 'M.'),
    (8, 'Frank', 'R.'),
    (9, 'Jennifer', 'F.'),
    (10, 'Henry', 'W.');

CREATE TABLE raw.raw_orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    order_date DATE,
    status TEXT
);

INSERT INTO raw.raw_orders VALUES
    (1, 1, '2018-01-01', 'returned'),
    (2, 3, '2018-01-02', 'completed'),
    (3, 4, '2018-01-04', 'completed'),
    (4, 1, '2018-01-05', 'completed'),
    (5, 2, '2018-01-05', 'completed'),
    (6, 3, '2018-01-07', 'completed'),
    (7, 7, '2018-01-09', 'completed'),
    (8, 2, '2018-01-11', 'returned'),
    (9, 9, '2018-01-12', 'completed'),
    (10, 7, '2018-01-14', 'completed');

CREATE TABLE raw.raw_payments (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    payment_method TEXT,
    amount INTEGER
);

-- amounts are in cents (original jaffle-shop convention)
INSERT INTO raw.raw_payments VALUES
    (1, 1, 'credit_card', 1000),
    (2, 2, 'credit_card', 2000),
    (3, 3, 'coupon', 100),
    (4, 4, 'coupon', 2500),
    (5, 5, 'bank_transfer', 1700),
    (6, 6, 'credit_card', 600),
    (7, 7, 'credit_card', 1600),
    (8, 8, 'credit_card', 2300),
    (9, 9, 'gift_card', 2300),
    (10, 9, 'bank_transfer', 0),
    (11, 10, 'bank_transfer', 2600);
