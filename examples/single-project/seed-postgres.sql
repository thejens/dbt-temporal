-- Seed data for the single-project example (Postgres).
-- Run: psql $POSTGRES_URL -f examples/single-project/seed-postgres.sql

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.bikeshare_stations (
    station_id INTEGER PRIMARY KEY,
    name TEXT,
    status TEXT,
    number_of_docks INTEGER
);

TRUNCATE raw.bikeshare_stations;

INSERT INTO raw.bikeshare_stations VALUES
    (1, 'Downtown', 'active', 15),
    (2, 'University', 'active', 11),
    (3, 'Zilker Park', 'closed', 19),
    (4, 'Congress Ave', 'active', 13),
    (5, 'East Side', 'active', 9);
