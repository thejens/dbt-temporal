-- Seed data for the multi-project example (Postgres).
-- Run: psql $POSTGRES_URL -f examples/multi-project/seed-postgres.sql

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

CREATE TABLE IF NOT EXISTS raw.bikeshare_trips (
    trip_id INTEGER PRIMARY KEY,
    start_station_id INTEGER,
    duration_minutes DOUBLE PRECISION,
    start_time TIMESTAMP,
    subscriber_type TEXT
);

TRUNCATE raw.bikeshare_trips;

INSERT INTO raw.bikeshare_trips VALUES
    (1, 1, 25.5, '2021-03-15 08:30:00', 'Annual'),
    (2, 2, 12.0, '2021-03-15 09:00:00', 'Casual'),
    (3, 1, 45.0, '2021-03-15 10:15:00', 'Annual'),
    (4, 3, 8.5, '2021-03-16 07:45:00', 'Annual'),
    (5, 4, 60.0, '2021-03-16 11:00:00', 'Casual'),
    (6, 2, 15.0, '2021-03-16 14:30:00', 'Annual'),
    (7, 5, 33.0, '2021-03-17 08:00:00', 'Casual'),
    (8, 1, 18.0, '2021-03-17 09:30:00', 'Annual');
