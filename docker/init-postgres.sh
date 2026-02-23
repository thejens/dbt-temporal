#!/bin/bash
# Creates the databases used by the example projects.
# Mounted as /docker-entrypoint-initdb.d/init.sh — runs automatically on first start.
set -e

for db in single_project multi_project env_override jaffle_shop; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
        CREATE DATABASE $db;
SQL
done
