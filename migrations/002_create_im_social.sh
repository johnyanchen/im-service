#!/bin/bash
# Create im_social database and apply its migrations
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE im_social;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname im_social < /docker-entrypoint-initdb.d/im_social/001_init.sql
