#!/bin/bash
set -e

function create_user_and_database() {
    local database=$1
    echo "Creating user and database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE USER ${database}_user WITH PASSWORD '${database}123';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO ${database}_user;
        
        -- Connect to the new database and set up extensions
        \c $database;
        
        -- Enable common extensions
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
        CREATE EXTENSION IF NOT EXISTS "btree_gin";
        CREATE EXTENSION IF NOT EXISTS "btree_gist";
        
        -- Grant usage on extensions
        GRANT USAGE ON SCHEMA public TO ${database}_user;
        GRANT CREATE ON SCHEMA public TO ${database}_user;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_user_and_database $db
    done
    echo "Multiple databases created"
fi