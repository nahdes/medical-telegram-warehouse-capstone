-- Setup script for PostgreSQL database
-- Run this before loading data

-- Create database (run as superuser)
-- CREATE DATABASE medical_warehouse;

-- Connect to the database
-- \c medical_warehouse

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS seeds;
CREATE SCHEMA IF NOT EXISTS test_failures;

-- Grant permissions (adjust user as needed)
GRANT ALL ON SCHEMA raw TO postgres;
GRANT ALL ON SCHEMA staging TO postgres;
GRANT ALL ON SCHEMA marts TO postgres;
GRANT ALL ON SCHEMA seeds TO postgres;
GRANT ALL ON SCHEMA test_failures TO postgres;

-- Create extension for better text search (optional)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Display created schemas
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('raw', 'staging', 'marts', 'seeds', 'test_failures')
ORDER BY schema_name;