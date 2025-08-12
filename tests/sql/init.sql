-- Initialize database schema and test data for SyncTool tests

-- Create source schema and tables
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS public.user_profiles (
    user_id INTEGER PRIMARY KEY REFERENCES public.users(id),
    email VARCHAR(50)
);

-- Create destination schema and table
CREATE SCHEMA IF NOT EXISTS public_copy;

CREATE TABLE IF NOT EXISTS public_copy.user_profile (
    user_id INTEGER PRIMARY KEY,
    email VARCHAR(50),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    status VARCHAR(50),
    checksum VARCHAR(50),
    name VARCHAR(100)  -- result of enrichment: first_name + last_name
);

-- Insert test data into source tables
INSERT INTO public.users (id, first_name, last_name, created_at, updated_at, status)
SELECT
    i,
    'FirstName_' || i,
    'LastName_' || i,
    NOW() - (i || ' days')::INTERVAL,
    NOW() - (i % 5 || ' hours')::INTERVAL,
    CASE WHEN i % 2 = 0 THEN 'active' ELSE 'inactive' END
FROM generate_series(1, 100) AS s(i)
ON CONFLICT (id) DO NOTHING;

-- Insert into public.user_profiles
INSERT INTO public.user_profiles (user_id, email)
SELECT
    i,
    'user' || i || '@example.com'
FROM generate_series(1, 100) AS s(i)
ON CONFLICT (user_id) DO NOTHING;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON SCHEMA public TO rohitanand;
GRANT ALL PRIVILEGES ON SCHEMA public_copy TO rohitanand;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO rohitanand;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public_copy TO rohitanand;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO rohitanand;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public_copy TO rohitanand;
