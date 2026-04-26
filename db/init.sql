CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    url TEXT UNIQUE,
    description TEXT,
    source TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

