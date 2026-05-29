CREATE TABLE IF NOT EXISTS raw_repositories (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    full_name VARCHAR(255),
    owner_login VARCHAR(255),
    stargazers_count INT,
    created_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW()
);