CREATE TABLE IF NOT EXISTS repo_analytics (
    owner_login String,
    avg_stars_per_repo Float64,
    total_stars UInt64,
    repo_count UInt32,
    updated_at Date
) ENGINE = MergeTree()
ORDER BY (owner_login, updated_at);