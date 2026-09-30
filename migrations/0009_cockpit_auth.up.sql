BEGIN;

CREATE TABLE agent_economy.auth_sessions (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces(namespace_id) ON DELETE CASCADE,
    token_hash text NOT NULL CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    csrf_hash text NOT NULL CHECK (csrf_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at),
    PRIMARY KEY (namespace_id, token_hash)
);

CREATE INDEX auth_sessions_expiry_idx
    ON agent_economy.auth_sessions (namespace_id, expires_at);

CREATE TABLE agent_economy.auth_login_limits (
    namespace_id uuid PRIMARY KEY REFERENCES agent_economy.namespaces(namespace_id) ON DELETE CASCADE,
    window_started_at timestamptz NOT NULL,
    attempt_count smallint NOT NULL CHECK (attempt_count BETWEEN 0 AND 5)
);

COMMIT;
