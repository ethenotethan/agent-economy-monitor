CREATE TABLE IF NOT EXISTS rpc_collection_cursors (
    chain text PRIMARY KEY CHECK (chain IN ('ethereum', 'base', 'solana', 'tempo')),
    next_height bigint NOT NULL CHECK (next_height >= 0),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);
