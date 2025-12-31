CREATE TABLE IF NOT EXISTS fraud_scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag INTEGER NOT NULL CHECK (fraud_flag IN (0, 1)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
