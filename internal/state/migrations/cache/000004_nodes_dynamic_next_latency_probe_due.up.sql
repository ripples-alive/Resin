ALTER TABLE nodes_dynamic
ADD COLUMN next_latency_probe_due_ns INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_nodes_dynamic_next_latency_probe_due
ON nodes_dynamic(next_latency_probe_due_ns);

CREATE INDEX IF NOT EXISTS idx_subscription_nodes_hash_active
ON subscription_nodes(node_hash, subscription_id)
WHERE evicted = 0;
