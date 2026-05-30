DROP INDEX IF EXISTS idx_subscription_nodes_hash_active;
DROP INDEX IF EXISTS idx_nodes_dynamic_next_latency_probe_due;

ALTER TABLE nodes_dynamic
DROP COLUMN next_latency_probe_due_ns;
