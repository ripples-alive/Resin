ALTER TABLE nodes_dynamic
ADD COLUMN next_latency_probe_due_ns INTEGER NOT NULL DEFAULT 0;

UPDATE nodes_dynamic
SET next_latency_probe_due_ns = last_latency_probe_attempt_ns +
	(3600000000000 * CASE
		WHEN failure_count <= 0 THEN 1
		WHEN failure_count = 1 THEN 2
		WHEN failure_count = 2 THEN 4
		WHEN failure_count = 3 THEN 8
		WHEN failure_count = 4 THEN 16
		ELSE 32
	END)
WHERE last_latency_probe_attempt_ns > 0;

CREATE INDEX IF NOT EXISTS idx_nodes_dynamic_next_latency_probe_due
ON nodes_dynamic(next_latency_probe_due_ns);

CREATE INDEX IF NOT EXISTS idx_subscription_nodes_hash_active
ON subscription_nodes(node_hash, subscription_id)
WHERE evicted = 0;
