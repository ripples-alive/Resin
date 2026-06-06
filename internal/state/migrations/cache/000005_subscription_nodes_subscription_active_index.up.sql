CREATE INDEX IF NOT EXISTS idx_subscription_nodes_subscription_active
ON subscription_nodes(subscription_id)
WHERE evicted = 0;
