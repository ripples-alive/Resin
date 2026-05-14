CREATE TABLE IF NOT EXISTS subscriptions (
	id                           TEXT PRIMARY KEY,
	name                         TEXT NOT NULL,
	source_type                  TEXT NOT NULL DEFAULT 'remote',
	url                          TEXT NOT NULL,
	content                      TEXT NOT NULL DEFAULT '',
	update_interval_ns           INTEGER NOT NULL,
	enabled                      INTEGER NOT NULL DEFAULT 1,
	ephemeral                    INTEGER NOT NULL DEFAULT 0,
	ephemeral_node_evict_delay_ns INTEGER NOT NULL,
	created_at_ns                INTEGER NOT NULL,
	updated_at_ns                INTEGER NOT NULL
);

ALTER TABLE subscriptions
ADD COLUMN last_checked_ns INTEGER NOT NULL DEFAULT 0;

ALTER TABLE subscriptions
ADD COLUMN last_updated_ns INTEGER NOT NULL DEFAULT 0;

ALTER TABLE subscriptions
ADD COLUMN last_error TEXT NOT NULL DEFAULT '';
