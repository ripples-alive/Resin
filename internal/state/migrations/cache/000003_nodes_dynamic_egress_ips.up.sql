ALTER TABLE nodes_dynamic
ADD COLUMN egress_ips_json TEXT NOT NULL DEFAULT '[]';