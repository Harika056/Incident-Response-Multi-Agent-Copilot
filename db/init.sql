-- OrderHub schema + a read-only diagnostic role for the copilot.
CREATE TABLE IF NOT EXISTS orders (
  id         BIGSERIAL PRIMARY KEY,
  sku        TEXT        NOT NULL,
  qty        INTEGER     NOT NULL,
  status     TEXT        NOT NULL DEFAULT 'NEW',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_created_at_idx ON orders (created_at DESC);

CREATE TABLE IF NOT EXISTS inventory (
  sku   TEXT PRIMARY KEY,
  stock INTEGER NOT NULL
);
INSERT INTO inventory(sku, stock) VALUES ('SKU-1', 1000000), ('SKU-2', 1000000), ('SKU-3', 1000000)
ON CONFLICT DO NOTHING;

-- Read-only role used by the diagnostic MCP server (can see pg_stat_activity via pg_monitor).
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'diag_ro') THEN
    CREATE ROLE diag_ro LOGIN PASSWORD 'diag_ro';
  END IF;
END $$;
GRANT pg_monitor TO diag_ro;
GRANT CONNECT ON DATABASE orderhub TO diag_ro;
GRANT USAGE ON SCHEMA public TO diag_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO diag_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO diag_ro;
ALTER ROLE diag_ro SET default_transaction_read_only = on;
