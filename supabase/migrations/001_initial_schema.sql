-- =====================================================
-- Pokemon Card Price Tracker - Initial Schema
-- =====================================================
-- This migration creates the core tables needed for the daily price update cron job

-- Groups table (card sets/expansions)
CREATE TABLE IF NOT EXISTS groups (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  category_id INTEGER NOT NULL,  -- 3 = English, 85 = Japanese
  published_on DATE,
  modified_on TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_groups_category_id ON groups(category_id);

-- Products table (individual cards)
CREATE TABLE IF NOT EXISTS products (
  product_id INTEGER NOT NULL,
  variant_key TEXT PRIMARY KEY,  -- productId:finish (e.g., "12345:Normal" or "12345:Holofoil")
  name TEXT NOT NULL,
  group_id INTEGER NOT NULL REFERENCES groups(id),

  -- Current market price
  market_price NUMERIC,

  -- Historical prices stored as JSONB
  -- Format: {"2024-02-08": 3.51, "2024-02-09": 3.48, ...}
  price_history JSONB DEFAULT '{}'::jsonb,

  -- Cached metrics (calculated by batch_update_price_history)
  -- 1-day changes
  chg_1d_pct NUMERIC,
  chg_1d_abs NUMERIC,

  -- 3-day changes
  chg_3d_pct NUMERIC,
  chg_3d_abs NUMERIC,

  -- 7-day changes
  chg_7d_pct NUMERIC,
  chg_7d_abs NUMERIC,

  -- 1-month changes
  chg_1m_pct NUMERIC,
  chg_1m_abs NUMERIC,

  -- 3-month changes
  chg_3m_pct NUMERIC,
  chg_3m_abs NUMERIC,

  -- 6-month changes
  chg_6m_pct NUMERIC,
  chg_6m_abs NUMERIC,

  -- 1-year changes
  chg_1y_pct NUMERIC,
  chg_1y_abs NUMERIC,

  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_products_product_id ON products(product_id);
CREATE INDEX IF NOT EXISTS idx_products_group_id ON products(group_id);
CREATE INDEX IF NOT EXISTS idx_products_market_price ON products(market_price);

-- Optional: Create indexes on change metrics for sorting/filtering
CREATE INDEX IF NOT EXISTS idx_products_chg_1d_pct ON products(chg_1d_pct);
CREATE INDEX IF NOT EXISTS idx_products_chg_1d_abs ON products(chg_1d_abs);
CREATE INDEX IF NOT EXISTS idx_products_chg_7d_pct ON products(chg_7d_pct);
CREATE INDEX IF NOT EXISTS idx_products_chg_1m_pct ON products(chg_1m_pct);

COMMENT ON TABLE groups IS 'Pokemon card sets/expansions from TCGPlayer';
COMMENT ON TABLE products IS 'Individual Pokemon cards with price history and metrics';
COMMENT ON COLUMN products.variant_key IS 'Unique identifier: productId:finish (e.g., "12345:Normal")';
COMMENT ON COLUMN products.price_history IS 'JSONB object mapping dates to prices: {"2024-02-08": 3.51}';
COMMENT ON COLUMN products.chg_1d_pct IS '1-day price change percentage';
COMMENT ON COLUMN products.chg_1d_abs IS '1-day price change absolute ($)';
