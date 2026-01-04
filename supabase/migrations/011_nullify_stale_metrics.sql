-- =====================================================
-- Recalculate Stale Metrics (With Overflow Protection)
-- =====================================================
-- This function replaces 'nullify_stale_metrics'.
-- Instead of wiping metrics, it RECALCULATES them for products that weren't updated.
-- It assumes the last known 'market_price' is still valid today, and compares it
-- against history relative to NOW().
--
-- UPDATE: Added clamping to [-9999.999, 9999.999] to prevent numeric overflow errors.

DROP FUNCTION IF EXISTS nullify_stale_metrics(TIMESTAMPTZ);
DROP FUNCTION IF EXISTS recalculate_stale_metrics(TIMESTAMPTZ); -- Drop old signature to avoid ambiguity

CREATE OR REPLACE FUNCTION recalculate_stale_metrics(cutoff_time TIMESTAMPTZ, batch_size INTEGER DEFAULT 1000)
RETURNS TABLE(updated_count INTEGER) AS $$
DECLARE
  v_count INTEGER;
BEGIN
  -- Batch update stale metrics
  -- We select a batch of keys first to avoid locking the whole table or timing out
  
  WITH stale_products AS (
    SELECT variant_key
    FROM products
    WHERE updated_at < cutoff_time
    LIMIT batch_size
    FOR UPDATE SKIP LOCKED -- Optional: helps if running parallel, but mainly just selects a batch
  ),
  updated_rows AS (
    UPDATE products p
    SET
      -- 1-day changes (Relative to NOW)
      chg_1d_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 0, 2) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 0, 2) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 0, 2)) / get_nearest_price_in_range(p.price_history, now()::DATE, 0, 2)) * 100))
        ELSE NULL END,
      chg_1d_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 0, 2) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 0, 2)
        ELSE NULL END,

      -- 3-day changes
      chg_3d_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 2, 4) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 2, 4) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 2, 4)) / get_nearest_price_in_range(p.price_history, now()::DATE, 2, 4)) * 100))
        ELSE NULL END,
      chg_3d_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 2, 4) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 2, 4)
        ELSE NULL END,

      -- 7-day changes
      chg_7d_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 5, 9) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 5, 9) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 5, 9)) / get_nearest_price_in_range(p.price_history, now()::DATE, 5, 9)) * 100))
        ELSE NULL END,
      chg_7d_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 5, 9) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 5, 9)
        ELSE NULL END,

      -- 1-month changes
      chg_1m_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 25, 35) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 25, 35) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 25, 35)) / get_nearest_price_in_range(p.price_history, now()::DATE, 25, 35)) * 100))
        ELSE NULL END,
      chg_1m_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 25, 35) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 25, 35)
        ELSE NULL END,

      -- 3-month changes
      chg_3m_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 80, 100) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 80, 100) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 80, 100)) / get_nearest_price_in_range(p.price_history, now()::DATE, 80, 100)) * 100))
        ELSE NULL END,
      chg_3m_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 80, 100) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 80, 100)
        ELSE NULL END,

      -- 6-month changes
      chg_6m_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 170, 190) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 170, 190) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 170, 190)) / get_nearest_price_in_range(p.price_history, now()::DATE, 170, 190)) * 100))
        ELSE NULL END,
      chg_6m_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 170, 190) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 170, 190)
        ELSE NULL END,

       -- 1-year changes
      chg_1y_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 350, 380) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 350, 380) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 350, 380)) / get_nearest_price_in_range(p.price_history, now()::DATE, 350, 380)) * 100))
        ELSE NULL END,
      chg_1y_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 350, 380) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 350, 380)
        ELSE NULL END,

      -- IMPORTANT: We MUST update the 'updated_at' timestamp for these rows
      -- Otherwise they will remain "stale" and be picked up in the next batch forever,
      -- leading to an infinite loop.
      -- We set it to NOW() to mark them as "processed" for this cutoff_time check.
      -- Or better: We set it to NEW value?
      -- Wait, if we set updated_at to NOW(), they are "fresh".
      -- Is that what we want? Yes. A "stale" product that has just had its metrics recalculated
      -- IS arguably "updated".
      -- AND this is critical for the batching logic to progress.
      updated_at = now()

    FROM stale_products
    WHERE p.variant_key = stale_products.variant_key
    RETURNING 1
  )
  SELECT count(*) INTO v_count FROM updated_rows;

  RETURN QUERY SELECT v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION recalculate_stale_metrics IS 'Recalculates change metrics for stale products by comparing their last known market_price against history relative to TODAY.';
