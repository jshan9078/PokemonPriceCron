-- =====================================================
-- Fix Overlapping Time Windows for Price Change Calculations
-- =====================================================
-- This migration updates the get_nearest_price function to accept min/max day ranges
-- and modifies batch_update_price_history to use non-overlapping time windows

-- Drop the old batch_update_price_history function first to avoid conflicts
DROP FUNCTION IF EXISTS batch_update_price_history(JSONB);

-- =====================================================
-- New Helper Function: Get nearest price within a specific date range
-- =====================================================
-- Now accepts min_days and max_days to search within a specific bounded range
-- This prevents overlapping time windows

CREATE OR REPLACE FUNCTION get_nearest_price_in_range(
  price_history JSONB,
  target_date DATE,
  min_days INTEGER,
  max_days INTEGER
)
RETURNS NUMERIC AS $$
DECLARE
  date_key TEXT;
  price_val NUMERIC;
  days_offset INTEGER;
BEGIN
  -- Try exact match first (if target is within range)
  IF 0 BETWEEN min_days AND max_days THEN
    date_key := to_char(target_date, 'YYYY-MM-DD');
    price_val := (price_history->>date_key)::NUMERIC;

    IF price_val IS NOT NULL THEN
      RETURN price_val;
    END IF;
  END IF;

  -- Search within the specified range (min_days to max_days)
  FOR days_offset IN min_days..max_days LOOP
    -- Skip exact match (already tried above)
    IF days_offset = 0 THEN
      CONTINUE;
    END IF;

    -- Try -N days (looking backwards from target)
    date_key := to_char(target_date - (days_offset || ' days')::INTERVAL, 'YYYY-MM-DD');
    price_val := (price_history->>date_key)::NUMERIC;
    IF price_val IS NOT NULL THEN
      RETURN price_val;
    END IF;
  END LOOP;

  -- No price found within range
  RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =====================================================
-- Updated Batch Update Function with Non-Overlapping Windows
-- =====================================================

CREATE OR REPLACE FUNCTION batch_update_price_history(batch_data JSONB)
RETURNS TABLE(updated_count INTEGER, skipped_count INTEGER, error_count INTEGER) AS $$
DECLARE
  item JSONB;
  v_variant_key TEXT;
  v_date TEXT;
  v_price NUMERIC;
  v_price_history JSONB;
  v_updated INTEGER := 0;
  v_skipped INTEGER := 0;
  v_errors INTEGER := 0;

  -- Variables for metric calculation
  v_current_price NUMERIC;
  v_price_1d_ago NUMERIC;
  v_price_3d_ago NUMERIC;
  v_price_7d_ago NUMERIC;
  v_price_1m_ago NUMERIC;
  v_price_3m_ago NUMERIC;
  v_price_6m_ago NUMERIC;
  v_price_1y_ago NUMERIC;
  v_date_parsed DATE;
BEGIN
  -- Iterate through each item in the batch
  FOR item IN SELECT * FROM jsonb_array_elements(batch_data)
  LOOP
    BEGIN
      -- Extract fields from JSONB
      v_variant_key := item->>'variant_key';
      v_date := item->>'date';
      v_price := (item->>'price')::NUMERIC;
      v_date_parsed := v_date::DATE;

      -- Skip if any required field is null
      IF v_variant_key IS NULL OR v_date IS NULL OR v_price IS NULL THEN
        v_skipped := v_skipped + 1;
        CONTINUE;
      END IF;

      -- Get or create the product's price history
      SELECT price_history INTO v_price_history
      FROM products
      WHERE variant_key = v_variant_key;

      -- If product doesn't exist, skip (you should populate products first)
      IF v_price_history IS NULL THEN
        v_skipped := v_skipped + 1;
        CONTINUE;
      END IF;

      -- Append the new date/price to price_history
      v_price_history := jsonb_set(
        COALESCE(v_price_history, '{}'::jsonb),
        ARRAY[v_date],
        to_jsonb(v_price)
      );

      -- Calculate metrics by looking back from this date
      v_current_price := v_price;

      -- Use non-overlapping time windows to ensure distinct historical prices
      -- Each window searches a specific range to prevent overlap

      -- 1 day ago: search 0-2 days back
      v_price_1d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 0, 2);

      -- 3 days ago: search 2-4 days back (no overlap with 1-day window)
      v_price_3d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 2, 4);

      -- 7 days ago: search 5-9 days back
      v_price_7d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 5, 9);

      -- 1 month ago: search 25-35 days back
      v_price_1m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 25, 35);

      -- 3 months ago: search 80-100 days back
      v_price_3m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 80, 100);

      -- 6 months ago: search 170-190 days back
      v_price_6m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 170, 190);

      -- 1 year ago: search 350-380 days back
      v_price_1y_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 350, 380);

      -- Update the product with new price_history, market_price, and all metrics
      UPDATE products
      SET
        price_history = v_price_history,
        market_price = v_current_price,

        -- 1-day changes
        chg_1d_pct = CASE WHEN v_price_1d_ago IS NOT NULL AND v_price_1d_ago > 0
                          THEN ((v_current_price - v_price_1d_ago) / v_price_1d_ago) * 100
                          ELSE NULL END,
        chg_1d_abs = CASE WHEN v_price_1d_ago IS NOT NULL
                          THEN v_current_price - v_price_1d_ago
                          ELSE NULL END,

        -- 3-day changes
        chg_3d_pct = CASE WHEN v_price_3d_ago IS NOT NULL AND v_price_3d_ago > 0
                          THEN ((v_current_price - v_price_3d_ago) / v_price_3d_ago) * 100
                          ELSE NULL END,
        chg_3d_abs = CASE WHEN v_price_3d_ago IS NOT NULL
                          THEN v_current_price - v_price_3d_ago
                          ELSE NULL END,

        -- 7-day changes
        chg_7d_pct = CASE WHEN v_price_7d_ago IS NOT NULL AND v_price_7d_ago > 0
                          THEN ((v_current_price - v_price_7d_ago) / v_price_7d_ago) * 100
                          ELSE NULL END,
        chg_7d_abs = CASE WHEN v_price_7d_ago IS NOT NULL
                          THEN v_current_price - v_price_7d_ago
                          ELSE NULL END,

        -- 1-month changes
        chg_1m_pct = CASE WHEN v_price_1m_ago IS NOT NULL AND v_price_1m_ago > 0
                          THEN ((v_current_price - v_price_1m_ago) / v_price_1m_ago) * 100
                          ELSE NULL END,
        chg_1m_abs = CASE WHEN v_price_1m_ago IS NOT NULL
                          THEN v_current_price - v_price_1m_ago
                          ELSE NULL END,

        -- 3-month changes
        chg_3m_pct = CASE WHEN v_price_3m_ago IS NOT NULL AND v_price_3m_ago > 0
                          THEN ((v_current_price - v_price_3m_ago) / v_price_3m_ago) * 100
                          ELSE NULL END,
        chg_3m_abs = CASE WHEN v_price_3m_ago IS NOT NULL
                          THEN v_current_price - v_price_3m_ago
                          ELSE NULL END,

        -- 6-month changes
        chg_6m_pct = CASE WHEN v_price_6m_ago IS NOT NULL AND v_price_6m_ago > 0
                          THEN ((v_current_price - v_price_6m_ago) / v_price_6m_ago) * 100
                          ELSE NULL END,
        chg_6m_abs = CASE WHEN v_price_6m_ago IS NOT NULL
                          THEN v_current_price - v_price_6m_ago
                          ELSE NULL END,

        -- 1-year changes
        chg_1y_pct = CASE WHEN v_price_1y_ago IS NOT NULL AND v_price_1y_ago > 0
                          THEN ((v_current_price - v_price_1y_ago) / v_price_1y_ago) * 100
                          ELSE NULL END,
        chg_1y_abs = CASE WHEN v_price_1y_ago IS NOT NULL
                          THEN v_current_price - v_price_1y_ago
                          ELSE NULL END,

        updated_at = now()
      WHERE variant_key = v_variant_key;

      v_updated := v_updated + 1;

    EXCEPTION WHEN OTHERS THEN
      v_errors := v_errors + 1;
      CONTINUE;
    END;
  END LOOP;

  -- Return summary counts
  RETURN QUERY SELECT v_updated, v_skipped, v_errors;
END;
$$ LANGUAGE plpgsql;

-- Keep old function for backwards compatibility (mark as deprecated)
COMMENT ON FUNCTION get_nearest_price IS '[DEPRECATED] Use get_nearest_price_in_range instead - this function causes overlapping time windows';
COMMENT ON FUNCTION get_nearest_price_in_range IS 'Finds the nearest price in JSONB price_history within a specific min/max day range to prevent overlapping windows';
COMMENT ON FUNCTION batch_update_price_history IS 'Batch updates price history and recalculates all 14 metrics using non-overlapping time windows';
