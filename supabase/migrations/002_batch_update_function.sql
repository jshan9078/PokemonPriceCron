-- =====================================================
-- Batch Update Price History RPC Function
-- =====================================================
-- This function is called by the daily cron job to update prices in batches
-- It handles: inserting/updating products, appending to price_history, and recalculating all 14 metrics

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

      -- Helper function to get nearest price N days ago
      v_price_1d_ago := get_nearest_price(v_price_history, v_date_parsed - INTERVAL '1 day');
      v_price_3d_ago := get_nearest_price(v_price_history, v_date_parsed - INTERVAL '3 days');
      v_price_7d_ago := get_nearest_price(v_price_history, v_date_parsed - INTERVAL '7 days');
      v_price_1m_ago := get_nearest_price(v_price_history, v_date_parsed - INTERVAL '1 month');
      v_price_3m_ago := get_nearest_price(v_price_history, v_date_parsed - INTERVAL '3 months');
      v_price_6m_ago := get_nearest_price(v_price_history, v_date_parsed - INTERVAL '6 months');
      v_price_1y_ago := get_nearest_price(v_price_history, v_date_parsed - INTERVAL '1 year');

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

-- =====================================================
-- Helper function: Get nearest price to a target date
-- =====================================================
-- Searches the price_history JSONB for a price within 7 days of the target date

CREATE OR REPLACE FUNCTION get_nearest_price(price_history JSONB, target_date DATE)
RETURNS NUMERIC AS $$
DECLARE
  date_key TEXT;
  price_val NUMERIC;
  days_offset INTEGER;
BEGIN
  -- Try exact match first
  date_key := to_char(target_date, 'YYYY-MM-DD');
  price_val := (price_history->>date_key)::NUMERIC;

  IF price_val IS NOT NULL THEN
    RETURN price_val;
  END IF;

  -- Search within ±7 days
  FOR days_offset IN 1..7 LOOP
    -- Try +N days
    date_key := to_char(target_date + (days_offset || ' days')::INTERVAL, 'YYYY-MM-DD');
    price_val := (price_history->>date_key)::NUMERIC;
    IF price_val IS NOT NULL THEN
      RETURN price_val;
    END IF;

    -- Try -N days
    date_key := to_char(target_date - (days_offset || ' days')::INTERVAL, 'YYYY-MM-DD');
    price_val := (price_history->>date_key)::NUMERIC;
    IF price_val IS NOT NULL THEN
      RETURN price_val;
    END IF;
  END LOOP;

  -- No price found within ±7 days
  RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION batch_update_price_history IS 'Batch updates price history and recalculates all 14 metrics for multiple products';
COMMENT ON FUNCTION get_nearest_price IS 'Finds the nearest price in JSONB price_history within ±7 days of target date';
