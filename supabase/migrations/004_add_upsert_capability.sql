-- =====================================================
-- Add Upsert Capability for New Product Detection
-- =====================================================
-- This migration adds a new RPC function that can insert new products
-- when they are detected in the price data but don't exist in the DB

-- Drop existing function to recreate with new logic
DROP FUNCTION IF EXISTS batch_update_price_history(JSONB);

-- =====================================================
-- New Batch Update Function with UPSERT Support
-- =====================================================
-- This function now:
-- 1. Checks if product exists
-- 2. If NOT exists AND product_name is provided, INSERT new product
-- 3. If exists, UPDATE price history and metrics

CREATE OR REPLACE FUNCTION batch_update_price_history(batch_data JSONB)
RETURNS TABLE(
  updated_count INTEGER,
  inserted_count INTEGER,
  skipped_count INTEGER,
  error_count INTEGER
) AS $$
DECLARE
  item JSONB;
  v_variant_key TEXT;
  v_date TEXT;
  v_price NUMERIC;
  v_price_history JSONB;
  v_product_id INTEGER;
  v_product_name TEXT;
  v_group_id INTEGER;
  v_updated INTEGER := 0;
  v_inserted INTEGER := 0;
  v_skipped INTEGER := 0;
  v_errors INTEGER := 0;
  v_product_exists BOOLEAN;

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

      -- Optional fields for new product creation
      v_product_id := (item->>'product_id')::INTEGER;
      v_product_name := item->>'product_name';
      v_group_id := (item->>'group_id')::INTEGER;

      -- Skip if required fields are null
      IF v_variant_key IS NULL OR v_date IS NULL OR v_price IS NULL THEN
        v_skipped := v_skipped + 1;
        CONTINUE;
      END IF;

      -- Check if product exists
      SELECT EXISTS(SELECT 1 FROM products WHERE variant_key = v_variant_key)
      INTO v_product_exists;

      -- If product doesn't exist, try to insert it
      IF NOT v_product_exists THEN
        -- Can only insert if we have product details
        IF v_product_name IS NULL OR v_group_id IS NULL OR v_product_id IS NULL THEN
          v_skipped := v_skipped + 1;
          CONTINUE;
        END IF;

        -- Insert new product with initial price history
        INSERT INTO products (
          product_id,
          variant_key,
          name,
          group_id,
          market_price,
          price_history,
          created_at,
          updated_at
        ) VALUES (
          v_product_id,
          v_variant_key,
          v_product_name,
          v_group_id,
          v_price,
          jsonb_build_object(v_date, v_price),
          now(),
          now()
        );

        v_inserted := v_inserted + 1;
        CONTINUE; -- Skip metric calculation for new products (no history yet)
      END IF;

      -- Product exists - get its price history
      SELECT price_history INTO v_price_history
      FROM products
      WHERE variant_key = v_variant_key;

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
  RETURN QUERY SELECT v_updated, v_inserted, v_skipped, v_errors;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION batch_update_price_history IS 'Batch updates price history with UPSERT support - inserts new products if product_name and group_id are provided';
