-- =====================================================
-- Fix Daily Update Metrics (Stricter Time Windows)
-- =====================================================
-- This migration updates the metric calculation logic in both
-- batch_update_price_history AND recalculate_stale_metrics.
--
-- FIXES:
-- 1. Enforces stricter time windows to avoid off-by-one errors.
--    - 3d change: Searches 3-5 days back (was possibly matching 1-2 days back in potential regression)
--    - 7d change: Searches 7-10 days back (was 6-10 or 5-9)
-- 2. Ensures ranges do not overlap or drift too close to the present.
--
-- RANGES:
-- 1d: 1-2 days ago
-- 3d: 3-5 days ago
-- 7d: 7-10 days ago
-- 1m: 30-35 days ago
-- 3m: 90-100 days ago
-- 6m: 180-200 days ago
-- 1y: 365-380 days ago

-- =====================================================
-- 1. Update batch_update_price_history
-- =====================================================

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
  v_low_price NUMERIC;
  v_high_price NUMERIC;
  v_price_history JSONB;
  v_product_id INTEGER;
  v_product_name TEXT;
  v_group_id INTEGER;
  v_rarity TEXT;
  v_number TEXT;
  v_image_url TEXT;
  v_url TEXT;
  v_clean_name TEXT;
  v_finish TEXT;
  v_pricecharting_url TEXT;
  v_updated INTEGER := 0;
  v_inserted INTEGER := 0;
  v_skipped INTEGER := 0;
  v_errors INTEGER := 0;
  v_product_exists BOOLEAN;

  -- Variables for reading existing data
  v_existing_rarity TEXT;
  v_existing_number TEXT;
  
  -- Variables for final values to check eligibility
  v_final_rarity TEXT;
  v_final_number TEXT;

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
      v_low_price := (item->>'low_price')::NUMERIC;
      v_high_price := (item->>'high_price')::NUMERIC;
      v_date_parsed := v_date::DATE;

      -- Optional fields for new product creation
      v_product_id := (item->>'product_id')::INTEGER;
      v_product_name := item->>'product_name';
      v_group_id := (item->>'group_id')::INTEGER;
      v_rarity := item->>'rarity';
      v_number := item->>'number';
      v_image_url := item->>'image_url';
      v_url := item->>'url';
      v_clean_name := item->>'clean_name';
      v_finish := item->>'finish';
      v_pricecharting_url := item->>'pricecharting_url';

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
          low_price,
          high_price,
          rarity,
          number,
          image_url,
          url,
          clean_name,
          finish,
          pricecharting_url,
          is_eligible,
          price_history,
          created_at,
          updated_at
        ) VALUES (
          v_product_id,
          v_variant_key,
          v_product_name,
          v_group_id,
          v_price,
          v_low_price,
          v_high_price,
          v_rarity,
          v_number,
          v_image_url,
          v_url,
          v_clean_name,
          v_finish,
          v_pricecharting_url,
          -- Eligible IF: Price >= 15 AND NOT (Unsealed)
          CASE 
            WHEN v_price >= 15 AND NOT (v_rarity IS NULL AND v_number IS NULL) THEN TRUE 
            ELSE FALSE 
          END,
          jsonb_build_object(v_date, v_price),
          now(),
          now()
        );

        v_inserted := v_inserted + 1;
        CONTINUE; -- Skip metric calculation for new products
      END IF;

      -- Product exists - get its price history AND existing attributes
      SELECT price_history, rarity, number 
      INTO v_price_history, v_existing_rarity, v_existing_number
      FROM products
      WHERE variant_key = v_variant_key;

      -- Determine final rarity/number to check 'unsealed' status
      v_final_rarity := COALESCE(v_rarity, v_existing_rarity);
      v_final_number := COALESCE(v_number, v_existing_number);

      -- Calculate metrics BEFORE adding today's price
      v_current_price := v_price;
      
      -- STRICT RANGES
      v_price_1d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 1, 2);
      v_price_3d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 3, 5);
      v_price_7d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 7, 10);
      v_price_1m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 30, 35);
      v_price_3m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 90, 100);
      v_price_6m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 180, 200);
      v_price_1y_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 365, 380);

      -- Append today's price to price_history
      v_price_history := jsonb_set(
        COALESCE(v_price_history, '{}'::jsonb),
        ARRAY[v_date],
        to_jsonb(v_price)
      );

      -- Update the product
      UPDATE products
      SET
        price_history = v_price_history,
        market_price = v_current_price,
        low_price = v_low_price,
        high_price = v_high_price,
        rarity = COALESCE(v_rarity, rarity),
        number = COALESCE(v_number, number),
        image_url = COALESCE(v_image_url, image_url),
        url = COALESCE(v_url, url),
        clean_name = COALESCE(v_clean_name, clean_name),
        finish = COALESCE(v_finish, finish),
        pricecharting_url = COALESCE(v_pricecharting_url, pricecharting_url),
        
        -- Eligible IF: Price >= 15 AND NOT (Unsealed)
        is_eligible = CASE 
          WHEN v_current_price >= 15 AND NOT (v_final_rarity IS NULL AND v_final_number IS NULL) THEN TRUE 
          ELSE FALSE 
        END,

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

COMMENT ON FUNCTION batch_update_price_history IS 'Batch updates price history with support for pricecharting_url, product upsert, STRICT metric calculation ranges, and automatic is_eligible tracking';

-- =====================================================
-- 2. Update recalculate_stale_metrics
-- =====================================================

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
    FOR UPDATE SKIP LOCKED
  ),
  updated_rows AS (
    UPDATE products p
    SET
      -- 1-day changes (Relative to NOW)
      -- STRICT RANGES: 1-2, 3-5, 7-10, 30-35, 90-100, 180-200, 365-380
      
      chg_1d_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 1, 2) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 1, 2) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 1, 2)) / get_nearest_price_in_range(p.price_history, now()::DATE, 1, 2)) * 100))
        ELSE NULL END,
      chg_1d_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 1, 2) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 1, 2)
        ELSE NULL END,

      -- 3-day changes
      chg_3d_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 3, 5) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 3, 5) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 3, 5)) / get_nearest_price_in_range(p.price_history, now()::DATE, 3, 5)) * 100))
        ELSE NULL END,
      chg_3d_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 3, 5) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 3, 5)
        ELSE NULL END,

      -- 7-day changes
      chg_7d_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 7, 10) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 7, 10) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 7, 10)) / get_nearest_price_in_range(p.price_history, now()::DATE, 7, 10)) * 100))
        ELSE NULL END,
      chg_7d_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 7, 10) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 7, 10)
        ELSE NULL END,

      -- 1-month changes
      chg_1m_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 30, 35) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 30, 35) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 30, 35)) / get_nearest_price_in_range(p.price_history, now()::DATE, 30, 35)) * 100))
        ELSE NULL END,
      chg_1m_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 30, 35) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 30, 35)
        ELSE NULL END,

      -- 3-month changes
      chg_3m_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 90, 100) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 90, 100) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 90, 100)) / get_nearest_price_in_range(p.price_history, now()::DATE, 90, 100)) * 100))
        ELSE NULL END,
      chg_3m_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 90, 100) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 90, 100)
        ELSE NULL END,

      -- 6-month changes
      chg_6m_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 180, 200) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 180, 200) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 180, 200)) / get_nearest_price_in_range(p.price_history, now()::DATE, 180, 200)) * 100))
        ELSE NULL END,
      chg_6m_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 180, 200) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 180, 200)
        ELSE NULL END,

       -- 1-year changes
      chg_1y_pct = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 365, 380) IS NOT NULL AND get_nearest_price_in_range(p.price_history, now()::DATE, 365, 380) > 0
        THEN GREATEST(-9999.999, LEAST(9999.999, ((p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 365, 380)) / get_nearest_price_in_range(p.price_history, now()::DATE, 365, 380)) * 100))
        ELSE NULL END,
      chg_1y_abs = CASE 
        WHEN get_nearest_price_in_range(p.price_history, now()::DATE, 365, 380) IS NOT NULL
        THEN p.market_price - get_nearest_price_in_range(p.price_history, now()::DATE, 365, 380)
        ELSE NULL END,

      updated_at = now()

    FROM stale_products
    WHERE p.variant_key = stale_products.variant_key
    RETURNING 1
  )
  SELECT count(*) INTO v_count FROM updated_rows;

  RETURN QUERY SELECT v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION recalculate_stale_metrics IS 'Recalculates change metrics for stale products using STRICT time ranges to avoid off-by-one errors.';
