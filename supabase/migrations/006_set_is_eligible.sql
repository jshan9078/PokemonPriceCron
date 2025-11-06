-- =====================================================
-- Update batch_update_price_history to set is_eligible based on market_price
-- =====================================================
-- This migration updates the batch_update_price_history function to automatically
-- set is_eligible = TRUE whenever market_price >= $15, FALSE otherwise.
-- The is_eligible column already exists in the schema with DEFAULT FALSE.

-- Update the batch_update_price_history function to maintain the flag automatically.
DROP FUNCTION IF EXISTS batch_update_price_history(JSONB);

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
        -- IMPORTANT: Store ONLY the numeric price in price_history
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
          CASE WHEN v_price IS NOT NULL AND v_price >= 15 THEN TRUE ELSE FALSE END,
          jsonb_build_object(v_date, v_price),  -- ONLY store numeric price
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
      -- IMPORTANT: Use to_jsonb(v_price) to ensure we store ONLY the numeric value
      v_price_history := jsonb_set(
        COALESCE(v_price_history, '{}'::jsonb),
        ARRAY[v_date],
        to_jsonb(v_price)  -- This ensures ONLY the numeric price is stored
      );

      -- Calculate metrics by looking back from this date
      v_current_price := v_price;

      -- Use non-overlapping time windows to ensure distinct historical prices
      v_price_1d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 0, 2);
      v_price_3d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 2, 4);
      v_price_7d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 5, 9);
      v_price_1m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 25, 35);
      v_price_3m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 80, 100);
      v_price_6m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 170, 190);
      v_price_1y_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 350, 380);

      -- Update the product with new price_history, market_price, low/high, and all metrics
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
        is_eligible = CASE WHEN v_current_price IS NOT NULL AND v_current_price >= 15 THEN TRUE ELSE FALSE END,

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

COMMENT ON FUNCTION batch_update_price_history IS 'Batch updates price history (NUMERIC VALUES ONLY) with UPSERT support, eligibility tracking, and low/high price tracking';
