-- =====================================================
-- Add pricecharting_url Support to Batch Update
-- =====================================================
-- This migration updates the batch_update_price_history function to:
-- 1. Extract pricecharting_url from input JSONB
-- 2. Include it in INSERT statement for new products
-- 3. Include it in UPDATE statement for existing products

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
  v_pricecharting_url TEXT; -- New variable
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
      v_pricecharting_url := item->>'pricecharting_url'; -- Extract new field

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
          pricecharting_url, -- Added column
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
          v_pricecharting_url, -- Added value
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
      v_price_1d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 1, 3);
      v_price_3d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 3, 5);
      v_price_7d_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 6, 10);
      v_price_1m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 26, 36);
      v_price_3m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 85, 105);
      v_price_6m_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 175, 195);
      v_price_1y_ago := get_nearest_price_in_range(v_price_history, v_date_parsed, 355, 385);

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
        pricecharting_url = COALESCE(v_pricecharting_url, pricecharting_url), -- Added update (only if new value provided)
        
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

COMMENT ON FUNCTION batch_update_price_history IS 'Batch updates price history with support for pricecharting_url, product upsert, metric calculation, and automatic is_eligible tracking';
