# Daily Price Update Cron Job - Complete Pipeline Documentation

## Overview
The cron job runs daily to download the latest Pokemon card prices from TCGPlayer and update the database with new prices, calculating price change metrics.

## Complete Pipeline Flow

### Step 1: Download & Extract Data
**File**: [scripts/daily-price-update.ts](scripts/daily-price-update.ts)
**Function**: `downloadAndExtract()`

1. Gets current date in EST timezone (TCGPlayer's timezone)
2. Downloads archive from: `https://tcgcsv.com/archive/tcgplayer/prices-YYYY-MM-DD.ppmd.7z`
3. Validates file size (must be > 1MB to avoid error pages)
4. Extracts using 7-Zip to `DATA_DIR` (default: `/Users/jonathan/Desktop/Data`)

**Output**: Date string (e.g., "2025-10-30")

---

### Step 2: Build Price Update Batches
**Function**: `updatePriceHistory(today)`

#### 2.1: Fetch Existing Products
```typescript
const { data: existing } = await supabase.from('products').select('variant_key');
const existingKeys = new Set(existing?.map(p => p.variant_key));
```
- Fetches all existing `variant_key` values from database
- Creates a Set for fast lookup

#### 2.1a: Fetch Group Metadata ⭐ NEW
```typescript
const [englishGroups, japaneseGroups] = await Promise.all([
  fetchGroupsForCategory('3'),
  fetchGroupsForCategory('85')
]);
```
- Fetches ALL groups for English (category 3) and Japanese (category 85)
- API: `https://tcgcsv.com/tcgplayer/{categoryId}/groups`
- Caches ~648 groups total (210 English + 438 Japanese)
- Enables automatic group creation for new Pokemon sets
- See [GROUP_HANDLING_DOCUMENTATION.md](GROUP_HANDLING_DOCUMENTATION.md) for details

#### 2.2: Read Price Data from Files
For each category (English=3, Japanese=85):
  - Reads `DATA_DIR/YYYY-MM-DD/categoryId/groupId/prices` files
  - Each file contains JSON with price data:
    ```json
    {
      "results": [
        {
          "productId": 123,
          "marketPrice": 10.50,
          "lowPrice": 9.00,
          "highPrice": 12.00,
          "subTypeName": "Normal" or "Holofoil"
        }
      ]
    }
    ```

#### 2.3: Build Batch Items
For each price entry:
- Creates `variant_key` = `${productId}:${subTypeName}` (e.g., "544444:Holofoil")
- Checks if product exists in database
- If NEW product: adds to `newProducts` map for metadata fetch
- **NEW**: Looks up group metadata from cache and attaches to batch item
- Adds to batches array:
  ```typescript
  {
    variant_key: "544444:Holofoil",
    date: "2025-10-30",
    price: 10.50,           // ← ONLY marketPrice (numeric)
    low_price: 9.00,
    high_price: 12.00,
    finish: "Holofoil",
    group_id: 23651,
    // ⭐ NEW: Group metadata fields
    group_name: "SV08: Surging Sparks",
    group_abbreviation: "SSP",
    group_published_on: "2024-11-08T00:00:00",
    group_modified_on: "2025-11-04T00:51:09.793",
    group_is_supplemental: false,
    group_category_id: 3
  }
  ```

**IMPORTANT**: The cron passes `price: entry.marketPrice!` - this is a NUMERIC value, not an object.

---

### Step 3: Fetch Metadata for New Products
**Lines**: 140-197

For each new product:
1. Fetches from: `https://tcgcsv.com/tcgplayer/{categoryId}/{groupId}/products`
2. Finds matching product by `productId`
3. Extracts metadata:
   - `name`: Product name
   - `rarity`: From extendedData array
   - `number`: Card number from extendedData
   - `imageUrl`: Card image URL
   - `url`: TCGPlayer product page
   - `cleanName`: Cleaned product name
4. Adds metadata to corresponding batch items

---

### Step 4: Update Database via RPC Function
**Lines**: 202-217
**Function Called**: `batch_update_price_history`

Sends batches of 1000 items to the SQL function.

Each batch item contains:
```typescript
{
  variant_key: string,
  date: string,
  price: number,              // ← NUMERIC market price
  low_price: number | null,
  high_price: number | null,
  // For new products only:
  product_id?: number,
  product_name?: string,
  group_id?: number,
  rarity?: string,
  number?: string,
  image_url?: string,
  url?: string,
  clean_name?: string
}
```

---

## SQL Function: `batch_update_price_history`

**Current Migration**: [008_add_group_upsert.sql](supabase/migrations/008_add_group_upsert.sql)

### What the SQL Function Does:

#### For Each Item in Batch:

1. **Extract Data**
   ```sql
   v_variant_key := item->>'variant_key';
   v_date := item->>'date';
   v_price := (item->>'price')::NUMERIC;  -- ← Converts to NUMERIC
   v_product_id := (item->>'product_id')::INTEGER;
   v_product_name := item->>'product_name';
   v_group_id := (item->>'group_id')::INTEGER;
   -- ⭐ NEW: Extract group metadata
   v_group_name := item->>'group_name';
   v_group_abbreviation := item->>'group_abbreviation';
   v_group_published_on := (item->>'group_published_on')::TIMESTAMPTZ;
   v_group_modified_on := (item->>'group_modified_on')::TIMESTAMPTZ;
   v_group_is_supplemental := (item->>'group_is_supplemental')::BOOLEAN;
   v_group_category_id := (item->>'group_category_id')::INTEGER;
   ```

1a. **⭐ NEW: Upsert Group** (BEFORE product insertion)
   ```sql
   IF v_group_id IS NOT NULL AND v_group_name IS NOT NULL THEN
     INSERT INTO groups (id, name, abbreviation, category_id, ...)
     VALUES (v_group_id, v_group_name, ...)
     ON CONFLICT (id) DO UPDATE SET
       name = EXCLUDED.name,
       modified_on = EXCLUDED.modified_on,
       last_synced_at = now();
   END IF;
   ```
   - Ensures group exists BEFORE attempting product insert
   - Updates existing groups with latest metadata
   - Prevents foreign key violations
   - See [GROUP_HANDLING_DOCUMENTATION.md](GROUP_HANDLING_DOCUMENTATION.md)

2. **Check if Product Exists** (line 73)
   ```sql
   SELECT EXISTS(SELECT 1 FROM products WHERE variant_key = v_variant_key)
   INTO v_product_exists;
   ```

3. **If NEW Product** (lines 77-106):
   - Inserts new row with initial price_history:
     ```sql
     price_history = jsonb_build_object(v_date, v_price)
     ```
   - This stores: `{"2025-10-30": 8.04}` (NUMERIC value)
   - Skips metric calculation (no history yet)

4. **If EXISTING Product** (lines 109-212):
   - Fetches current `price_history`
   - Appends new date/price:
     ```sql
     v_price_history := jsonb_set(
       COALESCE(v_price_history, '{}'::jsonb),
       ARRAY[v_date],
       to_jsonb(v_price)  -- ← Converts NUMERIC to JSONB number
     );
     ```
   - **This should store**: `{"2025-10-30": 8.04}`
   - **Bug was storing**: `{"2025-10-30": {"low":6,"high":59.99,"market":8.04}}`

5. **Calculate Price Change Metrics**:
   - Uses `get_nearest_price_in_range()` to find historical prices
   - Non-overlapping time windows:
     - 1-day: 0-2 days back
     - 3-day: 2-4 days back
     - 7-day: 5-9 days back
     - 1-month: 25-35 days back
     - 3-month: 80-100 days back
     - 6-month: 170-190 days back
     - 1-year: 350-380 days back

6. **Update Product Row**:
   - Updates `price_history` (entire JSONB object)
   - Updates `market_price` = current price
   - Updates `low_price` and `high_price`
   - Updates all 14 change metrics (7 percent + 7 absolute)
   - Updates `updated_at` timestamp

---

## THE BUG: What Went Wrong on 2025-10-30?

### Evidence:
- 162+ products had `{"2025-10-30": {"low":6,"high":59.99,"market":8.04}}` format
- The TypeScript code passes `price: entry.marketPrice!` (numeric)
- The SQL function uses `to_jsonb(v_price)` which should convert numeric to JSONB number

### Possible Causes:

1. **Manual SQL Query**: Someone may have run an UPDATE directly on the database with wrong format
2. **Temporary Bug in SQL Function**: The deployed function may have temporarily been different than migration 004
3. **Data Corruption**: Could have been a Supabase/Postgres issue during write

### Why Migration 005 Exists:
I created [005_ensure_price_history_numeric.sql](supabase/migrations/005_ensure_price_history_numeric.sql) to:
- Explicitly handle `low_price` and `high_price` in the function
- Add better comments about storing NUMERIC values only
- Ensure the function matches the actual database schema (which has low_price/high_price columns)

---

## Current Status

### ✅ FIXED:
- All 162+ corrupted products have been fixed
- `2025-10-30` entries now contain numeric values only

### ✅ ADDITIONAL FIX APPLIED:

**Missing `finish` Field**: The `finish` column was not being populated for new products, even though:
- It's in the database schema
- The data is available (`entry.subTypeName`)
- It's part of the variant identification

**Fixed in**:
- [scripts/daily-price-update.ts](scripts/daily-price-update.ts) - Now extracts and passes `finish` field
- [supabase/migrations/005_ensure_price_history_numeric.sql](supabase/migrations/005_ensure_price_history_numeric.sql) - Now stores `finish` field for new/existing products

### ⚠️ TO DO:
1. **Apply Migration 005** to Supabase to ensure the SQL function is up-to-date
2. **Verify** the deployed function matches migration 005
3. **Monitor** next cron run to ensure no recurrence
4. **Backfill** existing products' `finish` field from their `variant_key` if needed

---

## Verification Steps

After next cron run, check:
```sql
-- Should return 0 rows
SELECT variant_key, price_history->'2025-10-31'
FROM products
WHERE jsonb_typeof(price_history->'2025-10-31') = 'object';
```

If any rows returned, the bug has recurred.

---

## Data Flow Summary

```
TCGPlayer Archive
       ↓
[Download & Extract]
       ↓
Local Price Files (JSON)
       ↓
[Read & Parse]
       ↓
Batch Items: {variant_key, date, price: NUMERIC, ...}
       ↓
[RPC: batch_update_price_history]
       ↓
SQL Function:
  - to_jsonb(v_price) → converts NUMERIC to JSONB number
  - jsonb_set(..., to_jsonb(v_price)) → stores in price_history
       ↓
Database:
  - price_history: {"2025-10-30": 8.04, "2025-10-31": 8.10, ...}
  - market_price: 8.10
  - low_price: 7.50
  - high_price: 9.00
  - chg_1d_pct: +1.23
  - ... (14 metrics total)
```

---

## Key Files

- **Cron Job**: [scripts/daily-price-update.ts](scripts/daily-price-update.ts)
- **SQL Function**: [supabase/migrations/004_add_upsert_capability.sql](supabase/migrations/004_add_upsert_capability.sql)
- **Proposed Update**: [supabase/migrations/005_ensure_price_history_numeric.sql](supabase/migrations/005_ensure_price_history_numeric.sql)
- **Fix Script**: [fix_price_history_format.js](fix_price_history_format.js)
- **Verification Script**: [verify_price_history_fix.js](verify_price_history_fix.js)
