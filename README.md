# Pokemon Card Price Updater - Daily Cron Job

Automated daily cron job that downloads Pokemon card price data from [tcgcsv.com](https://tcgcsv.com) and updates a Supabase database with:
- Market prices
- Price history (JSONB)
- 14 cached metrics (1d, 3d, 7d, 1m, 3m, 6m, 1y changes - both % and absolute)

## How It Works

**Runs daily at 6PM EST (11PM UTC)** via GitHub Actions:

1. Downloads latest price data from `https://tcgcsv.com/archive/tcgplayer/prices-YYYY-MM-DD.ppmd.7z`
2. Extracts using 7z on Ubuntu runner
3. Processes categories 3 (English) and 85 (Japanese)
4. Batch updates Supabase database via PostgreSQL RPC function
5. Updates `market_price`, appends to `price_history` JSONB, recalculates all 14 metrics
6. Cleans up temporary files (runner destroyed automatically)

## Triggers

The workflow runs:
- ✅ **Daily**: 6PM EST (11PM UTC) via cron schedule
- ✅ **On push**: When script or workflow changes are pushed to `main`
- ✅ **Manual**: Via GitHub Actions "Run workflow" button

## Setup

### 1. Prerequisites

- GitHub account (free)
- Supabase project with:
  - `products` table
  - `batch_update_price_history` RPC function (handles batch updates server-side)

### 2. Clone and Push

```bash
git clone <your-fork>
cd cards
git push origin main
```

### 3. Add GitHub Secrets

In your GitHub repo: **Settings → Secrets and variables → Actions**

Add these secrets:
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` - Service role key (not anon key)

### 4. Enable GitHub Actions

Go to **Actions** tab in your repo and enable workflows.

### 5. Test

**Option A**: Push changes to `scripts/daily-price-update.ts` (auto-triggers)
**Option B**: Go to **Actions → Daily Price Update → Run workflow**
**Option C**: Wait for 6PM EST

## Database Schema

The `batch_update_price_history` RPC function expects:

```sql
-- Simplified example
CREATE TABLE products (
  product_id INTEGER PRIMARY KEY,
  market_price NUMERIC,
  price_history JSONB,  -- {"2024-02-08": 3.51, "2024-02-09": 3.48}

  -- Cached metrics (14 total)
  chg_1d_pct NUMERIC,
  chg_1d_abs NUMERIC,
  chg_3d_pct NUMERIC,
  chg_3d_abs NUMERIC,
  -- ... (7d, 1m, 3m, 6m, 1y)
);

-- RPC function signature
CREATE FUNCTION batch_update_price_history(
  batch_data JSONB  -- [{"product_id": 123, "price": 4.99, "date": "2025-10-22"}, ...]
) RETURNS void;
```

The RPC function should:
1. Update `market_price` to latest price
2. Append date/price to `price_history` JSONB
3. Recalculate all 14 metrics based on updated history

## Architecture

### Why GitHub Actions?
- ✅ **100% Free** (2,000 minutes/month for private repos, unlimited for public)
- ✅ Fresh Ubuntu runner with 7z pre-installed
- ✅ No persistent storage needed (download → process → upload → destroy)
- ✅ Built-in scheduling and logging

### Why RPC Function?
- Server-side batch processing in PostgreSQL
- Avoids 60-second timeout issues with large batches
- Reduces network round-trips (1 call per 1,000 products vs 1,000 individual updates)
- Automatic metric recalculation ensures consistency

### Performance
- **Batch size**: 1,000 products per RPC call
- **Retry logic**: 3 attempts with 2-second delay for timeouts
- **Processing time**: ~5-10 minutes for ~40,000 products
- **Data transfer**: ~100MB download per day

## Files

```
.github/workflows/daily-price-update.yml  # GitHub Actions workflow
scripts/daily-price-update.ts             # Main cron script
package.json                              # Dependencies
.gitignore                                # Git ignore rules
```

## Cost

**$0/month** - Completely free using GitHub Actions free tier

## Monitoring

View logs in **GitHub Actions** tab:
1. Click on any workflow run
2. Expand "Run daily price update" step
3. Check for errors or timeouts

## Troubleshooting

**Error: Download failed (404)**
- Data for today not yet posted on tcgcsv.com
- Archives typically posted around 8-10PM EST
- Consider changing cron to 10PM EST (`0 3 * * *`)

**Error: Statement timeout (code 57014)**
- Batch taking >60 seconds on free tier
- Retry logic handles this automatically
- If persistent, consider upgrading Supabase tier

**Error: Database connection failed**
- Check `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` secrets
- Verify Supabase project isn't paused (free tier auto-pauses after 1 week inactivity)

## License

MIT
