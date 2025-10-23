# Pokemon Card Price Updater - Daily Cron Job

Automated daily cron job that downloads Pokemon card price data from [tcgcsv.com](https://tcgcsv.com) and updates a Supabase PostgreSQL database with:
- ✅ Market prices for ~40,000 cards
- ✅ Historical price data (stored as JSONB)
- ✅ 14 cached metrics (1d, 3d, 7d, 1m, 3m, 6m, 1y price changes - both % and $)

**100% free** using GitHub Actions (no credit card required).

## How It Works

Runs **daily at 6PM EST (11PM UTC)** via GitHub Actions:

1. 📥 Downloads latest price data from tcgcsv.com
2. 📦 Extracts archive using 7z
3. 🔍 Processes categories 3 (English) and 85 (Japanese)
4. 🚀 Batch updates via PostgreSQL RPC (1,000 products/batch)
5. 📊 Updates market_price, price_history, and 14 metrics
6. 🧹 Cleans up (runner destroyed automatically)

### Workflow Triggers

- ⏰ **Scheduled**: Daily at 6PM EST
- 🔄 **On push**: When script/workflow changes
- 👆 **Manual**: Via GitHub Actions UI

## Quick Start

### 1. Fork This Repository

```bash
git clone https://github.com/YOUR_USERNAME/PokemonPriceCron.git
cd PokemonPriceCron
```

### 2. Set Up Supabase

1. Create free project at [supabase.com](https://supabase.com)
2. Go to **SQL Editor**
3. Run `supabase/migrations/001_initial_schema.sql`
4. Run `supabase/migrations/002_batch_update_function.sql`

### 3. Add GitHub Secrets

In your repo: **Settings → Secrets → Actions**

Add:
- `SUPABASE_URL` - Your project URL
- `SUPABASE_SERVICE_ROLE_KEY` - Service role key (not anon)

### 4. Enable & Test

1. Go to **Actions** tab → Enable workflows
2. Push a change or click "Run workflow"
3. Check logs for success

## Database Schema

See `supabase/migrations/` for complete schema.

**Key tables:**
- `groups` - Card sets/expansions
- `products` - Cards with price_history JSONB and 14 metrics

**RPC function:**
- `batch_update_price_history` - Batch updates with metric recalculation

## Troubleshooting

**Error: supabaseUrl is required**
→ Add GitHub secrets (see step 3)

**Error: Download failed (404)**
→ Data not posted yet, change cron to 10PM EST

**Error: Statement timeout**
→ Script has retry logic, or reduce BATCH_SIZE

See full README for complete documentation.

## Cost

**$0/month** - Completely free using GitHub Actions + Supabase free tiers

## License

MIT
