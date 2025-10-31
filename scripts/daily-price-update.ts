import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';

dotenv.config();

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

// Paths
const DATA_DIR = process.env.DATA_DIR || '/Users/jonathan/Desktop/Data';
const TEMP_DOWNLOAD_DIR = process.env.TEMP_DOWNLOAD_DIR || '/tmp/tcg-price-data';
const DOWNLOAD_BASE_URL = 'https://tcgcsv.com/archive/tcgplayer';
const SEVEN_ZIP_PATH = process.env.SEVEN_ZIP_PATH || '/Users/jonathan/Downloads/7z2501-mac/7zz';

const BATCH_SIZE = 1000;

interface PriceEntry {
  productId: number;
  marketPrice: number | null;
  lowPrice: number | null;
  highPrice: number | null;
  subTypeName: string;
}

interface BatchItem {
  variant_key: string;
  date: string;
  price: number;
  low_price: number | null;
  high_price: number | null;
  product_id?: number;
  product_name?: string;
  group_id?: number;
  rarity?: string;
  number?: string;
  image_url?: string;
  url?: string;
  clean_name?: string;
  finish?: string;
}

async function tryDownloadForDate(dateStr: string): Promise<boolean> {
  const url = `${DOWNLOAD_BASE_URL}/prices-${dateStr}.ppmd.7z`;
  const archivePath = path.join(TEMP_DOWNLOAD_DIR, `prices-${dateStr}.ppmd.7z`);

  console.log(`📥 Attempting download: ${url}`);

  try {
    execSync(`curl -f -L -o "${archivePath}" "${url}"`, { stdio: 'inherit' });

    const stats = fs.statSync(archivePath);
    if (stats.size < 1000000) {
      console.error(`❌ File too small (${stats.size} bytes)`);
      console.error(`File content preview:`);
      const content = fs.readFileSync(archivePath, 'utf-8');
      console.error(content.substring(0, 500));
      return false;
    }

    console.log(`✅ Downloaded ${(stats.size / 1024 / 1024).toFixed(2)} MB`);
    console.log(`📦 Extracting archive...`);

    execSync(`"${SEVEN_ZIP_PATH}" x "${archivePath}" -o"${DATA_DIR}" -y`, { stdio: 'inherit' });
    return true;
  } catch (error) {
    console.error(`❌ Download failed for ${dateStr}`);
    console.error(error);
    return false;
  }
}

async function downloadAndExtract(): Promise<string> {
  const now = new Date();
  const estDate = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));

  fs.mkdirSync(TEMP_DOWNLOAD_DIR, { recursive: true });

  const today = `${estDate.getFullYear()}-${String(estDate.getMonth() + 1).padStart(2, '0')}-${String(
    estDate.getDate()
  ).padStart(2, '0')}`;

  console.log(`📅 Current EST date: ${today}`);
  console.log(`📅 System time: ${now.toISOString()}`);

  if (await tryDownloadForDate(today)) {
    return today;
  }

  throw new Error(`Archive not available for ${today}. Check if the date is correct and the file exists at: ${DOWNLOAD_BASE_URL}/prices-${today}.ppmd.7z`);
}

function readPriceJson(date: string, categoryId: string, groupId: string): PriceEntry[] {
  const filePath = path.join(DATA_DIR, date, categoryId, groupId, 'prices');
  if (!fs.existsSync(filePath)) return [];

  const json = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
  return json.results || [];
}

function variant(productId: number, finish?: string) {
  return `${productId}:${finish || 'Normal'}`;
}

async function updatePriceHistory(today: string) {
  const { data: existing } = await supabase.from('products').select('variant_key');
  const existingKeys = new Set(existing?.map(p => p.variant_key));

  const batches: BatchItem[] = [];
  const newProducts = new Map<string, { categoryId: string; groupId: string; productId: number }>();

  for (const categoryId of ['3', '85']) {
    const categoryPath = path.join(DATA_DIR, today, categoryId);
    if (!fs.existsSync(categoryPath)) continue;

    for (const groupId of fs.readdirSync(categoryPath)) {
      const entries = readPriceJson(today, categoryId, groupId);

      for (const entry of entries) {
        const key = variant(entry.productId, entry.subTypeName);

        if (!existingKeys.has(key) && !newProducts.has(key)) {
          newProducts.set(key, { categoryId, groupId, productId: entry.productId });
        }

        batches.push({
          variant_key: key,
          date: today,
          price: entry.marketPrice!,
          low_price: entry.lowPrice,
          high_price: entry.highPrice,
          finish: entry.subTypeName || 'Normal'
        });
      }
    }
  }

  console.log(`📊 Found ${newProducts.size} new products to fetch metadata for`);

  let processedNew = 0;
  for (const [variantKey, meta] of newProducts.entries()) {
    processedNew++;
    if (processedNew % 10 === 0) {
      console.log(`  Fetching metadata: ${processedNew}/${newProducts.size}`);
    }

    const url = `https://tcgcsv.com/tcgplayer/${meta.categoryId}/${meta.groupId}/products`;

    try {
      const response = await fetch(url);

      if (!response.ok) {
        console.error(`❌ API request failed for ${url}: ${response.status} ${response.statusText}`);
        continue;
      }

      const contentType = response.headers.get('content-type');
      if (!contentType || (!contentType.includes('application/json') && !contentType.includes('text/json'))) {
        console.error(`❌ Invalid content type for ${url}: ${contentType}`);
        const text = await response.text();
        console.error(`Response preview: ${text.substring(0, 200)}`);
        continue;
      }

      const json = await response.json();

      if (!json.results || !Array.isArray(json.results)) {
        console.error(`❌ Invalid JSON structure for ${url}`);
        continue;
      }

      const product = json.results.find((p: any) => p.productId === meta.productId);
      if (!product) {
        console.warn(`⚠️  Product ${meta.productId} not found in group ${meta.groupId}`);
        continue;
      }

      const rarity = product.extendedData?.find((x: any) => x.name === 'Rarity')?.value ?? null;
      const number = product.extendedData?.find((x: any) => x.name === 'Number')?.value ?? null;

      for (const row of batches.filter(b => b.variant_key === variantKey)) {
        row.product_id = product.productId;
        row.group_id = Number(meta.groupId);
        row.product_name = product.name;
        row.rarity = rarity;
        row.number = number;
        row.image_url = product.imageUrl ?? null;
        row.url = product.url ?? null;
        row.clean_name = product.cleanName ?? product.name ?? null;
      }
    } catch (error) {
      console.error(`❌ Error fetching metadata for ${variantKey} from ${url}:`, error);
      // Continue processing other products
    }
  }

  console.log(`✅ Finished fetching metadata for new products`);
  console.log(`💾 Updating database with ${batches.length} price records in batches of ${BATCH_SIZE}...`);

  for (let i = 0; i < batches.length; i += BATCH_SIZE) {
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(batches.length / BATCH_SIZE);
    console.log(`  Processing batch ${batchNum}/${totalBatches}...`);

    const { data, error } = await supabase.rpc('batch_update_price_history', {
      batch_data: batches.slice(i, i + BATCH_SIZE)
    });

    if (error) {
      console.error(`❌ Error updating batch ${batchNum}:`, error);
    } else if (data && data.length > 0) {
      const result = data[0];
      console.log(`    ✅ Updated: ${result.updated_count}, Inserted: ${result.inserted_count}, Skipped: ${result.skipped_count}, Errors: ${result.error_count}`);
    }
  }

  console.log(`✅ Database update complete!`);
}

async function main() {
  const today = await downloadAndExtract();
  await updatePriceHistory(today);
  console.log(`✅ Completed daily price update for ${today}`);
}

main();
