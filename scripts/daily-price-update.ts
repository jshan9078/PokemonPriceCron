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
}

async function downloadAndExtract(): Promise<string> {
  const now = new Date();
  const estDate = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));

  const today = `${estDate.getFullYear()}-${String(estDate.getMonth() + 1).padStart(2, '0')}-${String(
    estDate.getDate()
  ).padStart(2, '0')}`;

  const url = `${DOWNLOAD_BASE_URL}/prices-${today}.ppmd.7z`;
  const archivePath = path.join(TEMP_DOWNLOAD_DIR, `prices-${today}.ppmd.7z`);

  fs.mkdirSync(TEMP_DOWNLOAD_DIR, { recursive: true });
  execSync(`curl -L -o "${archivePath}" "${url}"`, { stdio: 'inherit' });
  execSync(`"${SEVEN_ZIP_PATH}" x "${archivePath}" -o"${DATA_DIR}" -y`, { stdio: 'inherit' });

  return today;
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
          high_price: entry.highPrice
        });
      }
    }
  }

  for (const [variantKey, meta] of newProducts.entries()) {
    const url = `https://tcgcsv.com/tcgplayer/${meta.categoryId}/${meta.groupId}/products`;
    const json = await fetch(url).then(r => r.json());

    const product = json.results.find((p: any) => p.productId === meta.productId);
    if (!product) continue;

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
  }

  for (let i = 0; i < batches.length; i += BATCH_SIZE) {
    await supabase.rpc('batch_update_price_history', {
      batch_data: batches.slice(i, i + BATCH_SIZE)
    });
  }
}

async function main() {
  const today = await downloadAndExtract();
  await updatePriceHistory(today);
  console.log(`✅ Completed daily price update for ${today}`);
}

main();
