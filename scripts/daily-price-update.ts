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

// TCGPlayer API Configuration
const TCGPLAYER_API_BASE = 'https://api.tcgplayer.com';
const TCGPLAYER_CLIENT_ID = process.env.TCGPLAYER_CLIENT_ID;
const TCGPLAYER_CLIENT_SECRET = process.env.TCGPLAYER_CLIENT_SECRET;
const UNOPENED_CONDITION_ID = 6;

const BATCH_SIZE = 1000;

interface PriceEntry {
  productId: number;
  marketPrice: number | null;
  lowPrice: number | null;
  highPrice: number | null;
  subTypeName: string;
}

interface GroupData {
  groupId: number;
  name: string;
  abbreviation: string | null;
  isSupplemental: boolean;
  publishedOn: string | null;
  modifiedOn: string | null;
  categoryId: number;
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
  sealed?: boolean;
}

// TCGPlayer API Types
interface TcgPlayerSku {
  skuId: number;
  productId: number;
  languageId: number;
  printingId: number;
  conditionId: number;
}

// TCGPlayer OAuth Token Management
let tcgAccessToken: string | null = null;
let tcgTokenExpiry = 0;

async function getTcgPlayerAccessToken(): Promise<string | null> {
  if (!TCGPLAYER_CLIENT_ID || !TCGPLAYER_CLIENT_SECRET) {
    console.warn('⚠️ TCGPlayer credentials not configured, skipping sealed detection');
    return null;
  }

  const now = Date.now();
  if (tcgAccessToken && tcgTokenExpiry > now + 300000) {
    return tcgAccessToken;
  }

  console.log('🔑 Fetching TCGPlayer access token...');

  try {
    const response = await fetch(`${TCGPLAYER_API_BASE}/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `grant_type=client_credentials&client_id=${TCGPLAYER_CLIENT_ID}&client_secret=${TCGPLAYER_CLIENT_SECRET}`,
    });

    if (!response.ok) {
      console.error(`❌ TCGPlayer auth failed: ${response.status}`);
      return null;
    }

    const data = await response.json();
    tcgAccessToken = data.access_token;
    tcgTokenExpiry = now + data.expires_in * 1000;
    return tcgAccessToken;
  } catch (error) {
    console.error('❌ TCGPlayer auth error:', error);
    return null;
  }
}

async function getProductSkus(productId: number): Promise<TcgPlayerSku[]> {
  const token = await getTcgPlayerAccessToken();
  if (!token) return [];

  try {
    const response = await fetch(`${TCGPLAYER_API_BASE}/catalog/products/${productId}/skus`, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json',
      },
    });

    if (!response.ok) {
      return [];
    }

    const data = await response.json();
    return data.results ?? [];
  } catch {
    return [];
  }
}

function isProductSealed(skus: TcgPlayerSku[]): boolean {
  if (skus.length === 0) return false;
  return skus.every(sku => sku.conditionId === UNOPENED_CONDITION_ID);
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

async function downloadAndExtract(dateOverride?: string): Promise<string> {
  // Ensure directory exists first
  fs.mkdirSync(TEMP_DOWNLOAD_DIR, { recursive: true });

  if (dateOverride) {
    console.log(`📅 Manual override: Using date ${dateOverride}`);
    if (await tryDownloadForDate(dateOverride)) {
      return dateOverride;
    }
    throw new Error(`Archive not available for ${dateOverride}. Check if the date is correct and the file exists at: ${DOWNLOAD_BASE_URL}/prices-${dateOverride}.ppmd.7z`);
  }

  const now = new Date();
  const estDate = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));

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

async function fetchGroupsForCategory(categoryId: string): Promise<Map<number, GroupData>> {
  const url = `https://tcgcsv.com/tcgplayer/${categoryId}/groups`;
  console.log(`📥 Fetching groups for category ${categoryId}...`);

  try {
    const response = await fetch(url);

    if (!response.ok) {
      console.error(`❌ Failed to fetch groups: ${response.status} ${response.statusText}`);
      return new Map();
    }

    const json = await response.json();

    if (!json.results || !Array.isArray(json.results)) {
      console.error(`❌ Invalid groups response structure`);
      return new Map();
    }

    const groupMap = new Map<number, GroupData>();
    for (const group of json.results) {
      groupMap.set(group.groupId, {
        groupId: group.groupId,
        name: group.name,
        abbreviation: group.abbreviation || null,
        isSupplemental: group.isSupplemental || false,
        publishedOn: group.publishedOn || null,
        modifiedOn: group.modifiedOn || null,
        categoryId: group.categoryId
      });
    }

    console.log(`✅ Fetched ${groupMap.size} groups for category ${categoryId}`);
    return groupMap;
  } catch (error) {
    console.error(`❌ Error fetching groups for category ${categoryId}:`, error);
    return new Map();
  }
}

async function updatePriceHistory(today: string) {
  console.log('📊 Loading existing products from database...');
  let allProducts: { variant_key: string }[] = [];
  let page = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from('products')
      .select('variant_key')
      .range(page * pageSize, (page + 1) * pageSize - 1);

    if (error) {
      console.error('❌ Error fetching existing products:', error);
      throw error;
    }

    if (!data || data.length === 0) break;

    allProducts = allProducts.concat(data);
    page++;

    if (page % 10 === 0) {
      console.log(`  Loaded ${allProducts.length} products so far...`);
    }
  }

  const existingKeys = new Set(allProducts.map(p => p.variant_key));
  console.log(`✅ Loaded ${existingKeys.size} existing products from database`);

  // Fetch group metadata for both categories
  console.log('📥 Fetching group metadata from TCGPlayer...');
  const [englishGroups, japaneseGroups] = await Promise.all([
    fetchGroupsForCategory('3'),
    fetchGroupsForCategory('85')
  ]);

  const allGroups = new Map<string, GroupData>();
  for (const [groupId, group] of englishGroups) {
    allGroups.set(`3:${groupId}`, group);
  }
  for (const [groupId, group] of japaneseGroups) {
    allGroups.set(`85:${groupId}`, group);
  }

  console.log(`✅ Loaded ${allGroups.size} total groups from TCGPlayer`);

  // Identify unique groups present in price data
  console.log('🔍 Identifying groups in price data...');
  const groupsInPriceData = new Set<string>();

  for (const categoryId of ['3', '85']) {
    const categoryPath = path.join(DATA_DIR, today, categoryId);
    if (!fs.existsSync(categoryPath)) continue;

    for (const groupId of fs.readdirSync(categoryPath)) {
      groupsInPriceData.add(`${categoryId}:${groupId}`);
    }
  }

  console.log(`📊 Found ${groupsInPriceData.size} unique groups in price data`);

  // Upsert all groups ONCE before processing products
  console.log('💾 Upserting groups to database...');
  let groupsUpserted = 0;
  let groupsSkipped = 0;

  for (const groupKey of groupsInPriceData) {
    const groupData = allGroups.get(groupKey);

    if (!groupData) {
      console.warn(`⚠️  No metadata found for group ${groupKey}, skipping`);
      groupsSkipped++;
      continue;
    }

    const { error } = await supabase
      .from('groups')
      .upsert({
        id: groupData.groupId,
        name: groupData.name,
        abbreviation: groupData.abbreviation,
        category_id: groupData.categoryId,
        published_on: groupData.publishedOn,
        modified_on: groupData.modifiedOn,
        is_supplemental: groupData.isSupplemental,
        last_synced_at: new Date().toISOString()
      }, {
        onConflict: 'id'
      });

    if (error) {
      console.error(`❌ Error upserting group ${groupData.groupId}:`, error);
    } else {
      groupsUpserted++;
    }
  }

  console.log(`✅ Upserted ${groupsUpserted} groups (${groupsSkipped} skipped)`);

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
          finish: entry.subTypeName || 'Normal',
          group_id: Number(groupId)
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

      // Check if product is sealed (all SKUs have conditionId=6/Unopened)
      const skus = await getProductSkus(product.productId);
      const sealed = isProductSealed(skus);
      if (sealed) {
        console.log(`  📦 Product ${product.productId} is SEALED`);
      }
      for (const row of batches.filter(b => b.variant_key === variantKey)) {
        row.sealed = sealed;
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
  const scriptStart = new Date(); // Capture start time for stale data cleanup
  const today = await downloadAndExtract(process.argv[2]);
  await updatePriceHistory(today);

  // Clean up stale metrics for products that weren't updated in this run
  console.log('🔄 Recalculating stale metrics...');
  const { error: cleanupError, data: cleanupData } = await supabase.rpc('recalculate_stale_metrics', {
    cutoff_time: scriptStart.toISOString()
  });

  if (cleanupError) {
    console.error('❌ Error recalculating stale metrics:', cleanupError);
  } else {
    // Check if cleanupData is an array (if returns table) or number (if returns scalar)
    // The function RETURNS TABLE(updated_count INTEGER)
    const count = cleanupData && cleanupData[0] ? cleanupData[0].updated_count : 0;
    console.log(`✅ Recalculated metrics for ${count} stale products`);
  }

  console.log(`✅ Completed daily price update for ${today}`);
}

main();
