import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';

dotenv.config();

const supabaseUrl = process.env.SUPABASE_URL as string;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY as string;
const supabase = createClient(supabaseUrl, supabaseKey);

// Configuration (uses environment variables for Railway deployment)
const DATA_DIR = process.env.DATA_DIR || '/Users/jonathan/Desktop/Data';
const TEMP_DOWNLOAD_DIR = process.env.TEMP_DOWNLOAD_DIR || '/tmp/tcg-price-data';
const DOWNLOAD_BASE_URL = 'https://tcgcsv.com/archive/tcgplayer';
const BATCH_SIZE = 1000; // Reduced from 3000 to avoid timeouts on free tier
const SEVEN_ZIP_PATH = process.env.SEVEN_ZIP_PATH || '/Users/jonathan/Downloads/7z2501-mac/7zz'; // Path to 7zz binary

interface PriceEntry {
  productId: number;
  marketPrice: number | null;
  subTypeName: string;
}

interface PriceUpdate {
  variant_key: string;
  date: string;
  price: number;
  product_id?: number;
  product_name?: string;
  group_id?: number;
}

interface ProductDetails {
  productId: number;
  name: string;
  groupId: number;
}

interface GroupProductsResponse {
  success: boolean;
  results: Array<{
    productId: number;
    name: string;
  }>;
}

/**
 * Step 1: Download and extract today's price data from tcgcsv.com
 */
async function downloadAndExtractTodaysData(): Promise<string> {
  // Get today's date in EST timezone (tcgcsv.com follows US Eastern time)
  const now = new Date();
  const estDate = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const year = estDate.getFullYear();
  const month = String(estDate.getMonth() + 1).padStart(2, '0');
  const day = String(estDate.getDate()).padStart(2, '0');
  const today = `${year}-${month}-${day}`;

  console.log(`\n📥 Downloading data for ${today} from tcgcsv.com...\n`);

  // Create temp directory
  if (!fs.existsSync(TEMP_DOWNLOAD_DIR)) {
    fs.mkdirSync(TEMP_DOWNLOAD_DIR, { recursive: true });
  }

  // Download URL: https://tcgcsv.com/archive/tcgplayer/prices-YYYY-MM-DD.ppmd.7z
  const downloadUrl = `${DOWNLOAD_BASE_URL}/prices-${today}.ppmd.7z`;
  const archivePath = path.join(TEMP_DOWNLOAD_DIR, `prices-${today}.ppmd.7z`);

  console.log(`   URL: ${downloadUrl}`);
  console.log(`   Downloading to: ${archivePath}\n`);

  try {
    // Download using curl with progress bar
    execSync(`curl -L -o "${archivePath}" "${downloadUrl}"`, { stdio: 'inherit' });
    console.log(`\n✅ Download complete\n`);
  } catch (error) {
    console.error('❌ Error downloading archive:', error);
    throw new Error(`Failed to download data for ${today}`);
  }

  // Verify the file was downloaded and is valid
  if (!fs.existsSync(archivePath)) {
    throw new Error(`Downloaded file not found: ${archivePath}`);
  }

  const fileSize = fs.statSync(archivePath).size;

  // Check if file is too small (likely an error page)
  if (fileSize < 1000) {
    console.error(`❌ Downloaded file is too small (${fileSize} bytes)`);
    console.error(`   This usually means the file doesn't exist on tcgcsv.com yet`);
    console.error(`   URL attempted: ${downloadUrl}\n`);

    // Clean up invalid file
    fs.unlinkSync(archivePath);

    throw new Error(`Data for ${today} not available yet on tcgcsv.com`);
  }

  console.log(`📦 Archive size: ${(fileSize / 1024 / 1024).toFixed(2)} MB\n`);

  // Extract the archive to DATA_DIR
  console.log(`📦 Extracting archive...\n`);

  // Extract directly to DATA_DIR (archive contains the date folder)
  try {
    // Extract using 7zz (use local binary in project root)
    execSync(`"${SEVEN_ZIP_PATH}" x "${archivePath}" -o"${DATA_DIR}" -y`, { stdio: 'inherit' });
    console.log(`\n✅ Extracted to ${DATA_DIR}\n`);
  } catch (error) {
    console.error('❌ Error extracting archive:', error);
    console.error(`   Make sure 7zz binary exists at: ${SEVEN_ZIP_PATH}`);
    throw new Error('Extraction failed');
  }

  // Verify the extraction created the expected directory structure
  const expectedPath = path.join(DATA_DIR, today);
  if (!fs.existsSync(expectedPath)) {
    console.error(`❌ Extraction succeeded but expected directory not found: ${expectedPath}`);
    throw new Error('Unexpected archive structure');
  }

  // Verify categories exist
  const cat3Path = path.join(expectedPath, '3');
  const cat85Path = path.join(expectedPath, '85');

  if (!fs.existsSync(cat3Path) && !fs.existsSync(cat85Path)) {
    console.error(`❌ Categories 3 and 85 not found in ${expectedPath}`);
    throw new Error('Invalid archive structure');
  }

  console.log(`✅ Verified directory structure at ${expectedPath}\n`);

  // Clean up: Remove any directories that are not category 3 or 85
  console.log(`🧹 Cleaning up extra directories...\n`);

  const items = fs.readdirSync(expectedPath);
  let removedCount = 0;

  for (const item of items) {
    const itemPath = path.join(expectedPath, item);

    // Only process directories
    if (!fs.statSync(itemPath).isDirectory()) {
      continue;
    }

    // Keep only directories named "3" and "85"
    if (item !== '3' && item !== '85') {
      console.log(`   🗑️  Removing directory: ${item}`);
      fs.rmSync(itemPath, { recursive: true, force: true });
      removedCount++;
    }
  }

  if (removedCount > 0) {
    console.log(`\n✅ Removed ${removedCount} extra directory(ies)\n`);
  } else {
    console.log(`   ✅ No extra directories to remove\n`);
  }

  // Cleanup: Remove the downloaded archive
  console.log(`🗑️  Cleaning up archive file...\n`);
  fs.unlinkSync(archivePath);

  console.log(`✅ Data ready for processing\n`);

  return today;
}

/**
 * Step 2: Read prices from the downloaded data
 */
function readPricesFile(date: string, categoryId: string, groupId: string): PriceEntry[] {
  const filePath = path.join(DATA_DIR, date, categoryId, groupId, 'prices');

  if (!fs.existsSync(filePath)) {
    return [];
  }

  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    const data = JSON.parse(content);

    if (!data.success || !data.results) {
      return [];
    }

    return data.results.filter((entry: PriceEntry) => entry.marketPrice !== null);
  } catch (error) {
    console.error(`⚠️  Error reading ${filePath}:`, error);
    return [];
  }
}

/**
 * Step 3: Get all group IDs for a date
 */
function getGroupIdsForDate(date: string, categoryId: string): string[] {
  const categoryPath = path.join(DATA_DIR, date, categoryId);

  if (!fs.existsSync(categoryPath)) {
    return [];
  }

  return fs.readdirSync(categoryPath).filter(item => {
    const itemPath = path.join(categoryPath, item);
    return fs.statSync(itemPath).isDirectory();
  });
}

/**
 * Step 3a: Load blacklist from file
 */
function loadBlacklist(): Set<string> {
  const blacklistPath = path.join(process.cwd(), 'product-blacklist.json');

  if (!fs.existsSync(blacklistPath)) {
    return new Set();
  }

  try {
    const content = fs.readFileSync(blacklistPath, 'utf-8');
    const data = JSON.parse(content);
    return new Set(data.blacklistedProducts || []);
  } catch (error) {
    console.error('⚠️  Error loading blacklist:', error);
    return new Set();
  }
}

/**
 * Step 3b: Save blacklist to file
 */
function saveBlacklist(blacklist: Set<string>): void {
  const blacklistPath = path.join(process.cwd(), 'product-blacklist.json');

  try {
    const data = {
      blacklistedProducts: Array.from(blacklist),
      lastUpdated: new Date().toISOString()
    };
    fs.writeFileSync(blacklistPath, JSON.stringify(data, null, 2), 'utf-8');
  } catch (error) {
    console.error('⚠️  Error saving blacklist:', error);
  }
}

/**
 * Step 3c: Fetch product details from tcgcsv.com API
 */
async function fetchProductDetails(
  categoryId: string,
  groupId: string,
  productId: number
): Promise<ProductDetails | null> {
  const url = `https://tcgcsv.com/tcgplayer/${categoryId}/${groupId}/products`;

  try {
    const response = await fetch(url);

    if (!response.ok) {
      console.error(`   ⚠️  Failed to fetch products for group ${groupId}: ${response.status}`);
      return null;
    }

    const data: GroupProductsResponse = await response.json();

    if (!data.success || !data.results) {
      console.error(`   ⚠️  Invalid response from API for group ${groupId}`);
      return null;
    }

    // Find the product in the results
    const product = data.results.find(p => p.productId === productId);

    if (!product) {
      console.error(`   ⚠️  Product ${productId} not found in group ${groupId}`);
      return null;
    }

    return {
      productId: product.productId,
      name: product.name,
      groupId: parseInt(groupId)
    };
  } catch (error) {
    console.error(`   ⚠️  Error fetching product details for ${productId}:`, error);
    return null;
  }
}

/**
 * Step 3d: Check if product should be blacklisted
 */
function shouldBlacklist(productName: string): boolean {
  const blacklistPatterns = [
    'Code Card',
    'code card',
    'CODE CARD'
  ];

  return blacklistPatterns.some(pattern => productName.includes(pattern));
}

/**
 * Step 4: Generate variant key
 */
function generateVariantKey(productId: number, finish: string | null): string {
  return `${productId}:${finish || 'Normal'}`;
}

/**
 * Step 5: Update price history for today's date
 */
async function updatePriceHistory(date: string): Promise<void> {
  console.log(`\n┌─ Processing ${date}`);

  // Load blacklist
  const blacklist = loadBlacklist();
  const newBlacklistedProducts = new Set<string>();
  console.log(`   🚫 Loaded blacklist: ${blacklist.size} products`);

  const allUpdates: PriceUpdate[] = [];

  // Get existing products from database
  console.log(`   🔍 Fetching existing products from database...`);
  const { data: existingProducts, error: fetchError } = await supabase
    .from('products')
    .select('variant_key');

  if (fetchError) {
    console.error(`   ❌ Error fetching existing products:`, fetchError);
    throw new Error('Failed to fetch existing products');
  }

  const existingVariantKeys = new Set(
    existingProducts?.map(p => p.variant_key) || []
  );
  console.log(`   ✅ Found ${existingVariantKeys.size.toLocaleString()} existing products`);

  // Track new products that need lookup
  const newProductsMap = new Map<string, { categoryId: string; groupId: string; productId: number; finish: string }>();

  // Collect all price updates for this date
  for (const categoryId of ['3', '85']) {
    const groupIds = getGroupIdsForDate(date, categoryId);
    console.log(`   📂 Category ${categoryId}: ${groupIds.length} groups`);

    for (const groupId of groupIds) {
      const priceEntries = readPricesFile(date, categoryId, groupId);

      for (const entry of priceEntries) {
        const variantKey = generateVariantKey(entry.productId, entry.subTypeName);

        // Check if product is blacklisted
        if (blacklist.has(variantKey)) {
          continue; // Skip blacklisted products
        }

        // If product doesn't exist in DB and not in blacklist, mark for lookup
        if (!existingVariantKeys.has(variantKey)) {
          newProductsMap.set(variantKey, {
            categoryId,
            groupId,
            productId: entry.productId,
            finish: entry.subTypeName || 'Normal'
          });
        }

        allUpdates.push({
          variant_key: variantKey,
          date: date,
          price: entry.marketPrice!
        });
      }
    }
  }

  console.log(`   📊 Collected ${allUpdates.length.toLocaleString()} price updates`);
  console.log(`   🆕 Detected ${newProductsMap.size.toLocaleString()} new products`);

  // Fetch details for new products
  if (newProductsMap.size > 0) {
    console.log(`   🔎 Fetching details for new products...`);

    // Group by categoryId + groupId to minimize API calls
    const groupedByCategory = new Map<string, Set<number>>();

    for (const [variantKey, info] of newProductsMap.entries()) {
      const key = `${info.categoryId}:${info.groupId}`;
      if (!groupedByCategory.has(key)) {
        groupedByCategory.set(key, new Set());
      }
      groupedByCategory.get(key)!.add(info.productId);
    }

    let lookupCount = 0;
    let blacklistedCount = 0;
    let validCount = 0;

    for (const [key, productIds] of groupedByCategory.entries()) {
      const [categoryId, groupId] = key.split(':');

      console.log(`   🌐 Fetching group ${groupId} (category ${categoryId}) - ${productIds.size} products...`);

      // Fetch all products for this group
      const url = `https://tcgcsv.com/tcgplayer/${categoryId}/${groupId}/products`;

      try {
        const response = await fetch(url);

        if (!response.ok) {
          console.error(`   ⚠️  Failed to fetch products for group ${groupId}: ${response.status}`);
          continue;
        }

        const data: GroupProductsResponse = await response.json();

        if (!data.success || !data.results) {
          console.error(`   ⚠️  Invalid response from API for group ${groupId}`);
          continue;
        }

        // Check each product we're looking for
        for (const productId of productIds) {
          const product = data.results.find(p => p.productId === productId);

          if (!product) {
            console.error(`   ⚠️  Product ${productId} not found in group ${groupId}`);
            continue;
          }

          lookupCount++;

          // Check if product should be blacklisted
          if (shouldBlacklist(product.name)) {
            console.log(`   🚫 Blacklisting: ${product.name} (${productId})`);

            // Find all variant keys for this product
            for (const [variantKey, info] of newProductsMap.entries()) {
              if (info.productId === productId && info.categoryId === categoryId && info.groupId === groupId) {
                newBlacklistedProducts.add(variantKey);

                // Remove from updates
                const updateIndex = allUpdates.findIndex(u => u.variant_key === variantKey);
                if (updateIndex !== -1) {
                  allUpdates.splice(updateIndex, 1);
                }
              }
            }

            blacklistedCount++;
            continue;
          }

          // Add product details to all relevant updates
          for (const [variantKey, info] of newProductsMap.entries()) {
            if (info.productId === productId && info.categoryId === categoryId && info.groupId === groupId) {
              const updateIndex = allUpdates.findIndex(u => u.variant_key === variantKey);
              if (updateIndex !== -1) {
                allUpdates[updateIndex].product_id = productId;
                allUpdates[updateIndex].product_name = product.name;
                allUpdates[updateIndex].group_id = parseInt(groupId);
                validCount++;
              }
            }
          }
        }

        // Add small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 100));
      } catch (error) {
        console.error(`   ⚠️  Error fetching products for group ${groupId}:`, error);
      }
    }

    console.log(`   ✅ Looked up ${lookupCount} products`);
    console.log(`   ✅ Valid new products: ${validCount}`);
    console.log(`   🚫 Blacklisted: ${blacklistedCount} products`);

    // Save updated blacklist
    if (newBlacklistedProducts.size > 0) {
      const updatedBlacklist = new Set([...blacklist, ...newBlacklistedProducts]);
      saveBlacklist(updatedBlacklist);
      console.log(`   💾 Saved ${updatedBlacklist.size} products to blacklist`);
    }
  }

  if (allUpdates.length === 0) {
    console.log(`   ⏭️  No updates for this date`);
    return;
  }

  // Send updates to PostgreSQL in batches
  let totalUpdated = 0;
  let totalInserted = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  const numBatches = Math.ceil(allUpdates.length / BATCH_SIZE);

  for (let i = 0; i < allUpdates.length; i += BATCH_SIZE) {
    const batch = allUpdates.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;

    console.log(`   ⚙️  Processing batch ${batchNum}/${numBatches} (${batch.length} updates)...`);

    // Retry logic for timeouts
    let retries = 3;
    let success = false;

    while (retries > 0 && !success) {
      try {
        const { data, error } = await supabase.rpc('batch_update_price_history', {
          batch_data: batch
        });

        if (error) {
          // Check if it's a timeout error (code 57014)
          if (error.code === '57014' && retries > 1) {
            console.log(`   ⏳ Batch ${batchNum} timed out, retrying (${retries - 1} attempts left)...`);
            retries--;
            await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2 seconds before retry
            continue;
          }

          console.error(`   ❌ Error processing batch ${batchNum}:`, error);
          totalErrors += batch.length;
          break;
        }

        if (data && data.length > 0) {
          const result = data[0];
          totalUpdated += result.updated_count || 0;
          totalInserted += result.inserted_count || 0;
          totalSkipped += result.skipped_count || 0;
          totalErrors += result.error_count || 0;
        }

        success = true;

      } catch (error) {
        console.error(`   ❌ Exception processing batch ${batchNum}:`, error);
        totalErrors += batch.length;
        break;
      }
    }

    if (!success && retries === 0) {
      console.error(`   ❌ Batch ${batchNum} failed after 3 timeout attempts`);
    }
  }

  console.log(`   ✅ Complete: ${totalUpdated} updated, ${totalInserted} inserted, ${totalSkipped} skipped, ${totalErrors} errors`);
}

/**
 * Main execution
 */
async function main() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║              DAILY PRICE UPDATE - TCG PLAYER DATA              ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');

  const startTime = Date.now();

  try {
    // Step 1: Download and extract today's data
    const today = await downloadAndExtractTodaysData();

    // Step 2: Update price history in database
    await updatePriceHistory(today);

    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log('║                    UPDATE COMPLETE                             ║');
    console.log('╚════════════════════════════════════════════════════════════════╝\n');
    console.log(`✅ Successfully updated prices for ${today}`);
    console.log(`⏱️  Total time: ${totalTime}s\n`);

  } catch (error) {
    console.error('\n❌ Daily update failed:', error);
    process.exit(1);
  }
}

// Run if executed directly (ES module compatible)
const isMainModule = import.meta.url === `file://${process.argv[1]}`;

if (isMainModule) {
  main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

export { main as runDailyUpdate };
