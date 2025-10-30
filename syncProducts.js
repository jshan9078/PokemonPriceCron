// syncProducts.js
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config();

// Supabase client (service role key required for inserting)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// Base API endpoint
const BASE_URL = "https://tcgcsv.com/tcgplayer";

const CATEGORY_IDS = [3, 85]; // Pokémon Singles (3) & Pokémon Sealed (85) or whatever you're using

// Store unique rarity levels
const raritySet = new Set();

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Request failed: ${url} -> ${res.status}`);
  return res.json();
}

async function processCategory(categoryId) {
  console.log(`\n📦 Fetching groups for category: ${categoryId}`);

  const groups = await fetchJson(`${BASE_URL}/${categoryId}/groups`);
  console.log(`→ Found ${groups.totalItems} groups.`);

  for (const group of groups.results) {
    console.log(`\n📁 Group ${group.groupId} - ${group.name}`);

    const products = await fetchJson(`${BASE_URL}/${categoryId}/${group.groupId}/products`);

    for (const product of products.results) {
      const productId = product.productId;

      // Default values
      let rarity = null;
      let number = null;

      for (const data of product.extendedData ?? []) {
        if (data.name === "Rarity") rarity = data.value;
        if (data.name === "Number") number = data.value;
      }

      if (rarity) raritySet.add(rarity);

      // Insert into Supabase
      const { error } = await supabase
        .from("products")
        .upsert(
          {
            product_id: productId,
            group_id: group.groupId,
            rarity,
            number
          },
          { onConflict: "id" }
        );

      if (error) {
        console.error(`❌ Supabase insert failed for ${productId}`, error);
      } else {
        console.log(`✅ Inserted product ${productId} (rarity: ${rarity}, number: ${number})`);
      }
    }
  }
}

(async () => {
  console.log("🚀 Starting TCGPlayer sync...\n");

  for (const categoryId of CATEGORY_IDS) {
    await processCategory(categoryId);
  }

  console.log("\n🎉 Unique rarity levels found:");
  console.log([...raritySet].sort());

  console.log("\n✅ Sync completed.");
})();
