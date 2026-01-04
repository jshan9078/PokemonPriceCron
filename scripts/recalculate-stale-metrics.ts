import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';

// Load environment variables (defaults to .env in cwd)
dotenv.config();

const supabase = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
);

async function main() {
    const cutoffTime = new Date().toISOString();
    console.log(`🔄 Recalculating metrics for all products (cutoff: ${cutoffTime})...`);

    let totalUpdated = 0;
    const BATCH_SIZE = 1000;

    while (true) {
        const { error, data } = await supabase.rpc('recalculate_stale_metrics', {
            cutoff_time: cutoffTime,
            batch_size: BATCH_SIZE
        });

        if (error) {
            console.error('❌ Error recalculating metrics:', error);
            process.exit(1);
        }

        const count = data && data[0] ? data[0].updated_count : 0;

        if (count === 0) {
            break;
        }

        totalUpdated += count;
        console.log(`  ✅ Processed batch of ${count} products (Total: ${totalUpdated})`);
    }

    console.log(`✅ Successfully recalculated metrics for ${totalUpdated} total products.`);
}

main().catch(console.error);
