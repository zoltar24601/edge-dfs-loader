
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

async function main() {
  console.log('Clearing edge_matchup_cache...');
  const res = await fetch(SUPABASE_URL + '/rest/v1/edge_matchup_cache', {
    method: 'DELETE',
    headers: {
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'apikey': SUPABASE_KEY,
      'Prefer': 'return=minimal'
    }
  });
  // Delete needs a filter or it won't delete anything - use a broad filter
  const res2 = await fetch(SUPABASE_URL + '/rest/v1/edge_matchup_cache?season=gte.0', {
    method: 'DELETE',
    headers: {
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'apikey': SUPABASE_KEY,
      'Prefer': 'return=minimal'
    }
  });
  console.log('Cleared. Status:', res2.status);
}

main().catch(e => { console.error(e); process.exit(1); });
