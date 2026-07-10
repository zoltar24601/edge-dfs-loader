// Dump edge_pitcher_cache to a JSON file (paged select).
// Used by pitcher-loader-v4.yml to snapshot the table before and
// after a v4 run so outputs can be diffed against the old loader.
// Env: SUPABASE_URL, SUPABASE_KEY, OUT_FILE
import fetch from 'node-fetch';
import fs from 'fs';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const OUT_FILE = process.env.OUT_FILE || 'pitcher-cache-dump.json';

if (!SUPABASE_URL || !SUPABASE_KEY) { console.error('Missing SUPABASE_URL / SUPABASE_KEY'); process.exit(1); }

async function main() {
  const out = [];
  const PAGE = 1000;
  for (let from = 0; ; from += PAGE) {
    const res = await fetch(SUPABASE_URL + '/rest/v1/edge_pitcher_cache?select=*&order=pitcher_id.asc,season.asc', {
      headers: { 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Range': from + '-' + (from + PAGE - 1) }
    });
    if (!res.ok) { const t = await res.text(); throw new Error('select ' + res.status + ': ' + t.substring(0, 120)); }
    const page = await res.json();
    out.push(...page);
    if (page.length < PAGE) break;
  }
  fs.writeFileSync(OUT_FILE, JSON.stringify(out));
  console.log('Dumped', out.length, 'rows to', OUT_FILE);
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
