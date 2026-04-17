// FanGraphs Park Factors Scraper
// Run once a year (or whenever needed) to update park factors
// Usage: SUPABASE_URL=... SUPABASE_KEY=... node park-factors-loader.js

import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const SEASON = process.env.SEASON || '2025'; // or 2026 once data is meaningful

// FG team name to MLB abbreviation mapping
const FG_TO_ABBR = {
  'Angels': 'LAA', 'Astros': 'HOU', 'Athletics': 'ATH', 'Blue Jays': 'TOR',
  'Braves': 'ATL', 'Brewers': 'MIL', 'Cardinals': 'STL', 'Cubs': 'CHC',
  'D-backs': 'ARI', 'Diamondbacks': 'ARI', 'Dodgers': 'LAD', 'Giants': 'SF',
  'Guardians': 'CLE', 'Indians': 'CLE', 'Mariners': 'SEA', 'Marlins': 'MIA',
  'Mets': 'NYM', 'Nationals': 'WSH', 'Orioles': 'BAL', 'Padres': 'SD',
  'Phillies': 'PHI', 'Pirates': 'PIT', 'Rangers': 'TEX', 'Rays': 'TB',
  'Red Sox': 'BOS', 'Reds': 'CIN', 'Rockies': 'COL', 'Royals': 'KC',
  'Tigers': 'DET', 'Twins': 'MIN', 'White Sox': 'CWS', 'Yankees': 'NYY',
};

async function sbInsert(table, data) {
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY },
    body: JSON.stringify(data)
  });
  if (!res.ok) throw new Error(await res.text());
}

async function sbDelete(table, where) {
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table + '?' + where, {
    method: 'DELETE',
    headers: { 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY }
  });
}

async function main() {
  console.log('🏟️  PARK FACTORS LOADER — Season ' + SEASON);
  
  // FG Guts page with handedness park factors
  // type=pf-hand returns HR factors by batter handedness
  const url = 'https://www.fangraphs.com/guts.aspx?type=pf-hand&season=' + SEASON + '&teamid=0';
  console.log('Fetching:', url);
  
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml',
    }
  });
  
  if (!res.ok) {
    console.error('FG fetch failed:', res.status, res.statusText);
    process.exit(1);
  }
  
  const html = await res.text();
  console.log('HTML length:', html.length);
  
  // Find the data table — FG uses table id="GutsBoard1_dg1"
  // Match all <tr> rows in the data table
  const tableMatch = html.match(/<table[^>]*id="GutsBoard1[^"]*"[^>]*>([\s\S]*?)<\/table>/i);
  if (!tableMatch) {
    // Try a more permissive match
    const allTables = html.match(/<table[^>]*class="[^"]*rgMasterTable[^"]*"[^>]*>([\s\S]*?)<\/table>/gi);
    if (!allTables || !allTables.length) {
      console.error('Could not find FG park factors table in HTML');
      console.log('Sample HTML around "park":', html.match(/.{200}park.{200}/i)?.[0]);
      process.exit(1);
    }
    console.log('Found via permissive match');
  }
  
  const tableHtml = tableMatch ? tableMatch[1] : html;
  
  // Parse rows
  const rowMatches = [...tableHtml.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)];
  console.log('Rows found:', rowMatches.length);
  
  // First row is header. Look for: Season | Team | Basic (5 yr) | 3yr | 1yr | HR as L | HR as R | ...
  const headerRow = rowMatches[0]?.[1] || '';
  const headerCells = [...headerRow.matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/gi)].map(m => m[1].replace(/<[^>]+>/g, '').trim());
  console.log('Headers:', headerCells.join(' | '));
  
  // Find column indexes
  const colIdx = {};
  headerCells.forEach((h, i) => {
    if (h === 'Team') colIdx.team = i;
    if (h === 'Season') colIdx.season = i;
    if (h === 'HR as L') colIdx.hrAsL = i;
    if (h === 'HR as R') colIdx.hrAsR = i;
    if (h === '1B as L') colIdx.oneBAsL = i;
    if (h === '1B as R') colIdx.oneBAsR = i;
    if (h === '2B as L') colIdx.twoBAsL = i;
    if (h === '2B as R') colIdx.twoBAsR = i;
    if (h === '3B as L') colIdx.threeBAsL = i;
    if (h === '3B as R') colIdx.threeBAsR = i;
    if (h === 'BB as L') colIdx.bbAsL = i;
    if (h === 'BB as R') colIdx.bbAsR = i;
    if (h === 'SO as L') colIdx.soAsL = i;
    if (h === 'SO as R') colIdx.soAsR = i;
    if (h === 'Basic (5 yr)' || h === 'Basic') colIdx.basic5yr = i;
  });
  console.log('Column indexes:', colIdx);
  
  if (colIdx.team === undefined || colIdx.hrAsL === undefined || colIdx.hrAsR === undefined) {
    console.error('Could not find required columns in header');
    process.exit(1);
  }
  
  // Clear existing season data
  await sbDelete('edge_park_factors', 'season=eq.' + SEASON);
  
  // Parse data rows
  let saved = 0;
  for (let i = 1; i < rowMatches.length; i++) {
    const cells = [...rowMatches[i][1].matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/gi)].map(m => m[1].replace(/<[^>]+>/g, '').trim());
    if (cells.length < 5) continue;
    const teamName = cells[colIdx.team];
    const abbr = FG_TO_ABBR[teamName];
    if (!abbr) {
      console.log('  ✗ Unknown team:', teamName);
      continue;
    }
    
    const data = {
      team: abbr,
      season: parseInt(SEASON),
      basic_5yr: colIdx.basic5yr !== undefined ? parseInt(cells[colIdx.basic5yr]) || null : null,
      hr_as_l: parseInt(cells[colIdx.hrAsL]) || null,
      hr_as_r: parseInt(cells[colIdx.hrAsR]) || null,
      one_b_as_l: colIdx.oneBAsL !== undefined ? parseInt(cells[colIdx.oneBAsL]) || null : null,
      one_b_as_r: colIdx.oneBAsR !== undefined ? parseInt(cells[colIdx.oneBAsR]) || null : null,
      two_b_as_l: colIdx.twoBAsL !== undefined ? parseInt(cells[colIdx.twoBAsL]) || null : null,
      two_b_as_r: colIdx.twoBAsR !== undefined ? parseInt(cells[colIdx.twoBAsR]) || null : null,
      three_b_as_l: colIdx.threeBAsL !== undefined ? parseInt(cells[colIdx.threeBAsL]) || null : null,
      three_b_as_r: colIdx.threeBAsR !== undefined ? parseInt(cells[colIdx.threeBAsR]) || null : null,
      bb_as_l: colIdx.bbAsL !== undefined ? parseInt(cells[colIdx.bbAsL]) || null : null,
      bb_as_r: colIdx.bbAsR !== undefined ? parseInt(cells[colIdx.bbAsR]) || null : null,
      so_as_l: colIdx.soAsL !== undefined ? parseInt(cells[colIdx.soAsL]) || null : null,
      so_as_r: colIdx.soAsR !== undefined ? parseInt(cells[colIdx.soAsR]) || null : null,
      updated_at: new Date().toISOString(),
    };
    
    try {
      await sbInsert('edge_park_factors', data);
      saved++;
      console.log('  ✓', abbr, 'HR(L):', data.hr_as_l, 'HR(R):', data.hr_as_r);
    } catch(e) {
      console.error('  ✗', abbr, e.message);
    }
  }
  
  console.log('========================================');
  console.log('PARK FACTORS LOADER DONE! Saved:', saved, '/ 30');
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
