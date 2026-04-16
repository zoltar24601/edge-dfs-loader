import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MLB = 'https://statsapi.mlb.com/api/v1';
let saved = 0, errors = 0;

async function sbUpsert(table, data, conflictCols) {
  // First try DELETE then INSERT (avoids on_conflict issues)
  if (conflictCols && data.catcher_id && data.season) {
    try {
      await fetch(SUPABASE_URL + '/rest/v1/' + table + '?catcher_id=eq.' + data.catcher_id + '&season=eq.' + data.season, {
        method: 'DELETE',
        headers: { 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY }
      });
    } catch(e) {}
  }
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY },
    body: JSON.stringify(data)
  });
  if (!res.ok) { const t = await res.text(); throw new Error(t); }
}

function sleep(ms){return new Promise(r=>setTimeout(r,ms))}
function r3(v){return Math.round(v*1000)/1000}

async function fetchCatcherStats(catcherId, season) {
  try {
    const r = await fetch(MLB + '/people/' + catcherId + '/stats?stats=season&season=' + season + '&group=catching');
    const d = await r.json();
    const stats = d.stats?.[0]?.splits?.[0]?.stat;
    if (!stats) return null;
    const sb = parseInt(stats.stolenBases || 0);
    const cs = parseInt(stats.caughtStealing || 0);
    const innings = parseFloat(stats.inningsPlayed || stats.inningsCaught || 0);
    return {
      sb, cs,
      innings,
      csPct: (sb + cs) > 0 ? cs / (sb + cs) : null,
    };
  } catch(e) { return null; }
}

async function main() {
  console.log('🥎 EDGE DFS CATCHER LOADER');
  
  // Get today's games
  const today = new Date().toISOString().split('T')[0];
  const schedRes = await fetch(MLB + '/schedule?sportId=1&date=' + today);
  const schedData = await schedRes.json();
  const games = schedData.dates?.[0]?.games || [];
  console.log('Games:', games.length);
  if (!games.length) { console.log('No games today'); return; }

  // Get all catchers from rosters
  const teamIds = new Set();
  games.forEach(g => { teamIds.add(g.teams.away.team.id); teamIds.add(g.teams.home.team.id); });

  const catchers = [];
  for (const tid of teamIds) {
    try {
      const r = await fetch(MLB + '/teams/' + tid + '/roster?rosterType=active&hydrate=person');
      const d = await r.json();
      const teamAbbr = d.teams?.[0]?.abbreviation || '';
      for (const p of (d.roster || [])) {
        const pos = p.position?.abbreviation || '';
        if (pos === 'C') {
          const gm = games.find(g => g.teams.away.team.id === tid || g.teams.home.team.id === tid);
          const isAway = gm?.teams.away.team.id === tid;
          const abbr = isAway ? gm?.teams.away.team.abbreviation : gm?.teams.home.team.abbreviation;
          catchers.push({ id: p.person.id, name: p.person.fullName, team: abbr || '???' });
        }
      }
    } catch(e) {}
  }
  console.log('Found', catchers.length, 'catchers');

  for (let i = 0; i < catchers.length; i++) {
    const c = catchers[i];
    try {
      // Try 2026, fall back to 2025
      let stats = await fetchCatcherStats(c.id, 2026);
      let season = 2026;
      if (!stats || stats.innings < 50) {
        const s25 = await fetchCatcherStats(c.id, 2025);
        if (s25 && s25.innings > (stats?.innings || 0)) {
          stats = s25;
          season = 2025;
        }
      }

      if (!stats) { console.log((i+1)+'/'+catchers.length, c.name, '✗ no data'); continue; }

      await sbUpsert('edge_catcher_cache', {
        catcher_id: c.id,
        catcher_name: c.name,
        team: c.team,
        season: 2025, // store under 2025 as primary key for consistency
        innings_caught: stats.innings,
        sb_against: stats.sb,
        cs_caught: stats.cs,
        cs_pct: stats.csPct ? r3(stats.csPct) : null,
        updated_at: new Date().toISOString(),
      }, 'catcher_id,season');

      saved++;
      console.log((i+1)+'/'+catchers.length, c.name, '✓', 'SB:'+stats.sb, 'CS:'+stats.cs, 'CS%:'+(stats.csPct ? (stats.csPct*100).toFixed(1)+'%' : 'N/A'), '['+season+']');
      await sleep(200);
    } catch(e) {
      errors++;
      console.log((i+1)+'/'+catchers.length, c.name, '✗', e.message.substring(0, 80));
    }
  }

  console.log('========================================');
  console.log('CATCHER LOADER DONE! Saved:', saved, '| Errors:', errors);
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
