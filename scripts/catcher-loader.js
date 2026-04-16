// ============================================================
// EDGE DFS — Catcher Loader
// Loads catcher CS% from MLB Stats API
// ============================================================
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MLB = 'https://statsapi.mlb.com/api/v1';

let saved = 0, errors = 0;

async function sbUpsert(table, data, conflictCols) {
  const conflict = conflictCols ? '?on_conflict=' + conflictCols : '';
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table + conflict, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Prefer': 'resolution=merge-duplicates' },
    body: JSON.stringify(data)
  });
  if (!res.ok) { const t = await res.text(); throw new Error(t); }
}

function sleep(ms){return new Promise(r=>setTimeout(r,ms))}

async function fetchCatcherStats(catcherId) {
  try {
    let stats = null, season = 2026;
    const r26 = await fetch(MLB + '/people/' + catcherId + '/stats?stats=season&season=2026&group=catching');
    const d26 = await r26.json();
    stats = d26.stats?.[0]?.splits?.[0]?.stat;
    let inn = stats ? parseFloat(stats.innings || 0) : 0;
    if (!stats || inn < 100) {
      const r25 = await fetch(MLB + '/people/' + catcherId + '/stats?stats=season&season=2025&group=catching');
      const d25 = await r25.json();
      const s25 = d25.stats?.[0]?.splits?.[0]?.stat;
      if (s25 && parseFloat(s25.innings || 0) > inn) {
        stats = s25;
        season = 2025;
      }
    }
    if (!stats) return null;
    const sb = parseInt(stats.stolenBases || 0);
    const cs = parseInt(stats.caughtStealing || 0);
    const csPct = (sb + cs) > 0 ? cs / (sb + cs) : null;
    return { csPct, sb, cs, season, framingRuns: null };
  } catch(e) {
    return null;
  }
}

async function main() {
  console.log('============================================================');
  console.log('EDGE DFS — Catcher Loader');
  console.log('============================================================');

  const teamsRes = await fetch(MLB + '/teams?sportId=1');
  const teamsData = await teamsRes.json();
  const teams = teamsData.teams.filter(t => t.active && t.sport?.id === 1);
  console.log('Teams found:', teams.length);

  const allCatchers = [];
  for (const team of teams) {
    try {
      const rRes = await fetch(MLB + '/teams/' + team.id + '/roster?rosterType=active');
      const rData = await rRes.json();
      const catchers = (rData.roster || []).filter(p => p.position?.abbreviation === 'C');
      catchers.forEach(c => {
        allCatchers.push({
          id: c.person.id,
          name: c.person.fullName,
          team: team.abbreviation,
        });
      });
    } catch(e) { /* skip */ }
    await sleep(150);
  }

  console.log('Catchers found:', allCatchers.length);

  for (let i = 0; i < allCatchers.length; i++) {
    const c = allCatchers[i];
    try {
      const stats = await fetchCatcherStats(c.id);
      if (!stats) {
        console.log((i+1) + '/' + allCatchers.length, c.name, '✗ No stats');
        continue;
      }
      await sbUpsert('edge_catcher_cache', {
        catcher_id: c.id,
        catcher_name: c.name,
        team: c.team,
        cs_pct: stats.csPct,
        framing_runs: stats.framingRuns,
        season: stats.season,
        updated_at: new Date().toISOString(),
      }, 'catcher_id');
      saved++;
      const csPctStr = stats.csPct !== null ? (stats.csPct * 100).toFixed(1) + '%' : 'N/A';
      console.log((i+1) + '/' + allCatchers.length, c.name, '(' + c.team + ') ✓ CS%:', csPctStr);
      await sleep(300);
    } catch(e) {
      errors++;
      console.log((i+1) + '/' + allCatchers.length, c.name, '✗', e.message?.substring(0, 80));
    }
  }

  console.log('========================================');
  console.log('CATCHER LOADER DONE! Saved:', saved, '| Errors:', errors);
}

main().catch(e => { console.error(e); process.exit(1); });
