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

function r1(v){return Math.round(v*10)/10}
function r3(v){return Math.round(v*1000)/1000}
function sleep(ms){return new Promise(r=>setTimeout(r,ms))}

const LABELS = {FF:'4-Seam FB',SI:'Sinker',FC:'Cutter',FA:'Fastball',SL:'Slider',CU:'Curveball',KC:'Knuckle-Curve',CS:'Slow Curve',CH:'Changeup',FS:'Splitter',ST:'Sweeper',SV:'Slurve',SW:'Sweeper'};

// ============================================================
//  BULK SAVANT STATS — pre-computed xSLG, xwOBA, barrel% by hand
//  2 API calls get ALL pitchers at once (vs RHB, vs LHB)
// ============================================================

async function fetchSavantBulk(playerType, batterHand, season) {
  const url = 'https://baseballsavant.mlb.com/statcast_search/csv?all=true' +
    '&player_type=' + playerType +
    '&batter_stands=' + batterHand +
    '&hfSea=' + season + '%7C' +
    '&group_by=name' +
    '&min_results=25' +
    '&type=details' +
    '&sort_col=pitches&sort_order=desc';
  
  console.log('Fetching Savant bulk:', playerType, 'vs', batterHand + 'HB...');
  
  for (let att = 0; att < 3; att++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!r.ok) { console.warn('Savant returned', r.status); await sleep(5000); continue; }
      const text = await r.text();
      if (!text || text.length < 200) { console.warn('Savant empty response'); await sleep(5000); continue; }
      
      const lines = text.trim().split('\n');
      if (lines.length < 2) return {};
      const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
      
      const results = {};
      for (let i = 1; i < lines.length; i++) {
        const vals = lines[i].split(',').map(v => v.trim().replace(/"/g, ''));
        const row = {};
        headers.forEach((h, idx) => { row[h] = vals[idx] || ''; });
        
        const playerId = parseInt(row.player_id || row.pitcher || row.batter);
        if (!playerId) continue;
        
        const pf = (field) => { const v = parseFloat(row[field]); return !isNaN(v) ? v : null; };
        
        results[playerId] = {
          playerId,
          playerName: row.player_name || '',
          xslg: pf('xslg') != null ? r3(pf('xslg')) : null,
          xba: pf('xba') != null ? r3(pf('xba')) : null,
          xwoba: pf('xwoba') != null ? r3(pf('xwoba')) : null,
          slg: pf('slg_percent') != null ? r3(pf('slg_percent')) : null,
          barrelPct: pf('barrel_batted_rate') != null ? r1(pf('barrel_batted_rate')) : null,
          hardHitPct: pf('hard_hit_percent') != null ? r1(pf('hard_hit_percent')) : null,
          avgEV: pf('launch_speed') != null ? r1(pf('launch_speed')) : null,
          fbPct: pf('fly_ball_percent') != null ? r1(pf('fly_ball_percent')) : null,
        };
      }
      
      console.log('Got', Object.keys(results).length, playerType + 's vs', batterHand + 'HB');
      return results;
      
    } catch(e) {
      console.warn('Savant fetch error:', e.message);
      await sleep(5000 * (att + 1));
    }
  }
  console.warn('Failed to fetch Savant bulk data for', playerType, 'vs', batterHand);
  return {};
}

// ============================================================
//  PITCH-BY-PITCH — only for arsenal data (pitch mix, velocity)
// ============================================================
function parseCSV(text){
  if(!text||text.length<100)return null;
  const lines=text.trim().split('\n');if(lines.length<2)return null;
  const hdrs=lines[0].split(',').map(h=>h.trim().replace(/"/g,''));
  const rows=[];
  for(let i=1;i<lines.length;i++){
    const vals=[];let cur='',inQ=false;
    for(let c=0;c<lines[i].length;c++){
      const ch=lines[i][c];
      if(ch==='"'){inQ=!inQ}
      else if(ch===','&&!inQ){vals.push(cur.trim());cur=''}
      else{cur+=ch}
    }
    vals.push(cur);
    if(vals.length<hdrs.length/2)continue;
    const row={};hdrs.forEach((h,idx)=>{row[h]=vals[idx]?.trim().replace(/"/g,'')||'';});
    rows.push(row);
  }
  return rows;
}

function computeArsenalByHand(rows, batterHand) {
  const filtered = batterHand ? rows.filter(r => r.stand === batterHand) : rows;
  if (filtered.length < 50) return null;
  const byType = {};
  filtered.forEach(r => {
    const pt = r.pitch_type;
    if (!pt || pt === 'PO' || pt === 'IN') return;
    if (!byType[pt]) byType[pt] = { count: 0, velos: [] };
    byType[pt].count++;
    const v = parseFloat(r.release_speed);
    if (!isNaN(v)) byType[pt].velos.push(v);
  });
  const total = Object.values(byType).reduce((s, t) => s + t.count, 0);
  if (total < 50) return null;
  const result = {};
  Object.entries(byType).forEach(([pt, d]) => {
    if (d.count / total < 0.02) return;
    result[pt] = {
      label: LABELS[pt] || pt,
      usage: Math.round(d.count / total * 1000) / 1000,
      velo: d.velos.length ? Math.round(d.velos.reduce((a,b)=>a+b,0)/d.velos.length*10)/10 : null,
    };
  });
  return Object.keys(result).length ? result : null;
}

async function fetchCSV(playerId, season) {
  const url = 'https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&player_type=pitcher&pitchers_lookup%5B%5D=' + playerId + '&hfSea=' + season + '&group_by=name&min_pitches=0&min_results=0&min_pas=0';
  for (let att = 0; att < 3; att++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (r.ok) { const t = await r.text(); if (t && t.length > 100) return t; }
      await sleep(2000 * att);
    } catch(e) {
      await sleep(2000 * att);
    }
  }
  return null;
}

async function fetchPitcherSeasonStats(pitcherId) {
  try {
    let stats = null, season = 2026;
    const r26 = await fetch(MLB + '/people/' + pitcherId + '/stats?stats=season&season=2026&group=pitching');
    const d26 = await r26.json();
    stats = d26.stats?.[0]?.splits?.[0]?.stat;
    let ip = stats ? parseFloat(stats.inningsPitched || 0) : 0;
    if (ip < 10) {
      const r25 = await fetch(MLB + '/people/' + pitcherId + '/stats?stats=season&season=2025&group=pitching');
      const d25 = await r25.json();
      stats = d25.stats?.[0]?.splits?.[0]?.stat;
      season = 2025;
    }
    if (!stats) return null;
    const sb = parseInt(stats.stolenBases || 0);
    const cs = parseInt(stats.caughtStealing || 0);
    const ipStr = stats.inningsPitched || '0';
    const gs = parseInt(stats.gamesStarted || 0);
    const bf = parseInt(stats.battersFaced || 0);
    let avgIpPerStart = null, avgBfPerStart = null;
    if (gs >= 3) {
      avgIpPerStart = parseFloat(ipStr) / gs;
      avgBfPerStart = bf / gs;
      if (avgIpPerStart < 4.0 || avgIpPerStart > 8.0) avgIpPerStart = null;
      if (avgBfPerStart < 18 || avgBfPerStart > 30) avgBfPerStart = null;
    }
    return { sb, cs, ip: ipStr, gs, gp: parseInt(stats.gamesPitched || 0), bf, avgIpPerStart, avgBfPerStart };
  } catch(e) { return null; }
}

async function main() {
  console.log('⚾ EDGE DFS PITCHER LOADER v2 — Pre-computed Savant stats');

  // STEP 1: Bulk fetch pre-computed Savant stats (2 calls for all pitchers)
  const savantVsR = await fetchSavantBulk('pitcher', 'R', '2025');
  await sleep(3000);
  const savantVsL = await fetchSavantBulk('pitcher', 'L', '2025');
  await sleep(3000);

  // STEP 2: Get all pitcher rosters
  console.log('Fetching all team rosters...');
  const teamsRes = await fetch(MLB + '/teams?sportId=1');
  const teamsData = await teamsRes.json();
  const allTeams = teamsData.teams || [];

  const pitchers = [];
  for (const team of allTeams) {
    try {
      const r = await fetch(MLB + '/teams/' + team.id + '/roster?rosterType=active&hydrate=person');
      const d = await r.json();
      for (const p of (d.roster || [])) {
        const pos = p.position?.abbreviation || '';
        if (pos !== 'P' && pos !== 'TWP') continue;
        const hand = p.person?.pitchHand?.code || 'R';
        pitchers.push({ id: p.person.id, name: p.person.fullName, team: team.abbreviation, hand });
      }
    } catch(e) {}
  }
  console.log('Found', pitchers.length, 'pitchers');

  // STEP 3: For each pitcher, fetch arsenal + season stats, merge with bulk Savant data
  for (let i = 0; i < pitchers.length; i++) {
    const p = pitchers[i];
    try {
      // Arsenal still needs pitch-by-pitch (pitch mix, velocity by type)
      const csv = await fetchCSV(p.id, '2025%7C2026');
      const rows = parseCSV(csv);
      
      let arsenalAll = null, arsenalVsR = null, arsenalVsL = null;
      if (rows && rows.length >= 100) {
        arsenalAll = computeArsenalByHand(rows, null);
        arsenalVsR = computeArsenalByHand(rows, 'R');
        arsenalVsL = computeArsenalByHand(rows, 'L');
      }
      
      const svR = savantVsR[p.id] || {};
      const svL = savantVsL[p.id] || {};
      
      if (!arsenalAll && !svR.xwoba && !svL.xwoba) {
        console.log((i+1) + '/' + pitchers.length, p.name, '— skipped (no data)');
        continue;
      }

      let era = null, whip = null, kPer9 = null, bbPer9 = null, mlbIp = null;
      try {
        const sr = await fetch(MLB + '/people/' + p.id + '/stats?stats=season&season=2025&group=pitching');
        const sd = await sr.json();
        const st = sd.stats?.[0]?.splits?.[0]?.stat;
        if (st) { era = parseFloat(st.era)||null; whip = parseFloat(st.whip)||null; kPer9 = parseFloat(st.strikeoutsPer9Inn)||null; bbPer9 = parseFloat(st.walksPer9Inn)||null; mlbIp = st.inningsPitched||null; }
      } catch(e) {}

      const ext = await fetchPitcherSeasonStats(p.id);

      await sbUpsert('edge_pitcher_cache', {
        pitcher_id: p.id, pitcher_name: p.name, team: p.team, hand: p.hand, season: 2025,
        arsenal: arsenalAll, arsenal_vs_r: arsenalVsR, arsenal_vs_l: arsenalVsL,
        era, whip, k_per_9: kPer9, bb_per_9: bbPer9, ip: mlbIp,
        // Pre-computed from Savant bulk (NOT calculated)
        xwoba_vs_r: svR.xwoba || null,
        xwoba_vs_l: svL.xwoba || null,
        xslg_allowed_r: svR.xslg || null,
        xslg_allowed_l: svL.xslg || null,
        slg_allowed_r: svR.slg || null,
        slg_allowed_l: svL.slg || null,
        barrel_pct_allowed_r: svR.barrelPct || null,
        barrel_pct_allowed_l: svL.barrelPct || null,
        hard_hit_pct_vs_r: svR.hardHitPct || null,
        hard_hit_pct_vs_l: svL.hardHitPct || null,
        avg_ev_allowed_r: svR.avgEV || null,
        avg_ev_allowed_l: svL.avgEV || null,
        fb_pct_r: svR.fbPct || null,
        fb_pct_l: svL.fbPct || null,
        // MLB API season stats
        sb_allowed: ext?.sb || 0,
        cs_caught: ext?.cs || 0,
        innings_pitched: ext?.ip || null,
        games_started: ext?.gs || 0,
        avg_ip_per_start: ext?.avgIpPerStart ? r1(ext.avgIpPerStart) : null,
        avg_bf_per_start: ext?.avgBfPerStart ? r1(ext.avgBfPerStart) : null,
        game_date: new Date().toISOString().split('T')[0],
        updated_at: new Date().toISOString(),
      }, 'pitcher_id,season');

      saved++;
      const splitR = svR.xwoba ? 'xwOBA:'+svR.xwoba+' xSLG:'+(svR.xslg||'?') : 'N/A';
      const splitL = svL.xwoba ? 'xwOBA:'+svL.xwoba+' xSLG:'+(svL.xslg||'?') : 'N/A';
      console.log((i+1) + '/' + pitchers.length, p.name, '(' + p.team + ' ' + p.hand + 'HP) ✓ vsR:', splitR, '| vsL:', splitL);

      if (i % 3 === 2) await sleep(1000);
      else await sleep(400);

    } catch(e) {
      errors++;
      console.error((i+1) + '/' + pitchers.length, p.name, '✗ ERROR:', e.message);
      await sleep(1000);
    }
  }

  console.log('\n✅ Pitcher loader complete. Saved:', saved, '| Errors:', errors);
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
