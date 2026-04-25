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

async function fetchSavantBulk(playerType, batterHand) {
  // NOTE: Do NOT include type=details — that returns pitch-by-pitch instead of aggregated.
  // hfGT=R%7C limits to regular-season games.
  const url = 'https://baseballsavant.mlb.com/statcast_search/csv' +
    '?hfGT=R%7C' +
    '&hfSea=2026%7C2025%7C' +
    '&player_type=' + playerType +
    '&batter_stands=' + batterHand +
    '&group_by=name' +
    '&min_pitches=0&min_results=0&min_pas=0' +
    '&sort_col=pitches&sort_order=desc';

  console.log('Fetching Savant bulk:', playerType, 'vs', batterHand + 'HB...');

  for (let att = 0; att < 3; att++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!r.ok) { console.warn('Savant returned', r.status); await sleep(5000); continue; }
      const text = await r.text();
      if (!text || text.length < 200 || text.includes('<html')) { console.warn('Savant empty/html response'); await sleep(5000); continue; }

      const lines = text.trim().split('\n');
      if (lines.length < 2) return {};
      // Strip UTF-8 BOM if present
      const headerLine = lines[0].charCodeAt(0) === 0xFEFF ? lines[0].slice(1) : lines[0];
      const headers = headerLine.split(',').map(h => h.trim().replace(/"/g, ''));

      // Quote-aware CSV split — handles commas inside "quoted" fields like
      // player names (e.g. "McCullers Jr., Lance"). Naive split(',') was
      // shifting all subsequent columns by 1, causing every stat field to
      // read from the wrong column.
      const splitCsv = (line) => {
        const out = [];
        let cur = '';
        let inQ = false;
        for (let j = 0; j < line.length; j++) {
          const c = line[j];
          if (c === '"') { inQ = !inQ; continue; }
          if (c === ',' && !inQ) { out.push(cur); cur = ''; continue; }
          cur += c;
        }
        out.push(cur);
        return out.map(v => v.trim());
      };

      const results = {};
      for (let i = 1; i < lines.length; i++) {
        const vals = splitCsv(lines[i]);
        const row = {};
        headers.forEach((h, idx) => { row[h] = vals[idx] || ''; });

        const playerId = parseInt(row.player_id);
        if (!playerId) continue;

        const pf = (field) => { const v = parseFloat(row[field]); return !isNaN(v) ? v : null; };

        results[playerId] = {
          playerId,
          playerName: row.player_name || '',
          xslg: pf('xslg') != null ? r3(pf('xslg')) : null,
          xba: pf('xba') != null ? r3(pf('xba')) : null,
          xwoba: pf('xwoba') != null ? r3(pf('xwoba')) : null,
          xobp: pf('xobp') != null ? r3(pf('xobp')) : null,
          barrelPct: pf('barrels_per_bbe_percent') != null ? r1(pf('barrels_per_bbe_percent')) : null,
          hardHitPct: pf('hardhit_percent') != null ? r1(pf('hardhit_percent')) : null,
          kPct: pf('k_percent') != null ? r1(pf('k_percent')) : null,
          bbPct: pf('bb_percent') != null ? r1(pf('bb_percent')) : null,
          pa: pf('pa') != null ? parseInt(pf('pa')) : null,
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
    let sb = 0, cs = 0, ipTotal = 0, gs = 0, gp = 0, bf = 0;
    let hr = 0, er = 0, ks = 0, bbs = 0, hits = 0;
    
    // Combine 2025 + 2026
    for (const yr of [2026, 2025]) {
      const r = await fetch(MLB + '/people/' + pitcherId + '/stats?stats=season&season=' + yr + '&group=pitching');
      const d = await r.json();
      const st = d.stats?.[0]?.splits?.[0]?.stat;
      if (st) {
        sb += parseInt(st.stolenBases || 0);
        cs += parseInt(st.caughtStealing || 0);
        ipTotal += parseFloat(st.inningsPitched || 0);
        gs += parseInt(st.gamesStarted || 0);
        gp += parseInt(st.gamesPitched || 0);
        bf += parseInt(st.battersFaced || 0);
        hr += parseInt(st.homeRuns || 0);
        er += parseInt(st.earnedRuns || 0);
        ks += parseInt(st.strikeOuts || 0);
        bbs += parseInt(st.baseOnBalls || 0);
        hits += parseInt(st.hits || 0);
      }
    }
    
    if (ipTotal === 0) return null;
    
    // Rate stats from TOTALS across both seasons (not cherry-picked from one year)
    // Require 5+ IP combined for rate stats — below that, numbers are noise
    let era = null, whip = null, kPer9 = null, bbPer9 = null, hrPer9 = null;
    if (ipTotal >= 5) {
      era = (er * 9) / ipTotal;
      whip = (bbs + hits) / ipTotal;
      kPer9 = (ks * 9) / ipTotal;
      bbPer9 = (bbs * 9) / ipTotal;
      hrPer9 = (hr * 9) / ipTotal;
    }
    
    let avgIpPerStart = null, avgBfPerStart = null;
    if (gs >= 3) {
      avgIpPerStart = ipTotal / gs;
      avgBfPerStart = bf / gs;
      if (avgIpPerStart < 4.0 || avgIpPerStart > 8.0) avgIpPerStart = null;
      if (avgBfPerStart < 18 || avgBfPerStart > 30) avgBfPerStart = null;
    }
    return { sb, cs, ip: String(ipTotal), gs, gp, bf, hr, er, ks, bbs, hits,
             era, whip, kPer9, bbPer9, hrPer9,
             avgIpPerStart, avgBfPerStart };
  } catch(e) { return null; }
}

async function main() {
  console.log('⚾ EDGE DFS PITCHER LOADER v2 — Pre-computed Savant stats');

  // STEP 1: Bulk fetch pre-computed Savant stats (2 calls for all pitchers)
  const savantVsR = await fetchSavantBulk('pitcher', 'R');
  await sleep(3000);
  const savantVsL = await fetchSavantBulk('pitcher', 'L');
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
      // Pull both combined (2025+2026) and 2026-only pitch-by-pitch data.
      // Combined: stable arsenal effectiveness with bigger sample.
      // 2026-only: current arsenal usage (catches in-season pitch-mix changes).
      const csvCombined = await fetchCSV(p.id, '2025%7C2026');
      const rowsCombined = parseCSV(csvCombined);
      await sleep(300);
      const csv2026 = await fetchCSV(p.id, '2026');
      const rows2026 = parseCSV(csv2026);

      // Pull 2025-only too — needed for arsenal-change detection. We don't
      // strictly NEED a separate fetch; we can derive 2025 by subtracting
      // 2026 from combined, but a clean separate pull is simpler and doesn't
      // require the row-counts to match.
      await sleep(300);
      const csv2025 = await fetchCSV(p.id, '2025');
      const rows2025 = parseCSV(csv2025);

      // Compute arsenals from combined (default fallback).
      let arsenalAll = null, arsenalCombinedVsR = null, arsenalCombinedVsL = null;
      if (rowsCombined && rowsCombined.length >= 100) {
        arsenalAll = computeArsenalByHand(rowsCombined, null);
        arsenalCombinedVsR = computeArsenalByHand(rowsCombined, 'R');
        arsenalCombinedVsL = computeArsenalByHand(rowsCombined, 'L');
      }

      // Compute 2026-only arsenals — these win when the sample is sufficient.
      let arsenal2026VsR = null, arsenal2026VsL = null;
      let pitchCount2026VsR = 0, pitchCount2026VsL = 0;
      if (rows2026 && rows2026.length >= 50) {
        pitchCount2026VsR = rows2026.filter(r => r.stand === 'R').length;
        pitchCount2026VsL = rows2026.filter(r => r.stand === 'L').length;
        arsenal2026VsR = computeArsenalByHand(rows2026, 'R');
        arsenal2026VsL = computeArsenalByHand(rows2026, 'L');
      }

      // Compute 2025-only arsenals — used only for change-detection display.
      let arsenal2025VsR = null, arsenal2025VsL = null;
      if (rows2025 && rows2025.length >= 50) {
        arsenal2025VsR = computeArsenalByHand(rows2025, 'R');
        arsenal2025VsL = computeArsenalByHand(rows2025, 'L');
      }

      // Choose which arsenal to write to the primary `arsenal_vs_*` columns.
      // If a pitcher has thrown 200+ pitches in 2026 vs that hand, use his
      // 2026 arsenal — he's reshaped his approach this year. Otherwise use
      // combined (more stable for relievers / early-season pitchers).
      const ARSENAL_THRESHOLD = 200;
      const arsenalVsR = (pitchCount2026VsR >= ARSENAL_THRESHOLD && arsenal2026VsR)
        ? arsenal2026VsR : arsenalCombinedVsR;
      const arsenalVsL = (pitchCount2026VsL >= ARSENAL_THRESHOLD && arsenal2026VsL)
        ? arsenal2026VsL : arsenalCombinedVsL;
      const arsenalSourceR = (pitchCount2026VsR >= ARSENAL_THRESHOLD && arsenal2026VsR) ? '2026' : 'combined';
      const arsenalSourceL = (pitchCount2026VsL >= ARSENAL_THRESHOLD && arsenal2026VsL) ? '2026' : 'combined';

      const svR = savantVsR[p.id] || {};
      const svL = savantVsL[p.id] || {};

      if (!arsenalAll && !svR.xwoba && !svL.xwoba) {
        console.log((i+1) + '/' + pitchers.length, p.name, '— skipped (no data)');
        continue;
      }

      // Rate stats now come from fetchPitcherSeasonStats — combined 2025+2026 totals
      // (Old code took whichever single season had >=10 IP, which ignored the other year's data
      // AND left relievers with <10 IP in either season with null rates.)
      const ext = await fetchPitcherSeasonStats(p.id);

      await sbUpsert('edge_pitcher_cache', {
        pitcher_id: p.id, pitcher_name: p.name, team: p.team, hand: p.hand, season: 2025,
        arsenal: arsenalAll, arsenal_vs_r: arsenalVsR, arsenal_vs_l: arsenalVsL,
        // Per-season arsenals — used by frontend for arsenal-change detection
        // ("McCullers cutter usage 8% → 43% vs LHB" insights).
        arsenal_2025_vs_r: arsenal2025VsR,
        arsenal_2025_vs_l: arsenal2025VsL,
        arsenal_2026_vs_r: arsenal2026VsR,
        arsenal_2026_vs_l: arsenal2026VsL,
        era: ext?.era != null ? r3(ext.era) : null,
        whip: ext?.whip != null ? r3(ext.whip) : null,
        k_per_9: ext?.kPer9 != null ? r1(ext.kPer9) : null,
        bb_per_9: ext?.bbPer9 != null ? r1(ext.bbPer9) : null,
        ip: ext?.ip || null,
        hr_per_9: ext?.hrPer9 != null ? r1(ext.hrPer9) : null,
        // Pre-computed from Savant bulk (NOT calculated)
        xwoba_vs_r: svR.xwoba || null,
        xwoba_vs_l: svL.xwoba || null,
        xslg_allowed_r: svR.xslg || null,
        xslg_allowed_l: svL.xslg || null,
        barrel_pct_allowed_r: svR.barrelPct || null,
        barrel_pct_allowed_l: svL.barrelPct || null,
        hard_hit_pct_vs_r: svR.hardHitPct || null,
        hard_hit_pct_vs_l: svL.hardHitPct || null,
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
      const arsenalTag = '[arsR:' + arsenalSourceR + ' L:' + arsenalSourceL + ']';
      console.log((i+1) + '/' + pitchers.length, p.name, '(' + p.team + ' ' + p.hand + 'HP) ✓', arsenalTag, 'vsR:', splitR, '| vsL:', splitL);

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
