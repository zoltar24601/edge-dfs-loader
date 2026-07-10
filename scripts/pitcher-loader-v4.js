// ============================================================
// EDGE DFS PITCHER LOADER v4 - INCREMENTAL
//
// Replaces the ~66-minute per-pitcher Savant crawl (3 pitch-by-pitch
// CSV pulls per pitcher) with reads from edge_statcast_pitch_daily.
// Flow:
//   1. Ingest new dates into edge_statcast_pitch_daily (bulk, 1
//      Savant call per missing date - self-healing if a day was missed)
//   2. Bulk Savant season stats vs RHB / vs LHB (2 calls, unchanged)
//   3. Active rosters (MLB statsapi, unchanged)
//   4. Per pitcher: read daily rows from edge_statcast_pitch_daily
//      and compute arsenals (combined / 2026 / 2025, vs L / vs R) -
//      identical output shape to pitcher-loader.js - plus statsapi
//      season stats incl. last-8-starts avg_ip_per_start, then
//      upsert edge_pitcher_cache (conflict pitcher_id,season).
//
// Env: SUPABASE_URL, SUPABASE_KEY
// ============================================================
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MLB = 'https://statsapi.mlb.com/api/v1';
let saved = 0, errors = 0, skipped = 0;

if (!SUPABASE_URL || !SUPABASE_KEY) { console.error('Missing SUPABASE_URL / SUPABASE_KEY'); process.exit(1); }

function r1(v){return Math.round(v*10)/10}
function r3(v){return Math.round(v*1000)/1000}
function sleep(ms){return new Promise(r=>setTimeout(r,ms))}

const LABELS = {FF:'4-Seam FB',SI:'Sinker',FC:'Cutter',FA:'Fastball',SL:'Slider',CU:'Curveball',KC:'Knuckle-Curve',CS:'Slow Curve',CH:'Changeup',FS:'Splitter',ST:'Sweeper',SV:'Slurve',SW:'Sweeper'};

async function sbUpsert(table, data, conflictCols) {
  const conflict = conflictCols ? '?on_conflict=' + conflictCols : '';
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table + conflict, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Prefer': 'resolution=merge-duplicates' },
    body: JSON.stringify(data)
  });
  if (!res.ok) { const t = await res.text(); throw new Error(t.substring(0, 200)); }
}

// Paged select (PostgREST caps ~1000 rows per request; a 2-season
// starter can exceed that across stand x pitch_type x date rows)
async function sbSelectPitcherRows(pitcherId) {
  const out = [];
  const PAGE = 1000;
  for (let from = 0; ; from += PAGE) {
    const res = await fetch(SUPABASE_URL + '/rest/v1/edge_statcast_pitch_daily?pitcher_id=eq.' + pitcherId + '&select=*', {
      headers: { 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Range': from + '-' + (from + PAGE - 1) }
    });
    if (!res.ok) { const t = await res.text(); throw new Error('select ' + res.status + ': ' + t.substring(0, 120)); }
    const page = await res.json();
    out.push(...page);
    if (page.length < PAGE) break;
  }
  return out;
}

// ---------------- STEP 1: ingest missing dates ----------------
// Aggregation identical to pitcher-statcast-backfill.js.
const PA_EVENTS = ['single','double','triple','home_run','field_out','strikeout','walk','hit_by_pitch','grounded_into_double_play','sac_fly','force_out','fielders_choice','fielders_choice_out'];
const SWING_DESC = ['swinging_strike','swinging_strike_blocked','foul','hit_into_play','foul_tip'];
const WHIFF_DESC = ['swinging_strike','swinging_strike_blocked'];
const CONTACT_DESC = ['foul','hit_into_play','foul_tip'];
const CHASE_DESC = ['swinging_strike','swinging_strike_blocked','foul','hit_into_play'];
const OZ_ZONES = ['11','12','13','14'];

function parseCSV(text) {
  if (!text || text.length < 100) return null;
  const lines = text.trim().split('\n');
  if (lines.length < 2) return null;
  const headerLine = lines[0].charCodeAt(0) === 0xFEFF ? lines[0].slice(1) : lines[0];
  const hdrs = headerLine.split(',').map(h => h.trim().replace(/"/g, ''));
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = [];
    let cur = '', inQ = false;
    for (let j = 0; j < lines[i].length; j++) {
      const c = lines[i][j];
      if (c === '"') inQ = !inQ;
      else if (c === ',' && !inQ) { vals.push(cur); cur = ''; }
      else cur += c;
    }
    vals.push(cur);
    if (vals.length < hdrs.length / 2) continue;
    const row = {};
    hdrs.forEach((h, idx) => { row[h] = vals[idx] ? vals[idx].trim().replace(/"/g, '') : ''; });
    rows.push(row);
  }
  return rows;
}

async function fetchDayCSV(dateStr) {
  const url = 'https://baseballsavant.mlb.com/statcast_search/csv' +
    '?all=true&type=details&player_type=pitcher&hfGT=R%7C' +
    '&game_date_gt=' + dateStr + '&game_date_lt=' + dateStr +
    '&min_pitches=0&min_results=0&min_pas=0';
  for (let att = 1; att <= 3; att++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      const text = await r.text();
      if (text && !text.includes('<html')) return text;
      if (att < 3) await sleep(4000 * att);
    } catch (e) { if (att === 3) throw e; await sleep(4000 * att); }
  }
  return null;
}

function newAgg(pid, name, dateStr, stand, pt) {
  return {
    pitcher_id: pid, pitcher_name: name, game_date: dateStr, stand: stand, pitch_type: pt,
    pitches: 0, velo_sum: 0, velo_n: 0,
    pa_events: 0, walks: 0, strikeouts: 0,
    singles: 0, doubles: 0, triples: 0, home_runs: 0, hbp: 0,
    bip: 0, bip_typed: 0, fb_bip: 0, bip_ev: 0, ev_sum: 0, hard_hits: 0, barrels: 0, liners: 0,
    la_sum: 0, la_n: 0,
    swings: 0, whiffs: 0, contacts: 0,
    oz_pitches: 0, chases: 0, zone_pitches: 0, zone_swings: 0,
    xwoba_sum: 0, xwoba_n: 0, xba_sum: 0, xba_n: 0,
  };
}

function aggregateDay(rows, dateStr) {
  const aggs = {};
  for (const r of rows) {
    const pid = parseInt(r.pitcher);
    if (!pid || isNaN(pid)) continue;
    const stand = r.stand === 'L' ? 'L' : 'R';
    const pt = (r.pitch_type && r.pitch_type !== 'null') ? r.pitch_type : 'UNK';
    const key = pid + '|' + stand + '|' + pt;
    if (!aggs[key]) aggs[key] = newAgg(pid, r.player_name || '', dateStr, stand, pt);
    const a = aggs[key];
    a.pitches++;
    const vel = parseFloat(r.release_speed);
    if (!isNaN(vel)) { a.velo_sum += vel; a.velo_n++; }
    const ev = r.events;
    if (ev && PA_EVENTS.includes(ev)) {
      a.pa_events++;
      if (ev === 'walk') a.walks++;
      if (ev === 'strikeout') a.strikeouts++;
      if (ev === 'single') a.singles++;
      if (ev === 'double') a.doubles++;
      if (ev === 'triple') a.triples++;
      if (ev === 'home_run') a.home_runs++;
      if (ev === 'hit_by_pitch') a.hbp++;
    }
    const desc = r.description || '';
    const isSwing = SWING_DESC.includes(desc);
    if (isSwing) a.swings++;
    if (WHIFF_DESC.includes(desc)) a.whiffs++;
    if (CONTACT_DESC.includes(desc)) a.contacts++;
    const zone = r.zone || '';
    if (OZ_ZONES.includes(zone)) {
      a.oz_pitches++;
      if (CHASE_DESC.includes(desc)) a.chases++;
    } else if (zone !== '' && zone !== 'null') {
      a.zone_pitches++;
      if (isSwing) a.zone_swings++;
    }
    if (r.type === 'X') {
      a.bip++;
      const bt = r.bb_type || '';
      if (bt && bt !== 'null') {
        a.bip_typed++;
        if (bt === 'fly_ball' || bt === 'popup') a.fb_bip++;
      }
      const ls = parseFloat(r.launch_speed);
      if (!isNaN(ls)) {
        a.bip_ev++;
        a.ev_sum += ls;
        if (ls >= 95) a.hard_hits++;
        const la = parseFloat(r.launch_angle);
        if (!isNaN(la)) {
          a.la_sum += la;
          a.la_n++;
          if (ls >= 98 && la >= 26 && la <= 30) a.barrels++;
          if (la >= 10 && la <= 25) a.liners++;
        }
      }
    }
    const xw = parseFloat(r.estimated_woba_using_speedangle);
    if (!isNaN(xw)) { a.xwoba_sum += xw; a.xwoba_n++; }
    const xb = parseFloat(r.estimated_ba_using_speedangle);
    if (!isNaN(xb)) { a.xba_sum += xb; a.xba_n++; }
  }
  return Object.values(aggs).map(a => ({
    ...a,
    velo_sum: Math.round(a.velo_sum * 10) / 10,
    ev_sum: Math.round(a.ev_sum * 10) / 10,
    la_sum: Math.round(a.la_sum * 10) / 10,
    xwoba_sum: Math.round(a.xwoba_sum * 1000) / 1000,
    xba_sum: Math.round(a.xba_sum * 1000) / 1000,
  }));
}

async function upsertDailyBatch(rows) {
  const BATCH = 500;
  for (let i = 0; i < rows.length; i += BATCH) {
    const chunk = rows.slice(i, i + BATCH);
    const res = await fetch(SUPABASE_URL + '/rest/v1/edge_statcast_pitch_daily?on_conflict=pitcher_id,game_date,stand,pitch_type', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Prefer': 'resolution=merge-duplicates' },
      body: JSON.stringify(chunk),
    });
    if (!res.ok) { const t = await res.text(); throw new Error('daily upsert ' + res.status + ': ' + t.substring(0, 200)); }
  }
}

async function ingestNewDates() {
  // Find the last stored date, then ingest from that date (re-ingest it, in
  // case games finished after the last run) through yesterday. Self-healing:
  // if a run is missed, the gap fills automatically on the next run.
  const res = await fetch(SUPABASE_URL + '/rest/v1/edge_statcast_pitch_daily?select=game_date&order=game_date.desc&limit=1', {
    headers: { 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY }
  });
  const rows = await res.json();
  if (!rows.length) { console.log('edge_statcast_pitch_daily is empty - run the pitcher backfill first.'); return; }
  const lastStored = rows[0].game_date;

  const yesterday = new Date(Date.now() - 24*3600*1000).toISOString().split('T')[0];
  const d = new Date(lastStored + 'T12:00:00Z');
  const end = new Date(yesterday + 'T12:00:00Z');
  let ingested = 0;
  while (d <= end) {
    const dateStr = d.toISOString().split('T')[0];
    try {
      const csv = await fetchDayCSV(dateStr);
      const parsed = csv ? parseCSV(csv) : null;
      if (parsed && parsed.length >= 10) {
        const aggs = aggregateDay(parsed, dateStr);
        await upsertDailyBatch(aggs);
        console.log('ingest', dateStr, '-', parsed.length, 'pitches ->', aggs.length, 'rows');
        ingested++;
      } else {
        console.log('ingest', dateStr, '- no games / no data');
      }
    } catch (e) {
      console.log('ingest', dateStr, '- FAILED:', (e.message || String(e)).substring(0, 100));
    }
    d.setUTCDate(d.getUTCDate() + 1);
    await sleep(2500);
  }
  console.log('Ingest complete. Dates with data:', ingested);
}

// ============================================================
//  BULK SAVANT STATS - pre-computed xSLG, xwOBA, barrel% by hand
//  (verbatim from pitcher-loader.js; 2 API calls for all pitchers)
// ============================================================

async function fetchSavantBulk(playerType, batterHand) {
  // NOTE: Do NOT include type=details - that returns pitch-by-pitch instead of aggregated.
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

      const rows = parseCSV(text) || [];
      const results = {};
      for (const row of rows) {
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
//  ARSENALS - from edge_statcast_pitch_daily rows
//  Gate semantics mirror pitcher-loader.js computeArsenalByHand:
//  raw pitch counts include PO/IN/UNK; usage denominators exclude them.
// ============================================================

function rawPitchCount(rows, stand) {
  let n = 0;
  for (const r of rows) if (!stand || r.stand === stand) n += (r.pitches || 0);
  return n;
}

function computeArsenalFromRows(rows, stand) {
  const filtered = stand ? rows.filter(r => r.stand === stand) : rows;
  let raw = 0;
  for (const r of filtered) raw += (r.pitches || 0);
  if (raw < 50) return null;
  const byType = {};
  for (const r of filtered) {
    const pt = r.pitch_type;
    if (!pt || pt === 'PO' || pt === 'IN' || pt === 'UNK') continue;
    if (!byType[pt]) byType[pt] = { count: 0, velo_sum: 0, velo_n: 0 };
    byType[pt].count += (r.pitches || 0);
    byType[pt].velo_sum += (r.velo_sum || 0);
    byType[pt].velo_n += (r.velo_n || 0);
  }
  const total = Object.values(byType).reduce((s, t) => s + t.count, 0);
  if (total < 50) return null;
  const result = {};
  Object.entries(byType).forEach(([pt, d]) => {
    if (d.count / total < 0.02) return;
    result[pt] = {
      label: LABELS[pt] || pt,
      usage: Math.round(d.count / total * 1000) / 1000,
      velo: d.velo_n ? Math.round(d.velo_sum / d.velo_n * 10) / 10 : null,
    };
  });
  return Object.keys(result).length ? result : null;
}

// ============================================================
//  MLB STATSAPI SEASON STATS (verbatim from pitcher-loader.js,
//  incl. last-8-starts avg_ip_per_start)
// ============================================================

// Average IP over the pitcher's most recent starts, relief outings excluded.
// Season totals lump relief innings into the IP/GS division, which inflates
// avg_ip_per_start for swingmen (e.g. Manaea); game logs count starts only.
async function fetchRecentStartIP(pitcherId) {
  const MAX_STARTS = 8, MIN_STARTS = 3;
  try {
    const startIPs = [];
    for (const yr of [2026, 2025]) {
      if (startIPs.length >= MAX_STARTS) break;
      const r = await fetch(MLB + '/people/' + pitcherId + '/stats?stats=gameLog&season=' + yr + '&group=pitching');
      const d = await r.json();
      const splits = d.stats?.[0]?.splits || [];
      const starts = splits
        .filter(s => parseInt(s.stat?.gamesStarted || 0) >= 1)
        .sort((a, b) => (b.date || '').localeCompare(a.date || ''));
      for (const s of starts) {
        if (startIPs.length >= MAX_STARTS) break;
        // inningsPitched is baseball notation: "6.1" = 6 innings + 1 out
        const raw = parseFloat(s.stat.inningsPitched || 0);
        const outs = Math.round((raw - Math.floor(raw)) * 10);
        startIPs.push(Math.floor(raw) + outs / 3);
      }
    }
    if (startIPs.length < MIN_STARTS) return null;
    return startIPs.reduce((a, b) => a + b, 0) / startIPs.length;
  } catch(e) { return null; }
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
    // Require 5+ IP combined for rate stats - below that, numbers are noise
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
      // Primary: average of the last 8 actual starts from game logs.
      avgIpPerStart = await fetchRecentStartIP(pitcherId);
      if (avgIpPerStart == null) {
        // Fallback: season-total estimate, with ~1.3 IP per relief outing
        // (gp - gs) removed so relief innings don't inflate the average.
        // If relief outings outnumber starts the estimate is too polluted; keep null.
        const relief = gp - gs;
        if (relief <= gs) avgIpPerStart = (ipTotal - 1.3 * relief) / gs;
      }
      avgBfPerStart = bf / gs;
      if (avgIpPerStart != null && (avgIpPerStart < 4.0 || avgIpPerStart > 8.0)) avgIpPerStart = null;
      if (avgBfPerStart < 18 || avgBfPerStart > 30) avgBfPerStart = null;
    }
    return { sb, cs, ip: String(ipTotal), gs, gp, bf, hr, er, ks, bbs, hits,
             era, whip, kPer9, bbPer9, hrPer9,
             avgIpPerStart, avgBfPerStart };
  } catch(e) { return null; }
}

// ============================================================
//  MAIN
// ============================================================

async function main() {
  console.log('EDGE DFS PITCHER LOADER v4 - incremental (edge_statcast_pitch_daily)');

  // STEP 1: ingest any dates not yet stored
  await ingestNewDates();

  // STEP 2: bulk fetch pre-computed Savant stats (2 calls for all pitchers)
  const savantVsR = await fetchSavantBulk('pitcher', 'R');
  await sleep(3000);
  const savantVsL = await fetchSavantBulk('pitcher', 'L');
  await sleep(3000);

  // STEP 3: get all pitcher rosters
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

  // STEP 4: per pitcher, compute arsenals from the daily table and merge
  for (let i = 0; i < pitchers.length; i++) {
    const p = pitchers[i];
    try {
      const rowsAll = await sbSelectPitcherRows(p.id);
      const rows2026 = rowsAll.filter(r => String(r.game_date).startsWith('2026'));
      const rows2025 = rowsAll.filter(r => String(r.game_date).startsWith('2025'));

      // Combined (2025+2026) arsenals - default fallback.
      // Gate mirrors old rowsCombined.length >= 100 (raw pitch count).
      let arsenalAll = null, arsenalCombinedVsR = null, arsenalCombinedVsL = null;
      if (rawPitchCount(rowsAll, null) >= 100) {
        arsenalAll = computeArsenalFromRows(rowsAll, null);
        arsenalCombinedVsR = computeArsenalFromRows(rowsAll, 'R');
        arsenalCombinedVsL = computeArsenalFromRows(rowsAll, 'L');
      }

      // 2026-only arsenals - these win when the sample is sufficient.
      let arsenal2026VsR = null, arsenal2026VsL = null;
      let pitchCount2026VsR = 0, pitchCount2026VsL = 0;
      if (rawPitchCount(rows2026, null) >= 50) {
        pitchCount2026VsR = rawPitchCount(rows2026, 'R');
        pitchCount2026VsL = rawPitchCount(rows2026, 'L');
        arsenal2026VsR = computeArsenalFromRows(rows2026, 'R');
        arsenal2026VsL = computeArsenalFromRows(rows2026, 'L');
      }

      // 2025-only arsenals - used only for change-detection display.
      let arsenal2025VsR = null, arsenal2025VsL = null;
      if (rawPitchCount(rows2025, null) >= 50) {
        arsenal2025VsR = computeArsenalFromRows(rows2025, 'R');
        arsenal2025VsL = computeArsenalFromRows(rows2025, 'L');
      }

      // Choose which arsenal to write to the primary arsenal_vs_* columns.
      // If a pitcher has thrown 200+ pitches in 2026 vs that hand, use his
      // 2026 arsenal - he's reshaped his approach this year. Otherwise use
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
        skipped++;
        console.log((i+1) + '/' + pitchers.length, p.name, '- skipped (no data)');
        continue;
      }

      // Rate stats from statsapi - combined 2025+2026 totals
      const ext = await fetchPitcherSeasonStats(p.id);

      await sbUpsert('edge_pitcher_cache', {
        pitcher_id: p.id, pitcher_name: p.name, team: p.team, hand: p.hand, season: 2025,
        arsenal: arsenalAll, arsenal_vs_r: arsenalVsR, arsenal_vs_l: arsenalVsL,
        // Per-season arsenals - used by frontend for arsenal-change detection
        // ("McCullers cutter usage 8% -> 43% vs LHB" insights).
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
      console.log((i+1) + '/' + pitchers.length, p.name, '(' + p.team + ' ' + p.hand + 'HP) OK', arsenalTag, 'vsR:', splitR, '| vsL:', splitL);

      if (i % 10 === 9) await sleep(200);

    } catch(e) {
      errors++;
      console.error((i+1) + '/' + pitchers.length, p.name, 'X ERROR:', (e.message || String(e)).substring(0, 80));
      await sleep(1000);
    }
  }

  console.log('========================================');
  console.log('PITCHER LOADER v4 DONE! Saved:', saved, '| Errors:', errors, '| Skipped:', skipped);
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
