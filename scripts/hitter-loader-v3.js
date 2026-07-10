// ============================================================
// EDGE DFS HITTER LOADER v3 - INCREMENTAL
//
// Replaces the 90-minute per-player Savant crawl. Flow:
//   1. Ingest new dates into edge_statcast_daily (bulk, 1 Savant
//      call per missing date - self-healing if a day was missed)
//   2. Bulk Savant season stats vs R / vs L (2 calls, same as old)
//   3. Schedule + active rosters (MLB statsapi)
//   4. Per player: read daily rows from edge_statcast_daily and
//      compute splits, L7/L14/L28/season windows, hot score,
//      flags - identical output shape to the old loader - then
//      upsert edge_matchup_cache (vsR + vsL) and edge_hot_history.
//
// Env: SUPABASE_URL, SUPABASE_KEY
// ============================================================
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MLB = 'https://statsapi.mlb.com/api/v1';
const today = new Date().toISOString().split('T')[0];
let saved = 0, errors = 0, skipped = 0;

if (!SUPABASE_URL || !SUPABASE_KEY) { console.error('Missing SUPABASE_URL / SUPABASE_KEY'); process.exit(1); }

// ---------------- small utils ----------------
function avg(a){ return a.reduce((x,y)=>x+y,0)/a.length; }
function r1(v){ return Math.round(v*10)/10; }
function r3(v){ return Math.round(v*1000)/1000; }
function clamp(v,lo,hi){ return Math.max(lo,Math.min(hi,v)); }
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }

async function sbUpsert(table, data, conflictCols) {
  const conflict = conflictCols ? '?on_conflict=' + conflictCols : '';
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table + conflict, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Prefer': 'resolution=merge-duplicates' },
    body: JSON.stringify(data)
  });
  if (!res.ok) { const t = await res.text(); throw new Error(t.substring(0, 200)); }
}

// Paged select (PostgREST caps ~1000 rows per request; a 2-season player
// can have ~1500 daily rows)
async function sbSelectPlayerRows(playerId) {
  const out = [];
  const PAGE = 1000;
  for (let from = 0; ; from += PAGE) {
    const res = await fetch(SUPABASE_URL + '/rest/v1/edge_statcast_daily?player_id=eq.' + playerId + '&select=*', {
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
    '?all=true&type=details&player_type=batter&hfGT=R%7C' +
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

function newAgg(pid, name, dateStr, hand, pt) {
  return {
    player_id: pid, player_name: name, game_date: dateStr, p_throws: hand, pitch_type: pt,
    pitches: 0, pa_events: 0, walks: 0, strikeouts: 0,
    bip: 0, bip_ev: 0, ev_sum: 0, hard_hits: 0, barrels: 0, liners: 0,
    swings: 0, whiffs: 0, contacts: 0,
    xwoba_sum: 0, xwoba_n: 0, xba_sum: 0, xba_n: 0,
    oz_pitches: 0, chases: 0, zone_pitches: 0, zone_swings: 0,
    singles: 0, doubles: 0, triples: 0, home_runs: 0, hbp: 0,
    la_sum: 0, la_n: 0, fb_bip: 0, bip_typed: 0,
  };
}

function aggregateDay(rows, dateStr) {
  const aggs = {};
  for (const r of rows) {
    const pid = parseInt(r.batter);
    if (!pid || isNaN(pid)) continue;
    const hand = r.p_throws === 'L' ? 'L' : 'R';
    const pt = (r.pitch_type && r.pitch_type !== 'null') ? r.pitch_type : 'UNK';
    const key = pid + '|' + hand + '|' + pt;
    if (!aggs[key]) aggs[key] = newAgg(pid, r.player_name || '', dateStr, hand, pt);
    const a = aggs[key];
    a.pitches++;
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
    const res = await fetch(SUPABASE_URL + '/rest/v1/edge_statcast_daily?on_conflict=player_id,game_date,p_throws,pitch_type', {
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
  const res = await fetch(SUPABASE_URL + '/rest/v1/edge_statcast_daily?select=game_date&order=game_date.desc&limit=1', {
    headers: { 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY }
  });
  const rows = await res.json();
  if (!rows.length) { console.log('edge_statcast_daily is empty - run the backfill first.'); return; }
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

// ---------------- window + split math from daily rows ----------------
function sumRows(rows) {
  const t = { pa_events:0, walks:0, strikeouts:0, bip:0, bip_ev:0, ev_sum:0, hard_hits:0,
    barrels:0, liners:0, swings:0, whiffs:0, contacts:0, xwoba_sum:0, xwoba_n:0, xba_sum:0,
    xba_n:0, oz_pitches:0, chases:0, zone_pitches:0, zone_swings:0, singles:0, doubles:0,
    triples:0, home_runs:0, hbp:0, la_sum:0, la_n:0, fb_bip:0, bip_typed:0, pitches:0 };
  for (const r of rows) for (const k of Object.keys(t)) t[k] += (r[k] || 0);
  return t;
}

// Mirrors old computeStreak exactly: window relative to the PLAYER's most
// recent game date; nPA<5 early exit; same fallbacks and rounding.
function computeStreak(rows, days) {
  if (!rows || !rows.length) return null;
  let working;
  if (days === null || days === undefined) {
    working = rows;
  } else {
    const dates = [...new Set(rows.map(r => r.game_date))].sort().reverse();
    const lastDate = dates[0] ? new Date(dates[0]) : new Date();
    const cutoff = new Date(lastDate); cutoff.setDate(cutoff.getDate() - days);
    working = rows.filter(r => new Date(r.game_date) >= cutoff);
  }
  if (!working.length) return { nPA: 0 };
  const t = sumRows(working);
  const nPA = t.pa_events;
  if (nPA < 5) return { nPA };

  let wobaNum = t.walks*0.69 + t.hbp*0.72 + t.singles*0.89 + t.doubles*1.27 + t.triples*1.62 + t.home_runs*2.10;
  const woba = nPA > 0 ? wobaNum / nPA : null;

  return {
    nPA,
    bbPct: r1(t.walks / nPA * 100),
    kPct:  r1(t.strikeouts / nPA * 100),
    hardHitPct: r1(t.bip_ev ? t.hard_hits / t.bip_ev * 100 : 0),
    barrelPct:  r1(t.bip_ev ? t.barrels / t.bip_ev * 100 : 0),
    ldPct:      r1(t.bip_ev ? t.liners / t.bip_ev * 100 : 0),
    avgEV:      r1(t.bip_ev ? t.ev_sum / t.bip_ev : 0),
    avgLA:      r1(t.la_n ? t.la_sum / t.la_n : 0),
    fbPct:      r1(t.bip_typed ? t.fb_bip / t.bip_typed * 100 : 0),
    xwoba:      t.xwoba_n ? r3(t.xwoba_sum / t.xwoba_n) : null,
    xba:        t.xba_n ? r3(t.xba_sum / t.xba_n) : null,
    contactPct: r1(t.swings ? t.contacts / t.swings * 100 : 0),
    chasePct:   t.oz_pitches ? r1(t.chases / t.oz_pitches * 100) : null,
    whiffPct:   r1(t.swings ? t.whiffs / t.swings * 100 : 0),
    woba:         woba != null ? r3(woba) : null,
    zoneSwingPct: t.zone_pitches ? r1(t.zone_swings / t.zone_pitches * 100) : null,
    bipEV: t.bip_ev, nIZ: t.zone_pitches, nOZ: t.oz_pitches,
  };
}

const PITCH_LABELS = { FF:'4-Seam FB', SI:'Sinker', FC:'Cutter', FA:'Fastball', SL:'Slider', CU:'Curveball', KC:'Knuckle-Curve', CS:'Slow Curve', CH:'Changeup', FS:'Splitter', ST:'Sweeper', SV:'Slurve' };

function computeSplits(rows, hand) {
  if (!rows || !rows.length) return {};
  const f = hand ? rows.filter(r => r.p_throws === hand) : rows;
  if (!f.length) return {};
  const byP = {};
  for (const r of f) {
    const pt = r.pitch_type;
    if (!pt || pt === 'null' || pt === 'UNK') continue;
    if (!byP[pt]) byP[pt] = [];
    byP[pt].push(r);
  }
  const res = {};
  for (const [pt, group] of Object.entries(byP)) {
    const t = sumRows(group);
    if (t.pitches < 10) continue;
    res[pt] = {
      label: PITCH_LABELS[pt] || pt, n: t.pitches,
      woba: t.xwoba_n ? r3(t.xwoba_sum / t.xwoba_n) : null,
      xba:  t.xba_n ? r3(t.xba_sum / t.xba_n) : null,
      hardHitPct: t.bip_ev ? r1(t.hard_hits / t.bip_ev * 100) : 0,
      ldPct:      t.bip_ev ? r1(t.liners / t.bip_ev * 100) : 0,
      whiffPct:   t.swings ? r1(t.whiffs / t.swings * 100) : 0,
      avgEV:      t.bip_ev ? r1(t.ev_sum / t.bip_ev) : 0,
    };
  }
  return res;
}

function computeSeasonKPct(rows, hand) {
  if (!rows || !rows.length) return null;
  const f = hand ? rows.filter(r => r.p_throws === hand) : rows;
  if (!f.length) return null;
  const t = sumRows(f);
  if (t.pa_events < 20) return null;
  return { kPct: r1(t.strikeouts / t.pa_events * 100), nPA: t.pa_events };
}

// ---------------- flags + hot score (verbatim logic from old loader) ----------------
function evaluateFlag(l7, l14, season) {
  const out = {
    is_emerging: false, emerging_tier: 0, emerging_signals: [],
    is_cooling:  false, cooling_tier:  0, cooling_signals:  [],
    is_accelerating: false,
  };
  if (!l7 || !season || (season.nPA || 0) < 150) return out;
  const signalsUp = [], signalsDown = [];
  if ((l7.nIZ || 0) + (l7.nOZ || 0) >= 40 && l7.chasePct != null && season.chasePct != null) {
    const delta = season.chasePct - l7.chasePct;
    if (delta >= 4.0) signalsUp.push('discipline');
    if (delta <= -4.0) signalsDown.push('discipline');
  }
  if ((l7.bipEV || 0) >= 10 && l7.avgEV != null && season.avgEV != null) {
    const delta = l7.avgEV - season.avgEV;
    if (delta >= 1.5) signalsUp.push('exit_velo');
    if (delta <= -1.5) signalsDown.push('exit_velo');
  }
  if ((l7.bipEV || 0) >= 10 && l7.barrelPct != null && season.barrelPct != null) {
    const delta = l7.barrelPct - season.barrelPct;
    if (delta >= 4.0) signalsUp.push('barrels');
    if (delta <= -4.0) signalsDown.push('barrels');
  }
  if (l14 && (l14.nPA || 0) >= 40 && l14.xwoba != null && l14.woba != null) {
    const gap = l14.xwoba - l14.woba;
    if (gap >= 0.050) signalsUp.push('hidden_heat');
    if (gap <= -0.050) signalsDown.push('hidden_heat');
  }
  if (signalsUp.length >= 2) {
    out.is_emerging = true;
    out.emerging_tier = Math.min(3, signalsUp.length);
    out.emerging_signals = signalsUp;
  }
  if (signalsDown.length >= 2) {
    out.is_cooling = true;
    out.cooling_tier = Math.min(3, signalsDown.length);
    out.cooling_signals = signalsDown;
  }
  if (l7.avgEV != null && l14 && l14.avgEV != null && season.avgEV != null) {
    if (l7.avgEV > l14.avgEV && l14.avgEV > season.avgEV && (l7.avgEV - season.avgEV) >= 1.0) {
      out.is_accelerating = true;
    }
  }
  return out;
}

function calcHot(s) {
  if (!s || (s.nPA || 0) < 5) return { score: 50, grade: 'C', trend: 'NEUTRAL' };
  const hh = s.hardHitPct ?? 38, xw = s.xwoba ?? 0.320, bb = s.bbPct ?? 8.5, ld = s.ldPct ?? 21, k = s.kPct ?? 23;
  const sc = Math.round(clamp((hh-25)/32*100,0,100)*0.35 + clamp((xw-0.270)/0.155*100,0,100)*0.25 + clamp((bb-5)/14*100,0,100)*0.20 + clamp((ld-14)/22*100,0,100)*0.10 + clamp((35-k)/20*100,0,100)*0.10);
  return { score: sc, grade: sc>=80?'A+':sc>=70?'A':sc>=60?'B+':sc>=50?'B':sc>=40?'C+':sc>=30?'C':'D',
    trend: sc>=68?'HOT':sc>=55?'WARM':sc>=42?'NEUTRAL':'COLD' };
}

// ---------------- MLB statsapi bits (unchanged from old loader) ----------------
async function fetchPlayerSBData(playerId) {
  try {
    let sb = 0, cs = 0, pa = 0, hr = 0, ab = 0;
    for (const yr of [2026, 2025]) {
      const r = await fetch(MLB + '/people/' + playerId + '/stats?stats=season&season=' + yr + '&group=hitting');
      const d = await r.json();
      const s = d.stats?.[0]?.splits?.[0]?.stat;
      if (s) {
        sb += parseInt(s.stolenBases || 0);
        cs += parseInt(s.caughtStealing || 0);
        pa += parseInt(s.plateAppearances || 0);
        hr += parseInt(s.homeRuns || 0);
        ab += parseInt(s.atBats || 0);
      }
    }
    return { sb, cs, pa, hr, ab, attempts: sb + cs, successRate: (sb + cs) > 0 ? sb / (sb + cs) : null, hrPerPA: pa > 0 ? r3(hr / pa) : null };
  } catch (e) {
    return { sb: 0, cs: 0, pa: 0, hr: 0, ab: 0, attempts: 0, successRate: null, hrPerPA: null };
  }
}

async function fetchSavantBulkHitter(pitcherHand) {
  const url = 'https://baseballsavant.mlb.com/statcast_search/csv' +
    '?hfGT=R%7C&hfSea=2026%7C2025%7C&player_type=batter' +
    '&pitcher_throws=' + pitcherHand +
    '&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&sort_order=desc';
  console.log('Fetching Savant bulk: hitters vs', pitcherHand + 'HP...');
  for (let att = 0; att < 3; att++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!r.ok) { console.warn('Savant returned', r.status); await sleep(5000); continue; }
      const text = await r.text();
      if (!text || text.length < 200 || text.includes('<html')) { console.warn('Savant empty/html'); await sleep(5000); continue; }
      const rows = parseCSV(text) || [];
      const results = {};
      for (const row of rows) {
        const pid = parseInt(row.player_id);
        if (!pid) continue;
        const pf = (f) => { const v = parseFloat(row[f]); return !isNaN(v) ? v : null; };
        const pi = (f) => { const v = parseInt(row[f]); return !isNaN(v) ? v : null; };
        results[pid] = {
          xslg: pf('xslg') != null ? r3(pf('xslg')) : null,
          xba: pf('xba') != null ? r3(pf('xba')) : null,
          xwoba: pf('xwoba') != null ? r3(pf('xwoba')) : null,
          xobp: pf('xobp') != null ? r3(pf('xobp')) : null,
          barrelPct: pf('barrels_per_bbe_percent') != null ? r1(pf('barrels_per_bbe_percent')) : null,
          hardHitPct: pf('hardhit_percent') != null ? r1(pf('hardhit_percent')) : null,
          kPct: pf('k_percent') != null ? r1(pf('k_percent')) : null,
          bbPct: pf('bb_percent') != null ? r1(pf('bb_percent')) : null,
          pa: pi('pa'), hrs: pi('hrs'), singles: pi('singles'), doubles: pi('doubles'), triples: pi('triples'),
        };
      }
      console.log('Got', Object.keys(results).length, 'hitters vs', pitcherHand + 'HP');
      return results;
    } catch (e) {
      console.warn('Savant bulk hitter error:', e.message);
      await sleep(5000 * (att + 1));
    }
  }
  console.warn('Failed to fetch Savant bulk hitter data vs', pitcherHand);
  return {};
}

// ---------------- main ----------------
async function main() {
  console.log('EDGE DFS HITTER LOADER v3 - incremental (edge_statcast_daily)');
  console.log('Date:', today);

  // STEP 1: ingest any dates not yet stored
  await ingestNewDates();

  // STEP 2: bulk Savant season stats by pitcher hand
  const savantVsR = await fetchSavantBulkHitter('R');
  await sleep(3000);
  const savantVsL = await fetchSavantBulkHitter('L');

  // STEP 3: schedule + rosters
  console.log('Fetching schedule...');
  const schedRes = await fetch(MLB + '/schedule?sportId=1&date=' + today + '&hydrate=probablePitcher,team');
  const schedData = await schedRes.json();
  const games = schedData.dates?.[0]?.games || [];
  console.log('Games:', games.length);
  if (!games.length) { console.log('No games today'); return; }

  console.log('Fetching rosters...');
  const hitters = [];
  const teamIds = new Set();
  games.forEach(g => { teamIds.add(g.teams.away.team.id); teamIds.add(g.teams.home.team.id); });
  for (const tid of teamIds) {
    try {
      const r = await fetch(MLB + '/teams/' + tid + '/roster?rosterType=active&hydrate=person');
      const d = await r.json();
      for (const p of (d.roster || [])) {
        const pos = p.position?.abbreviation || '';
        if (pos === 'P') continue; // keep TWP (Ohtani)
        const displayPos = pos === 'TWP' ? 'DH' : pos;
        const gm = games.find(g => g.teams.away.team.id === tid || g.teams.home.team.id === tid);
        const isAway = gm?.teams.away.team.id === tid;
        const abbr = isAway ? gm?.teams.away.team.abbreviation : gm?.teams.home.team.abbreviation;
        const batSide = p.person?.batSide?.code || 'R';
        hitters.push({ id: p.person.id, name: p.person.fullName, team: abbr || '???', position: displayPos, batSide });
      }
    } catch (e) {}
  }
  console.log('Found', hitters.length, 'hitters');

  // STEP 4: per player, compute from the daily table
  for (let i = 0; i < hitters.length; i++) {
    const h = hitters[i];
    try {
      const rowsAll = await sbSelectPlayerRows(h.id);
      const totalPitches = rowsAll.reduce((a, r) => a + (r.pitches || 0), 0);
      if (!rowsAll.length || totalPitches < 20) {
        skipped++;
        console.log((i+1) + '/' + hitters.length, h.name, '- skipped (no data)');
        continue;
      }

      const rows2026 = rowsAll.filter(r => String(r.game_date).startsWith('2026'));
      const pitches2026 = rows2026.reduce((a, r) => a + (r.pitches || 0), 0);

      // Splits + season K% use combined 2025+2026 (same as old loader)
      const splitsR = computeSplits(rowsAll, 'R');
      const splitsL = computeSplits(rowsAll, 'L');
      const seasonKvsR = computeSeasonKPct(rowsAll, 'R');
      const seasonKvsL = computeSeasonKPct(rowsAll, 'L');

      // Windows use 2026 data if the sample is big enough (same rule)
      const windowSource = pitches2026 >= 20 ? rows2026 : rowsAll;
      const windowSrc = pitches2026 >= 20 ? '2026' : 'all';

      const streakL7  = computeStreak(windowSource, 7);
      const streakL14 = computeStreak(windowSource, 14);
      const streakL28 = computeStreak(windowSource, 28);
      const seasonBaseline = computeStreak(rowsAll, null);

      const hot = calcHot(streakL14);
      const flag = evaluateFlag(streakL7, streakL14, seasonBaseline);

      const d = (a, b) => (a != null && b != null) ? r3(a - b) : null;
      const deltas = {
        chase_delta_l7:      d(seasonBaseline?.chasePct, streakL7?.chasePct),
        zone_swing_delta_l7: d(streakL7?.zoneSwingPct, seasonBaseline?.zoneSwingPct),
        whiff_delta_l7:      d(seasonBaseline?.whiffPct, streakL7?.whiffPct),
        ev_delta_l7:         d(streakL7?.avgEV, seasonBaseline?.avgEV),
        barrel_delta_l7:     d(streakL7?.barrelPct, seasonBaseline?.barrelPct),
        xwoba_delta_l7:      d(streakL7?.xwoba, seasonBaseline?.xwoba),
      };

      const sbData = await fetchPlayerSBData(h.id);
      const svR = savantVsR[h.id] || {};
      const svL = savantVsL[h.id] || {};

      const sbFields = {
        sb_count: sbData.sb, cs_count: sbData.cs, sb_attempts: sbData.attempts,
        sb_success_rate: sbData.successRate, season_pa: sbData.pa, season_hr: sbData.hr,
        season_ab: sbData.ab, season_hr_per_pa: sbData.hrPerPA, sprint_speed: null,
      };
      const flagFields = {
        is_emerging: flag.is_emerging, emerging_tier: flag.emerging_tier, emerging_signals: flag.emerging_signals,
        is_cooling: flag.is_cooling, cooling_tier: flag.cooling_tier, cooling_signals: flag.cooling_signals,
        is_accelerating: flag.is_accelerating,
      };
      const l14Fields = {
        n_pa: streakL14?.nPA || 0, hard_hit_pct: streakL14?.hardHitPct || null,
        barrel_pct: streakL14?.barrelPct || null, xwoba: streakL14?.xwoba || null,
        xba: streakL14?.xba || null, bb_pct: streakL14?.bbPct || null, k_pct: streakL14?.kPct || null,
        ld_pct: streakL14?.ldPct || null, avg_ev: streakL14?.avgEV || null,
        contact_pct: streakL14?.contactPct || null, chase_pct: streakL14?.chasePct || null,
        whiff_pct: streakL14?.whiffPct || null, avg_la: streakL14?.avgLA || null, fb_pct: streakL14?.fbPct || null,
      };

      if (splitsR && Object.keys(splitsR).length > 0) {
        await sbUpsert('edge_matchup_cache', {
          player_id: h.id, player_name: h.name, team: h.team, position: h.position,
          pitcher_hand: 'R', season: 2025, pitch_splits: splitsR, bat_side: h.batSide,
          hot_score: hot.score, hot_grade: hot.grade, trend: hot.trend,
          ...l14Fields,
          season_k_pct: seasonKvsR?.kPct || null, season_pa_vs_hand: seasonKvsR?.nPA || null,
          season_xslg: svR.xslg || null, season_xba: svR.xba || null, season_xwoba: svR.xwoba || null,
          season_xobp: svR.xobp || null, season_barrel_pct: svR.barrelPct || null,
          season_hard_hit_pct: svR.hardHitPct || null,
          season_hr_vs_hand: svR.hrs != null ? svR.hrs : null,
          season_pa_vs_hand_bulk: svR.pa != null ? svR.pa : null,
          updated_at: new Date().toISOString(),
          ...sbFields, ...flagFields,
        }, 'player_id,pitcher_hand,season');
      }

      if (splitsL && Object.keys(splitsL).length > 0) {
        await sbUpsert('edge_matchup_cache', {
          player_id: h.id, player_name: h.name, team: h.team, position: h.position,
          pitcher_hand: 'L', season: 2025, pitch_splits: splitsL, bat_side: h.batSide,
          hot_score: hot.score, hot_grade: hot.grade, trend: hot.trend,
          ...l14Fields,
          season_k_pct: seasonKvsL?.kPct || null, season_pa_vs_hand: seasonKvsL?.nPA || null,
          season_xslg: svL.xslg || null, season_xba: svL.xba || null, season_xwoba: svL.xwoba || null,
          season_xobp: svL.xobp || null, season_barrel_pct: svL.barrelPct || null,
          season_hard_hit_pct: svL.hardHitPct || null,
          season_hr_vs_hand: svL.hrs != null ? svL.hrs : null,
          season_pa_vs_hand_bulk: svL.pa != null ? svL.pa : null,
          updated_at: new Date().toISOString(),
          ...sbFields, ...flagFields,
        }, 'player_id,pitcher_hand,season');
      }

      await sbUpsert('edge_hot_history', {
        player_id: h.id, player_name: h.name, game_date: today, hot_score: hot.score,
        hard_hit_pct: streakL14?.hardHitPct || null, xwoba: streakL14?.xwoba || null,
        bb_pct: streakL14?.bbPct || null, k_pct: streakL14?.kPct || null,
        barrel_pct: streakL14?.barrelPct || null, avg_ev: streakL14?.avgEV || null,
        ld_pct: streakL14?.ldPct || null, chase_pct: streakL14?.chasePct || null,
        whiff_pct: streakL14?.whiffPct || null, contact_pct: streakL14?.contactPct || null,
        n_pa: streakL14?.nPA || 0,
        npa_l7: streakL7?.nPA || 0, npa_l28: streakL28?.nPA || 0, npa_season: seasonBaseline?.nPA || 0,
        chase_pct_l7: streakL7?.chasePct || null, zone_swing_l7: streakL7?.zoneSwingPct || null,
        whiff_pct_l7: streakL7?.whiffPct || null, avg_ev_l7: streakL7?.avgEV || null,
        barrel_pct_l7: streakL7?.barrelPct || null, xwoba_l7: streakL7?.xwoba || null,
        chase_pct_l28: streakL28?.chasePct || null, zone_swing_l28: streakL28?.zoneSwingPct || null,
        whiff_pct_l28: streakL28?.whiffPct || null, avg_ev_l28: streakL28?.avgEV || null,
        barrel_pct_l28: streakL28?.barrelPct || null, xwoba_l28: streakL28?.xwoba || null,
        chase_pct_season: seasonBaseline?.chasePct || null, zone_swing_season: seasonBaseline?.zoneSwingPct || null,
        whiff_pct_season: seasonBaseline?.whiffPct || null, avg_ev_season: seasonBaseline?.avgEV || null,
        barrel_pct_season: seasonBaseline?.barrelPct || null, xwoba_season: seasonBaseline?.xwoba || null,
        woba_season: seasonBaseline?.woba || null, woba_l14: streakL14?.woba || null,
        zone_swing_l14: streakL14?.zoneSwingPct || null,
        ...deltas, ...flagFields,
      }, 'player_id,game_date');

      saved++;
      console.log((i+1) + '/' + hitters.length, h.name,
        'OK vsR:' + Object.keys(splitsR).length, 'vsL:' + Object.keys(splitsL).length,
        'hot:' + hot.score, 'L7:' + (streakL7?.nPA || 0) + 'pa', '[' + windowSrc + ']');

      if (i % 10 === 9) await sleep(200);
    } catch (e) {
      errors++;
      console.log((i+1) + '/' + hitters.length, h.name, 'X', (e.message || String(e)).substring(0, 80));
    }
  }

  console.log('========================================');
  console.log('DONE! Saved:', saved, '| Errors:', errors, '| Skipped:', skipped);
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
