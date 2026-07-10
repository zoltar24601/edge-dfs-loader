// ============================================================
// EDGE DFS - Pitcher statcast daily backfill (one-time, chunked)
// Fetches ONE bulk Savant CSV per date (ALL pitchers at once),
// aggregates per pitcher + batter stand + pitch type, and upserts
// into edge_statcast_pitch_daily.
//
// Run with a date range (inclusive):
//   START_DATE=2026-06-01 END_DATE=2026-06-30 node scripts/pitcher-statcast-backfill.js
// Env: SUPABASE_URL, SUPABASE_KEY, START_DATE, END_DATE
//
// Classification rules intentionally mirror statcast-backfill.js /
// hitter-loader-v3.js so the two daily tables stay consistent.
// Rows with pitch_type PO/IN/UNK are stored on purpose: the arsenal
// gates in pitcher-loader count RAW pitches, so exact reproduction
// needs them; compute-time code filters them out of arsenals.
// ============================================================
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const START_DATE = process.env.START_DATE;
const END_DATE = process.env.END_DATE;

if (!SUPABASE_URL || !SUPABASE_KEY) { console.error('Missing SUPABASE_URL / SUPABASE_KEY'); process.exit(1); }
if (!START_DATE || !END_DATE) { console.error('Missing START_DATE / END_DATE (YYYY-MM-DD)'); process.exit(1); }

const PA_EVENTS = ['single','double','triple','home_run','field_out','strikeout','walk','hit_by_pitch','grounded_into_double_play','sac_fly','force_out','fielders_choice','fielders_choice_out'];
const SWING_DESC = ['swinging_strike','swinging_strike_blocked','foul','hit_into_play','foul_tip'];
const WHIFF_DESC = ['swinging_strike','swinging_strike_blocked'];
const CONTACT_DESC = ['foul','hit_into_play','foul_tip'];
const CHASE_DESC = ['swinging_strike','swinging_strike_blocked','foul','hit_into_play'];
const OZ_ZONES = ['11','12','13','14'];

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

// Quote-aware CSV parse (player names contain commas)
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
    '?all=true&type=details&player_type=pitcher' +
    '&hfGT=R%7C' +
    '&game_date_gt=' + dateStr +
    '&game_date_lt=' + dateStr +
    '&min_pitches=0&min_results=0&min_pas=0';
  for (let att = 1; att <= 3; att++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      const text = await r.text();
      if (text && !text.includes('<html')) return text;
      if (att < 3) await sleep(4000 * att);
    } catch (e) {
      if (att === 3) throw e;
      await sleep(4000 * att);
    }
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
  // Round the float sums to keep payloads tidy
  return Object.values(aggs).map(a => ({
    ...a,
    velo_sum: Math.round(a.velo_sum * 10) / 10,
    ev_sum: Math.round(a.ev_sum * 10) / 10,
    la_sum: Math.round(a.la_sum * 10) / 10,
    xwoba_sum: Math.round(a.xwoba_sum * 1000) / 1000,
    xba_sum: Math.round(a.xba_sum * 1000) / 1000,
  }));
}

async function upsertBatch(rows) {
  const BATCH = 500;
  for (let i = 0; i < rows.length; i += BATCH) {
    const chunk = rows.slice(i, i + BATCH);
    const res = await fetch(SUPABASE_URL + '/rest/v1/edge_statcast_pitch_daily?on_conflict=pitcher_id,game_date,stand,pitch_type', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + SUPABASE_KEY,
        'apikey': SUPABASE_KEY,
        'Prefer': 'resolution=merge-duplicates',
      },
      body: JSON.stringify(chunk),
    });
    if (!res.ok) { const t = await res.text(); throw new Error('Supabase upsert ' + res.status + ': ' + t.substring(0, 200)); }
  }
}

function* dateRange(startStr, endStr) {
  const d = new Date(startStr + 'T12:00:00Z');
  const end = new Date(endStr + 'T12:00:00Z');
  while (d <= end) {
    yield d.toISOString().split('T')[0];
    d.setUTCDate(d.getUTCDate() + 1);
  }
}

async function main() {
  console.log('EDGE DFS Pitcher statcast backfill:', START_DATE, '->', END_DATE);
  let daysDone = 0, daysEmpty = 0, rowsTotal = 0, failures = 0;

  for (const dateStr of dateRange(START_DATE, END_DATE)) {
    try {
      const csv = await fetchDayCSV(dateStr);
      const rows = csv ? parseCSV(csv) : null;
      if (!rows || rows.length < 10) {
        daysEmpty++;
        console.log(dateStr, '- no games / no data');
        await sleep(1500);
        continue;
      }
      const aggs = aggregateDay(rows, dateStr);
      await upsertBatch(aggs);
      daysDone++;
      rowsTotal += aggs.length;
      console.log(dateStr, '- ' + rows.length + ' pitches -> ' + aggs.length + ' rows stored');
      await sleep(2500); // polite pacing between Savant calls
    } catch (e) {
      failures++;
      console.log(dateStr, '- FAILED:', (e.message || String(e)).substring(0, 120));
      await sleep(5000);
      // keep going - a single bad date never kills the run; rerun the
      // same range later and upserts make it idempotent
    }
  }

  console.log('==============================================');
  console.log('DONE. days loaded:', daysDone, '| empty:', daysEmpty, '| failed:', failures, '| rows:', rowsTotal);
  if (failures > 0) process.exit(1); // mark the run red so failed dates get noticed
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
