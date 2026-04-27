import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MLB = 'https://statsapi.mlb.com/api/v1';
const today = new Date().toISOString().split('T')[0];
let saved = 0, errors = 0, skipped = 0;

async function sbUpsert(table, data, conflictCols) {
  const conflict = conflictCols ? '?on_conflict=' + conflictCols : '';
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table + conflict, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Prefer': 'resolution=merge-duplicates' },
    body: JSON.stringify(data)
  });
  if (!res.ok) { const t = await res.text(); throw new Error(t); }
}

function avg(a){return a.reduce((x,y)=>x+y,0)/a.length}
function r1(v){return Math.round(v*10)/10}
function r3(v){return Math.round(v*1000)/1000}
function clamp(v,lo,hi){return Math.max(lo,Math.min(hi,v))}
function sleep(ms){return new Promise(r=>setTimeout(r,ms))}

// ---- IL / inactive player status loader ----
// Pulls full roster for all 30 teams, flags anyone whose status isn't Active.
// Uses last_updated timestamp to clean up stale rows (players who came off IL).
async function loadPlayerStatuses() {
  console.log('---- Loading player statuses (IL/inactive) ----');
  const runStart = new Date().toISOString();
  let ilSaved = 0, ilErrors = 0;

  // Fetch all 30 MLB teams
  const teamsRes = await fetch(MLB + '/teams?sportId=1&season=2026');
  const teamsData = await teamsRes.json();
  const teams = (teamsData.teams || []).filter(t => t.sport?.id === 1);

  for (let i = 0; i < teams.length; i++) {
    const team = teams[i];
    try {
      const r = await fetch(MLB + '/teams/' + team.id + '/roster?rosterType=fullRoster&season=2026');
      const d = await r.json();
      const roster = d.roster || [];
      // Anyone not Active: D7/D10/D15/D60 (IL), BRV, PL, RM, SU, etc.
      const inactive = roster.filter(p => p.status?.code && p.status.code !== 'A');

      for (const p of inactive) {
        try {
          await sbUpsert('edge_player_status', {
            player_id: p.person.id,
            player_name: p.person.fullName,
            team_abbr: team.abbreviation,
            status_code: p.status.code,
            status_description: p.status.description,
            last_updated: new Date().toISOString(),
          }, 'player_id');
          ilSaved++;
        } catch (upErr) {
          ilErrors++;
          console.log('  upsert err', p.person.fullName, upErr.message?.substring(0, 60));
        }
      }

      console.log((i+1) + '/' + teams.length, team.abbreviation, 'inactive:', inactive.length);
      await sleep(300);
    } catch (e) {
      ilErrors++;
      console.log(team.abbreviation, '✗', e.message?.substring(0, 80));
    }
  }

  // Clean up rows not touched this run (player came off IL / activated)
  try {
    const delRes = await fetch(
      SUPABASE_URL + '/rest/v1/edge_player_status?last_updated=lt.' + encodeURIComponent(runStart),
      { method: 'DELETE',
        headers: { 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Prefer': 'return=minimal' } }
    );
    if (delRes.ok) console.log('IL cleanup: stale rows removed');
    else console.log('IL cleanup warn:', (await delRes.text()).substring(0, 80));
  } catch (e) {
    console.log('IL cleanup error:', e.message?.substring(0, 80));
  }

  console.log('Player status load complete: saved', ilSaved, 'errors', ilErrors);
}

// Fetch player's stolen base stats and sprint speed for current season
async function fetchPlayerSBData(playerId) {
  try {
    let sb = 0, cs = 0, pa = 0, hr = 0, ab = 0;

    // Get 2026 stats
    const r26 = await fetch(MLB + '/people/' + playerId + '/stats?stats=season&season=2026&group=hitting');
    const d26 = await r26.json();
    const s26 = d26.stats?.[0]?.splits?.[0]?.stat;
    if (s26) {
      sb += parseInt(s26.stolenBases || 0);
      cs += parseInt(s26.caughtStealing || 0);
      pa += parseInt(s26.plateAppearances || 0);
      hr += parseInt(s26.homeRuns || 0);
      ab += parseInt(s26.atBats || 0);
    }

    // Also get 2025 stats and combine
    const r25 = await fetch(MLB + '/people/' + playerId + '/stats?stats=season&season=2025&group=hitting');
    const d25 = await r25.json();
    const s25 = d25.stats?.[0]?.splits?.[0]?.stat;
    if (s25) {
      sb += parseInt(s25.stolenBases || 0);
      cs += parseInt(s25.caughtStealing || 0);
      pa += parseInt(s25.plateAppearances || 0);
      hr += parseInt(s25.homeRuns || 0);
      ab += parseInt(s25.atBats || 0);
    }

    return { sb, cs, pa, hr, ab, attempts: sb + cs, successRate: (sb + cs) > 0 ? sb / (sb + cs) : null, hrPerPA: pa > 0 ? r3(hr / pa) : null };
  } catch(e) {
    return { sb: 0, cs: 0, pa: 0, hr: 0, ab: 0, attempts: 0, successRate: null, hrPerPA: null };
  }
}

function parseCSV(text){
  if(!text||text.length<100)return null;
  const lines=text.trim().split('\n');if(lines.length<2)return null;
  const hdrs=lines[0].split(',').map(h=>h.trim().replace(/"/g,''));
  const rows=[];
  for(let i=1;i<lines.length;i++){
    const vals=[];let cur='',inQ=false;
    for(let j=0;j<lines[i].length;j++){
      if(lines[i][j]==='"')inQ=!inQ;
      else if(lines[i][j]===','&&!inQ){vals.push(cur);cur='';}
      else cur+=lines[i][j];
    }
    vals.push(cur);
    if(vals.length<hdrs.length/2)continue;
    const row={};hdrs.forEach((h,idx)=>{row[h]=vals[idx]?.trim().replace(/"/g,'')||'';});
    rows.push(row);
  }
  return rows;
}

function computeSplits(rows,hand){
  if(!rows||!rows.length)return{};
  const f=hand?rows.filter(r=>r.p_throws===hand):rows;
  if(!f.length)return{};
  const byP={};
  f.forEach(r=>{const pt=r.pitch_type;if(!pt||pt==='null')return;if(!byP[pt])byP[pt]=[];byP[pt].push(r);});
  const L={FF:'4-Seam FB',SI:'Sinker',FC:'Cutter',FA:'Fastball',SL:'Slider',CU:'Curveball',KC:'Knuckle-Curve',CS:'Slow Curve',CH:'Changeup',FS:'Splitter',ST:'Sweeper',SV:'Slurve'};
  const res={};
  Object.entries(byP).forEach(([pt,p])=>{
    if(p.length<10)return;
    const ip=p.filter(x=>x.type==='X');
    const ipWithEV=ip.filter(x=>!isNaN(parseFloat(x.launch_speed)));
    const sw=p.filter(x=>['swinging_strike','swinging_strike_blocked','foul','hit_into_play','foul_tip'].includes(x.description));
    const wh=p.filter(x=>['swinging_strike','swinging_strike_blocked'].includes(x.description));
    const ev=ipWithEV.map(x=>parseFloat(x.launch_speed));
    const wb=p.map(x=>parseFloat(x.estimated_woba_using_speedangle)).filter(v=>!isNaN(v));
    const xb=p.map(x=>parseFloat(x.estimated_ba_using_speedangle)).filter(v=>!isNaN(v));
    const hh=ipWithEV.filter(x=>parseFloat(x.launch_speed)>=95);
    const ld=ipWithEV.filter(x=>{const a=parseFloat(x.launch_angle);return a>=10&&a<=25;});
    res[pt]={label:L[pt]||pt,n:p.length,
      woba:wb.length?r3(avg(wb)):null,xba:xb.length?r3(avg(xb)):null,
      hardHitPct:ipWithEV.length?r1(hh.length/ipWithEV.length*100):0,
      ldPct:ipWithEV.length?r1(ld.length/ipWithEV.length*100):0,
      whiffPct:sw.length?r1(wh.length/sw.length*100):0,
      avgEV:ev.length?r1(avg(ev)):0};
  });
  return res;
}

// Compute full-season K% against a specific pitcher hand (2025+2026 combined)
function computeSeasonKPct(rows, hand) {
  if (!rows || !rows.length) return null;
  const f = hand ? rows.filter(r => r.p_throws === hand) : rows;
  if (!f.length) return null;
  const paE = ['single','double','triple','home_run','field_out','strikeout','walk','hit_by_pitch','grounded_into_double_play','sac_fly','force_out','fielders_choice','fielders_choice_out'];
  const pas = f.filter(r => paE.includes(r.events));
  const nPA = pas.length;
  if (nPA < 20) return null;
  const ks = pas.filter(r => r.events === 'strikeout').length;
  return { kPct: r1(ks / nPA * 100), nPA };
}

// Compute stats over a given window.
// If days === null, uses ALL rows passed in (used for season baseline).
// Otherwise filters to rows within `days` of the most recent game.
//
// IMPORTANT: The original fields (nPA, bbPct, kPct, hardHitPct, barrelPct,
// ldPct, avgEV, avgLA, fbPct, xwoba, xba, contactPct, chasePct, whiffPct)
// preserve the EXACT SAME output behavior as the pre-v2 loader — same zero
// fallbacks, same early-exit on nPA<5 — so downstream projection code that
// reads these columns from edge_matchup_cache is unaffected.
//
// New fields (woba, zoneSwingPct, bipEV, nIZ, nOZ) are ONLY consumed by
// evaluateFlag() and the new rolling-window UI.
function computeStreak(rows,days){
  if(!rows||!rows.length)return null;
  let working;
  if (days === null || days === undefined) {
    working = rows;
  } else {
    const dates=[...new Set(rows.map(r=>r.game_date))].sort().reverse();
    const lastDate=dates[0]?new Date(dates[0]):new Date();
    const cutoff=new Date(lastDate);cutoff.setDate(cutoff.getDate()-days);
    working = rows.filter(r=>new Date(r.game_date)>=cutoff);
  }
  if(!working.length)return{nPA:0};
  const paE=['single','double','triple','home_run','field_out','strikeout','walk','hit_by_pitch','grounded_into_double_play','sac_fly','force_out','fielders_choice','fielders_choice_out'];
  const pas=working.filter(r=>paE.includes(r.events));const nPA=pas.length;
  // EARLY EXIT preserved from old loader — don't compute stats for tiny samples.
  // This keeps the L14 output identical to before for downstream projections.
  if(nPA<5)return{nPA};
  const wk=working.filter(r=>r.events==='walk'),ks=working.filter(r=>r.events==='strikeout');
  const bip=working.filter(r=>r.type==='X');
  const bipEV=bip.filter(r=>!isNaN(parseFloat(r.launch_speed)));
  const hh=bipEV.filter(r=>parseFloat(r.launch_speed)>=95);
  const br=bipEV.filter(r=>{const e=parseFloat(r.launch_speed),a=parseFloat(r.launch_angle);return e>=98&&a>=26&&a<=30;});
  const ld=bipEV.filter(r=>{const a=parseFloat(r.launch_angle);return !isNaN(a)&&a>=10&&a<=25;});
  const ev=bipEV.map(r=>parseFloat(r.launch_speed));
  const las=bipEV.map(r=>parseFloat(r.launch_angle)).filter(a=>!isNaN(a));
  const fbBip=bip.filter(r=>r.bb_type==='fly_ball'||r.bb_type==='popup');
  const bipTyped=bip.filter(r=>r.bb_type);
  const wb=working.map(r=>parseFloat(r.estimated_woba_using_speedangle)).filter(v=>!isNaN(v));
  const xb=working.map(r=>parseFloat(r.estimated_ba_using_speedangle)).filter(v=>!isNaN(v));

  // --- NEW: actual wOBA (outcome-based) for hidden-heat detection ---
  // wOBA weights: BB=0.69, HBP=0.72, 1B=0.89, 2B=1.27, 3B=1.62, HR=2.10
  let wobaNum = 0, wobaDen = 0;
  pas.forEach(r => {
    const e = r.events;
    if (e === 'walk') { wobaNum += 0.69; wobaDen += 1; }
    else if (e === 'hit_by_pitch') { wobaNum += 0.72; wobaDen += 1; }
    else if (e === 'single') { wobaNum += 0.89; wobaDen += 1; }
    else if (e === 'double') { wobaNum += 1.27; wobaDen += 1; }
    else if (e === 'triple') { wobaNum += 1.62; wobaDen += 1; }
    else if (e === 'home_run') { wobaNum += 2.10; wobaDen += 1; }
    else if (['strikeout','field_out','grounded_into_double_play','sac_fly','force_out','fielders_choice','fielders_choice_out'].includes(e)) {
      wobaDen += 1;
    }
  });
  const woba = wobaDen > 0 ? wobaNum / wobaDen : null;

  const sw=working.filter(r=>['swinging_strike','swinging_strike_blocked','foul','hit_into_play','foul_tip'].includes(r.description));
  const ct=working.filter(r=>['foul','hit_into_play','foul_tip'].includes(r.description));
  const wf=working.filter(r=>['swinging_strike','swinging_strike_blocked'].includes(r.description));
  const oz=working.filter(r=>['11','12','13','14'].includes(r.zone));
  const ch=oz.filter(r=>['swinging_strike','swinging_strike_blocked','foul','hit_into_play'].includes(r.description));

  // --- NEW: zone-swing (in-zone pitch reaction) for the flag ---
  const inZone    = working.filter(r => ['1','2','3','4','5','6','7','8','9'].includes(r.zone));
  const zoneSwings = inZone.filter(r => ['swinging_strike','swinging_strike_blocked','foul','hit_into_play','foul_tip'].includes(r.description));

  // --- ORIGINAL return shape preserved exactly (same fallbacks, same types) ---
  return {
    nPA,
    bbPct: r1(wk.length/nPA*100),
    kPct:  r1(ks.length/nPA*100),
    hardHitPct: r1(bipEV.length ? hh.length/bipEV.length*100 : 0),
    barrelPct:  r1(bipEV.length ? br.length/bipEV.length*100 : 0),
    ldPct:      r1(bipEV.length ? ld.length/bipEV.length*100 : 0),
    avgEV:      r1(ev.length ? avg(ev) : 0),
    avgLA:      r1(las.length ? avg(las) : 0),
    fbPct:      r1(bipTyped.length ? fbBip.length/bipTyped.length*100 : 0),
    xwoba:      wb.length ? r3(avg(wb)) : null,
    xba:        xb.length ? r3(avg(xb)) : null,
    contactPct: r1(sw.length ? ct.length/sw.length*100 : 0),
    chasePct:   oz.length ? r1(ch.length/oz.length*100) : null,
    whiffPct:   r1(sw.length ? wf.length/sw.length*100 : 0),

    // --- NEW fields (used only by evaluateFlag and new rolling-window UI) ---
    woba:         woba != null ? r3(woba) : null,
    zoneSwingPct: inZone.length ? r1(zoneSwings.length/inZone.length*100) : null,
    bipEV:        bipEV.length,  // sample-size guard for contact-quality checks
    nIZ:          inZone.length, // sample-size guard for zone-swing
    nOZ:          oz.length,     // sample-size guard for chase
  };
}

// =====================================================================
//  EMERGING / COOLING FLAG
//
//  Evaluates a hitter's L7 window against their season baseline on
//  4 independent signals (plate discipline, contact quality, barrels,
//  hidden heat). Returns tier 0-3 and the signal names that triggered.
//
//  Minimum sample sizes (below these, signal is suppressed):
//    L7 PA >= 20       — for any L7 vs season check
//    L7 BBE >= 10      — for EV/barrel checks specifically
//    L14 PA >= 40      — for hidden-heat (xwOBA - wOBA)
//    season PA >= 150  — for season baseline to be trustworthy
//
//  THRESHOLDS are intentionally conservative — we want few false
//  positives, since each flag should actually mean something.
// =====================================================================
function evaluateFlag(l7, l14, season) {
  const out = {
    is_emerging: false, emerging_tier: 0, emerging_signals: [],
    is_cooling:  false, cooling_tier:  0, cooling_signals:  [],
    is_accelerating: false,
  };
  if (!l7 || !season || (season.nPA || 0) < 150) return out;

  const signalsUp = [];
  const signalsDown = [];

  // --- 1. PLATE DISCIPLINE IMPROVING (chase down) ---
  if ((l7.nIZ || 0) + (l7.nOZ || 0) >= 40 && l7.chasePct != null && season.chasePct != null) {
    const delta = season.chasePct - l7.chasePct; // positive = chasing less
    if (delta >= 4.0) signalsUp.push('discipline');
    if (delta <= -4.0) signalsDown.push('discipline');
  }

  // --- 2. CONTACT QUALITY JUMPING (avg EV up) ---
  if ((l7.bipEV || 0) >= 10 && l7.avgEV != null && season.avgEV != null) {
    const delta = l7.avgEV - season.avgEV;
    if (delta >= 1.5) signalsUp.push('exit_velo');
    if (delta <= -1.5) signalsDown.push('exit_velo');
  }

  // --- 3. BARREL SPIKE ---
  if ((l7.bipEV || 0) >= 10 && l7.barrelPct != null && season.barrelPct != null) {
    const delta = l7.barrelPct - season.barrelPct;
    if (delta >= 4.0) signalsUp.push('barrels');
    if (delta <= -4.0) signalsDown.push('barrels');
  }

  // --- 4. HIDDEN HEAT (xwOBA > wOBA over L14 — market hasn't priced it in) ---
  if (l14 && (l14.nPA || 0) >= 40 && l14.xwoba != null && l14.woba != null) {
    const gap = l14.xwoba - l14.woba;
    if (gap >= 0.050) signalsUp.push('hidden_heat');
    // "Reverse hidden heat" (wOBA running above xwOBA) is weaker as a cool
    // signal, but still worth noting.
    if (gap <= -0.050) signalsDown.push('hidden_heat');
  }

  // --- Compose result ---
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

  // --- Acceleration check: L7 avgEV > L14 avgEV AND L14 avgEV > season avgEV ---
  // Only counts as an extra flag; doesn't itself trigger emerging.
  if (l7.avgEV != null && l14 && l14.avgEV != null && season.avgEV != null) {
    if (l7.avgEV > l14.avgEV && l14.avgEV > season.avgEV && (l7.avgEV - season.avgEV) >= 1.0) {
      out.is_accelerating = true;
    }
  }

  return out;
}

async function fetchCSV(playerId, season) {
  const url = 'https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&player_type=batter&batters_lookup%5B%5D=' + playerId + '&hfSea=' + season + '%7C&group_by=name&min_pitches=0&min_results=0&min_pas=0';
  for (let att = 1; att <= 3; att++) {
    try {
      const r = await fetch(url);
      const text = await r.text();
      if (text && text.length > 200 && !text.includes('<html')) return text;
      if (att < 3) await sleep(2000 * att);
    } catch(e) {
      if (att === 3) throw e;
      await sleep(2000 * att);
    }
  }
  return null;
}

// Bulk fetch pre-computed Savant stats for ALL hitters vs a pitcher hand
async function fetchSavantBulkHitter(pitcherHand) {
  const url = 'https://baseballsavant.mlb.com/statcast_search/csv' +
    '?hfGT=R%7C' +
    '&hfSea=2026%7C2025%7C' +
    '&player_type=batter' +
    '&pitcher_throws=' + pitcherHand +
    '&group_by=name' +
    '&min_pitches=0&min_results=0&min_pas=0' +
    '&sort_col=pitches&sort_order=desc';

  console.log('Fetching Savant bulk: hitters vs', pitcherHand + 'HP...');

  for (let att = 0; att < 3; att++) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!r.ok) { console.warn('Savant returned', r.status); await sleep(5000); continue; }
      const text = await r.text();
      if (!text || text.length < 200 || text.includes('<html')) { console.warn('Savant empty/html response'); await sleep(5000); continue; }

      const lines = text.trim().split('\n');
      if (lines.length < 2) return {};
      // Strip BOM from first char if present, then parse headers
      const headerLine = lines[0].charCodeAt(0) === 0xFEFF ? lines[0].slice(1) : lines[0];
      const headers = headerLine.split(',').map(h => h.trim().replace(/"/g, ''));

      const results = {};

      // Quote-aware CSV split — handles commas inside "quoted" fields like
      // player names (e.g. "Alvarez, Yordan"). Naive split(',') was shifting
      // all subsequent columns by 1, causing hardhit_percent to read bbdist
      // values (e.g. 186) and other columns to be misaligned.
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

      for (let i = 1; i < lines.length; i++) {
        const vals = splitCsv(lines[i]);
        const row = {};
        headers.forEach((h, idx) => { row[h] = vals[idx] || ''; });

        const playerId = parseInt(row.player_id);
        if (!playerId) continue;

        const pf = (field) => { const v = parseFloat(row[field]); return !isNaN(v) ? v : null; };
        const pi = (field) => { const v = parseInt(row[field]); return !isNaN(v) ? v : null; };

        results[playerId] = {
          xslg: pf('xslg') != null ? r3(pf('xslg')) : null,
          xba: pf('xba') != null ? r3(pf('xba')) : null,
          xwoba: pf('xwoba') != null ? r3(pf('xwoba')) : null,
          xobp: pf('xobp') != null ? r3(pf('xobp')) : null,
          barrelPct: pf('barrels_per_bbe_percent') != null ? r1(pf('barrels_per_bbe_percent')) : null,
          hardHitPct: pf('hardhit_percent') != null ? r1(pf('hardhit_percent')) : null,
          kPct: pf('k_percent') != null ? r1(pf('k_percent')) : null,
          bbPct: pf('bb_percent') != null ? r1(pf('bb_percent')) : null,
          pa: pi('pa'),
          hrs: pi('hrs'),
          singles: pi('singles'),
          doubles: pi('doubles'),
          triples: pi('triples'),
        };
      }

      console.log('Got', Object.keys(results).length, 'hitters vs', pitcherHand + 'HP');
      return results;

    } catch(e) {
      console.warn('Savant bulk hitter error:', e.message);
      await sleep(5000 * (att + 1));
    }
  }
  console.warn('Failed to fetch Savant bulk hitter data vs', pitcherHand);
  return {};
}

// Legacy calcHot kept only for backwards-compat display.
// New flag logic is in evaluateFlag(). Once the UI no longer reads hot_score,
// this can be removed.
function calcHot(s){
  if(!s||(s.nPA||0)<5)return{score:50,grade:'C',trend:'NEUTRAL'};
  const hh=s.hardHitPct??38,xw=s.xwoba??0.320,bb=s.bbPct??8.5,ld=s.ldPct??21,k=s.kPct??23;
  const sc=Math.round(clamp((hh-25)/32*100,0,100)*0.35+clamp((xw-0.270)/0.155*100,0,100)*0.25+clamp((bb-5)/14*100,0,100)*0.20+clamp((ld-14)/22*100,0,100)*0.10+clamp((35-k)/20*100,0,100)*0.10);
  return{score:sc,grade:sc>=80?'A+':sc>=70?'A':sc>=60?'B+':sc>=50?'B':sc>=40?'C+':sc>=30?'C':'D',
    trend:sc>=68?'HOT':sc>=55?'WARM':sc>=42?'NEUTRAL':'COLD'};
}

async function main() {
  console.log('⚾ EDGE DFS HITTER LOADER — Node.js [v2 — rolling window flags]');
  console.log('Date:', today);

  // STEP 0: Bulk fetch pre-computed Savant stats for ALL hitters by pitcher hand
  console.log('Fetching bulk Savant hitter stats...');
  const savantVsR = await fetchSavantBulkHitter('R');
  await sleep(3000);
  const savantVsL = await fetchSavantBulkHitter('L');
  await sleep(3000);

  // 1. Schedule
  console.log('Fetching schedule...');
  const schedRes = await fetch(MLB + '/schedule?sportId=1&date=' + today + '&hydrate=probablePitcher,team');
  const schedData = await schedRes.json();
  const games = schedData.dates?.[0]?.games || [];
  console.log('Games:', games.length);

  if (!games.length) {
    console.log('No games today');
    // Still load IL statuses even if no slate today
    await loadPlayerStatuses();
    return;
  }

  // 2. Rosters
  console.log('Fetching rosters...');
  const hitters = [];
  const teamIds = new Set();
  games.forEach(g => { teamIds.add(g.teams.away.team.id); teamIds.add(g.teams.home.team.id); });

  for (const tid of teamIds) {
    try {
      // fullRoster (vs active) includes IL'd players. We want their splits
      // cached so that if they're surprise-activated, projectHitterFP has
      // real data to work with instead of generic fallbacks. The frontend
      // separately tracks IL status from edge_player_status and zeroes out
      // their projection until they're confirmed in a lineup.
      const r = await fetch(MLB + '/teams/' + tid + '/roster?rosterType=fullRoster&hydrate=person');
      const d = await r.json();
      for (const p of (d.roster || [])) {
        const pos = p.position?.abbreviation || '';
        if (pos === 'P') continue;
        const displayPos = pos === 'TWP' ? 'DH' : pos;
        const gm = games.find(g => g.teams.away.team.id === tid || g.teams.home.team.id === tid);
        const isAway = gm?.teams.away.team.id === tid;
        const abbr = isAway ? gm?.teams.away.team.abbreviation : gm?.teams.home.team.abbreviation;
        const batSide = p.person?.batSide?.code || 'R';
        hitters.push({ id: p.person.id, name: p.person.fullName, team: abbr || '???', position: displayPos, batSide });
      }
    } catch(e) {}
  }
  console.log('Found', hitters.length, 'hitters');

  // 3. Fetch Statcast
  for (let i = 0; i < hitters.length; i++) {
    const h = hitters[i];
    try {
      const csvAll = await fetchCSV(h.id, '2025%7C2026');
      const rowsAll = parseCSV(csvAll);
      if (!rowsAll || rowsAll.length < 20) { skipped++; console.log((i+1) + '/' + hitters.length, h.name, '— skipped'); continue; }

      const csv2026 = await fetchCSV(h.id, 2026);
      const rows2026 = parseCSV(csv2026);

      // Splits use combined 2025+2026 data
      const splitsR = computeSplits(rowsAll, 'R');
      const splitsL = computeSplits(rowsAll, 'L');

      // Full-season K% by pitcher hand
      const seasonKvsR = computeSeasonKPct(rowsAll, 'R');
      const seasonKvsL = computeSeasonKPct(rowsAll, 'L');

      // === NEW: Rolling windows + season baseline ===
      // L7 / L14 / L28 use 2026-only current-season data (fallback to all if sparse)
      const windowSource = (rows2026 && rows2026.length >= 20) ? rows2026 : rowsAll;
      const windowSrc = (rows2026 && rows2026.length >= 20) ? '2026' : 'all';

      const streakL7  = computeStreak(windowSource, 7);
      const streakL14 = computeStreak(windowSource, 14);
      const streakL28 = computeStreak(windowSource, 28);
      // Season baseline = the full 2025+2026 combined dataset, no date filter
      const seasonBaseline = computeStreak(rowsAll, null);

      // Legacy hot score still uses L14 (so existing UI keeps working)
      const hot = calcHot(streakL14);

      // Evaluate the new emerging/cooling flag
      const flag = evaluateFlag(streakL7, streakL14, seasonBaseline);

      // Deltas (for UI display & backtesting)
      const d = (a, b) => (a != null && b != null) ? r3(a - b) : null;
      const deltas = {
        chase_delta_l7:     d(seasonBaseline?.chasePct,  streakL7?.chasePct),   // season - L7 (positive = chasing less)
        zone_swing_delta_l7: d(streakL7?.zoneSwingPct,   seasonBaseline?.zoneSwingPct), // L7 - season
        whiff_delta_l7:     d(seasonBaseline?.whiffPct,  streakL7?.whiffPct),   // season - L7 (positive = whiffing less)
        ev_delta_l7:        d(streakL7?.avgEV,           seasonBaseline?.avgEV),
        barrel_delta_l7:    d(streakL7?.barrelPct,       seasonBaseline?.barrelPct),
        xwoba_delta_l7:     d(streakL7?.xwoba,           seasonBaseline?.xwoba),
      };

      // Stolen base data
      const sbData = await fetchPlayerSBData(h.id);

      const svR = savantVsR[h.id] || {};
      const svL = savantVsL[h.id] || {};

      const sbFields = {
        sb_count: sbData.sb,
        cs_count: sbData.cs,
        sb_attempts: sbData.attempts,
        sb_success_rate: sbData.successRate,
        season_pa: sbData.pa,
        season_hr: sbData.hr,
        season_ab: sbData.ab,
        season_hr_per_pa: sbData.hrPerPA,
        sprint_speed: null,
      };

      // Common extra fields for edge_matchup_cache
      const flagFields = {
        is_emerging: flag.is_emerging,
        emerging_tier: flag.emerging_tier,
        emerging_signals: flag.emerging_signals,
        is_cooling: flag.is_cooling,
        cooling_tier: flag.cooling_tier,
        cooling_signals: flag.cooling_signals,
        is_accelerating: flag.is_accelerating,
      };

      if (splitsR && Object.keys(splitsR).length > 0) {
        await sbUpsert('edge_matchup_cache', {
          player_id: h.id, player_name: h.name, team: h.team, position: h.position,
          pitcher_hand: 'R', season: 2025, pitch_splits: splitsR, bat_side: h.batSide,
          hot_score: hot.score, hot_grade: hot.grade, trend: hot.trend,
          n_pa: streakL14?.nPA || 0, hard_hit_pct: streakL14?.hardHitPct || null,
          barrel_pct: streakL14?.barrelPct || null, xwoba: streakL14?.xwoba || null,
          xba: streakL14?.xba || null, bb_pct: streakL14?.bbPct || null, k_pct: streakL14?.kPct || null,
          ld_pct: streakL14?.ldPct || null, avg_ev: streakL14?.avgEV || null,
          contact_pct: streakL14?.contactPct || null, chase_pct: streakL14?.chasePct || null,
          whiff_pct: streakL14?.whiffPct || null, avg_la: streakL14?.avgLA || null, fb_pct: streakL14?.fbPct || null,
          season_k_pct: seasonKvsR?.kPct || null, season_pa_vs_hand: seasonKvsR?.nPA || null,
          season_xslg: svR.xslg || null,
          season_xba: svR.xba || null,
          season_xwoba: svR.xwoba || null,
          season_xobp: svR.xobp || null,
          season_barrel_pct: svR.barrelPct || null,
          season_hard_hit_pct: svR.hardHitPct || null,
          season_hr_vs_hand: svR.hrs != null ? svR.hrs : null,
          season_pa_vs_hand_bulk: svR.pa != null ? svR.pa : null,
          updated_at: new Date().toISOString(),
          ...sbFields,
          ...flagFields,
        }, 'player_id,pitcher_hand,season');
      }

      if (splitsL && Object.keys(splitsL).length > 0) {
        await sbUpsert('edge_matchup_cache', {
          player_id: h.id, player_name: h.name, team: h.team, position: h.position,
          pitcher_hand: 'L', season: 2025, pitch_splits: splitsL, bat_side: h.batSide,
          hot_score: hot.score, hot_grade: hot.grade, trend: hot.trend,
          n_pa: streakL14?.nPA || 0, hard_hit_pct: streakL14?.hardHitPct || null,
          barrel_pct: streakL14?.barrelPct || null, xwoba: streakL14?.xwoba || null,
          xba: streakL14?.xba || null, bb_pct: streakL14?.bbPct || null, k_pct: streakL14?.kPct || null,
          ld_pct: streakL14?.ldPct || null, avg_ev: streakL14?.avgEV || null,
          contact_pct: streakL14?.contactPct || null, chase_pct: streakL14?.chasePct || null,
          whiff_pct: streakL14?.whiffPct || null, avg_la: streakL14?.avgLA || null, fb_pct: streakL14?.fbPct || null,
          season_k_pct: seasonKvsL?.kPct || null, season_pa_vs_hand: seasonKvsL?.nPA || null,
          season_xslg: svL.xslg || null,
          season_xba: svL.xba || null,
          season_xwoba: svL.xwoba || null,
          season_xobp: svL.xobp || null,
          season_barrel_pct: svL.barrelPct || null,
          season_hard_hit_pct: svL.hardHitPct || null,
          season_hr_vs_hand: svL.hrs != null ? svL.hrs : null,
          season_pa_vs_hand_bulk: svL.pa != null ? svL.pa : null,
          updated_at: new Date().toISOString(),
          ...sbFields,
          ...flagFields,
        }, 'player_id,pitcher_hand,season');
      }

      // Log hot score + all rolling components + flags to history for backtesting
      await sbUpsert('edge_hot_history', {
        player_id: h.id, player_name: h.name, game_date: today, hot_score: hot.score,
        // Legacy L14 columns (unchanged)
        hard_hit_pct: streakL14?.hardHitPct || null, xwoba: streakL14?.xwoba || null,
        bb_pct: streakL14?.bbPct || null, k_pct: streakL14?.kPct || null,
        barrel_pct: streakL14?.barrelPct || null, avg_ev: streakL14?.avgEV || null,
        ld_pct: streakL14?.ldPct || null, chase_pct: streakL14?.chasePct || null,
        whiff_pct: streakL14?.whiffPct || null, contact_pct: streakL14?.contactPct || null,
        n_pa: streakL14?.nPA || 0,
        // New rolling columns
        npa_l7:     streakL7?.nPA || 0,
        npa_l28:    streakL28?.nPA || 0,
        npa_season: seasonBaseline?.nPA || 0,

        chase_pct_l7:   streakL7?.chasePct || null,
        zone_swing_l7:  streakL7?.zoneSwingPct || null,
        whiff_pct_l7:   streakL7?.whiffPct || null,
        avg_ev_l7:      streakL7?.avgEV || null,
        barrel_pct_l7:  streakL7?.barrelPct || null,
        xwoba_l7:       streakL7?.xwoba || null,

        chase_pct_l28:  streakL28?.chasePct || null,
        zone_swing_l28: streakL28?.zoneSwingPct || null,
        whiff_pct_l28:  streakL28?.whiffPct || null,
        avg_ev_l28:     streakL28?.avgEV || null,
        barrel_pct_l28: streakL28?.barrelPct || null,
        xwoba_l28:      streakL28?.xwoba || null,

        chase_pct_season:  seasonBaseline?.chasePct || null,
        zone_swing_season: seasonBaseline?.zoneSwingPct || null,
        whiff_pct_season:  seasonBaseline?.whiffPct || null,
        avg_ev_season:     seasonBaseline?.avgEV || null,
        barrel_pct_season: seasonBaseline?.barrelPct || null,
        xwoba_season:      seasonBaseline?.xwoba || null,
        woba_season:       seasonBaseline?.woba || null,
        woba_l14:          streakL14?.woba || null,
        zone_swing_l14:    streakL14?.zoneSwingPct || null,

        ...deltas,
        ...flagFields,
      }, 'player_id,game_date');

      saved++;
      const flagTag = flag.is_emerging ? '🔥'.repeat(flag.emerging_tier)
                    : flag.is_cooling  ? '❄️'.repeat(flag.cooling_tier)
                    : '';
      console.log((i+1) + '/' + hitters.length, h.name,
        '✓ vsR:' + Object.keys(splitsR).length, 'vsL:' + Object.keys(splitsL).length,
        'hot:' + hot.score,
        'L7:' + (streakL7?.nPA || 0) + 'pa',
        flagTag,
        '[' + windowSrc + ']');

      if (i % 3 === 2) await sleep(1200);
      else await sleep(500);

    } catch(e) {
      errors++;
      console.log((i+1) + '/' + hitters.length, h.name, '✗', e.message?.substring(0, 80));
    }
  }

  // Load IL/inactive player statuses for the entire league
  await loadPlayerStatuses();

  console.log('========================================');
  console.log('DONE! Saved:', saved, '| Errors:', errors, '| Skipped:', skipped);
}

main().catch(e => { console.error(e); process.exit(1); });
