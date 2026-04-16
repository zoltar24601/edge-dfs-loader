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

function avg(a){return a.reduce((x,y)=>x+y,0)/a.length}
function r1(v){return Math.round(v*10)/10}
function r3(v){return Math.round(v*1000)/1000}
function sleep(ms){return new Promise(r=>setTimeout(r,ms))}

// Fetch pitcher's season stats (IP, GS, SB allowed, LOB%, etc)
async function fetchPitcherSeasonStats(pitcherId) {
  try {
    let stats = null, season = 2026;
    const r26 = await fetch(MLB + '/people/' + pitcherId + '/stats?stats=season&season=2026&group=pitching');
    const d26 = await r26.json();
    stats = d26.stats?.[0]?.splits?.[0]?.stat;
    let ip = stats ? parseFloat(stats.inningsPitched || 0) : 0;
    // Need at least 30 IP for meaningful sample, else fall back to 2025
    if (!stats || ip < 30) {
      const r25 = await fetch(MLB + '/people/' + pitcherId + '/stats?stats=season&season=2025&group=pitching');
      const d25 = await r25.json();
      const s25 = d25.stats?.[0]?.splits?.[0]?.stat;
      if (s25 && parseFloat(s25.inningsPitched || 0) > ip) {
        stats = s25;
        season = 2025;
        ip = parseFloat(s25.inningsPitched || 0);
      }
    }
    if (!stats) return null;
    const gs = parseInt(stats.gamesStarted || 0);
    const games = parseInt(stats.gamesPlayed || 0);
    const bf = parseInt(stats.battersFaced || 0);
    const sb = parseInt(stats.stolenBases || 0);
    const cs = parseInt(stats.caughtStealing || 0);
    const lob = parseFloat(stats.leftOnBase || 0);
    const h = parseInt(stats.hits || 0);
    const bb = parseInt(stats.baseOnBalls || 0);
    const er = parseInt(stats.earnedRuns || 0);
    const hr = parseInt(stats.homeRuns || 0);
    
    // Only compute per-start averages if pitcher is mostly a starter
    // (gamesStarted is at least 50% of games played, AND has 3+ starts)
    const mostlyStarter = gs >= 3 && (games === 0 || gs / games >= 0.5);
    const avgIp = mostlyStarter ? ip / gs : null;
    const avgBf = mostlyStarter ? bf / gs : null;
    
    // Sanity check — if avg IP per start is impossibly high, it's bad data
    const cleanAvgIp = (avgIp && avgIp >= 3.0 && avgIp <= 8.0) ? avgIp : null;
    const cleanAvgBf = (avgBf && avgBf >= 12 && avgBf <= 32) ? avgBf : null;
    
    return {
      ip, gs, bf, sb, cs,
      avgIpPerStart: cleanAvgIp,
      avgBfPerStart: cleanAvgBf,
      lobPct: null, // disabled - formula was wrong
      hr,
      season,
    };
  } catch(e) {
    return null;
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

const LABELS = {FF:'4-Seam FB',SI:'Sinker',FC:'Cutter',FA:'Fastball',SL:'Slider',CU:'Curveball',KC:'Knuckle-Curve',CS:'Slow Curve',CH:'Changeup',FS:'Splitter',ST:'Sweeper',SV:'Slurve',SW:'Sweeper'};

function computeResultsByHand(rows, batterHand) {
  if (!rows || !rows.length) return null;
  const filtered = batterHand ? rows.filter(r => r.stand === batterHand) : rows;
  if (filtered.length < 50) return null;

  // PA events
  const paEvents = ['single','double','triple','home_run','field_out','strikeout','walk','hit_by_pitch','grounded_into_double_play','sac_fly','force_out','fielders_choice','fielders_choice_out'];
  const pas = filtered.filter(r => paEvents.includes(r.events));
  const nPA = pas.length;
  if (nPA < 20) return null;

  // xwOBA allowed
  const xwVals = filtered.map(r => parseFloat(r.estimated_woba_using_speedangle)).filter(v => !isNaN(v));
  const xwoba = xwVals.length ? r3(avg(xwVals)) : null;

  // K% and BB%
  const ks = pas.filter(r => r.events === 'strikeout').length;
  const bbs = pas.filter(r => r.events === 'walk').length;
  const kPct = r1(ks / nPA * 100);
  const bbPct = r1(bbs / nPA * 100);

  // Hard Hit % allowed
  const bip = filtered.filter(r => r.type === 'X');
  const bipEV = bip.filter(r => !isNaN(parseFloat(r.launch_speed)));
  const hh = bipEV.filter(r => parseFloat(r.launch_speed) >= 95);
  const hardHitPct = bipEV.length ? r1(hh.length / bipEV.length * 100) : null;

  // Whiff %
  const swings = filtered.filter(r => ['swinging_strike','swinging_strike_blocked','foul','hit_into_play','foul_tip'].includes(r.description));
  const whiffs = filtered.filter(r => ['swinging_strike','swinging_strike_blocked'].includes(r.description));
  const whiffPct = swings.length ? r1(whiffs.length / swings.length * 100) : null;

  // Avg EV allowed
  const avgEV = bipEV.length ? r1(avg(bipEV.map(r => parseFloat(r.launch_speed)))) : null;

  // FB% (fly balls / balls in play). bb_type can be: ground_ball, line_drive, fly_ball, popup
  const bbTyped = bip.filter(r => r.bb_type);
  const flyBalls = bbTyped.filter(r => r.bb_type === 'fly_ball' || r.bb_type === 'popup').length;
  const fbPct = bbTyped.length ? r1(flyBalls / bbTyped.length * 100) : null;

  // Barrel% allowed (using launch angle + EV optimal range as proxy)
  // Barrel: EV >= 98 and LA between 26-30 (rough definition)
  const barrels = bipEV.filter(r => {
    const ev = parseFloat(r.launch_speed);
    const la = parseFloat(r.launch_angle);
    return ev >= 98 && la >= 26 && la <= 30;
  });
  const barrelPct = bipEV.length ? r1(barrels.length / bipEV.length * 100) : null;

  return { xwoba, kPct, bbPct, hardHitPct, whiffPct, nPA, avgEV, fbPct, barrelPct };
}

function computeArsenalByHand(rows, batterHand) {
  if (!rows || !rows.length) return null;
  const filtered = batterHand ? rows.filter(r => r.stand === batterHand) : rows;
  if (filtered.length < 50) return null;
  const byPitch = {};
  filtered.forEach(r => {
    const pt = r.pitch_type;
    if (!pt || pt === 'null' || pt === '') return;
    if (!byPitch[pt]) byPitch[pt] = { count: 0, velos: [], spinRates: [] };
    byPitch[pt].count++;
    const v = parseFloat(r.release_speed); if (!isNaN(v)) byPitch[pt].velos.push(v);
    const sp = parseFloat(r.release_spin_rate); if (!isNaN(sp)) byPitch[pt].spinRates.push(sp);
  });
  const total = Object.values(byPitch).reduce((sum, p) => sum + p.count, 0);
  if (total < 50) return null;
  const arsenal = {};
  Object.entries(byPitch).forEach(([pt, data]) => {
    const usage = data.count / total;
    if (usage < 0.02) return;
    arsenal[pt] = {
      label: LABELS[pt] || pt, usage: r3(usage), count: data.count,
      avgVelo: data.velos.length ? r1(avg(data.velos)) : null,
      avgSpin: data.spinRates.length ? Math.round(avg(data.spinRates)) : null,
    };
  });
  return Object.keys(arsenal).length >= 2 ? arsenal : null;
}

async function fetchCSV(pitcherId, season) {
  const url = 'https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&player_type=pitcher&pitchers_lookup%5B%5D=' + pitcherId + '&hfSea=' + season + '%7C&group_by=name&min_pitches=0&min_results=0&min_pas=0';
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

async function main() {
  console.log('⚾ EDGE DFS PITCHER LOADER — Node.js');

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

  for (let i = 0; i < pitchers.length; i++) {
    const p = pitchers[i];
    try {
      const csv = await fetchCSV(p.id, '2025%7C2026');
      const rows = parseCSV(csv);
      if (!rows || rows.length < 100) { console.log((i+1) + '/' + pitchers.length, p.name, '— skipped'); continue; }

      const arsenalAll = computeArsenalByHand(rows, null);
      const arsenalVsR = computeArsenalByHand(rows, 'R');
      const arsenalVsL = computeArsenalByHand(rows, 'L');
      if (!arsenalAll) { console.log((i+1) + '/' + pitchers.length, p.name, '— not enough data'); continue; }

      // Compute results by batter hand
      const resultsVsR = computeResultsByHand(rows, 'R');
      const resultsVsL = computeResultsByHand(rows, 'L');

      let era = null, whip = null, kPer9 = null, bbPer9 = null, ip = null;
      try {
        const sr = await fetch(MLB + '/people/' + p.id + '/stats?stats=season&season=2025&group=pitching');
        const sd = await sr.json();
        const st = sd.stats?.[0]?.splits?.[0]?.stat;
        if (st) { era = parseFloat(st.era)||null; whip = parseFloat(st.whip)||null; kPer9 = parseFloat(st.strikeoutsPer9Inn)||null; bbPer9 = parseFloat(st.walksPer9Inn)||null; ip = st.inningsPitched||null; }
      } catch(e) {}

      // Fetch enhanced season stats (IP/start, BF/start, SB allowed, LOB%)
      const ext = await fetchPitcherSeasonStats(p.id);

      await sbUpsert('edge_pitcher_cache', {
        pitcher_id: p.id, pitcher_name: p.name, team: p.team, hand: p.hand, season: 2025,
        arsenal: arsenalAll, arsenal_vs_r: arsenalVsR, arsenal_vs_l: arsenalVsL,
        era, whip, k_per_9: kPer9, bb_per_9: bbPer9, ip,
        xwoba_vs_r: resultsVsR?.xwoba || null,
        xwoba_vs_l: resultsVsL?.xwoba || null,
        k_pct_vs_r: resultsVsR?.kPct || null,
        k_pct_vs_l: resultsVsL?.kPct || null,
        bb_pct_vs_r: resultsVsR?.bbPct || null,
        bb_pct_vs_l: resultsVsL?.bbPct || null,
        hard_hit_pct_vs_r: resultsVsR?.hardHitPct || null,
        hard_hit_pct_vs_l: resultsVsL?.hardHitPct || null,
        whiff_pct_vs_r: resultsVsR?.whiffPct || null,
        whiff_pct_vs_l: resultsVsL?.whiffPct || null,
        n_pa_vs_r: resultsVsR?.nPA || null,
        n_pa_vs_l: resultsVsL?.nPA || null,
        avg_ev_allowed_r: resultsVsR?.avgEV || null,
        avg_ev_allowed_l: resultsVsL?.avgEV || null,
        fb_pct_r: resultsVsR?.fbPct || null,
        fb_pct_l: resultsVsL?.fbPct || null,
        barrel_pct_allowed_r: resultsVsR?.barrelPct || null,
        barrel_pct_allowed_l: resultsVsL?.barrelPct || null,
        sb_allowed: ext?.sb || 0,
        cs_caught: ext?.cs || 0,
        innings_pitched: ext?.ip || null,
        games_started: ext?.gs || 0,
        avg_ip_per_start: ext?.avgIpPerStart ? r1(ext.avgIpPerStart) : null,
        avg_bf_per_start: ext?.avgBfPerStart ? r1(ext.avgBfPerStart) : null,
        lob_pct: ext?.lobPct ? r3(ext.lobPct) : null,
        game_date: new Date().toISOString().split('T')[0],
        updated_at: new Date().toISOString(),
      }, 'pitcher_id,season');

      saved++;
      const topR = arsenalVsR ? Object.entries(arsenalVsR).sort((a,b)=>b[1].usage-a[1].usage).slice(0,2).map(([pt,d])=>d.label+' '+Math.round(d.usage*100)+'%').join(', ') : 'N/A';
      const topL = arsenalVsL ? Object.entries(arsenalVsL).sort((a,b)=>b[1].usage-a[1].usage).slice(0,2).map(([pt,d])=>d.label+' '+Math.round(d.usage*100)+'%').join(', ') : 'N/A';
      const splitR = resultsVsR ? 'xwOBA:'+resultsVsR.xwoba+' K:'+resultsVsR.kPct+'%' : 'N/A';
      const splitL = resultsVsL ? 'xwOBA:'+resultsVsL.xwoba+' K:'+resultsVsL.kPct+'%' : 'N/A';
      console.log((i+1) + '/' + pitchers.length, p.name, '(' + p.team + ' ' + p.hand + 'HP) ✓ vsR:', splitR, '| vsL:', splitL);

      if (i % 3 === 2) await sleep(1000);
      else await sleep(400);

    } catch(e) {
      errors++;
      console.log((i+1) + '/' + pitchers.length, p.name, '✗', e.message?.substring(0, 80));
    }
  }

  console.log('========================================');
  console.log('PITCHER LOADER DONE! Saved:', saved, '| Errors:', errors);
}

main().catch(e => { console.error(e); process.exit(1); });
