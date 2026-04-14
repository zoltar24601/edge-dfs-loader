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

  return { xwoba, kPct, bbPct, hardHitPct, whiffPct, nPA };
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
