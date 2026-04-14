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

function computeStreak(rows,days){
  if(!rows||!rows.length)return null;
  const dates=[...new Set(rows.map(r=>r.game_date))].sort().reverse();
  const lastDate=dates[0]?new Date(dates[0]):new Date();
  const cutoff=new Date(lastDate);cutoff.setDate(cutoff.getDate()-days);
  const recent=rows.filter(r=>new Date(r.game_date)>=cutoff);
  if(!recent.length)return{nPA:0};
  const paE=['single','double','triple','home_run','field_out','strikeout','walk','hit_by_pitch','grounded_into_double_play','sac_fly','force_out','fielders_choice','fielders_choice_out'];
  const pas=recent.filter(r=>paE.includes(r.events));const nPA=pas.length;
  if(nPA<5)return{nPA};
  const wk=recent.filter(r=>r.events==='walk'),ks=recent.filter(r=>r.events==='strikeout');
  const bip=recent.filter(r=>r.type==='X');
  const bipEV=bip.filter(r=>!isNaN(parseFloat(r.launch_speed)));
  const hh=bipEV.filter(r=>parseFloat(r.launch_speed)>=95);
  const br=bipEV.filter(r=>{const e=parseFloat(r.launch_speed),a=parseFloat(r.launch_angle);return e>=98&&a>=26&&a<=30;});
  const ld=bipEV.filter(r=>{const a=parseFloat(r.launch_angle);return !isNaN(a)&&a>=10&&a<=25;});
  const ev=bipEV.map(r=>parseFloat(r.launch_speed));
  const wb=recent.map(r=>parseFloat(r.estimated_woba_using_speedangle)).filter(v=>!isNaN(v));
  const xb=recent.map(r=>parseFloat(r.estimated_ba_using_speedangle)).filter(v=>!isNaN(v));
  const sw=recent.filter(r=>['swinging_strike','swinging_strike_blocked','foul','hit_into_play','foul_tip'].includes(r.description));
  const ct=recent.filter(r=>['foul','hit_into_play','foul_tip'].includes(r.description));
  const wf=recent.filter(r=>['swinging_strike','swinging_strike_blocked'].includes(r.description));
  const oz=recent.filter(r=>['11','12','13','14'].includes(r.zone));
  const ch=oz.filter(r=>['swinging_strike','swinging_strike_blocked','foul','hit_into_play'].includes(r.description));
  return{nPA,bbPct:r1(wk.length/nPA*100),kPct:r1(ks.length/nPA*100),
    hardHitPct:r1(bipEV.length?hh.length/bipEV.length*100:0),
    barrelPct:r1(bipEV.length?br.length/bipEV.length*100:0),
    ldPct:r1(bipEV.length?ld.length/bipEV.length*100:0),
    avgEV:r1(ev.length?avg(ev):0),
    xwoba:wb.length?r3(avg(wb)):null,xba:xb.length?r3(avg(xb)):null,
    contactPct:r1(sw.length?ct.length/sw.length*100:0),
    chasePct:oz.length?r1(ch.length/oz.length*100):null,
    whiffPct:r1(sw.length?wf.length/sw.length*100:0)};
}

function calcHot(s){
  if(!s||(s.nPA||0)<5)return{score:50,grade:'C',trend:'NEUTRAL'};
  const hh=s.hardHitPct??38,xw=s.xwoba??0.320,bb=s.bbPct??8.5,ld=s.ldPct??21,k=s.kPct??23;
  const sc=Math.round(clamp((hh-25)/32*100,0,100)*0.35+clamp((xw-0.270)/0.155*100,0,100)*0.25+clamp((bb-5)/14*100,0,100)*0.20+clamp((ld-14)/22*100,0,100)*0.10+clamp((35-k)/20*100,0,100)*0.10);
  return{score:sc,grade:sc>=80?'A+':sc>=70?'A':sc>=60?'B+':sc>=50?'B':sc>=40?'C+':sc>=30?'C':'D',
    trend:sc>=68?'HOT':sc>=55?'WARM':sc>=42?'NEUTRAL':'COLD'};
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

async function main() {
  console.log('⚾ EDGE DFS HITTER LOADER — Node.js');
  console.log('Date:', today);

  // 1. Schedule
  console.log('Fetching schedule...');
  const schedRes = await fetch(MLB + '/schedule?sportId=1&date=' + today + '&hydrate=probablePitcher,team');
  const schedData = await schedRes.json();
  const games = schedData.dates?.[0]?.games || [];
  console.log('Games:', games.length);

  if (!games.length) { console.log('No games today'); return; }

  // 2. Rosters
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
        if (pos === 'P') continue; // Keep TWP (Ohtani)
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

      // Splits use combined 2025+2026 data (bigger sample + current season)
      const splitsR = computeSplits(rowsAll, 'R');
      const splitsL = computeSplits(rowsAll, 'L');

      // Hot streak uses 2026 only (current season), fallback to last 14 days of all data
      let streak, streakSrc;
      if (rows2026 && rows2026.length >= 20) { streak = computeStreak(rows2026, 14); streakSrc = '2026'; }
      else { streak = computeStreak(rowsAll, 14); streakSrc = 'all'; }
      const hot = calcHot(streak);

      if (splitsR && Object.keys(splitsR).length > 0) {
        await sbUpsert('edge_matchup_cache', {
          player_id: h.id, player_name: h.name, team: h.team, position: h.position,
          pitcher_hand: 'R', season: 2025, pitch_splits: splitsR, bat_side: h.batSide,
          hot_score: hot.score, hot_grade: hot.grade, trend: hot.trend,
          n_pa: streak?.nPA || 0, hard_hit_pct: streak?.hardHitPct || null,
          barrel_pct: streak?.barrelPct || null, xwoba: streak?.xwoba || null,
          xba: streak?.xba || null, bb_pct: streak?.bbPct || null, k_pct: streak?.kPct || null,
          ld_pct: streak?.ldPct || null, avg_ev: streak?.avgEV || null,
          contact_pct: streak?.contactPct || null, chase_pct: streak?.chasePct || null,
          whiff_pct: streak?.whiffPct || null, updated_at: new Date().toISOString()
        }, 'player_id,pitcher_hand,season');
      }

      if (splitsL && Object.keys(splitsL).length > 0) {
        await sbUpsert('edge_matchup_cache', {
          player_id: h.id, player_name: h.name, team: h.team, position: h.position,
          pitcher_hand: 'L', season: 2025, pitch_splits: splitsL, bat_side: h.batSide,
          hot_score: hot.score, hot_grade: hot.grade, trend: hot.trend,
          n_pa: streak?.nPA || 0, hard_hit_pct: streak?.hardHitPct || null,
          barrel_pct: streak?.barrelPct || null, xwoba: streak?.xwoba || null,
          xba: streak?.xba || null, bb_pct: streak?.bbPct || null, k_pct: streak?.kPct || null,
          ld_pct: streak?.ldPct || null, avg_ev: streak?.avgEV || null,
          contact_pct: streak?.contactPct || null, chase_pct: streak?.chasePct || null,
          whiff_pct: streak?.whiffPct || null, updated_at: new Date().toISOString()
        }, 'player_id,pitcher_hand,season');
      }

      // Log hot score + all components to history table for trend tracking
      await sbUpsert('edge_hot_history', {
        player_id: h.id, player_name: h.name, game_date: today, hot_score: hot.score,
        hard_hit_pct: streak?.hardHitPct || null, xwoba: streak?.xwoba || null,
        bb_pct: streak?.bbPct || null, k_pct: streak?.kPct || null,
        barrel_pct: streak?.barrelPct || null, avg_ev: streak?.avgEV || null,
        ld_pct: streak?.ldPct || null, chase_pct: streak?.chasePct || null,
        whiff_pct: streak?.whiffPct || null, contact_pct: streak?.contactPct || null,
        n_pa: streak?.nPA || 0,
      }, 'player_id,game_date');

      saved++;
      console.log((i+1) + '/' + hitters.length, h.name, '✓ vsR:' + Object.keys(splitsR).length, 'vsL:' + Object.keys(splitsL).length, 'hot:' + hot.score, '[' + streakSrc + ']');

      if (i % 3 === 2) await sleep(1200);
      else await sleep(500);

    } catch(e) {
      errors++;
      console.log((i+1) + '/' + hitters.length, h.name, '✗', e.message?.substring(0, 80));
    }
  }

  console.log('========================================');
  console.log('DONE! Saved:', saved, '| Errors:', errors, '| Skipped:', skipped);
}

main().catch(e => { console.error(e); process.exit(1); });
