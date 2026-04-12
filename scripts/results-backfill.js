import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MLB = 'https://statsapi.mlb.com/api/v1';

// Yesterday's date
const yesterday = new Date();
yesterday.setDate(yesterday.getDate() - 1);
const date = yesterday.toISOString().split('T')[0];

async function sbUpdate(table, match, data) {
  const params = Object.entries(match).map(([k,v]) => `${k}=eq.${v}`).join('&');
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table + '?' + params, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY },
    body: JSON.stringify(data)
  });
  return res.ok;
}

// DK MLB Scoring
// Hitters: 1B=3, 2B=5, 3B=8, HR=10, RBI=2, R=2, BB=2, HBP=2, SB=5
// Pitchers: IP out=2.25, K=2, W=4, ER=-2, H=-0.6, BB=-0.6, HBP=-0.6, CG=2.5, CGSO=2.5, NH=5

function calcHitterDKFP(stats) {
  const s = stats;
  const singles = (s.hits || 0) - (s.doubles || 0) - (s.triples || 0) - (s.homeRuns || 0);
  return (singles * 3) + ((s.doubles || 0) * 5) + ((s.triples || 0) * 8) + ((s.homeRuns || 0) * 10) +
    ((s.rbi || 0) * 2) + ((s.runs || 0) * 2) + ((s.baseOnBalls || 0) * 2) + ((s.hitByPitch || 0) * 2) +
    ((s.stolenBases || 0) * 5);
}

function calcPitcherDKFP(stats) {
  const s = stats;
  // Parse IP: "6.2" means 6 innings + 2 outs = 20 outs
  const ipStr = String(s.inningsPitched || '0');
  const ipParts = ipStr.split('.');
  const fullInnings = parseInt(ipParts[0]) || 0;
  const extraOuts = parseInt(ipParts[1]) || 0;
  const totalOuts = fullInnings * 3 + extraOuts;
  
  const ipPoints = totalOuts * 2.25;
  const kPoints = (s.strikeOuts || 0) * 2;
  const wPoints = (s.wins || 0) * 4;
  const erPenalty = (s.earnedRuns || 0) * -2;
  const hPenalty = (s.hits || 0) * -0.6;
  const bbPenalty = (s.baseOnBalls || 0) * -0.6;
  const hbpPenalty = (s.hitByPitch || 0) * -0.6;
  
  // CG bonus
  let cgBonus = 0;
  if (s.completeGames && s.completeGames > 0) {
    cgBonus += 2.5;
    if ((s.earnedRuns || 0) === 0 && (s.runs || 0) === 0) cgBonus += 2.5; // CGSO
  }
  // No-hitter
  if (s.completeGames > 0 && (s.hits || 0) === 0) cgBonus += 5;
  
  return ipPoints + kPoints + wPoints + erPenalty + hPenalty + bbPenalty + hbpPenalty + cgBonus;
}

async function backfillResults() {
  console.log('='.repeat(60));
  console.log('EDGE DFS — Results Backfill');
  console.log('Pulling actual DK scores for:', date);
  console.log('='.repeat(60));

  // 1. Get yesterday's schedule
  const schedRes = await fetch(MLB + '/schedule?sportId=1&date=' + date + '&hydrate=linescore');
  const schedData = await schedRes.json();
  const games = schedData.dates?.[0]?.games || [];
  console.log('Games found:', games.length);

  if (games.length === 0) {
    console.log('No games yesterday. Exiting.');
    return;
  }

  let hittersSaved = 0, pitchersSaved = 0, errors = 0;

  for (const game of games) {
    const gamePk = game.gamePk;
    // Skip games not final
    if (game.status?.detailedState !== 'Final' && game.status?.detailedState !== 'Game Over') {
      console.log('  Game', gamePk, '— not final, skipping');
      continue;
    }

    // 2. Get box score
    try {
      const boxRes = await fetch(MLB + '/game/' + gamePk + '/boxscore');
      const box = await boxRes.json();

      // Process both teams
      for (const side of ['away', 'home']) {
        const teamData = box.teams?.[side];
        if (!teamData) continue;

        // Hitters
        const batters = teamData.batters || [];
        const playerMap = teamData.players || {};
        
        for (const batterId of batters) {
          const playerKey = 'ID' + batterId;
          const player = playerMap[playerKey];
          if (!player) continue;
          
          const batting = player.stats?.batting;
          if (!batting || batting.atBats === undefined) continue;
          // Skip pitchers who didn't bat (or had 0 AB and 0 BB)
          if ((batting.atBats || 0) === 0 && (batting.baseOnBalls || 0) === 0 && (batting.hitByPitch || 0) === 0) continue;

          const dkFP = calcHitterDKFP(batting);
          const ok = await sbUpdate('edge_projection_log',
            { player_id: batterId, game_date: date, player_type: 'hitter' },
            { actual_dk_fp: Math.round(dkFP * 10) / 10 }
          );
          if (ok) hittersSaved++;
          else errors++;
        }

        // Pitchers
        const pitcherIds = teamData.pitchers || [];
        for (const pitcherId of pitcherIds) {
          const playerKey = 'ID' + pitcherId;
          const player = playerMap[playerKey];
          if (!player) continue;
          
          const pitching = player.stats?.pitching;
          if (!pitching || pitching.inningsPitched === undefined) continue;

          // Check for win
          const decisions = player.stats?.pitching;
          
          const dkFP = calcPitcherDKFP(pitching);
          const ok = await sbUpdate('edge_projection_log',
            { player_id: pitcherId, game_date: date, player_type: 'pitcher' },
            { actual_dk_fp: Math.round(dkFP * 10) / 10 }
          );
          if (ok) pitchersSaved++;
          else errors++;
        }
      }

      // Small delay between games
      await new Promise(r => setTimeout(r, 300));
    } catch(e) {
      console.log('  Error processing game', gamePk, ':', e.message);
      errors++;
    }
  }

  console.log('');
  console.log('='.repeat(60));
  console.log('DONE — Hitters:', hittersSaved, '| Pitchers:', pitchersSaved, '| Errors:', errors);
  console.log('='.repeat(60));
}

backfillResults().catch(e => { console.error('Fatal:', e); process.exit(1); });
