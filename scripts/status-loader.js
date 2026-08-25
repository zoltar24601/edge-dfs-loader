import fetch from 'node-fetch';

// ============================================================
// edge_player_status loader — IL / inactive player status.
// Extracted from the RETIRED hitter-loader.js: when the hitter-loader-v3 cutover
// went live 2026-07-10, this status load was orphaned (v3 doesn't do it), so
// edge_player_status froze on 2026-07-09. This standalone runs in daily-loader.yml.
//
// Pulls full roster for all 30 teams, upserts anyone whose status.code !== 'A'
// (IL D7/D10/D15/D60, RM, PL, SU, etc.), then deletes rows not touched this run
// so a player who came off IL / was activated auto-clears (absence = Active).
// Env: SUPABASE_URL, SUPABASE_KEY (service role).
// ============================================================
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MLB = 'https://statsapi.mlb.com/api/v1';
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function sbUpsert(table, data, conflictCols) {
  const conflict = conflictCols ? '?on_conflict=' + conflictCols : '';
  const res = await fetch(SUPABASE_URL + '/rest/v1/' + table + conflict, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Prefer': 'resolution=merge-duplicates' },
    body: JSON.stringify(data),
  });
  if (!res.ok) throw new Error(await res.text());
}

async function loadPlayerStatuses() {
  console.log('---- Loading player statuses (IL/inactive) ----');
  const runStart = new Date().toISOString();
  let ilSaved = 0, ilErrors = 0;

  const teamsRes = await fetch(MLB + '/teams?sportId=1&season=2026');
  const teamsData = await teamsRes.json();
  const teams = (teamsData.teams || []).filter(t => t.sport?.id === 1);

  for (let i = 0; i < teams.length; i++) {
    const team = teams[i];
    try {
      const r = await fetch(MLB + '/teams/' + team.id + '/roster?rosterType=fullRoster&season=2026');
      const d = await r.json();
      const roster = d.roster || [];
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
          console.log('  upsert err', p.person.fullName, (upErr.message || '').substring(0, 60));
        }
      }
      console.log((i + 1) + '/' + teams.length, team.abbreviation, 'inactive:', inactive.length);
      await sleep(300);
    } catch (e) {
      ilErrors++;
      console.log(team.abbreviation, 'X', (e.message || '').substring(0, 80));
    }
  }

  // Clean up rows not touched this run (player came off IL / activated).
  try {
    const delRes = await fetch(
      SUPABASE_URL + '/rest/v1/edge_player_status?last_updated=lt.' + encodeURIComponent(runStart),
      { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + SUPABASE_KEY, 'apikey': SUPABASE_KEY, 'Prefer': 'return=minimal' } }
    );
    if (delRes.ok) console.log('IL cleanup: stale rows removed');
    else console.log('IL cleanup warn:', (await delRes.text()).substring(0, 80));
  } catch (e) {
    console.log('IL cleanup error:', (e.message || '').substring(0, 80));
  }

  console.log('Player status load complete: saved', ilSaved, 'errors', ilErrors);
}

loadPlayerStatuses().then(() => process.exit(0)).catch(e => { console.error('FATAL', e.message); process.exit(1); });
