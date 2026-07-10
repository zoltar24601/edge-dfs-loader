# EDGE DFS - Data Loaders

GitHub Actions data pipeline feeding Supabase (project rklfzqqusainitumsvta) for the Burning Edges MLB app.

## Workflows (.github/workflows/)
- daily-loader.yml - scheduled 6am UTC daily: results-backfill -> hitter-loader-v3 -> pitcher-loader -> catcher-loader
- hitter-loader-v3.yml - manual dispatch for the incremental hitter loader
- statcast-backfill.yml - manual dispatch, date-range inputs, backfills edge_statcast_daily
- Secrets: SUPABASE_URL, SUPABASE_KEY (service role)
- NOTE: GitHub disables scheduled workflows after 60 days of repo inactivity. If daily runs stop, check Actions for an "Enable workflow" banner.

## Scripts (root scripts/ folder - the nested .github/workflows/scripts/ copy is dead/duplicate)
- hitter-loader-v3.js - CURRENT hitter loader (incremental). Ingests missing dates into edge_statcast_daily (1 bulk Savant call per date, self-healing), then computes pitch splits (2025+2026), L7/L14/L28/season windows, hot score, emerging/cooling flags FROM the table. Writes edge_matchup_cache (vsR+vsL rows, conflict player_id,pitcher_hand,season) and edge_hot_history (conflict player_id,game_date). Runs ~10 min.
- hitter-loader.js - RETIRED 90-minute per-player crawler. Kept as rollback only. Do not run on schedule.
- statcast-backfill.js - date-range backfill for edge_statcast_daily (idempotent upserts; safe to re-run any range)
- pitcher-loader.js - pitcher stats + arsenal into edge_pitcher_cache. KNOWN ISSUE: avg_ip_per_start divides total IP (incl. relief) by GS, inflating starters with relief outings (e.g. Manaea). A fixed version using last-8-starts game logs exists but is NOT yet deployed.
- catcher-loader.js, results-backfill.js, clear-hitters.js, fangraphs-loader.py, park-factors-loader.js - supporting loaders

## Key table: edge_statcast_daily
- One row per player + game_date + p_throws + pitch_type. Raw COUNTS and SUMS only (never averages) so any window rebuilds exactly by summing rows.
- Backfilled 2025-03 through present, regular season only (hfGT=R). Old loader included spring/postseason in season PA; v3's regular-season-only baseline is intentional.
- zone_swing columns come from this table (the old loader never populated zone_swing_l14).

## Data sources
- Baseball Savant statcast_search CSV (flaky; always retry with backoff, User-Agent header, detect <html error pages)
- MLB statsapi (statsapi.mlb.com/api/v1) - schedule, rosters (rosterType=active, keep TWP/Ohtani, relabel TWP->DH), season stats
- Savant bulk hitter season stats vs R/L: 2 CSV calls per run

## Working rules
- Fix root causes, not band-aids. QA before declaring done. One change at a time.
- Keep code ASCII-only (em-dashes have corrupted GitHub pastes before).
- All Supabase writes are idempotent upserts - safe to re-run anything.
- Never let one player/date failure kill a run: per-item try/catch, log and continue.
