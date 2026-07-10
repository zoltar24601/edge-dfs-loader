# EDGE DFS - Data Loaders

GitHub Actions data pipeline feeding Supabase (project rklfzqqusainitumsvta) for the Burning Edges MLB app.

## Architecture rules (apply to every loader)
- Counts, not averages: daily aggregate tables store raw COUNTS and SUMS only, so any window or split rebuilds exactly by summing rows. Averages/rates are computed at read time.
- Bulk per date, never per player: backfills and daily ingest fetch ONE Savant CSV per DATE (all players at once). Per-player Savant crawls are what made the old loaders take 60-90 min.
- Idempotent upserts: every Supabase write is an upsert with an explicit on_conflict target - any run, backfill, or chunk is safe to re-run.
- Never kill the run: per-date and per-player failures are caught, logged, and skipped; the run continues and self-heals later.
- Self-healing ingest: daily loaders find the max stored game_date and ingest from there through yesterday, so missed runs backfill themselves.
- Verify before cutover: new loader versions run manually via workflow_dispatch and their output is compared against the current table before daily-loader.yml is switched.

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
- pitcher-loader-v4.js - CURRENT pitcher loader (in daily-loader.yml since 2026-07-10; ~3 min vs old 66). Incremental on edge_statcast_pitch_daily, same pattern as hitter v3. Arsenals from the table (regular-season only, matching hitter convention); bulk Savant xwOBA/xSLG and statsapi season stats unchanged; avg_ip_per_start from last 8 actual starts via game logs (fetchRecentStartIP), falling back to relief-adjusted season totals, null if relief outings exceed starts. Writes edge_pitcher_cache (conflict pitcher_id,season).
- pitcher-loader.js - RETIRED 66-minute per-pitcher Savant crawler. Kept as rollback only (single-line swap in daily-loader.yml). Note its arsenals included spring/postseason pitches; v4's regular-season-only baseline is intentional.
- pitcher-statcast-backfill.js - date-range backfill for edge_statcast_pitch_daily (one bulk Savant CSV per date, all pitchers; idempotent; safe to re-run any range)
- catcher-loader.js, results-backfill.js, clear-hitters.js, fangraphs-loader.py, park-factors-loader.js - supporting loaders

## Key table: edge_statcast_daily
- One row per player + game_date + p_throws + pitch_type. Raw COUNTS and SUMS only (never averages) so any window rebuilds exactly by summing rows.
- Pitcher twin: edge_statcast_pitch_daily - one row per pitcher + game_date + stand (batter side) + pitch_type, same counts/sums pattern plus velo_sum/velo_n. PO/IN/UNK pitch_type rows are stored on purpose (arsenal gates count raw pitches); exclude them from arsenal mix at compute time.
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
