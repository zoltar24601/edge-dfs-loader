-- Add stolen base columns to hitter table
ALTER TABLE edge_matchup_cache 
  ADD COLUMN IF NOT EXISTS sb_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cs_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS sb_attempts INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS sb_success_rate REAL,
  ADD COLUMN IF NOT EXISTS season_pa INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS sprint_speed REAL;

-- Add SB allowed and avg IP per start to pitcher table
ALTER TABLE edge_pitcher_cache
  ADD COLUMN IF NOT EXISTS sb_allowed INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cs_caught INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS innings_pitched REAL,
  ADD COLUMN IF NOT EXISTS games_started INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS avg_ip_per_start REAL,
  ADD COLUMN IF NOT EXISTS avg_bf_per_start REAL,
  ADD COLUMN IF NOT EXISTS lob_pct REAL,
  ADD COLUMN IF NOT EXISTS hr_fb_pct REAL;

-- Create catcher data table
CREATE TABLE IF NOT EXISTS edge_catcher_cache (
  catcher_id INTEGER,
  catcher_name TEXT,
  team TEXT,
  season INTEGER,
  innings_caught REAL,
  sb_against INTEGER DEFAULT 0,
  cs_caught INTEGER DEFAULT 0,
  cs_pct REAL,
  framing_runs REAL,
  pop_time REAL,
  arm_strength REAL,
  updated_at TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (catcher_id, season)
);

ALTER TABLE edge_catcher_cache ENABLE ROW LEVEL SECURITY;
CREATE POLICY "public_read" ON edge_catcher_cache FOR SELECT USING (true);
CREATE POLICY "public_insert" ON edge_catcher_cache FOR INSERT WITH CHECK (true);
CREATE POLICY "public_update" ON edge_catcher_cache FOR UPDATE USING (true);
