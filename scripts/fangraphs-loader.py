#!/usr/bin/env python3
"""
EDGE DFS — FanGraphs Data Loader
Pulls wRC+, ISO (hitters) and SIERA, xFIP, Stuff+, GB% (pitchers)
from FanGraphs via pybaseball. Stores in Supabase.
Runs weekly via GitHub Actions.
"""

import os
import json
import re
import unicodedata
from datetime import datetime

# pybaseball for FanGraphs data
from pybaseball import batting_stats, pitching_stats

# HTTP for Supabase
import urllib.request

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
SEASON = 2026

def clean_name(name):
    """Normalize player names for matching:
    - Strip HTML tags
    - Remove accents/diacritics
    - Handle apostrophes consistently
    - Lowercase for matching
    """
    if not name:
        return ''
    # Strip HTML tags (FanGraphs sometimes wraps names in <a> tags)
    name = re.sub(r'<[^>]+>', '', str(name))
    # Normalize unicode (remove accents: é→e, ñ→n, etc.)
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in name if not unicodedata.combining(c))
    # Standardize apostrophes and quotes
    name = name.replace('\u2019', "'").replace('\u2018', "'").replace('\u201c', '"').replace('\u201d', '"')
    # Remove periods (J.D. Martinez → JD Martinez)
    name = name.replace('.', '')
    # Clean whitespace
    name = ' '.join(name.split())
    return name.strip()

def clean_name_for_match(name):
    """Even more aggressive cleaning for fuzzy matching"""
    c = clean_name(name).lower()
    # Remove Jr, Sr, II, III, IV suffixes
    c = re.sub(r'\s+(jr|sr|ii|iii|iv)\.?$', '', c, flags=re.IGNORECASE)
    # Remove apostrophes and hyphens
    c = c.replace("'", "").replace("-", " ")
    # Remove non-alpha except spaces
    c = re.sub(r'[^a-z ]', '', c)
    return c.strip()

def sb_upsert(table, data):
    """Upsert a row to Supabase"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }
    body = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(url, data=body, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except Exception as e:
        print(f"  Supabase error: {e}")
        return None

def load_hitters():
    """Pull hitter leaderboard from FanGraphs"""
    print(f"\n{'='*60}")
    print(f"LOADING HITTER STATS FROM FANGRAPHS ({SEASON})")
    print(f"{'='*60}")
    
    try:
        # qual=50 to get most players with enough PAs
        # We want: wRC+, ISO, wOBA
        df = batting_stats(SEASON, qual=50)
        print(f"Fetched {len(df)} hitters from FanGraphs")
    except Exception as e:
        print(f"Error fetching hitter data: {e}")
        # Try combined 2025+2026 if 2026 alone doesn't have enough data
        try:
            print("Trying 2025-2026 combined...")
            df = batting_stats(2025, end_season=SEASON, qual=50, ind=0)
            print(f"Fetched {len(df)} hitters (2025-2026 combined)")
        except Exception as e2:
            print(f"Error fetching combined: {e2}")
            return 0
    
    saved = 0
    for _, row in df.iterrows():
        name = str(row.get('Name', ''))
        team = str(row.get('Team', ''))
        fg_id = int(row.get('IDfg', 0)) if 'IDfg' in row else None
        mlb_id = int(row.get('xMLBAMID', 0)) if 'xMLBAMID' in row else None
        
        wrc_plus = float(row.get('wRC+', 0)) if 'wRC+' in row and row.get('wRC+') else None
        iso = float(row.get('ISO', 0)) if 'ISO' in row and row.get('ISO') else None
        
        if not name or not fg_id:
            continue
        
        record = {
            'player_name': clean_name(name),
            'player_name_clean': clean_name_for_match(name),
            'team': team,
            'player_type': 'hitter',
            'season': SEASON,
            'wrc_plus': wrc_plus,
            'iso': iso,
            'fg_player_id': fg_id,
            'mlb_player_id': mlb_id,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        status = sb_upsert('edge_fangraphs_cache', record)
        if status:
            saved += 1
            if saved % 50 == 0:
                print(f"  Saved {saved} hitters...")
    
    print(f"✓ Saved {saved} hitters to Supabase")
    return saved

def load_pitchers():
    """Pull pitcher leaderboard from FanGraphs"""
    print(f"\n{'='*60}")
    print(f"LOADING PITCHER STATS FROM FANGRAPHS ({SEASON})")
    print(f"{'='*60}")
    
    try:
        df = pitching_stats(SEASON, qual=10)
        print(f"Fetched {len(df)} pitchers from FanGraphs")
    except Exception as e:
        print(f"Error fetching pitcher data: {e}")
        try:
            print("Trying 2025-2026 combined...")
            df = pitching_stats(2025, end_season=SEASON, qual=10, ind=0)
            print(f"Fetched {len(df)} pitchers (2025-2026 combined)")
        except Exception as e2:
            print(f"Error fetching combined: {e2}")
            return 0
    
    saved = 0
    for _, row in df.iterrows():
        name = str(row.get('Name', ''))
        team = str(row.get('Team', ''))
        fg_id = int(row.get('IDfg', 0)) if 'IDfg' in row else None
        mlb_id = int(row.get('xMLBAMID', 0)) if 'xMLBAMID' in row else None
        
        siera = float(row.get('SIERA', 0)) if 'SIERA' in row and row.get('SIERA') else None
        xfip = float(row.get('xFIP', 0)) if 'xFIP' in row and row.get('xFIP') else None
        gb_pct = float(row.get('GB%', 0)) if 'GB%' in row and row.get('GB%') else None
        
        # Stuff+ might be named differently
        stuff_plus = None
        for col_name in ['Stuff+', 'StuffPlus', 'stuff_plus']:
            if col_name in row and row.get(col_name):
                try:
                    stuff_plus = float(row[col_name])
                except:
                    pass
                break
        
        # K-BB%
        k_bb_pct = None
        for col_name in ['K-BB%', 'K_BB_pct']:
            if col_name in row and row.get(col_name):
                try:
                    val = row[col_name]
                    if isinstance(val, str):
                        val = val.replace('%', '').strip()
                    k_bb_pct = float(val)
                except:
                    pass
                break
        
        if not name or not fg_id:
            continue
        
        record = {
            'player_name': clean_name(name),
            'player_name_clean': clean_name_for_match(name),
            'team': team,
            'player_type': 'pitcher',
            'season': SEASON,
            'siera': siera,
            'xfip': xfip,
            'stuff_plus': stuff_plus,
            'gb_pct': gb_pct,
            'k_bb_pct': k_bb_pct,
            'fg_player_id': fg_id,
            'mlb_player_id': mlb_id,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        status = sb_upsert('edge_fangraphs_cache', record)
        if status:
            saved += 1
            if saved % 50 == 0:
                print(f"  Saved {saved} pitchers...")
    
    print(f"✓ Saved {saved} pitchers to Supabase")
    return saved

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set")
        exit(1)
    
    print(f"EDGE DFS — FanGraphs Loader")
    print(f"Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Season: {SEASON}")
    
    h = load_hitters()
    p = load_pitchers()
    
    print(f"\n{'='*60}")
    print(f"DONE — {h} hitters, {p} pitchers saved")
    print(f"{'='*60}")
