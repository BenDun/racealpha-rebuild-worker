"""
RaceAlpha Training Dataset Rebuild Worker
==========================================
Uses DuckDB for fast local processing, then syncs back to Supabase.

Run: python rebuild_worker.py
Schedule: Weekly Sunday 2am AEST (via Railway cron)
"""

import os
import sys
import time
import duckdb
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://pabqrixgzcnqttrkwkil.supabase.co")
DATABASE_URL = os.getenv("DATABASE_URL")  # Direct postgres connection string
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# DuckDB settings
DUCKDB_MEMORY_LIMIT = "4GB"
DUCKDB_THREADS = 4


def log(message: str, level: str = "INFO"):
    """Timestamped logging"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}", flush=True)


def get_db_connection_string() -> str:
    """Build postgres connection string for DuckDB"""
    if DATABASE_URL:
        return DATABASE_URL
    
    # Fallback: construct from individual vars
    db_host = os.getenv("DB_HOST", "db.pabqrixgzcnqttrkwkil.supabase.co")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = SUPABASE_SERVICE_KEY or os.getenv("DB_PASSWORD", "")
    
    return f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"


def run_rebuild():
    """Main rebuild function"""
    start_time = time.time()
    log("=" * 60)
    log("RACEALPHA TRAINING DATASET REBUILD - STARTING")
    log("=" * 60)
    
    # Initialize DuckDB
    log("Initializing DuckDB...")
    con = duckdb.connect(":memory:")
    
    # Configure DuckDB for performance
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    con.execute("SET enable_progress_bar = true")
    
    # Install and load postgres extension
    log("Loading PostgreSQL extension...")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    
    # Connect to Supabase
    db_url = get_db_connection_string()
    log("Connecting to Supabase PostgreSQL...")
    
    try:
        con.execute(f"""
            ATTACH '{db_url}' AS supa (TYPE POSTGRES, READ_ONLY)
        """)
        log("✓ Connected to Supabase")
    except Exception as e:
        log(f"✗ Failed to connect to Supabase: {e}", "ERROR")
        sys.exit(1)
    
    # ==========================================================================
    # PHASE 1: EXTRACT SOURCE DATA
    # ==========================================================================
    phase_start = time.time()
    log("")
    log("=" * 60)
    log("PHASE 1: EXTRACT SOURCE DATA")
    log("=" * 60)
    
    log("Extracting races table...")
    con.execute("""
        CREATE TABLE races AS 
        SELECT * FROM supa.public.races
        WHERE race_date >= '2020-01-01'
    """)
    races_count = con.execute("SELECT COUNT(*) FROM races").fetchone()[0]
    log(f"  ✓ races: {races_count:,} rows")
    
    log("Extracting race_results table...")
    con.execute("""
        CREATE TABLE race_results AS 
        SELECT * FROM supa.public.race_results
        WHERE race_id IN (SELECT race_id FROM races)
    """)
    results_count = con.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
    log(f"  ✓ race_results: {results_count:,} rows")
    
    log("Extracting sectional times...")
    con.execute("""
        CREATE TABLE race_results_sectional_times AS 
        SELECT * FROM supa.public.race_results_sectional_times
        WHERE race_id IN (SELECT race_id FROM races)
    """)
    sectional_count = con.execute("SELECT COUNT(*) FROM race_results_sectional_times").fetchone()[0]
    log(f"  ✓ race_results_sectional_times: {sectional_count:,} rows")
    
    log(f"Phase 1 complete in {time.time() - phase_start:.1f}s")
    
    # ==========================================================================
    # PHASE 2: BASE REBUILD
    # ==========================================================================
    phase_start = time.time()
    log("")
    log("=" * 60)
    log("PHASE 2: BASE REBUILD")
    log("=" * 60)
    
    # Fix location data
    log("Fixing location data...")
    con.execute("""
        UPDATE races SET location = CASE
            WHEN track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
            WHEN track_name IN (
                'Royal Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood',
                'Cheltenham', 'Aintree', 'Newbury', 'Sandown Park', 'Doncaster', 
                'Haydock', 'Kempton', 'Wolverhampton', 'Southwell', 'Lingfield', 
                'Chester', 'Hamilton', 'Musselburgh', 'Nottingham', 'Pontefract', 
                'Sedgefield', 'Plumpton', 'Yarmouth', 'Down Royal'
            ) THEN 'UK'
            WHEN track_name IN (
                'Curragh', 'Leopardstown', 'Punchestown', 'Fairyhouse', 'Naas',
                'Gowran Park', 'Galway', 'Cork', 'Killarney', 'Dundalk', 'Navan', 'Thurles'
            ) THEN 'IE'
            WHEN track_name IN (
                'ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud',
                'Compiegne', 'Fontainebleau', 'Lyon-Parilly', 'Vichy'
            ) THEN 'FR'
            WHEN track_name IN (
                'Tokyo', 'Kyoto', 'Hanshin', 'Nakayama', 'Chukyo',
                'Niigata', 'Fukushima', 'Kokura', 'Sapporo'
            ) THEN 'JP'
            WHEN track_name = 'Meydan' THEN 'AE'
            ELSE 'AU'
        END
        WHERE track_name IS NOT NULL
    """)
    
    # Create base training dataset
    log("Creating base training dataset...")
    con.execute("""
        CREATE TABLE race_training_dataset AS
        SELECT 
            rr.race_id,
            r.race_date,
            r.race_number,
            r.track_name,
            r.race_distance,
            r.race_class,
            r.track_condition,
            r.prize_money,
            COALESCE(r.location, 'AU') AS location,
            
            -- Horse info
            rr.horse_name,
            rr.horse_slug,
            rr.horse_slug || '_' || COALESCE(r.location, 'AU') AS horse_location_slug,
            rr.barrier,
            rr.weight_carried,
            rr.horse_number,
            
            -- Jockey/Trainer
            rr.jockey_name,
            rr.jockey_slug,
            rr.jockey_slug || '_' || COALESCE(r.location, 'AU') AS jockey_location_slug,
            rr.trainer_name,
            rr.trainer_slug,
            rr.trainer_slug || '_' || COALESCE(r.location, 'AU') AS trainer_location_slug,
            
            -- Result data
            rr.position AS final_position,
            rr.margin,
            rr.win_odds,
            rr.raw_time_seconds,
            
            -- Sectional positions (will be backfilled later)
            rr.position_800m,
            rr.position_400m,
            
            -- Placeholder columns to be filled
            CAST(NULL AS INTEGER) AS total_runners,
            CAST(NULL AS INTEGER) AS total_races,
            CAST(NULL AS INTEGER) AS wins,
            CAST(NULL AS INTEGER) AS places,
            CAST(NULL AS DOUBLE) AS win_percentage,
            CAST(NULL AS DOUBLE) AS place_percentage,
            CAST(NULL AS INTEGER) AS days_since_last_race,
            CAST(NULL AS DOUBLE) AS last_5_avg_position,
            CAST(NULL AS DOUBLE) AS last_5_win_rate,
            CAST(NULL AS DOUBLE) AS last_5_place_rate,
            CAST(NULL AS DOUBLE) AS form_momentum,
            CAST(NULL AS DOUBLE) AS form_recency_score,
            CAST(NULL AS INTEGER) AS horse_elo,
            CAST(NULL AS BOOLEAN) AS is_elo_default,
            CAST(NULL AS DOUBLE) AS jockey_win_rate,
            CAST(NULL AS DOUBLE) AS jockey_place_rate,
            CAST(NULL AS INTEGER) AS jockey_total_rides,
            CAST(NULL AS DOUBLE) AS trainer_win_rate,
            CAST(NULL AS DOUBLE) AS trainer_place_rate,
            CAST(NULL AS INTEGER) AS trainer_total_runners,
            CAST(NULL AS VARCHAR) AS running_style,
            CAST(NULL AS INTEGER) AS class_rating,
            CAST(NULL AS VARCHAR) AS track_direction,
            CAST(NULL AS VARCHAR) AS track_category,
            CAST(NULL AS VARCHAR) AS distance_range,
            CAST(NULL AS VARCHAR) AS barrier_position,
            CAST(NULL AS BOOLEAN) AS is_maiden,
            CAST(NULL AS BOOLEAN) AS is_handicap,
            CAST(NULL AS BOOLEAN) AS is_first_timer,
            CAST(NULL AS BOOLEAN) AS is_cross_region_horse,
            CAST(NULL AS BOOLEAN) AS never_placed_flag,
            CAST(NULL AS DOUBLE) AS elo_percentile_in_race,
            CAST(NULL AS DOUBLE) AS odds_percentile_in_race,
            CAST(NULL AS BOOLEAN) AS is_favorite,
            CAST(NULL AS DOUBLE) AS odds_implied_probability,
            CAST(NULL AS BOOLEAN) AS is_longshot,
            CAST(NULL AS INTEGER) AS pos_improvement_800_finish,
            
            -- Timestamps
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
            
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE rr.horse_slug IS NOT NULL 
          AND rr.position IS NOT NULL 
          AND rr.position < 50
    """)
    
    base_count = con.execute("SELECT COUNT(*) FROM race_training_dataset").fetchone()[0]
    log(f"  ✓ Base dataset: {base_count:,} rows")
    
    # Calculate total runners per race
    log("Calculating total runners per race...")
    con.execute("""
        WITH field_sizes AS (
            SELECT race_id, COUNT(*) AS field_size 
            FROM race_training_dataset 
            GROUP BY race_id
        )
        UPDATE race_training_dataset rtd
        SET total_runners = fs.field_size
        FROM field_sizes fs
        WHERE rtd.race_id = fs.race_id
    """)
    
    # Set track properties
    log("Setting track properties...")
    con.execute("""
        UPDATE race_training_dataset SET 
            track_direction = CASE
                WHEN track_name IN ('Flemington', 'Caulfield', 'Moonee Valley', 'Sandown Lakeside', 
                    'Ballarat', 'Bendigo', 'Geelong', 'Mornington', 'Pakenham Synthetic',
                    'Randwick', 'Rosehill', 'Canterbury Park', 'Warwick Farm', 'Newcastle',
                    'Eagle Farm', 'Doomben', 'Sunshine Coast', 'Gold Coast',
                    'Sha Tin', 'Happy Valley') THEN 'Clockwise'
                ELSE 'Anti-Clockwise'
            END,
            track_category = CASE
                WHEN track_name IN ('Flemington', 'Caulfield', 'Moonee Valley', 'Randwick', 
                    'Rosehill', 'Eagle Farm', 'Doomben', 'Morphettville',
                    'Sha Tin', 'Happy Valley') THEN 'Metro'
                WHEN track_name IN ('Sandown Lakeside', 'Sandown Hillside', 'Cranbourne',
                    'Canterbury Park', 'Warwick Farm', 'Newcastle', 'Kembla Grange',
                    'Sunshine Coast', 'Gold Coast', 'Ipswich') THEN 'Provincial'
                ELSE 'Country'
            END
    """)
    
    # Set race class flags
    log("Setting race class flags...")
    con.execute("""
        UPDATE race_training_dataset SET 
            is_maiden = (race_class ILIKE '%maiden%'),
            is_handicap = (race_class ILIKE '%handicap%' OR race_class ILIKE '%hcp%')
    """)
    
    # Distance range categorization
    log("Categorizing distance ranges...")
    con.execute("""
        UPDATE race_training_dataset SET distance_range = CASE
            WHEN race_distance < 1000 THEN 'Sprint (<1000m)'
            WHEN race_distance < 1400 THEN 'Speed (1000-1399m)'
            WHEN race_distance < 1800 THEN 'Mile (1400-1799m)'
            WHEN race_distance < 2200 THEN 'Middle (1800-2199m)'
            ELSE 'Staying (2200m+)'
        END WHERE race_distance IS NOT NULL
    """)
    
    # Barrier position classification
    log("Classifying barrier positions...")
    con.execute("""
        UPDATE race_training_dataset SET barrier_position = CASE
            WHEN total_runners IS NULL OR total_runners = 0 THEN 'unknown'
            WHEN barrier::float / total_runners <= 0.33 THEN 'inner'
            WHEN barrier::float / total_runners <= 0.66 THEN 'middle'
            ELSE 'outer'
        END WHERE barrier IS NOT NULL
    """)
    
    log(f"Phase 2 complete in {time.time() - phase_start:.1f}s")
    
    # ==========================================================================
    # PHASE 3: CAREER & FORM STATS
    # ==========================================================================
    phase_start = time.time()
    log("")
    log("=" * 60)
    log("PHASE 3: CAREER & FORM STATS (Anti-leakage)")
    log("=" * 60)
    
    # Horse career stats with anti-leakage window
    log("Calculating horse career stats...")
    con.execute("""
        WITH horse_career AS (
            SELECT 
                race_id,
                horse_location_slug,
                COUNT(*) OVER w_prior AS prior_races,
                SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior AS prior_wins,
                SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w_prior AS prior_places,
                LAG(race_date) OVER w_order AS prev_date
            FROM race_training_dataset
            WHERE final_position IS NOT NULL AND horse_location_slug IS NOT NULL
            WINDOW 
                w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id 
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                w_order AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id)
        )
        UPDATE race_training_dataset rtd SET 
            total_races = COALESCE(hc.prior_races, 0),
            wins = COALESCE(hc.prior_wins, 0),
            places = COALESCE(hc.prior_places, 0),
            win_percentage = CASE 
                WHEN COALESCE(hc.prior_races, 0) > 0 
                THEN ROUND(hc.prior_wins::numeric / hc.prior_races * 100, 2) 
                ELSE 0 
            END,
            place_percentage = CASE 
                WHEN COALESCE(hc.prior_races, 0) > 0 
                THEN ROUND(hc.prior_places::numeric / hc.prior_races * 100, 2) 
                ELSE 0 
            END,
            is_first_timer = (COALESCE(hc.prior_races, 0) = 0),
            days_since_last_race = CASE 
                WHEN hc.prev_date IS NOT NULL 
                THEN (rtd.race_date - hc.prev_date)::integer 
                ELSE NULL 
            END
        FROM horse_career hc
        WHERE rtd.race_id = hc.race_id AND rtd.horse_location_slug = hc.horse_location_slug
    """)
    
    # Last 5 race stats
    log("Calculating last 5 race stats...")
    con.execute("""
        WITH l5 AS (
            SELECT 
                race_id,
                horse_location_slug,
                COUNT(*) OVER w AS cnt,
                AVG(final_position) OVER w AS avg_pos,
                SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS l5_wins,
                SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w AS l5_places
            FROM race_training_dataset
            WHERE final_position IS NOT NULL AND final_position < 50
            WINDOW w AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id
                        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
        )
        UPDATE race_training_dataset rtd SET 
            last_5_avg_position = ROUND(l5.avg_pos, 2),
            last_5_win_rate = CASE WHEN l5.cnt > 0 THEN ROUND(l5.l5_wins::numeric / LEAST(l5.cnt, 5), 4) ELSE 0 END,
            last_5_place_rate = CASE WHEN l5.cnt > 0 THEN ROUND(l5.l5_places::numeric / LEAST(l5.cnt, 5), 4) ELSE 0 END
        FROM l5
        WHERE rtd.race_id = l5.race_id AND rtd.horse_location_slug = l5.horse_location_slug
    """)
    
    # Form recency score
    log("Calculating form recency score...")
    con.execute("""
        UPDATE race_training_dataset SET form_recency_score = CASE
            WHEN days_since_last_race IS NULL THEN 0.5
            WHEN days_since_last_race <= 14 THEN 1.0
            WHEN days_since_last_race <= 28 THEN 0.9
            WHEN days_since_last_race <= 60 THEN 0.6
            ELSE 0.3
        END
    """)
    
    # Form momentum
    log("Calculating form momentum...")
    con.execute("""
        UPDATE race_training_dataset SET form_momentum = CASE
            WHEN last_5_avg_position IS NULL THEN 0
            WHEN last_5_avg_position <= 2 THEN 1.0
            WHEN last_5_avg_position <= 4 THEN 0.7
            WHEN last_5_avg_position <= 6 THEN 0.4
            ELSE 0.1
        END
    """)
    
    # ELO ratings
    log("Calculating ELO ratings...")
    con.execute("""
        UPDATE race_training_dataset SET 
            horse_elo = ROUND(LEAST(GREATEST(
                1500 + COALESCE(win_percentage, 0) * 5 + LEAST(COALESCE(total_races, 0), 50) * 2, 
                1200
            ), 2000), 0),
            is_elo_default = (total_races = 0 OR total_races IS NULL)
    """)
    
    # Jockey stats
    log("Calculating jockey stats...")
    con.execute("""
        WITH js AS (
            SELECT 
                race_id,
                jockey_location_slug,
                COUNT(*) OVER w AS rides,
                SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins,
                SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w AS places
            FROM race_training_dataset
            WHERE jockey_location_slug IS NOT NULL
            WINDOW w AS (PARTITION BY jockey_location_slug ORDER BY race_date, race_id 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
        )
        UPDATE race_training_dataset rtd SET 
            jockey_win_rate = CASE WHEN COALESCE(js.rides, 0) > 0 THEN ROUND(js.wins::numeric / js.rides, 4) ELSE 0 END,
            jockey_place_rate = CASE WHEN COALESCE(js.rides, 0) > 0 THEN ROUND(js.places::numeric / js.rides, 4) ELSE 0 END,
            jockey_total_rides = COALESCE(js.rides, 0)
        FROM js
        WHERE rtd.race_id = js.race_id AND rtd.jockey_location_slug = js.jockey_location_slug
    """)
    
    # Trainer stats
    log("Calculating trainer stats...")
    con.execute("""
        WITH ts AS (
            SELECT 
                race_id,
                trainer_location_slug,
                COUNT(*) OVER w AS runners,
                SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins,
                SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w AS places
            FROM race_training_dataset
            WHERE trainer_location_slug IS NOT NULL
            WINDOW w AS (PARTITION BY trainer_location_slug ORDER BY race_date, race_id 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
        )
        UPDATE race_training_dataset rtd SET 
            trainer_win_rate = CASE WHEN COALESCE(ts.runners, 0) > 0 THEN ROUND(ts.wins::numeric / ts.runners, 4) ELSE 0 END,
            trainer_place_rate = CASE WHEN COALESCE(ts.runners, 0) > 0 THEN ROUND(ts.places::numeric / ts.runners, 4) ELSE 0 END,
            trainer_total_runners = COALESCE(ts.runners, 0)
        FROM ts
        WHERE rtd.race_id = ts.race_id AND rtd.trainer_location_slug = ts.trainer_location_slug
    """)
    
    # Cross-region flags
    log("Setting cross-region flags...")
    con.execute("""
        WITH multi_region AS (
            SELECT horse_slug 
            FROM race_training_dataset 
            GROUP BY horse_slug 
            HAVING COUNT(DISTINCT location) > 1
        )
        UPDATE race_training_dataset rtd 
        SET is_cross_region_horse = TRUE 
        FROM multi_region mr 
        WHERE rtd.horse_slug = mr.horse_slug
    """)
    con.execute("UPDATE race_training_dataset SET is_cross_region_horse = FALSE WHERE is_cross_region_horse IS NULL")
    con.execute("UPDATE race_training_dataset SET never_placed_flag = (total_races > 0 AND places = 0)")
    
    log(f"Phase 3 complete in {time.time() - phase_start:.1f}s")
    
    # ==========================================================================
    # PHASE 4: ADVANCED FEATURES
    # ==========================================================================
    phase_start = time.time()
    log("")
    log("=" * 60)
    log("PHASE 4: ADVANCED FEATURES")
    log("=" * 60)
    
    # Position improvement
    log("Calculating position improvements...")
    con.execute("""
        UPDATE race_training_dataset SET 
            pos_improvement_800_finish = CASE 
                WHEN position_800m IS NOT NULL AND final_position IS NOT NULL 
                THEN position_800m - final_position 
                ELSE NULL 
            END
    """)
    
    # Running style
    log("Classifying running styles...")
    con.execute("""
        UPDATE race_training_dataset SET running_style = CASE
            WHEN position_800m IS NULL THEN 'unknown'
            WHEN position_800m <= 2 THEN 'leader'
            WHEN position_800m <= 4 THEN 'stalker'
            WHEN position_800m <= total_runners * 0.6 THEN 'midfield'
            ELSE 'closer'
        END
    """)
    
    # Class rating
    log("Setting class ratings...")
    con.execute("""
        UPDATE race_training_dataset SET class_rating = CASE
            WHEN race_class ILIKE '%group 1%' THEN 100
            WHEN race_class ILIKE '%group 2%' THEN 90
            WHEN race_class ILIKE '%group 3%' THEN 80
            WHEN race_class ILIKE '%listed%' THEN 70
            WHEN race_class ILIKE '%benchmark%' OR race_class ILIKE '%bm%' THEN 55
            WHEN track_category = 'Metro' THEN 45
            WHEN track_category = 'Provincial' THEN 35
            ELSE 25
        END
    """)
    
    # Field percentiles
    log("Calculating field percentiles...")
    con.execute("""
        WITH rp AS (
            SELECT 
                race_id, 
                horse_slug,
                PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY horse_elo DESC) AS elo_pct,
                PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY win_odds ASC) AS odds_pct,
                ROW_NUMBER() OVER (PARTITION BY race_id ORDER BY win_odds ASC) AS odds_rank
            FROM race_training_dataset
        )
        UPDATE race_training_dataset rtd SET 
            elo_percentile_in_race = ROUND(rp.elo_pct, 4),
            odds_percentile_in_race = ROUND(rp.odds_pct, 4),
            is_favorite = (rp.odds_rank = 1)
        FROM rp
        WHERE rtd.race_id = rp.race_id AND rtd.horse_slug = rp.horse_slug
    """)
    
    # Odds features
    log("Calculating odds features...")
    con.execute("""
        UPDATE race_training_dataset SET 
            odds_implied_probability = CASE WHEN win_odds > 0 THEN ROUND(1.0 / win_odds, 4) ELSE NULL END,
            is_longshot = (win_odds > 20)
    """)
    
    log(f"Phase 4 complete in {time.time() - phase_start:.1f}s")
    
    # ==========================================================================
    # PHASE 5: EXPORT TO SUPABASE
    # ==========================================================================
    phase_start = time.time()
    log("")
    log("=" * 60)
    log("PHASE 5: EXPORT TO SUPABASE")
    log("=" * 60)
    
    final_count = con.execute("SELECT COUNT(*) FROM race_training_dataset").fetchone()[0]
    log(f"Final dataset: {final_count:,} rows")
    
    # Export to Parquet first (for backup)
    log("Exporting to Parquet backup...")
    con.execute("COPY race_training_dataset TO 'training_dataset_backup.parquet' (FORMAT PARQUET)")
    
    # Write back to Supabase shadow table
    log("Writing to Supabase shadow table...")
    try:
        # Detach read-only and reattach with write access
        con.execute("DETACH supa")
        con.execute(f"""
            ATTACH '{db_url}' AS supa (TYPE POSTGRES)
        """)
        
        # Drop existing shadow table
        con.execute("DROP TABLE IF EXISTS supa.public.race_training_dataset_shadow")
        
        # Create shadow table from our data
        con.execute("""
            CREATE TABLE supa.public.race_training_dataset_shadow AS 
            SELECT * FROM race_training_dataset
        """)
        
        log("✓ Shadow table created")
        
        # Atomic swap would need to be done via SQL on Supabase side
        log("NOTE: Run atomic swap on Supabase to complete:")
        log("  ALTER TABLE race_training_dataset RENAME TO race_training_dataset_old;")
        log("  ALTER TABLE race_training_dataset_shadow RENAME TO race_training_dataset;")
        
    except Exception as e:
        log(f"✗ Failed to write to Supabase: {e}", "ERROR")
        log("Parquet backup available: training_dataset_backup.parquet")
        sys.exit(1)
    
    log(f"Phase 5 complete in {time.time() - phase_start:.1f}s")
    
    # ==========================================================================
    # SUMMARY
    # ==========================================================================
    total_time = time.time() - start_time
    log("")
    log("=" * 60)
    log("REBUILD COMPLETE")
    log("=" * 60)
    log(f"Total rows: {final_count:,}")
    log(f"Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    log("=" * 60)
    
    return {
        "status": "success",
        "rows": final_count,
        "duration_seconds": round(total_time, 1)
    }


if __name__ == "__main__":
    try:
        result = run_rebuild()
        print(f"\n✅ Rebuild completed successfully: {result}")
    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)
