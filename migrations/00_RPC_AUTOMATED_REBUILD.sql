-- ============================================================================
-- RPC: AUTOMATED TRAINING REBUILD V2
-- ============================================================================
-- Single RPC function for n8n/cron automation
-- USAGE: SELECT run_training_rebuild_v2();
-- 
-- n8n WORKFLOW:
--   1. HTTP Request node → Supabase REST API
--   2. POST to /rest/v1/rpc/run_training_rebuild_v2
--   3. Headers: apikey, Authorization Bearer
--   4. Schedule: Weekly on Sunday 2am AEST
-- ============================================================================

-- Progress tracking table
CREATE TABLE IF NOT EXISTS rebuild_progress_v2 (
    id SERIAL PRIMARY KEY,
    run_id UUID DEFAULT gen_random_uuid(),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    current_phase TEXT,
    phase_number INTEGER DEFAULT 0,
    total_phases INTEGER DEFAULT 4,
    status TEXT DEFAULT 'running', -- running, completed, failed
    error_message TEXT,
    records_processed INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_rebuild_v2_run_id ON rebuild_progress_v2(run_id);
CREATE INDEX IF NOT EXISTS idx_rebuild_v2_status ON rebuild_progress_v2(status);

-- ============================================================================
-- ENSURE UNIQUE CONSTRAINT FOR UPSERT (Required for ON CONFLICT)
-- ============================================================================
-- This ensures entity IDs are preserved across rebuilds
DO $$
BEGIN
    -- Add unique constraint if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'race_training_dataset_race_id_horse_slug_key'
    ) THEN
        ALTER TABLE race_training_dataset 
        ADD CONSTRAINT race_training_dataset_race_id_horse_slug_key 
        UNIQUE (race_id, horse_slug);
        RAISE NOTICE 'Created unique constraint on (race_id, horse_slug)';
    END IF;
END $$;

-- ============================================================================
-- MAIN RPC FUNCTION
-- ============================================================================
CREATE OR REPLACE FUNCTION run_training_rebuild_v2()
RETURNS JSON AS $$
DECLARE
    v_run_id UUID;
    v_start_time TIMESTAMP;
    v_phase_start TIMESTAMP;
    v_records INTEGER;
    v_result JSON;
BEGIN
    v_start_time := NOW();
    
    -- Create progress record
    INSERT INTO rebuild_progress_v2 (status, current_phase)
    VALUES ('running', 'Initializing')
    RETURNING run_id INTO v_run_id;
    
    -- Set session parameters
    SET statement_timeout = '0';
    SET lock_timeout = '0';
    SET work_mem = '512MB';
    SET maintenance_work_mem = '1GB';
    
    -- ========================================================================
    -- PHASE 1: BASE REBUILD
    -- ========================================================================
    UPDATE rebuild_progress_v2 SET current_phase = 'Phase 1: Base Rebuild', phase_number = 1 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- ========================================================================
    -- FIX SOURCE DATA: Correct location in races table based on track_name
    -- ========================================================================
    UPDATE races SET location = CASE
        -- Hong Kong (ONLY these two tracks)
        WHEN track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
        -- UK tracks (Note: Hamilton is UK, not VIC - 402 vs 40 races)
        -- Note: Ascot omitted - could be UK or WA, defaults to AU currently (WA Ascot)
        WHEN track_name IN (
            'Royal Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood',
            'Cheltenham', 'Aintree', 'Newbury', 'Sandown Park', 'Doncaster', 'Haydock',
            'Kempton', 'Wolverhampton', 'Southwell', 'Lingfield', 'Chester', 'Hamilton',
            'Musselburgh', 'Nottingham', 'Pontefract', 'Sedgefield',
            'Plumpton', 'Yarmouth', 'Down Royal'
        ) THEN 'UK'
        -- Ireland tracks
        WHEN track_name IN (
            'Curragh', 'Leopardstown', 'Punchestown', 'Fairyhouse', 'Naas',
            'Gowran Park', 'Galway', 'Cork', 'Killarney', 'Dundalk', 'Navan', 'Thurles'
        ) THEN 'IE'
        -- France tracks
        WHEN track_name IN (
            'ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud',
            'Compiegne', 'Fontainebleau', 'Lyon-Parilly', 'Vichy',
            'Clairefontaine', 'Bordeaux Le Bouscat', 'Nantes'
        ) THEN 'FR'
        -- Japan tracks
        WHEN track_name IN (
            'Tokyo', 'Kyoto', 'Hanshin', 'Nakayama', 'Chukyo',
            'Niigata', 'Fukushima', 'Kokura', 'Sapporo'
        ) THEN 'JP'
        -- USA tracks
        WHEN track_name IN (
            'Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream',
            'Kentucky Downs', 'Tampa Bay Downs'
        ) THEN 'US'
        -- UAE tracks
        WHEN track_name = 'Meydan' THEN 'AE'
        -- Default everything else to AU (fixes HK misassignments)
        ELSE 'AU'
    END
    WHERE track_name IS NOT NULL;
    
    RAISE NOTICE 'Fixed location data in races table';
    
    -- Backup (only if needed for recovery)
    DROP TABLE IF EXISTS _backup_training_rebuild_v2;
    CREATE TABLE _backup_training_rebuild_v2 AS SELECT * FROM race_training_dataset;
    
    -- ========================================================================
    -- ADD MISSING COLUMNS (ensures schema compatibility)
    -- ========================================================================
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'distance_range') THEN
        ALTER TABLE race_training_dataset ADD COLUMN distance_range TEXT;
        RAISE NOTICE 'Added missing column: distance_range';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'barrier_position') THEN
        ALTER TABLE race_training_dataset ADD COLUMN barrier_position TEXT;
        RAISE NOTICE 'Added missing column: barrier_position';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'class_level_numeric') THEN
        ALTER TABLE race_training_dataset ADD COLUMN class_level_numeric INTEGER;
        RAISE NOTICE 'Added missing column: class_level_numeric';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'is_stepping_up') THEN
        ALTER TABLE race_training_dataset ADD COLUMN is_stepping_up BOOLEAN DEFAULT FALSE;
        RAISE NOTICE 'Added missing column: is_stepping_up';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'is_stepping_down') THEN
        ALTER TABLE race_training_dataset ADD COLUMN is_stepping_down BOOLEAN DEFAULT FALSE;
        RAISE NOTICE 'Added missing column: is_stepping_down';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'races_at_distance') THEN
        ALTER TABLE race_training_dataset ADD COLUMN races_at_distance INTEGER DEFAULT 0;
        RAISE NOTICE 'Added missing column: races_at_distance';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'races_at_track') THEN
        ALTER TABLE race_training_dataset ADD COLUMN races_at_track INTEGER DEFAULT 0;
        RAISE NOTICE 'Added missing column: races_at_track';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'historical_avg_improvement') THEN
        ALTER TABLE race_training_dataset ADD COLUMN historical_avg_improvement NUMERIC(6,2);
        RAISE NOTICE 'Added missing column: historical_avg_improvement';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'pos_improvement_800_finish') THEN
        ALTER TABLE race_training_dataset ADD COLUMN pos_improvement_800_finish INTEGER;
        RAISE NOTICE 'Added missing column: pos_improvement_800_finish';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'avg_late_improvement') THEN
        ALTER TABLE race_training_dataset ADD COLUMN avg_late_improvement NUMERIC(6,2);
        RAISE NOTICE 'Added missing column: avg_late_improvement';
    END IF;
    
    -- ========================================================================
    -- UPSERT PATTERN: Preserves existing IDs, only updates changed data
    -- ========================================================================
    
    -- Step 1: Delete rows that no longer exist in source (orphaned records)
    DELETE FROM race_training_dataset rtd
    WHERE NOT EXISTS (
        SELECT 1 FROM race_results rr 
        JOIN races r ON rr.race_id = r.race_id
        WHERE rr.race_id = rtd.race_id 
          AND rr.horse_slug = rtd.horse_slug
          AND rr.position IS NOT NULL 
          AND rr.position < 50
    );
    
    -- Step 2: UPSERT - Insert new rows OR update existing ones (preserves IDs!)
    INSERT INTO race_training_dataset (
        race_id, race_date, race_number, track_name, race_distance, race_class,
        track_condition, track_weather, horse_name, horse_slug, barrier, weight,
        final_position, margin, jockey, jockey_slug, trainer, trainer_slug, win_odds,
        location, horse_location_slug, jockey_location_slug, trainer_location_slug,
        created_at, updated_at
    )
    SELECT 
        rr.race_id, r.race_date, r.race_number,
        CASE 
            WHEN r.track_name ILIKE '%royal randwick%' THEN 'Randwick'
            WHEN r.track_name ILIKE '%randwick%' THEN 'Randwick'
            ELSE COALESCE(r.track_name, 'Unknown')
        END,
        r.race_distance, r.race_class, r.track_condition, r.track_weather,
        REPLACE(rr.horse_name, 'E+', 'E'), rr.horse_slug, rr.barrier,
        rr.original_weight, rr.position, rr.margin, rr.jockey, rr.jockey_slug,
        rr.trainer, rr.trainer_slug, rr.win_odds,
        -- LOCATION FIX: Determine correct location from track_name
        CASE
            -- Hong Kong (ONLY these two tracks)
            WHEN r.track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
            -- UK tracks (Hamilton = UK 402 races vs AU 40)
            WHEN r.track_name IN (
                'Royal Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood',
                'Cheltenham', 'Aintree', 'Newbury', 'Sandown Park', 'Doncaster', 'Haydock',
                'Kempton', 'Wolverhampton', 'Southwell', 'Lingfield', 'Chester', 'Hamilton',
                'Musselburgh', 'Nottingham', 'Pontefract', 'Sedgefield',
                'Plumpton', 'Yarmouth', 'Down Royal'
            ) THEN 'UK'
            -- Ireland tracks
            WHEN r.track_name IN (
                'Curragh', 'Leopardstown', 'Punchestown', 'Fairyhouse', 'Naas', 
                'Gowran Park', 'Galway', 'Cork', 'Killarney', 'Dundalk', 'Navan', 'Thurles'
            ) THEN 'IE'
            -- France tracks
            WHEN r.track_name IN (
                'ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud',
                'Compiegne', 'Fontainebleau', 'Lyon-Parilly', 'Vichy',
                'Clairefontaine', 'Bordeaux Le Bouscat', 'Nantes'
            ) THEN 'FR'
            -- Japan tracks
            WHEN r.track_name IN (
                'Tokyo', 'Kyoto', 'Hanshin', 'Nakayama', 'Chukyo', 
                'Niigata', 'Fukushima', 'Kokura', 'Sapporo'
            ) THEN 'JP'
            -- USA tracks
            WHEN r.track_name IN (
                'Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream', 
                'Kentucky Downs', 'Tampa Bay Downs'
            ) THEN 'US'
            -- UAE tracks
            WHEN r.track_name = 'Meydan' THEN 'AE'
            -- ALL Australian tracks (comprehensive list - fixes HK misassignments)
            WHEN r.track_name IN (
                -- VIC Metro
                'Flemington', 'Caulfield', 'Moonee Valley', 'The Valley', 'Sandown', 'Sandown Lakeside', 'Sandown Hillside',
                -- VIC Provincial
                'Geelong', 'Bendigo', 'Ballarat', 'Ballarat Synthetic', 'Mornington', 'Cranbourne',
                'Pakenham', 'Pakenham Synthetic', 'Werribee', 'Seymour', 'Kilmore', 'Kyneton',
                'Sale', 'Moe', 'Traralgon', 'Bairnsdale', 'Wangaratta', 'Benalla', 'Wodonga',
                'Warrnambool', 'Echuca', 'Swan Hill',
                -- VIC Country
                'Donald', 'Mildura', 'Ararat', 'Warracknabeal', 'Horsham', 'Yarra Valley',
                'Tatura', 'Terang', 'Stony Creek', 'Camperdown', 'Nhill', 'Colac',
                'Stawell', 'Casterton', 'Kerang', 'Hamilton', 'Great Western',
                -- NSW Metro
                'Randwick', 'Royal Randwick', 'Rosehill', 'Rosehill Gardens', 'Canterbury', 'Warwick Farm',
                -- NSW Provincial
                'Newcastle', 'Kembla Grange', 'Hawkesbury', 'Gosford', 'Wyong', 'Scone',
                'Muswellbrook', 'Tamworth', 'Dubbo', 'Wagga', 'Albury', 'Canberra', 'Grafton',
                -- NSW Country
                'Gundagai', 'Cootamundra', 'Corowa', 'Griffith', 'Murwillumbah', 'Amiens',
                'Orange', 'Bathurst', 'Goulburn', 'Narrandera', 'Moulamein',
                -- QLD
                'Eagle Farm', 'Doomben', 'Sunshine Coast', 'Gold Coast', 'Ipswich', 'Toowoomba',
                -- SA Metro
                'Morphettville', 'Morphettville Parks',
                -- SA Provincial
                'Murray Bridge', 'Murray Bdge', 'Gawler', 'Strathalbyn', 'Port Lincoln', 'Mount Gambier',
                -- SA Country
                'Bordertown', 'Kingscote', 'Oakbank', 'Naracoorte', 'Port Augusta', 'Clare',
                -- TAS
                'Launceston', 'Hobart', 'Devonport Synthetic', 'Longford',
                -- WA
                'Ascot', 'Belmont'
            ) THEN 'AU'
            -- Default to AU for unknown Australian-sounding tracks
            ELSE COALESCE(r.location, 'AU')
        END as corrected_location,
        rr.horse_slug || '-' || CASE
            WHEN r.track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
            WHEN r.track_name IN ('Royal Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood', 'Cheltenham', 'Aintree', 'Newbury', 'Sandown Park', 'Doncaster', 'Haydock', 'Kempton', 'Wolverhampton', 'Southwell', 'Lingfield', 'Chester', 'Hamilton', 'Musselburgh', 'Nottingham', 'Pontefract', 'Sedgefield', 'Plumpton', 'Yarmouth', 'Down Royal') THEN 'UK'
            WHEN r.track_name IN ('Curragh', 'Leopardstown', 'Punchestown', 'Fairyhouse', 'Naas', 'Gowran Park', 'Galway', 'Cork', 'Killarney', 'Dundalk', 'Navan', 'Thurles') THEN 'IE'
            WHEN r.track_name IN ('ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud', 'Compiegne', 'Fontainebleau', 'Lyon-Parilly', 'Vichy', 'Clairefontaine', 'Bordeaux Le Bouscat', 'Nantes') THEN 'FR'
            WHEN r.track_name IN ('Tokyo', 'Kyoto', 'Hanshin', 'Nakayama', 'Chukyo', 'Niigata', 'Fukushima', 'Kokura', 'Sapporo') THEN 'JP'
            WHEN r.track_name IN ('Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream', 'Kentucky Downs', 'Tampa Bay Downs') THEN 'US'
            WHEN r.track_name = 'Meydan' THEN 'AE'
            ELSE 'AU'
        END,
        rr.jockey_slug || '-' || CASE
            WHEN r.track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
            WHEN r.track_name IN ('Royal Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood', 'Cheltenham', 'Aintree', 'Newbury', 'Sandown Park', 'Doncaster', 'Haydock', 'Kempton', 'Wolverhampton', 'Southwell', 'Lingfield', 'Chester', 'Hamilton', 'Musselburgh', 'Nottingham', 'Pontefract', 'Sedgefield', 'Plumpton', 'Yarmouth', 'Down Royal') THEN 'UK'
            WHEN r.track_name IN ('Curragh', 'Leopardstown', 'Punchestown', 'Fairyhouse', 'Naas', 'Gowran Park', 'Galway', 'Cork', 'Killarney', 'Dundalk', 'Navan', 'Thurles') THEN 'IE'
            WHEN r.track_name IN ('ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud', 'Compiegne', 'Fontainebleau', 'Lyon-Parilly', 'Vichy', 'Clairefontaine', 'Bordeaux Le Bouscat', 'Nantes') THEN 'FR'
            WHEN r.track_name IN ('Tokyo', 'Kyoto', 'Hanshin', 'Nakayama', 'Chukyo', 'Niigata', 'Fukushima', 'Kokura', 'Sapporo') THEN 'JP'
            WHEN r.track_name IN ('Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream', 'Kentucky Downs', 'Tampa Bay Downs') THEN 'US'
            WHEN r.track_name = 'Meydan' THEN 'AE'
            ELSE 'AU'
        END,
        rr.trainer_slug || '-' || CASE
            WHEN r.track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
            WHEN r.track_name IN ('Royal Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood', 'Cheltenham', 'Aintree', 'Newbury', 'Sandown Park', 'Doncaster', 'Haydock', 'Kempton', 'Wolverhampton', 'Southwell', 'Lingfield', 'Chester', 'Hamilton', 'Musselburgh', 'Nottingham', 'Pontefract', 'Sedgefield', 'Plumpton', 'Yarmouth', 'Down Royal') THEN 'UK'
            WHEN r.track_name IN ('Curragh', 'Leopardstown', 'Punchestown', 'Fairyhouse', 'Naas', 'Gowran Park', 'Galway', 'Cork', 'Killarney', 'Dundalk', 'Navan', 'Thurles') THEN 'IE'
            WHEN r.track_name IN ('ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud', 'Compiegne', 'Fontainebleau', 'Lyon-Parilly', 'Vichy', 'Clairefontaine', 'Bordeaux Le Bouscat', 'Nantes') THEN 'FR'
            WHEN r.track_name IN ('Tokyo', 'Kyoto', 'Hanshin', 'Nakayama', 'Chukyo', 'Niigata', 'Fukushima', 'Kokura', 'Sapporo') THEN 'JP'
            WHEN r.track_name IN ('Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream', 'Kentucky Downs', 'Tampa Bay Downs') THEN 'US'
            WHEN r.track_name = 'Meydan' THEN 'AE'
            ELSE 'AU'
        END,
        COALESCE((SELECT created_at FROM race_training_dataset WHERE race_id = rr.race_id AND horse_slug = rr.horse_slug), NOW()),
        NOW()
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    WHERE rr.horse_slug IS NOT NULL AND rr.position IS NOT NULL AND rr.position < 50
    ON CONFLICT (race_id, horse_slug) DO UPDATE SET
        race_date = EXCLUDED.race_date,
        race_number = EXCLUDED.race_number,
        track_name = EXCLUDED.track_name,
        race_distance = EXCLUDED.race_distance,
        race_class = EXCLUDED.race_class,
        track_condition = EXCLUDED.track_condition,
        track_weather = EXCLUDED.track_weather,
        horse_name = EXCLUDED.horse_name,
        barrier = EXCLUDED.barrier,
        weight = EXCLUDED.weight,
        final_position = EXCLUDED.final_position,
        margin = EXCLUDED.margin,
        jockey = EXCLUDED.jockey,
        jockey_slug = EXCLUDED.jockey_slug,
        trainer = EXCLUDED.trainer,
        trainer_slug = EXCLUDED.trainer_slug,
        win_odds = EXCLUDED.win_odds,
        location = EXCLUDED.location,
        horse_location_slug = EXCLUDED.horse_location_slug,
        jockey_location_slug = EXCLUDED.jockey_location_slug,
        trainer_location_slug = EXCLUDED.trainer_location_slug,
        updated_at = NOW();
    
    -- Track properties (COMPLETE LIST - All 150+ tracks from database as of Jan 2026)
    UPDATE race_training_dataset SET 
        track_direction = CASE
            -- CLOCKWISE TRACKS (NSW, QLD, HK)
            WHEN track_name IN (
                -- Hong Kong
                'Sha Tin', 'Happy Valley',
                -- NSW Metro
                'Randwick', 'Royal Randwick', 'Rosehill', 'Canterbury', 'Warwick Farm',
                -- NSW Provincial/Country
                'Newcastle', 'Kembla Grange', 'Hawkesbury', 'Gosford', 'Wyong', 'Scone', 
                'Muswellbrook', 'Tamworth', 'Dubbo', 'Orange', 'Bathurst', 'Grafton', 
                'Wagga', 'Albury', 'Canberra', 'Goulburn', 'Gundagai', 'Cootamundra',
                'Corowa', 'Griffith', 'Murwillumbah', 'Narrandera', 'Moulamein',
                -- QLD
                'Eagle Farm', 'Doomben', 'Sunshine Coast', 'Gold Coast', 'Ipswich', 'Toowoomba'
            ) THEN 'clockwise'
            -- ANTICLOCKWISE TRACKS (VIC, SA, WA, TAS - default for AU)
            WHEN location = 'AU' THEN 'anticlockwise'
            -- International - right-handed (clockwise)
            WHEN track_name IN (
                'Ascot', 'Royal Ascot', 'Newmarket', 'Newbury', 'York', 'Goodwood', 'Epsom Downs',
                'Curragh', 'Leopardstown', 'Naas', 'Gowran Park',
                'ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud',
                'Tokyo', 'Nakayama', 'Kyoto', 'Hanshin',
                'Meydan', 'Churchill Downs', 'Keeneland', 'Saratoga'
            ) THEN 'clockwise'
            -- International - left-handed (anticlockwise)
            WHEN track_name IN (
                'Chester', 'Hamilton', 'Kempton', 'Sandown', 'Sandown Park', 'Wolverhampton',
                'Newcastle', 'Lingfield', 'Southwell', 'Cheltenham', 'Aintree', 'Haydock',
                'Doncaster', 'Musselburgh', 'Pontefract', 'Sedgefield', 'Plumpton',
                'Fairyhouse', 'Punchestown', 'Galway', 'Cork', 'Killarney', 'Dundalk', 'Navan', 'Thurles', 'Down Royal',
                'Gulfstream', 'Tampa Bay Downs', 'Kentucky Downs'
            ) THEN 'anticlockwise'
            ELSE NULL
        END,
        track_category = CASE
            -- =====================================================================
            -- METRO TRACKS (Major city tracks - highest class)
            -- =====================================================================
            WHEN track_name IN (
                -- VIC Metro
                'Flemington', 'Caulfield', 'Moonee Valley', 'The Valley', 'Sandown', 'Sandown Lakeside', 'Sandown Hillside',
                -- NSW Metro
                'Randwick', 'Royal Randwick', 'Rosehill', 'Rosehill Gardens', 'Canterbury', 'Warwick Farm',
                -- QLD Metro
                'Eagle Farm', 'Doomben',
                -- SA Metro
                'Morphettville', 'Morphettville Parks',
                -- WA Metro
                'Ascot', 'Belmont',
                -- TAS Metro
                'Launceston', 'Hobart',
                -- HK Metro
                'Sha Tin', 'Happy Valley'
            ) THEN 'Metro'
            -- =====================================================================
            -- PROVINCIAL TRACKS (Regional city tracks)
            -- =====================================================================
            WHEN track_name IN (
                -- VIC Provincial
                'Geelong', 'Bendigo', 'Ballarat', 'Ballarat Synthetic', 'Mornington', 'Cranbourne', 
                'Pakenham', 'Pakenham Synthetic', 'Werribee', 'Seymour', 'Kilmore', 'Kyneton',
                'Sale', 'Moe', 'Traralgon', 'Bairnsdale', 'Wangaratta', 'Benalla', 'Wodonga',
                'Warrnambool', 'Echuca', 'Swan Hill',
                -- NSW Provincial
                'Newcastle', 'Kembla Grange', 'Hawkesbury', 'Gosford', 'Wyong', 'Scone', 
                'Muswellbrook', 'Tamworth', 'Dubbo', 'Wagga', 'Albury', 'Canberra', 'Grafton',
                -- QLD Provincial
                'Sunshine Coast', 'Gold Coast', 'Ipswich', 'Toowoomba',
                -- SA Provincial
                'Murray Bridge', 'Murray Bdge', 'Gawler', 'Strathalbyn', 'Port Lincoln', 'Mount Gambier',
                -- TAS Provincial
                'Devonport Synthetic', 'Longford'
            ) THEN 'Provincial'
            -- =====================================================================
            -- COUNTRY TRACKS (All AU country tracks - explicitly listed)
            -- =====================================================================
            WHEN track_name IN (
                -- VIC Country
                'Donald', 'Mildura', 'Ararat', 'Warracknabeal', 'Horsham', 'Yarra Valley', 
                'Tatura', 'Terang', 'Stony Creek', 'Camperdown', 'Nhill', 'Colac', 
                'Stawell', 'Casterton', 'Kerang', 'Great Western', 'Hanging Rock',
                -- NSW Country
                'Gundagai', 'Cootamundra', 'Corowa', 'Griffith', 'Murwillumbah', 'Amiens',
                'Orange', 'Bathurst', 'Goulburn', 'Narrandera', 'Moulamein',
                -- SA Country
                'Bordertown', 'Kingscote', 'Oakbank', 'Naracoorte', 'Port Augusta', 'Clare',
                'Murray Bdge'
            ) THEN 'Country'
            -- =====================================================================
            -- INTERNATIONAL TRACKS - By country
            -- =====================================================================
            -- UK Major
            WHEN track_name IN (
                'Royal Ascot', 'Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood', 
                'Cheltenham', 'Aintree', 'Newbury', 'Sandown', 'Sandown Park', 'Doncaster', 'Haydock'
            ) THEN 'Metro'
            -- UK Provincial
            WHEN track_name IN (
                'Newcastle', 'Kempton', 'Wolverhampton', 'Southwell', 'Lingfield', 'Chester',
                'Hamilton', 'Musselburgh', 'Nottingham', 'Pontefract', 'Sedgefield', 
                'Plumpton', 'Yarmouth', 'Down Royal'
            ) AND location = 'UK' THEN 'Provincial'
            -- Ireland
            WHEN track_name IN (
                'Curragh', 'Leopardstown'
            ) THEN 'Metro'
            WHEN track_name IN (
                'Punchestown', 'Fairyhouse', 'Naas', 'Gowran Park', 'Galway', 'Cork', 
                'Killarney', 'Dundalk', 'Navan', 'Thurles'
            ) THEN 'Provincial'
            -- France
            WHEN track_name IN (
                'ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud'
            ) THEN 'Metro'
            WHEN track_name IN (
                'Compiegne', 'Fontainebleau', 'Lyon-Parilly', 'Vichy', 
                'Clairefontaine', 'Bordeaux Le Bouscat', 'Nantes'
            ) THEN 'Provincial'
            -- Japan
            WHEN track_name IN (
                'Tokyo', 'Kyoto', 'Hanshin', 'Nakayama'
            ) THEN 'Metro'
            WHEN track_name IN (
                'Chukyo', 'Niigata', 'Fukushima', 'Kokura', 'Sapporo'
            ) THEN 'Provincial'
            -- USA
            WHEN track_name IN (
                'Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream'
            ) THEN 'Metro'
            WHEN track_name IN (
                'Kentucky Downs', 'Tampa Bay Downs'
            ) THEN 'Provincial'
            -- UAE
            WHEN track_name = 'Meydan' THEN 'Metro'
            -- Default: Country for anything else
            ELSE 'Country'
        END,
        track_type = CASE 
            -- Australian Synthetic
            WHEN track_name IN ('Pakenham Synthetic', 'Ballarat Synthetic', 'Devonport Synthetic') THEN 'Synthetic'
            WHEN track_name ILIKE '%synthetic%' THEN 'Synthetic'
            -- UK/Ireland All-Weather (Polytrack/Tapeta/Fibresand)
            WHEN track_name IN ('Wolverhampton', 'Southwell', 'Lingfield', 'Kempton', 'Newcastle', 'Dundalk') 
                 AND location IN ('UK', 'IE') THEN 'Synthetic'
            -- USA Dirt tracks
            WHEN track_name IN ('Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream', 'Tampa Bay Downs') THEN 'Dirt'
            -- UAE - Meydan has both but primarily dirt for big races
            WHEN track_name = 'Meydan' THEN 'Dirt'
            -- Japan - mostly turf but some dirt
            WHEN track_name IN ('Tokyo', 'Kyoto', 'Hanshin', 'Nakayama', 'Chukyo', 'Niigata', 'Fukushima', 'Kokura', 'Sapporo') THEN 'Turf'
            -- Kentucky Downs is turf
            WHEN track_name = 'Kentucky Downs' THEN 'Turf'
            -- Default to Turf
            ELSE 'Turf' 
        END,
        is_maiden_race = (race_class ILIKE '%maiden%'),
        is_handicap_race = (race_class ILIKE '%handicap%' OR race_class ILIKE '%hcp%'),
        distance_range = CASE 
            WHEN race_distance < 1200 THEN 'sprint' 
            WHEN race_distance < 1600 THEN 'mile' 
            WHEN race_distance < 2000 THEN 'middle' 
            ELSE 'staying' 
        END;
    
    -- Total runners
    WITH fs AS (SELECT race_id, COUNT(*) as field_size FROM race_training_dataset GROUP BY race_id)
    UPDATE race_training_dataset rtd SET total_runners = fs.field_size FROM fs WHERE rtd.race_id = fs.race_id;
    
    -- Barrier position
    UPDATE race_training_dataset SET barrier_position = CASE
        WHEN total_runners IS NULL OR total_runners = 0 THEN 'unknown'
        WHEN barrier::float / total_runners <= 0.33 THEN 'inner'
        WHEN barrier::float / total_runners <= 0.66 THEN 'middle' ELSE 'outer'
    END WHERE barrier IS NOT NULL;
    
    RAISE NOTICE 'Phase 1 complete in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- PHASE 2: CAREER & FORM STATS
    -- ========================================================================
    UPDATE rebuild_progress_v2 SET current_phase = 'Phase 2: Career Stats', phase_number = 2 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Horse career stats
    WITH hc AS (
        SELECT race_id, horse_location_slug,
            COUNT(*) OVER w as prior_races,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w as prior_wins,
            SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w as prior_places,
            LAG(race_date) OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id) as prev_date
        FROM race_training_dataset WHERE final_position IS NOT NULL
        WINDOW w AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    )
    UPDATE race_training_dataset rtd SET 
        total_races = COALESCE(hc.prior_races, 0),
        wins = COALESCE(hc.prior_wins, 0),
        places = COALESCE(hc.prior_places, 0),
        win_percentage = CASE WHEN COALESCE(hc.prior_races, 0) > 0 THEN ROUND(hc.prior_wins::numeric / hc.prior_races * 100, 2) ELSE 0 END,
        place_percentage = CASE WHEN COALESCE(hc.prior_races, 0) > 0 THEN ROUND(hc.prior_places::numeric / hc.prior_races * 100, 2) ELSE 0 END,
        is_first_timer = (COALESCE(hc.prior_races, 0) = 0),
        days_since_last_race = CASE WHEN hc.prev_date IS NOT NULL THEN (rtd.race_date - hc.prev_date)::integer ELSE NULL END
    FROM hc WHERE rtd.race_id = hc.race_id AND rtd.horse_location_slug = hc.horse_location_slug;
    
    -- Last 5 stats
    WITH l5 AS (
        SELECT race_id, horse_location_slug,
            LAG(final_position, 1) OVER w as p1, LAG(final_position, 2) OVER w as p2,
            LAG(final_position, 3) OVER w as p3, LAG(final_position, 4) OVER w as p4,
            LAG(final_position, 5) OVER w as p5,
            COUNT(*) OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as cnt
        FROM race_training_dataset WHERE final_position IS NOT NULL AND final_position < 50
        WINDOW w AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id)
    ),
    calc AS (
        SELECT race_id, horse_location_slug, LEAST(cnt, 5) as used,
            CASE WHEN cnt = 0 THEN NULL WHEN cnt = 1 THEN p1 WHEN cnt = 2 THEN (p1+p2)/2.0 WHEN cnt = 3 THEN (p1+p2+p3)/3.0 WHEN cnt = 4 THEN (p1+p2+p3+p4)/4.0 ELSE (p1+p2+p3+p4+p5)/5.0 END as avg_pos,
            CASE WHEN cnt = 0 THEN 0 ELSE ((CASE WHEN p1=1 THEN 1 ELSE 0 END)+(CASE WHEN p2=1 THEN 1 ELSE 0 END)+(CASE WHEN p3=1 THEN 1 ELSE 0 END)+(CASE WHEN p4=1 THEN 1 ELSE 0 END)+(CASE WHEN p5=1 THEN 1 ELSE 0 END))::numeric/LEAST(cnt,5) END as win_rate,
            CASE WHEN cnt = 0 THEN 0 ELSE ((CASE WHEN p1<=3 THEN 1 ELSE 0 END)+(CASE WHEN p2<=3 THEN 1 ELSE 0 END)+(CASE WHEN p3<=3 THEN 1 ELSE 0 END)+(CASE WHEN p4<=3 THEN 1 ELSE 0 END)+(CASE WHEN p5<=3 THEN 1 ELSE 0 END))::numeric/LEAST(cnt,5) END as place_rate
        FROM l5
    )
    UPDATE race_training_dataset rtd SET 
        last_5_avg_position = ROUND(calc.avg_pos, 2),
        last_5_win_rate = ROUND(calc.win_rate, 4),
        last_5_place_rate = ROUND(calc.place_rate, 4),
        last_5_races_used = calc.used
    FROM calc WHERE rtd.race_id = calc.race_id AND rtd.horse_location_slug = calc.horse_location_slug;
    
    -- Form recency
    UPDATE race_training_dataset SET form_recency_score = CASE
        WHEN days_since_last_race IS NULL THEN 0.5 WHEN days_since_last_race <= 14 THEN 1.0
        WHEN days_since_last_race <= 28 THEN 0.9 WHEN days_since_last_race <= 60 THEN 0.6 ELSE 0.3
    END;
    
    -- Form momentum (trend of recent results)
    UPDATE race_training_dataset SET form_momentum = CASE
        WHEN last_5_avg_position IS NULL THEN 0
        WHEN last_5_avg_position <= 2 THEN 1.0
        WHEN last_5_avg_position <= 4 THEN 0.7
        WHEN last_5_avg_position <= 6 THEN 0.4
        WHEN last_5_avg_position <= 8 THEN 0.2
        ELSE 0.1
    END;
    
    -- ELO
    UPDATE race_training_dataset SET 
        horse_elo = ROUND(LEAST(GREATEST(1500 + COALESCE(win_percentage, 0) * 5 + LEAST(COALESCE(total_races, 0), 50) * 2, 1200), 2000), 0),
        is_elo_default = (total_races = 0 OR total_races IS NULL);
    
    -- Jockey stats
    WITH js AS (
        SELECT race_id, jockey_location_slug,
            COUNT(*) OVER w as rides, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w as wins,
            SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w as places
        FROM race_training_dataset WHERE final_position IS NOT NULL
        WINDOW w AS (PARTITION BY jockey_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    )
    UPDATE race_training_dataset rtd SET 
        jockey_win_rate = CASE WHEN COALESCE(js.rides, 0) > 0 THEN ROUND(js.wins::numeric / js.rides, 4) ELSE 0 END,
        jockey_place_rate = CASE WHEN COALESCE(js.rides, 0) > 0 THEN ROUND(js.places::numeric / js.rides, 4) ELSE 0 END,
        jockey_total_rides = COALESCE(js.rides, 0)
    FROM js WHERE rtd.race_id = js.race_id AND rtd.jockey_location_slug = js.jockey_location_slug;
    
    -- Trainer stats
    WITH ts AS (
        SELECT race_id, trainer_location_slug,
            COUNT(*) OVER w as runners, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w as wins,
            SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w as places
        FROM race_training_dataset WHERE final_position IS NOT NULL
        WINDOW w AS (PARTITION BY trainer_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    )
    UPDATE race_training_dataset rtd SET 
        trainer_win_rate = CASE WHEN COALESCE(ts.runners, 0) > 0 THEN ROUND(ts.wins::numeric / ts.runners, 4) ELSE 0 END,
        trainer_place_rate = CASE WHEN COALESCE(ts.runners, 0) > 0 THEN ROUND(ts.places::numeric / ts.runners, 4) ELSE 0 END,
        trainer_total_runners = COALESCE(ts.runners, 0)
    FROM ts WHERE rtd.race_id = ts.race_id AND rtd.trainer_location_slug = ts.trainer_location_slug;
    
    -- Cross-region flags
    WITH mr AS (SELECT horse_slug FROM race_training_dataset GROUP BY horse_slug HAVING COUNT(DISTINCT location) > 1)
    UPDATE race_training_dataset rtd SET is_cross_region_horse = TRUE FROM mr WHERE rtd.horse_slug = mr.horse_slug;
    UPDATE race_training_dataset SET is_cross_region_horse = FALSE WHERE is_cross_region_horse IS NULL;
    UPDATE race_training_dataset SET never_placed_flag = (total_races > 0 AND places = 0);
    
    RAISE NOTICE 'Phase 2 complete in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- PHASE 3: ADVANCED FEATURES (Simplified)
    -- ========================================================================
    UPDATE rebuild_progress_v2 SET current_phase = 'Phase 3: Advanced Features', phase_number = 3 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Sectional positions
    UPDATE race_training_dataset rtd SET 
        position_800m = rr.position_800m, position_400m = rr.position_400m
    FROM race_results rr WHERE rtd.race_id = rr.race_id AND rtd.horse_slug = rr.horse_slug;
    
    UPDATE race_training_dataset SET 
        pos_improvement_800_finish = CASE WHEN position_800m IS NOT NULL AND final_position IS NOT NULL THEN position_800m - final_position ELSE NULL END;
    
    -- Running style (simplified)
    UPDATE race_training_dataset SET running_style = CASE
        WHEN position_800m IS NULL THEN 'unknown'
        WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.2 THEN 'leader'
        WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.4 THEN 'on_pace'
        WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.6 THEN 'midfield'
        WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.8 THEN 'off_pace'
        ELSE 'closer'
    END;
    
    -- Class rating
    UPDATE race_training_dataset SET class_rating = CASE
        WHEN race_class ILIKE '%group 1%' THEN 100 WHEN race_class ILIKE '%group 2%' THEN 90
        WHEN race_class ILIKE '%group 3%' THEN 80 WHEN race_class ILIKE '%listed%' THEN 75
        WHEN race_class ILIKE '%benchmark%' THEN 50 WHEN race_class ILIKE '%maiden%' THEN 20
        WHEN track_category = 'Metro' THEN 45 WHEN track_category = 'Provincial' THEN 35 ELSE 25
    END;
    
    -- Field percentiles
    WITH rp AS (
        SELECT race_id, horse_slug,
            PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY COALESCE(horse_elo, 1500)) as elo_pct,
            RANK() OVER (PARTITION BY race_id ORDER BY COALESCE(win_odds, 999)) as odds_rank
        FROM race_training_dataset
    )
    UPDATE race_training_dataset rtd SET 
        elo_percentile_in_race = ROUND(rp.elo_pct::numeric, 4),
        odds_rank_in_race = rp.odds_rank,
        is_favorite = (rp.odds_rank = 1)
    FROM rp WHERE rtd.race_id = rp.race_id AND rtd.horse_slug = rp.horse_slug;
    
    -- Odds features
    UPDATE race_training_dataset SET 
        odds_implied_probability = CASE WHEN win_odds > 0 THEN ROUND(1.0 / win_odds, 4) ELSE NULL END,
        is_longshot = (win_odds > 20);
    
    RAISE NOTICE 'Phase 3 complete in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- PHASE 4: INTERACTIONS & FINALIZE
    -- ========================================================================
    UPDATE rebuild_progress_v2 SET current_phase = 'Phase 4: Interactions', phase_number = 4 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Key interactions
    UPDATE race_training_dataset SET 
        elo_x_jockey_win_rate = ROUND(COALESCE(horse_elo, 1500)::numeric / 1500 * COALESCE(jockey_win_rate, 0), 4),
        jockey_wr_x_trainer_wr = ROUND(COALESCE(jockey_win_rate, 0) * COALESCE(trainer_win_rate, 0), 4),
        form_x_freshness = ROUND(COALESCE(form_momentum, 0) * COALESCE(form_recency_score, 0.5), 4),
        barrier_x_total_runners = ROUND(CASE WHEN COALESCE(total_runners, 1) > 0 THEN COALESCE(barrier, 1)::numeric / total_runners ELSE 0.5 END, 4);
    
    -- Anti-leakage flags
    UPDATE race_training_dataset SET 
        extreme_staleness_flag = (days_since_last_race > 180),
        low_sample_flag = (total_races < 3);
    
    -- Recreate indexes
    CREATE INDEX IF NOT EXISTS idx_rtd_horse_slug ON race_training_dataset(horse_slug);
    CREATE INDEX IF NOT EXISTS idx_rtd_race_date ON race_training_dataset(race_date);
    CREATE INDEX IF NOT EXISTS idx_rtd_track_name ON race_training_dataset(track_name);
    CREATE INDEX IF NOT EXISTS idx_rtd_horse_location_slug ON race_training_dataset(horse_location_slug);
    CREATE INDEX IF NOT EXISTS idx_rtd_race_id ON race_training_dataset(race_id);
    
    RAISE NOTICE 'Phase 4 complete in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- COMPLETE
    -- ========================================================================
    SELECT COUNT(*) INTO v_records FROM race_training_dataset;
    
    UPDATE rebuild_progress_v2 SET 
        status = 'completed',
        completed_at = NOW(),
        current_phase = 'Complete',
        records_processed = v_records
    WHERE run_id = v_run_id;
    
    v_result := json_build_object(
        'status', 'success',
        'run_id', v_run_id,
        'records', v_records,
        'duration_seconds', EXTRACT(EPOCH FROM (NOW() - v_start_time))::integer
    );
    
    RETURN v_result;
    
EXCEPTION WHEN OTHERS THEN
    UPDATE rebuild_progress_v2 SET 
        status = 'failed',
        error_message = SQLERRM,
        completed_at = NOW()
    WHERE run_id = v_run_id;
    
    RETURN json_build_object('status', 'error', 'message', SQLERRM, 'run_id', v_run_id);
END;
$$ LANGUAGE plpgsql;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION run_training_rebuild_v2() TO service_role;
GRANT EXECUTE ON FUNCTION run_training_rebuild_v2() TO authenticated;

DO $$ BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'RPC FUNCTION CREATED: run_training_rebuild_v2()';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Usage: SELECT run_training_rebuild_v2();';
    RAISE NOTICE 'Returns JSON with status, run_id, records, duration_seconds';
END $$;
