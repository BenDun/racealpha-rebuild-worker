-- ============================================================================
-- PHASE 1: BASE REBUILD
-- ============================================================================
-- Consolidates: 01_data_cleanup, 02_track_standardization, 02b_location_resolution,
--               03_entity_tables (partial), 04_core_dataset (base insert only)
-- Est. Time: 5-10 minutes
-- 
-- WHAT THIS DOES:
--   1. Creates backup of existing data
--   2. Truncates and rebuilds from race_results + races
--   3. Sets location-aware slugs
--   4. Sets track properties (direction, category, type)
--   5. Sets race flags (maiden, handicap, age/sex)
--   6. Calculates total_runners per race
-- ============================================================================

-- Configuration
SET statement_timeout = '0';
SET lock_timeout = '0';
SET work_mem = '256MB';

DO $$ BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 1: BASE REBUILD - Started at %', NOW();
    RAISE NOTICE '============================================================';
END $$;

-- ============================================================================
-- STEP 1: CREATE BACKUP
-- ============================================================================
DROP TABLE IF EXISTS _backup_training_rebuild_v2;
CREATE TABLE _backup_training_rebuild_v2 AS SELECT * FROM race_training_dataset;

DO $$
DECLARE
    backup_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO backup_count FROM _backup_training_rebuild_v2;
    RAISE NOTICE '[1.1] Backup created: % records', backup_count;
    RAISE NOTICE '      Rollback: INSERT INTO race_training_dataset SELECT * FROM _backup_training_rebuild_v2;';
END $$;

-- ============================================================================
-- STEP 2: DROP INDEXES FOR FASTER BULK OPERATIONS
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[1.2] Dropping indexes for faster updates...'; END $$;

DROP INDEX IF EXISTS idx_rtd_horse_slug;
DROP INDEX IF EXISTS idx_rtd_race_date;
DROP INDEX IF EXISTS idx_rtd_track_name;
DROP INDEX IF EXISTS idx_rtd_jockey_slug;
DROP INDEX IF EXISTS idx_rtd_trainer_slug;
DROP INDEX IF EXISTS idx_rtd_horse_location_slug;
DROP INDEX IF EXISTS idx_rtd_race_id;
DROP INDEX IF EXISTS idx_rtd_location;

-- Add any missing columns
DO $$ 
BEGIN
    -- distance_range column for grouping races
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'distance_range') THEN
        ALTER TABLE race_training_dataset ADD COLUMN distance_range TEXT;
        RAISE NOTICE 'Added missing column: distance_range';
    END IF;
    
    -- barrier_position column for barrier analysis
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'barrier_position') THEN
        ALTER TABLE race_training_dataset ADD COLUMN barrier_position TEXT;
        RAISE NOTICE 'Added missing column: barrier_position';
    END IF;
    
    -- class_level_numeric for class step up/down tracking
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'class_level_numeric') THEN
        ALTER TABLE race_training_dataset ADD COLUMN class_level_numeric INTEGER;
        RAISE NOTICE 'Added missing column: class_level_numeric';
    END IF;
    
    -- is_stepping_up for class progression
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'is_stepping_up') THEN
        ALTER TABLE race_training_dataset ADD COLUMN is_stepping_up BOOLEAN DEFAULT FALSE;
        RAISE NOTICE 'Added missing column: is_stepping_up';
    END IF;
    
    -- is_stepping_down for class progression
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'is_stepping_down') THEN
        ALTER TABLE race_training_dataset ADD COLUMN is_stepping_down BOOLEAN DEFAULT FALSE;
        RAISE NOTICE 'Added missing column: is_stepping_down';
    END IF;
    
    -- races_at_distance for distance experience tracking
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'races_at_distance') THEN
        ALTER TABLE race_training_dataset ADD COLUMN races_at_distance INTEGER DEFAULT 0;
        RAISE NOTICE 'Added missing column: races_at_distance';
    END IF;
    
    -- races_at_track for track experience tracking
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'races_at_track') THEN
        ALTER TABLE race_training_dataset ADD COLUMN races_at_track INTEGER DEFAULT 0;
        RAISE NOTICE 'Added missing column: races_at_track';
    END IF;
    
    -- historical_avg_improvement for sectional improvement tracking
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'historical_avg_improvement') THEN
        ALTER TABLE race_training_dataset ADD COLUMN historical_avg_improvement NUMERIC(6,2);
        RAISE NOTICE 'Added missing column: historical_avg_improvement';
    END IF;
    
    -- pos_improvement_800_finish for position improvement tracking
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'pos_improvement_800_finish') THEN
        ALTER TABLE race_training_dataset ADD COLUMN pos_improvement_800_finish INTEGER;
        RAISE NOTICE 'Added missing column: pos_improvement_800_finish';
    END IF;
    
    -- avg_late_improvement for late-race performance tracking
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'avg_late_improvement') THEN
        ALTER TABLE race_training_dataset ADD COLUMN avg_late_improvement NUMERIC(6,2);
        RAISE NOTICE 'Added missing column: avg_late_improvement';
    END IF;
END $$;

-- ============================================================================
-- STEP 3: TRUNCATE AND REBUILD BASE DATA
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[1.3] Truncating and rebuilding from source...'; END $$;

-- First fix source data location based on track_name
UPDATE races SET location = CASE
    -- Hong Kong (ONLY these two tracks)
    WHEN track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
    -- UK tracks (Hamilton = UK 402 races vs AU 40)
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

DO $$ BEGIN RAISE NOTICE '[1.3a] Fixed location data in races table'; END $$;

-- Fix scientific notation in horse_number (e.g., "13e" should be "13")
DO $$
DECLARE
    fixed_count INTEGER;
BEGIN
    UPDATE race_results 
    SET horse_number = REGEXP_REPLACE(horse_number, '[eE]\+?[0-9]*$', '', 'g')
    WHERE horse_number ~ '[eE]';
    
    GET DIAGNOSTICS fixed_count = ROW_COUNT;
    RAISE NOTICE '[1.3b] Fixed scientific notation in horse_number: % records', fixed_count;
END $$;

TRUNCATE TABLE race_training_dataset;

INSERT INTO race_training_dataset (
    race_id, race_date, race_number, track_name, race_distance, race_class,
    track_condition, track_weather, horse_name, horse_slug, barrier, weight,
    final_position, margin, jockey, jockey_slug, trainer, trainer_slug, win_odds,
    location, horse_location_slug, jockey_location_slug, trainer_location_slug,
    created_at, updated_at
)
SELECT 
    rr.race_id,
    r.race_date,
    r.race_number,
    -- Standardize track names
    CASE 
        WHEN r.track_name ILIKE '%royal randwick%' THEN 'Randwick'
        WHEN r.track_name ILIKE '%randwick%' THEN 'Randwick'
        WHEN r.track_name ILIKE '%flemington%racecourse%' THEN 'Flemington'
        WHEN r.track_name ILIKE '%moonee valley%' THEN 'Moonee Valley'
        WHEN r.track_name ILIKE '%caulfield%racecourse%' THEN 'Caulfield'
        ELSE COALESCE(r.track_name, 'Unknown')
    END as track_name,
    r.race_distance,
    r.race_class,
    r.track_condition,
    r.track_weather,
    -- Fix scientific notation in horse names
    REPLACE(rr.horse_name, 'E+', 'E') as horse_name,
    rr.horse_slug,
    rr.barrier,
    rr.original_weight,
    rr.position,
    rr.margin,
    rr.jockey,
    rr.jockey_slug,
    rr.trainer,
    rr.trainer_slug,
    rr.win_odds,
    -- Location-aware fields
    COALESCE(r.location, 'AU') as location,
    rr.horse_slug || '-' || COALESCE(r.location, 'AU') as horse_location_slug,
    rr.jockey_slug || '-' || COALESCE(r.location, 'AU') as jockey_location_slug,
    rr.trainer_slug || '-' || COALESCE(r.location, 'AU') as trainer_location_slug,
    NOW(),
    NOW()
FROM race_results rr
JOIN races r ON rr.race_id = r.race_id
WHERE rr.horse_slug IS NOT NULL 
  AND rr.position IS NOT NULL
  AND rr.position < 50;  -- Exclude DNF placeholders

DO $$
DECLARE
    cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM race_training_dataset;
    RAISE NOTICE '[1.3] Inserted % records from source', cnt;
END $$;

-- ============================================================================
-- STEP 4: SET ALL TRACK & RACE PROPERTIES (Single UPDATE)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[1.4] Setting track and race properties...'; END $$;

UPDATE race_training_dataset
SET 
    -- Track direction (clockwise tracks are mainly NSW, QLD, HK)
    track_direction = CASE
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
    
    -- Track category (Metro/Provincial/Country)
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
            'Devonport Synthetic', 'Longford',
            -- WA Provincial
            'Bunbury', 'Pinjarra', 'Northam'
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
            'Bordertown', 'Kingscote', 'Oakbank', 'Naracoorte', 'Port Augusta', 'Clare'
        ) THEN 'Country'
        -- =====================================================================
        -- INTERNATIONAL TRACKS - By country
        -- =====================================================================
        -- UK Major
        WHEN track_name IN (
            'Royal Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood', 
            'Cheltenham', 'Aintree', 'Newbury', 'Sandown Park', 'Doncaster', 'Haydock'
        ) THEN 'Metro'
        -- UK Provincial
        WHEN track_name IN (
            'Kempton', 'Wolverhampton', 'Southwell', 'Lingfield', 'Chester', 'Hamilton',
            'Musselburgh', 'Nottingham', 'Pontefract', 'Sedgefield', 
            'Plumpton', 'Yarmouth', 'Down Royal'
        ) AND location = 'UK' THEN 'Provincial'
        -- Ireland
        WHEN track_name IN ('Curragh', 'Leopardstown') THEN 'Metro'
        WHEN track_name IN (
            'Punchestown', 'Fairyhouse', 'Naas', 'Gowran Park', 'Galway', 'Cork', 
            'Killarney', 'Dundalk', 'Navan', 'Thurles'
        ) THEN 'Provincial'
        -- France
        WHEN track_name IN ('ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud') THEN 'Metro'
        WHEN track_name IN (
            'Compiegne', 'Fontainebleau', 'Lyon-Parilly', 'Vichy', 
            'Clairefontaine', 'Bordeaux Le Bouscat', 'Nantes'
        ) THEN 'Provincial'
        -- Japan
        WHEN track_name IN ('Tokyo', 'Kyoto', 'Hanshin', 'Nakayama') THEN 'Metro'
        WHEN track_name IN ('Chukyo', 'Niigata', 'Fukushima', 'Kokura', 'Sapporo') THEN 'Provincial'
        -- USA
        WHEN track_name IN ('Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream') THEN 'Metro'
        WHEN track_name IN ('Kentucky Downs', 'Tampa Bay Downs') THEN 'Provincial'
        -- UAE
        WHEN track_name = 'Meydan' THEN 'Metro'
        -- Default: Country for anything else
        ELSE 'Country'
    END,
    
    -- Track type (turf/synthetic/dirt)
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
    
    -- Race class flags
    is_maiden_race = (race_class ILIKE '%maiden%' OR race_class ILIKE '%mdn%'),
    is_handicap_race = (race_class ILIKE '%handicap%' OR race_class ILIKE '%hcp%'),
    
    -- Age restriction
    age_restriction = CASE
        WHEN race_class ~* '2[- ]?y(ear)?[- ]?o(ld)?' THEN '2yo'
        WHEN race_class ~* '3[- ]?y(ear)?[- ]?o(ld)?' THEN '3yo'
        WHEN race_class ~* '4[- ]?y(ear)?[- ]?o(ld)?' THEN '4yo+'
        ELSE 'open'
    END,
    
    -- Sex restriction
    sex_restriction = CASE
        WHEN race_class ILIKE '%fillies%' AND race_class ILIKE '%mares%' THEN 'fillies_mares'
        WHEN race_class ILIKE '%fillies%' THEN 'fillies'
        WHEN race_class ILIKE '%mares%' THEN 'mares'
        ELSE 'open'
    END,
    
    -- Distance range for grouping
    distance_range = CASE 
        WHEN race_distance < 1200 THEN 'sprint'
        WHEN race_distance < 1600 THEN 'mile'
        WHEN race_distance < 2000 THEN 'middle'
        ELSE 'staying'
    END,
    
    updated_at = NOW();

DO $$ BEGIN RAISE NOTICE '[1.4] Track and race properties set'; END $$;

-- ============================================================================
-- STEP 5: CALCULATE TOTAL RUNNERS PER RACE
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[1.5] Calculating total runners per race...'; END $$;

WITH race_field_size AS (
    SELECT race_id, COUNT(*) as field_size
    FROM race_training_dataset
    WHERE final_position IS NOT NULL
    GROUP BY race_id
)
UPDATE race_training_dataset rtd
SET total_runners = rfs.field_size
FROM race_field_size rfs
WHERE rtd.race_id = rfs.race_id;

-- ============================================================================
-- STEP 6: SET BARRIER POSITION (inner/middle/outer)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[1.6] Setting barrier positions...'; END $$;

UPDATE race_training_dataset
SET barrier_position = CASE
    WHEN total_runners IS NULL OR total_runners = 0 THEN 'unknown'
    WHEN barrier::float / total_runners <= 0.33 THEN 'inner'
    WHEN barrier::float / total_runners <= 0.66 THEN 'middle'
    ELSE 'outer'
END
WHERE barrier IS NOT NULL;

-- ============================================================================
-- COMPLETION
-- ============================================================================
DO $$
DECLARE
    final_count INTEGER;
    track_cats RECORD;
BEGIN
    SELECT COUNT(*) INTO final_count FROM race_training_dataset;
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 1 COMPLETE - % records', final_count;
    RAISE NOTICE '============================================================';
    
    -- Quick validation
    FOR track_cats IN 
        SELECT track_category, COUNT(*) as cnt 
        FROM race_training_dataset 
        GROUP BY track_category 
        ORDER BY cnt DESC
    LOOP
        RAISE NOTICE '  %: %', track_cats.track_category, track_cats.cnt;
    END LOOP;
    
    RAISE NOTICE '';
    RAISE NOTICE 'Next: Run 02_career_form_stats.sql';
END $$;
