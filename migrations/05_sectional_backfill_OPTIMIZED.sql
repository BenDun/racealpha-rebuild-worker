-- ============================================================================
-- SCRIPT 05: SECTIONAL DATA BACKFILL (OPTIMIZED)
-- ============================================================================
-- Run Order: 5 of 7 (MUST run BEFORE 06_current_form_views.sql)
-- Dependencies: Scripts 01-04, race_results_sectional_times table
-- Est. Time: 3-5 minutes (down from 10-15 minutes)
-- 
-- OPTIMIZATIONS:
--   1. Create indexes FIRST before any updates
--   2. Consolidate ALL distance mappings into SINGLE CTE-based updates
--   3. Use CASE expressions instead of 48 separate UPDATE statements
--   4. Process AU and HK in TWO updates (pos_800m, pos_400m) instead of 48
--   5. Batch sync to race_training_dataset
-- ============================================================================

SET statement_timeout = '0';
SET lock_timeout = '0';
SET work_mem = '512MB';

DO $$ BEGIN RAISE NOTICE '[05] Starting OPTIMIZED sectional backfill at %', NOW(); END $$;

-- ============================================================================
-- STEP 0: CREATE INDEXES FOR JOIN PERFORMANCE
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[05.0] Creating indexes for join performance...'; END $$;

CREATE INDEX IF NOT EXISTS idx_rr_race_id ON race_results(race_id);
CREATE INDEX IF NOT EXISTS idx_rr_horse_name_lower ON race_results(LOWER(TRIM(horse_name)));
CREATE INDEX IF NOT EXISTS idx_st_race_id ON race_results_sectional_times(race_id);
CREATE INDEX IF NOT EXISTS idx_st_horse_name_lower ON race_results_sectional_times(LOWER(TRIM(horse_name)));
CREATE INDEX IF NOT EXISTS idx_races_track_dist ON races(race_id, race_distance, track_name);

DO $$ BEGIN RAISE NOTICE '[05.0] ✓ Indexes created'; END $$;

-- ============================================================================
-- STEP 1: FIX HTML-ENCODED APOSTROPHES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[05.1] Fixing HTML-encoded apostrophes...'; END $$;

UPDATE race_results_sectional_times
SET horse_name = REPLACE(horse_name, '&#39;', '''')
WHERE horse_name LIKE '%&#39;%';

DO $$ BEGIN RAISE NOTICE '[05.1] ✓ Apostrophes fixed'; END $$;

-- ============================================================================
-- STEP 2: PRE-BACKFILL STATS
-- ============================================================================
DO $$
DECLARE
    total_rr INTEGER;
    has_800m INTEGER;
    has_400m INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_rr FROM race_results;
    SELECT COUNT(*) INTO has_800m FROM race_results WHERE position_800m IS NOT NULL AND position_800m < 50;
    SELECT COUNT(*) INTO has_400m FROM race_results WHERE position_400m IS NOT NULL AND position_400m < 50;
    
    RAISE NOTICE '[05.2] BEFORE backfill:';
    RAISE NOTICE '     Total race_results: %', total_rr;
    RAISE NOTICE '     With position_800m: % (%.1f%%)', has_800m, (100.0 * has_800m / NULLIF(total_rr, 0));
    RAISE NOTICE '     With position_400m: % (%.1f%%)', has_400m, (100.0 * has_400m / NULLIF(total_rr, 0));
END $$;

-- ============================================================================
-- STEP 3: CONSOLIDATED POSITION_800M BACKFILL (AU + HK in ONE query)
-- ============================================================================
-- This replaces ~24 separate UPDATE statements with ONE
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[05.3] Backfilling position_800m (consolidated)...'; END $$;

WITH source_mapping AS (
    SELECT 
        rr.race_id,
        rr.horse_name,
        r.race_distance,
        r.track_name,
        -- Is this Hong Kong?
        (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%') AS is_hk,
        st.position_200m, st.position_400m, st.position_600m, st.position_800m,
        st.position_1000m, st.position_1200m, st.position_1400m, st.position_1600m,
        st.position_1800m, st.position_2000m, st.position_2200m, st.position_2400m
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    JOIN race_results_sectional_times st 
        ON rr.race_id = st.race_id 
        AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
    WHERE rr.position_800m IS NULL
),
calculated_800m AS (
    SELECT 
        race_id,
        horse_name,
        CASE
            -- === AUSTRALIAN TRACKS (200m intervals) ===
            WHEN NOT is_hk THEN CASE race_distance
                WHEN 1000 THEN position_200m    -- 800 remaining = 200m traveled
                WHEN 1200 THEN position_400m    -- 800 remaining = 400m traveled
                WHEN 1400 THEN position_600m    -- 800 remaining = 600m traveled
                WHEN 1600 THEN position_800m    -- 800 remaining = 800m traveled
                WHEN 1650 THEN position_800m    -- closest checkpoint
                WHEN 1800 THEN position_1000m   -- 800 remaining = 1000m traveled
                WHEN 2000 THEN position_1200m   -- 800 remaining = 1200m traveled
                WHEN 2200 THEN position_1400m   -- 800 remaining = 1400m traveled
                WHEN 2400 THEN position_1600m   -- 800 remaining = 1600m traveled
                WHEN 2600 THEN position_1800m
                WHEN 2800 THEN position_2000m
                WHEN 3000 THEN position_2200m
                WHEN 3200 THEN position_2400m
                ELSE NULL
            END
            -- === HONG KONG TRACKS (400m intervals) ===
            ELSE CASE
                WHEN race_distance IN (1000, 1200) THEN position_400m
                WHEN race_distance IN (1400, 1600, 1650) THEN position_800m
                WHEN race_distance IN (1800, 2000, 2200) THEN position_1200m
                WHEN race_distance = 2400 THEN position_1600m
                ELSE NULL
            END
        END AS new_pos_800m
    FROM source_mapping
)
UPDATE race_results rr
SET position_800m = c.new_pos_800m
FROM calculated_800m c
WHERE rr.race_id = c.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(c.horse_name))
  AND c.new_pos_800m IS NOT NULL
  AND c.new_pos_800m < 50;

DO $$ BEGIN RAISE NOTICE '[05.3] ✓ position_800m backfill complete'; END $$;

-- ============================================================================
-- STEP 4: CONSOLIDATED POSITION_400M BACKFILL (AU + HK in ONE query)
-- ============================================================================
-- This replaces ~24 separate UPDATE statements with ONE
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[05.4] Backfilling position_400m (consolidated)...'; END $$;

WITH source_mapping AS (
    SELECT 
        rr.race_id,
        rr.horse_name,
        r.race_distance,
        r.track_name,
        (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%') AS is_hk,
        st.position_600m, st.position_800m, st.position_1000m, st.position_1200m,
        st.position_1400m, st.position_1600m, st.position_1800m, st.position_2000m,
        st.position_2200m, st.position_2400m, st.position_2600m, st.position_2800m
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    JOIN race_results_sectional_times st 
        ON rr.race_id = st.race_id 
        AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
    WHERE rr.position_400m IS NULL
),
calculated_400m AS (
    SELECT 
        race_id,
        horse_name,
        CASE
            -- === AUSTRALIAN TRACKS ===
            WHEN NOT is_hk THEN CASE race_distance
                WHEN 1000 THEN position_600m    -- 400 remaining = 600m traveled
                WHEN 1200 THEN position_800m    -- 400 remaining = 800m traveled
                WHEN 1400 THEN position_1000m   -- 400 remaining = 1000m traveled
                WHEN 1600 THEN position_1200m   -- 400 remaining = 1200m traveled
                WHEN 1650 THEN position_1200m   -- closest checkpoint
                WHEN 1800 THEN position_1400m   -- 400 remaining = 1400m traveled
                WHEN 2000 THEN position_1600m   -- 400 remaining = 1600m traveled
                WHEN 2200 THEN position_1800m
                WHEN 2400 THEN position_2000m
                WHEN 2600 THEN position_2200m
                WHEN 2800 THEN position_2400m
                WHEN 3000 THEN position_2600m
                WHEN 3200 THEN position_2800m
                ELSE NULL
            END
            -- === HONG KONG TRACKS ===
            ELSE CASE
                WHEN race_distance IN (1000, 1200) THEN position_800m
                WHEN race_distance IN (1400, 1600, 1650) THEN position_1200m
                WHEN race_distance IN (1800, 2000) THEN position_1600m
                WHEN race_distance IN (2200, 2400) THEN position_2000m
                ELSE NULL
            END
        END AS new_pos_400m
    FROM source_mapping
)
UPDATE race_results rr
SET position_400m = c.new_pos_400m
FROM calculated_400m c
WHERE rr.race_id = c.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(c.horse_name))
  AND c.new_pos_400m IS NOT NULL
  AND c.new_pos_400m < 50;

DO $$ BEGIN RAISE NOTICE '[05.4] ✓ position_400m backfill complete'; END $$;

-- ============================================================================
-- STEP 5: HK HORSE NUMBER FALLBACK (for late replacements)
-- ============================================================================
-- Some HK races have late replacements where name changed but number stayed
-- Consolidated into TWO updates (pos_800m + pos_400m)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[05.5] HK horse_number fallback (consolidated)...'; END $$;

-- Position 800m by horse number
WITH hk_number_mapping AS (
    SELECT 
        rr.race_id,
        rr.horse_number,
        r.race_distance,
        st.position_400m, st.position_800m, st.position_1200m, st.position_1600m
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    JOIN race_results_sectional_times st 
        ON rr.race_id = st.race_id 
        AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
    WHERE (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
      AND rr.position_800m IS NULL
      AND rr.horse_number ~ '[0-9]'
),
calc_hk_800m AS (
    SELECT 
        race_id,
        horse_number,
        CASE
            WHEN race_distance IN (1000, 1200) THEN position_400m
            WHEN race_distance IN (1400, 1600, 1650) THEN position_800m
            WHEN race_distance IN (1800, 2000, 2200) THEN position_1200m
            WHEN race_distance = 2400 THEN position_1600m
        END AS new_pos_800m
    FROM hk_number_mapping
)
UPDATE race_results rr
SET position_800m = c.new_pos_800m
FROM calc_hk_800m c
WHERE rr.race_id = c.race_id
  AND rr.horse_number = c.horse_number
  AND c.new_pos_800m IS NOT NULL
  AND c.new_pos_800m < 50;

-- Position 400m by horse number
WITH hk_number_mapping AS (
    SELECT 
        rr.race_id,
        rr.horse_number,
        r.race_distance,
        st.position_800m, st.position_1200m, st.position_1600m, st.position_2000m
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    JOIN race_results_sectional_times st 
        ON rr.race_id = st.race_id 
        AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
    WHERE (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
      AND rr.position_400m IS NULL
      AND rr.horse_number ~ '[0-9]'
),
calc_hk_400m AS (
    SELECT 
        race_id,
        horse_number,
        CASE
            WHEN race_distance IN (1000, 1200) THEN position_800m
            WHEN race_distance IN (1400, 1600, 1650) THEN position_1200m
            WHEN race_distance IN (1800, 2000) THEN position_1600m
            WHEN race_distance IN (2200, 2400) THEN position_2000m
        END AS new_pos_400m
    FROM hk_number_mapping
)
UPDATE race_results rr
SET position_400m = c.new_pos_400m
FROM calc_hk_400m c
WHERE rr.race_id = c.race_id
  AND rr.horse_number = c.horse_number
  AND c.new_pos_400m IS NOT NULL
  AND c.new_pos_400m < 50;

DO $$ BEGIN RAISE NOTICE '[05.5] ✓ HK horse_number fallback complete'; END $$;

-- ============================================================================
-- STEP 6: SYNC TO RACE_TRAINING_DATASET (consolidated)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[05.6] Syncing to race_training_dataset...'; END $$;

-- Single consolidated update for all position columns
UPDATE race_training_dataset rtd
SET 
    position_at_800 = COALESCE(rtd.position_at_800, rr.position_800m),
    position_at_400 = COALESCE(rtd.position_at_400, rr.position_400m),
    position_800m = COALESCE(rtd.position_800m, rr.position_800m),
    position_400m = COALESCE(rtd.position_400m, rr.position_400m)
FROM race_results rr
WHERE rtd.race_id = rr.race_id
  AND rtd.horse_slug = rr.horse_slug
  AND (rr.position_800m IS NOT NULL OR rr.position_400m IS NOT NULL);

DO $$ BEGIN RAISE NOTICE '[05.6] ✓ Position sync complete'; END $$;

-- ============================================================================
-- STEP 7: RECALCULATE DERIVED COLUMNS
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[05.7] Recalculating derived columns...'; END $$;

-- pos_improvement_800_finish and running_style in ONE update
UPDATE race_training_dataset
SET 
    pos_improvement_800_finish = CASE 
        WHEN position_800m IS NOT NULL AND final_position IS NOT NULL 
        THEN position_800m - final_position 
        ELSE pos_improvement_800_finish
    END,
    running_style = CASE
        WHEN position_800m IS NULL THEN COALESCE(running_style, 'unknown')
        WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.2 THEN 'leader'
        WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.4 THEN 'on_pace'
        WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.6 THEN 'midfield'
        WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.8 THEN 'off_pace'
        ELSE 'closer'
    END
WHERE position_800m IS NOT NULL;

DO $$ BEGIN RAISE NOTICE '[05.7] ✓ Derived columns updated'; END $$;

-- ============================================================================
-- STEP 8: FINAL VALIDATION
-- ============================================================================
DO $$
DECLARE
    total_rr INTEGER;
    has_800m INTEGER;
    has_400m INTEGER;
    hk_total INTEGER;
    hk_has_800m INTEGER;
    hk_has_400m INTEGER;
    rtd_has_800m INTEGER;
    rtd_total INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_rr FROM race_results;
    SELECT COUNT(*) INTO has_800m FROM race_results WHERE position_800m IS NOT NULL AND position_800m < 50;
    SELECT COUNT(*) INTO has_400m FROM race_results WHERE position_400m IS NOT NULL AND position_400m < 50;
    
    SELECT COUNT(*), 
           SUM(CASE WHEN rr.position_800m IS NOT NULL THEN 1 ELSE 0 END),
           SUM(CASE WHEN rr.position_400m IS NOT NULL THEN 1 ELSE 0 END)
    INTO hk_total, hk_has_800m, hk_has_400m
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    WHERE r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%';
    
    SELECT COUNT(*), SUM(CASE WHEN position_800m IS NOT NULL THEN 1 ELSE 0 END)
    INTO rtd_total, rtd_has_800m
    FROM race_training_dataset;
    
    RAISE NOTICE '[05] ============================================';
    RAISE NOTICE '[05] SECTIONAL BACKFILL COMPLETE';
    RAISE NOTICE '[05] ============================================';
    RAISE NOTICE '[05] race_results AFTER backfill:';
    RAISE NOTICE '     Total: %', total_rr;
    RAISE NOTICE '     With position_800m: % (%.1f%%)', has_800m, (100.0 * has_800m / NULLIF(total_rr, 0));
    RAISE NOTICE '     With position_400m: % (%.1f%%)', has_400m, (100.0 * has_400m / NULLIF(total_rr, 0));
    RAISE NOTICE '';
    RAISE NOTICE '     HK race_results: % total', hk_total;
    RAISE NOTICE '     HK with position_800m: % (%.1f%%)', hk_has_800m, (100.0 * hk_has_800m / NULLIF(hk_total, 0));
    RAISE NOTICE '     HK with position_400m: % (%.1f%%)', hk_has_400m, (100.0 * hk_has_400m / NULLIF(hk_total, 0));
    RAISE NOTICE '';
    RAISE NOTICE '     race_training_dataset: % total', rtd_total;
    RAISE NOTICE '     RTD with position_800m: % (%.1f%%)', rtd_has_800m, (100.0 * rtd_has_800m / NULLIF(rtd_total, 0));
    RAISE NOTICE '[05] ============================================';
END $$;

-- Running style distribution check
SELECT 
    running_style,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM race_training_dataset
WHERE running_style IS NOT NULL
GROUP BY running_style
ORDER BY count DESC;
