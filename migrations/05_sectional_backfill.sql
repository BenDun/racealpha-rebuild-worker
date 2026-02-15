-- ============================================================================
-- SCRIPT 05: SECTIONAL DATA BACKFILL (CONSOLIDATED AU + HK)
-- ============================================================================
-- Run Order: 5 of 7 (MUST run BEFORE 06_current_form_views.sql)
-- Dependencies: Scripts 01-04, race_results_sectional_times table
-- Est. Time: 10-15 minutes
-- 
-- PURPOSE:
--   - BACKFILL missing position_400m and position_800m in race_results
--   - THEN sync to race_training_dataset
--   - Handles BOTH Australian AND Hong Kong checkpoint systems
--
-- AUSTRALIAN TRACKS: Use 200m-interval checkpoints
--   Distance | pos_800m remaining | pos_400m remaining
--   ---------+-------------------+-------------------
--   1000m    | position_200m     | position_600m
--   1200m    | position_400m     | position_800m
--   1400m    | position_600m     | position_1000m
--   1600m    | position_800m     | position_1200m
--   1800m    | position_1000m    | position_1400m
--   2000m    | position_1200m    | position_1600m
--   2400m    | position_1600m    | position_2000m
--
-- HONG KONG TRACKS (Sha Tin, Happy Valley): Use 400m-interval checkpoints
--   HK checkpoints: 400m, 800m, 1200m, 1600m, 2000m (no 200m, 600m, etc)
--   Distance | pos_800m remaining | pos_400m remaining
--   ---------+-------------------+-------------------
--   1000m    | position_400m*    | position_800m*      (* closest available)
--   1200m    | position_400m ✓   | position_800m ✓
--   1400m    | position_800m*    | position_1200m*
--   1600m    | position_800m ✓   | position_1200m ✓
--   1800m    | position_1200m*   | position_1600m*
--   2000m    | position_1200m ✓  | position_1600m ✓
--   2400m    | position_1600m ✓  | position_2000m ✓
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '[06] Starting sectional data backfill at %', NOW();
END $$;

-- ============================================================================
-- STEP 0: FIX HTML-ENCODED APOSTROPHES IN SECTIONAL TIMES
-- ============================================================================
UPDATE race_results_sectional_times
SET horse_name = REPLACE(horse_name, '&#39;', '''')
WHERE horse_name LIKE '%&#39;%';

-- ============================================================================
-- STEP 1: CHECK SOURCE DATA BEFORE BACKFILL
-- ============================================================================
DO $$
DECLARE
    total_rr INTEGER;
    has_800m INTEGER;
    has_400m INTEGER;
    sectional_records INTEGER;
    hk_total INTEGER;
    hk_missing_800m INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_rr FROM race_results;
    SELECT COUNT(*) INTO has_800m FROM race_results WHERE position_800m IS NOT NULL AND position_800m < 50;
    SELECT COUNT(*) INTO has_400m FROM race_results WHERE position_400m IS NOT NULL AND position_400m < 50;
    SELECT COUNT(*) INTO sectional_records FROM race_results_sectional_times;
    
    SELECT COUNT(*), SUM(CASE WHEN rr.position_800m IS NULL THEN 1 ELSE 0 END)
    INTO hk_total, hk_missing_800m
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    WHERE r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%';
    
    RAISE NOTICE '[06] BEFORE backfill:';
    RAISE NOTICE '     Total race_results: %', total_rr;
    RAISE NOTICE '     With position_800m: % (%.1f%%)', has_800m, (100.0 * has_800m / NULLIF(total_rr, 0));
    RAISE NOTICE '     With position_400m: % (%.1f%%)', has_400m, (100.0 * has_400m / NULLIF(total_rr, 0));
    RAISE NOTICE '     Sectional source records: %', sectional_records;
    RAISE NOTICE '     HK total / missing 800m: % / %', hk_total, hk_missing_800m;
END $$;

-- ============================================================================
-- SECTION A: AUSTRALIAN TRACKS BACKFILL
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '[06] Section A: Australian tracks sectional backfill...';
END $$;

-- POSITION_800M FOR AUSTRALIAN TRACKS (800m remaining = distance-800 from start)

-- 1000m: 800m remaining = 200m from start
UPDATE race_results rr
SET position_800m = st.position_200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1000
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_200m IS NOT NULL AND st.position_200m < 50;

-- 1200m: 800m remaining = 400m from start
UPDATE race_results rr
SET position_800m = st.position_400m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1200
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_400m IS NOT NULL AND st.position_400m < 50;

-- 1400m: 800m remaining = 600m from start
UPDATE race_results rr
SET position_800m = st.position_600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1400
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_600m IS NOT NULL AND st.position_600m < 50;

-- 1600m: 800m remaining = 800m from start
UPDATE race_results rr
SET position_800m = st.position_800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1600
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_800m IS NOT NULL AND st.position_800m < 50;

-- 1650m: 800m remaining = 850m from start (use position_800m as closest)
UPDATE race_results rr
SET position_800m = st.position_800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1650
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_800m IS NOT NULL AND st.position_800m < 50;

-- 1800m: 800m remaining = 1000m from start
UPDATE race_results rr
SET position_800m = st.position_1000m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1800
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_1000m IS NOT NULL AND st.position_1000m < 50;

-- 2000m: 800m remaining = 1200m from start
UPDATE race_results rr
SET position_800m = st.position_1200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2000
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_1200m IS NOT NULL AND st.position_1200m < 50;

-- 2200m: 800m remaining = 1400m from start
UPDATE race_results rr
SET position_800m = st.position_1400m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2200
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_1400m IS NOT NULL AND st.position_1400m < 50;

-- 2400m: 800m remaining = 1600m from start
UPDATE race_results rr
SET position_800m = st.position_1600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2400
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_1600m IS NOT NULL AND st.position_1600m < 50;

-- 2600m: 800m remaining = 1800m from start
UPDATE race_results rr
SET position_800m = st.position_1800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2600
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_1800m IS NOT NULL AND st.position_1800m < 50;

-- 2800m, 3000m, 3200m follow same pattern
UPDATE race_results rr SET position_800m = st.position_2000m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2800
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL AND st.position_2000m IS NOT NULL AND st.position_2000m < 50;

UPDATE race_results rr SET position_800m = st.position_2200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 3000
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL AND st.position_2200m IS NOT NULL AND st.position_2200m < 50;

UPDATE race_results rr SET position_800m = st.position_2400m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 3200
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL AND st.position_2400m IS NOT NULL AND st.position_2400m < 50;

-- POSITION_400M FOR AUSTRALIAN TRACKS (400m remaining = distance-400 from start)

-- 1000m: 400m remaining = 600m from start
UPDATE race_results rr
SET position_400m = st.position_600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1000
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_600m IS NOT NULL AND st.position_600m < 50;

-- 1200m: 400m remaining = 800m from start
UPDATE race_results rr
SET position_400m = st.position_800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1200
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_800m IS NOT NULL AND st.position_800m < 50;

-- 1400m: 400m remaining = 1000m from start
UPDATE race_results rr
SET position_400m = st.position_1000m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1400
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_1000m IS NOT NULL AND st.position_1000m < 50;

-- 1600m: 400m remaining = 1200m from start
UPDATE race_results rr
SET position_400m = st.position_1200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1600
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_1200m IS NOT NULL AND st.position_1200m < 50;

-- 1650m: 400m remaining = 1250m from start (use position_1200m as closest)
UPDATE race_results rr
SET position_400m = st.position_1200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1650
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_1200m IS NOT NULL AND st.position_1200m < 50;

-- 1800m: 400m remaining = 1400m from start
UPDATE race_results rr
SET position_400m = st.position_1400m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 1800
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_1400m IS NOT NULL AND st.position_1400m < 50;

-- 2000m: 400m remaining = 1600m from start
UPDATE race_results rr
SET position_400m = st.position_1600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2000
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_1600m IS NOT NULL AND st.position_1600m < 50;

-- 2200m: 400m remaining = 1800m from start
UPDATE race_results rr
SET position_400m = st.position_1800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2200
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_1800m IS NOT NULL AND st.position_1800m < 50;

-- 2400m: 400m remaining = 2000m from start
UPDATE race_results rr
SET position_400m = st.position_2000m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2400
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_2000m IS NOT NULL AND st.position_2000m < 50;

-- 2600m, 2800m, 3000m, 3200m
UPDATE race_results rr SET position_400m = st.position_2200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2600
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL AND st.position_2200m IS NOT NULL AND st.position_2200m < 50;

UPDATE race_results rr SET position_400m = st.position_2400m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2800
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL AND st.position_2400m IS NOT NULL AND st.position_2400m < 50;

UPDATE race_results rr SET position_400m = st.position_2600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 3000
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL AND st.position_2600m IS NOT NULL AND st.position_2600m < 50;

UPDATE race_results rr SET position_400m = st.position_2800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 3200
  AND NOT (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL AND st.position_2800m IS NOT NULL AND st.position_2800m < 50;

DO $$
BEGIN
    RAISE NOTICE '[06] ✓ Australian tracks sectional backfill complete';
END $$;

-- ============================================================================
-- SECTION B: HONG KONG TRACKS BACKFILL
-- ============================================================================
-- HK uses 400m-interval checkpoints (400m, 800m, 1200m, 1600m, 2000m)
-- Different mapping than Australian tracks
DO $$
BEGIN
    RAISE NOTICE '[06] Section B: Hong Kong tracks sectional backfill...';
END $$;

-- POSITION_800M FOR HK TRACKS

-- 1000m, 1200m: 800m remaining → use position_400m (closest to 400m traveled)
UPDATE race_results rr
SET position_800m = st.position_400m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance IN (1000, 1200)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_400m IS NOT NULL AND st.position_400m < 50;

-- 1400m, 1600m, 1650m: 800m remaining → use position_800m
UPDATE race_results rr
SET position_800m = st.position_800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance IN (1400, 1600, 1650)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_800m IS NOT NULL AND st.position_800m < 50;

-- 1800m, 2000m, 2200m: 800m remaining → use position_1200m
UPDATE race_results rr
SET position_800m = st.position_1200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance IN (1800, 2000, 2200)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_1200m IS NOT NULL AND st.position_1200m < 50;

-- 2400m: 800m remaining = 1600m from start → position_1600m
UPDATE race_results rr
SET position_800m = st.position_1600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance = 2400
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL
  AND st.position_1600m IS NOT NULL AND st.position_1600m < 50;

-- POSITION_400M FOR HK TRACKS

-- 1000m, 1200m: 400m remaining → use position_800m
UPDATE race_results rr
SET position_400m = st.position_800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance IN (1000, 1200)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_800m IS NOT NULL AND st.position_800m < 50;

-- 1400m, 1600m, 1650m: 400m remaining → use position_1200m
UPDATE race_results rr
SET position_400m = st.position_1200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance IN (1400, 1600, 1650)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_1200m IS NOT NULL AND st.position_1200m < 50;

-- 1800m, 2000m: 400m remaining → use position_1600m
UPDATE race_results rr
SET position_400m = st.position_1600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance IN (1800, 2000)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_1600m IS NOT NULL AND st.position_1600m < 50;

-- 2200m, 2400m: 400m remaining → use position_2000m
UPDATE race_results rr
SET position_400m = st.position_2000m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND LOWER(TRIM(rr.horse_name)) = LOWER(TRIM(st.horse_name))
  AND r.race_distance IN (2200, 2400)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL
  AND st.position_2000m IS NOT NULL AND st.position_2000m < 50;

DO $$
BEGIN
    RAISE NOTICE '[06] ✓ Hong Kong tracks sectional backfill (by horse_name) complete';
END $$;

-- ============================================================================
-- SECTION C: HK BACKFILL BY HORSE_NUMBER (for late replacements)
-- ============================================================================
-- Some horses are late replacements where the name changed but saddle cloth
-- number stayed the same. Match by horse_number to catch these.
DO $$
BEGIN
    RAISE NOTICE '[06] Section C: HK backfill by horse_number for late replacements...';
END $$;

-- position_800m by horse_number
UPDATE race_results rr SET position_800m = st.position_400m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
  AND r.race_distance IN (1000, 1200)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL AND rr.horse_number ~ '[0-9]'
  AND st.position_400m IS NOT NULL AND st.position_400m < 50;

UPDATE race_results rr SET position_800m = st.position_800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
  AND r.race_distance IN (1400, 1600, 1650)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL AND rr.horse_number ~ '[0-9]'
  AND st.position_800m IS NOT NULL AND st.position_800m < 50;

UPDATE race_results rr SET position_800m = st.position_1200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
  AND r.race_distance IN (1800, 2000, 2200)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL AND rr.horse_number ~ '[0-9]'
  AND st.position_1200m IS NOT NULL AND st.position_1200m < 50;

UPDATE race_results rr SET position_800m = st.position_1600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
  AND r.race_distance = 2400
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_800m IS NULL AND rr.horse_number ~ '[0-9]'
  AND st.position_1600m IS NOT NULL AND st.position_1600m < 50;

-- position_400m by horse_number
UPDATE race_results rr SET position_400m = st.position_800m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
  AND r.race_distance IN (1000, 1200)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL AND rr.horse_number ~ '[0-9]'
  AND st.position_800m IS NOT NULL AND st.position_800m < 50;

UPDATE race_results rr SET position_400m = st.position_1200m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
  AND r.race_distance IN (1400, 1600, 1650)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL AND rr.horse_number ~ '[0-9]'
  AND st.position_1200m IS NOT NULL AND st.position_1200m < 50;

UPDATE race_results rr SET position_400m = st.position_1600m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
  AND r.race_distance IN (1800, 2000)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL AND rr.horse_number ~ '[0-9]'
  AND st.position_1600m IS NOT NULL AND st.position_1600m < 50;

UPDATE race_results rr SET position_400m = st.position_2000m
FROM race_results_sectional_times st, races r
WHERE rr.race_id = st.race_id AND rr.race_id = r.race_id
  AND REGEXP_REPLACE(rr.horse_number, '[^0-9]', '', 'g')::integer = st.horse_number
  AND r.race_distance IN (2200, 2400)
  AND (r.track_name ILIKE '%sha tin%' OR r.track_name ILIKE '%happy valley%')
  AND rr.position_400m IS NULL AND rr.horse_number ~ '[0-9]'
  AND st.position_2000m IS NOT NULL AND st.position_2000m < 50;

DO $$
BEGIN
    RAISE NOTICE '[06] ✓ HK horse_number backfill complete';
END $$;

-- ============================================================================
-- SECTION D: SYNC TO RACE_TRAINING_DATASET
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '[06] Section D: Syncing sectional data to race_training_dataset...';
END $$;

-- Update position_at_800 and position_at_400 from race_results
UPDATE race_training_dataset rtd
SET 
    position_at_800 = rr.position_800m,
    position_at_400 = rr.position_400m
FROM race_results rr
WHERE rtd.race_id = rr.race_id
  AND rtd.horse_slug = rr.horse_slug
  AND (rtd.position_at_800 IS NULL OR rtd.position_at_400 IS NULL)
  AND (rr.position_800m IS NOT NULL OR rr.position_400m IS NOT NULL);

-- Update position_800m and position_400m columns directly
UPDATE race_training_dataset rtd
SET 
    position_800m = rr.position_800m,
    position_400m = rr.position_400m
FROM race_results rr
WHERE rtd.race_id = rr.race_id
  AND rtd.horse_slug = rr.horse_slug
  AND (rtd.position_800m IS NULL OR rtd.position_400m IS NULL)
  AND (rr.position_800m IS NOT NULL OR rr.position_400m IS NOT NULL);

-- Recalculate pos_improvement_800_finish
UPDATE race_training_dataset
SET pos_improvement_800_finish = CASE 
    WHEN position_800m IS NOT NULL AND final_position IS NOT NULL 
    THEN position_800m - final_position 
    ELSE NULL 
END
WHERE position_800m IS NOT NULL;

-- Recalculate running_style based on updated positions
UPDATE race_training_dataset SET running_style = CASE
    WHEN position_800m IS NULL THEN 'unknown'
    WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.2 THEN 'leader'
    WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.4 THEN 'on_pace'
    WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.6 THEN 'midfield'
    WHEN position_800m::float / NULLIF(total_runners, 0) <= 0.8 THEN 'off_pace'
    ELSE 'closer'
END
WHERE position_800m IS NOT NULL;

DO $$
BEGIN
    RAISE NOTICE '[06] ✓ race_training_dataset sync complete';
END $$;

-- ============================================================================
-- STEP 5: VALIDATION
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
    
    RAISE NOTICE '[06] ============================================';
    RAISE NOTICE '[06] SECTIONAL BACKFILL COMPLETE';
    RAISE NOTICE '[06] ============================================';
    RAISE NOTICE '[06] race_results AFTER backfill:';
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
    RAISE NOTICE '[06] ============================================';
    RAISE NOTICE '[06] ✓ Sectional backfill complete. Proceed to verify running_style distribution.';
END $$;

-- Show running style distribution after update
SELECT 
    running_style,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM race_training_dataset
WHERE running_style IS NOT NULL
GROUP BY running_style
ORDER BY count DESC;
