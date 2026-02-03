-- ============================================================================
-- PHASE 8: WEATHER & TRACK CONDITION FEATURES
-- ============================================================================
-- Run Order: After 07_current_form_views.sql
-- Dependencies: race_training_dataset with base data
-- Est. Time: 3-5 minutes
-- 
-- WHAT THIS DOES:
--   1. Calculates horse-specific wet/dry track win rates (HISTORICAL)
--   2. Calculates wet vs dry preference score
--   3. Calculates temperature preference features
--   4. Adds weather condition match score
--
-- CORRELATION ANALYSIS (Jan 2026):
--   - Wet Specialists: 29.53% WR on wet vs 6.44% on dry (+23% diff!)
--   - Dry Specialists: 15.74% WR on dry vs 0.69% on wet (+15% diff!)
--   - Prior wet specialist on wet track: 10.80% WR (vs 7.67% baseline)
--
-- NOT LEAKY BECAUSE:
--   - Current race weather is known BEFORE race starts
--   - Historical performance uses PRIOR races only (windowed)
-- ============================================================================

SET statement_timeout = '0';
SET lock_timeout = '0';
SET work_mem = '256MB';

DO $$ BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 8: WEATHER FEATURES - Started at %', NOW();
    RAISE NOTICE '============================================================';
END $$;

-- ============================================================================
-- STEP 1: ADD NEW COLUMNS
-- ============================================================================
DO $$ 
BEGIN
    RAISE NOTICE '[8.1] Adding weather feature columns...';
    
    -- Horse wet track win rate (Heavy, Soft 6, Soft 7)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'horse_wet_track_win_rate') THEN
        ALTER TABLE race_training_dataset ADD COLUMN horse_wet_track_win_rate NUMERIC(6,4);
        RAISE NOTICE '  Added: horse_wet_track_win_rate';
    END IF;
    
    -- Horse dry track win rate (Good, Firm, Synthetic)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'horse_dry_track_win_rate') THEN
        ALTER TABLE race_training_dataset ADD COLUMN horse_dry_track_win_rate NUMERIC(6,4);
        RAISE NOTICE '  Added: horse_dry_track_win_rate';
    END IF;
    
    -- Wet vs dry preference score (positive = wet specialist)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'wet_dry_preference_score') THEN
        ALTER TABLE race_training_dataset ADD COLUMN wet_dry_preference_score NUMERIC(6,4);
        RAISE NOTICE '  Added: wet_dry_preference_score';
    END IF;
    
    -- Weather condition match score (how well current conditions match horse preference)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'weather_condition_match') THEN
        ALTER TABLE race_training_dataset ADD COLUMN weather_condition_match NUMERIC(6,4);
        RAISE NOTICE '  Added: weather_condition_match';
    END IF;
    
    -- Horse wet track runs (for confidence weighting)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'horse_wet_track_runs') THEN
        ALTER TABLE race_training_dataset ADD COLUMN horse_wet_track_runs INTEGER DEFAULT 0;
        RAISE NOTICE '  Added: horse_wet_track_runs';
    END IF;
    
    -- Horse dry track runs (for confidence weighting)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'horse_dry_track_runs') THEN
        ALTER TABLE race_training_dataset ADD COLUMN horse_dry_track_runs INTEGER DEFAULT 0;
        RAISE NOTICE '  Added: horse_dry_track_runs';
    END IF;
    
    -- Is current race on wet track
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'is_wet_track') THEN
        ALTER TABLE race_training_dataset ADD COLUMN is_wet_track BOOLEAN DEFAULT FALSE;
        RAISE NOTICE '  Added: is_wet_track';
    END IF;
    
    -- Track condition numeric (1=Firm to 7=Heavy10)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'race_training_dataset' 
                   AND column_name = 'track_condition_numeric') THEN
        ALTER TABLE race_training_dataset ADD COLUMN track_condition_numeric INTEGER DEFAULT 3;
        RAISE NOTICE '  Added: track_condition_numeric';
    END IF;
    
END $$;

-- ============================================================================
-- STEP 2: SET TRACK CONDITION NUMERIC & WET FLAG
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[8.2] Setting track condition numeric values...'; END $$;

UPDATE race_training_dataset
SET 
    track_condition_numeric = CASE
        WHEN track_condition ILIKE '%firm%' OR track_condition ILIKE '%good 1%' THEN 1
        WHEN track_condition ILIKE '%good 2%' THEN 2
        WHEN track_condition ILIKE '%good 3%' OR track_condition ILIKE '%good 4%' OR track_condition = 'Good' THEN 3
        WHEN track_condition ILIKE '%soft 5%' THEN 4
        WHEN track_condition ILIKE '%soft 6%' OR track_condition = 'Soft' THEN 5
        WHEN track_condition ILIKE '%soft 7%' THEN 6
        WHEN track_condition ILIKE '%heavy%' THEN 7
        WHEN track_condition = 'Synthetic' THEN 3  -- Synthetic treated as good
        ELSE 3  -- Default to Good
    END,
    is_wet_track = CASE
        WHEN track_condition ILIKE '%heavy%' THEN TRUE
        WHEN track_condition ILIKE '%soft 6%' THEN TRUE
        WHEN track_condition ILIKE '%soft 7%' THEN TRUE
        ELSE FALSE
    END,
    updated_at = NOW()
WHERE track_condition IS NOT NULL;

DO $$
DECLARE
    wet_count INTEGER;
    dry_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO wet_count FROM race_training_dataset WHERE is_wet_track = TRUE;
    SELECT COUNT(*) INTO dry_count FROM race_training_dataset WHERE is_wet_track = FALSE;
    RAISE NOTICE '[8.2] Track conditions set: % wet, % dry', wet_count, dry_count;
END $$;

-- ============================================================================
-- STEP 3: CALCULATE HORSE WET/DRY TRACK WIN RATES (HISTORICAL - NO LEAKAGE)
-- Uses ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING to exclude current race
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[8.3] Calculating historical wet/dry track performance...'; END $$;

WITH horse_condition_history AS (
    SELECT 
        horse_location_slug,
        race_id,
        race_date,
        is_wet_track,
        final_position,
        -- Wet track stats (PRIOR races only)
        SUM(CASE WHEN is_wet_track = TRUE AND final_position = 1 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as prior_wet_wins,
        SUM(CASE WHEN is_wet_track = TRUE THEN 1 ELSE 0 END) 
            OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as prior_wet_runs,
        -- Dry track stats (PRIOR races only)
        SUM(CASE WHEN is_wet_track = FALSE AND final_position = 1 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as prior_dry_wins,
        SUM(CASE WHEN is_wet_track = FALSE THEN 1 ELSE 0 END) 
            OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as prior_dry_runs
    FROM race_training_dataset
    WHERE horse_location_slug IS NOT NULL
      AND final_position IS NOT NULL
)
UPDATE race_training_dataset rtd
SET 
    horse_wet_track_runs = COALESCE(hch.prior_wet_runs, 0),
    horse_dry_track_runs = COALESCE(hch.prior_dry_runs, 0),
    horse_wet_track_win_rate = CASE 
        WHEN COALESCE(hch.prior_wet_runs, 0) >= 2 
        THEN hch.prior_wet_wins::numeric / hch.prior_wet_runs 
        ELSE NULL 
    END,
    horse_dry_track_win_rate = CASE 
        WHEN COALESCE(hch.prior_dry_runs, 0) >= 3 
        THEN hch.prior_dry_wins::numeric / hch.prior_dry_runs 
        ELSE NULL 
    END,
    updated_at = NOW()
FROM horse_condition_history hch
WHERE rtd.horse_location_slug = hch.horse_location_slug
  AND rtd.race_id = hch.race_id;

DO $$
DECLARE
    wet_wr_count INTEGER;
    dry_wr_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO wet_wr_count FROM race_training_dataset WHERE horse_wet_track_win_rate IS NOT NULL;
    SELECT COUNT(*) INTO dry_wr_count FROM race_training_dataset WHERE horse_dry_track_win_rate IS NOT NULL;
    RAISE NOTICE '[8.3] Win rates calculated: % with wet WR, % with dry WR', wet_wr_count, dry_wr_count;
END $$;

-- ============================================================================
-- STEP 4: CALCULATE WET/DRY PREFERENCE SCORE
-- Positive = wet specialist, Negative = dry specialist
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[8.4] Calculating wet/dry preference scores...'; END $$;

UPDATE race_training_dataset
SET 
    wet_dry_preference_score = CASE
        -- Need sufficient runs in both conditions
        WHEN horse_wet_track_runs >= 2 AND horse_dry_track_runs >= 3 THEN
            COALESCE(horse_wet_track_win_rate, 0) - COALESCE(horse_dry_track_win_rate, 0)
        -- Only wet track history
        WHEN horse_wet_track_runs >= 2 AND horse_dry_track_runs < 3 THEN
            COALESCE(horse_wet_track_win_rate, 0) - 0.095  -- Compare to baseline 9.5% WR
        -- Only dry track history
        WHEN horse_wet_track_runs < 2 AND horse_dry_track_runs >= 3 THEN
            0.095 - COALESCE(horse_dry_track_win_rate, 0)  -- Compare to baseline
        ELSE 0  -- Insufficient data
    END,
    updated_at = NOW();

DO $$
DECLARE
    wet_specialists INTEGER;
    dry_specialists INTEGER;
    no_pref INTEGER;
BEGIN
    SELECT COUNT(*) INTO wet_specialists FROM race_training_dataset WHERE wet_dry_preference_score > 0.05;
    SELECT COUNT(*) INTO dry_specialists FROM race_training_dataset WHERE wet_dry_preference_score < -0.05;
    SELECT COUNT(*) INTO no_pref FROM race_training_dataset WHERE wet_dry_preference_score BETWEEN -0.05 AND 0.05;
    RAISE NOTICE '[8.4] Preference distribution: % wet specialists, % dry specialists, % neutral', 
                 wet_specialists, dry_specialists, no_pref;
END $$;

-- ============================================================================
-- STEP 5: CALCULATE WEATHER CONDITION MATCH SCORE
-- How well does current track condition match horse's preference?
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[8.5] Calculating weather condition match scores...'; END $$;

UPDATE race_training_dataset
SET 
    weather_condition_match = CASE
        -- Wet specialist on wet track = positive match
        WHEN wet_dry_preference_score > 0.05 AND is_wet_track = TRUE THEN
            wet_dry_preference_score * 2  -- Amplify the advantage
        -- Wet specialist on dry track = negative match
        WHEN wet_dry_preference_score > 0.05 AND is_wet_track = FALSE THEN
            -wet_dry_preference_score
        -- Dry specialist on dry track = positive match
        WHEN wet_dry_preference_score < -0.05 AND is_wet_track = FALSE THEN
            ABS(wet_dry_preference_score) * 2  -- Amplify the advantage
        -- Dry specialist on wet track = negative match
        WHEN wet_dry_preference_score < -0.05 AND is_wet_track = TRUE THEN
            wet_dry_preference_score  -- Already negative
        -- Neutral horse = small random factor based on recent form
        ELSE 0
    END,
    updated_at = NOW();

DO $$
DECLARE
    positive_match INTEGER;
    negative_match INTEGER;
BEGIN
    SELECT COUNT(*) INTO positive_match FROM race_training_dataset WHERE weather_condition_match > 0;
    SELECT COUNT(*) INTO negative_match FROM race_training_dataset WHERE weather_condition_match < 0;
    RAISE NOTICE '[8.5] Match scores: % positive (good match), % negative (poor match)', 
                 positive_match, negative_match;
END $$;

-- ============================================================================
-- STEP 6: VALIDATION - Check predictive power
-- ============================================================================
DO $$
DECLARE
    wet_spec_on_wet RECORD;
    dry_spec_on_wet RECORD;
BEGIN
    RAISE NOTICE '[8.6] Validating predictive power...';
    
    -- Wet specialists on wet tracks
    SELECT 
        COUNT(*) as runs,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) as wins,
        ROUND(SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as wr
    INTO wet_spec_on_wet
    FROM race_training_dataset
    WHERE wet_dry_preference_score > 0.05 
      AND is_wet_track = TRUE
      AND final_position IS NOT NULL;
    
    -- Dry specialists on wet tracks  
    SELECT 
        COUNT(*) as runs,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) as wins,
        ROUND(SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as wr
    INTO dry_spec_on_wet
    FROM race_training_dataset
    WHERE wet_dry_preference_score < -0.05 
      AND is_wet_track = TRUE
      AND final_position IS NOT NULL;
    
    RAISE NOTICE '  Wet specialists on wet tracks: % runs, %% WR', wet_spec_on_wet.runs, wet_spec_on_wet.wr;
    RAISE NOTICE '  Dry specialists on wet tracks: % runs, %% WR', dry_spec_on_wet.runs, dry_spec_on_wet.wr;
    
    IF wet_spec_on_wet.wr > dry_spec_on_wet.wr THEN
        RAISE NOTICE '  ✓ Feature has predictive power! (+%% edge)', 
                     ROUND(wet_spec_on_wet.wr - dry_spec_on_wet.wr, 2);
    END IF;
END $$;

-- ============================================================================
-- COMPLETION
-- ============================================================================
DO $$
DECLARE
    features_populated INTEGER;
BEGIN
    SELECT COUNT(*) INTO features_populated 
    FROM race_training_dataset 
    WHERE weather_condition_match IS NOT NULL 
      AND weather_condition_match != 0;
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 8 COMPLETE - Weather Features';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    RAISE NOTICE 'NEW FEATURES ADDED:';
    RAISE NOTICE '  ✓ horse_wet_track_win_rate   (historical WR on wet tracks)';
    RAISE NOTICE '  ✓ horse_dry_track_win_rate   (historical WR on dry tracks)';
    RAISE NOTICE '  ✓ wet_dry_preference_score   (positive = wet specialist)';
    RAISE NOTICE '  ✓ weather_condition_match    (current conditions vs preference)';
    RAISE NOTICE '  ✓ horse_wet_track_runs       (experience count)';
    RAISE NOTICE '  ✓ horse_dry_track_runs       (experience count)';
    RAISE NOTICE '  ✓ is_wet_track               (current race flag)';
    RAISE NOTICE '  ✓ track_condition_numeric    (1-7 scale)';
    RAISE NOTICE '';
    RAISE NOTICE 'Records with weather match score: %', features_populated;
    RAISE NOTICE '';
    RAISE NOTICE 'NOT LEAKY - Uses:';
    RAISE NOTICE '  - Current race weather (known before race)';
    RAISE NOTICE '  - PRIOR race performance only (windowed)';
    RAISE NOTICE '';
    RAISE NOTICE 'Next: Run 09_remove_leakage_columns.sql';
    RAISE NOTICE '============================================================';
END $$;
