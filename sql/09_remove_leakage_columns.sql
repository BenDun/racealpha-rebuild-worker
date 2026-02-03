-- ============================================================================
-- SCRIPT 08: REMOVE DATA LEAKAGE COLUMNS FROM TRAINING DATASET
-- ============================================================================
-- Run Order: 08 (After all rebuilds complete)
-- Dependencies: race_training_dataset table exists
-- Est. Time: 1-2 minutes
-- 
-- PURPOSE:
--   Remove columns that contain CURRENT RACE data from the training dataset.
--   These columns cause data leakage because they wouldn't be available at
--   prediction time (before the race starts).
--
-- LEAKAGE CATEGORIES:
--   1. OUTCOME DATA: final_position, margin, raw_time_seconds
--   2. IN-RACE POSITIONS: position_400m, position_800m, position_at_*
--   3. POSITION IMPROVEMENTS: pos_improvement_*, best_late_improvement
--   4. DERIVED SCORES: closing_ability_score, early_speed_score, sustained_run_score
--   5. CURRENT RACE SPEED: speed_figure (calculated from race finish time)
--
-- SQL VERIFICATION (Jan 2026):
--   - pos_improvement_800_finish = position_800m - final_position (EXACT MATCH)
--   - pos_improvement_400_finish = position_400m - final_position (EXACT MATCH)
--   - best_late_improvement = pos_improvement_800_finish (EXACT COPY!)
--   - speed_figure calculated from raw_time_seconds (current race time)
--
-- SAFE COLUMNS (NOT REMOVED - verified as historical):
--   - avg_early_position_800m: Historical average, values differ from current race
--   - avg_mid_position_400m: Historical average, values differ from current race  
--   - historical_late_improvement: Verified historical
--   - avg_speed_figure: Historical average from prior races
--   - best_speed_figure: Best from prior races
--   - avg_improvement_from_400m: Historical average (values differ from current)
--   - avg_late_improvement: Historical average
--   - jockey_avg_position_*: Rolling historical jockey stats
--   - trainer_avg_position_*: Rolling historical trainer stats
-- ============================================================================

SET statement_timeout = '0';
SET lock_timeout = '0';

DO $$
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE '[08] REMOVE DATA LEAKAGE COLUMNS - Starting at %', NOW();
    RAISE NOTICE '============================================================';
END $$;


-- ############################################################################
-- PART 1: DROP OUTCOME DATA COLUMNS (DIRECT LEAKAGE)
-- These are the actual race results - OBVIOUS leakage
-- ############################################################################

DO $$
BEGIN
    RAISE NOTICE '[08.1] Removing OUTCOME columns (final_position, margin, raw_time_seconds)...';
    
    -- final_position - THE TARGET VARIABLE! Must be separate
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'final_position') THEN
        ALTER TABLE race_training_dataset DROP COLUMN final_position;
        RAISE NOTICE '       ✓ Dropped: final_position';
    END IF;
    
    -- margin - Distance behind winner (only known after race)
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'margin') THEN
        ALTER TABLE race_training_dataset DROP COLUMN margin;
        RAISE NOTICE '       ✓ Dropped: margin';
    END IF;
    
    -- raw_time_seconds - Race finish time (only known after race)
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'raw_time_seconds') THEN
        ALTER TABLE race_training_dataset DROP COLUMN raw_time_seconds;
        RAISE NOTICE '       ✓ Dropped: raw_time_seconds';
    END IF;
END $$;


-- ############################################################################
-- PART 2: DROP IN-RACE POSITION COLUMNS (CURRENT RACE SECTIONALS)
-- These are positions at checkpoints DURING the current race
-- ############################################################################

DO $$
BEGIN
    RAISE NOTICE '[08.2] Removing IN-RACE POSITION columns (current race sectional positions)...';
    
    -- position_800m - Position at 800m mark of CURRENT race
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'position_800m') THEN
        ALTER TABLE race_training_dataset DROP COLUMN position_800m;
        RAISE NOTICE '       ✓ Dropped: position_800m';
    END IF;
    
    -- position_400m - Position at 400m mark of CURRENT race
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'position_400m') THEN
        ALTER TABLE race_training_dataset DROP COLUMN position_400m;
        RAISE NOTICE '       ✓ Dropped: position_400m';
    END IF;
    
    -- position_at_800 - Alias for position_800m
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'position_at_800') THEN
        ALTER TABLE race_training_dataset DROP COLUMN position_at_800;
        RAISE NOTICE '       ✓ Dropped: position_at_800';
    END IF;
    
    -- position_at_400 - Alias for position_400m
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'position_at_400') THEN
        ALTER TABLE race_training_dataset DROP COLUMN position_at_400;
        RAISE NOTICE '       ✓ Dropped: position_at_400';
    END IF;
    
    -- position_at_end - Another alias for final_position
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'position_at_end') THEN
        ALTER TABLE race_training_dataset DROP COLUMN position_at_end;
        RAISE NOTICE '       ✓ Dropped: position_at_end';
    END IF;
END $$;


-- ############################################################################
-- PART 3: DROP POSITION IMPROVEMENT COLUMNS (DERIVED FROM CURRENT RACE)
-- These are calculated from current race positions - verified via SQL
-- pos_improvement_800_finish = position_800m - final_position (exact match!)
-- ############################################################################

DO $$
BEGIN
    RAISE NOTICE '[08.3] Removing POSITION IMPROVEMENT columns (derived from current race)...';
    
    -- pos_improvement_800_finish = position_800m - final_position
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'pos_improvement_800_finish') THEN
        ALTER TABLE race_training_dataset DROP COLUMN pos_improvement_800_finish;
        RAISE NOTICE '       ✓ Dropped: pos_improvement_800_finish (= position_800m - final_position)';
    END IF;
    
    -- pos_improvement_400_finish = position_400m - final_position
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'pos_improvement_400_finish') THEN
        ALTER TABLE race_training_dataset DROP COLUMN pos_improvement_400_finish;
        RAISE NOTICE '       ✓ Dropped: pos_improvement_400_finish (= position_400m - final_position)';
    END IF;
    
    -- pos_improvement_800_400 = position_800m - position_400m
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'pos_improvement_800_400') THEN
        ALTER TABLE race_training_dataset DROP COLUMN pos_improvement_800_400;
        RAISE NOTICE '       ✓ Dropped: pos_improvement_800_400 (= position_800m - position_400m)';
    END IF;
    
    -- best_late_improvement - SQL verified: EXACT COPY of pos_improvement_800_finish!
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'best_late_improvement') THEN
        ALTER TABLE race_training_dataset DROP COLUMN best_late_improvement;
        RAISE NOTICE '       ✓ Dropped: best_late_improvement (= pos_improvement_800_finish copy!)';
    END IF;
END $$;


-- ############################################################################
-- PART 4: DROP DERIVED SCORE COLUMNS (CALCULATED FROM LEAKY FEATURES)
-- These scores are derived from the position improvement columns above
-- ############################################################################

DO $$
BEGIN
    RAISE NOTICE '[08.4] Removing DERIVED SCORE columns (calculated from leaky features)...';
    
    -- closing_ability_score - Derived from pos_improvement features
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'closing_ability_score') THEN
        ALTER TABLE race_training_dataset DROP COLUMN closing_ability_score;
        RAISE NOTICE '       ✓ Dropped: closing_ability_score';
    END IF;
    
    -- early_speed_score - Derived from position_800m data
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'early_speed_score') THEN
        ALTER TABLE race_training_dataset DROP COLUMN early_speed_score;
        RAISE NOTICE '       ✓ Dropped: early_speed_score';
    END IF;
    
    -- sustained_run_score - Derived from current race position changes
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'sustained_run_score') THEN
        ALTER TABLE race_training_dataset DROP COLUMN sustained_run_score;
        RAISE NOTICE '       ✓ Dropped: sustained_run_score';
    END IF;
    
    -- closing_power_score - Similar to closing_ability_score
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'closing_power_score') THEN
        ALTER TABLE race_training_dataset DROP COLUMN closing_power_score;
        RAISE NOTICE '       ✓ Dropped: closing_power_score';
    END IF;
    
    -- finishing_kick_consistency - Derived from current race late surge
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'finishing_kick_consistency') THEN
        ALTER TABLE race_training_dataset DROP COLUMN finishing_kick_consistency;
        RAISE NOTICE '       ✓ Dropped: finishing_kick_consistency';
    END IF;
END $$;


-- ############################################################################
-- PART 5: DROP CURRENT RACE SPEED FIGURE
-- speed_figure is calculated from raw_time_seconds (current race finish time)
-- Keep avg_speed_figure and best_speed_figure (historical - safe)
-- ############################################################################

DO $$
BEGIN
    RAISE NOTICE '[08.5] Removing CURRENT RACE SPEED columns...';
    
    -- speed_figure - Calculated from current race raw_time_seconds
    -- Formula: race_distance / raw_time_seconds * factor
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'speed_figure') THEN
        ALTER TABLE race_training_dataset DROP COLUMN speed_figure;
        RAISE NOTICE '       ✓ Dropped: speed_figure (calculated from current race time)';
    END IF;
    
    -- speed_rating - SQL verified: EXACT COPY of speed_figure! Same leakage!
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'speed_rating') THEN
        ALTER TABLE race_training_dataset DROP COLUMN speed_rating;
        RAISE NOTICE '       ✓ Dropped: speed_rating (= speed_figure, same leakage)';
    END IF;
    
    -- early_speed_pct - Calculated from current race position_800m
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'early_speed_pct') THEN
        ALTER TABLE race_training_dataset DROP COLUMN early_speed_pct;
        RAISE NOTICE '       ✓ Dropped: early_speed_pct';
    END IF;
END $$;


-- ############################################################################
-- PART 6: DROP ADDITIONAL POTENTIALLY LEAKY COLUMNS
-- These columns might contain current race data based on naming
-- ############################################################################

DO $$
BEGIN
    RAISE NOTICE '[08.6] Removing ADDITIONAL potentially leaky columns...';
    
    -- strong_finish_pct - May be derived from current race finish
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'strong_finish_pct') THEN
        ALTER TABLE race_training_dataset DROP COLUMN strong_finish_pct;
        RAISE NOTICE '       ✓ Dropped: strong_finish_pct';
    END IF;
    
    -- pos_volatility_800 - Derived from position_800m
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'race_training_dataset' AND column_name = 'pos_volatility_800') THEN
        ALTER TABLE race_training_dataset DROP COLUMN pos_volatility_800;
        RAISE NOTICE '       ✓ Dropped: pos_volatility_800';
    END IF;
END $$;


-- ############################################################################
-- PART 7: VALIDATION - Verify leaky columns are gone
-- ############################################################################

DO $$
DECLARE
    remaining_leaky TEXT[];
    col TEXT;
BEGIN
    RAISE NOTICE '[08.7] Validating leakage removal...';
    
    -- Check for any remaining leaky columns
    SELECT ARRAY_AGG(column_name) INTO remaining_leaky
    FROM information_schema.columns
    WHERE table_name = 'race_training_dataset'
    AND column_name IN (
        'final_position', 'margin', 'raw_time_seconds',
        'position_800m', 'position_400m', 'position_at_800', 'position_at_400', 'position_at_end',
        'pos_improvement_800_finish', 'pos_improvement_400_finish', 'pos_improvement_800_400',
        'best_late_improvement', 'closing_ability_score', 'early_speed_score', 
        'sustained_run_score', 'closing_power_score', 'finishing_kick_consistency',
        'speed_figure', 'speed_rating', 'early_speed_pct', 'strong_finish_pct', 'pos_volatility_800'
    );
    
    IF remaining_leaky IS NULL OR array_length(remaining_leaky, 1) IS NULL THEN
        RAISE NOTICE '       ✓ All leakage columns successfully removed!';
    ELSE
        RAISE WARNING '       ⚠️ Some leaky columns still exist: %', remaining_leaky;
    END IF;
END $$;


-- ############################################################################
-- PART 8: FINAL SUMMARY
-- ############################################################################

DO $$
DECLARE
    total_columns INTEGER;
    sample_columns TEXT[];
BEGIN
    -- Count remaining columns
    SELECT COUNT(*) INTO total_columns
    FROM information_schema.columns
    WHERE table_name = 'race_training_dataset';
    
    -- Get sample of remaining columns
    SELECT ARRAY_AGG(column_name ORDER BY column_name) INTO sample_columns
    FROM (
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'race_training_dataset'
        ORDER BY column_name
        LIMIT 10
    ) sub;
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE '[08] REMOVE DATA LEAKAGE COLUMNS - COMPLETE';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    RAISE NOTICE 'COLUMNS REMOVED (20 total):';
    RAISE NOTICE '  OUTCOME: final_position, margin, raw_time_seconds';
    RAISE NOTICE '  IN-RACE: position_800m, position_400m, position_at_800, position_at_400, position_at_end';
    RAISE NOTICE '  IMPROVEMENTS: pos_improvement_800_finish, pos_improvement_400_finish, pos_improvement_800_400, best_late_improvement';
    RAISE NOTICE '  DERIVED: closing_ability_score, early_speed_score, sustained_run_score, closing_power_score, finishing_kick_consistency';
    RAISE NOTICE '  SPEED: speed_figure, early_speed_pct';
    RAISE NOTICE '  OTHER: strong_finish_pct, pos_volatility_800';
    RAISE NOTICE '';
    RAISE NOTICE 'COLUMNS KEPT (safe historical data):';
    RAISE NOTICE '  ✓ avg_early_position_800m (historical average)';
    RAISE NOTICE '  ✓ avg_mid_position_400m (historical average)';
    RAISE NOTICE '  ✓ historical_late_improvement (verified historical)';
    RAISE NOTICE '  ✓ avg_speed_figure (historical average)';
    RAISE NOTICE '  ✓ best_speed_figure (best from prior races)';
    RAISE NOTICE '  ✓ avg_improvement_from_400m (historical average)';
    RAISE NOTICE '  ✓ avg_late_improvement (historical average)';
    RAISE NOTICE '  ✓ jockey_avg_position_20/50 (rolling historical)';
    RAISE NOTICE '  ✓ trainer_avg_position_50/100 (rolling historical)';
    RAISE NOTICE '';
    RAISE NOTICE 'Remaining columns: %', total_columns;
    RAISE NOTICE 'First 10: %', sample_columns;
    RAISE NOTICE '';
    RAISE NOTICE '⚠️  IMPORTANT: After running this script, retrain models!';
    RAISE NOTICE '    The blocklist in trainer.py is a safety net, but removing';
    RAISE NOTICE '    columns from the database is the definitive fix.';
    RAISE NOTICE '============================================================';
END $$;

-- Reset timeouts
RESET statement_timeout;
RESET lock_timeout;

-- ============================================================================
-- POST-EXECUTION CHECKLIST
-- ============================================================================
-- After running this script:
-- 
-- 1. VERIFY: Run this query to confirm no leaky columns remain:
--    SELECT column_name FROM information_schema.columns 
--    WHERE table_name = 'race_training_dataset'
--    AND column_name ILIKE ANY(ARRAY['%position%', '%finish%', '%margin%'])
--    AND column_name NOT IN ('avg_early_position_800m', 'avg_mid_position_400m', 
--                            'last_5_avg_position', 'jockey_avg_position_20',
--                            'jockey_avg_position_50', 'trainer_avg_position_50',
--                            'trainer_avg_position_100', 'barrier_position',
--                            'speed_map_position');
--
-- 2. RETRAIN MODELS: The AUC should drop from 0.91+ to a realistic 0.60-0.75
--
-- 3. UPDATE config.py: Remove references to dropped columns from:
--    - BASE_MODEL_FEATURES
--    - POSITION_MODEL_FEATURES
--    - Any other feature lists
-- ============================================================================
