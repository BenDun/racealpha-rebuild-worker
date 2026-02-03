-- ============================================================================
-- SCRIPT 07: ELO REBUILD AND ENTITY SYNC (V2 FINAL)
-- ============================================================================
-- Run Order: 07 (FINAL - after all views created)
-- Dependencies: Scripts 01-06, horse_elo_ratings table, horse_current_form view
-- Est. Time: 30-45 minutes (ELO rebuild is chronological and heavy)
-- 
-- PURPOSE:
--   - Rebuild horse_elo_ratings table chronologically for accurate current_elo
--   - Sync missing horses from race_results to horses table
--   - Ensure all entity data is complete and current
--
-- WHY THIS IS SEPARATE:
--   - ELO rebuild is expensive (processes ALL races chronologically)
--   - Must run AFTER horse_current_form materialized view exists
--   - Sync depends on horse_current_form data
--   - Can be re-run independently if ELO needs recalculation
--
-- FUNCTIONS CREATED:
--   - rebuild_horse_elo_ratings_for_current_form() - Full ELO rebuild
--   - sync_missing_horses_from_results() - Sync horses table
-- ============================================================================

-- Disable timeouts for heavy operations
SET statement_timeout = '0';
SET lock_timeout = '0';

DO $$
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE '[07] ELO REBUILD AND ENTITY SYNC - Starting at %', NOW();
    RAISE NOTICE '============================================================';
END $$;


-- ############################################################################
-- PART 1: CREATE ELO REBUILD FUNCTION
-- ############################################################################
-- Rebuilds horse_elo_ratings table using traditional K-factor ELO progression
-- Processes ALL races in CHRONOLOGICAL order (critical for ELO accuracy)
-- Different from race_training_dataset.horse_elo which is a composite score

DO $$
BEGIN
    RAISE NOTICE '[07.1] Creating rebuild_horse_elo_ratings_for_current_form function...';
END $$;

CREATE OR REPLACE FUNCTION rebuild_horse_elo_ratings_for_current_form()
RETURNS TABLE(records_processed INTEGER, execution_time_seconds NUMERIC) AS $$
DECLARE
    start_time TIMESTAMP;
    processed_count INTEGER := 0;
    race_record RECORD;
    horse_record RECORD;
    current_elo DECIMAL(10,2);
    elo_change DECIMAL(10,2);
    new_elo DECIMAL(10,2);
    K_FACTOR INTEGER := 32;
BEGIN
    start_time := clock_timestamp();
    
    -- Clear existing ELO data for clean rebuild
    DELETE FROM horse_elo_ratings;
    RAISE NOTICE '[07.1] Cleared existing ELO data, starting chronological rebuild...';

    -- Process ALL races in CHRONOLOGICAL order (critical for ELO progression)
    FOR race_record IN 
        SELECT DISTINCT 
            r.race_id, 
            r.race_date, 
            r.race_class,
            COUNT(DISTINCT rr.horse_slug) as total_runners
        FROM races r
        JOIN race_results rr ON r.race_id = rr.race_id
        WHERE rr.position IS NOT NULL
        AND rr.finish_status = 'FINISHED'
        AND r.race_date >= '2017-01-01'
        GROUP BY r.race_id, r.race_date, r.race_class
        ORDER BY r.race_date, r.race_id  -- CHRONOLOGICAL ORDER CRITICAL
    LOOP
        -- Process each unique horse in this race
        FOR horse_record IN
            SELECT DISTINCT ON (horse_slug)
                horse_slug, 
                position,
                COALESCE(win_odds, 5.0) as win_odds
            FROM race_results 
            WHERE race_id = race_record.race_id 
            AND position IS NOT NULL
            AND finish_status = 'FINISHED'
            ORDER BY horse_slug, id
        LOOP
            -- Get horse's ACTUAL previous ELO (not always 1200!)
            SELECT COALESCE(
                (SELECT elo_after 
                 FROM horse_elo_ratings 
                 WHERE horse_slug = horse_record.horse_slug 
                 AND race_date < race_record.race_date
                 ORDER BY race_date DESC, id DESC 
                 LIMIT 1), 
                1200.0  -- Default for brand new horses
            ) INTO current_elo;

            -- ELO calculation based on finishing position
            CASE 
                WHEN horse_record.position = 1 THEN
                    elo_change := K_FACTOR * 0.8;  -- Winner: +25.6
                WHEN horse_record.position = 2 THEN
                    elo_change := K_FACTOR * 0.4;  -- Second: +12.8
                WHEN horse_record.position = 3 THEN
                    elo_change := K_FACTOR * 0.1;  -- Third: +3.2
                WHEN horse_record.position <= race_record.total_runners / 2 THEN
                    elo_change := K_FACTOR * (-0.2); -- Mid-field: -6.4
                ELSE
                    elo_change := K_FACTOR * (-0.5); -- Back markers: -16
            END CASE;

            -- Race quality adjustments
            IF race_record.race_class ILIKE '%Group 1%' OR race_record.race_class ILIKE '%G1%' THEN
                elo_change := elo_change * 1.5;
            ELSIF race_record.race_class ILIKE '%Group%' THEN
                elo_change := elo_change * 1.3;
            ELSIF race_record.race_class ILIKE '%Listed%' THEN
                elo_change := elo_change * 1.2;
            END IF;

            -- Field size adjustments
            IF race_record.total_runners >= 14 THEN
                elo_change := elo_change * 1.2;
            ELSIF race_record.total_runners <= 8 THEN
                elo_change := elo_change * 0.8;
            END IF;

            new_elo := current_elo + elo_change;
            new_elo := GREATEST(600, LEAST(2200, new_elo));

            -- Insert ELO rating record
            INSERT INTO horse_elo_ratings (
                horse_slug, 
                race_date, 
                race_id, 
                elo_before,
                elo_after,
                position, 
                total_runners,
                created_at,
                updated_at
            ) VALUES (
                horse_record.horse_slug, 
                race_record.race_date, 
                race_record.race_id,
                current_elo,
                new_elo, 
                horse_record.position, 
                race_record.total_runners,
                NOW(),
                NOW()
            );

            processed_count := processed_count + 1;

            -- Progress logging every 50000 records
            IF processed_count % 50000 = 0 THEN
                RAISE NOTICE '[07.1] Processed % ELO records...', processed_count;
            END IF;
        END LOOP;
    END LOOP;

    RAISE NOTICE '[07.1] ✓ Completed ELO rebuild: % records', processed_count;
    
    RETURN QUERY SELECT 
        processed_count,
        ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - start_time))::NUMERIC, 2);
END;
$$ LANGUAGE plpgsql;

-- Grant execute to authenticated users
GRANT EXECUTE ON FUNCTION rebuild_horse_elo_ratings_for_current_form() TO authenticated;


-- ############################################################################
-- PART 2: EXECUTE ELO REBUILD
-- ############################################################################

DO $$
BEGIN
    RAISE NOTICE '[07.2] Executing ELO rebuild (this takes 20-40 minutes)...';
END $$;

SELECT * FROM rebuild_horse_elo_ratings_for_current_form();

-- Validate ELO distribution
DO $$
DECLARE
    total_elo_records INTEGER;
    unique_horses INTEGER;
    avg_elo NUMERIC;
    min_elo NUMERIC;
    max_elo NUMERIC;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT horse_slug), 
           ROUND(AVG(elo_after), 0), MIN(elo_after), MAX(elo_after)
    INTO total_elo_records, unique_horses, avg_elo, min_elo, max_elo
    FROM horse_elo_ratings;
    
    RAISE NOTICE '[07.2] ✓ ELO Validation:';
    RAISE NOTICE '       Total ELO records: %', total_elo_records;
    RAISE NOTICE '       Unique horses: %', unique_horses;
    RAISE NOTICE '       ELO range: % to % (avg: %)', min_elo, max_elo, avg_elo;
END $$;


-- ############################################################################
-- PART 3: CREATE SYNC MISSING HORSES FUNCTION
-- ############################################################################
-- Ensures any horse in race_results (via horse_current_form) also exists in horses table

DO $$
BEGIN
    RAISE NOTICE '[07.3] Creating sync_missing_horses_from_results function...';
END $$;

CREATE OR REPLACE FUNCTION sync_missing_horses_from_results()
RETURNS INTEGER AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH inserted AS (
        INSERT INTO horses (
            horse_name,
            horse_slug,
            total_races,
            wins,
            places,
            last_race_date,
            win_percentage,
            place_percentage,
            created_at,
            updated_at
        )
        SELECT 
            INITCAP(REPLACE(hcf.horse_slug, '-', ' ')) as horse_name,
            hcf.horse_slug,
            hcf.total_races,
            hcf.wins,
            hcf.places,
            hcf.last_race_date,
            LEAST(hcf.win_percentage::integer, 100)::smallint as win_percentage,
            LEAST(hcf.place_percentage::integer, 100)::smallint as place_percentage,
            NOW() as created_at,
            NOW() as updated_at
        FROM horse_current_form hcf
        LEFT JOIN horses h ON hcf.horse_slug = h.horse_slug
        WHERE h.id IS NULL
        ON CONFLICT (horse_slug) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;
    
    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

GRANT EXECUTE ON FUNCTION sync_missing_horses_from_results() TO authenticated;


-- ############################################################################
-- PART 4: EXECUTE HORSE SYNC
-- ############################################################################

DO $$
DECLARE
    synced_count INTEGER;
BEGIN
    RAISE NOTICE '[07.4] Syncing missing horses to horses table...';
    SELECT sync_missing_horses_from_results() INTO synced_count;
    RAISE NOTICE '[07.4] ✓ Synced % missing horses to horses table', synced_count;
END $$;


-- ############################################################################
-- PART 5: UPDATE HORSE_CURRENT_FORM WITH FRESH ELO
-- ############################################################################
-- Refresh the materialized view to pick up the new ELO values

DO $$
BEGIN
    RAISE NOTICE '[07.5] Refreshing horse_current_form to include updated ELO...';
END $$;

REFRESH MATERIALIZED VIEW CONCURRENTLY horse_current_form;

DO $$
DECLARE
    horses_with_elo INTEGER;
    avg_elo NUMERIC;
BEGIN
    SELECT COUNT(*), ROUND(AVG(current_elo), 0) 
    INTO horses_with_elo, avg_elo
    FROM horse_current_form 
    WHERE current_elo IS NOT NULL AND current_elo != 1200;
    
    RAISE NOTICE '[07.5] ✓ horse_current_form refreshed: % horses with calculated ELO (avg: %)', horses_with_elo, avg_elo;
END $$;


-- ############################################################################
-- PART 6: FINAL VALIDATION
-- ############################################################################

DO $$
DECLARE
    elo_records INTEGER;
    unique_horses INTEGER;
    horses_table_count INTEGER;
    hcf_count INTEGER;
    sample_horse RECORD;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT horse_slug) INTO elo_records, unique_horses FROM horse_elo_ratings;
    SELECT COUNT(*) INTO horses_table_count FROM horses;
    SELECT COUNT(*) INTO hcf_count FROM horse_current_form;
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE '[07] ELO REBUILD AND ENTITY SYNC - COMPLETE';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '[07] Results:';
    RAISE NOTICE '     horse_elo_ratings: % records for % unique horses', elo_records, unique_horses;
    RAISE NOTICE '     horses table: % horses', horses_table_count;
    RAISE NOTICE '     horse_current_form: % horses with current stats', hcf_count;
    RAISE NOTICE '';
    RAISE NOTICE '[07] Functions Available:';
    RAISE NOTICE '     SELECT * FROM rebuild_horse_elo_ratings_for_current_form();';
    RAISE NOTICE '     SELECT sync_missing_horses_from_results();';
    RAISE NOTICE '';
    
    -- Sample high-ELO horse
    SELECT * INTO sample_horse 
    FROM horse_current_form 
    WHERE total_races > 20 
    ORDER BY current_elo DESC 
    LIMIT 1;
    
    IF sample_horse IS NOT NULL THEN
        RAISE NOTICE '[07] Sample High-ELO Horse:';
        RAISE NOTICE '     %: ELO % | % races | %.1f%% win rate | Last5 win: %.1f%%', 
            sample_horse.horse_slug, 
            sample_horse.current_elo,
            sample_horse.total_races, 
            sample_horse.win_percentage,
            sample_horse.last_5_win_rate * 100;
    END IF;
    
    RAISE NOTICE '============================================================';
END $$;

-- Reset timeouts
RESET statement_timeout;
RESET lock_timeout;

-- ============================================================================
-- USAGE NOTES
-- ============================================================================
-- 
-- WHEN TO RE-RUN:
--   - After major data import/backfill
--   - If ELO values seem incorrect
--   - Monthly as part of full rebuild
--
-- ELO PROGRESSION:
--   - Base K-Factor: 32
--   - Win: +25.6 | 2nd: +12.8 | 3rd: +3.2
--   - Mid-field: -6.4 | Back: -16
--   - Group 1 races: 1.5x multiplier
--   - Large fields (14+): 1.2x multiplier
--
-- HORSE SYNC:
--   - Creates horse records from horse_current_form
--   - Uses INITCAP(REPLACE(slug, '-', ' ')) for horse_name
--   - Safe to run multiple times (ON CONFLICT DO NOTHING)
-- ============================================================================
