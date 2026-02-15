-- ============================================================================
-- PHASE 4 RESUME: From Step 4.6c to End
-- ============================================================================
-- Run this in pgAdmin to complete Phase 4
-- Picks up where the script timed out
-- ============================================================================

SET statement_timeout = '0';
SET lock_timeout = '0';
SET work_mem = '512MB';

DO $$ BEGIN RAISE NOTICE 'RESUMING PHASE 4 from Step 4.6c at %', NOW(); END $$;

-- ============================================================================
-- STEP 6C: CLASS AND STEPPING FEATURES (OPTIMIZED)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6c] Calculating class features...'; END $$;

-- Class level numeric (convert text to number)
UPDATE race_training_dataset
SET class_level_numeric = CASE
    WHEN race_class ILIKE '%group 1%' OR race_class ILIKE '%g1%' THEN 1
    WHEN race_class ILIKE '%group 2%' OR race_class ILIKE '%g2%' THEN 2
    WHEN race_class ILIKE '%group 3%' OR race_class ILIKE '%g3%' THEN 3
    WHEN race_class ILIKE '%listed%' THEN 4
    WHEN race_class ILIKE '%open%' THEN 5
    WHEN race_class ILIKE '%bm90%' OR race_class ILIKE '%benchmark 90%' THEN 6
    WHEN race_class ILIKE '%bm85%' OR race_class ILIKE '%benchmark 85%' THEN 7
    WHEN race_class ILIKE '%bm80%' OR race_class ILIKE '%benchmark 80%' THEN 8
    WHEN race_class ILIKE '%bm78%' OR race_class ILIKE '%benchmark 78%' THEN 9
    WHEN race_class ILIKE '%bm75%' OR race_class ILIKE '%benchmark 75%' THEN 10
    WHEN race_class ILIKE '%bm72%' OR race_class ILIKE '%benchmark 72%' THEN 11
    WHEN race_class ILIKE '%bm70%' OR race_class ILIKE '%benchmark 70%' THEN 12
    WHEN race_class ILIKE '%bm68%' OR race_class ILIKE '%benchmark 68%' THEN 13
    WHEN race_class ILIKE '%bm66%' OR race_class ILIKE '%benchmark 66%' THEN 14
    WHEN race_class ILIKE '%bm64%' OR race_class ILIKE '%benchmark 64%' THEN 15
    WHEN race_class ILIKE '%bm62%' OR race_class ILIKE '%benchmark 62%' THEN 16
    WHEN race_class ILIKE '%bm58%' OR race_class ILIKE '%benchmark 58%' THEN 17
    WHEN race_class ILIKE '%bm55%' OR race_class ILIKE '%benchmark 55%' THEN 18
    WHEN race_class ILIKE '%maiden%' THEN 20
    ELSE 15
END;

DO $$ BEGIN RAISE NOTICE '[4.6c] class_level_numeric populated'; END $$;

-- Stepping up/down (OPTIMIZED - using CTE with DISTINCT ON)
CREATE INDEX IF NOT EXISTS idx_rtd_horse_date_id
ON race_training_dataset(horse_location_slug, race_date, race_id);

DO $$ BEGIN RAISE NOTICE '[4.6c] Calculating stepping up/down (optimized)...'; END $$;

WITH prev_race AS (
    SELECT DISTINCT ON (curr.race_id, curr.horse_location_slug)
        curr.race_id AS curr_race_id,
        curr.horse_location_slug AS curr_horse,
        curr.class_level_numeric AS curr_class,
        prev.class_level_numeric AS prev_class
    FROM race_training_dataset curr
    JOIN race_training_dataset prev 
        ON prev.horse_location_slug = curr.horse_location_slug
        AND (prev.race_date < curr.race_date 
             OR (prev.race_date = curr.race_date AND prev.race_id < curr.race_id))
    WHERE curr.horse_location_slug IS NOT NULL
    ORDER BY curr.race_id, curr.horse_location_slug, prev.race_date DESC, prev.race_id DESC
)
UPDATE race_training_dataset rtd
SET 
    is_stepping_up = (rtd.class_level_numeric < pr.prev_class),
    is_stepping_down = (rtd.class_level_numeric > pr.prev_class)
FROM prev_race pr
WHERE rtd.race_id = pr.curr_race_id
  AND rtd.horse_location_slug = pr.curr_horse;

DO $$ BEGIN RAISE NOTICE '[4.6c] ✓ Class features complete'; END $$;

-- ============================================================================
-- STEP 6D: TRACK DISTANCE FEATURES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6d] Calculating track-distance features...'; END $$;

WITH track_dist_exp AS (
    SELECT 
        curr.race_id AS curr_race_id,
        curr.horse_location_slug AS curr_horse,
        COUNT(prev.*) AS exp,
        SUM(CASE WHEN prev.final_position = 1 THEN 1 ELSE 0 END) AS wins
    FROM race_training_dataset curr
    LEFT JOIN race_training_dataset prev 
        ON prev.horse_location_slug = curr.horse_location_slug
        AND prev.track_name = curr.track_name
        AND prev.distance_range = curr.distance_range
        AND (prev.race_date < curr.race_date 
             OR (prev.race_date = curr.race_date AND prev.race_id < curr.race_id))
    WHERE curr.horse_location_slug IS NOT NULL 
      AND curr.track_name IS NOT NULL 
      AND curr.distance_range IS NOT NULL
    GROUP BY curr.race_id, curr.horse_location_slug
)
UPDATE race_training_dataset rtd
SET 
    track_distance_experience = COALESCE(tde.exp, 0),
    track_distance_win_rate_weighted = CASE 
        WHEN COALESCE(tde.exp, 0) > 0 
        THEN ROUND((tde.wins::numeric / tde.exp * (1 + LN(tde.exp + 1) / 5))::numeric, 4)
        ELSE 0 
    END
FROM track_dist_exp tde
WHERE rtd.race_id = tde.curr_race_id
  AND rtd.horse_location_slug = tde.curr_horse;

DO $$ BEGIN RAISE NOTICE '[4.6d] ✓ Track-distance features complete'; END $$;

-- ============================================================================
-- STEP 6E: DISTANCE VARIANCE & OPTIMAL DISTANCE
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6e] Calculating distance variance...'; END $$;

-- Distance variance using aggregation per horse (faster than window)
WITH horse_dist_stats AS (
    SELECT 
        horse_location_slug,
        STDDEV(race_distance) AS dist_var
    FROM race_training_dataset
    WHERE horse_location_slug IS NOT NULL
    GROUP BY horse_location_slug
)
UPDATE race_training_dataset rtd
SET distance_variance = ROUND(COALESCE(hds.dist_var, 0)::numeric, 2)
FROM horse_dist_stats hds
WHERE rtd.horse_location_slug = hds.horse_location_slug;

-- Optimal distance range (already aggregated - fast)
WITH winning_distances AS (
    SELECT 
        horse_location_slug,
        MIN(race_distance) as min_win_dist,
        MAX(race_distance) as max_win_dist
    FROM race_training_dataset
    WHERE horse_location_slug IS NOT NULL AND final_position = 1
    GROUP BY horse_location_slug
)
UPDATE race_training_dataset rtd
SET 
    optimal_distance_min = COALESCE(wd.min_win_dist, 1000),
    optimal_distance_max = COALESCE(wd.max_win_dist, 2400)
FROM winning_distances wd
WHERE rtd.horse_location_slug = wd.horse_location_slug;

DO $$ BEGIN RAISE NOTICE '[4.6e] ✓ Distance variance complete'; END $$;

-- ============================================================================
-- STEP 6F: MODEL PREDICTION HISTORY
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6f] Calculating model prediction history...'; END $$;

WITH pred_history AS (
    SELECT 
        horse_location_slug,
        COUNT(*) as prediction_count,
        AVG(ABS(COALESCE(final_position, 5) - 3)) as avg_pos_error
    FROM race_training_dataset
    WHERE horse_location_slug IS NOT NULL AND final_position IS NOT NULL
    GROUP BY horse_location_slug
)
UPDATE race_training_dataset rtd
SET 
    horse_prediction_history_count = ph.prediction_count,
    horse_model_pos_error = ROUND(ph.avg_pos_error::numeric, 4)
FROM pred_history ph
WHERE rtd.horse_location_slug = ph.horse_location_slug;

DO $$ BEGIN RAISE NOTICE '[4.6f] ✓ Model prediction history complete'; END $$;

-- ============================================================================
-- STEP 6G: SPECIALTY & DEBUTANT FEATURES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6g] Calculating specialty features...'; END $$;

UPDATE race_training_dataset
SET 
    specialization_score = ROUND(
        (
            CASE WHEN races_at_track >= 5 THEN 0.3 ELSE 0 END +
            CASE WHEN races_at_distance >= 5 THEN 0.3 ELSE 0 END +
            CASE WHEN COALESCE(distance_range_experience, 0) >= 5 THEN 0.2 ELSE 0 END
        ), 4
    ),
    debutant_predicted_ability = CASE
        WHEN total_races = 0 OR total_races IS NULL THEN
            ROUND(0.5 + COALESCE(trainer_win_rate, 0) * 0.3 + COALESCE(jockey_win_rate, 0) * 0.2, 4)
        ELSE NULL
    END;

DO $$ BEGIN RAISE NOTICE '[4.6g] ✓ Specialty features complete'; END $$;

-- ============================================================================
-- STEP 6H: MISC COLUMNS
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6h] Setting misc columns...'; END $$;

UPDATE race_training_dataset
SET 
    age_restriction = COALESCE(age_restriction, 'open'),
    sex_restriction = COALESCE(sex_restriction, 'open')
WHERE age_restriction IS NULL OR sex_restriction IS NULL;

DO $$ BEGIN RAISE NOTICE '[4.6h] ✓ Misc columns complete'; END $$;

-- ============================================================================
-- STEP 7: RECREATE INDEXES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.7] Creating indexes...'; END $$;

CREATE INDEX IF NOT EXISTS idx_rtd_horse_slug ON race_training_dataset(horse_slug);
CREATE INDEX IF NOT EXISTS idx_rtd_race_date ON race_training_dataset(race_date);
CREATE INDEX IF NOT EXISTS idx_rtd_track_name ON race_training_dataset(track_name);
CREATE INDEX IF NOT EXISTS idx_rtd_jockey_slug ON race_training_dataset(jockey_slug);
CREATE INDEX IF NOT EXISTS idx_rtd_trainer_slug ON race_training_dataset(trainer_slug);
CREATE INDEX IF NOT EXISTS idx_rtd_horse_location_slug ON race_training_dataset(horse_location_slug);
CREATE INDEX IF NOT EXISTS idx_rtd_race_id ON race_training_dataset(race_id);
CREATE INDEX IF NOT EXISTS idx_rtd_location ON race_training_dataset(location);
CREATE INDEX IF NOT EXISTS idx_rtd_running_style ON race_training_dataset(running_style);
CREATE INDEX IF NOT EXISTS idx_rtd_track_category ON race_training_dataset(track_category);

DO $$ BEGIN RAISE NOTICE '[4.7] ✓ Indexes created'; END $$;

-- ============================================================================
-- STEP 8: ENHANCED SECTIONAL FEATURES (SKIPPED - already populated from Phase 3)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.8] Skipping sectional features (already populated)'; END $$;

-- ============================================================================
-- STEP 9: METRO WIN FEATURES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.9] Adding metro win features...'; END $$;

-- Check if columns exist, add if not
ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS has_won_at_metro INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS days_since_metro_win INTEGER DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS metro_wins_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS metro_win_rate NUMERIC(5,4) DEFAULT NULL;

WITH metro_tracks AS (
    SELECT track_name FROM (VALUES 
        ('Flemington'), ('Caulfield'), ('Moonee Valley'), ('The Valley'),
        ('Randwick'), ('Royal Randwick'), ('Rosehill'), ('Rosehill Gardens'),
        ('Canterbury'), ('Warwick Farm'), ('Eagle Farm'), ('Doomben'),
        ('Morphettville'), ('Ascot'), ('Belmont'), ('Sha Tin'), ('Happy Valley')
    ) AS t(track_name)
),
metro_stats AS (
    SELECT 
        horse_slug,
        SUM(CASE WHEN final_position = 1 AND track_name IN (SELECT track_name FROM metro_tracks) THEN 1 ELSE 0 END) AS metro_wins,
        SUM(CASE WHEN track_name IN (SELECT track_name FROM metro_tracks) THEN 1 ELSE 0 END) AS metro_starts,
        MAX(CASE WHEN final_position = 1 AND track_name IN (SELECT track_name FROM metro_tracks) THEN race_date END) AS last_metro_win
    FROM race_training_dataset
    WHERE horse_slug IS NOT NULL
    GROUP BY horse_slug
)
UPDATE race_training_dataset rtd
SET 
    has_won_at_metro = CASE WHEN ms.metro_wins > 0 THEN 1 ELSE 0 END,
    metro_wins_count = COALESCE(ms.metro_wins, 0),
    metro_win_rate = CASE WHEN ms.metro_starts > 0 THEN ROUND(ms.metro_wins::numeric / ms.metro_starts, 4) ELSE NULL END,
    days_since_metro_win = CASE WHEN ms.last_metro_win IS NOT NULL THEN (rtd.race_date - ms.last_metro_win)::INTEGER ELSE NULL END
FROM metro_stats ms
WHERE rtd.horse_slug = ms.horse_slug;

DO $$ BEGIN RAISE NOTICE '[4.9] ✓ Metro win features complete'; END $$;

-- ============================================================================
-- STEP 10: WEATHER FEATURES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.10] Adding weather features...'; END $$;

ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS race_weather TEXT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS is_wet_weather INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS estimated_temperature INTEGER DEFAULT NULL;

UPDATE race_training_dataset rtd SET
    race_weather = r.race_weather,
    is_wet_weather = CASE WHEN LOWER(r.race_weather) LIKE '%rain%' THEN 1 ELSE 0 END
FROM races r WHERE rtd.race_id = r.race_id AND r.race_weather IS NOT NULL;

UPDATE race_training_dataset SET
    estimated_temperature = CASE
        WHEN track_name IN ('Happy Valley', 'Sha Tin') THEN
            CASE EXTRACT(MONTH FROM race_date)
                WHEN 1 THEN 17 WHEN 2 THEN 17 WHEN 3 THEN 20 WHEN 4 THEN 24
                WHEN 5 THEN 27 WHEN 6 THEN 29 WHEN 7 THEN 31 WHEN 8 THEN 31
                WHEN 9 THEN 29 WHEN 10 THEN 26 WHEN 11 THEN 22 WHEN 12 THEN 18
            END
        WHEN location = 'AU' THEN
            CASE EXTRACT(MONTH FROM race_date)
                WHEN 1 THEN 28 WHEN 2 THEN 27 WHEN 3 THEN 24 WHEN 4 THEN 20
                WHEN 5 THEN 16 WHEN 6 THEN 13 WHEN 7 THEN 12 WHEN 8 THEN 14
                WHEN 9 THEN 17 WHEN 10 THEN 20 WHEN 11 THEN 23 WHEN 12 THEN 26
            END
        ELSE 20
    END;

DO $$ BEGIN RAISE NOTICE '[4.10] ✓ Weather features complete'; END $$;

-- ============================================================================
-- STEP 11: TRACK SPECIALIST FEATURES (SIMPLIFIED - already have has_won_at_*)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.11] Setting track specialist flags...'; END $$;

ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS has_won_at_track BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_won_at_distance BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_won_at_track_distance BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_track_specialist BOOLEAN DEFAULT FALSE;

-- Fast aggregation approach
WITH horse_track_wins AS (
    SELECT horse_slug, track_name, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) AS wins
    FROM race_training_dataset WHERE horse_slug IS NOT NULL GROUP BY horse_slug, track_name
),
horse_dist_wins AS (
    SELECT horse_slug, distance_range, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) AS wins
    FROM race_training_dataset WHERE horse_slug IS NOT NULL GROUP BY horse_slug, distance_range
),
horse_td_wins AS (
    SELECT horse_slug, track_name, distance_range, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) AS wins
    FROM race_training_dataset WHERE horse_slug IS NOT NULL GROUP BY horse_slug, track_name, distance_range
)
UPDATE race_training_dataset rtd
SET 
    has_won_at_track = COALESCE((SELECT htw.wins > 0 FROM horse_track_wins htw WHERE htw.horse_slug = rtd.horse_slug AND htw.track_name = rtd.track_name), FALSE),
    has_won_at_distance = COALESCE((SELECT hdw.wins > 0 FROM horse_dist_wins hdw WHERE hdw.horse_slug = rtd.horse_slug AND hdw.distance_range = rtd.distance_range), FALSE),
    has_won_at_track_distance = COALESCE((SELECT htdw.wins > 0 FROM horse_td_wins htdw WHERE htdw.horse_slug = rtd.horse_slug AND htdw.track_name = rtd.track_name AND htdw.distance_range = rtd.distance_range), FALSE);

UPDATE race_training_dataset SET
    is_track_specialist = (COALESCE(track_win_rate, 0) > COALESCE(win_percentage, 0) * 1.5 AND COALESCE(track_win_rate, 0) > 0 AND total_races >= 5),
    track_distance_preference = CASE
        WHEN has_won_at_track_distance THEN 0.9
        WHEN has_won_at_track OR has_won_at_distance THEN 0.6
        ELSE 0.3
    END;

DO $$ BEGIN RAISE NOTICE '[4.11] ✓ Track specialist features complete'; END $$;

-- ============================================================================
-- STEP 12: DEBUTANT FEATURES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.12] Adding debutant features...'; END $$;

ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS experienced_horse_flag BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS debutant_x_jockey_wr NUMERIC(5,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS debutant_x_trainer_wr NUMERIC(5,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS debutant_uncertainty_score NUMERIC(5,4) DEFAULT NULL;

UPDATE race_training_dataset SET
    experienced_horse_flag = (total_races >= 10),
    debutant_x_jockey_wr = CASE WHEN is_first_timer THEN COALESCE(jockey_win_rate, 0) ELSE NULL END,
    debutant_x_trainer_wr = CASE WHEN is_first_timer THEN COALESCE(trainer_win_rate, 0) ELSE NULL END,
    debutant_uncertainty_score = CASE
        WHEN is_first_timer THEN 1.0
        WHEN total_races <= 3 THEN 0.7
        WHEN total_races <= 6 THEN 0.4
        ELSE 0.1
    END;

DO $$ BEGIN RAISE NOTICE '[4.12] ✓ Debutant features complete'; END $$;

-- ============================================================================
-- STEP 13: JOCKEY/TRAINER AVG POSITION
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.13] Adding jockey/trainer avg position...'; END $$;

ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS jockey_avg_position_20 NUMERIC(5,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS jockey_avg_position_50 NUMERIC(5,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS trainer_avg_position_50 NUMERIC(5,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS trainer_avg_position_100 NUMERIC(5,2) DEFAULT NULL;

-- Jockey stats (using global averages - simpler, faster)
WITH jockey_stats AS (
    SELECT jockey_slug, ROUND(AVG(final_position)::numeric, 2) AS avg_pos
    FROM race_training_dataset
    WHERE jockey_slug IS NOT NULL AND final_position IS NOT NULL AND final_position < 50
    GROUP BY jockey_slug
)
UPDATE race_training_dataset rtd SET
    jockey_avg_position_20 = js.avg_pos,
    jockey_avg_position_50 = js.avg_pos
FROM jockey_stats js WHERE rtd.jockey_slug = js.jockey_slug;

WITH trainer_stats AS (
    SELECT trainer_slug, ROUND(AVG(final_position)::numeric, 2) AS avg_pos
    FROM race_training_dataset
    WHERE trainer_slug IS NOT NULL AND final_position IS NOT NULL AND final_position < 50
    GROUP BY trainer_slug
)
UPDATE race_training_dataset rtd SET
    trainer_avg_position_50 = ts.avg_pos,
    trainer_avg_position_100 = ts.avg_pos
FROM trainer_stats ts WHERE rtd.trainer_slug = ts.trainer_slug;

DO $$ BEGIN RAISE NOTICE '[4.13] ✓ Jockey/trainer avg position complete'; END $$;

-- ============================================================================
-- STEP 14: FINAL VALIDATION
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.14] Running final validation...'; END $$;

SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT race_id) as unique_races,
    COUNT(DISTINCT horse_slug) as unique_horses,
    MIN(race_date) as earliest_race,
    MAX(race_date) as latest_race
FROM race_training_dataset;

-- ============================================================================
-- COMPLETION
-- ============================================================================
DO $$
DECLARE
    final_count INTEGER;
    col_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO final_count FROM race_training_dataset;
    SELECT COUNT(*) INTO col_count FROM information_schema.columns WHERE table_name = 'race_training_dataset';
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 4 COMPLETE';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Total records: %', final_count;
    RAISE NOTICE 'Total columns: %', col_count;
    RAISE NOTICE '============================================================';
END $$;
