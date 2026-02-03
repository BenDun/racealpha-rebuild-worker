-- ============================================================================
-- PHASE 2: CAREER & FORM STATS
-- ============================================================================
-- Consolidates: 04_core_dataset (stats), 05b_elo_ratings, 06_last5_stats,
--               jockey/trainer stats from multiple scripts
-- Est. Time: 25-35 minutes
-- 
-- WHAT THIS DOES:
--   1. Career stats with anti-leakage (wins, places, percentages)
--   2. Days since last race
--   3. Last 5 race stats (win rate, place rate, avg position, form momentum)
--   4. Horse ELO ratings (location-aware)
--   5. Jockey win/place rates
--   6. Trainer win/place rates
--   7. Cross-region horse flags
--   8. First-timer flags
-- ============================================================================

SET statement_timeout = '0';
SET lock_timeout = '0';
SET work_mem = '512MB';  -- Boost for heavy window functions

DO $$ BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 2: CAREER & FORM STATS - Started at %', NOW();
    RAISE NOTICE '============================================================';
END $$;

-- ============================================================================
-- STEP 1: HORSE CAREER STATS (Anti-leakage)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.1] Calculating horse career stats (this takes ~10 min)...'; END $$;

WITH horse_career AS (
    SELECT 
        race_id,
        horse_location_slug,
        -- Prior race counts (EXCLUDE current race)
        COUNT(*) OVER w_prior as prior_total_races,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as prior_wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w_prior as prior_places,
        -- Days since last race
        LAG(race_date) OVER w_order as prev_race_date
    FROM race_training_dataset
    WHERE final_position IS NOT NULL 
      AND horse_location_slug IS NOT NULL
    WINDOW 
        w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w_order AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id)
)
UPDATE race_training_dataset rtd
SET 
    total_races = COALESCE(hc.prior_total_races, 0),
    wins = COALESCE(hc.prior_wins, 0),
    places = COALESCE(hc.prior_places, 0),
    win_percentage = CASE 
        WHEN COALESCE(hc.prior_total_races, 0) > 0 
        THEN ROUND(hc.prior_wins::numeric / hc.prior_total_races * 100, 2) 
        ELSE 0 
    END,
    place_percentage = CASE 
        WHEN COALESCE(hc.prior_total_races, 0) > 0 
        THEN ROUND(hc.prior_places::numeric / hc.prior_total_races * 100, 2) 
        ELSE 0 
    END,
    is_first_timer = (COALESCE(hc.prior_total_races, 0) = 0),
    days_since_last_race = CASE 
        WHEN hc.prev_race_date IS NOT NULL 
        THEN (rtd.race_date - hc.prev_race_date)::integer 
        ELSE NULL 
    END
FROM horse_career hc
WHERE rtd.race_id = hc.race_id 
  AND rtd.horse_location_slug = hc.horse_location_slug;

DO $$ BEGIN RAISE NOTICE '[2.1] Horse career stats complete'; END $$;

-- ============================================================================
-- STEP 2: LAST 5 RACE STATS (Anti-leakage - Fixes "She's A Hustler" bug)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.2] Calculating last 5 race stats...'; END $$;

WITH last_5_data AS (
    SELECT 
        race_id,
        horse_location_slug,
        final_position,
        -- Previous 5 positions (EXCLUDE current race)
        LAG(final_position, 1) OVER w as prev_1,
        LAG(final_position, 2) OVER w as prev_2,
        LAG(final_position, 3) OVER w as prev_3,
        LAG(final_position, 4) OVER w as prev_4,
        LAG(final_position, 5) OVER w as prev_5,
        -- Count of previous races
        COUNT(*) OVER (
            PARTITION BY horse_location_slug 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) as prev_count
    FROM race_training_dataset
    WHERE final_position IS NOT NULL 
      AND final_position < 50
      AND horse_location_slug IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id)
),
calculated AS (
    SELECT 
        race_id,
        horse_location_slug,
        prev_count,
        LEAST(prev_count, 5) as races_used,
        
        -- Average position (last 5 or fewer)
        CASE 
            WHEN prev_count = 0 THEN NULL
            WHEN prev_count = 1 THEN prev_1::numeric
            WHEN prev_count = 2 THEN (prev_1 + prev_2)::numeric / 2
            WHEN prev_count = 3 THEN (prev_1 + prev_2 + prev_3)::numeric / 3
            WHEN prev_count = 4 THEN (prev_1 + prev_2 + prev_3 + prev_4)::numeric / 4
            ELSE (prev_1 + prev_2 + prev_3 + prev_4 + prev_5)::numeric / 5
        END as avg_pos,
        
        -- Win rate (last 5)
        CASE 
            WHEN prev_count = 0 THEN 0
            ELSE (
                (CASE WHEN prev_1 = 1 THEN 1 ELSE 0 END) +
                (CASE WHEN prev_2 = 1 THEN 1 ELSE 0 END) +
                (CASE WHEN prev_3 = 1 THEN 1 ELSE 0 END) +
                (CASE WHEN prev_4 = 1 THEN 1 ELSE 0 END) +
                (CASE WHEN prev_5 = 1 THEN 1 ELSE 0 END)
            )::numeric / LEAST(prev_count, 5)
        END as win_rate,
        
        -- Place rate (last 5)
        CASE 
            WHEN prev_count = 0 THEN 0
            ELSE (
                (CASE WHEN prev_1 <= 3 THEN 1 ELSE 0 END) +
                (CASE WHEN prev_2 <= 3 THEN 1 ELSE 0 END) +
                (CASE WHEN prev_3 <= 3 THEN 1 ELSE 0 END) +
                (CASE WHEN prev_4 <= 3 THEN 1 ELSE 0 END) +
                (CASE WHEN prev_5 <= 3 THEN 1 ELSE 0 END)
            )::numeric / LEAST(prev_count, 5)
        END as place_rate,
        
        -- Form momentum (recency-weighted placing)
        CASE 
            WHEN prev_count = 0 THEN 0
            WHEN prev_count = 1 THEN (CASE WHEN prev_1 <= 3 THEN 1 ELSE 0 END)::numeric
            WHEN prev_count = 2 THEN (
                (CASE WHEN prev_1 <= 3 THEN 2 ELSE 0 END) + 
                (CASE WHEN prev_2 <= 3 THEN 1 ELSE 0 END)
            )::numeric / 3
            WHEN prev_count = 3 THEN (
                (CASE WHEN prev_1 <= 3 THEN 3 ELSE 0 END) + 
                (CASE WHEN prev_2 <= 3 THEN 2 ELSE 0 END) +
                (CASE WHEN prev_3 <= 3 THEN 1 ELSE 0 END)
            )::numeric / 6
            WHEN prev_count = 4 THEN (
                (CASE WHEN prev_1 <= 3 THEN 4 ELSE 0 END) + 
                (CASE WHEN prev_2 <= 3 THEN 3 ELSE 0 END) +
                (CASE WHEN prev_3 <= 3 THEN 2 ELSE 0 END) +
                (CASE WHEN prev_4 <= 3 THEN 1 ELSE 0 END)
            )::numeric / 10
            ELSE (
                (CASE WHEN prev_1 <= 3 THEN 5 ELSE 0 END) + 
                (CASE WHEN prev_2 <= 3 THEN 4 ELSE 0 END) +
                (CASE WHEN prev_3 <= 3 THEN 3 ELSE 0 END) +
                (CASE WHEN prev_4 <= 3 THEN 2 ELSE 0 END) +
                (CASE WHEN prev_5 <= 3 THEN 1 ELSE 0 END)
            )::numeric / 15
        END as momentum
    FROM last_5_data
)
UPDATE race_training_dataset rtd
SET 
    last_5_avg_position = ROUND(c.avg_pos, 2),
    last_5_win_rate = ROUND(c.win_rate, 4),
    last_5_place_rate = ROUND(c.place_rate, 4),
    form_momentum = ROUND(c.momentum, 4)
FROM calculated c
WHERE rtd.race_id = c.race_id 
  AND rtd.horse_location_slug = c.horse_location_slug;

-- Set defaults for first-timers
UPDATE race_training_dataset
SET 
    last_5_avg_position = NULL,
    last_5_win_rate = 0,
    last_5_place_rate = 0,
    form_momentum = 0
WHERE is_first_timer = TRUE OR total_races = 0;

DO $$ BEGIN RAISE NOTICE '[2.2] Last 5 stats complete'; END $$;

-- ============================================================================
-- STEP 3: FORM RECENCY SCORE (days since last race impact)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.3] Calculating form recency score...'; END $$;

UPDATE race_training_dataset
SET form_recency_score = CASE
    WHEN days_since_last_race IS NULL THEN 0.5  -- First timer
    WHEN days_since_last_race <= 14 THEN 1.0   -- Fresh
    WHEN days_since_last_race <= 28 THEN 0.9   -- Good
    WHEN days_since_last_race <= 42 THEN 0.8   -- Acceptable
    WHEN days_since_last_race <= 60 THEN 0.6   -- Concerning
    WHEN days_since_last_race <= 90 THEN 0.4   -- Long spell
    ELSE 0.2  -- Very long spell
END;

-- ============================================================================
-- STEP 4: HORSE ELO RATINGS (Location-aware)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.4] Calculating horse ELO ratings (this takes ~5 min)...'; END $$;

-- Simple ELO approach: Start at 1500, adjust based on performance
WITH elo_calc AS (
    SELECT 
        race_id,
        horse_location_slug,
        -- Base ELO: 1500 + adjustments for win percentage and experience
        1500 + 
        COALESCE(win_percentage, 0) * 5 +  -- +5 per % win rate
        LEAST(COALESCE(total_races, 0), 50) * 2 +  -- +2 per race up to 50
        CASE WHEN COALESCE(place_percentage, 0) > 50 THEN 50 ELSE 0 END +  -- Bonus for consistent placers
        CASE 
            WHEN track_category = 'Metro' THEN 50
            WHEN track_category = 'Provincial' THEN 25
            ELSE 0
        END as calc_elo
    FROM race_training_dataset
    WHERE horse_location_slug IS NOT NULL
)
UPDATE race_training_dataset rtd
SET 
    horse_elo = ROUND(LEAST(GREATEST(ec.calc_elo, 1200), 2000), 0),
    is_elo_default = (rtd.total_races = 0 OR rtd.total_races IS NULL)
FROM elo_calc ec
WHERE rtd.race_id = ec.race_id 
  AND rtd.horse_location_slug = ec.horse_location_slug;

-- Set default ELO for horses with no data
UPDATE race_training_dataset
SET 
    horse_elo = 1500,
    is_elo_default = TRUE
WHERE horse_elo IS NULL;

DO $$ BEGIN RAISE NOTICE '[2.4] Horse ELO complete'; END $$;

-- ============================================================================
-- STEP 5: JOCKEY STATS (Anti-leakage)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.5] Calculating jockey stats...'; END $$;

WITH jockey_stats AS (
    SELECT 
        race_id,
        jockey_location_slug,
        COUNT(*) OVER w_prior as prior_rides,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as prior_wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w_prior as prior_places
    FROM race_training_dataset
    WHERE final_position IS NOT NULL 
      AND jockey_location_slug IS NOT NULL
    WINDOW w_prior AS (
        PARTITION BY jockey_location_slug 
        ORDER BY race_date, race_id 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)
UPDATE race_training_dataset rtd
SET 
    jockey_win_rate = CASE 
        WHEN COALESCE(js.prior_rides, 0) > 0 
        THEN ROUND(js.prior_wins::numeric / js.prior_rides, 4) 
        ELSE 0 
    END,
    jockey_place_rate = CASE 
        WHEN COALESCE(js.prior_rides, 0) > 0 
        THEN ROUND(js.prior_places::numeric / js.prior_rides, 4) 
        ELSE 0 
    END
FROM jockey_stats js
WHERE rtd.race_id = js.race_id 
  AND rtd.jockey_location_slug = js.jockey_location_slug;

DO $$ BEGIN RAISE NOTICE '[2.5] Jockey stats complete'; END $$;

-- ============================================================================
-- STEP 6: TRAINER STATS (Anti-leakage)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.6] Calculating trainer stats...'; END $$;

WITH trainer_stats AS (
    SELECT 
        race_id,
        trainer_location_slug,
        COUNT(*) OVER w_prior as prior_runners,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as prior_wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w_prior as prior_places
    FROM race_training_dataset
    WHERE final_position IS NOT NULL 
      AND trainer_location_slug IS NOT NULL
    WINDOW w_prior AS (
        PARTITION BY trainer_location_slug 
        ORDER BY race_date, race_id 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)
UPDATE race_training_dataset rtd
SET 
    trainer_win_rate = CASE 
        WHEN COALESCE(ts.prior_runners, 0) > 0 
        THEN ROUND(ts.prior_wins::numeric / ts.prior_runners, 4) 
        ELSE 0 
    END,
    trainer_place_rate = CASE 
        WHEN COALESCE(ts.prior_runners, 0) > 0 
        THEN ROUND(ts.prior_places::numeric / ts.prior_runners, 4) 
        ELSE 0 
    END
FROM trainer_stats ts
WHERE rtd.race_id = ts.race_id 
  AND rtd.trainer_location_slug = ts.trainer_location_slug;

DO $$ BEGIN RAISE NOTICE '[2.6] Trainer stats complete'; END $$;

-- ============================================================================
-- STEP 7: DISTANCE WIN RATES (Anti-leakage)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.7] Calculating distance-specific win rates...'; END $$;

WITH distance_stats AS (
    SELECT 
        race_id,
        horse_location_slug,
        distance_range,
        COUNT(*) OVER w_dist as prior_at_dist,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_dist as wins_at_dist
    FROM race_training_dataset
    WHERE final_position IS NOT NULL 
      AND horse_location_slug IS NOT NULL
      AND distance_range IS NOT NULL
    WINDOW w_dist AS (
        PARTITION BY horse_location_slug, distance_range 
        ORDER BY race_date, race_id 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)
UPDATE race_training_dataset rtd
SET 
    distance_win_rate = CASE 
        WHEN COALESCE(ds.prior_at_dist, 0) > 0 
        THEN ROUND(ds.wins_at_dist::numeric / ds.prior_at_dist, 4) 
        ELSE 0 
    END,
    races_at_distance = COALESCE(ds.prior_at_dist, 0)
FROM distance_stats ds
WHERE rtd.race_id = ds.race_id 
  AND rtd.horse_location_slug = ds.horse_location_slug;

DO $$ BEGIN RAISE NOTICE '[2.7] Distance stats complete'; END $$;

-- ============================================================================
-- STEP 8: TRACK WIN RATES (Anti-leakage)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.8] Calculating track-specific win rates...'; END $$;

WITH track_stats AS (
    SELECT 
        race_id,
        horse_location_slug,
        track_name,
        COUNT(*) OVER w_track as prior_at_track,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_track as wins_at_track
    FROM race_training_dataset
    WHERE final_position IS NOT NULL 
      AND horse_location_slug IS NOT NULL
      AND track_name IS NOT NULL
    WINDOW w_track AS (
        PARTITION BY horse_location_slug, track_name 
        ORDER BY race_date, race_id 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)
UPDATE race_training_dataset rtd
SET 
    track_win_rate = CASE 
        WHEN COALESCE(ts.prior_at_track, 0) > 0 
        THEN ROUND(ts.wins_at_track::numeric / ts.prior_at_track, 4) 
        ELSE 0 
    END,
    races_at_track = COALESCE(ts.prior_at_track, 0)
FROM track_stats ts
WHERE rtd.race_id = ts.race_id 
  AND rtd.horse_location_slug = ts.horse_location_slug;

DO $$ BEGIN RAISE NOTICE '[2.8] Track stats complete'; END $$;

-- ============================================================================
-- STEP 9: CROSS-REGION FLAGS
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[2.9] Setting cross-region flags...'; END $$;

WITH multi_region AS (
    SELECT horse_slug 
    FROM race_training_dataset
    WHERE horse_slug IS NOT NULL
    GROUP BY horse_slug 
    HAVING COUNT(DISTINCT location) > 1
)
UPDATE race_training_dataset rtd
SET is_cross_region_horse = TRUE
FROM multi_region mr 
WHERE rtd.horse_slug = mr.horse_slug;

-- Set false for others
UPDATE race_training_dataset
SET is_cross_region_horse = FALSE
WHERE is_cross_region_horse IS NULL;

-- ============================================================================
-- STEP 10: NEVER PLACED FLAG
-- ============================================================================
UPDATE race_training_dataset
SET never_placed_flag = (total_races > 0 AND places = 0);

-- ============================================================================
-- COMPLETION
-- ============================================================================
DO $$
DECLARE
    total_count INTEGER;
    first_timers INTEGER;
    cross_region INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_count FROM race_training_dataset;
    SELECT COUNT(*) INTO first_timers FROM race_training_dataset WHERE is_first_timer = TRUE;
    SELECT COUNT(*) INTO cross_region FROM race_training_dataset WHERE is_cross_region_horse = TRUE;
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 2 COMPLETE';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Total records: %', total_count;
    RAISE NOTICE 'First timers: %', first_timers;
    RAISE NOTICE 'Cross-region horses: %', cross_region;
    RAISE NOTICE '';
    RAISE NOTICE 'Next: Run 03_advanced_features.sql';
END $$;
