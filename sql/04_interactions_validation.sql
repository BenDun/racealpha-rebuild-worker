-- ============================================================================
-- PHASE 4: INTERACTIONS, BARRIER ANALYSIS & VALIDATION
-- ============================================================================
-- Consolidates: 09_interaction_features, 10_barrier_analysis, 11_anti_leakage,
--               12_final_validation, 13_remove_deprecated
-- Est. Time: 15-20 minutes
-- 
-- WHAT THIS DOES:
--   1. Feature interactions (ELO x jockey, barrier x direction, etc.)
--   2. Barrier analysis (track-specific advantages)
--   3. Anti-leakage flags
--   4. Model bias columns
--   5. Final validation
--   6. Recreate indexes
-- ============================================================================

SET statement_timeout = '0';
SET lock_timeout = '0';
SET work_mem = '512MB';

DO $$ BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 4: INTERACTIONS & VALIDATION - Started at %', NOW();
    RAISE NOTICE '============================================================';
END $$;

-- ============================================================================
-- STEP 0: ADD MISSING COLUMNS
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.0] Adding missing columns...'; END $$;

ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS elo_x_jockey_win_rate NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS elo_x_trainer_win_rate NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS weight_x_elo NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS barrier_x_total_runners NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS barrier_x_distance NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS barrier_x_direction NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS running_style_x_direction NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS jockey_wr_x_trainer_wr NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS jockey_place_x_trainer_place NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS form_x_freshness NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS track_wr_x_distance_wr NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS running_style_x_barrier NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS competitive_level_score NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS barrier_advantage_at_track NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS barrier_effectiveness_score NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS horse_barrier_group_starts INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS horse_barrier_group_win_rate NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS extreme_staleness_flag BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS low_sample_flag BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS new_jockey_flag BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS new_trainer_flag BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS horse_model_win_bias NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS horse_model_top3_bias NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS jockey_model_win_bias NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS jockey_model_top3_bias NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS trainer_model_win_bias NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS trainer_model_top3_bias NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS closing_power_score NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS strong_closer_flag BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS barrier_vs_run_style NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS barrier_track_condition_advantage NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS weight_relative_x_distance NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS track_distance_experience INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS track_distance_win_rate_weighted NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS distance_variance NUMERIC(8,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS optimal_distance_min INTEGER DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS optimal_distance_max INTEGER DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS horse_prediction_history_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS horse_model_pos_error NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS specialization_score NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS debutant_predicted_ability NUMERIC(8,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS prize_money NUMERIC(12,2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sectional_800m NUMERIC(8,3) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sectional_400m NUMERIC(8,3) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS age_restriction TEXT DEFAULT 'open',
    ADD COLUMN IF NOT EXISTS sex_restriction TEXT DEFAULT 'open',
    ADD COLUMN IF NOT EXISTS jockey_total_rides INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS trainer_total_runners INTEGER DEFAULT 0,
    -- Step 6 columns
    ADD COLUMN IF NOT EXISTS track_condition_numeric INTEGER DEFAULT 3,
    ADD COLUMN IF NOT EXISTS closing_ability_score NUMERIC(5,4) DEFAULT 0.5,
    ADD COLUMN IF NOT EXISTS sustained_run_score NUMERIC(5,4) DEFAULT 0.5,
    ADD COLUMN IF NOT EXISTS weight_vs_avg NUMERIC(6,2) DEFAULT 0,
    -- Step 6B value columns
    ADD COLUMN IF NOT EXISTS value_indicator NUMERIC(6,4) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS market_undervalued BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS class_drop_longshot BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS connection_longshot BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS improving_longshot BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS elo_vs_odds_gap NUMERIC(6,4) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS form_vs_odds_gap NUMERIC(6,4) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS is_class_drop BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS total_connection_score NUMERIC(5,4) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS odds_rank_in_race INTEGER DEFAULT NULL,
    -- Step 6C class columns
    ADD COLUMN IF NOT EXISTS class_level_numeric INTEGER DEFAULT 15,
    ADD COLUMN IF NOT EXISTS is_stepping_up BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_stepping_down BOOLEAN DEFAULT FALSE,
    -- Step 6G columns (specialization)
    ADD COLUMN IF NOT EXISTS races_at_track INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS races_at_distance INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS track_condition_wins INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS distance_range_experience INTEGER DEFAULT 0,
    -- Step 12 columns (debutant)
    ADD COLUMN IF NOT EXISTS is_first_timer BOOLEAN DEFAULT FALSE;

DO $$ BEGIN RAISE NOTICE '[4.0] Missing columns added'; END $$;

-- Populate track_condition_numeric from track_condition text
UPDATE race_training_dataset
SET track_condition_numeric = CASE
    WHEN track_condition ILIKE '%firm%' OR track_condition ILIKE '%good 1%' THEN 1
    WHEN track_condition ILIKE '%good 2%' OR track_condition ILIKE '%good%' THEN 2
    WHEN track_condition ILIKE '%good 3%' OR track_condition ILIKE '%good 4%' THEN 3
    WHEN track_condition ILIKE '%soft 5%' OR track_condition ILIKE '%soft 6%' THEN 4
    WHEN track_condition ILIKE '%soft 7%' OR track_condition ILIKE '%soft%' THEN 5
    WHEN track_condition ILIKE '%heavy 8%' OR track_condition ILIKE '%heavy 9%' THEN 6
    WHEN track_condition ILIKE '%heavy 10%' OR track_condition ILIKE '%heavy%' THEN 7
    WHEN track_condition ILIKE '%yielding%' THEN 4
    WHEN track_condition ILIKE '%wet%' THEN 5
    ELSE 3
END
WHERE track_condition IS NOT NULL;

DO $$ BEGIN RAISE NOTICE '[4.0c] Track condition numeric populated'; END $$;

-- Populate races_at_track (anti-leakage)
WITH track_exp AS (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER (PARTITION BY horse_location_slug, track_name ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as prior_races
    FROM race_training_dataset WHERE horse_location_slug IS NOT NULL AND track_name IS NOT NULL
)
UPDATE race_training_dataset rtd SET races_at_track = COALESCE(te.prior_races, 0)
FROM track_exp te WHERE rtd.race_id = te.race_id AND rtd.horse_location_slug = te.horse_location_slug;

-- Populate races_at_distance (anti-leakage, bucket to 200m)
WITH dist_exp AS (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER (PARTITION BY horse_location_slug, FLOOR(race_distance/200)*200 ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as prior_races
    FROM race_training_dataset WHERE horse_location_slug IS NOT NULL
)
UPDATE race_training_dataset rtd SET races_at_distance = COALESCE(de.prior_races, 0)
FROM dist_exp de WHERE rtd.race_id = de.race_id AND rtd.horse_location_slug = de.horse_location_slug;

-- Populate track_condition_wins (anti-leakage)
WITH cond_wins AS (
    SELECT race_id, horse_location_slug,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY horse_location_slug, track_condition ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) as prior_wins
    FROM race_training_dataset WHERE horse_location_slug IS NOT NULL AND track_condition IS NOT NULL
)
UPDATE race_training_dataset rtd SET track_condition_wins = COALESCE(cw.prior_wins, 0)
FROM cond_wins cw WHERE rtd.race_id = cw.race_id AND rtd.horse_location_slug = cw.horse_location_slug;

-- Populate distance_range_experience (anti-leakage)
WITH dr_exp AS (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER (PARTITION BY horse_location_slug, distance_range ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as prior_races
    FROM race_training_dataset WHERE horse_location_slug IS NOT NULL AND distance_range IS NOT NULL
)
UPDATE race_training_dataset rtd SET distance_range_experience = COALESCE(dre.prior_races, 0)
FROM dr_exp dre WHERE rtd.race_id = dre.race_id AND rtd.horse_location_slug = dre.horse_location_slug;

-- Set is_first_timer flag
UPDATE race_training_dataset SET is_first_timer = (COALESCE(total_races, 0) = 0);

DO $$ BEGIN RAISE NOTICE '[4.0d] Track/distance experience and first-timer flag populated'; END $$;

-- Populate jockey_total_rides from race_results (anti-leakage)
WITH jockey_ride_counts AS (
    SELECT 
        rr.jockey_slug,
        r.race_date,
        rr.race_id,
        COUNT(*) OVER (
            PARTITION BY rr.jockey_slug 
            ORDER BY r.race_date, rr.race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) as prior_rides
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    WHERE rr.jockey_slug IS NOT NULL
)
UPDATE race_training_dataset rtd
SET jockey_total_rides = COALESCE(jrc.prior_rides, 0)
FROM jockey_ride_counts jrc
WHERE rtd.race_id = jrc.race_id AND rtd.jockey_slug = jrc.jockey_slug;

-- Populate trainer_total_runners from race_results (anti-leakage)
WITH trainer_run_counts AS (
    SELECT 
        rr.trainer_slug,
        r.race_date,
        rr.race_id,
        COUNT(*) OVER (
            PARTITION BY rr.trainer_slug 
            ORDER BY r.race_date, rr.race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) as prior_runners
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    WHERE rr.trainer_slug IS NOT NULL
)
UPDATE race_training_dataset rtd
SET trainer_total_runners = COALESCE(trc.prior_runners, 0)
FROM trainer_run_counts trc
WHERE rtd.race_id = trc.race_id AND rtd.trainer_slug = trc.trainer_slug;

DO $$ BEGIN RAISE NOTICE '[4.0b] Jockey/trainer ride counts populated'; END $$;

-- ============================================================================
-- STEP 1: SIMPLE COLUMN INTERACTIONS (Single UPDATE)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.1] Calculating simple feature interactions...'; END $$;

UPDATE race_training_dataset
SET 
    -- ELO Interactions
    elo_x_jockey_win_rate = ROUND(
        COALESCE(horse_elo, 1500)::numeric / 1500 * COALESCE(jockey_win_rate, 0), 4
    ),
    elo_x_trainer_win_rate = ROUND(
        COALESCE(horse_elo, 1500)::numeric / 1500 * COALESCE(trainer_win_rate, 0), 4
    ),
    weight_x_elo = ROUND(
        COALESCE(weight, 55)::numeric / 55 * COALESCE(horse_elo, 1500)::numeric / 1500, 4
    ),
    
    -- Barrier Interactions
    barrier_x_total_runners = ROUND(
        CASE WHEN COALESCE(total_runners, 1) > 0 
             THEN COALESCE(barrier, 1)::numeric / total_runners
             ELSE 0.5 END, 4
    ),
    barrier_x_distance = ROUND(
        CASE WHEN COALESCE(race_distance, 1200) > 0 
             THEN COALESCE(barrier, 1)::numeric / (race_distance::numeric / 200)
             ELSE 0 END, 4
    ),
    
    -- Track Direction Interactions
    barrier_x_direction = ROUND(
        CASE 
            WHEN track_direction = 'clockwise' THEN
                (1 - COALESCE(barrier, 1)::numeric / NULLIF(total_runners, 0)) * 1
            WHEN track_direction = 'anticlockwise' THEN
                (1 - COALESCE(barrier, 1)::numeric / NULLIF(total_runners, 0)) * -1
            ELSE 0
        END, 4
    ),
    running_style_x_direction = ROUND(
        CASE 
            WHEN LOWER(running_style) = 'leader' THEN
                CASE WHEN track_direction = 'clockwise' THEN 1.0
                     WHEN track_direction = 'anticlockwise' THEN -1.0 ELSE 0 END
            WHEN LOWER(running_style) IN ('on_pace', 'on pace', 'stalker') THEN
                CASE WHEN track_direction = 'clockwise' THEN 0.5
                     WHEN track_direction = 'anticlockwise' THEN -0.5 ELSE 0 END
            WHEN LOWER(running_style) = 'midfield' THEN 0
            WHEN LOWER(running_style) IN ('off_pace', 'off pace') THEN
                CASE WHEN track_direction = 'clockwise' THEN -0.5
                     WHEN track_direction = 'anticlockwise' THEN 0.5 ELSE 0 END
            WHEN LOWER(running_style) = 'closer' THEN
                CASE WHEN track_direction = 'clockwise' THEN -1.0
                     WHEN track_direction = 'anticlockwise' THEN 1.0 ELSE 0 END
            ELSE 0
        END, 4
    ),
    
    -- Jockey/Trainer Interactions
    jockey_wr_x_trainer_wr = ROUND(
        COALESCE(jockey_win_rate, 0) * COALESCE(trainer_win_rate, 0), 4
    ),
    jockey_place_x_trainer_place = ROUND(
        COALESCE(jockey_place_rate, 0) * COALESCE(trainer_place_rate, 0), 4
    ),
    
    -- Form Interactions
    form_x_freshness = ROUND(
        COALESCE(form_momentum, 0) * COALESCE(form_recency_score, 0.5), 4
    ),
    track_wr_x_distance_wr = ROUND(
        COALESCE(track_win_rate, 0) * COALESCE(distance_win_rate, 0), 4
    ),
    
    -- Running Style x Barrier Position
    running_style_x_barrier = ROUND(
        CASE 
            WHEN LOWER(running_style) = 'leader' THEN
                CASE WHEN barrier_position = 'inner' THEN 1.0
                     WHEN barrier_position = 'middle' THEN 0.5 ELSE 0.2 END
            WHEN LOWER(running_style) IN ('on_pace', 'stalker') THEN
                CASE WHEN barrier_position = 'inner' THEN 0.8
                     WHEN barrier_position = 'middle' THEN 0.7 ELSE 0.4 END
            WHEN LOWER(running_style) = 'closer' THEN
                CASE WHEN barrier_position = 'outer' THEN 0.6
                     WHEN barrier_position = 'middle' THEN 0.5 ELSE 0.4 END
            ELSE 0.5
        END, 4
    );

DO $$ BEGIN RAISE NOTICE '[4.1] Simple interactions complete'; END $$;

-- ============================================================================
-- STEP 2: COMPETITIVE LEVEL SCORE
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.2] Calculating competitive level scores...'; END $$;

UPDATE race_training_dataset
SET competitive_level_score = ROUND(
    (
        COALESCE(elo_percentile_in_race, 0.5) * 0.3 +
        COALESCE(win_pct_vs_field_avg, 1) / 5 * 0.25 +
        COALESCE(jockey_win_rate, 0) * 3 * 0.2 +
        COALESCE(trainer_win_rate, 0) * 3 * 0.15 +
        COALESCE(form_momentum, 0) * 0.1
    ), 4
);

-- ============================================================================
-- STEP 3: BARRIER ANALYSIS (Track-specific advantages)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.3] Calculating barrier analysis features...'; END $$;

-- Calculate barrier group (inner/middle/outer) win rates per track
WITH barrier_track_stats AS (
    SELECT 
        track_name,
        barrier_position,
        COUNT(*) as total_starts,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) as wins,
        ROUND(SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) as win_rate
    FROM race_training_dataset
    WHERE barrier_position IS NOT NULL
      AND track_name IS NOT NULL
    GROUP BY track_name, barrier_position
),
track_avg AS (
    SELECT 
        track_name,
        AVG(win_rate) as avg_track_wr
    FROM barrier_track_stats
    GROUP BY track_name
)
UPDATE race_training_dataset rtd
SET 
    barrier_advantage_at_track = ROUND(
        COALESCE(bts.win_rate, 0) - COALESCE(ta.avg_track_wr, 0.1), 
        4
    ),
    barrier_effectiveness_score = ROUND(
        CASE 
            WHEN COALESCE(ta.avg_track_wr, 0) > 0 
            THEN COALESCE(bts.win_rate, 0) / ta.avg_track_wr
            ELSE 1 
        END, 
        4
    )
FROM barrier_track_stats bts
JOIN track_avg ta ON bts.track_name = ta.track_name
WHERE rtd.track_name = bts.track_name 
  AND rtd.barrier_position = bts.barrier_position;

-- Horse's historical win rate at their barrier group
WITH horse_barrier_history AS (
    SELECT 
        race_id,
        horse_location_slug,
        barrier_position,
        COUNT(*) OVER w_prior as starts_at_barrier_group,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as wins_at_barrier_group
    FROM race_training_dataset
    WHERE horse_location_slug IS NOT NULL
      AND barrier_position IS NOT NULL
    WINDOW w_prior AS (
        PARTITION BY horse_location_slug, barrier_position 
        ORDER BY race_date, race_id 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)
UPDATE race_training_dataset rtd
SET 
    horse_barrier_group_starts = COALESCE(hbh.starts_at_barrier_group, 0),
    horse_barrier_group_win_rate = CASE 
        WHEN COALESCE(hbh.starts_at_barrier_group, 0) > 0 
        THEN ROUND(hbh.wins_at_barrier_group::numeric / hbh.starts_at_barrier_group, 4)
        ELSE 0 
    END
FROM horse_barrier_history hbh
WHERE rtd.race_id = hbh.race_id 
  AND rtd.horse_location_slug = hbh.horse_location_slug;

DO $$ BEGIN RAISE NOTICE '[4.3] Barrier analysis complete'; END $$;

-- ============================================================================
-- STEP 4: ANTI-LEAKAGE FLAGS
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.4] Setting anti-leakage flags...'; END $$;

UPDATE race_training_dataset
SET 
    -- Extreme staleness: hasn't raced in 180+ days
    extreme_staleness_flag = (COALESCE(days_since_last_race, 0) > 180),
    
    -- Experience flags
    low_sample_flag = (COALESCE(total_races, 0) < 3),
    new_jockey_flag = (COALESCE(jockey_total_rides, 0) < 10),
    new_trainer_flag = (COALESCE(trainer_total_runners, 0) < 10);

-- ============================================================================
-- STEP 5: MODEL BIAS COLUMNS (Historical over/under performance vs predictions)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.5] Calculating model bias columns...'; END $$;

-- These help identify if a horse/jockey/trainer consistently beats or misses expectations
WITH historical_performance AS (
    SELECT 
        race_id,
        horse_location_slug,
        jockey_location_slug,
        trainer_location_slug,
        -- Horse: actual win rate vs ELO-implied rate
        AVG(
            CASE WHEN final_position = 1 THEN 1 ELSE 0 END - 
            (COALESCE(horse_elo, 1500) - 1400) / 600 * 0.1
        ) OVER w_horse as horse_win_bias,
        -- Horse: actual place rate vs expected
        AVG(
            CASE WHEN final_position <= 3 THEN 1 ELSE 0 END - 0.3
        ) OVER w_horse as horse_place_bias,
        -- Jockey: actual vs expected
        AVG(
            CASE WHEN final_position = 1 THEN 1 ELSE 0 END - 0.1
        ) OVER w_jockey as jockey_win_bias,
        AVG(
            CASE WHEN final_position <= 3 THEN 1 ELSE 0 END - 0.3
        ) OVER w_jockey as jockey_place_bias,
        -- Trainer: actual vs expected
        AVG(
            CASE WHEN final_position = 1 THEN 1 ELSE 0 END - 0.1
        ) OVER w_trainer as trainer_win_bias,
        AVG(
            CASE WHEN final_position <= 3 THEN 1 ELSE 0 END - 0.3
        ) OVER w_trainer as trainer_place_bias
    FROM race_training_dataset
    WHERE final_position IS NOT NULL
    WINDOW 
        w_horse AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w_jockey AS (PARTITION BY jockey_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w_trainer AS (PARTITION BY trainer_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
)
UPDATE race_training_dataset rtd
SET 
    horse_model_win_bias = ROUND(COALESCE(hp.horse_win_bias, 0)::numeric, 4),
    horse_model_top3_bias = ROUND(COALESCE(hp.horse_place_bias, 0)::numeric, 4),
    jockey_model_win_bias = ROUND(COALESCE(hp.jockey_win_bias, 0)::numeric, 4),
    jockey_model_top3_bias = ROUND(COALESCE(hp.jockey_place_bias, 0)::numeric, 4),
    trainer_model_win_bias = ROUND(COALESCE(hp.trainer_win_bias, 0)::numeric, 4),
    trainer_model_top3_bias = ROUND(COALESCE(hp.trainer_place_bias, 0)::numeric, 4)
FROM historical_performance hp
WHERE rtd.race_id = hp.race_id 
  AND rtd.horse_location_slug = hp.horse_location_slug;

DO $$ BEGIN RAISE NOTICE '[4.5] Model bias columns complete'; END $$;

-- ============================================================================
-- STEP 6: CLOSING POWER SCORE & EXTENDED BARRIER FEATURES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6] Calculating closing power score and extended barrier features...'; END $$;

UPDATE race_training_dataset
SET 
    closing_power_score = ROUND(
        (
            COALESCE(closing_ability_score, 0.5) * 0.5 +
            COALESCE(sustained_run_score, 0.5) * 0.3 +
            CASE WHEN running_style IN ('closer', 'off_pace') THEN 0.2 ELSE 0 END
        ), 4
    ),
    strong_closer_flag = (
        COALESCE(closing_ability_score, 0) > 0.7 
        AND running_style IN ('closer', 'off_pace')
    ),
    
    -- Barrier vs running style
    barrier_vs_run_style = ROUND(
        CASE 
            WHEN running_style = 'leader' AND barrier_position = 'inner' THEN 1.0
            WHEN running_style = 'leader' AND barrier_position = 'middle' THEN 0.6
            WHEN running_style = 'leader' AND barrier_position = 'outer' THEN 0.3
            WHEN running_style IN ('on_pace', 'stalker') AND barrier_position = 'inner' THEN 0.9
            WHEN running_style IN ('on_pace', 'stalker') AND barrier_position = 'middle' THEN 0.8
            WHEN running_style IN ('on_pace', 'stalker') AND barrier_position = 'outer' THEN 0.5
            WHEN running_style = 'midfield' THEN 0.6
            WHEN running_style IN ('closer', 'off_pace') AND barrier_position = 'outer' THEN 0.7
            WHEN running_style IN ('closer', 'off_pace') AND barrier_position = 'middle' THEN 0.6
            WHEN running_style IN ('closer', 'off_pace') AND barrier_position = 'inner' THEN 0.5
            ELSE 0.5
        END, 4
    ),
    
    -- Barrier x track condition advantage
    barrier_track_condition_advantage = ROUND(
        CASE 
            WHEN track_condition_numeric <= 2 AND barrier_position = 'inner' THEN 0.15
            WHEN track_condition_numeric <= 2 AND barrier_position = 'middle' THEN 0.10
            WHEN track_condition_numeric >= 4 AND barrier_position = 'outer' THEN 0.08
            ELSE 0
        END +
        CASE
            WHEN race_distance > 2000 AND barrier_position = 'inner' THEN 0.10
            WHEN race_distance > 2000 AND barrier_position = 'middle' THEN 0.05
            ELSE 0
        END, 4
    ),
    
    -- Weight relative x distance interaction
    weight_relative_x_distance = ROUND(
        COALESCE(weight_vs_avg, 0) * (COALESCE(race_distance, 1400) / 1400.0),
        4
    );

-- ============================================================================
-- STEP 6B: VALUE INDICATORS & LONGSHOT PROFILES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6b] Calculating value indicators...'; END $$;

UPDATE race_training_dataset
SET 
    -- Value indicator (model expects better than market odds imply)
    value_indicator = ROUND(
        CASE 
            WHEN odds_implied_probability > 0 AND horse_elo IS NOT NULL
            THEN (COALESCE(horse_elo, 1500) - 1400) / 600 * 0.25 - odds_implied_probability + 0.1
            ELSE 0
        END, 4
    ),
    
    -- Market undervalued flag
    market_undervalued = (
        COALESCE(elo_percentile_in_race, 0) > 0.6 
        AND COALESCE(odds_rank_in_race, 1) > 3
    ),
    
    -- Class drop longshot
    class_drop_longshot = (
        COALESCE(is_class_drop, FALSE) 
        AND COALESCE(win_odds, 0) > 10
    ),
    
    -- Connection longshot
    connection_longshot = (
        COALESCE(total_connection_score, 0) > 0.3 
        AND COALESCE(win_odds, 0) > 10
    ),
    
    -- Improving longshot (form trending up + good odds)
    improving_longshot = (
        COALESCE(form_momentum, 0) > 0.6 
        AND COALESCE(win_odds, 0) > 8
    ),
    
    -- Elo vs odds gap
    elo_vs_odds_gap = ROUND(
        COALESCE(elo_percentile_in_race, 0.5) - 
        (1.0 / NULLIF(odds_rank_in_race, 0)),
        4
    ),
    
    -- Form vs odds gap
    form_vs_odds_gap = ROUND(
        COALESCE(form_momentum, 0.5) - COALESCE(odds_implied_probability, 0.2),
        4
    );

-- ============================================================================
-- STEP 6C: CLASS AND STEPPING FEATURES
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

-- Stepping up/down (optimized lookup of prior class per horse)
CREATE INDEX IF NOT EXISTS idx_rtd_horse_date_id
ON race_training_dataset(horse_location_slug, race_date, race_id);

UPDATE race_training_dataset rtd
SET 
    is_stepping_up = CASE 
        WHEN prev.prev_class IS NULL THEN FALSE 
        ELSE rtd.class_level_numeric < prev.prev_class 
    END,
    is_stepping_down = CASE 
        WHEN prev.prev_class IS NULL THEN FALSE 
        ELSE rtd.class_level_numeric > prev.prev_class 
    END
FROM LATERAL (
    SELECT rtd2.class_level_numeric AS prev_class
    FROM race_training_dataset rtd2
    WHERE rtd2.horse_location_slug = rtd.horse_location_slug
      AND (
        rtd2.race_date < rtd.race_date
        OR (rtd2.race_date = rtd.race_date AND rtd2.race_id < rtd.race_id)
      )
    ORDER BY rtd2.race_date DESC, rtd2.race_id DESC
    LIMIT 1
) prev
WHERE rtd.horse_location_slug IS NOT NULL;

-- ============================================================================
-- STEP 6D: TRACK DISTANCE FEATURES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6d] Calculating track-distance features...'; END $$;

-- Track + distance combo experience
WITH track_dist_exp AS (
    SELECT 
        race_id,
        horse_location_slug,
        track_name,
        distance_range,
        COUNT(*) OVER w_prior as exp,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as wins
    FROM race_training_dataset
    WHERE horse_location_slug IS NOT NULL AND track_name IS NOT NULL AND distance_range IS NOT NULL
    WINDOW w_prior AS (
        PARTITION BY horse_location_slug, track_name, distance_range 
        ORDER BY race_date, race_id 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)
UPDATE race_training_dataset rtd
SET 
    track_distance_experience = COALESCE(tde.exp, 0),
    track_distance_win_rate_weighted = CASE 
        WHEN COALESCE(tde.exp, 0) > 0 
        THEN ROUND(tde.wins::numeric / tde.exp * (1 + LN(tde.exp + 1) / 5), 4)
        ELSE 0 
    END
FROM track_dist_exp tde
WHERE rtd.race_id = tde.race_id AND rtd.horse_location_slug = tde.horse_location_slug;

-- ============================================================================
-- STEP 6E: DISTANCE VARIANCE & OPTIMAL DISTANCE
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6e] Calculating distance variance and optimal distance...'; END $$;

-- Distance variance (how much the horse's raced distances vary)
WITH dist_stats AS (
    SELECT 
        race_id,
        horse_location_slug,
        STDDEV(race_distance) OVER w_prior as dist_var,
        -- Use CASE instead of FILTER for window function compatibility
        AVG(CASE WHEN final_position = 1 THEN race_distance ELSE NULL END) OVER w_prior as avg_winning_dist
    FROM race_training_dataset
    WHERE horse_location_slug IS NOT NULL
    WINDOW w_prior AS (
        PARTITION BY horse_location_slug 
        ORDER BY race_date, race_id 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)
UPDATE race_training_dataset rtd
SET 
    distance_variance = ROUND(COALESCE(ds.dist_var, 0)::numeric, 2)
FROM dist_stats ds
WHERE rtd.race_id = ds.race_id AND rtd.horse_location_slug = ds.horse_location_slug;

-- Optimal distance range
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

-- ============================================================================
-- STEP 6F: MODEL PREDICTION HISTORY
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6f] Calculating model prediction history...'; END $$;

-- Horse prediction history count and average position error
WITH pred_history AS (
    SELECT 
        horse_location_slug,
        COUNT(*) as prediction_count,
        AVG(ABS(COALESCE(final_position, 5) - 3)) as avg_pos_error  -- Simplified error measure
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

-- ============================================================================
-- STEP 6G: SPECIALTY & DEBUTANT FEATURES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6g] Calculating specialty and debutant features...'; END $$;

UPDATE race_training_dataset
SET 
    -- Specialization score (how focused on specific conditions)
    specialization_score = ROUND(
        (
            CASE WHEN races_at_track >= 5 THEN 0.3 ELSE 0 END +
            CASE WHEN races_at_distance >= 5 THEN 0.3 ELSE 0 END +
            CASE WHEN track_condition_wins >= 2 THEN 0.2 ELSE 0 END +
            CASE WHEN COALESCE(distance_range_experience, 0) >= 5 THEN 0.2 ELSE 0 END
        ), 4
    ),
    
    -- Debutant predicted ability (for first starters)
    debutant_predicted_ability = CASE
        WHEN total_races = 0 OR total_races IS NULL THEN
            ROUND(
                0.5 + 
                COALESCE(trainer_win_rate, 0) * 0.3 +
                COALESCE(jockey_win_rate, 0) * 0.2,
                4
            )
        ELSE NULL
    END;

-- ============================================================================
-- STEP 6H: PRIZE MONEY, SECTIONALS, RESTRICTIONS
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.6h] Setting misc columns...'; END $$;

-- These may be NULL if source data doesn't have them
UPDATE race_training_dataset rtd
SET 
    prize_money = COALESCE(prize_money, 0),
    sectional_800m = COALESCE(sectional_800m, 0),
    sectional_400m = COALESCE(sectional_400m, 0),
    age_restriction = COALESCE(age_restriction, 'open'),
    sex_restriction = COALESCE(sex_restriction, 'open')
WHERE prize_money IS NULL OR sectional_800m IS NULL OR sectional_400m IS NULL;

-- ============================================================================
-- STEP 7: RECREATE INDEXES
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.7] Recreating indexes...'; END $$;

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

DO $$ BEGIN RAISE NOTICE '[4.7] Indexes created'; END $$;

-- ============================================================================
-- STEP 8: ENHANCED SECTIONAL FEATURES (from V1 script 15)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.8] Adding enhanced sectional features...'; END $$;

-- Add columns
ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS avg_pos_200m NUMERIC(5,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS early_speed_pct NUMERIC(5,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS strong_finish_pct NUMERIC(5,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS pos_volatility_800 NUMERIC(5,3) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS running_style_distance_fit NUMERIC(5,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS leader_advantage_score NUMERIC(5,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS finishing_kick_consistency NUMERIC(5,3) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS avg_sectional_400m NUMERIC(8,3) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS avg_sectional_800m NUMERIC(8,3) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS avg_cumulative_800m NUMERIC(8,3) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS sectional_time_consistency NUMERIC(5,3) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS early_vs_mid_pace_change NUMERIC(6,3) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS historical_speed_vs_field NUMERIC(6,4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS position_at_400 INTEGER DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS position_at_800 INTEGER DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS position_at_end INTEGER DEFAULT NULL;

-- Calculate enhanced sectional features (anti-leakage)
WITH horse_history AS (
    SELECT 
        rr.horse_slug,
        s.race_id,
        s.finish_position,
        s.position_200m,
        s.position_400m,
        s.position_800m,
        EXTRACT(EPOCH FROM s.sectional_400m) as sec_400m_time,
        EXTRACT(EPOCH FROM s.sectional_800m) as sec_800m_time,
        EXTRACT(EPOCH FROM s.cumulative_800m) as cum_800m_time,
        s.race_distance,
        s.scraped_at,
        ROW_NUMBER() OVER (PARTITION BY rr.horse_slug ORDER BY s.scraped_at, s.race_id) as race_seq
    FROM race_results_sectional_times s
    JOIN race_results rr ON s.race_id = rr.race_id AND s.horse_number::text = rr.horse_number
    WHERE s.finish_position IS NOT NULL AND s.finish_position <= 20 AND rr.horse_slug IS NOT NULL
),
race_averages AS (
    SELECT race_id, AVG(sec_400m_time) as race_avg_sec_400m
    FROM horse_history WHERE sec_400m_time IS NOT NULL GROUP BY race_id
),
rolling_stats AS (
    SELECT h.horse_slug, h.race_id,
        AVG(h.position_200m) OVER w AS hist_avg_pos_200m,
        AVG(CASE WHEN h.position_400m <= 3 THEN 1.0 ELSE 0.0 END) OVER w AS hist_early_speed_pct,
        AVG(CASE WHEN h.position_800m - h.finish_position >= 3 THEN 1.0 ELSE 0.0 END) OVER w AS hist_strong_finish_pct,
        STDDEV(h.position_800m) OVER w AS hist_pos_volatility_800,
        STDDEV(h.position_800m - h.finish_position) OVER w AS hist_finishing_kick_consistency,
        AVG(h.sec_400m_time) OVER w AS hist_avg_sec_400m,
        AVG(h.sec_800m_time) OVER w AS hist_avg_sec_800m,
        AVG(h.cum_800m_time) OVER w AS hist_avg_cum_800m,
        STDDEV(h.sec_400m_time) OVER w AS hist_sec_time_consistency,
        AVG(h.sec_800m_time - h.sec_400m_time) OVER w AS hist_pace_change,
        AVG(ra.race_avg_sec_400m - h.sec_400m_time) OVER w AS hist_speed_vs_field
    FROM horse_history h
    LEFT JOIN race_averages ra ON h.race_id = ra.race_id
    WHERE h.position_800m IS NOT NULL
    WINDOW w AS (PARTITION BY h.horse_slug ORDER BY h.race_seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
)
UPDATE race_training_dataset rtd SET
    avg_pos_200m = ROUND(rs.hist_avg_pos_200m::numeric, 2),
    early_speed_pct = ROUND(rs.hist_early_speed_pct::numeric, 4),
    strong_finish_pct = ROUND(rs.hist_strong_finish_pct::numeric, 4),
    pos_volatility_800 = ROUND(rs.hist_pos_volatility_800::numeric, 3),
    finishing_kick_consistency = ROUND(rs.hist_finishing_kick_consistency::numeric, 3),
    avg_sectional_400m = ROUND(rs.hist_avg_sec_400m::numeric, 3),
    avg_sectional_800m = ROUND(rs.hist_avg_sec_800m::numeric, 3),
    avg_cumulative_800m = ROUND(rs.hist_avg_cum_800m::numeric, 3),
    sectional_time_consistency = ROUND(rs.hist_sec_time_consistency::numeric, 3),
    early_vs_mid_pace_change = ROUND(rs.hist_pace_change::numeric, 3),
    historical_speed_vs_field = ROUND(rs.hist_speed_vs_field::numeric, 4)
FROM rolling_stats rs WHERE rtd.race_id = rs.race_id AND rtd.horse_slug = rs.horse_slug;

-- Position columns from sectional times
UPDATE race_training_dataset rtd SET
    position_at_400 = s.position_400m,
    position_at_800 = s.position_800m,
    position_at_end = s.finish_position
FROM race_results_sectional_times s
JOIN race_results rr ON s.race_id = rr.race_id AND s.horse_number::text = rr.horse_number
WHERE rtd.race_id = s.race_id AND rtd.horse_slug = rr.horse_slug;

-- Running style distance fit
UPDATE race_training_dataset SET
    running_style_distance_fit = CASE
        WHEN LOWER(running_style) = 'leader' AND race_distance < 1200 THEN 0.9
        WHEN LOWER(running_style) = 'leader' AND race_distance < 1600 THEN 0.7
        WHEN LOWER(running_style) = 'leader' THEN 0.5
        WHEN LOWER(running_style) IN ('on_pace', 'stalker') AND race_distance BETWEEN 1200 AND 1800 THEN 0.85
        WHEN LOWER(running_style) = 'midfield' THEN 0.7
        WHEN LOWER(running_style) = 'closer' AND race_distance >= 1600 THEN 0.85
        WHEN LOWER(running_style) = 'closer' AND race_distance >= 2000 THEN 0.95
        ELSE 0.5
    END,
    leader_advantage_score = CASE
        WHEN LOWER(running_style) = 'leader' AND race_distance < 1200 THEN 0.8
        WHEN LOWER(running_style) = 'leader' AND race_distance < 1600 THEN 0.5
        WHEN LOWER(running_style) = 'leader' THEN 0.2
        WHEN LOWER(running_style) = 'closer' THEN -0.3
        ELSE 0.0
    END;

DO $$ BEGIN RAISE NOTICE '[4.8] ✓ Enhanced sectional features added'; END $$;

-- ============================================================================
-- STEP 9: METRO WIN FEATURES (from V1 script 16)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.9] Adding metro win features...'; END $$;

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
horse_races AS (
    SELECT rr.horse_slug, rr.race_id, r.race_date, r.track_name,
        CASE WHEN mt.track_name IS NOT NULL THEN 1 ELSE 0 END as is_metro,
        CASE WHEN rr.position = 1 AND mt.track_name IS NOT NULL THEN 1 ELSE 0 END as is_metro_win,
        ROW_NUMBER() OVER (PARTITION BY rr.horse_slug ORDER BY r.race_date, rr.race_id) as race_seq
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    LEFT JOIN metro_tracks mt ON r.track_name = mt.track_name
    WHERE rr.horse_slug IS NOT NULL AND rr.position IS NOT NULL
),
rolling_metro AS (
    SELECT horse_slug, race_id, race_date,
        SUM(is_metro_win) OVER w as prior_metro_wins,
        SUM(is_metro) OVER w as prior_metro_starts,
        MAX(CASE WHEN is_metro_win = 1 THEN race_date END) OVER w as last_metro_win_date
    FROM horse_races
    WINDOW w AS (PARTITION BY horse_slug ORDER BY race_seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
)
UPDATE race_training_dataset rtd SET
    has_won_at_metro = CASE WHEN COALESCE(rm.prior_metro_wins, 0) > 0 THEN 1 ELSE 0 END,
    days_since_metro_win = CASE WHEN rm.last_metro_win_date IS NOT NULL THEN (rm.race_date - rm.last_metro_win_date)::INTEGER ELSE NULL END,
    metro_wins_count = COALESCE(rm.prior_metro_wins, 0)::INTEGER,
    metro_win_rate = CASE WHEN COALESCE(rm.prior_metro_starts, 0) > 0 THEN ROUND(rm.prior_metro_wins::numeric / rm.prior_metro_starts, 4) ELSE NULL END
FROM rolling_metro rm WHERE rtd.race_id = rm.race_id AND rtd.horse_slug = rm.horse_slug;

DO $$ BEGIN RAISE NOTICE '[4.9] ✓ Metro win features added'; END $$;

-- ============================================================================
-- STEP 10: WEATHER FEATURES (from V1 script 17)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.10] Adding weather features...'; END $$;

ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS race_weather TEXT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS is_wet_weather INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS estimated_temperature INTEGER DEFAULT NULL;

-- Pull weather from races table
UPDATE race_training_dataset rtd SET
    race_weather = r.race_weather,
    is_wet_weather = CASE WHEN LOWER(r.race_weather) LIKE '%rain%' THEN 1 ELSE 0 END
FROM races r WHERE rtd.race_id = r.race_id AND r.race_weather IS NOT NULL;

-- Estimate temperature based on location and month
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

DO $$ BEGIN RAISE NOTICE '[4.10] ✓ Weather features added'; END $$;

-- ============================================================================
-- STEP 11: TRACK SPECIALIST FEATURES (from V1 script 10)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.11] Adding track specialist features...'; END $$;

ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS has_won_at_track BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_won_at_distance BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_won_at_track_distance BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_track_specialist BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS track_distance_preference NUMERIC(5,4) DEFAULT NULL;

-- Has won at track (anti-leakage window)
WITH track_win_history AS (
    SELECT rr.race_id, rr.horse_slug,
        SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY rr.horse_slug, r.track_name
            ORDER BY r.race_date, rr.race_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) as prior_track_wins
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id WHERE rr.horse_slug IS NOT NULL
)
UPDATE race_training_dataset rtd SET has_won_at_track = COALESCE(twh.prior_track_wins, 0) > 0
FROM track_win_history twh WHERE rtd.race_id = twh.race_id AND rtd.horse_slug = twh.horse_slug;

-- Has won at distance (bucket to nearest 200m)
WITH distance_win_history AS (
    SELECT rr.race_id, rr.horse_slug,
        SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY rr.horse_slug, FLOOR(r.race_distance / 200)
            ORDER BY r.race_date, rr.race_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) as prior_dist_wins
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id WHERE rr.horse_slug IS NOT NULL
)
UPDATE race_training_dataset rtd SET has_won_at_distance = COALESCE(dwh.prior_dist_wins, 0) > 0
FROM distance_win_history dwh WHERE rtd.race_id = dwh.race_id AND rtd.horse_slug = dwh.horse_slug;

-- Has won at track+distance combo
WITH td_win_history AS (
    SELECT rr.race_id, rr.horse_slug,
        SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY rr.horse_slug, r.track_name, FLOOR(r.race_distance / 200)
            ORDER BY r.race_date, rr.race_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) as prior_td_wins
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id WHERE rr.horse_slug IS NOT NULL
)
UPDATE race_training_dataset rtd SET has_won_at_track_distance = COALESCE(tdh.prior_td_wins, 0) > 0
FROM td_win_history tdh WHERE rtd.race_id = tdh.race_id AND rtd.horse_slug = tdh.horse_slug;

-- Track specialist flag
UPDATE race_training_dataset SET
    is_track_specialist = (
        COALESCE(track_win_rate, 0) > COALESCE(win_percentage, 0) * 1.5
        AND COALESCE(track_win_rate, 0) > 0
        AND total_races >= 5
    ),
    track_distance_preference = CASE
        WHEN has_won_at_track_distance THEN 0.9
        WHEN has_won_at_track OR has_won_at_distance THEN 0.6
        ELSE 0.3
    END;

DO $$ BEGIN RAISE NOTICE '[4.11] ✓ Track specialist features added'; END $$;

-- ============================================================================
-- STEP 12: DEBUTANT FEATURES (from V1 script 11)
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

DO $$ BEGIN RAISE NOTICE '[4.12] ✓ Debutant features added'; END $$;

-- ============================================================================
-- STEP 13: JOCKEY/TRAINER AVG POSITION (from V1 script 08a/08b)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.13] Adding jockey/trainer avg position features...'; END $$;

ALTER TABLE race_training_dataset
    ADD COLUMN IF NOT EXISTS jockey_avg_position_20 NUMERIC(5,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS jockey_avg_position_50 NUMERIC(5,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS trainer_avg_position_50 NUMERIC(5,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS trainer_avg_position_100 NUMERIC(5,2) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS track_condition_advantage NUMERIC(5,4) DEFAULT NULL;

-- Jockey avg position (last 20 and 50 rides)
WITH jockey_history AS (
    SELECT rr.jockey_slug, rr.race_id, rr.position,
        ROW_NUMBER() OVER (PARTITION BY rr.jockey_slug ORDER BY r.race_date DESC, rr.race_id DESC) as ride_seq
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id
    WHERE rr.jockey_slug IS NOT NULL AND rr.position IS NOT NULL AND rr.position < 50
),
jockey_stats AS (
    SELECT jockey_slug,
        AVG(position) FILTER (WHERE ride_seq <= 20) as avg_pos_20,
        AVG(position) FILTER (WHERE ride_seq <= 50) as avg_pos_50
    FROM jockey_history GROUP BY jockey_slug
)
UPDATE race_training_dataset rtd SET
    jockey_avg_position_20 = ROUND(js.avg_pos_20::numeric, 2),
    jockey_avg_position_50 = ROUND(js.avg_pos_50::numeric, 2)
FROM jockey_stats js WHERE rtd.jockey_slug = js.jockey_slug;

-- Trainer avg position (last 50 and 100 runners)
WITH trainer_history AS (
    SELECT rr.trainer_slug, rr.race_id, rr.position,
        ROW_NUMBER() OVER (PARTITION BY rr.trainer_slug ORDER BY r.race_date DESC, rr.race_id DESC) as run_seq
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id
    WHERE rr.trainer_slug IS NOT NULL AND rr.position IS NOT NULL AND rr.position < 50
),
trainer_stats AS (
    SELECT trainer_slug,
        AVG(position) FILTER (WHERE run_seq <= 50) as avg_pos_50,
        AVG(position) FILTER (WHERE run_seq <= 100) as avg_pos_100
    FROM trainer_history GROUP BY trainer_slug
)
UPDATE race_training_dataset rtd SET
    trainer_avg_position_50 = ROUND(ts.avg_pos_50::numeric, 2),
    trainer_avg_position_100 = ROUND(ts.avg_pos_100::numeric, 2)
FROM trainer_stats ts WHERE rtd.trainer_slug = ts.trainer_slug;

-- Track condition advantage (horse's win rate on this condition vs overall)
WITH horse_condition_stats AS (
    SELECT rr.horse_slug, r.track_condition,
        SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) as condition_win_rate
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id
    WHERE rr.horse_slug IS NOT NULL AND rr.position IS NOT NULL AND r.track_condition IS NOT NULL
    GROUP BY rr.horse_slug, r.track_condition HAVING COUNT(*) >= 3
)
UPDATE race_training_dataset rtd SET
    track_condition_advantage = ROUND(hcs.condition_win_rate - COALESCE(rtd.win_percentage, 0) / 100, 4)
FROM horse_condition_stats hcs
WHERE rtd.horse_slug = hcs.horse_slug AND rtd.track_condition = hcs.track_condition;

DO $$ BEGIN RAISE NOTICE '[4.13] ✓ Jockey/trainer avg position features added'; END $$;

-- ============================================================================
-- STEP 14: FINAL VALIDATION
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[4.14] Running final validation...'; END $$;

-- Summary statistics
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT race_id) as unique_races,
    COUNT(DISTINCT horse_slug) as unique_horses,
    COUNT(DISTINCT jockey_slug) as unique_jockeys,
    COUNT(DISTINCT trainer_slug) as unique_trainers,
    COUNT(DISTINCT track_name) as unique_tracks,
    MIN(race_date) as earliest_race,
    MAX(race_date) as latest_race
FROM race_training_dataset;

-- Location distribution
SELECT 
    location,
    COUNT(*) as records,
    COUNT(DISTINCT horse_slug) as horses
FROM race_training_dataset
GROUP BY location
ORDER BY records DESC;

-- Track category distribution
SELECT 
    track_category,
    COUNT(*) as records,
    ROUND(COUNT(*)::numeric / (SELECT COUNT(*) FROM race_training_dataset) * 100, 2) as pct
FROM race_training_dataset
GROUP BY track_category
ORDER BY records DESC;

-- Running style distribution
SELECT 
    running_style,
    COUNT(*) as records
FROM race_training_dataset
GROUP BY running_style
ORDER BY records DESC;

-- Key columns completeness
SELECT 
    'last_5_win_rate' as column_name, COUNT(*) as populated 
    FROM race_training_dataset WHERE last_5_win_rate IS NOT NULL
UNION ALL
SELECT 'horse_elo', COUNT(*) FROM race_training_dataset WHERE horse_elo IS NOT NULL
UNION ALL
SELECT 'running_style (not unknown)', COUNT(*) FROM race_training_dataset WHERE running_style != 'unknown'
UNION ALL
SELECT 'speed_figure', COUNT(*) FROM race_training_dataset WHERE speed_figure IS NOT NULL
UNION ALL
SELECT 'class_rating', COUNT(*) FROM race_training_dataset WHERE class_rating IS NOT NULL
UNION ALL
SELECT 'barrier_advantage_at_track', COUNT(*) FROM race_training_dataset WHERE barrier_advantage_at_track IS NOT NULL;

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
    RAISE NOTICE 'PHASE 4 COMPLETE - REBUILD FINISHED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Total records: %', final_count;
    RAISE NOTICE 'Total columns: %', col_count;
    RAISE NOTICE '';
    RAISE NOTICE 'Optional: Run 05_current_form_views.sql for prediction views';
    RAISE NOTICE '============================================================';
END $$;
