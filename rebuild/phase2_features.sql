-- ============================================================================
-- PHASE 2: ADVANCED FEATURES, INTERACTIONS & SPECIALIST FEATURES (DuckDB)
-- ============================================================================
-- Covers: Old 03_advanced_features + 04_interactions + 05-07 sectional/elo
-- Est. Time: 3-8 minutes in DuckDB
--
-- WHAT THIS DOES:
--   1. Sectional positions + historical sectional averages
--   2. Running style classification
--   3. Speed figures (from race_results_sectional_times)
--   4. Class rating + class change/drop/rise
--   5. Field percentiles (ELO, odds, win%, experience)
--   6. Odds features (favorite, implied prob, value)
--   7. Value indicators (longshot, market gap, etc.)
--   8. Distance features (suited score, experience levels)
--   9. Form flags, run counts, weight features
--  10. Connection features (jockey-horse, jockey-trainer)
--  11. Track-specific win rates (jockey/trainer at track)
--  12. Feature interactions (ELO x jockey, barrier x distance, etc.)
--  13. Barrier analysis (track-specific advantages)
--  14. Anti-leakage flags + model bias columns
--  15. Closing power + extended barrier features
--  16. Enhanced sectional features
--  17. Metro win features, weather, track specialist, debutant
--  18. Jockey/trainer avg position features
-- ============================================================================

-- ============================================================================
-- STEP 1: SECTIONAL POSITIONS FROM race_results
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    position_800m = rr.position_800m,
    position_400m = rr.position_400m
FROM race_results rr
WHERE rtd.race_id = rr.race_id
  AND rtd.horse_slug = rr.horse_slug
  AND (rr.position_800m IS NOT NULL OR rr.position_400m IS NOT NULL);

-- Position improvements (from current race — will be removed in Phase 3)
UPDATE race_training_dataset_new SET
    pos_improvement_800_400 = CASE
        WHEN position_800m IS NOT NULL AND position_400m IS NOT NULL
        THEN position_800m - position_400m ELSE NULL END,
    pos_improvement_400_finish = CASE
        WHEN position_400m IS NOT NULL AND final_position IS NOT NULL
        THEN position_400m - final_position ELSE NULL END,
    pos_improvement_800_finish = CASE
        WHEN position_800m IS NOT NULL AND final_position IS NOT NULL
        THEN position_800m - final_position ELSE NULL END;

-- ============================================================================
-- STEP 2: HISTORICAL SECTIONAL AVERAGES (Anti-leakage)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    avg_early_position_800m = ROUND(sh.avg_800m, 2),
    avg_mid_position_400m   = ROUND(sh.avg_400m, 2),
    historical_avg_improvement = ROUND(sh.avg_improvement, 2)
FROM (
    SELECT race_id, horse_location_slug,
        AVG(position_800m) OVER w_prior AS avg_800m,
        AVG(position_400m) OVER w_prior AS avg_400m,
        AVG(pos_improvement_800_finish) OVER w_prior AS avg_improvement
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND position_800m IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) sh
WHERE rtd.race_id = sh.race_id AND rtd.horse_location_slug = sh.horse_location_slug;

-- ============================================================================
-- STEP 3: RUNNING STYLE CLASSIFICATION
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    running_style = CASE
        WHEN sd.early_pct IS NULL THEN 'unknown'
        WHEN sd.early_pct <= 0.20 THEN 'leader'
        WHEN sd.early_pct <= 0.35 THEN 'on_pace'
        WHEN sd.early_pct <= 0.55 THEN 'midfield'
        WHEN sd.early_pct <= 0.75 THEN 'off_pace'
        ELSE 'closer'
    END
FROM (
    SELECT race_id, horse_location_slug,
        CASE WHEN avg_early_position_800m IS NOT NULL AND total_runners > 1
            THEN avg_early_position_800m / total_runners ELSE NULL END AS early_pct
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
) sd
WHERE rtd.race_id = sd.race_id AND rtd.horse_location_slug = sd.horse_location_slug;

UPDATE race_training_dataset_new SET running_style = 'unknown' WHERE running_style IS NULL;

-- ============================================================================
-- STEP 4: CLOSING/EARLY/SUSTAINED SCORES (will be in leakage set for removal)
-- ============================================================================
UPDATE race_training_dataset_new SET
    closing_ability_score = CASE
        WHEN historical_avg_improvement IS NOT NULL
        THEN GREATEST(0, LEAST(1, 0.5 + historical_avg_improvement / 10.0))
        ELSE 0.5 END,
    early_speed_score = CASE
        WHEN avg_early_position_800m IS NOT NULL AND total_runners > 1
        THEN GREATEST(0, LEAST(1, 1 - (avg_early_position_800m / total_runners)))
        ELSE 0.5 END,
    sustained_run_score = CASE
        WHEN avg_early_position_800m IS NOT NULL AND avg_mid_position_400m IS NOT NULL
        THEN GREATEST(0, LEAST(1, 0.5 + (avg_early_position_800m - avg_mid_position_400m) / 10.0))
        ELSE 0.5 END;

-- ============================================================================
-- STEP 5: SPEED FIGURES (from race_results_sectional_times)
-- ============================================================================
-- Pull raw_time_seconds
UPDATE race_training_dataset_new AS rtd SET
    raw_time_seconds = EXTRACT(EPOCH FROM rst.finish_time)
FROM race_results_sectional_times rst
WHERE rtd.race_id = rst.race_id
  AND rtd.horse_name = rst.horse_name
  AND rst.finish_time IS NOT NULL;

-- Calculate speed figure
UPDATE race_training_dataset_new AS rtd SET
    speed_figure = CASE
        WHEN rtd.raw_time_seconds IS NOT NULL AND rtd.raw_time_seconds > 0 AND tb.std_time > 0
        THEN ROUND(100 - ((rtd.raw_time_seconds - tb.avg_time) / tb.std_time * 10), 2)
        ELSE NULL END
FROM (
    SELECT race_distance, AVG(raw_time_seconds) AS avg_time, STDDEV(raw_time_seconds) AS std_time
    FROM race_training_dataset_new
    WHERE raw_time_seconds IS NOT NULL AND raw_time_seconds > 0 AND final_position <= 3
    GROUP BY race_distance HAVING COUNT(*) > 10
) tb
WHERE rtd.race_distance = tb.race_distance;

-- Historical speed figure averages
UPDATE race_training_dataset_new AS rtd SET
    avg_speed_figure  = ROUND(sh.avg_speed, 2),
    best_speed_figure = ROUND(sh.best_speed, 2)
FROM (
    SELECT race_id, horse_location_slug,
        AVG(speed_figure) OVER w_prior AS avg_speed,
        MAX(speed_figure) OVER w_prior AS best_speed
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND speed_figure IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) sh
WHERE rtd.race_id = sh.race_id AND rtd.horse_location_slug = sh.horse_location_slug;

-- ============================================================================
-- STEP 6: CLASS RATING + CLASS CHANGE
-- ============================================================================
UPDATE race_training_dataset_new SET
    class_rating = CASE
        WHEN race_class ILIKE '%group 1%' OR race_class ILIKE '%g1%' THEN 100
        WHEN race_class ILIKE '%group 2%' OR race_class ILIKE '%g2%' THEN 90
        WHEN race_class ILIKE '%group 3%' OR race_class ILIKE '%g3%' THEN 80
        WHEN race_class ILIKE '%listed%' THEN 75
        WHEN race_class ILIKE '%benchmark%' OR race_class ILIKE '%bm%' THEN
            CASE
                WHEN regexp_matches(race_class, 'bm\s*9[0-9]|benchmark\s*9[0-9]', 'i') THEN 70
                WHEN regexp_matches(race_class, 'bm\s*8[0-9]|benchmark\s*8[0-9]', 'i') THEN 60
                WHEN regexp_matches(race_class, 'bm\s*7[0-9]|benchmark\s*7[0-9]', 'i') THEN 50
                WHEN regexp_matches(race_class, 'bm\s*6[0-9]|benchmark\s*6[0-9]', 'i') THEN 40
                WHEN regexp_matches(race_class, 'bm\s*5[0-9]|benchmark\s*5[0-9]', 'i') THEN 35
                ELSE 30
            END
        WHEN regexp_matches(race_class, 'class\s*1|c1', 'i') THEN 50
        WHEN regexp_matches(race_class, 'class\s*2|c2', 'i') THEN 45
        WHEN regexp_matches(race_class, 'class\s*3|c3', 'i') THEN 40
        WHEN regexp_matches(race_class, 'class\s*4|c4', 'i') THEN 35
        WHEN regexp_matches(race_class, 'class\s*5|c5', 'i') THEN 30
        WHEN regexp_matches(race_class, 'class\s*6|c6', 'i') THEN 25
        WHEN race_class ILIKE '%maiden%' OR race_class ILIKE '%mdn%' THEN 20
        WHEN race_class ILIKE '%2yo%' OR race_class ILIKE '%2-y-o%' THEN 35
        WHEN race_class ILIKE '%3yo%' OR race_class ILIKE '%3-y-o%' THEN 40
        WHEN track_category = 'Metro' THEN 45
        WHEN track_category = 'Provincial' THEN 35
        ELSE 25
    END;

-- Class change from previous race + store prev_class_rating
UPDATE race_training_dataset_new AS rtd SET
    prev_class_rating = pc.prev_class_rating,
    class_change = rtd.class_rating - COALESCE(pc.prev_class_rating, rtd.class_rating),
    class_drop = (rtd.class_rating < COALESCE(pc.prev_class_rating, rtd.class_rating)),
    class_rise = (rtd.class_rating > COALESCE(pc.prev_class_rating, rtd.class_rating))
FROM (
    SELECT race_id, horse_location_slug,
        LAG(class_rating) OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id) AS prev_class_rating
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
) pc
WHERE rtd.race_id = pc.race_id AND rtd.horse_location_slug = pc.horse_location_slug;

-- Class level numeric
UPDATE race_training_dataset_new SET
    class_level_numeric = CASE
        WHEN race_class ILIKE '%group 1%' OR race_class ILIKE '%g1%' THEN 1
        WHEN race_class ILIKE '%group 2%' OR race_class ILIKE '%g2%' THEN 2
        WHEN race_class ILIKE '%group 3%' OR race_class ILIKE '%g3%' THEN 3
        WHEN race_class ILIKE '%listed%' THEN 4
        WHEN race_class ILIKE '%open%' THEN 5
        WHEN regexp_matches(race_class, 'bm\s*9[0-9]|benchmark\s*9[0-9]', 'i') THEN 6
        WHEN regexp_matches(race_class, 'bm\s*8[0-9]|benchmark\s*8[0-9]', 'i') THEN 8
        WHEN regexp_matches(race_class, 'bm\s*7[0-9]|benchmark\s*7[0-9]', 'i') THEN 10
        WHEN regexp_matches(race_class, 'bm\s*6[0-9]|benchmark\s*6[0-9]', 'i') THEN 14
        WHEN regexp_matches(race_class, 'bm\s*5[0-9]|benchmark\s*5[0-9]', 'i') THEN 17
        WHEN race_class ILIKE '%maiden%' THEN 20
        ELSE 15
    END;

-- Stepping up/down
UPDATE race_training_dataset_new AS rtd SET
    is_stepping_up = CASE WHEN pc.prev_class IS NULL THEN FALSE ELSE rtd.class_level_numeric < pc.prev_class END,
    is_stepping_down = CASE WHEN pc.prev_class IS NULL THEN FALSE ELSE rtd.class_level_numeric > pc.prev_class END
FROM (
    SELECT race_id, horse_location_slug,
        LAG(class_level_numeric) OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id) AS prev_class
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
) pc
WHERE rtd.race_id = pc.race_id AND rtd.horse_location_slug = pc.horse_location_slug;

-- ============================================================================
-- STEP 7: FIELD COMPARISON FEATURES (Percentiles within race)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    elo_percentile_in_race   = ROUND(COALESCE(rp.elo_pct, 0.5), 4),
    odds_percentile_in_race  = ROUND(COALESCE(rp.odds_pct, 0.5), 4),
    win_pct_vs_field_avg     = ROUND(LEAST(COALESCE(rp.win_pct_ratio, 1), 5), 4),
    experience_vs_field_avg  = ROUND(LEAST(COALESCE(rp.exp_ratio, 1), 5), 4),
    odds_rank_in_race        = COALESCE(rp.odds_rank, 1)
FROM (
    SELECT race_id, horse_slug,
        PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY COALESCE(horse_elo, 1200)) AS elo_pct,
        PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY COALESCE(win_odds, 100) DESC) AS odds_pct,
        CASE WHEN AVG(COALESCE(win_percentage, 0)) OVER (PARTITION BY race_id) > 0
            THEN COALESCE(win_percentage, 0) / NULLIF(AVG(COALESCE(win_percentage, 0)) OVER (PARTITION BY race_id), 0)
            ELSE 1.0 END AS win_pct_ratio,
        CASE WHEN AVG(COALESCE(total_races, 0)) OVER (PARTITION BY race_id) > 0
            THEN COALESCE(total_races, 0)::NUMERIC / NULLIF(AVG(COALESCE(total_races, 0)) OVER (PARTITION BY race_id), 0)
            ELSE 1.0 END AS exp_ratio,
        RANK() OVER (PARTITION BY race_id ORDER BY COALESCE(win_odds, 999)) AS odds_rank
    FROM race_training_dataset_new
    WHERE horse_slug IS NOT NULL
) rp
WHERE rtd.race_id = rp.race_id AND rtd.horse_slug = rp.horse_slug;

-- ============================================================================
-- STEP 8: ODDS FEATURES + VALUE INDICATORS (single UPDATE)
-- ============================================================================
UPDATE race_training_dataset_new SET
    is_favorite = (odds_rank_in_race = 1),
    odds_implied_probability = CASE WHEN win_odds IS NOT NULL AND win_odds > 0 THEN ROUND(1.0 / win_odds, 4) ELSE NULL END,
    odds_value_score = CASE WHEN win_odds > 0 AND win_percentage > 0 THEN ROUND((win_percentage / 100.0) - (1.0 / win_odds), 4) ELSE 0 END,
    is_longshot = (win_odds > 20),
    value_indicator = ROUND(COALESCE(win_percentage, 0) / 100.0 - COALESCE(CASE WHEN win_odds > 0 THEN 1.0 / win_odds ELSE 0.1 END, 0.1), 4),
    market_undervalued = (COALESCE(win_percentage, 0) / 100.0 > COALESCE(CASE WHEN win_odds > 0 THEN 1.0 / win_odds ELSE 0 END, 0) * 1.2),
    elo_vs_odds_gap = ROUND(COALESCE(elo_percentile_in_race, 0.5) - COALESCE(odds_percentile_in_race, 0.5), 4),
    form_vs_odds_gap = ROUND(COALESCE(form_momentum, 0.5) - COALESCE(odds_percentile_in_race, 0.5), 4),
    improving_longshot = (COALESCE(form_momentum, 0) > 0.6 AND COALESCE(win_odds, 0) > 10),
    class_drop_longshot = (COALESCE(class_drop, FALSE) = TRUE AND COALESCE(win_odds, 0) > 10),
    connection_longshot = ((COALESCE(jockey_win_rate, 0) > 0.15 OR COALESCE(trainer_win_rate, 0) > 0.15) AND COALESCE(win_odds, 0) > 10);

-- ============================================================================
-- STEP 9: DISTANCE FEATURES
-- ============================================================================
UPDATE race_training_dataset_new SET
    distance_suited_score = CASE
        WHEN races_at_distance > 0 AND distance_win_rate > 0 THEN LEAST(1.0, GREATEST(0, 0.5 + distance_win_rate))
        ELSE 0.5 END,
    distance_experience_level = CASE
        WHEN races_at_distance >= 10 THEN 3
        WHEN races_at_distance >= 5 THEN 2
        WHEN races_at_distance >= 1 THEN 1
        ELSE 0 END,
    track_experience_level = CASE
        WHEN races_at_track >= 10 THEN 3
        WHEN races_at_track >= 5 THEN 2
        WHEN races_at_track >= 1 THEN 1
        ELSE 0 END;

-- Distance range stats
UPDATE race_training_dataset_new AS rtd SET
    distance_range_experience = COALESCE(drs.at_range, 0),
    distance_range_win_rate = CASE WHEN COALESCE(drs.at_range, 0) > 0 THEN ROUND(drs.wins_at_range::NUMERIC / drs.at_range, 4) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w AS at_range,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins_at_range
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND distance_range IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug, distance_range ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) drs
WHERE rtd.race_id = drs.race_id AND rtd.horse_location_slug = drs.horse_location_slug;

-- ============================================================================
-- STEP 10: EXTENDED RUNNING STYLE + SPEED MAP
-- ============================================================================
UPDATE race_training_dataset_new SET
    historical_late_improvement = COALESCE(historical_avg_improvement, 0),
    best_late_improvement = COALESCE(pos_improvement_800_finish, 0),
    avg_late_improvement = COALESCE(historical_avg_improvement, 0),
    speed_map_position = CASE
        WHEN running_style = 'leader' THEN 'leading'
        WHEN running_style = 'on_pace' THEN 'prominent'
        WHEN running_style = 'midfield' THEN 'midfield'
        WHEN running_style IN ('off_pace', 'closer') THEN 'back'
        ELSE 'unknown' END,
    speed_rating = COALESCE(speed_figure, 80);

-- ============================================================================
-- STEP 11: FORM FLAGS + RUNS IN LAST 60/90 DAYS
-- ============================================================================
UPDATE race_training_dataset_new SET
    has_recent_form = (days_since_last_race IS NOT NULL AND days_since_last_race <= 30),
    has_winning_form = (last_5_win_rate > 0);

-- Runs in last 60/90 days
UPDATE race_training_dataset_new AS rtd SET
    runs_last_60_days = rc.runs_60,
    runs_last_90_days = rc.runs_90,
    avg_days_between_runs = CASE WHEN rc.runs_90 > 1 THEN ROUND(90.0 / rc.runs_90, 1) ELSE NULL END
FROM (
    WITH ranked AS (
        SELECT race_id, horse_location_slug, race_date,
            ROW_NUMBER() OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id) AS race_num
        FROM race_training_dataset_new
        WHERE horse_location_slug IS NOT NULL
    )
    SELECT r1.race_id, r1.horse_location_slug,
        COUNT(r2.race_id) FILTER (WHERE r2.race_date >= r1.race_date - 60 AND r2.race_date < r1.race_date) AS runs_60,
        COUNT(r2.race_id) FILTER (WHERE r2.race_date >= r1.race_date - 90 AND r2.race_date < r1.race_date) AS runs_90
    FROM ranked r1
    LEFT JOIN ranked r2 ON r1.horse_location_slug = r2.horse_location_slug AND r2.race_num < r1.race_num
    GROUP BY r1.race_id, r1.horse_location_slug
) rc
WHERE rtd.race_id = rc.race_id AND rtd.horse_location_slug = rc.horse_location_slug;

-- ============================================================================
-- STEP 12: WEIGHT FEATURES
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    weight_carried_avg = ROUND(wh.avg_weight, 1),
    weight_vs_avg = CASE WHEN wh.avg_weight > 0 THEN ROUND((rtd.weight - wh.avg_weight), 2) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        AVG(weight) OVER w_prior AS avg_weight
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND weight IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) wh
WHERE rtd.race_id = wh.race_id AND rtd.horse_location_slug = wh.horse_location_slug;

-- ============================================================================
-- STEP 13: CONNECTION FEATURES (Jockey-Horse + Jockey-Trainer)
-- ============================================================================
-- Jockey-Horse
UPDATE race_training_dataset_new AS rtd SET
    jockey_horse_connection_races = COALESCE(jh.races_together, 0),
    jockey_horse_connection_wins = COALESCE(jh.wins_together, 0),
    jockey_horse_connection_win_rate = CASE WHEN COALESCE(jh.races_together, 0) > 0 THEN ROUND(jh.wins_together::NUMERIC / jh.races_together, 4) ELSE 0 END,
    strong_jockey_connection = (COALESCE(jh.races_together, 0) >= 3 AND CASE WHEN jh.races_together > 0 THEN jh.wins_together::NUMERIC / jh.races_together ELSE 0 END > 0.2)
FROM (
    SELECT race_id, horse_location_slug, jockey_slug,
        COUNT(*) OVER w AS races_together,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins_together
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND jockey_slug IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug, jockey_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) jh
WHERE rtd.race_id = jh.race_id AND rtd.horse_location_slug = jh.horse_location_slug;

-- Jockey-Trainer
UPDATE race_training_dataset_new AS rtd SET
    jockey_trainer_connection_races = COALESCE(jt.races_together, 0),
    jockey_trainer_connection_wins = COALESCE(jt.wins_together, 0),
    jockey_trainer_connection_win_rate = CASE WHEN COALESCE(jt.races_together, 0) > 0 THEN ROUND(jt.wins_together::NUMERIC / jt.races_together, 4) ELSE 0 END,
    strong_trainer_connection = (COALESCE(jt.races_together, 0) >= 5 AND CASE WHEN jt.races_together > 0 THEN jt.wins_together::NUMERIC / jt.races_together ELSE 0 END > 0.15)
FROM (
    SELECT race_id, jockey_slug, trainer_slug,
        COUNT(*) OVER w AS races_together,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins_together
    FROM race_training_dataset_new
    WHERE jockey_slug IS NOT NULL AND trainer_slug IS NOT NULL
    WINDOW w AS (PARTITION BY jockey_slug, trainer_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) jt
WHERE rtd.race_id = jt.race_id AND rtd.jockey_slug = jt.jockey_slug AND rtd.trainer_slug = jt.trainer_slug;

-- Total connection score
UPDATE race_training_dataset_new SET
    total_connection_score = ROUND(
        COALESCE(jockey_horse_connection_win_rate, 0) * 0.5 +
        COALESCE(jockey_trainer_connection_win_rate, 0) * 0.3 +
        CASE WHEN strong_jockey_connection THEN 0.1 ELSE 0 END +
        CASE WHEN strong_trainer_connection THEN 0.1 ELSE 0 END
    , 4);

-- ============================================================================
-- STEP 14: TRACK-SPECIFIC WIN RATES (Jockey/Trainer at track)
-- ============================================================================
-- Jockey at track
UPDATE race_training_dataset_new AS rtd SET
    jockey_track_win_rate = CASE WHEN COALESCE(jt.rides, 0) > 0 THEN ROUND(jt.wins::NUMERIC / jt.rides, 4) ELSE 0 END
FROM (
    SELECT race_id, jockey_slug, track_name,
        COUNT(*) OVER w AS rides, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins
    FROM race_training_dataset_new WHERE jockey_slug IS NOT NULL AND track_name IS NOT NULL
    WINDOW w AS (PARTITION BY jockey_slug, track_name ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) jt
WHERE rtd.race_id = jt.race_id AND rtd.jockey_slug = jt.jockey_slug AND rtd.track_name = jt.track_name;

-- Trainer at track
UPDATE race_training_dataset_new AS rtd SET
    trainer_track_win_rate = CASE WHEN COALESCE(tt.runners, 0) > 0 THEN ROUND(tt.wins::NUMERIC / tt.runners, 4) ELSE 0 END
FROM (
    SELECT race_id, trainer_slug, track_name,
        COUNT(*) OVER w AS runners, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins
    FROM race_training_dataset_new WHERE trainer_slug IS NOT NULL AND track_name IS NOT NULL
    WINDOW w AS (PARTITION BY trainer_slug, track_name ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) tt
WHERE rtd.race_id = tt.race_id AND rtd.trainer_slug = tt.trainer_slug AND rtd.track_name = tt.track_name;

-- Track category win rate
UPDATE race_training_dataset_new AS rtd SET
    track_category_win_rate = CASE WHEN COALESCE(tcs.at_cat, 0) > 0 THEN ROUND(tcs.wins_at_cat::NUMERIC / tcs.at_cat, 4) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w AS at_cat, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins_at_cat
    FROM race_training_dataset_new WHERE horse_location_slug IS NOT NULL AND track_category IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug, track_category ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) tcs
WHERE rtd.race_id = tcs.race_id AND rtd.horse_location_slug = tcs.horse_location_slug;

-- Track place rate
UPDATE race_training_dataset_new AS rtd SET
    track_place_rate = CASE WHEN COALESCE(tps.at_track, 0) > 0 THEN ROUND(tps.places_at_track::NUMERIC / tps.at_track, 4) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w AS at_track, SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w AS places_at_track
    FROM race_training_dataset_new WHERE horse_location_slug IS NOT NULL AND track_name IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug, track_name ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) tps
WHERE rtd.race_id = tps.race_id AND rtd.horse_location_slug = tps.horse_location_slug;

-- ============================================================================
-- STEP 15: TRACK CONDITION NUMERIC + EXPERIENCE COLUMNS
-- ============================================================================
-- IMPORTANT: Specific numbered conditions MUST come before generic patterns!
-- ILIKE '%good%' would match 'Good 3', 'Good 4' etc. before they reach their own rules.
-- Scale: 1 (firmest) → 7 (heaviest) — monotonic, higher = wetter
-- Verified: Firm=1, Good/Good2=2, Good3/Good4=3, Soft5/Soft6=4, Soft7/Soft=5, Heavy8=6, Heavy9/Heavy=7
UPDATE race_training_dataset_new SET
    track_condition_numeric = CASE
        -- Firm (driest)
        WHEN track_condition ILIKE 'firm%' OR track_condition ILIKE 'good 1%' OR track_condition ILIKE 'good 2%' THEN 1
        -- Good 3 / Good 4 (specific BEFORE generic 'good')
        WHEN track_condition ILIKE 'good 3%' OR track_condition ILIKE 'good 4%' THEN 3
        -- Generic Good (must come AFTER Good 3/4)
        WHEN track_condition ILIKE 'good%' THEN 2
        -- Soft 5 / Soft 6 (specific BEFORE generic 'soft')
        WHEN track_condition ILIKE 'soft 5%' OR track_condition ILIKE 'soft 6%' THEN 4
        -- Soft 7 (specific BEFORE generic 'soft')
        WHEN track_condition ILIKE 'soft 7%' THEN 5
        -- Generic Soft (must come AFTER Soft 5/6/7)
        WHEN track_condition ILIKE 'soft%' THEN 5
        -- Yielding ≈ Soft 5/6
        WHEN track_condition ILIKE 'yielding%' THEN 4
        -- Heavy 10 (worst)
        WHEN track_condition ILIKE 'heavy 10%' THEN 7
        -- Heavy 9 (specific BEFORE generic 'heavy')
        WHEN track_condition ILIKE 'heavy 9%' THEN 7
        -- Heavy 8 (specific BEFORE generic 'heavy')
        WHEN track_condition ILIKE 'heavy 8%' THEN 6
        -- Generic Heavy (must come AFTER Heavy 8/9/10)
        WHEN track_condition ILIKE 'heavy%' THEN 7
        -- Wet (generic)
        WHEN track_condition ILIKE 'wet%' THEN 5
        -- Synthetic/All Weather
        WHEN track_condition ILIKE 'synthetic%' OR track_condition ILIKE 'all weather%' THEN 3
        ELSE 3
    END
WHERE track_condition IS NOT NULL;

-- Track condition wins (anti-leakage)
UPDATE race_training_dataset_new AS rtd SET
    track_condition_wins = COALESCE(cw.prior_wins, 0)
FROM (
    SELECT race_id, horse_location_slug,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY horse_location_slug, track_condition ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_wins
    FROM race_training_dataset_new WHERE horse_location_slug IS NOT NULL AND track_condition IS NOT NULL
) cw
WHERE rtd.race_id = cw.race_id AND rtd.horse_location_slug = cw.horse_location_slug;

-- Jockey total rides (anti-leakage from race_results)
UPDATE race_training_dataset_new AS rtd SET
    jockey_total_rides = COALESCE(jrc.prior_rides, 0)
FROM (
    SELECT rr.jockey_slug, rr.race_id,
        COUNT(*) OVER (PARTITION BY rr.jockey_slug ORDER BY r.race_date, rr.race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_rides
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id
    WHERE rr.jockey_slug IS NOT NULL
) jrc
WHERE rtd.race_id = jrc.race_id AND rtd.jockey_slug = jrc.jockey_slug;

-- Trainer total runners (anti-leakage from race_results)
UPDATE race_training_dataset_new AS rtd SET
    trainer_total_runners = COALESCE(trc.prior_runners, 0)
FROM (
    SELECT rr.trainer_slug, rr.race_id,
        COUNT(*) OVER (PARTITION BY rr.trainer_slug ORDER BY r.race_date, rr.race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_runners
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id
    WHERE rr.trainer_slug IS NOT NULL
) trc
WHERE rtd.race_id = trc.race_id AND rtd.trainer_slug = trc.trainer_slug;

-- ============================================================================
-- STEP 16: FEATURE INTERACTIONS (single UPDATE)
-- ============================================================================
UPDATE race_training_dataset_new SET
    -- ELO interactions: normalize by 1200 (iterative ELO base from horse_elo_ratings)
    elo_x_jockey_win_rate = ROUND(COALESCE(horse_elo, 1200)::NUMERIC / 1200 * COALESCE(jockey_win_rate, 0), 4),
    elo_x_trainer_win_rate = ROUND(COALESCE(horse_elo, 1200)::NUMERIC / 1200 * COALESCE(trainer_win_rate, 0), 4),
    weight_x_elo = ROUND(COALESCE(weight, 55)::NUMERIC / 55 * COALESCE(horse_elo, 1200)::NUMERIC / 1200, 4),
    barrier_x_total_runners = ROUND(CASE WHEN COALESCE(total_runners, 1) > 0 THEN COALESCE(barrier, 1)::NUMERIC / total_runners ELSE 0.5 END, 4),
    barrier_x_distance = ROUND(CASE WHEN COALESCE(race_distance, 1200) > 0 THEN COALESCE(barrier, 1)::NUMERIC / (race_distance::NUMERIC / 200) ELSE 0 END, 4),
    barrier_x_direction = ROUND(CASE
        WHEN track_direction = 'clockwise' THEN (1 - COALESCE(barrier, 1)::NUMERIC / NULLIF(total_runners, 0)) * 1
        WHEN track_direction = 'anticlockwise' THEN (1 - COALESCE(barrier, 1)::NUMERIC / NULLIF(total_runners, 0)) * -1
        ELSE 0 END, 4),
    running_style_x_direction = ROUND(CASE
        WHEN LOWER(running_style) = 'leader' THEN CASE WHEN track_direction = 'clockwise' THEN 1.0 WHEN track_direction = 'anticlockwise' THEN -1.0 ELSE 0 END
        WHEN LOWER(running_style) IN ('on_pace', 'stalker') THEN CASE WHEN track_direction = 'clockwise' THEN 0.5 WHEN track_direction = 'anticlockwise' THEN -0.5 ELSE 0 END
        WHEN LOWER(running_style) = 'midfield' THEN 0
        WHEN LOWER(running_style) IN ('off_pace') THEN CASE WHEN track_direction = 'clockwise' THEN -0.5 WHEN track_direction = 'anticlockwise' THEN 0.5 ELSE 0 END
        WHEN LOWER(running_style) = 'closer' THEN CASE WHEN track_direction = 'clockwise' THEN -1.0 WHEN track_direction = 'anticlockwise' THEN 1.0 ELSE 0 END
        ELSE 0 END, 4),
    jockey_wr_x_trainer_wr = ROUND(COALESCE(jockey_win_rate, 0) * COALESCE(trainer_win_rate, 0), 4),
    jockey_place_x_trainer_place = ROUND(COALESCE(jockey_place_rate, 0) * COALESCE(trainer_place_rate, 0), 4),
    form_x_freshness = ROUND(COALESCE(form_momentum, 0) * COALESCE(form_recency_score, 0.5), 4),
    track_wr_x_distance_wr = ROUND(COALESCE(track_win_rate, 0) * COALESCE(distance_win_rate, 0), 4),
    running_style_x_barrier = ROUND(CASE
        WHEN LOWER(running_style) = 'leader' THEN CASE WHEN barrier_position = 'inner' THEN 1.0 WHEN barrier_position = 'middle' THEN 0.5 ELSE 0.2 END
        WHEN LOWER(running_style) IN ('on_pace', 'stalker') THEN CASE WHEN barrier_position = 'inner' THEN 0.8 WHEN barrier_position = 'middle' THEN 0.7 ELSE 0.4 END
        WHEN LOWER(running_style) = 'closer' THEN CASE WHEN barrier_position = 'outer' THEN 0.6 WHEN barrier_position = 'middle' THEN 0.5 ELSE 0.4 END
        ELSE 0.5 END, 4);

-- Competitive level score
UPDATE race_training_dataset_new SET
    competitive_level_score = ROUND(
        COALESCE(elo_percentile_in_race, 0.5) * 0.3 +
        COALESCE(win_pct_vs_field_avg, 1) / 5.0 * 0.25 +
        COALESCE(jockey_win_rate, 0) * 3 * 0.2 +
        COALESCE(trainer_win_rate, 0) * 3 * 0.15 +
        COALESCE(form_momentum, 0) * 0.1
    , 4);

-- ============================================================================
-- STEP 17: BARRIER ANALYSIS (Track-specific advantages)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    barrier_advantage_at_track = ROUND(COALESCE(bts.win_rate, 0) - COALESCE(ta.avg_track_wr, 0.1), 4),
    barrier_effectiveness_score = ROUND(CASE WHEN COALESCE(ta.avg_track_wr, 0) > 0 THEN COALESCE(bts.win_rate, 0) / ta.avg_track_wr ELSE 1 END, 4)
FROM (
    SELECT track_name, barrier_position, ROUND(SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0), 4) AS win_rate
    FROM race_training_dataset_new WHERE barrier_position IS NOT NULL AND track_name IS NOT NULL
    GROUP BY track_name, barrier_position
) bts
JOIN (
    SELECT track_name, AVG(win_rate) AS avg_track_wr
    FROM (SELECT track_name, barrier_position, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0) AS win_rate
          FROM race_training_dataset_new WHERE barrier_position IS NOT NULL GROUP BY track_name, barrier_position) sub
    GROUP BY track_name
) ta ON bts.track_name = ta.track_name
WHERE rtd.track_name = bts.track_name AND rtd.barrier_position = bts.barrier_position;

-- Horse barrier group history
UPDATE race_training_dataset_new AS rtd SET
    horse_barrier_group_starts = COALESCE(hbh.starts, 0),
    horse_barrier_group_win_rate = CASE WHEN COALESCE(hbh.starts, 0) > 0 THEN ROUND(hbh.wins::NUMERIC / hbh.starts, 4) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w AS starts, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins
    FROM race_training_dataset_new WHERE horse_location_slug IS NOT NULL AND barrier_position IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug, barrier_position ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) hbh
WHERE rtd.race_id = hbh.race_id AND rtd.horse_location_slug = hbh.horse_location_slug;

-- ============================================================================
-- STEP 18: ANTI-LEAKAGE FLAGS + MODEL BIAS
-- ============================================================================
UPDATE race_training_dataset_new SET
    extreme_staleness_flag = (COALESCE(days_since_last_race, 0) > 180),
    low_sample_flag = (COALESCE(total_races, 0) < 3),
    new_jockey_flag = (COALESCE(jockey_total_rides, 0) < 10),
    new_trainer_flag = (COALESCE(trainer_total_runners, 0) < 10);

-- Model bias columns
UPDATE race_training_dataset_new AS rtd SET
    horse_model_win_bias  = ROUND(COALESCE(hp.horse_win_bias, 0), 4),
    horse_model_top3_bias = ROUND(COALESCE(hp.horse_place_bias, 0), 4),
    jockey_model_win_bias  = ROUND(COALESCE(hp.jockey_win_bias, 0), 4),
    jockey_model_top3_bias = ROUND(COALESCE(hp.jockey_place_bias, 0), 4),
    trainer_model_win_bias  = ROUND(COALESCE(hp.trainer_win_bias, 0), 4),
    trainer_model_top3_bias = ROUND(COALESCE(hp.trainer_place_bias, 0), 4)
FROM (
    SELECT race_id, horse_location_slug,
        AVG(CASE WHEN final_position = 1 THEN 1 ELSE 0 END - (COALESCE(horse_elo, 1200) - 1100) / 600.0 * 0.1) OVER w_horse AS horse_win_bias,
        AVG(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END - 0.3) OVER w_horse AS horse_place_bias,
        AVG(CASE WHEN final_position = 1 THEN 1 ELSE 0 END - 0.1) OVER w_jockey AS jockey_win_bias,
        AVG(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END - 0.3) OVER w_jockey AS jockey_place_bias,
        AVG(CASE WHEN final_position = 1 THEN 1 ELSE 0 END - 0.1) OVER w_trainer AS trainer_win_bias,
        AVG(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END - 0.3) OVER w_trainer AS trainer_place_bias
    FROM race_training_dataset_new
    WHERE final_position IS NOT NULL
    WINDOW
        w_horse AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w_jockey AS (PARTITION BY jockey_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w_trainer AS (PARTITION BY trainer_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) hp
WHERE rtd.race_id = hp.race_id AND rtd.horse_location_slug = hp.horse_location_slug;

-- ============================================================================
-- STEP 19: CLOSING POWER + EXTENDED BARRIER FEATURES
-- ============================================================================
UPDATE race_training_dataset_new SET
    closing_power_score = ROUND(
        COALESCE(closing_ability_score, 0.5) * 0.5 +
        COALESCE(sustained_run_score, 0.5) * 0.3 +
        CASE WHEN running_style IN ('closer', 'off_pace') THEN 0.2 ELSE 0 END
    , 4),
    strong_closer_flag = (COALESCE(closing_ability_score, 0) > 0.7 AND running_style IN ('closer', 'off_pace')),
    barrier_vs_run_style = ROUND(CASE
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
        ELSE 0.5 END, 4),
    barrier_track_condition_advantage = ROUND(
        CASE WHEN track_condition_numeric <= 2 AND barrier_position = 'inner' THEN 0.15
             WHEN track_condition_numeric <= 2 AND barrier_position = 'middle' THEN 0.10
             WHEN track_condition_numeric >= 4 AND barrier_position = 'outer' THEN 0.08
             ELSE 0 END +
        CASE WHEN race_distance > 2000 AND barrier_position = 'inner' THEN 0.10
             WHEN race_distance > 2000 AND barrier_position = 'middle' THEN 0.05
             ELSE 0 END
    , 4),
    weight_relative_x_distance = ROUND(COALESCE(weight_vs_avg, 0) * (COALESCE(race_distance, 1400) / 1400.0), 4);

-- ============================================================================
-- STEP 20: TRACK-DISTANCE FEATURES + DISTANCE VARIANCE
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    track_distance_experience = COALESCE(tde.exp, 0),
    track_distance_win_rate_weighted = CASE WHEN COALESCE(tde.exp, 0) > 0 THEN ROUND(tde.wins::NUMERIC / tde.exp * (1 + LN(tde.exp + 1) / 5.0), 4) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w AS exp, SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins
    FROM race_training_dataset_new WHERE horse_location_slug IS NOT NULL AND track_name IS NOT NULL AND distance_range IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug, track_name, distance_range ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) tde
WHERE rtd.race_id = tde.race_id AND rtd.horse_location_slug = tde.horse_location_slug;

-- Distance variance
UPDATE race_training_dataset_new AS rtd SET
    distance_variance = ROUND(COALESCE(ds.dist_var, 0), 2)
FROM (
    SELECT race_id, horse_location_slug,
        STDDEV(race_distance) OVER w_prior AS dist_var
    FROM race_training_dataset_new WHERE horse_location_slug IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) ds
WHERE rtd.race_id = ds.race_id AND rtd.horse_location_slug = ds.horse_location_slug;

-- Optimal distance (Anti-leakage: only wins BEFORE this race)
UPDATE race_training_dataset_new AS rtd SET
    optimal_distance_min = COALESCE(wd.prior_min_win_dist, 1000),
    optimal_distance_max = COALESCE(wd.prior_max_win_dist, 2400)
FROM (
    SELECT race_id, horse_location_slug,
        MIN(CASE WHEN final_position = 1 THEN race_distance ELSE NULL END) OVER w_prior AS prior_min_win_dist,
        MAX(CASE WHEN final_position = 1 THEN race_distance ELSE NULL END) OVER w_prior AS prior_max_win_dist
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) wd
WHERE rtd.race_id = wd.race_id AND rtd.horse_location_slug = wd.horse_location_slug;

-- Model prediction history (Anti-leakage: count + error only from PRIOR races)
UPDATE race_training_dataset_new AS rtd SET
    horse_prediction_history_count = COALESCE(ph.prior_race_count, 0),
    horse_model_pos_error = ROUND(COALESCE(ph.prior_avg_pos_error, 3.5), 4)
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w_prior AS prior_race_count,
        AVG(ABS(COALESCE(final_position, 5) - 3)) OVER w_prior AS prior_avg_pos_error
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND final_position IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) ph
WHERE rtd.race_id = ph.race_id AND rtd.horse_location_slug = ph.horse_location_slug;

-- ============================================================================
-- STEP 21: MISC FEATURES (single UPDATE)
-- ============================================================================
UPDATE race_training_dataset_new SET
    career_roi_estimate = CASE WHEN total_races > 0 AND win_percentage > 0 THEN ROUND((win_percentage / 100.0 * 10 - 1), 4) ELSE -1 END,
    consistency_score = ROUND(COALESCE(place_percentage, 0) / 100.0, 4),
    race_experience_level = CASE WHEN total_races >= 30 THEN 4 WHEN total_races >= 15 THEN 3 WHEN total_races >= 5 THEN 2 WHEN total_races >= 1 THEN 1 ELSE 0 END,
    against_grain_score = CASE WHEN elo_percentile_in_race > 0.7 AND odds_rank_in_race > 3 THEN 0.8 WHEN elo_percentile_in_race > 0.5 AND odds_rank_in_race > 5 THEN 0.5 ELSE 0.2 END,
    longshot_value_profile = CASE WHEN win_odds > 15 AND form_momentum > 0.5 THEN 0.8 WHEN win_odds > 10 AND form_momentum > 0.4 THEN 0.5 ELSE 0.2 END,
    historical_value_winner = (COALESCE(win_percentage, 0) > 20 AND COALESCE(odds_implied_probability, 1) < 0.15),
    peak_performance_indicator = ROUND(COALESCE(form_momentum, 0) * 0.4 + COALESCE(elo_percentile_in_race, 0.5) * 0.3 + COALESCE(win_pct_vs_field_avg, 1) / 5.0 * 0.3, 4);

-- Specialization score
UPDATE race_training_dataset_new SET
    specialization_score = ROUND(
        CASE WHEN races_at_track >= 5 THEN 0.3 ELSE 0 END +
        CASE WHEN races_at_distance >= 5 THEN 0.3 ELSE 0 END +
        CASE WHEN track_condition_wins >= 2 THEN 0.2 ELSE 0 END +
        CASE WHEN COALESCE(distance_range_experience, 0) >= 5 THEN 0.2 ELSE 0 END
    , 4);

-- Debutant predicted ability
UPDATE race_training_dataset_new SET
    debutant_predicted_ability = CASE
        WHEN total_races = 0 OR total_races IS NULL THEN ROUND(0.5 + COALESCE(trainer_win_rate, 0) * 0.3 + COALESCE(jockey_win_rate, 0) * 0.2, 4)
        ELSE NULL END;

-- ============================================================================
-- STEP 22: ENHANCED SECTIONAL FEATURES (from race_results_sectional_times)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    avg_pos_200m = ROUND(rs.hist_avg_pos_200m, 2),
    avg_sectional_400m = ROUND(rs.hist_avg_sec_400m, 3),
    avg_sectional_800m = ROUND(rs.hist_avg_sec_800m, 3),
    avg_cumulative_800m = ROUND(rs.hist_avg_cum_800m, 3),
    sectional_time_consistency = ROUND(rs.hist_sec_time_consistency, 3),
    early_vs_mid_pace_change = ROUND(rs.hist_pace_change, 3),
    historical_speed_vs_field = ROUND(rs.hist_speed_vs_field, 4)
FROM (
    WITH horse_history AS (
        SELECT rr.horse_slug, s.race_id,
            s.position_200m, s.position_400m AS s_pos_400m, s.position_800m AS s_pos_800m, s.finish_position,
            EXTRACT(EPOCH FROM s.sectional_400m) AS sec_400m_time,
            EXTRACT(EPOCH FROM s.sectional_800m) AS sec_800m_time,
            EXTRACT(EPOCH FROM s.cumulative_800m) AS cum_800m_time,
            ROW_NUMBER() OVER (PARTITION BY rr.horse_slug ORDER BY r.race_date, s.race_id) AS race_seq
        FROM race_results_sectional_times s
        JOIN race_results rr ON s.race_id = rr.race_id AND s.horse_number::TEXT = rr.horse_number
        JOIN races r ON s.race_id = r.race_id
        WHERE s.finish_position IS NOT NULL AND s.finish_position <= 20 AND rr.horse_slug IS NOT NULL
    ),
    race_avgs AS (
        SELECT race_id, AVG(sec_400m_time) AS race_avg_sec_400m FROM horse_history WHERE sec_400m_time IS NOT NULL GROUP BY race_id
    )
    SELECT h.horse_slug, h.race_id,
        AVG(h.position_200m) OVER w AS hist_avg_pos_200m,
        AVG(h.sec_400m_time) OVER w AS hist_avg_sec_400m,
        AVG(h.sec_800m_time) OVER w AS hist_avg_sec_800m,
        AVG(h.cum_800m_time) OVER w AS hist_avg_cum_800m,
        STDDEV(h.sec_400m_time) OVER w AS hist_sec_time_consistency,
        AVG(h.sec_800m_time - h.sec_400m_time) OVER w AS hist_pace_change,
        AVG(ra.race_avg_sec_400m - h.sec_400m_time) OVER w AS hist_speed_vs_field
    FROM horse_history h
    LEFT JOIN race_avgs ra ON h.race_id = ra.race_id
    WHERE h.s_pos_800m IS NOT NULL
    WINDOW w AS (PARTITION BY h.horse_slug ORDER BY h.race_seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) rs
WHERE rtd.race_id = rs.race_id AND rtd.horse_slug = rs.horse_slug;

-- Position columns from sectional times (leakage - will be removed in Phase 3)
UPDATE race_training_dataset_new AS rtd SET
    position_at_400 = s.position_400m,
    position_at_800 = s.position_800m,
    position_at_end = s.finish_position
FROM race_results_sectional_times s
JOIN race_results rr ON s.race_id = rr.race_id AND s.horse_number::TEXT = rr.horse_number
WHERE rtd.race_id = s.race_id AND rtd.horse_slug = rr.horse_slug;

-- Running style distance fit + leader advantage
UPDATE race_training_dataset_new SET
    running_style_distance_fit = CASE
        WHEN LOWER(running_style) = 'leader' AND race_distance < 1200 THEN 0.9
        WHEN LOWER(running_style) = 'leader' AND race_distance < 1600 THEN 0.7
        WHEN LOWER(running_style) = 'leader' THEN 0.5
        WHEN LOWER(running_style) IN ('on_pace', 'stalker') AND race_distance BETWEEN 1200 AND 1800 THEN 0.85
        WHEN LOWER(running_style) = 'midfield' THEN 0.7
        WHEN LOWER(running_style) = 'closer' AND race_distance >= 2000 THEN 0.95
        WHEN LOWER(running_style) = 'closer' AND race_distance >= 1600 THEN 0.85
        ELSE 0.5 END,
    leader_advantage_score = CASE
        WHEN LOWER(running_style) = 'leader' AND race_distance < 1200 THEN 0.8
        WHEN LOWER(running_style) = 'leader' AND race_distance < 1600 THEN 0.5
        WHEN LOWER(running_style) = 'leader' THEN 0.2
        WHEN LOWER(running_style) = 'closer' THEN -0.3
        ELSE 0.0 END;

-- ============================================================================
-- STEP 23: METRO WIN FEATURES
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    has_won_at_metro = CASE WHEN COALESCE(rm.prior_metro_wins, 0) > 0 THEN 1 ELSE 0 END,
    days_since_metro_win = CASE WHEN rm.last_metro_win_date IS NOT NULL THEN (rm.race_date - rm.last_metro_win_date) ELSE NULL END,
    metro_wins_count = COALESCE(rm.prior_metro_wins, 0),
    metro_win_rate = CASE WHEN COALESCE(rm.prior_metro_starts, 0) > 0 THEN ROUND(rm.prior_metro_wins::NUMERIC / rm.prior_metro_starts, 4) ELSE NULL END
FROM (
    WITH metro_list AS (
        SELECT track_name FROM (VALUES
            ('Flemington'),('Caulfield'),('Moonee Valley'),('The Valley'),('Randwick'),('Royal Randwick'),
            ('Rosehill'),('Rosehill Gardens'),('Canterbury'),('Warwick Farm'),('Eagle Farm'),('Doomben'),
            ('Morphettville'),('Ascot'),('Belmont'),('Sha Tin'),('Happy Valley')
        ) AS t(track_name)
    ),
    horse_races AS (
        SELECT rr.horse_slug, rr.race_id, r.race_date,
            CASE WHEN ml.track_name IS NOT NULL THEN 1 ELSE 0 END AS is_metro,
            CASE WHEN rr.position = 1 AND ml.track_name IS NOT NULL THEN 1 ELSE 0 END AS is_metro_win,
            ROW_NUMBER() OVER (PARTITION BY rr.horse_slug ORDER BY r.race_date, rr.race_id) AS race_seq
        FROM race_results rr JOIN races r ON rr.race_id = r.race_id
        LEFT JOIN metro_list ml ON r.track_name = ml.track_name
        WHERE rr.horse_slug IS NOT NULL AND rr.position IS NOT NULL
    )
    SELECT horse_slug, race_id, race_date,
        SUM(is_metro_win) OVER w AS prior_metro_wins,
        SUM(is_metro) OVER w AS prior_metro_starts,
        MAX(CASE WHEN is_metro_win = 1 THEN race_date END) OVER w AS last_metro_win_date
    FROM horse_races
    WINDOW w AS (PARTITION BY horse_slug ORDER BY race_seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) rm
WHERE rtd.race_id = rm.race_id AND rtd.horse_slug = rm.horse_slug;

-- ============================================================================
-- STEP 24: WEATHER FEATURES
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    race_weather = r.race_weather,
    is_wet_weather = CASE WHEN LOWER(r.race_weather) LIKE '%rain%' THEN 1 ELSE 0 END
FROM races r WHERE rtd.race_id = r.race_id AND r.race_weather IS NOT NULL;

UPDATE race_training_dataset_new SET
    estimated_temperature = CASE
        WHEN track_name IN ('Happy Valley', 'Sha Tin') THEN
            CASE EXTRACT(MONTH FROM race_date)
                WHEN 1 THEN 17 WHEN 2 THEN 17 WHEN 3 THEN 20 WHEN 4 THEN 24
                WHEN 5 THEN 27 WHEN 6 THEN 29 WHEN 7 THEN 31 WHEN 8 THEN 31
                WHEN 9 THEN 29 WHEN 10 THEN 26 WHEN 11 THEN 22 WHEN 12 THEN 18 END
        WHEN location = 'AU' THEN
            CASE EXTRACT(MONTH FROM race_date)
                WHEN 1 THEN 28 WHEN 2 THEN 27 WHEN 3 THEN 24 WHEN 4 THEN 20
                WHEN 5 THEN 16 WHEN 6 THEN 13 WHEN 7 THEN 12 WHEN 8 THEN 14
                WHEN 9 THEN 17 WHEN 10 THEN 20 WHEN 11 THEN 23 WHEN 12 THEN 26 END
        ELSE 20 END;

-- ============================================================================
-- STEP 25: TRACK SPECIALIST FEATURES
-- ============================================================================
-- Has won at track (anti-leakage)
UPDATE race_training_dataset_new AS rtd SET
    has_won_at_track = COALESCE(twh.prior_track_wins, 0) > 0
FROM (
    SELECT rr.race_id, rr.horse_slug,
        SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY rr.horse_slug, r.track_name ORDER BY r.race_date, rr.race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_track_wins
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id WHERE rr.horse_slug IS NOT NULL
) twh WHERE rtd.race_id = twh.race_id AND rtd.horse_slug = twh.horse_slug;

-- Has won at distance (bucket 200m)
UPDATE race_training_dataset_new AS rtd SET
    has_won_at_distance = COALESCE(dwh.prior_dist_wins, 0) > 0
FROM (
    SELECT rr.race_id, rr.horse_slug,
        SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY rr.horse_slug, FLOOR(r.race_distance / 200) ORDER BY r.race_date, rr.race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_dist_wins
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id WHERE rr.horse_slug IS NOT NULL
) dwh WHERE rtd.race_id = dwh.race_id AND rtd.horse_slug = dwh.horse_slug;

-- Has won at track+distance
UPDATE race_training_dataset_new AS rtd SET
    has_won_at_track_distance = COALESCE(tdh.prior_td_wins, 0) > 0
FROM (
    SELECT rr.race_id, rr.horse_slug,
        SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY rr.horse_slug, r.track_name, FLOOR(r.race_distance / 200) ORDER BY r.race_date, rr.race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_td_wins
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id WHERE rr.horse_slug IS NOT NULL
) tdh WHERE rtd.race_id = tdh.race_id AND rtd.horse_slug = tdh.horse_slug;

-- Track specialist + preference
UPDATE race_training_dataset_new SET
    is_track_specialist = (COALESCE(track_win_rate, 0) > COALESCE(win_percentage, 0) * 1.5 AND COALESCE(track_win_rate, 0) > 0 AND total_races >= 5),
    track_distance_preference = CASE WHEN has_won_at_track_distance THEN 0.9 WHEN has_won_at_track OR has_won_at_distance THEN 0.6 ELSE 0.3 END;

-- ============================================================================
-- STEP 26: DEBUTANT + EXPERIENCE FEATURES
-- ============================================================================
UPDATE race_training_dataset_new SET
    experienced_horse_flag = (total_races >= 10),
    debutant_x_jockey_wr = CASE WHEN is_first_timer THEN COALESCE(jockey_win_rate, 0) ELSE NULL END,
    debutant_x_trainer_wr = CASE WHEN is_first_timer THEN COALESCE(trainer_win_rate, 0) ELSE NULL END,
    debutant_uncertainty_score = CASE WHEN is_first_timer THEN 1.0 WHEN total_races <= 3 THEN 0.7 WHEN total_races <= 6 THEN 0.4 ELSE 0.1 END;

-- ============================================================================
-- STEP 27: JOCKEY/TRAINER AVG POSITION FEATURES (Anti-leakage)
-- ============================================================================
-- FIX: Old code used global ROW_NUMBER DESC → every row got the SAME value
-- (jockey's latest 20 rides regardless of when THIS race happened).
-- New code: per-row rolling average of PRIOR 20/50 rides using window functions.
-- Each row only sees rides that happened BEFORE it → no future data leakage.

-- Jockey avg position (rolling last 20 / 50 PRIOR rides)
UPDATE race_training_dataset_new AS rtd SET
    jockey_avg_position_20 = ROUND(COALESCE(js.avg_pos_20, 6.0), 2),
    jockey_avg_position_50 = ROUND(COALESCE(js.avg_pos_50, 6.0), 2)
FROM (
    SELECT race_id, jockey_location_slug,
        AVG(final_position) OVER (
            PARTITION BY jockey_location_slug ORDER BY race_date, race_id
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_pos_20,
        AVG(final_position) OVER (
            PARTITION BY jockey_location_slug ORDER BY race_date, race_id
            ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
        ) AS avg_pos_50
    FROM race_training_dataset_new
    WHERE jockey_location_slug IS NOT NULL AND final_position IS NOT NULL AND final_position < 50
) js
WHERE rtd.race_id = js.race_id AND rtd.jockey_location_slug = js.jockey_location_slug;

-- Trainer avg position (rolling last 50 / 100 PRIOR runners)
UPDATE race_training_dataset_new AS rtd SET
    trainer_avg_position_50 = ROUND(COALESCE(ts.avg_pos_50, 6.0), 2),
    trainer_avg_position_100 = ROUND(COALESCE(ts.avg_pos_100, 6.0), 2)
FROM (
    SELECT race_id, trainer_location_slug,
        AVG(final_position) OVER (
            PARTITION BY trainer_location_slug ORDER BY race_date, race_id
            ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
        ) AS avg_pos_50,
        AVG(final_position) OVER (
            PARTITION BY trainer_location_slug ORDER BY race_date, race_id
            ROWS BETWEEN 100 PRECEDING AND 1 PRECEDING
        ) AS avg_pos_100
    FROM race_training_dataset_new
    WHERE trainer_location_slug IS NOT NULL AND final_position IS NOT NULL AND final_position < 50
) ts
WHERE rtd.race_id = ts.race_id AND rtd.trainer_location_slug = ts.trainer_location_slug;

-- Track condition advantage (horse's condition win rate vs overall)
-- FIX: Normalize track conditions to broad categories before matching
-- "Good"/"Good 3"/"Good 4" → 'good', "Soft"/"Soft 5"/"Soft 6"/"Soft 7" → 'soft_light'
-- "Heavy"/"heavy"/"Heavy 8"/"Heavy 9" → 'heavy', "Firm" → 'firm'
-- This reduces TCA null rate from ~49% to ~15% by pooling similar conditions
UPDATE race_training_dataset_new AS rtd SET
    track_condition_advantage = ROUND(hcs.condition_win_rate - COALESCE(rtd.win_percentage, 0) / 100.0, 4)
FROM (
    SELECT rr.horse_slug, 
        CASE
            WHEN LOWER(r.track_condition) LIKE 'good%' THEN 'good'
            WHEN LOWER(r.track_condition) IN ('firm', 'firm 1', 'firm 2') THEN 'firm'
            WHEN LOWER(r.track_condition) LIKE 'soft%' AND COALESCE(
                TRY_CAST(REGEXP_EXTRACT(r.track_condition, '[0-9]+') AS INTEGER), 5) <= 6 THEN 'soft_light'
            WHEN LOWER(r.track_condition) LIKE 'soft%' THEN 'soft_heavy'
            WHEN LOWER(r.track_condition) LIKE 'heavy%' THEN 'heavy'
            WHEN LOWER(r.track_condition) LIKE 'synthetic%' OR LOWER(r.track_condition) = 'synthetic' THEN 'synthetic'
            ELSE LOWER(r.track_condition)
        END AS condition_group,
        SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0) AS condition_win_rate
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id
    WHERE rr.horse_slug IS NOT NULL AND rr.position IS NOT NULL AND r.track_condition IS NOT NULL
    GROUP BY rr.horse_slug, condition_group HAVING COUNT(*) >= 2
) hcs
WHERE rtd.horse_slug = hcs.horse_slug 
  AND CASE
    WHEN LOWER(rtd.track_condition) LIKE 'good%' THEN 'good'
    WHEN LOWER(rtd.track_condition) IN ('firm', 'firm 1', 'firm 2') THEN 'firm'
    WHEN LOWER(rtd.track_condition) LIKE 'soft%' AND COALESCE(
        TRY_CAST(REGEXP_EXTRACT(rtd.track_condition, '[0-9]+') AS INTEGER), 5) <= 6 THEN 'soft_light'
    WHEN LOWER(rtd.track_condition) LIKE 'soft%' THEN 'soft_heavy'
    WHEN LOWER(rtd.track_condition) LIKE 'heavy%' THEN 'heavy'
    WHEN LOWER(rtd.track_condition) LIKE 'synthetic%' OR LOWER(rtd.track_condition) = 'synthetic' THEN 'synthetic'
    ELSE LOWER(rtd.track_condition)
  END = hcs.condition_group;

-- ============================================================================
-- STEP 28: SET DEFAULTS FOR MISC COLUMNS
-- ============================================================================
UPDATE race_training_dataset_new SET
    prize_money = COALESCE(prize_money, 0),
    sectional_800m = COALESCE(sectional_800m, 0),
    sectional_400m = COALESCE(sectional_400m, 0),
    age_restriction = COALESCE(age_restriction, 'open'),
    sex_restriction = COALESCE(sex_restriction, 'open'),
    is_class_drop = COALESCE(class_drop, FALSE);

-- ============================================================================
-- STEP 29: FADER / CLOSER / FRONT-RUNNER PATTERN FEATURES
-- ============================================================================
-- These are computed from historical sectional data (position_400m vs finish)
-- using anti-leakage window functions

UPDATE race_training_dataset_new AS rtd SET
    fader_count = COALESCE(fp.fader_cnt, 0),
    fader_rate = CASE WHEN COALESCE(fp.pattern_races, 0) > 0 THEN ROUND(fp.fader_cnt::NUMERIC / fp.pattern_races, 4) ELSE 0 END,
    closer_count = COALESCE(fp.closer_cnt, 0),
    closer_rate = CASE WHEN COALESCE(fp.pattern_races, 0) > 0 THEN ROUND(fp.closer_cnt::NUMERIC / fp.pattern_races, 4) ELSE 0 END,
    front_count = COALESCE(fp.front_cnt, 0),
    front_rate = CASE WHEN COALESCE(fp.pattern_races, 0) > 0 THEN ROUND(fp.front_cnt::NUMERIC / fp.pattern_races, 4) ELSE 0 END,
    is_historical_fader = (COALESCE(fp.pattern_races, 0) >= 3 AND COALESCE(fp.fader_cnt, 0)::NUMERIC / NULLIF(fp.pattern_races, 0) >= 0.30),
    avg_improvement_from_400m = CASE WHEN COALESCE(fp.pattern_races, 0) > 0 THEN ROUND(fp.avg_improv, 3) ELSE 0 END,
    front_closer_score = CASE WHEN COALESCE(fp.pattern_races, 0) > 0 THEN
        ROUND((1.0 - COALESCE(fp.avg_pos_400::NUMERIC / 10, 0.5)) * (1.0 + GREATEST(fp.avg_improv::NUMERIC / 5, 0)), 4)
        ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w_prior AS pattern_races,
        SUM(CASE WHEN position_400m IS NOT NULL AND final_position IS NOT NULL AND position_400m - final_position <= -2 THEN 1 ELSE 0 END) OVER w_prior AS fader_cnt,
        SUM(CASE WHEN position_400m IS NOT NULL AND final_position IS NOT NULL AND position_400m - final_position >= 3 THEN 1 ELSE 0 END) OVER w_prior AS closer_cnt,
        SUM(CASE WHEN position_400m IS NOT NULL AND position_400m <= 2 THEN 1 ELSE 0 END) OVER w_prior AS front_cnt,
        AVG(CASE WHEN position_400m IS NOT NULL AND final_position IS NOT NULL THEN position_400m - final_position ELSE NULL END) OVER w_prior AS avg_improv,
        AVG(position_400m) OVER w_prior AS avg_pos_400
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND position_400m IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) fp
WHERE rtd.race_id = fp.race_id AND rtd.horse_location_slug = fp.horse_location_slug;

-- Sprint fader rate (fade rate in sprint races <= 1200m)
UPDATE race_training_dataset_new AS rtd SET
    sprint_fader_rate = CASE WHEN COALESCE(sf.sprint_races, 0) > 0 THEN ROUND(sf.sprint_fades::NUMERIC / sf.sprint_races, 4) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        SUM(CASE WHEN race_distance <= 1200 THEN 1 ELSE 0 END) OVER w_prior AS sprint_races,
        SUM(CASE WHEN race_distance <= 1200 AND position_400m IS NOT NULL AND final_position IS NOT NULL AND position_400m - final_position <= -2 THEN 1 ELSE 0 END) OVER w_prior AS sprint_fades
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) sf
WHERE rtd.race_id = sf.race_id AND rtd.horse_location_slug = sf.horse_location_slug;

-- Staying leader rate (front-running rate in staying races > 2000m)
UPDATE race_training_dataset_new AS rtd SET
    staying_leader_rate = CASE WHEN COALESCE(sl.staying_races, 0) > 0 THEN ROUND(sl.staying_fronts::NUMERIC / sl.staying_races, 4) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        SUM(CASE WHEN race_distance > 2000 THEN 1 ELSE 0 END) OVER w_prior AS staying_races,
        SUM(CASE WHEN race_distance > 2000 AND position_400m IS NOT NULL AND position_400m <= 2 THEN 1 ELSE 0 END) OVER w_prior AS staying_fronts
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) sl
WHERE rtd.race_id = sl.race_id AND rtd.horse_location_slug = sl.horse_location_slug;

-- ============================================================================
-- STEP 30: AVG_POS_400M + AVG_LATE_SURGE (historical averages)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    avg_pos_400m = ROUND(hp.hist_avg_pos_400m, 2),
    avg_late_surge = ROUND(hp.hist_late_surge, 2)
FROM (
    SELECT race_id, horse_location_slug,
        AVG(position_400m) OVER w_prior AS hist_avg_pos_400m,
        AVG(COALESCE(position_800m, 0) - COALESCE(final_position, 0)) OVER w_prior AS hist_late_surge
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND position_400m IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) hp
WHERE rtd.race_id = hp.race_id AND rtd.horse_location_slug = hp.horse_location_slug;

-- ============================================================================
-- STEP 31: WET/DRY TRACK FEATURES (Anti-leakage)
-- ============================================================================
UPDATE race_training_dataset_new SET
    is_wet_track = (track_condition_numeric >= 5);

UPDATE race_training_dataset_new AS rtd SET
    horse_wet_track_runs = COALESCE(wt.wet_runs, 0),
    horse_dry_track_runs = COALESCE(wt.dry_runs, 0),
    horse_wet_track_win_rate = CASE WHEN COALESCE(wt.wet_runs, 0) > 0 THEN ROUND(wt.wet_wins::NUMERIC / wt.wet_runs, 4) ELSE 0 END,
    horse_dry_track_win_rate = CASE WHEN COALESCE(wt.dry_runs, 0) > 0 THEN ROUND(wt.dry_wins::NUMERIC / wt.dry_runs, 4) ELSE 0 END
FROM (
    SELECT race_id, horse_location_slug,
        SUM(CASE WHEN track_condition_numeric >= 5 THEN 1 ELSE 0 END) OVER w_prior AS wet_runs,
        SUM(CASE WHEN track_condition_numeric >= 5 AND final_position = 1 THEN 1 ELSE 0 END) OVER w_prior AS wet_wins,
        SUM(CASE WHEN track_condition_numeric <= 3 THEN 1 ELSE 0 END) OVER w_prior AS dry_runs,
        SUM(CASE WHEN track_condition_numeric <= 3 AND final_position = 1 THEN 1 ELSE 0 END) OVER w_prior AS dry_wins
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL AND track_condition_numeric IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) wt
WHERE rtd.race_id = wt.race_id AND rtd.horse_location_slug = wt.horse_location_slug;

-- Weather condition match + wet/dry preference
UPDATE race_training_dataset_new SET
    wet_dry_preference_score = ROUND(COALESCE(horse_wet_track_win_rate, 0) - COALESCE(horse_dry_track_win_rate, 0), 4),
    weather_condition_match = ROUND(CASE
        WHEN COALESCE(is_wet_track, 0) = 1 AND horse_wet_track_win_rate > horse_dry_track_win_rate THEN 0.8
        WHEN COALESCE(is_wet_track, 0) = 1 AND horse_wet_track_win_rate <= horse_dry_track_win_rate THEN -0.3
        WHEN COALESCE(is_wet_track, 0) = 0 AND horse_dry_track_win_rate > horse_wet_track_win_rate THEN 0.5
        WHEN COALESCE(is_wet_track, 0) = 0 AND horse_dry_track_win_rate <= horse_wet_track_win_rate THEN -0.1
        ELSE 0
    END, 4);

-- ============================================================================
-- STEP 32: SECTIONAL POSITION COLUMNS (from race_results_sectional_times)
-- ============================================================================
-- These are CURRENT RACE positions (leakage — removed in Phase 3)
UPDATE race_training_dataset_new AS rtd SET
    sectional_position_800m = s.position_800m::INTEGER,
    sectional_position_400m = s.position_400m::INTEGER,
    sectional_position_200m = s.position_200m::INTEGER,
    sectional_400m = EXTRACT(EPOCH FROM s.sectional_400m),
    sectional_800m = EXTRACT(EPOCH FROM s.sectional_800m)
FROM race_results_sectional_times s
JOIN race_results rr ON s.race_id = rr.race_id AND s.horse_number::TEXT = rr.horse_number
WHERE rtd.race_id = s.race_id AND rtd.horse_slug = rr.horse_slug
  AND s.finish_position IS NOT NULL;

-- Position changes (current race — leakage)
UPDATE race_training_dataset_new SET
    position_change_800_400 = CASE WHEN sectional_position_800m IS NOT NULL AND sectional_position_400m IS NOT NULL
        THEN sectional_position_800m - sectional_position_400m ELSE NULL END,
    position_change_400_finish = CASE WHEN sectional_position_400m IS NOT NULL AND final_position IS NOT NULL
        THEN sectional_position_400m - final_position ELSE NULL END;

-- Early speed rating + finish speed rating (current race — leakage)
UPDATE race_training_dataset_new SET
    early_speed_rating = CASE
        WHEN total_runners IS NOT NULL AND total_runners > 1 AND sectional_position_800m IS NOT NULL
        THEN ROUND(1.0 - (sectional_position_800m::NUMERIC - 1) / (total_runners - 1), 4)
        ELSE NULL END,
    finish_speed_rating = CASE
        WHEN total_runners IS NOT NULL AND total_runners > 1 AND final_position IS NOT NULL
        THEN ROUND(1.0 - (final_position::NUMERIC - 1) / (total_runners - 1), 4)
        ELSE NULL END;

-- Historical early speed pct + strong finish pct (anti-leakage: from PRIOR races only)
UPDATE race_training_dataset_new AS rtd SET
    early_speed_pct = ROUND(esp.hist_early_speed_pct, 4),
    strong_finish_pct = ROUND(esp.hist_strong_finish_pct, 4)
FROM (
    SELECT race_id, horse_location_slug,
        -- % of prior races where horse was in top 25% at 800m
        AVG(CASE WHEN position_800m IS NOT NULL AND total_runners > 1
            THEN CASE WHEN position_800m::NUMERIC / total_runners <= 0.25 THEN 1.0 ELSE 0.0 END
            ELSE NULL END) OVER w_prior AS hist_early_speed_pct,
        -- % of prior races where horse gained positions from 400m to finish
        AVG(CASE WHEN position_400m IS NOT NULL AND final_position IS NOT NULL
            THEN CASE WHEN (position_400m - final_position) > 0 THEN 1.0 ELSE 0.0 END
            ELSE NULL END) OVER w_prior AS hist_strong_finish_pct
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) esp
WHERE rtd.race_id = esp.race_id AND rtd.horse_location_slug = esp.horse_location_slug;

-- ============================================================================
-- STEP 33: POS VOLATILITY + HAS_400M/800M + WITH_RUNNING_STYLE
-- ============================================================================
-- Position volatility over last 5 races (anti-leakage)
UPDATE race_training_dataset_new AS rtd SET
    pos_volatility_last5 = ROUND(pv.vol, 2)
FROM (
    WITH lagged AS (
        SELECT race_id, horse_location_slug,
            LAG(final_position, 1) OVER w AS p1,
            LAG(final_position, 2) OVER w AS p2,
            LAG(final_position, 3) OVER w AS p3,
            LAG(final_position, 4) OVER w AS p4,
            LAG(final_position, 5) OVER w AS p5,
            COUNT(*) OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_count
        FROM race_training_dataset_new
        WHERE final_position IS NOT NULL AND final_position < 50 AND horse_location_slug IS NOT NULL
        WINDOW w AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id)
    )
    SELECT race_id, horse_location_slug,
        CASE WHEN prev_count >= 2 THEN
            -- stddev approximation from up to 5 prior positions
            CASE WHEN prev_count >= 5 THEN
                SQRT(((COALESCE(p1,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                     *(COALESCE(p1,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                    + (COALESCE(p2,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                     *(COALESCE(p2,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                    + (COALESCE(p3,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                     *(COALESCE(p3,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                    + (COALESCE(p4,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                     *(COALESCE(p4,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                    + (COALESCE(p5,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                     *(COALESCE(p5,0) - (COALESCE(p1,0)+COALESCE(p2,0)+COALESCE(p3,0)+COALESCE(p4,0)+COALESCE(p5,0))::NUMERIC/5)
                ) / 5.0)
            WHEN prev_count >= 2 THEN ABS(COALESCE(p1, 0) - COALESCE(p2, 0))::NUMERIC / 2.0
            ELSE NULL END
        ELSE NULL END AS vol
    FROM lagged
) pv
WHERE rtd.race_id = pv.race_id AND rtd.horse_location_slug = pv.horse_location_slug;

-- Historical position volatility at 800m (anti-leakage: avg |800m_pos - finish| from PRIOR races)
UPDATE race_training_dataset_new AS rtd SET
    pos_volatility_800 = ROUND(pv8.hist_vol_800, 2)
FROM (
    SELECT race_id, horse_location_slug,
        AVG(CASE WHEN position_800m IS NOT NULL AND final_position IS NOT NULL
            THEN ABS(position_800m - final_position)::NUMERIC ELSE NULL END) OVER w_prior AS hist_vol_800
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) pv8
WHERE rtd.race_id = pv8.race_id AND rtd.horse_location_slug = pv8.horse_location_slug;

-- has_400m / has_800m: whether this row has sectional data
UPDATE race_training_dataset_new SET
    has_400m = (position_400m IS NOT NULL),
    has_800m = (position_800m IS NOT NULL);

-- ============================================================================
-- STEP 35: RAIL POSITION FEATURES (Feb 2026)
-- ============================================================================
-- Rail position significantly modifies barrier advantage:
--   - True rail: inside barriers have maximum advantage
--   - Rail out 9m+: barrier 6 becomes best, inside advantage disappears
-- Data: 85% coverage, 44K+ races with parseable rail info
-- Patterns: "RAIL - True...", "RAIL - Out Xm...", "RAIL - +Xm..."

-- Parse track_info from races table into numeric rail features
UPDATE race_training_dataset_new AS rtd SET
    rail_out_metres = COALESCE(
        TRY_CAST(regexp_extract(r.track_info, '(?:Out|[+])\s*(\d+\.?\d*)', 1) AS DOUBLE),
        CASE WHEN r.track_info ILIKE 'RAIL - True%' THEN 0.0 ELSE NULL END
    ),
    is_rail_true = CASE
        WHEN r.track_info ILIKE 'RAIL - True%' THEN 1.0
        WHEN regexp_extract(r.track_info, '(?:Out|[+])\s*(\d+\.?\d*)', 1) != '' THEN 0.0
        ELSE NULL END
FROM races r
WHERE rtd.race_id = r.race_id
  AND r.track_info IS NOT NULL;

-- Rail × Barrier interaction: captures how rail position modifies barrier advantage
-- High values = outside barrier + rail out (track wider, less inside advantage)
UPDATE race_training_dataset_new SET
    rail_out_x_barrier = rail_out_metres * barrier
WHERE rail_out_metres IS NOT NULL AND barrier IS NOT NULL;

-- Effective barrier: adjusts barrier number for rail position
-- When rail is out 9m, barrier 1 is effectively like barrier 4 on true rail
-- Each 3m of rail movement shifts effective barrier by ~1
UPDATE race_training_dataset_new SET
    effective_barrier = GREATEST(barrier - (rail_out_metres / 3.0), 1.0)
WHERE rail_out_metres IS NOT NULL AND barrier IS NOT NULL;

-- Rail-adjusted barrier advantage: inside advantage shrinks when rail is far out
-- Dynamically widens the "advantageous" barrier threshold based on rail position
UPDATE race_training_dataset_new SET
    barrier_advantage_rail_adjusted = CASE
        WHEN race_distance <= 1200 AND barrier <= (6 + CAST(rail_out_metres / 3.0 AS INT)) THEN 1.0
        ELSE 0.0
    END
WHERE rail_out_metres IS NOT NULL AND barrier IS NOT NULL AND race_distance IS NOT NULL;

-- with_running_style: whether this row has a non-unknown running style (1.0 = yes, 0.0 = no)
UPDATE race_training_dataset_new SET
    with_running_style = CASE
        WHEN running_style IS NOT NULL AND running_style != 'unknown' THEN 1.0
        ELSE 0.0 END;

-- ============================================================================
-- STEP 34: FINISHING KICK CONSISTENCY (Anti-leakage historical average)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    finishing_kick_consistency = ROUND(fk.hist_fk_consistency, 4)
FROM (
    SELECT race_id, horse_location_slug,
        STDDEV(CASE WHEN position_400m IS NOT NULL AND final_position IS NOT NULL
            THEN position_400m - final_position ELSE NULL END) OVER w_prior AS hist_fk_consistency
    FROM race_training_dataset_new
    WHERE horse_location_slug IS NOT NULL
    WINDOW w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) fk
WHERE rtd.race_id = fk.race_id AND rtd.horse_location_slug = fk.horse_location_slug;
