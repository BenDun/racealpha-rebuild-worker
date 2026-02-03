    -- ============================================================================
    -- PHASE 3: ADVANCED FEATURES
    -- ============================================================================
    -- Consolidates: 05_sectional_backfill, 07a_core_features, 07b_class_rating,
    --               08_closing_power, speed figures, value indicators
    -- Est. Time: 15-20 minutes
    -- 
    -- WHAT THIS DOES:
    --   1. Sectional positions (800m, 400m, improvements)
    --   2. Running style classification
    --   3. Closing ability & early speed scores
    --   4. Speed figures
    --   5. Class rating system
    --   6. Field comparison features (percentiles)
    --   7. Odds-based features
    --   8. Value indicators
    -- ============================================================================

    SET statement_timeout = '0';
    SET lock_timeout = '0';
    SET work_mem = '512MB';

    DO $$ BEGIN
        RAISE NOTICE '============================================================';
        RAISE NOTICE 'PHASE 3: ADVANCED FEATURES - Started at %', NOW();
        RAISE NOTICE '============================================================';
    END $$;

    -- ============================================================================
    -- STEP 0: ADD MISSING COLUMNS IF NEEDED
    -- ============================================================================
    DO $$ 
    BEGIN
        -- raw_time_seconds
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'raw_time_seconds') THEN
            ALTER TABLE race_training_dataset ADD COLUMN raw_time_seconds NUMERIC(10,3);
            RAISE NOTICE 'Added missing column: raw_time_seconds';
        END IF;
        
        -- speed_figure
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'speed_figure') THEN
            ALTER TABLE race_training_dataset ADD COLUMN speed_figure NUMERIC(8,2);
            RAISE NOTICE 'Added missing column: speed_figure';
        END IF;
        
        -- avg_speed_figure
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'avg_speed_figure') THEN
            ALTER TABLE race_training_dataset ADD COLUMN avg_speed_figure NUMERIC(8,2);
            RAISE NOTICE 'Added missing column: avg_speed_figure';
        END IF;
        
        -- best_speed_figure
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'best_speed_figure') THEN
            ALTER TABLE race_training_dataset ADD COLUMN best_speed_figure NUMERIC(8,2);
            RAISE NOTICE 'Added missing column: best_speed_figure';
        END IF;
        
        -- class_rating
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'class_rating') THEN
            ALTER TABLE race_training_dataset ADD COLUMN class_rating INTEGER;
            RAISE NOTICE 'Added missing column: class_rating';
        END IF;
        
        -- class_change
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'class_change') THEN
            ALTER TABLE race_training_dataset ADD COLUMN class_change INTEGER;
            RAISE NOTICE 'Added missing column: class_change';
        END IF;
        
        -- class_drop
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'class_drop') THEN
            ALTER TABLE race_training_dataset ADD COLUMN class_drop BOOLEAN;
            RAISE NOTICE 'Added missing column: class_drop';
        END IF;
        
        -- class_rise
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'class_rise') THEN
            ALTER TABLE race_training_dataset ADD COLUMN class_rise BOOLEAN;
            RAISE NOTICE 'Added missing column: class_rise';
        END IF;
        
        -- running_style
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'running_style') THEN
            ALTER TABLE race_training_dataset ADD COLUMN running_style VARCHAR(20);
            RAISE NOTICE 'Added missing column: running_style';
        END IF;
        
        -- total_connection_score
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'total_connection_score') THEN
            ALTER TABLE race_training_dataset ADD COLUMN total_connection_score NUMERIC(8,4);
            RAISE NOTICE 'Added missing column: total_connection_score';
        END IF;
        
        -- jockey_track_win_rate
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'jockey_track_win_rate') THEN
            ALTER TABLE race_training_dataset ADD COLUMN jockey_track_win_rate NUMERIC(8,4);
            RAISE NOTICE 'Added missing column: jockey_track_win_rate';
        END IF;
        
        -- trainer_track_win_rate
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'trainer_track_win_rate') THEN
            ALTER TABLE race_training_dataset ADD COLUMN trainer_track_win_rate NUMERIC(8,4);
            RAISE NOTICE 'Added missing column: trainer_track_win_rate';
        END IF;
        
        -- track_category_win_rate
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'track_category_win_rate') THEN
            ALTER TABLE race_training_dataset ADD COLUMN track_category_win_rate NUMERIC(8,4);
            RAISE NOTICE 'Added missing column: track_category_win_rate';
        END IF;
        
        -- track_place_rate
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name = 'race_training_dataset' 
                       AND column_name = 'track_place_rate') THEN
            ALTER TABLE race_training_dataset ADD COLUMN track_place_rate NUMERIC(8,4);
            RAISE NOTICE 'Added missing column: track_place_rate';
        END IF;
    END $$;

    -- ============================================================================
    -- STEP 1: SECTIONAL POSITIONS FROM RACE_RESULTS
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.1] Pulling sectional positions from race_results...'; END $$;

    UPDATE race_training_dataset rtd
    SET 
        position_800m = rr.position_800m,
        position_400m = rr.position_400m
    FROM race_results rr
    WHERE rtd.race_id = rr.race_id 
    AND rtd.horse_slug = rr.horse_slug
    AND (rr.position_800m IS NOT NULL OR rr.position_400m IS NOT NULL);

    -- Calculate position improvements
    UPDATE race_training_dataset
    SET 
        pos_improvement_800_400 = CASE 
            WHEN position_800m IS NOT NULL AND position_400m IS NOT NULL 
            THEN position_800m - position_400m 
            ELSE NULL 
        END,
        pos_improvement_400_finish = CASE 
            WHEN position_400m IS NOT NULL AND final_position IS NOT NULL 
            THEN position_400m - final_position 
            ELSE NULL 
        END,
        pos_improvement_800_finish = CASE 
            WHEN position_800m IS NOT NULL AND final_position IS NOT NULL 
            THEN position_800m - final_position 
            ELSE NULL 
        END;

    DO $$
    DECLARE
        with_sectionals INTEGER;
    BEGIN
        SELECT COUNT(*) INTO with_sectionals 
        FROM race_training_dataset 
        WHERE position_800m IS NOT NULL OR position_400m IS NOT NULL;
        RAISE NOTICE '[3.1] Sectional data applied to % records', with_sectionals;
    END $$;

    -- ============================================================================
    -- STEP 2: HISTORICAL SECTIONAL AVERAGES (Anti-leakage)
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.2] Calculating historical sectional averages...'; END $$;

    WITH sectional_history AS (
        SELECT 
            race_id,
            horse_location_slug,
            AVG(position_800m) OVER w_prior as avg_800m,
            AVG(position_400m) OVER w_prior as avg_400m,
            AVG(pos_improvement_800_finish) OVER w_prior as avg_improvement
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL
        AND position_800m IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY horse_location_slug 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET 
        avg_early_position_800m = ROUND(sh.avg_800m::numeric, 2),
        avg_mid_position_400m = ROUND(sh.avg_400m::numeric, 2),
        historical_avg_improvement = ROUND(sh.avg_improvement::numeric, 2)
    FROM sectional_history sh
    WHERE rtd.race_id = sh.race_id 
    AND rtd.horse_location_slug = sh.horse_location_slug;

    -- ============================================================================
    -- STEP 3: RUNNING STYLE CLASSIFICATION
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.3] Classifying running styles...'; END $$;

    WITH style_data AS (
        SELECT 
            race_id,
            horse_location_slug,
            total_runners,
            -- Use historical averages for classification
            avg_early_position_800m,
            avg_mid_position_400m,
            -- Percentile position at 800m (normalized by field size)
            CASE 
                WHEN avg_early_position_800m IS NOT NULL AND total_runners > 1
                THEN avg_early_position_800m / total_runners
                ELSE NULL
            END as early_pct
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL
    )
    UPDATE race_training_dataset rtd
    SET running_style = CASE
        -- Based on typical 800m position relative to field
        WHEN sd.early_pct IS NULL THEN 'unknown'
        WHEN sd.early_pct <= 0.20 THEN 'leader'
        WHEN sd.early_pct <= 0.35 THEN 'on_pace'
        WHEN sd.early_pct <= 0.55 THEN 'midfield'
        WHEN sd.early_pct <= 0.75 THEN 'off_pace'
        ELSE 'closer'
    END
    FROM style_data sd
    WHERE rtd.race_id = sd.race_id 
    AND rtd.horse_location_slug = sd.horse_location_slug;

    -- Default unknown for horses without sectional history
    UPDATE race_training_dataset
    SET running_style = 'unknown'
    WHERE running_style IS NULL;

    DO $$
    DECLARE
        style_counts RECORD;
    BEGIN
        FOR style_counts IN 
            SELECT running_style, COUNT(*) as cnt 
            FROM race_training_dataset 
            GROUP BY running_style 
            ORDER BY cnt DESC
        LOOP
            RAISE NOTICE '  Running style %: %', style_counts.running_style, style_counts.cnt;
        END LOOP;
    END $$;

    -- ============================================================================
    -- STEP 4: CLOSING ABILITY & EARLY SPEED SCORES
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.4] Calculating closing ability and early speed scores...'; END $$;

    UPDATE race_training_dataset
    SET 
        -- Closing ability: how much they typically improve from 400m to finish
        closing_ability_score = CASE
            WHEN historical_avg_improvement IS NOT NULL 
            THEN GREATEST(0, LEAST(1, 0.5 + historical_avg_improvement / 10))
            ELSE 0.5
        END,
        
        -- Early speed: inverse of avg 800m position (lower = faster)
        early_speed_score = CASE
            WHEN avg_early_position_800m IS NOT NULL AND total_runners > 1
            THEN GREATEST(0, LEAST(1, 1 - (avg_early_position_800m / total_runners)))
            ELSE 0.5
        END,
        
        -- Sustained run: ability to maintain position from 800m to 400m
        sustained_run_score = CASE
            WHEN avg_early_position_800m IS NOT NULL AND avg_mid_position_400m IS NOT NULL
            THEN GREATEST(0, LEAST(1, 0.5 + (avg_early_position_800m - avg_mid_position_400m) / 10))
            ELSE 0.5
        END;

    -- ============================================================================
    -- STEP 5: SPEED FIGURES (Raw time-based)
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.5] Calculating speed figures...'; END $$;

    -- Pull finish times from race_results_sectional_times
    -- Join on race_id + horse_name (both are text columns)
    -- Convert interval to seconds using EXTRACT(EPOCH FROM ...)
    UPDATE race_training_dataset rtd
    SET raw_time_seconds = EXTRACT(EPOCH FROM rst.finish_time)
    FROM race_results_sectional_times rst
    WHERE rtd.race_id = rst.race_id 
    AND rtd.horse_name = rst.horse_name
    AND rst.finish_time IS NOT NULL;

    -- Calculate speed figure (normalized for distance)
    -- Higher = faster. Based on meters per second relative to expected time.
    WITH time_benchmarks AS (
        SELECT 
            race_distance,
            AVG(raw_time_seconds) as avg_time,
            STDDEV(raw_time_seconds) as std_time
        FROM race_training_dataset
        WHERE raw_time_seconds IS NOT NULL 
        AND raw_time_seconds > 0
        AND final_position <= 3  -- Use top 3 finishers for benchmark
        GROUP BY race_distance
        HAVING COUNT(*) > 10
    )
    UPDATE race_training_dataset rtd
    SET speed_figure = CASE
        WHEN rtd.raw_time_seconds IS NOT NULL 
            AND rtd.raw_time_seconds > 0 
            AND tb.std_time > 0
        THEN ROUND(100 - ((rtd.raw_time_seconds - tb.avg_time) / tb.std_time * 10), 2)
        ELSE NULL
    END
    FROM time_benchmarks tb
    WHERE rtd.race_distance = tb.race_distance;

    -- Historical speed figure averages
    WITH speed_history AS (
        SELECT 
            race_id,
            horse_location_slug,
            AVG(speed_figure) OVER w_prior as avg_speed,
            MAX(speed_figure) OVER w_prior as best_speed
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL
        AND speed_figure IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY horse_location_slug 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET 
        avg_speed_figure = ROUND(sh.avg_speed, 2),
        best_speed_figure = ROUND(sh.best_speed, 2)
    FROM speed_history sh
    WHERE rtd.race_id = sh.race_id 
    AND rtd.horse_location_slug = sh.horse_location_slug;

    -- ============================================================================
    -- STEP 6: CLASS RATING (1-100 scale)
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.6] Calculating class ratings...'; END $$;

    UPDATE race_training_dataset
    SET class_rating = CASE
        -- Group races (highest class)
        WHEN race_class ILIKE '%group 1%' OR race_class ILIKE '%g1%' THEN 100
        WHEN race_class ILIKE '%group 2%' OR race_class ILIKE '%g2%' THEN 90
        WHEN race_class ILIKE '%group 3%' OR race_class ILIKE '%g3%' THEN 80
        WHEN race_class ILIKE '%listed%' THEN 75
        
        -- Benchmark/quality handicaps
        WHEN race_class ILIKE '%benchmark%' OR race_class ILIKE '%bm%' THEN
            CASE 
                WHEN race_class ~* 'bm\s*9[0-9]|benchmark\s*9[0-9]' THEN 70
                WHEN race_class ~* 'bm\s*8[0-9]|benchmark\s*8[0-9]' THEN 60
                WHEN race_class ~* 'bm\s*7[0-9]|benchmark\s*7[0-9]' THEN 50
                WHEN race_class ~* 'bm\s*6[0-9]|benchmark\s*6[0-9]' THEN 40
                WHEN race_class ~* 'bm\s*5[0-9]|benchmark\s*5[0-9]' THEN 35
                ELSE 30
            END
        
        -- Class numbers
        WHEN race_class ~* 'class\s*1|c1' THEN 50
        WHEN race_class ~* 'class\s*2|c2' THEN 45
        WHEN race_class ~* 'class\s*3|c3' THEN 40
        WHEN race_class ~* 'class\s*4|c4' THEN 35
        WHEN race_class ~* 'class\s*5|c5' THEN 30
        WHEN race_class ~* 'class\s*6|c6' THEN 25
        
        -- Maidens (lowest class for experienced horses)
        WHEN race_class ILIKE '%maiden%' OR race_class ILIKE '%mdn%' THEN 20
        
        -- Restricted races
        WHEN race_class ILIKE '%2yo%' OR race_class ILIKE '%2-y-o%' THEN 35
        WHEN race_class ILIKE '%3yo%' OR race_class ILIKE '%3-y-o%' THEN 40
        
        -- Default based on track category
        WHEN track_category = 'Metro' THEN 45
        WHEN track_category = 'Provincial' THEN 35
        ELSE 25
    END;

    -- Calculate class change from previous race
    WITH prev_class AS (
        SELECT 
            race_id,
            horse_location_slug,
            LAG(class_rating) OVER (
                PARTITION BY horse_location_slug 
                ORDER BY race_date, race_id
            ) as prev_class_rating
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL
    )
    UPDATE race_training_dataset rtd
    SET 
        class_change = rtd.class_rating - COALESCE(pc.prev_class_rating, rtd.class_rating),
        class_drop = (rtd.class_rating < COALESCE(pc.prev_class_rating, rtd.class_rating)),
        class_rise = (rtd.class_rating > COALESCE(pc.prev_class_rating, rtd.class_rating))
    FROM prev_class pc
    WHERE rtd.race_id = pc.race_id 
    AND rtd.horse_location_slug = pc.horse_location_slug;

    -- ============================================================================
    -- STEP 7: FIELD COMPARISON FEATURES (Percentiles within race)
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.7] Calculating field comparison features...'; END $$;

    WITH race_percentiles AS (
        SELECT 
            race_id,
            horse_slug,
            -- ELO percentile in field
            PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY COALESCE(horse_elo, 1500)) as elo_pct,
            -- Odds percentile (lower odds = higher rank)
            PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY COALESCE(win_odds, 100) DESC) as odds_pct,
            -- Win percentage vs field average
            CASE 
                WHEN AVG(COALESCE(win_percentage, 0)) OVER (PARTITION BY race_id) > 0 
                THEN COALESCE(win_percentage, 0) / NULLIF(AVG(COALESCE(win_percentage, 0)) OVER (PARTITION BY race_id), 0)
                ELSE 1.0
            END as win_pct_ratio,
            -- Experience vs field average
            CASE 
                WHEN AVG(COALESCE(total_races, 0)) OVER (PARTITION BY race_id) > 0 
                THEN COALESCE(total_races, 0)::numeric / NULLIF(AVG(COALESCE(total_races, 0)) OVER (PARTITION BY race_id), 0)
                ELSE 1.0
            END as exp_ratio,
            -- Odds rank
            RANK() OVER (PARTITION BY race_id ORDER BY COALESCE(win_odds, 999)) as odds_rank
        FROM race_training_dataset
        WHERE horse_slug IS NOT NULL
    )
    UPDATE race_training_dataset rtd
    SET 
        elo_percentile_in_race = ROUND(COALESCE(rp.elo_pct, 0.5)::numeric, 4),
        odds_percentile_in_race = ROUND(COALESCE(rp.odds_pct, 0.5)::numeric, 4),
        win_pct_vs_field_avg = ROUND(LEAST(COALESCE(rp.win_pct_ratio, 1), 5)::numeric, 4),
        experience_vs_field_avg = ROUND(LEAST(COALESCE(rp.exp_ratio, 1), 5)::numeric, 4),
        odds_rank_in_race = COALESCE(rp.odds_rank, 1)
    FROM race_percentiles rp
    WHERE rtd.race_id = rp.race_id 
    AND rtd.horse_slug = rp.horse_slug;

    -- ============================================================================
    -- STEP 8: ODDS-BASED FEATURES
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.8] Calculating odds-based features...'; END $$;

    UPDATE race_training_dataset
    SET 
        -- Favorite flag
        is_favorite = (odds_rank_in_race = 1),
        
        -- Implied probability from odds
        odds_implied_probability = CASE 
            WHEN win_odds IS NOT NULL AND win_odds > 0 
            THEN ROUND(1.0 / win_odds, 4)
            ELSE NULL
        END,
        
        -- Odds value score (performance vs odds expectation)
        odds_value_score = CASE
            WHEN win_odds IS NOT NULL AND win_odds > 0 AND win_percentage > 0
            THEN ROUND((win_percentage / 100) - (1.0 / win_odds), 4)
            ELSE 0
        END,
        
        -- Longshot flag
        is_longshot = (win_odds > 20);

    -- ============================================================================
    -- STEP 9: VALUE INDICATORS
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.9] Calculating value indicators...'; END $$;

    UPDATE race_training_dataset
    SET 
        -- Value indicator: model win% vs market implied
        value_indicator = ROUND(
            COALESCE(win_percentage, 0) / 100 - COALESCE(odds_implied_probability, 0.1), 
            4
        ),
        
        -- Market undervalued: our model thinks better than market
        market_undervalued = (
            COALESCE(win_percentage, 0) / 100 > COALESCE(odds_implied_probability, 0) * 1.2
        ),
        
        -- ELO vs odds gap (higher = more value)
        elo_vs_odds_gap = ROUND(
            COALESCE(elo_percentile_in_race, 0.5) - COALESCE(odds_percentile_in_race, 0.5),
            4
        ),
        
        -- Form vs odds gap
        form_vs_odds_gap = ROUND(
            COALESCE(form_momentum, 0.5) - COALESCE(odds_percentile_in_race, 0.5),
            4
        ),
        
        -- Improving longshot: strong form, poor odds
        improving_longshot = (
            COALESCE(form_momentum, 0) > 0.6 
            AND COALESCE(win_odds, 0) > 10
        ),
        
        -- Class drop longshot
        class_drop_longshot = (
            COALESCE(class_drop, FALSE) = TRUE 
            AND COALESCE(win_odds, 0) > 10
        ),
        
        -- Connection longshot: good connections, long odds
        connection_longshot = (
            (COALESCE(jockey_win_rate, 0) > 0.15 OR COALESCE(trainer_win_rate, 0) > 0.15)
            AND COALESCE(win_odds, 0) > 10
        );

    -- ============================================================================
    -- STEP 10: DISTANCE SUITED SCORE & EXTENDED DISTANCE FEATURES
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.10] Calculating distance features...'; END $$;

    UPDATE race_training_dataset
    SET 
        distance_suited_score = CASE
            WHEN races_at_distance > 0 AND distance_win_rate > 0 THEN
                LEAST(1.0, GREATEST(0, 0.5 + distance_win_rate))
            WHEN distance_range = 'sprint' AND total_races > 3 THEN 0.5
            WHEN distance_range = 'middle' AND total_races > 3 THEN 0.5
            ELSE 0.5
        END,
        -- Distance experience level
        distance_experience_level = CASE
            WHEN races_at_distance >= 10 THEN 3
            WHEN races_at_distance >= 5 THEN 2
            WHEN races_at_distance >= 1 THEN 1
            ELSE 0
        END,
        -- Track experience level
        track_experience_level = CASE
            WHEN races_at_track >= 10 THEN 3
            WHEN races_at_track >= 5 THEN 2
            WHEN races_at_track >= 1 THEN 1
            ELSE 0
        END;

    -- Distance range stats
    WITH dist_range_stats AS (
        SELECT 
            race_id,
            horse_location_slug,
            distance_range,
            COUNT(*) OVER w_dist as at_range,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_dist as wins_at_range
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL AND distance_range IS NOT NULL
        WINDOW w_dist AS (
            PARTITION BY horse_location_slug, distance_range 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET 
        distance_range_experience = COALESCE(drs.at_range, 0),
        distance_range_win_rate = CASE 
            WHEN COALESCE(drs.at_range, 0) > 0 
            THEN ROUND(drs.wins_at_range::numeric / drs.at_range, 4) 
            ELSE 0 
        END
    FROM dist_range_stats drs
    WHERE rtd.race_id = drs.race_id AND rtd.horse_location_slug = drs.horse_location_slug;

    -- ============================================================================
    -- STEP 11: RUNNING STYLE EXTENDED FEATURES
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.11] Calculating extended running style features...'; END $$;

    UPDATE race_training_dataset
    SET 
        -- Early speed score
        early_speed_score = CASE
            WHEN avg_early_position_800m IS NOT NULL AND total_runners > 1
            THEN ROUND(GREATEST(0, LEAST(1, 1 - (avg_early_position_800m / total_runners)))::numeric, 4)
            ELSE 0.5
        END,
        
        -- Closing ability score
        closing_ability_score = CASE
            WHEN historical_avg_improvement IS NOT NULL 
            THEN ROUND(GREATEST(0, LEAST(1, 0.5 + historical_avg_improvement / 10))::numeric, 4)
            ELSE 0.5
        END,
        
        -- Sustained run score
        sustained_run_score = CASE
            WHEN avg_early_position_800m IS NOT NULL AND avg_mid_position_400m IS NOT NULL
            THEN ROUND(GREATEST(0, LEAST(1, 0.5 + (avg_early_position_800m - avg_mid_position_400m) / 10))::numeric, 4)
            ELSE 0.5
        END,
        
        -- Historical late improvement
        historical_late_improvement = COALESCE(historical_avg_improvement, 0),
        
        -- Best late improvement
        best_late_improvement = COALESCE(pos_improvement_800_finish, 0),
        
        -- Avg late improvement
        avg_late_improvement = COALESCE(historical_avg_improvement, 0),
        
        -- Speed map position
        speed_map_position = CASE
            WHEN running_style = 'leader' THEN 'leading'
            WHEN running_style = 'on_pace' THEN 'prominent'
            WHEN running_style = 'midfield' THEN 'midfield'
            WHEN running_style IN ('off_pace', 'closer') THEN 'back'
            ELSE 'unknown'
        END,
        
        -- Speed rating
        speed_rating = COALESCE(speed_figure, 80);

    -- ============================================================================
    -- STEP 12: FORM FLAGS & RUNS FEATURES
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.12] Calculating form flags and run counts...'; END $$;

    UPDATE race_training_dataset
    SET 
        -- Has recent form
        has_recent_form = (days_since_last_race IS NOT NULL AND days_since_last_race <= 30),
        
        -- Has winning form
        has_winning_form = (last_5_win_rate > 0);

    -- Runs in last 60/90 days (optimized with window functions instead of correlated subqueries)
    WITH ranked_races AS (
        SELECT 
            race_id,
            horse_location_slug,
            race_date,
            ROW_NUMBER() OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id) as race_num
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL
    ),
    run_counts AS (
        SELECT 
            r1.race_id,
            r1.horse_location_slug,
            COUNT(r2.race_id) FILTER (WHERE r2.race_date >= r1.race_date - 60 AND r2.race_date < r1.race_date) as runs_60,
            COUNT(r2.race_id) FILTER (WHERE r2.race_date >= r1.race_date - 90 AND r2.race_date < r1.race_date) as runs_90
        FROM ranked_races r1
        LEFT JOIN ranked_races r2 ON r1.horse_location_slug = r2.horse_location_slug 
            AND r2.race_num < r1.race_num
        GROUP BY r1.race_id, r1.horse_location_slug
    )
    UPDATE race_training_dataset rtd
    SET 
        runs_last_60_days = rc.runs_60,
        runs_last_90_days = rc.runs_90,
        avg_days_between_runs = CASE 
            WHEN rc.runs_90 > 1 THEN ROUND(90.0 / rc.runs_90, 1)
            ELSE NULL 
        END
    FROM run_counts rc
    WHERE rtd.race_id = rc.race_id AND rtd.horse_location_slug = rc.horse_location_slug;

    -- ============================================================================
    -- STEP 13: WEIGHT FEATURES
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.13] Calculating weight features...'; END $$;

    WITH weight_history AS (
        SELECT 
            race_id,
            horse_location_slug,
            AVG(weight) OVER w_prior as avg_weight
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL AND weight IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY horse_location_slug 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET 
        weight_carried_avg = ROUND(wh.avg_weight, 1),
        weight_vs_avg = CASE 
            WHEN wh.avg_weight > 0 
            THEN ROUND((rtd.weight - wh.avg_weight)::numeric, 2)
            ELSE 0 
        END
    FROM weight_history wh
    WHERE rtd.race_id = wh.race_id AND rtd.horse_location_slug = wh.horse_location_slug;

    -- ============================================================================
    -- STEP 14: CONNECTION FEATURES
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.14] Calculating connection features...'; END $$;

    -- Jockey-Horse connections
    WITH jh_connections AS (
        SELECT 
            race_id,
            horse_location_slug,
            jockey_slug,
            COUNT(*) OVER w_prior as races_together,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as wins_together
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL AND jockey_slug IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY horse_location_slug, jockey_slug 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET 
        jockey_horse_connection_races = COALESCE(jh.races_together, 0),
        jockey_horse_connection_wins = COALESCE(jh.wins_together, 0),
        jockey_horse_connection_win_rate = CASE 
            WHEN COALESCE(jh.races_together, 0) > 0 
            THEN ROUND(jh.wins_together::numeric / jh.races_together, 4)
            ELSE 0 
        END,
        strong_jockey_connection = (COALESCE(jh.races_together, 0) >= 3 AND 
            CASE WHEN jh.races_together > 0 THEN jh.wins_together::numeric / jh.races_together ELSE 0 END > 0.2)
    FROM jh_connections jh
    WHERE rtd.race_id = jh.race_id AND rtd.horse_location_slug = jh.horse_location_slug;

    -- Jockey-Trainer connections
    WITH jt_connections AS (
        SELECT 
            race_id,
            jockey_slug,
            trainer_slug,
            COUNT(*) OVER w_prior as races_together,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as wins_together
        FROM race_training_dataset
        WHERE jockey_slug IS NOT NULL AND trainer_slug IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY jockey_slug, trainer_slug 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET 
        jockey_trainer_connection_races = COALESCE(jt.races_together, 0),
        jockey_trainer_connection_wins = COALESCE(jt.wins_together, 0),
        jockey_trainer_connection_win_rate = CASE 
            WHEN COALESCE(jt.races_together, 0) > 0 
            THEN ROUND(jt.wins_together::numeric / jt.races_together, 4)
            ELSE 0 
        END,
        strong_trainer_connection = (COALESCE(jt.races_together, 0) >= 5 AND 
            CASE WHEN jt.races_together > 0 THEN jt.wins_together::numeric / jt.races_together ELSE 0 END > 0.15)
    FROM jt_connections jt
    WHERE rtd.race_id = jt.race_id AND rtd.jockey_slug = jt.jockey_slug AND rtd.trainer_slug = jt.trainer_slug;

    -- Total connection score
    UPDATE race_training_dataset
    SET total_connection_score = ROUND(
        (COALESCE(jockey_horse_connection_win_rate, 0) * 0.5 +
        COALESCE(jockey_trainer_connection_win_rate, 0) * 0.3 +
        CASE WHEN strong_jockey_connection THEN 0.1 ELSE 0 END +
        CASE WHEN strong_trainer_connection THEN 0.1 ELSE 0 END
        )::numeric, 4
    );

    -- ============================================================================
    -- STEP 15: TRACK-SPECIFIC WIN RATES (Jockey/Trainer at track)
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.15] Calculating jockey/trainer track win rates...'; END $$;

    -- Jockey at track
    WITH jockey_track AS (
        SELECT 
            race_id,
            jockey_slug,
            track_name,
            COUNT(*) OVER w_prior as rides,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as wins
        FROM race_training_dataset
        WHERE jockey_slug IS NOT NULL AND track_name IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY jockey_slug, track_name 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET jockey_track_win_rate = CASE 
        WHEN COALESCE(jt.rides, 0) > 0 
        THEN ROUND(jt.wins::numeric / jt.rides, 4)
        ELSE 0 
    END
    FROM jockey_track jt
    WHERE rtd.race_id = jt.race_id AND rtd.jockey_slug = jt.jockey_slug AND rtd.track_name = jt.track_name;

    -- Trainer at track
    WITH trainer_track AS (
        SELECT 
            race_id,
            trainer_slug,
            track_name,
            COUNT(*) OVER w_prior as runners,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as wins
        FROM race_training_dataset
        WHERE trainer_slug IS NOT NULL AND track_name IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY trainer_slug, track_name 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET trainer_track_win_rate = CASE 
        WHEN COALESCE(tt.runners, 0) > 0 
        THEN ROUND(tt.wins::numeric / tt.runners, 4)
        ELSE 0 
    END
    FROM trainer_track tt
    WHERE rtd.race_id = tt.race_id AND rtd.trainer_slug = tt.trainer_slug AND rtd.track_name = tt.track_name;

    -- Track category win rate
    WITH track_cat_stats AS (
        SELECT 
            race_id,
            horse_location_slug,
            track_category,
            COUNT(*) OVER w_prior as at_cat,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior as wins_at_cat
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL AND track_category IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY horse_location_slug, track_category 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET track_category_win_rate = CASE 
        WHEN COALESCE(tcs.at_cat, 0) > 0 
        THEN ROUND(tcs.wins_at_cat::numeric / tcs.at_cat, 4)
        ELSE 0 
    END
    FROM track_cat_stats tcs
    WHERE rtd.race_id = tcs.race_id AND rtd.horse_location_slug = tcs.horse_location_slug;

    -- Track place rate
    WITH track_place_stats AS (
        SELECT 
            race_id,
            horse_location_slug,
            track_name,
            COUNT(*) OVER w_prior as at_track,
            SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w_prior as places_at_track
        FROM race_training_dataset
        WHERE horse_location_slug IS NOT NULL AND track_name IS NOT NULL
        WINDOW w_prior AS (
            PARTITION BY horse_location_slug, track_name 
            ORDER BY race_date, race_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
    UPDATE race_training_dataset rtd
    SET track_place_rate = CASE 
        WHEN COALESCE(tps.at_track, 0) > 0 
        THEN ROUND(tps.places_at_track::numeric / tps.at_track, 4)
        ELSE 0 
    END
    FROM track_place_stats tps
    WHERE rtd.race_id = tps.race_id AND rtd.horse_location_slug = tps.horse_location_slug;

    -- ============================================================================
    -- STEP 16: MISC FEATURES
    -- ============================================================================
    DO $$ BEGIN RAISE NOTICE '[3.16] Calculating misc features...'; END $$;

    UPDATE race_training_dataset
    SET 
        -- Career ROI estimate
        career_roi_estimate = CASE 
            WHEN total_races > 0 AND win_percentage > 0 
            THEN ROUND((win_percentage / 100 * 10 - 1)::numeric, 4)  -- Simplified ROI
            ELSE -1 
        END,
        
        -- Consistency score (based on place rate)
        consistency_score = ROUND(COALESCE(place_percentage, 0) / 100, 4),
        
        -- Race experience level
        race_experience_level = CASE
            WHEN total_races >= 30 THEN 4
            WHEN total_races >= 15 THEN 3
            WHEN total_races >= 5 THEN 2
            WHEN total_races >= 1 THEN 1
            ELSE 0
        END,
        
        -- Odds value score
        odds_value_score = CASE
            WHEN win_odds > 0 AND win_percentage > 0
            THEN ROUND((win_percentage / 100) - (1.0 / win_odds), 4)
            ELSE 0
        END,
        
        -- Against grain score
        against_grain_score = CASE
            WHEN elo_percentile_in_race > 0.7 AND odds_rank_in_race > 3 THEN 0.8
            WHEN elo_percentile_in_race > 0.5 AND odds_rank_in_race > 5 THEN 0.5
            ELSE 0.2
        END,
        
        -- Longshot value profile
        longshot_value_profile = CASE
            WHEN win_odds > 15 AND form_momentum > 0.5 THEN 0.8
            WHEN win_odds > 10 AND form_momentum > 0.4 THEN 0.5
            ELSE 0.2
        END,
        
        -- Historical value winner
        historical_value_winner = (
            COALESCE(win_percentage, 0) > 20 AND 
            COALESCE(odds_implied_probability, 1) < 0.15
        ),
        
        -- Peak performance indicator
        peak_performance_indicator = ROUND(
            COALESCE(form_momentum, 0) * 0.4 +
            COALESCE(elo_percentile_in_race, 0.5) * 0.3 +
            COALESCE(win_pct_vs_field_avg, 1) / 5 * 0.3,
            4
        );

    -- ============================================================================
    -- COMPLETION
    -- ============================================================================
    DO $$
    DECLARE
        with_running_style INTEGER;
        with_speed_fig INTEGER;
        with_class_rating INTEGER;
    BEGIN
        SELECT COUNT(*) INTO with_running_style FROM race_training_dataset WHERE running_style != 'unknown';
        SELECT COUNT(*) INTO with_speed_fig FROM race_training_dataset WHERE speed_figure IS NOT NULL;
        SELECT COUNT(*) INTO with_class_rating FROM race_training_dataset WHERE class_rating IS NOT NULL;
        
        RAISE NOTICE '============================================================';
        RAISE NOTICE 'PHASE 3 COMPLETE';
        RAISE NOTICE '============================================================';
        RAISE NOTICE 'Records with running style: %', with_running_style;
        RAISE NOTICE 'Records with speed figure: %', with_speed_fig;
        RAISE NOTICE 'Records with class rating: %', with_class_rating;
        RAISE NOTICE '';
        RAISE NOTICE 'Next: Run 04_interactions_validation.sql';
    END $$;
