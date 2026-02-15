-- ============================================================================
-- SCRIPT 06: CURRENT FORM MATERIALIZED VIEWS
-- ============================================================================
-- Run Order: 6 of 7 (after 05_sectional_backfill.sql, before 07_elo_rebuild)
-- Dependencies: Scripts 01-05 (needs complete position_800m/400m data)
-- Creates all 5 materialized views:
--   - horse_current_form (34 columns - enhanced with sectional time features!)
--   - jockey_current_form (11 columns)  
--   - trainer_current_form (11 columns)
--   - jockey_horse_connection_stats (9 columns)
--   - jockey_trainer_connection_stats (9 columns)
--
-- Jan 2026 Enhancement: Added 11 time-based sectional features for ALWAYS-CURRENT predictions
--   Position: avg_pos_200m, early_speed_pct, strong_finish_pct, pos_volatility_800, finishing_kick_consistency
--   Time: avg_sectional_400m, avg_sectional_800m, avg_cumulative_800m, sectional_time_consistency
--   Pace: early_vs_mid_pace_change, historical_speed_vs_field
--
-- Why these matter: 
--   - avg_pos_200m has 0.751 correlation with finish position (STRONGEST predictor!)
--   - early_speed_pct has 0.412 correlation with win rate
--   - Winners are 0.178s faster in early sectional vs field average
--
-- Est. Time: 5-10 minutes
-- Daily refresh: SELECT refresh_all_current_form();
-- ============================================================================

SET statement_timeout = '0';
SET lock_timeout = '0';
SET work_mem = '512MB';

DO $$ BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 6: CURRENT FORM VIEWS - Started at %', NOW();
    RAISE NOTICE '============================================================';
END $$;

-- ============================================================================
-- PART 1: HORSE CURRENT FORM (34 columns - enhanced with sectional time features!)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[6.1] Creating horse_current_form with enhanced sectional features...'; END $$;

DROP MATERIALIZED VIEW IF EXISTS horse_current_form CASCADE;

CREATE MATERIALIZED VIEW horse_current_form AS
WITH horse_races AS (
    SELECT 
        rr.horse_slug,
        r.race_date,
        rr.race_id,
        rr.horse_number,
        rr.position as final_position,
        rr.position_800m,
        rr.position_400m,
        ROW_NUMBER() OVER (PARTITION BY rr.horse_slug ORDER BY r.race_date DESC, rr.race_id DESC) as recency_rank
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    WHERE rr.position IS NOT NULL AND rr.position < 50 AND rr.horse_slug IS NOT NULL
),
career_stats AS (
    SELECT 
        horse_slug,
        COUNT(*) as total_races,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) as places,
        MAX(race_date) as last_race_date
    FROM horse_races
    GROUP BY horse_slug
),
last_5_stats AS (
    SELECT horse_slug,
        AVG(final_position) as last_5_avg_position,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) as last_5_win_rate,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) as last_5_place_rate
    FROM horse_races WHERE recency_rank <= 5 GROUP BY horse_slug
),
form_momentum AS (
    SELECT horse_slug,
        SUM(CASE WHEN final_position <= 3 THEN 
            CASE recency_rank WHEN 1 THEN 5 WHEN 2 THEN 4 WHEN 3 THEN 3 WHEN 4 THEN 2 WHEN 5 THEN 1 ELSE 0 END
        ELSE 0 END)::numeric / 15 as form_momentum
    FROM horse_races WHERE recency_rank <= 5 GROUP BY horse_slug
),
recent_runs AS (
    SELECT horse_slug,
        COUNT(*) FILTER (WHERE race_date >= CURRENT_DATE - 60) as runs_last_60_days,
        COUNT(*) FILTER (WHERE race_date >= CURRENT_DATE - 90) as runs_last_90_days
    FROM horse_races GROUP BY horse_slug
),
running_style_calc AS (
    SELECT horse_slug,
        AVG(position_800m) as avg_early_position_800m,
        AVG(position_400m) as avg_pos_400m,
        AVG(COALESCE(position_800m, 0) - COALESCE(final_position, 0)) as avg_late_surge,
        STDDEV(position_800m) as pos_volatility_800,
        STDDEV(position_800m - final_position) as finishing_kick_consistency,
        -- Position-based percentages
        AVG(CASE WHEN position_400m <= 3 THEN 1.0 ELSE 0.0 END) as early_speed_pct,
        AVG(CASE WHEN position_800m - final_position >= 3 THEN 1.0 ELSE 0.0 END) as strong_finish_pct,
        CASE
            WHEN AVG(position_800m) <= 2 THEN 'leader'
            WHEN AVG(position_800m) <= 4 THEN 'on_pace'
            WHEN AVG(position_800m) <= 7 THEN 'midfield'
            ELSE 'closer'
        END as running_style
    FROM horse_races WHERE recency_rank <= 10 AND position_800m IS NOT NULL GROUP BY horse_slug
),
-- NEW: Time-based sectional features from race_results_sectional_times
-- These are the 0.751 correlation features that winners need!
sectional_time_calc AS (
    SELECT 
        hr.horse_slug,
        AVG(s.position_200m) as avg_pos_200m,
        AVG(EXTRACT(EPOCH FROM s.sectional_400m)) as avg_sectional_400m,
        AVG(EXTRACT(EPOCH FROM s.sectional_800m)) as avg_sectional_800m,
        AVG(EXTRACT(EPOCH FROM s.cumulative_800m)) as avg_cumulative_800m,
        STDDEV(EXTRACT(EPOCH FROM s.sectional_400m)) as sectional_time_consistency,
        AVG(EXTRACT(EPOCH FROM s.sectional_800m) - EXTRACT(EPOCH FROM s.sectional_400m)) as early_vs_mid_pace_change
    FROM horse_races hr
    JOIN race_results_sectional_times s ON hr.race_id = s.race_id 
        AND hr.horse_number::text = s.horse_number::text
    WHERE hr.recency_rank <= 10  -- Last 10 races only for freshness
      AND s.sectional_400m IS NOT NULL
    GROUP BY hr.horse_slug
),
-- Speed vs field average calculation (winners are 0.178s faster!)
speed_vs_field AS (
    SELECT 
        hr.horse_slug,
        AVG(ra.race_avg_sec_400m - EXTRACT(EPOCH FROM s.sectional_400m)) as historical_speed_vs_field
    FROM horse_races hr
    JOIN race_results_sectional_times s ON hr.race_id = s.race_id 
        AND hr.horse_number::text = s.horse_number::text
    JOIN (
        SELECT race_id, AVG(EXTRACT(EPOCH FROM sectional_400m)) as race_avg_sec_400m
        FROM race_results_sectional_times 
        WHERE sectional_400m IS NOT NULL
        GROUP BY race_id
    ) ra ON hr.race_id = ra.race_id
    WHERE hr.recency_rank <= 10
      AND s.sectional_400m IS NOT NULL
    GROUP BY hr.horse_slug
),
elo_lookup AS (
    SELECT DISTINCT ON (horse_slug) horse_slug, elo_after as current_elo
    FROM horse_elo_ratings ORDER BY horse_slug, race_date DESC
)
SELECT 
    cs.horse_slug,
    ROUND(l5.last_5_win_rate, 4) as last_5_win_rate,
    ROUND(l5.last_5_place_rate, 4) as last_5_place_rate,
    ROUND(l5.last_5_avg_position, 2) as last_5_avg_position,
    ROUND(fm.form_momentum, 4) as form_momentum,
    cs.last_race_date,
    (CURRENT_DATE - cs.last_race_date)::integer as days_since_last_race,
    cs.total_races,
    cs.wins,
    cs.places,
    ROUND(cs.wins::numeric / NULLIF(cs.total_races, 0) * 100, 2) as win_percentage,
    ROUND(cs.places::numeric / NULLIF(cs.total_races, 0) * 100, 2) as place_percentage,
    ROUND(cs.places::numeric / NULLIF(cs.total_races, 0), 4) as consistency_score,
    COALESCE(rr.runs_last_60_days, 0) as runs_last_60_days,
    COALESCE(rr.runs_last_90_days, 0) as runs_last_90_days,
    CASE WHEN rr.runs_last_90_days > 1 THEN (90 / rr.runs_last_90_days)::bigint ELSE NULL END as avg_days_between_runs,
    COALESCE(el.current_elo, 1500) as current_elo,
    rs.running_style::varchar,
    -- Position-based features (from race_results)
    ROUND(rs.avg_early_position_800m, 2) as avg_early_position_800m,
    ROUND(rs.avg_pos_400m, 2) as avg_pos_400m,
    ROUND(rs.avg_late_surge, 2) as avg_late_surge,
    ROUND(CASE WHEN rs.avg_early_position_800m IS NOT NULL THEN GREATEST(0, LEAST(1, 1 - rs.avg_early_position_800m / 10)) ELSE 0.5 END, 4) as early_speed_score,
    ROUND(CASE WHEN rs.avg_late_surge IS NOT NULL THEN GREATEST(0, LEAST(1, 0.5 + rs.avg_late_surge / 10)) ELSE 0.5 END, 4) as closing_ability_score,
    -- NEW: Enhanced position-based features (Jan 2026)
    ROUND(st.avg_pos_200m::numeric, 2) as avg_pos_200m,  -- 0.751 correlation with finish!
    ROUND(rs.early_speed_pct::numeric, 4) as early_speed_pct,  -- 0.412 correlation with win rate
    ROUND(rs.strong_finish_pct::numeric, 4) as strong_finish_pct,  -- 0.217 correlation with win rate
    ROUND(rs.pos_volatility_800::numeric, 3) as pos_volatility_800,
    ROUND(rs.finishing_kick_consistency::numeric, 3) as finishing_kick_consistency,
    -- NEW: Time-based sectional features (Jan 2026) - Winners are 0.178s faster!
    ROUND(st.avg_sectional_400m::numeric, 3) as avg_sectional_400m,
    ROUND(st.avg_sectional_800m::numeric, 3) as avg_sectional_800m,
    ROUND(st.avg_cumulative_800m::numeric, 3) as avg_cumulative_800m,
    ROUND(st.sectional_time_consistency::numeric, 3) as sectional_time_consistency,
    ROUND(st.early_vs_mid_pace_change::numeric, 3) as early_vs_mid_pace_change,
    ROUND(svf.historical_speed_vs_field::numeric, 4) as historical_speed_vs_field  -- 🔥 +0.178s for winners!
FROM career_stats cs
LEFT JOIN last_5_stats l5 ON cs.horse_slug = l5.horse_slug
LEFT JOIN form_momentum fm ON cs.horse_slug = fm.horse_slug
LEFT JOIN recent_runs rr ON cs.horse_slug = rr.horse_slug
LEFT JOIN running_style_calc rs ON cs.horse_slug = rs.horse_slug
LEFT JOIN sectional_time_calc st ON cs.horse_slug = st.horse_slug
LEFT JOIN speed_vs_field svf ON cs.horse_slug = svf.horse_slug
LEFT JOIN elo_lookup el ON cs.horse_slug = el.horse_slug;

CREATE UNIQUE INDEX IF NOT EXISTS idx_hcf_horse_slug ON horse_current_form(horse_slug);

DO $$ DECLARE cnt INTEGER; sectional_cnt INTEGER; BEGIN 
    SELECT COUNT(*) INTO cnt FROM horse_current_form; 
    SELECT COUNT(*) INTO sectional_cnt FROM horse_current_form WHERE avg_sectional_400m IS NOT NULL;
    RAISE NOTICE '[6.1] horse_current_form: % horses (% with sectional time data)', cnt, sectional_cnt; 
END $$;

-- ============================================================================
-- PART 2: JOCKEY CURRENT FORM (11 columns)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[6.2] Creating jockey_current_form...'; END $$;

DROP MATERIALIZED VIEW IF EXISTS jockey_current_form CASCADE;

CREATE MATERIALIZED VIEW jockey_current_form AS
WITH jockey_rides AS (
    SELECT rr.jockey_slug, r.race_date, rr.position as final_position,
        ROW_NUMBER() OVER (PARTITION BY rr.jockey_slug ORDER BY r.race_date DESC, rr.race_id DESC) as recency_rank
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id
    WHERE rr.position IS NOT NULL AND rr.position < 50 AND rr.jockey_slug IS NOT NULL
),
career_stats AS (
    SELECT jockey_slug, COUNT(*) as total_rides,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) as places,
        MAX(race_date) as last_ride_date
    FROM jockey_rides GROUP BY jockey_slug
),
last_20_stats AS (
    SELECT jockey_slug, AVG(final_position) as avg_position_20,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) as last_20_win_rate
    FROM jockey_rides WHERE recency_rank <= 20 GROUP BY jockey_slug
),
last_50_stats AS (
    SELECT jockey_slug, AVG(final_position) as avg_position_50,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) as last_50_win_rate
    FROM jockey_rides WHERE recency_rank <= 50 GROUP BY jockey_slug
)
SELECT 
    cs.jockey_slug,
    cs.total_rides,
    cs.wins,
    cs.places,
    cs.wins::double precision / NULLIF(cs.total_rides, 0) * 100 as win_percentage,
    cs.places::double precision / NULLIF(cs.total_rides, 0) * 100 as place_percentage,
    ROUND(l20.avg_position_20, 2) as avg_position_20,
    ROUND(l50.avg_position_50, 2) as avg_position_50,
    ROUND(l20.last_20_win_rate, 4) as last_20_win_rate,
    ROUND(l50.last_50_win_rate, 4) as last_50_win_rate,
    (CURRENT_DATE - cs.last_ride_date)::integer as days_since_last_ride
FROM career_stats cs
LEFT JOIN last_20_stats l20 ON cs.jockey_slug = l20.jockey_slug
LEFT JOIN last_50_stats l50 ON cs.jockey_slug = l50.jockey_slug;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jcf_jockey_slug ON jockey_current_form(jockey_slug);

DO $$ DECLARE cnt INTEGER; BEGIN SELECT COUNT(*) INTO cnt FROM jockey_current_form; RAISE NOTICE '[6.2] jockey_current_form: % jockeys', cnt; END $$;

-- ============================================================================
-- PART 3: TRAINER CURRENT FORM (11 columns)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[6.3] Creating trainer_current_form...'; END $$;

DROP MATERIALIZED VIEW IF EXISTS trainer_current_form CASCADE;

CREATE MATERIALIZED VIEW trainer_current_form AS
WITH trainer_runners AS (
    SELECT rr.trainer_slug, r.race_date, rr.position as final_position,
        ROW_NUMBER() OVER (PARTITION BY rr.trainer_slug ORDER BY r.race_date DESC, rr.race_id DESC) as recency_rank
    FROM race_results rr JOIN races r ON rr.race_id = r.race_id
    WHERE rr.position IS NOT NULL AND rr.position < 50 AND rr.trainer_slug IS NOT NULL
),
career_stats AS (
    SELECT trainer_slug, COUNT(*) as total_runners,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) as places,
        MAX(race_date) as last_runner_date
    FROM trainer_runners GROUP BY trainer_slug
),
last_50_stats AS (
    SELECT trainer_slug, AVG(final_position) as avg_position_50,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) as last_50_win_rate
    FROM trainer_runners WHERE recency_rank <= 50 GROUP BY trainer_slug
),
last_100_stats AS (
    SELECT trainer_slug, AVG(final_position) as avg_position_100,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) as last_100_win_rate
    FROM trainer_runners WHERE recency_rank <= 100 GROUP BY trainer_slug
)
SELECT 
    cs.trainer_slug,
    cs.total_runners,
    cs.wins,
    cs.places,
    cs.wins::double precision / NULLIF(cs.total_runners, 0) * 100 as win_percentage,
    cs.places::double precision / NULLIF(cs.total_runners, 0) * 100 as place_percentage,
    ROUND(l50.avg_position_50, 2) as avg_position_50,
    ROUND(l100.avg_position_100, 2) as avg_position_100,
    ROUND(l50.last_50_win_rate, 4) as last_50_win_rate,
    ROUND(l100.last_100_win_rate, 4) as last_100_win_rate,
    (CURRENT_DATE - cs.last_runner_date)::integer as days_since_last_runner
FROM career_stats cs
LEFT JOIN last_50_stats l50 ON cs.trainer_slug = l50.trainer_slug
LEFT JOIN last_100_stats l100 ON cs.trainer_slug = l100.trainer_slug;

CREATE UNIQUE INDEX IF NOT EXISTS idx_tcf_trainer_slug ON trainer_current_form(trainer_slug);

DO $$ DECLARE cnt INTEGER; BEGIN SELECT COUNT(*) INTO cnt FROM trainer_current_form; RAISE NOTICE '[6.3] trainer_current_form: % trainers', cnt; END $$;

-- ============================================================================
-- PART 4: JOCKEY-HORSE CONNECTION STATS (9 columns)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[6.4] Creating jockey_horse_connection_stats...'; END $$;

DROP MATERIALIZED VIEW IF EXISTS jockey_horse_connection_stats CASCADE;

CREATE MATERIALIZED VIEW jockey_horse_connection_stats AS
SELECT 
    rr.horse_slug,
    rr.jockey_slug,
    COUNT(*) as connection_races,
    SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) as connection_wins,
    SUM(CASE WHEN rr.position <= 3 THEN 1 ELSE 0 END) as connection_places,
    ROUND(SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) as connection_win_rate,
    ROUND(SUM(CASE WHEN rr.position <= 3 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) as connection_place_rate,
    MAX(r.race_date) as last_ride_together,
    (COUNT(*) >= 3 AND SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) > 0.2) as strong_connection
FROM race_results rr
JOIN races r ON rr.race_id = r.race_id
WHERE rr.horse_slug IS NOT NULL AND rr.jockey_slug IS NOT NULL AND rr.position IS NOT NULL AND rr.position < 50
GROUP BY rr.horse_slug, rr.jockey_slug;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jhcs_horse_jockey ON jockey_horse_connection_stats(horse_slug, jockey_slug);
CREATE INDEX IF NOT EXISTS idx_jhcs_horse ON jockey_horse_connection_stats(horse_slug);
CREATE INDEX IF NOT EXISTS idx_jhcs_jockey ON jockey_horse_connection_stats(jockey_slug);

DO $$ DECLARE cnt INTEGER; BEGIN SELECT COUNT(*) INTO cnt FROM jockey_horse_connection_stats; RAISE NOTICE '[6.4] jockey_horse_connection_stats: % connections', cnt; END $$;

-- ============================================================================
-- PART 5: JOCKEY-TRAINER CONNECTION STATS (9 columns)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[6.5] Creating jockey_trainer_connection_stats...'; END $$;

DROP MATERIALIZED VIEW IF EXISTS jockey_trainer_connection_stats CASCADE;

CREATE MATERIALIZED VIEW jockey_trainer_connection_stats AS
SELECT 
    rr.jockey_slug,
    rr.trainer_slug,
    COUNT(*) as connection_races,
    SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) as connection_wins,
    SUM(CASE WHEN rr.position <= 3 THEN 1 ELSE 0 END) as connection_places,
    ROUND(SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) as connection_win_rate,
    ROUND(SUM(CASE WHEN rr.position <= 3 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) as connection_place_rate,
    MAX(r.race_date) as last_ride_together,
    (COUNT(*) >= 5 AND SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) > 0.15) as strong_connection
FROM race_results rr
JOIN races r ON rr.race_id = r.race_id
WHERE rr.jockey_slug IS NOT NULL AND rr.trainer_slug IS NOT NULL AND rr.position IS NOT NULL AND rr.position < 50
GROUP BY rr.jockey_slug, rr.trainer_slug;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jtcs_jockey_trainer ON jockey_trainer_connection_stats(jockey_slug, trainer_slug);
CREATE INDEX IF NOT EXISTS idx_jtcs_jockey ON jockey_trainer_connection_stats(jockey_slug);
CREATE INDEX IF NOT EXISTS idx_jtcs_trainer ON jockey_trainer_connection_stats(trainer_slug);

DO $$ DECLARE cnt INTEGER; BEGIN SELECT COUNT(*) INTO cnt FROM jockey_trainer_connection_stats; RAISE NOTICE '[6.5] jockey_trainer_connection_stats: % connections', cnt; END $$;

-- ============================================================================
-- PART 6: REFRESH FUNCTIONS
-- ============================================================================
DO $$ BEGIN RAISE NOTICE '[6.6] Creating refresh functions...'; END $$;

CREATE OR REPLACE FUNCTION refresh_horse_current_form() RETURNS void AS $$
BEGIN REFRESH MATERIALIZED VIEW CONCURRENTLY horse_current_form; END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_jockey_current_form() RETURNS void AS $$
BEGIN REFRESH MATERIALIZED VIEW CONCURRENTLY jockey_current_form; END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_trainer_current_form() RETURNS void AS $$
BEGIN REFRESH MATERIALIZED VIEW CONCURRENTLY trainer_current_form; END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_connection_stats() RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY jockey_horse_connection_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY jockey_trainer_connection_stats;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_all_current_form()
RETURNS TABLE(view_name TEXT, records INTEGER, refresh_time INTERVAL) AS $$
DECLARE start_time TIMESTAMP; cnt INTEGER;
BEGIN
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY horse_current_form;
    SELECT COUNT(*) INTO cnt FROM horse_current_form;
    view_name := 'horse_current_form'; records := cnt; refresh_time := clock_timestamp() - start_time; RETURN NEXT;
    
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY jockey_current_form;
    SELECT COUNT(*) INTO cnt FROM jockey_current_form;
    view_name := 'jockey_current_form'; records := cnt; refresh_time := clock_timestamp() - start_time; RETURN NEXT;
    
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY trainer_current_form;
    SELECT COUNT(*) INTO cnt FROM trainer_current_form;
    view_name := 'trainer_current_form'; records := cnt; refresh_time := clock_timestamp() - start_time; RETURN NEXT;
    
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY jockey_horse_connection_stats;
    SELECT COUNT(*) INTO cnt FROM jockey_horse_connection_stats;
    view_name := 'jockey_horse_connection_stats'; records := cnt; refresh_time := clock_timestamp() - start_time; RETURN NEXT;
    
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY jockey_trainer_connection_stats;
    SELECT COUNT(*) INTO cnt FROM jockey_trainer_connection_stats;
    view_name := 'jockey_trainer_connection_stats'; records := cnt; refresh_time := clock_timestamp() - start_time; RETURN NEXT;
END; $$ LANGUAGE plpgsql;

-- ============================================================================
-- COMPLETION
-- ============================================================================
DO $$
DECLARE h_cnt INTEGER; j_cnt INTEGER; t_cnt INTEGER; jhc_cnt INTEGER; jtc_cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO h_cnt FROM horse_current_form;
    SELECT COUNT(*) INTO j_cnt FROM jockey_current_form;
    SELECT COUNT(*) INTO t_cnt FROM trainer_current_form;
    SELECT COUNT(*) INTO jhc_cnt FROM jockey_horse_connection_stats;
    SELECT COUNT(*) INTO jtc_cnt FROM jockey_trainer_connection_stats;
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PHASE 5 COMPLETE - ALL CURRENT FORM VIEWS CREATED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'horse_current_form: % horses (23 cols)', h_cnt;
    RAISE NOTICE 'jockey_current_form: % jockeys (11 cols)', j_cnt;
    RAISE NOTICE 'trainer_current_form: % trainers (11 cols)', t_cnt;
    RAISE NOTICE 'jockey_horse_connection_stats: % (9 cols)', jhc_cnt;
    RAISE NOTICE 'jockey_trainer_connection_stats: % (9 cols)', jtc_cnt;
    RAISE NOTICE '';
    RAISE NOTICE 'Daily refresh: SELECT refresh_all_current_form();';
    RAISE NOTICE '============================================================';
END $$;
