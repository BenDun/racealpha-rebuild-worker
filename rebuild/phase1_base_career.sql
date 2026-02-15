-- ============================================================================
-- PHASE 1: BASE INSERT + CAREER STATS (DuckDB)
-- ============================================================================
-- Covers: Old 01_base_rebuild + 02_career_form_stats
-- Est. Time: 2-5 minutes in DuckDB (vs 30-45min in Postgres)
--
-- WHAT THIS DOES:
--   1. Inserts base records from races + race_results
--   2. Sets track properties (direction, category, type)
--   3. Race flags (maiden, handicap, age/sex restriction)
--   4. Total runners per race + barrier position
--   5. Career stats with anti-leakage (wins, places, percentages)
--   6. Days since last race
--   7. Last 5 race stats (win rate, place rate, avg position, form momentum)
--   8. Form recency score
--   9. Horse ELO ratings
--  10. Jockey/trainer win/place rates
--  11. Distance & track win rates
--  12. Cross-region + first-timer flags
-- ============================================================================

-- ============================================================================
-- STEP 1: BASE INSERT
-- ============================================================================
INSERT INTO race_training_dataset_new (
    race_id, race_date, race_number, track_name, race_distance, race_class,
    track_condition, track_weather, horse_name, horse_slug, barrier, weight,
    final_position, margin, jockey, jockey_slug, trainer, trainer_slug, win_odds,
    horse_number, prize_money,
    location, horse_location_slug, jockey_location_slug, trainer_location_slug,
    created_at, updated_at
)
SELECT
    rr.race_id,
    r.race_date,
    r.race_number,
    -- Standardize track names
    CASE
        WHEN r.track_name ILIKE '%royal randwick%' THEN 'Randwick'
        WHEN r.track_name ILIKE '%randwick%' THEN 'Randwick'
        WHEN r.track_name ILIKE '%flemington%racecourse%' THEN 'Flemington'
        WHEN r.track_name ILIKE '%moonee valley%' THEN 'Moonee Valley'
        WHEN r.track_name ILIKE '%caulfield%racecourse%' THEN 'Caulfield'
        ELSE COALESCE(r.track_name, 'Unknown')
    END,
    r.race_distance,
    r.race_class,
    r.track_condition,
    r.track_weather,
    REPLACE(rr.horse_name, 'E+', 'E'),
    rr.horse_slug,
    rr.barrier,
    rr.original_weight,
    rr.position,
    rr.margin,
    rr.jockey,
    rr.jockey_slug,
    rr.trainer,
    rr.trainer_slug,
    rr.win_odds,
    TRY_CAST(NULLIF(rr.horse_number, '') AS DOUBLE),
    COALESCE(rr.prize_money, 0),
    -- Location
    CASE
        WHEN r.track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
        WHEN r.track_name IN ('Royal Ascot','Newmarket','Epsom Downs','York','Goodwood','Cheltenham','Aintree','Newbury','Sandown Park','Doncaster','Haydock','Kempton','Wolverhampton','Southwell','Lingfield','Chester','Hamilton','Musselburgh','Nottingham','Pontefract','Sedgefield','Plumpton','Yarmouth','Down Royal') THEN 'UK'
        WHEN r.track_name IN ('Curragh','Leopardstown','Punchestown','Fairyhouse','Naas','Gowran Park','Galway','Cork','Killarney','Dundalk','Navan','Thurles') THEN 'IE'
        WHEN r.track_name IN ('ParisLongchamp','Chantilly','Deauville','Saint-Cloud','Compiegne','Fontainebleau','Lyon-Parilly','Vichy','Clairefontaine','Bordeaux Le Bouscat','Nantes') THEN 'FR'
        WHEN r.track_name IN ('Tokyo','Kyoto','Hanshin','Nakayama','Chukyo','Niigata','Fukushima','Kokura','Sapporo') THEN 'JP'
        WHEN r.track_name IN ('Churchill Downs','Keeneland','Saratoga','Gulfstream','Kentucky Downs','Tampa Bay Downs') THEN 'US'
        WHEN r.track_name = 'Meydan' THEN 'AE'
        ELSE 'AU'
    END,
    rr.horse_slug || '-' || CASE WHEN r.track_name IN ('Sha Tin','Happy Valley') THEN 'HK' WHEN r.track_name IN ('Royal Ascot','Newmarket','Epsom Downs','York','Goodwood','Cheltenham','Aintree','Newbury','Sandown Park','Doncaster','Haydock','Kempton','Wolverhampton','Southwell','Lingfield','Chester','Hamilton','Musselburgh','Nottingham','Pontefract','Sedgefield','Plumpton','Yarmouth','Down Royal') THEN 'UK' WHEN r.track_name IN ('Curragh','Leopardstown','Punchestown','Fairyhouse','Naas','Gowran Park','Galway','Cork','Killarney','Dundalk','Navan','Thurles') THEN 'IE' ELSE 'AU' END,
    rr.jockey_slug || '-' || CASE WHEN r.track_name IN ('Sha Tin','Happy Valley') THEN 'HK' ELSE 'AU' END,
    rr.trainer_slug || '-' || CASE WHEN r.track_name IN ('Sha Tin','Happy Valley') THEN 'HK' ELSE 'AU' END,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM race_results rr
JOIN races r ON rr.race_id = r.race_id
WHERE rr.horse_slug IS NOT NULL
  AND rr.position IS NOT NULL
  AND rr.position < 50;

-- ============================================================================
-- STEP 2: TRACK & RACE PROPERTIES (single UPDATE)
-- ============================================================================
UPDATE race_training_dataset_new SET
    -- Track direction
    track_direction = CASE
        WHEN track_name IN ('Sha Tin','Happy Valley','Randwick','Royal Randwick','Rosehill','Canterbury','Warwick Farm','Newcastle','Kembla Grange','Hawkesbury','Gosford','Wyong','Scone','Muswellbrook','Tamworth','Dubbo','Orange','Bathurst','Grafton','Wagga','Albury','Canberra','Eagle Farm','Doomben','Sunshine Coast','Gold Coast','Ipswich','Toowoomba') THEN 'clockwise'
        WHEN location = 'AU' THEN 'anticlockwise'
        WHEN track_name IN ('Ascot','Royal Ascot','Newmarket','Newbury','York','Goodwood','Epsom Downs','Curragh','Leopardstown','ParisLongchamp','Chantilly','Deauville','Tokyo','Nakayama','Kyoto','Hanshin','Meydan','Churchill Downs','Keeneland','Saratoga') THEN 'clockwise'
        ELSE 'anticlockwise'
    END,
    -- Track category
    track_category = CASE
        WHEN track_name IN ('Flemington','Caulfield','Moonee Valley','The Valley','Sandown','Sandown Lakeside','Sandown Hillside','Randwick','Royal Randwick','Rosehill','Rosehill Gardens','Canterbury','Warwick Farm','Eagle Farm','Doomben','Morphettville','Morphettville Parks','Ascot','Belmont','Launceston','Hobart','Sha Tin','Happy Valley','Royal Ascot','Newmarket','Epsom Downs','York','Goodwood','Cheltenham','Aintree','Newbury','Curragh','Leopardstown','ParisLongchamp','Chantilly','Deauville','Saint-Cloud','Tokyo','Kyoto','Hanshin','Nakayama','Meydan','Churchill Downs','Keeneland','Saratoga','Gulfstream') THEN 'Metro'
        WHEN track_name IN ('Geelong','Bendigo','Ballarat','Mornington','Cranbourne','Pakenham','Werribee','Seymour','Kilmore','Sale','Moe','Wangaratta','Warrnambool','Newcastle','Kembla Grange','Hawkesbury','Gosford','Wyong','Scone','Muswellbrook','Tamworth','Dubbo','Wagga','Albury','Canberra','Grafton','Sunshine Coast','Gold Coast','Ipswich','Toowoomba','Murray Bridge','Gawler','Bunbury','Pinjarra','Northam','Kempton','Wolverhampton','Southwell','Lingfield','Chester','Hamilton','Punchestown','Fairyhouse','Naas','Gowran Park','Galway','Cork','Killarney','Dundalk','Navan','Thurles') THEN 'Provincial'
        ELSE 'Country'
    END,
    -- Track type
    track_type = CASE
        WHEN track_name ILIKE '%synthetic%' THEN 'Synthetic'
        WHEN track_name IN ('Pakenham Synthetic','Ballarat Synthetic','Devonport Synthetic') THEN 'Synthetic'
        WHEN track_name IN ('Wolverhampton','Southwell','Lingfield','Dundalk') AND location IN ('UK','IE') THEN 'Synthetic'
        WHEN track_name IN ('Churchill Downs','Keeneland','Saratoga','Gulfstream','Tampa Bay Downs','Meydan') THEN 'Dirt'
        ELSE 'Turf'
    END,
    -- Race flags
    is_maiden_race = (race_class ILIKE '%maiden%' OR race_class ILIKE '%mdn%'),
    -- Handicap detection: explicit keyword OR bare "Class X" without "Set Weights"
    -- In AU/HK racing, "Class 4" = handicap, "Class 4, Set Weights" = not handicap
    -- Also catch BM (Benchmark) races which are always handicaps
    is_handicap_race = (
        race_class ILIKE '%handicap%'
        OR race_class ILIKE '%hcp%'
        OR (regexp_matches(race_class, '^Class [0-9]', 'i') AND race_class NOT ILIKE '%set weights%' AND race_class NOT ILIKE '%SW%')
        OR regexp_matches(race_class, 'BM[0-9]', 'i')
        OR regexp_matches(race_class, '0 - [0-9]', 'i')
        OR regexp_matches(race_class, 'Rest\. [0-9]', 'i')
    ),
    age_restriction = CASE
        WHEN regexp_matches(race_class, '2[- ]?y(ear)?[- ]?o(ld)?', 'i') THEN '2yo'
        WHEN regexp_matches(race_class, '3[- ]?y(ear)?[- ]?o(ld)?', 'i') THEN '3yo'
        ELSE 'open'
    END,
    sex_restriction = CASE
        WHEN race_class ILIKE '%fillies%' AND race_class ILIKE '%mares%' THEN 'fillies_mares'
        WHEN race_class ILIKE '%fillies%' THEN 'fillies'
        WHEN race_class ILIKE '%mares%' THEN 'mares'
        ELSE 'open'
    END,
    -- Distance range
    distance_range = CASE
        WHEN race_distance < 1200 THEN 'sprint'
        WHEN race_distance < 1600 THEN 'mile'
        WHEN race_distance < 2000 THEN 'middle'
        ELSE 'staying'
    END;

-- ============================================================================
-- STEP 3: TOTAL RUNNERS + BARRIER POSITION
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    total_runners = sub.field_size
FROM (
    SELECT race_id, COUNT(*) AS field_size
    FROM race_training_dataset_new
    WHERE final_position IS NOT NULL
    GROUP BY race_id
) sub
WHERE rtd.race_id = sub.race_id;

UPDATE race_training_dataset_new SET
    barrier_position = CASE
        WHEN total_runners IS NULL OR total_runners = 0 THEN 'unknown'
        WHEN barrier::float / total_runners <= 0.33 THEN 'inner'
        WHEN barrier::float / total_runners <= 0.66 THEN 'middle'
        ELSE 'outer'
    END
WHERE barrier IS NOT NULL;

-- ============================================================================
-- STEP 4: HORSE CAREER STATS (Anti-leakage window functions)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    total_races  = COALESCE(hc.prior_total_races, 0),
    wins         = COALESCE(hc.prior_wins, 0),
    places       = COALESCE(hc.prior_places, 0),
    win_percentage = CASE WHEN COALESCE(hc.prior_total_races, 0) > 0 THEN ROUND(hc.prior_wins * 100.0 / hc.prior_total_races, 2) ELSE 0 END,
    place_percentage = CASE WHEN COALESCE(hc.prior_total_races, 0) > 0 THEN ROUND(hc.prior_places * 100.0 / hc.prior_total_races, 2) ELSE 0 END,
    is_first_timer = (COALESCE(hc.prior_total_races, 0) = 0),
    days_since_last_race = CASE WHEN hc.prev_race_date IS NOT NULL THEN (rtd.race_date - hc.prev_race_date) ELSE NULL END
FROM (
    SELECT
        race_id, horse_location_slug,
        COUNT(*) OVER w_prior AS prior_total_races,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w_prior AS prior_wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w_prior AS prior_places,
        LAG(race_date) OVER w_order AS prev_race_date
    FROM race_training_dataset_new
    WHERE final_position IS NOT NULL AND horse_location_slug IS NOT NULL
    WINDOW
        w_prior AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w_order AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id)
) hc
WHERE rtd.race_id = hc.race_id AND rtd.horse_location_slug = hc.horse_location_slug;

-- ============================================================================
-- STEP 5: LAST 5 RACE STATS (Anti-leakage)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    last_5_avg_position = c.avg_pos,
    last_5_win_rate     = c.win_rate,
    last_5_place_rate   = c.place_rate,
    form_momentum       = c.momentum
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
        ROUND(CASE
            WHEN prev_count = 0 THEN NULL
            WHEN prev_count = 1 THEN p1
            WHEN prev_count = 2 THEN (p1 + p2) / 2.0
            WHEN prev_count = 3 THEN (p1 + p2 + p3) / 3.0
            WHEN prev_count = 4 THEN (p1 + p2 + p3 + p4) / 4.0
            ELSE (p1 + p2 + p3 + p4 + p5) / 5.0
        END, 2) AS avg_pos,
        ROUND(CASE WHEN prev_count = 0 THEN 0 ELSE
            ((CASE WHEN p1=1 THEN 1 ELSE 0 END) + (CASE WHEN p2=1 THEN 1 ELSE 0 END) + (CASE WHEN p3=1 THEN 1 ELSE 0 END) + (CASE WHEN p4=1 THEN 1 ELSE 0 END) + (CASE WHEN p5=1 THEN 1 ELSE 0 END))::numeric / LEAST(prev_count, 5)
        END, 4) AS win_rate,
        ROUND(CASE WHEN prev_count = 0 THEN 0 ELSE
            ((CASE WHEN p1<=3 THEN 1 ELSE 0 END) + (CASE WHEN p2<=3 THEN 1 ELSE 0 END) + (CASE WHEN p3<=3 THEN 1 ELSE 0 END) + (CASE WHEN p4<=3 THEN 1 ELSE 0 END) + (CASE WHEN p5<=3 THEN 1 ELSE 0 END))::numeric / LEAST(prev_count, 5)
        END, 4) AS place_rate,
        ROUND(CASE
            WHEN prev_count = 0 THEN 0
            WHEN prev_count >= 5 THEN
                ((CASE WHEN p1<=3 THEN 5 ELSE 0 END) + (CASE WHEN p2<=3 THEN 4 ELSE 0 END) + (CASE WHEN p3<=3 THEN 3 ELSE 0 END) + (CASE WHEN p4<=3 THEN 2 ELSE 0 END) + (CASE WHEN p5<=3 THEN 1 ELSE 0 END))::numeric / 15
            WHEN prev_count = 1 THEN (CASE WHEN p1<=3 THEN 1 ELSE 0 END)::numeric
            ELSE ((CASE WHEN p1<=3 THEN prev_count ELSE 0 END) + (CASE WHEN p2<=3 THEN prev_count-1 ELSE 0 END))::numeric / (prev_count*(prev_count+1)/2)
        END, 4) AS momentum
    FROM lagged
) c
WHERE rtd.race_id = c.race_id AND rtd.horse_location_slug = c.horse_location_slug;

-- First-timers default
UPDATE race_training_dataset_new SET
    last_5_avg_position = NULL,
    last_5_win_rate = 0,
    last_5_place_rate = 0,
    form_momentum = 0
WHERE is_first_timer = TRUE OR total_races = 0;

-- ============================================================================
-- STEP 6: FORM RECENCY SCORE
-- ============================================================================
UPDATE race_training_dataset_new SET
    form_recency_score = CASE
        WHEN days_since_last_race IS NULL THEN 0.5
        WHEN days_since_last_race <= 14 THEN 1.0
        WHEN days_since_last_race <= 28 THEN 0.9
        WHEN days_since_last_race <= 42 THEN 0.8
        WHEN days_since_last_race <= 60 THEN 0.6
        WHEN days_since_last_race <= 90 THEN 0.4
        ELSE 0.2
    END;

-- ============================================================================
-- STEP 7: HORSE ELO RATINGS (from horse_elo_ratings table - real iterative ELO)
-- ============================================================================
-- Uses elo_before = the ELO the horse had BEFORE this race (no leakage).
-- This is the same ELO system that PredictButton.tsx sends at prediction time
-- from horse_elo_ratings.elo_after (the most recent completed race).
-- Base starting ELO = 1200 for unknown horses.
-- Range: 600 - 2200 (iterative, opponent-aware)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    horse_elo = COALESCE(her.elo_before, 1200),
    is_elo_default = (her.elo_before IS NULL)
FROM horse_elo_ratings her
WHERE rtd.horse_slug = her.horse_slug
  AND rtd.race_id = her.race_id;

-- Horses with no ELO record at all (not in horse_elo_ratings table)
UPDATE race_training_dataset_new SET
    horse_elo = 1200,
    is_elo_default = TRUE
WHERE horse_elo IS NULL;

-- ============================================================================
-- STEP 7b: FEATURE MATURITY INDICATOR (Cold-start improvement)
-- ============================================================================
-- Two new features that tell the model how much history exists for each horse:
--
-- elo_races_count: INTEGER count of prior ELO records (0 = debut)
--   - More granular than is_elo_default (boolean) or experienced_horse_flag (10+)
--   - Lets model learn diminishing returns of extra data points
--   - 0 for first race, 1 for second, etc.
--
-- feature_maturity_score: NUMERIC 0-1 score (logarithmic scaling)
--   - 0.0 = debut horse (no history)
--   - 0.5 = ~4 races
--   - 0.7 = ~7 races  
--   - 0.85 = ~12 races
--   - 1.0 = 20+ races (saturated)
--   - Smooth transformation that captures diminishing returns
--
-- Expected impact: +1-2pp Pure Alpha AUC (helps model handle cold-start)
-- ============================================================================

-- Count of prior ELO records per horse (using window function for performance)
UPDATE race_training_dataset_new AS rtd SET
    elo_races_count = COALESCE(ec.prior_elo_races, 0)
FROM (
    SELECT race_id, horse_slug,
        CAST(ROW_NUMBER() OVER (PARTITION BY horse_slug ORDER BY race_date, race_id) - 1 AS INTEGER) as prior_elo_races
    FROM horse_elo_ratings
) ec
WHERE rtd.horse_slug = ec.horse_slug AND rtd.race_id = ec.race_id;

-- Horses not in horse_elo_ratings at all (debut)
UPDATE race_training_dataset_new SET
    elo_races_count = 0
WHERE elo_races_count IS NULL;

-- Feature maturity score: logarithmic 0-1 based on total_races
-- LN(x+1)/LN(21): 0→0.0, 1→0.23, 2→0.36, 5→0.59, 10→0.79, 20→1.0
UPDATE race_training_dataset_new SET
    feature_maturity_score = LEAST(1.0, LN(COALESCE(total_races, 0) + 1) / LN(21));

-- ============================================================================
-- STEP 8: JOCKEY WIN/PLACE RATES (Anti-leakage)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    jockey_win_rate = CASE WHEN COALESCE(js.prior_rides, 0) > 0 THEN ROUND(js.prior_wins * 1.0 / js.prior_rides, 4) ELSE 0 END,
    jockey_place_rate = CASE WHEN COALESCE(js.prior_rides, 0) > 0 THEN ROUND(js.prior_places * 1.0 / js.prior_rides, 4) ELSE 0 END
FROM (
    SELECT race_id, jockey_location_slug,
        COUNT(*) OVER w AS prior_rides,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS prior_wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w AS prior_places
    FROM race_training_dataset_new
    WHERE final_position IS NOT NULL AND jockey_location_slug IS NOT NULL
    WINDOW w AS (PARTITION BY jockey_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) js
WHERE rtd.race_id = js.race_id AND rtd.jockey_location_slug = js.jockey_location_slug;

-- ============================================================================
-- STEP 9: TRAINER WIN/PLACE RATES (Anti-leakage)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    trainer_win_rate = CASE WHEN COALESCE(ts.prior_runners, 0) > 0 THEN ROUND(ts.prior_wins * 1.0 / ts.prior_runners, 4) ELSE 0 END,
    trainer_place_rate = CASE WHEN COALESCE(ts.prior_runners, 0) > 0 THEN ROUND(ts.prior_places * 1.0 / ts.prior_runners, 4) ELSE 0 END
FROM (
    SELECT race_id, trainer_location_slug,
        COUNT(*) OVER w AS prior_runners,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS prior_wins,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w AS prior_places
    FROM race_training_dataset_new
    WHERE final_position IS NOT NULL AND trainer_location_slug IS NOT NULL
    WINDOW w AS (PARTITION BY trainer_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) ts
WHERE rtd.race_id = ts.race_id AND rtd.trainer_location_slug = ts.trainer_location_slug;

-- ============================================================================
-- STEP 10: DISTANCE WIN RATES (Anti-leakage)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    distance_win_rate = CASE WHEN COALESCE(ds.prior_at_dist, 0) > 0 THEN ROUND(ds.wins_at_dist * 1.0 / ds.prior_at_dist, 4) ELSE 0 END,
    races_at_distance = COALESCE(ds.prior_at_dist, 0)
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w AS prior_at_dist,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins_at_dist
    FROM race_training_dataset_new
    WHERE final_position IS NOT NULL AND horse_location_slug IS NOT NULL AND distance_range IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug, distance_range ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) ds
WHERE rtd.race_id = ds.race_id AND rtd.horse_location_slug = ds.horse_location_slug;

-- ============================================================================
-- STEP 11: TRACK WIN/PLACE RATES (Anti-leakage)
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    track_win_rate = CASE WHEN COALESCE(ts.prior_at_track, 0) > 0 THEN ROUND(ts.wins_at_track * 1.0 / ts.prior_at_track, 4) ELSE 0 END,
    track_place_rate = CASE WHEN COALESCE(ts.prior_at_track, 0) > 0 THEN ROUND(ts.places_at_track * 1.0 / ts.prior_at_track, 4) ELSE 0 END,
    races_at_track = COALESCE(ts.prior_at_track, 0)
FROM (
    SELECT race_id, horse_location_slug,
        COUNT(*) OVER w AS prior_at_track,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w AS wins_at_track,
        SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w AS places_at_track
    FROM race_training_dataset_new
    WHERE final_position IS NOT NULL AND horse_location_slug IS NOT NULL AND track_name IS NOT NULL
    WINDOW w AS (PARTITION BY horse_location_slug, track_name ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) ts
WHERE rtd.race_id = ts.race_id AND rtd.horse_location_slug = ts.horse_location_slug;

-- ============================================================================
-- STEP 12: CROSS-REGION + NEVER PLACED FLAGS
-- ============================================================================
UPDATE race_training_dataset_new AS rtd SET
    is_cross_region_horse = COALESCE(cr.has_diff_region, FALSE)
FROM (
    -- FIXED: Anti-leakage — only check if horse raced in a different region in PRIOR races
    -- Previous version used GROUP BY across ALL races (knew about future cross-region activity)
    WITH with_first_loc AS (
        SELECT race_id, horse_slug, race_date, location,
            FIRST_VALUE(location) OVER (PARTITION BY horse_slug ORDER BY race_date, race_id) AS first_location
        FROM race_training_dataset_new
        WHERE horse_slug IS NOT NULL
    )
    SELECT race_id, horse_slug,
        SUM(CASE WHEN location != first_location THEN 1 ELSE 0 END) OVER (
            PARTITION BY horse_slug ORDER BY race_date, race_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) > 0 AS has_diff_region
    FROM with_first_loc
) cr
WHERE rtd.race_id = cr.race_id AND rtd.horse_slug = cr.horse_slug;

UPDATE race_training_dataset_new SET is_cross_region_horse = FALSE WHERE is_cross_region_horse IS NULL;
UPDATE race_training_dataset_new SET never_placed_flag = (total_races > 0 AND places = 0);
