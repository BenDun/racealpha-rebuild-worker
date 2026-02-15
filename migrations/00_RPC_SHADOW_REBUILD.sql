-- ============================================================================
-- RPC: SHADOW TABLE REBUILD V2 (Zero Downtime)
-- ============================================================================
-- Builds into a SHADOW TABLE while production keeps running
-- Then does an ATOMIC SWAP at the end
-- 
-- USAGE: SELECT run_training_shadow_rebuild();
-- 
-- BENEFITS:
--   - Zero production downtime during rebuild
--   - No locks on production table
--   - Validate new data before swap
--   - Easy rollback (swap back)
--   - Production queries continue at full speed
--
-- n8n WORKFLOW:
--   1. HTTP Request node → Supabase REST API
--   2. POST to /rest/v1/rpc/run_training_shadow_rebuild
--   3. Headers: apikey, Authorization Bearer
--   4. Schedule: Weekly on Sunday 2am AEST
-- ============================================================================

-- Progress tracking table
CREATE TABLE IF NOT EXISTS shadow_rebuild_progress (
    id SERIAL PRIMARY KEY,
    run_id UUID DEFAULT gen_random_uuid(),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    current_phase TEXT,
    phase_number INTEGER DEFAULT 0,
    total_phases INTEGER DEFAULT 5,
    status TEXT DEFAULT 'running', -- running, completed, failed, swapped
    error_message TEXT,
    records_processed INTEGER DEFAULT 0,
    shadow_table_name TEXT DEFAULT 'race_training_dataset_shadow'
);

CREATE INDEX IF NOT EXISTS idx_shadow_rebuild_run_id ON shadow_rebuild_progress(run_id);
CREATE INDEX IF NOT EXISTS idx_shadow_rebuild_status ON shadow_rebuild_progress(status);


-- ============================================================================
-- MAIN SHADOW REBUILD FUNCTION
-- ============================================================================
CREATE OR REPLACE FUNCTION run_training_shadow_rebuild()
RETURNS JSON AS $$
DECLARE
    v_run_id UUID;
    v_start_time TIMESTAMP;
    v_phase_start TIMESTAMP;
    v_records INTEGER;
    v_result JSON;
BEGIN
    v_start_time := NOW();
    
    -- Create progress record
    INSERT INTO shadow_rebuild_progress (status, current_phase)
    VALUES ('running', 'Initializing Shadow Table')
    RETURNING run_id INTO v_run_id;
    
    -- Set session parameters for heavy operations
    SET statement_timeout = '0';
    SET lock_timeout = '0';
    SET work_mem = '512MB';
    SET maintenance_work_mem = '1GB';
    
    -- ========================================================================
    -- PHASE 0: CREATE SHADOW TABLE (Copy structure, not data)
    -- ========================================================================
    UPDATE shadow_rebuild_progress SET current_phase = 'Phase 0: Creating Shadow Table', phase_number = 0 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Drop shadow table if exists from previous failed run
    DROP TABLE IF EXISTS race_training_dataset_shadow CASCADE;
    
    -- Create shadow table with SAME structure as production (no data)
    CREATE TABLE race_training_dataset_shadow (LIKE race_training_dataset INCLUDING ALL);
    
    -- Add unique constraint for upsert pattern
    ALTER TABLE race_training_dataset_shadow 
    ADD CONSTRAINT race_training_dataset_shadow_race_id_horse_slug_key 
    UNIQUE (race_id, horse_slug);
    
    RAISE NOTICE 'Phase 0 complete: Shadow table created in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- PHASE 1: BASE INSERT INTO SHADOW TABLE
    -- ========================================================================
    UPDATE shadow_rebuild_progress SET current_phase = 'Phase 1: Base Rebuild into Shadow', phase_number = 1 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Fix source data locations first (in races table - this is quick)
    UPDATE races SET location = CASE
        WHEN track_name IN ('Sha Tin', 'Happy Valley') THEN 'HK'
        WHEN track_name IN (
            'Royal Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood',
            'Sandown Park', 'Haydock Park', 'Kempton Park', 'Lingfield Park',
            'Newcastle', 'Doncaster', 'Chester', 'Aintree', 'Cheltenham',
            'Wolverhampton', 'Southwell', 'Nottingham', 'Bath', 'Brighton',
            'Carlisle', 'Catterick', 'Ffos Las', 'Fontwell', 'Hamilton',
            'Hexham', 'Huntingdon', 'Kelso', 'Leicester', 'Ludlow',
            'Market Rasen', 'Musselburgh', 'Newbury', 'Newton Abbot',
            'Perth', 'Pontefract', 'Redcar', 'Ripon', 'Salisbury',
            'Sedgefield', 'Stratford', 'Taunton', 'Thirsk', 'Towcester',
            'Uttoxeter', 'Warwick', 'Wetherby', 'Wincanton', 'Windsor',
            'Worcester', 'Ayr', 'Bangor-on-Dee', 'Beverley', 'Chepstow',
            'Exeter', 'Fakenham', 'Folkestone', 'Great Leighs', 'Hereford',
            'Plumpton', 'Yarmouth', 'Down Royal'
        ) THEN 'UK'
        WHEN track_name IN (
            'Curragh', 'Leopardstown', 'Punchestown', 'Fairyhouse', 'Naas',
            'Gowran Park', 'Galway', 'Cork', 'Killarney', 'Dundalk', 'Navan', 'Thurles'
        ) THEN 'IE'
        WHEN track_name IN (
            'ParisLongchamp', 'Chantilly', 'Deauville', 'Saint-Cloud',
            'Maisons-Laffitte', 'Longchamp', 'Auteuil', 'Compiegne',
            'Clairefontaine', 'Bordeaux Le Bouscat', 'Nantes'
        ) THEN 'FR'
        WHEN track_name IN (
            'Tokyo', 'Kyoto', 'Hanshin', 'Nakayama', 'Chukyo',
            'Niigata', 'Fukushima', 'Kokura', 'Sapporo'
        ) THEN 'JP'
        WHEN track_name IN (
            'Churchill Downs', 'Keeneland', 'Saratoga', 'Gulfstream',
            'Kentucky Downs', 'Tampa Bay Downs'
        ) THEN 'US'
        WHEN track_name = 'Meydan' THEN 'AE'
        ELSE 'AU'
    END
    WHERE track_name IS NOT NULL;
    
    -- Insert base data into SHADOW table
    INSERT INTO race_training_dataset_shadow (
        race_id, race_date, race_number, track_name, race_distance, race_class,
        track_condition, rail_position, prize_money, horse_slug, horse_name,
        jockey_slug, trainer_slug, barrier, weight_carried, win_odds, place_odds,
        final_position, finish_status, margin, finish_time, location,
        horse_location_slug, jockey_location_slug, trainer_location_slug,
        created_at, updated_at
    )
    SELECT 
        rr.race_id, r.race_date, r.race_number,
        r.track_name, r.distance as race_distance, r.race_class,
        r.track_condition, r.rail_position, r.prize_money,
        rr.horse_slug, rr.horse_name,
        rr.jockey_slug, rr.trainer_slug,
        rr.barrier, rr.weight_carried,
        rr.win_odds, rr.place_odds,
        rr.position as final_position,
        rr.finish_status, rr.margin, rr.finish_time,
        COALESCE(r.location, 'AU') as location,
        rr.horse_slug || '_' || COALESCE(r.location, 'AU') as horse_location_slug,
        rr.jockey_slug || '_' || COALESCE(r.location, 'AU') as jockey_location_slug,
        rr.trainer_slug || '_' || COALESCE(r.location, 'AU') as trainer_location_slug,
        NOW(), NOW()
    FROM race_results rr
    JOIN races r ON rr.race_id = r.race_id
    WHERE rr.horse_slug IS NOT NULL 
    AND rr.position IS NOT NULL 
    AND rr.position < 50;
    
    -- Set track properties on shadow table
    UPDATE race_training_dataset_shadow SET 
        track_direction = CASE
            -- Clockwise tracks (Australian)
            WHEN track_name IN ('Flemington', 'Caulfield', 'Moonee Valley', 'Sandown', 'Sandown Lakeside',
                'Sandown Hillside', 'Cranbourne', 'Mornington', 'Pakenham', 'Geelong', 'Ballarat', 'Bendigo',
                'Wangaratta', 'Seymour', 'Kilmore', 'Stony Creek', 'Sale', 'Tatura', 'Echuca', 'Swan Hill',
                'Warrnambool', 'Hamilton', 'Ararat', 'Stawell', 'Moe', 'Bairnsdale', 'Donald', 'Terang',
                'Dunkeld', 'Kyneton', 'Murtoa', 'Yarra Valley', 'Woolamai', 'Camperdown', 'Colac',
                'Randwick', 'Rosehill', 'Canterbury', 'Warwick Farm', 'Royal Randwick', 'Kensington',
                'Newcastle', 'Gosford', 'Wyong', 'Hawkesbury', 'Scone', 'Tamworth', 'Muswellbrook',
                'Grafton', 'Coffs Harbour', 'Taree', 'Port Macquarie', 'Dubbo', 'Wellington', 'Mudgee',
                'Orange', 'Bathurst', 'Goulburn', 'Queanbeyan', 'Canberra', 'Nowra', 'Moruya', 'Sapphire Coast',
                'Albury', 'Wagga', 'Wagga Wagga', 'Corowa', 'Tumut', 'Gundagai', 'Young', 'Cowra', 'Forbes',
                'Parkes', 'Narromine', 'Coonamble', 'Gilgandra', 'Coonabarabran', 'Nyngan', 'Warren',
                'Broken Hill', 'Menindee', 'Casino', 'Lismore', 'Ballina', 'Murwillumbah', 'Tweed River',
                'Brisbane', 'Eagle Farm', 'Doomben', 'Gold Coast', 'Ipswich', 'Sunshine Coast', 'Caloundra',
                'Toowoomba', 'Kilcoy', 'Gatton', 'Beaudesert', 'Dalby', 'Roma', 'Chinchilla', 'Goondiwindi',
                'Stanthorpe', 'Warwick', 'Emerald', 'Gladstone', 'Bundaberg', 'Rockhampton', 'Mackay',
                'Townsville', 'Cairns', 'Innisfail', 'Mount Isa', 'Longreach', 'Barcaldine', 'Blackall',
                'Charleville', 'Cunnamulla', 'Thangool', 'Biloela', 'Yeppoon', 'Bowen', 'Charters Towers',
                'Cloncurry', 'Julia Creek', 'Richmond', 'Hughenden', 'Winton',
                'Darwin', 'Alice Springs', 'Tennant Creek', 'Katherine',
                'Morphettville', 'Murray Bridge', 'Gawler', 'Port Augusta', 'Port Lincoln', 'Mount Gambier',
                'Strathalbyn', 'Balaklava', 'Clare', 'Naracoorte', 'Bordertown', 'Penola', 'Millicent', 'Oakbank',
                'Perth', 'Ascot', 'Belmont', 'Bunbury', 'Pinjarra', 'Northam', 'York', 'Narrogin', 'Kalgoorlie',
                'Albany', 'Geraldton', 'Broome', 'Carnarvon', 'Esperance', 'Lark Hill',
                'Hobart', 'Launceston', 'Devonport', 'Spreyton',
                'Sha Tin', 'Happy Valley'
            ) THEN 'Clockwise'
            -- Anti-clockwise tracks
            WHEN track_name IN (
                'Ascot', 'Newmarket', 'Epsom Downs', 'York', 'Goodwood',
                'Sandown Park', 'Haydock Park', 'Kempton Park', 'Lingfield Park',
                'Doncaster', 'Chester', 'Aintree', 'Cheltenham',
                'Curragh', 'Leopardstown', 'ParisLongchamp', 'Chantilly', 'Deauville'
            ) THEN 'Anti-clockwise'
            ELSE 'Clockwise'
        END,
        track_category = CASE
            WHEN track_name IN ('Flemington', 'Caulfield', 'Moonee Valley', 'Randwick', 'Rosehill', 
                'Royal Randwick', 'Canterbury', 'Warwick Farm', 'Eagle Farm', 'Doomben', 'Morphettville', 
                'Perth', 'Ascot', 'Sha Tin', 'Happy Valley') THEN 'Metro'
            WHEN track_name IN ('Sandown', 'Sandown Lakeside', 'Sandown Hillside', 'Cranbourne', 'Pakenham',
                'Newcastle', 'Gosford', 'Wyong', 'Hawkesbury', 'Gold Coast', 'Ipswich', 'Sunshine Coast',
                'Caloundra', 'Murray Bridge', 'Gawler', 'Belmont', 'Bunbury', 'Pinjarra') THEN 'Provincial'
            ELSE 'Country'
        END,
        track_surface = CASE
            WHEN track_name IN ('Wolverhampton', 'Kempton Park', 'Lingfield Park', 'Dundalk', 
                'Newcastle', 'Southwell') THEN 'Synthetic'
            ELSE 'Turf'
        END;
    
    -- Distance range categorization
    UPDATE race_training_dataset_shadow SET distance_range = CASE
        WHEN race_distance < 1000 THEN 'Sprint (<1000m)'
        WHEN race_distance < 1400 THEN 'Short (1000-1400m)'
        WHEN race_distance < 1800 THEN 'Mile (1400-1800m)'
        WHEN race_distance < 2200 THEN 'Middle (1800-2200m)'
        ELSE 'Staying (2200m+)'
    END WHERE race_distance IS NOT NULL;
    
    -- Class flags
    UPDATE race_training_dataset_shadow SET 
        is_maiden = (race_class ILIKE '%maiden%'),
        is_handicap = (race_class ILIKE '%handicap%' OR race_class ILIKE '%hcp%'),
        is_group_race = (race_class ILIKE '%group%' OR race_class ILIKE '%G1%' OR race_class ILIKE '%G2%' OR race_class ILIKE '%G3%'),
        is_listed_race = (race_class ILIKE '%listed%'),
        is_restricted_age = (race_class ~* '(2yo|3yo|4yo|2 year|3 year|4 year)'),
        is_restricted_sex = (race_class ~* '(fillies|mares|colts|geldings|f&m|c&g)');
    
    -- Calculate total_runners per race
    WITH fs AS (SELECT race_id, COUNT(*) as field_size FROM race_training_dataset_shadow GROUP BY race_id)
    UPDATE race_training_dataset_shadow rtd SET total_runners = fs.field_size FROM fs WHERE rtd.race_id = fs.race_id;
    
    -- Barrier position
    UPDATE race_training_dataset_shadow SET barrier_position = CASE
        WHEN total_runners IS NULL OR total_runners = 0 THEN 'unknown'
        WHEN barrier::float / total_runners <= 0.33 THEN 'inner'
        WHEN barrier::float / total_runners <= 0.66 THEN 'middle' 
        ELSE 'outer'
    END WHERE barrier IS NOT NULL;
    
    RAISE NOTICE 'Phase 1 complete in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- PHASE 2: CAREER & FORM STATS (into shadow table)
    -- ========================================================================
    UPDATE shadow_rebuild_progress SET current_phase = 'Phase 2: Career Stats', phase_number = 2 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Horse career stats with anti-leakage
    WITH hc AS (
        SELECT race_id, horse_location_slug,
            COUNT(*) OVER w - 1 as prior_races,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w as wins,
            SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w as places,
            LAG(race_date) OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id) as prev_date
        FROM race_training_dataset_shadow
        WINDOW w AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    )
    UPDATE race_training_dataset_shadow rtd SET 
        total_races = COALESCE(hc.prior_races, 0),
        wins = COALESCE(hc.wins, 0),
        places = COALESCE(hc.places, 0),
        win_percentage = CASE WHEN COALESCE(hc.prior_races, 0) > 0 THEN ROUND(hc.wins::numeric / hc.prior_races * 100, 2) ELSE 0 END,
        place_percentage = CASE WHEN COALESCE(hc.prior_races, 0) > 0 THEN ROUND(hc.places::numeric / hc.prior_races * 100, 2) ELSE 0 END,
        days_since_last_race = CASE WHEN hc.prev_date IS NOT NULL THEN (rtd.race_date - hc.prev_date)::integer ELSE NULL END
    FROM hc WHERE rtd.race_id = hc.race_id AND rtd.horse_location_slug = hc.horse_location_slug;
    
    -- Last 5 race stats
    WITH l5 AS (
        SELECT race_id, horse_location_slug,
            COUNT(*) OVER w as cnt,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w as l5_wins,
            SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w as l5_places,
            AVG(final_position) OVER w as l5_avg_pos
        FROM race_training_dataset_shadow
        WINDOW w AS (PARTITION BY horse_location_slug ORDER BY race_date, race_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
    ),
    calc AS (
        SELECT race_id, horse_location_slug, LEAST(cnt, 5) as used,
            CASE WHEN cnt > 0 THEN ROUND(l5_wins::numeric / LEAST(cnt, 5) * 100, 2) ELSE 0 END as win_rate,
            CASE WHEN cnt > 0 THEN ROUND(l5_places::numeric / LEAST(cnt, 5) * 100, 2) ELSE 0 END as place_rate,
            l5_avg_pos as avg_pos
        FROM l5
    )
    UPDATE race_training_dataset_shadow rtd SET 
        last_5_avg_position = ROUND(calc.avg_pos, 2),
        last_5_win_rate = calc.win_rate,
        last_5_place_rate = calc.place_rate,
        last_5_races_used = calc.used
    FROM calc WHERE rtd.race_id = calc.race_id AND rtd.horse_location_slug = calc.horse_location_slug;
    
    -- Form recency and momentum
    UPDATE race_training_dataset_shadow SET 
        form_recency_score = CASE
            WHEN days_since_last_race IS NULL THEN 0.5 
            WHEN days_since_last_race <= 14 THEN 1.0
            WHEN days_since_last_race <= 28 THEN 0.9 
            WHEN days_since_last_race <= 60 THEN 0.6 
            ELSE 0.3
        END,
        form_momentum = CASE
            WHEN last_5_avg_position IS NULL THEN 0
            WHEN last_5_avg_position <= 2 THEN 1.0
            WHEN last_5_avg_position <= 4 THEN 0.7
            WHEN last_5_avg_position <= 6 THEN 0.4
            WHEN last_5_avg_position <= 10 THEN 0.2
            ELSE 0.1
        END;
    
    -- ELO ratings
    UPDATE race_training_dataset_shadow SET 
        horse_elo = ROUND(LEAST(GREATEST(1500 + COALESCE(win_percentage, 0) * 5 + LEAST(COALESCE(total_races, 0), 50) * 2, 1200), 2000), 0),
        is_elo_default = (total_races = 0 OR total_races IS NULL);
    
    -- Jockey stats
    WITH js AS (
        SELECT race_id, jockey_location_slug,
            COUNT(*) OVER w as rides,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w as wins,
            SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w as places
        FROM race_training_dataset_shadow
        WINDOW w AS (PARTITION BY jockey_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    )
    UPDATE race_training_dataset_shadow rtd SET 
        jockey_win_rate = CASE WHEN COALESCE(js.rides, 0) > 0 THEN ROUND(js.wins::numeric / js.rides, 4) ELSE 0 END,
        jockey_place_rate = CASE WHEN COALESCE(js.rides, 0) > 0 THEN ROUND(js.places::numeric / js.rides, 4) ELSE 0 END,
        jockey_total_rides = COALESCE(js.rides, 0)
    FROM js WHERE rtd.race_id = js.race_id AND rtd.jockey_location_slug = js.jockey_location_slug;
    
    -- Trainer stats
    WITH ts AS (
        SELECT race_id, trainer_location_slug,
            COUNT(*) OVER w as runners,
            SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) OVER w as wins,
            SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) OVER w as places
        FROM race_training_dataset_shadow
        WINDOW w AS (PARTITION BY trainer_location_slug ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    )
    UPDATE race_training_dataset_shadow rtd SET 
        trainer_win_rate = CASE WHEN COALESCE(ts.runners, 0) > 0 THEN ROUND(ts.wins::numeric / ts.runners, 4) ELSE 0 END,
        trainer_place_rate = CASE WHEN COALESCE(ts.runners, 0) > 0 THEN ROUND(ts.places::numeric / ts.runners, 4) ELSE 0 END,
        trainer_total_runners = COALESCE(ts.runners, 0)
    FROM ts WHERE rtd.race_id = ts.race_id AND rtd.trainer_location_slug = ts.trainer_location_slug;
    
    -- Cross-region flags
    WITH mr AS (SELECT horse_slug FROM race_training_dataset_shadow GROUP BY horse_slug HAVING COUNT(DISTINCT location) > 1)
    UPDATE race_training_dataset_shadow rtd SET is_cross_region_horse = TRUE FROM mr WHERE rtd.horse_slug = mr.horse_slug;
    UPDATE race_training_dataset_shadow SET is_cross_region_horse = FALSE WHERE is_cross_region_horse IS NULL;
    UPDATE race_training_dataset_shadow SET never_placed_flag = (total_races > 0 AND places = 0);
    
    RAISE NOTICE 'Phase 2 complete in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- PHASE 3: ADVANCED FEATURES (into shadow table)
    -- ========================================================================
    UPDATE shadow_rebuild_progress SET current_phase = 'Phase 3: Advanced Features', phase_number = 3 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Sectional positions from race_results
    UPDATE race_training_dataset_shadow rtd SET 
        position_800m = rr.position_800m, 
        position_400m = rr.position_400m
    FROM race_results rr WHERE rtd.race_id = rr.race_id AND rtd.horse_slug = rr.horse_slug;
    
    UPDATE race_training_dataset_shadow SET 
        pos_improvement_800_finish = CASE 
            WHEN position_800m IS NOT NULL AND final_position IS NOT NULL 
            THEN position_800m - final_position 
            ELSE NULL 
        END;
    
    -- Running style classification
    UPDATE race_training_dataset_shadow SET running_style = CASE
        WHEN position_800m IS NULL THEN 'unknown'
        WHEN position_800m <= 2 THEN 'leader'
        WHEN position_800m <= 4 THEN 'stalker'
        WHEN position_800m <= 8 THEN 'midfield'
        ELSE 'closer'
    END;
    
    -- Class rating
    UPDATE race_training_dataset_shadow SET class_rating = CASE
        WHEN race_class ILIKE '%group 1%' THEN 100 
        WHEN race_class ILIKE '%group 2%' THEN 90
        WHEN race_class ILIKE '%group 3%' THEN 80 
        WHEN race_class ILIKE '%listed%' THEN 70
        WHEN track_category = 'Metro' THEN 45 
        WHEN track_category = 'Provincial' THEN 35 
        ELSE 25
    END;
    
    -- Class level numeric for stepping up/down
    UPDATE race_training_dataset_shadow SET class_level_numeric = CASE
        WHEN race_class ILIKE '%group 1%' OR race_class ILIKE '%G1%' THEN 10
        WHEN race_class ILIKE '%group 2%' OR race_class ILIKE '%G2%' THEN 9
        WHEN race_class ILIKE '%group 3%' OR race_class ILIKE '%G3%' THEN 8
        WHEN race_class ILIKE '%listed%' THEN 7
        WHEN race_class ILIKE '%open%' THEN 6
        WHEN race_class ILIKE '%benchmark%' AND race_class ~* '(90|95|100)' THEN 5
        WHEN race_class ILIKE '%benchmark%' AND race_class ~* '(70|75|80|85)' THEN 4
        WHEN race_class ILIKE '%benchmark%' THEN 3
        WHEN race_class ILIKE '%maiden%' THEN 2
        ELSE 3
    END;
    
    -- Stepping up/down calculations
    WITH prev_class AS (
        SELECT 
            race_id, horse_location_slug,
            LAG(class_level_numeric) OVER (PARTITION BY horse_location_slug ORDER BY race_date, race_id) as prev_class_level
        FROM race_training_dataset_shadow
    )
    UPDATE race_training_dataset_shadow rtd SET 
        is_stepping_up = (pc.prev_class_level IS NOT NULL AND rtd.class_level_numeric > pc.prev_class_level),
        is_stepping_down = (pc.prev_class_level IS NOT NULL AND rtd.class_level_numeric < pc.prev_class_level)
    FROM prev_class pc 
    WHERE rtd.race_id = pc.race_id AND rtd.horse_location_slug = pc.horse_location_slug;
    
    -- Field percentiles
    WITH rp AS (
        SELECT race_id, horse_slug,
            PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY horse_elo) as elo_pct,
            PERCENT_RANK() OVER (PARTITION BY race_id ORDER BY win_odds) as odds_pct,
            RANK() OVER (PARTITION BY race_id ORDER BY win_odds) as odds_rank
        FROM race_training_dataset_shadow
    )
    UPDATE race_training_dataset_shadow rtd SET 
        elo_percentile_in_race = ROUND(rp.elo_pct::numeric, 4),
        odds_percentile_in_race = ROUND(rp.odds_pct::numeric, 4),
        is_favorite = (rp.odds_rank = 1)
    FROM rp WHERE rtd.race_id = rp.race_id AND rtd.horse_slug = rp.horse_slug;
    
    -- Odds features
    UPDATE race_training_dataset_shadow SET 
        odds_implied_probability = CASE WHEN win_odds > 0 THEN ROUND(1.0 / win_odds, 4) ELSE NULL END,
        is_longshot = (win_odds > 20);
    
    -- Distance experience
    WITH dist_exp AS (
        SELECT race_id, horse_location_slug,
            COUNT(*) OVER (PARTITION BY horse_location_slug, distance_range ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as races_at_dist
        FROM race_training_dataset_shadow
    )
    UPDATE race_training_dataset_shadow rtd SET races_at_distance = COALESCE(de.races_at_dist, 0)
    FROM dist_exp de WHERE rtd.race_id = de.race_id AND rtd.horse_location_slug = de.horse_location_slug;
    
    -- Track experience
    WITH track_exp AS (
        SELECT race_id, horse_location_slug,
            COUNT(*) OVER (PARTITION BY horse_location_slug, track_name ORDER BY race_date, race_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as races_at_trk
        FROM race_training_dataset_shadow
    )
    UPDATE race_training_dataset_shadow rtd SET races_at_track = COALESCE(te.races_at_trk, 0)
    FROM track_exp te WHERE rtd.race_id = te.race_id AND rtd.horse_location_slug = te.horse_location_slug;
    
    RAISE NOTICE 'Phase 3 complete in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- PHASE 4: INTERACTIONS & FINALIZE (into shadow table)
    -- ========================================================================
    UPDATE shadow_rebuild_progress SET current_phase = 'Phase 4: Interactions', phase_number = 4 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Key interactions
    UPDATE race_training_dataset_shadow SET 
        elo_x_jockey_win_rate = ROUND(COALESCE(horse_elo, 1500)::numeric / 1500 * COALESCE(jockey_win_rate, 0), 4),
        elo_x_trainer_win_rate = ROUND(COALESCE(horse_elo, 1500)::numeric / 1500 * COALESCE(trainer_win_rate, 0), 4),
        form_x_class = ROUND(COALESCE(form_momentum, 0.5) * COALESCE(class_rating, 50)::numeric / 100, 4),
        barrier_x_total_runners = ROUND(CASE WHEN COALESCE(total_runners, 1) > 0 THEN COALESCE(barrier, 1)::numeric / total_runners ELSE 0.5 END, 4);
    
    -- Anti-leakage flags
    UPDATE race_training_dataset_shadow SET 
        extreme_staleness_flag = (days_since_last_race > 180),
        low_sample_flag = (total_races < 3);
    
    -- Create indexes on shadow table for validation queries
    CREATE INDEX IF NOT EXISTS idx_shadow_horse_slug ON race_training_dataset_shadow(horse_slug);
    CREATE INDEX IF NOT EXISTS idx_shadow_race_date ON race_training_dataset_shadow(race_date);
    CREATE INDEX IF NOT EXISTS idx_shadow_track_name ON race_training_dataset_shadow(track_name);
    CREATE INDEX IF NOT EXISTS idx_shadow_horse_location_slug ON race_training_dataset_shadow(horse_location_slug);
    CREATE INDEX IF NOT EXISTS idx_shadow_race_id ON race_training_dataset_shadow(race_id);
    
    RAISE NOTICE 'Phase 4 complete in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- PHASE 5: ATOMIC SWAP
    -- ========================================================================
    UPDATE shadow_rebuild_progress SET current_phase = 'Phase 5: Atomic Swap', phase_number = 5 WHERE run_id = v_run_id;
    v_phase_start := clock_timestamp();
    
    -- Validate shadow table before swap
    SELECT COUNT(*) INTO v_records FROM race_training_dataset_shadow;
    
    IF v_records < 100000 THEN
        RAISE EXCEPTION 'Shadow table has only % records, expected 400k+. Aborting swap.', v_records;
    END IF;
    
    -- ATOMIC SWAP: Rename tables
    -- 1. Rename production to old
    ALTER TABLE race_training_dataset RENAME TO race_training_dataset_old;
    
    -- 2. Rename shadow to production
    ALTER TABLE race_training_dataset_shadow RENAME TO race_training_dataset;
    
    -- 3. Rename constraint to match new table name
    ALTER TABLE race_training_dataset 
    RENAME CONSTRAINT race_training_dataset_shadow_race_id_horse_slug_key 
    TO race_training_dataset_race_id_horse_slug_key;
    
    -- 4. Rename indexes to match new table name
    ALTER INDEX idx_shadow_horse_slug RENAME TO idx_rtd_horse_slug;
    ALTER INDEX idx_shadow_race_date RENAME TO idx_rtd_race_date;
    ALTER INDEX idx_shadow_track_name RENAME TO idx_rtd_track_name;
    ALTER INDEX idx_shadow_horse_location_slug RENAME TO idx_rtd_horse_location_slug;
    ALTER INDEX idx_shadow_race_id RENAME TO idx_rtd_race_id;
    
    RAISE NOTICE 'Phase 5 complete: Tables swapped in %', clock_timestamp() - v_phase_start;
    
    -- ========================================================================
    -- COMPLETE
    -- ========================================================================
    UPDATE shadow_rebuild_progress SET 
        status = 'swapped',
        current_phase = 'Complete - Tables Swapped',
        completed_at = NOW(),
        records_processed = v_records
    WHERE run_id = v_run_id;
    
    v_result := json_build_object(
        'status', 'success',
        'run_id', v_run_id,
        'records', v_records,
        'duration_seconds', EXTRACT(EPOCH FROM (NOW() - v_start_time))::integer,
        'note', 'Old table preserved as race_training_dataset_old. Drop when ready.'
    );
    
    RETURN v_result;
    
EXCEPTION WHEN OTHERS THEN
    -- Attempt to rollback swap if it partially completed
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'race_training_dataset_old') 
           AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'race_training_dataset') THEN
            ALTER TABLE race_training_dataset_old RENAME TO race_training_dataset;
            RAISE NOTICE 'Rolled back table swap due to error';
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not rollback: %', SQLERRM;
    END;
    
    UPDATE shadow_rebuild_progress SET 
        status = 'failed',
        error_message = SQLERRM,
        completed_at = NOW()
    WHERE run_id = v_run_id;
    
    RETURN json_build_object('status', 'error', 'message', SQLERRM, 'run_id', v_run_id);
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- CLEANUP FUNCTION: Drop old table after validating new one works
-- ============================================================================
CREATE OR REPLACE FUNCTION cleanup_old_training_table()
RETURNS TEXT AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'race_training_dataset_old') THEN
        DROP TABLE race_training_dataset_old;
        RETURN 'Dropped race_training_dataset_old successfully';
    ELSE
        RETURN 'No old table to drop';
    END IF;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- ROLLBACK FUNCTION: Swap back to old table if issues found
-- ============================================================================
CREATE OR REPLACE FUNCTION rollback_training_rebuild()
RETURNS TEXT AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'race_training_dataset_old') THEN
        RETURN 'ERROR: No old table available for rollback';
    END IF;
    
    -- Swap back
    ALTER TABLE race_training_dataset RENAME TO race_training_dataset_failed;
    ALTER TABLE race_training_dataset_old RENAME TO race_training_dataset;
    
    RETURN 'Rolled back to previous version. Failed rebuild preserved as race_training_dataset_failed';
END;
$$ LANGUAGE plpgsql;


-- Grant permissions
GRANT EXECUTE ON FUNCTION run_training_shadow_rebuild() TO service_role;
GRANT EXECUTE ON FUNCTION run_training_shadow_rebuild() TO authenticated;
GRANT EXECUTE ON FUNCTION cleanup_old_training_table() TO service_role;
GRANT EXECUTE ON FUNCTION cleanup_old_training_table() TO authenticated;
GRANT EXECUTE ON FUNCTION rollback_training_rebuild() TO service_role;
GRANT EXECUTE ON FUNCTION rollback_training_rebuild() TO authenticated;


DO $$ BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'SHADOW REBUILD FUNCTIONS CREATED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    RAISE NOTICE 'USAGE:';
    RAISE NOTICE '  1. Start rebuild (runs in background, production unaffected):';
    RAISE NOTICE '     SELECT run_training_shadow_rebuild();';
    RAISE NOTICE '';
    RAISE NOTICE '  2. After validating new data works, cleanup old table:';
    RAISE NOTICE '     SELECT cleanup_old_training_table();';
    RAISE NOTICE '';
    RAISE NOTICE '  3. If issues found, rollback to previous version:';
    RAISE NOTICE '     SELECT rollback_training_rebuild();';
    RAISE NOTICE '';
    RAISE NOTICE 'BENEFITS:';
    RAISE NOTICE '  - Zero production downtime during rebuild';
    RAISE NOTICE '  - Production queries run at full speed';
    RAISE NOTICE '  - Atomic swap at end (milliseconds)';
    RAISE NOTICE '  - Easy rollback if issues found';
    RAISE NOTICE '============================================================';
END $$;
