-- ============================================================================
-- PHASE 3: VALIDATION + LEAKAGE COLUMN REMOVAL (DuckDB)
-- ============================================================================
-- Covers: Old 08_remove_leakage_columns + validation
-- Est. Time: < 1 minute
--
-- WHAT THIS DOES:
--   1. Removes all leakage columns (current-race data that wouldn't be available at prediction time)
--   2. Final column count + summary
--
-- LEAKAGE COLUMNS REMOVED:
--   IN-RACE: position_800m, position_400m, position_at_800, position_at_400, position_at_end
--   IN-RACE SECTIONALS: sectional_position_800m, sectional_position_400m, sectional_position_200m
--   IMPROVEMENTS: pos_improvement_800_finish, pos_improvement_400_finish, pos_improvement_800_400, best_late_improvement
--   POSITION CHANGES: position_change_800_400, position_change_400_finish
--   DERIVED: closing_ability_score, early_speed_score, sustained_run_score, closing_power_score
--   SPEED: speed_figure, speed_rating
--   RATINGS: early_speed_rating, finish_speed_rating
--
-- SAFE COLUMNS KEPT (verified as historical):
--   avg_early_position_800m, avg_mid_position_400m, avg_pos_200m, avg_pos_400m,
--   historical_late_improvement, avg_speed_figure, best_speed_figure, avg_late_improvement,
--   jockey_avg_position_20/50, trainer_avg_position_50/100,
--   fader_rate/count, closer_rate/count, front_rate/count, avg_improvement_from_400m,
--   horse_wet_track_win_rate, horse_dry_track_win_rate, pos_volatility_last5,
--   finishing_kick_consistency, early_speed_pct, strong_finish_pct, pos_volatility_800
-- ============================================================================

-- ============================================================================
-- STEP 1: KEEP OUTCOME/TARGET COLUMNS (needed for ML training)
-- final_position, margin, raw_time_seconds are TARGET variables, NOT leakage.
-- The model learns to predict these — they MUST stay in the training dataset.
-- ============================================================================
-- (no drops here — final_position, margin, raw_time_seconds are kept)

-- ============================================================================
-- STEP 2: DROP IN-RACE POSITION COLUMNS (current race sectionals)
-- ============================================================================
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS position_800m;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS position_400m;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS position_at_800;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS position_at_400;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS position_at_end;

-- ============================================================================
-- STEP 3: DROP SECTIONAL POSITION COLUMNS (current race from sectional_times)
-- ============================================================================
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS sectional_position_800m;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS sectional_position_400m;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS sectional_position_200m;

-- ============================================================================
-- STEP 4: DROP POSITION IMPROVEMENT COLUMNS (derived from current race)
-- ============================================================================
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS pos_improvement_800_finish;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS pos_improvement_400_finish;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS pos_improvement_800_400;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS best_late_improvement;

-- ============================================================================
-- STEP 5: DROP POSITION CHANGE COLUMNS (derived from current race sectionals)
-- ============================================================================
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS position_change_800_400;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS position_change_400_finish;

-- ============================================================================
-- STEP 6: DROP DERIVED SCORE COLUMNS (calculated from leaky features)
-- ============================================================================
-- NOTE (Feb 11, 2026): closing_ability_score, early_speed_score, sustained_run_score
-- are now KEPT. Phase2 Step 4 computes them from historical_avg_improvement,
-- avg_early_position_800m, avg_mid_position_400m (all windowed ROWS PRECEDING).
-- They are SAFE historical features.
-- ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS closing_ability_score;
-- ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS early_speed_score;
-- ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS sustained_run_score;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS closing_power_score;
-- finishing_kick_consistency KEPT — uses w_prior window (anti-leakage, prior races only)

-- ============================================================================
-- STEP 7: DROP CURRENT RACE SPEED COLUMNS
-- ============================================================================
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS speed_figure;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS speed_rating;
-- early_speed_pct KEPT — rewritten as historical (% of prior races in top 25% at 800m)
-- strong_finish_pct KEPT — rewritten as historical (% of prior races gaining positions 400m→finish)
-- pos_volatility_800 KEPT — rewritten as historical (avg |800m_pos - finish| in prior races)

-- ============================================================================
-- STEP 8: DROP CURRENT RACE SPEED RATINGS
-- ============================================================================
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS early_speed_rating;
ALTER TABLE race_training_dataset_new DROP COLUMN IF EXISTS finish_speed_rating;
