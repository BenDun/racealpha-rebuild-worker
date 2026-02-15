# Race Training Dataset Rebuild V2 (Streamlined)

## Overview

**7 scripts** instead of 18. Same 140+ features, ~60% faster execution.

| Phase | Script                           | Purpose                                                                  | Est. Time |
| ----- | -------------------------------- | ------------------------------------------------------------------------ | --------- |
| 1     | `01_base_rebuild.sql`            | Truncate, base insert, track properties, field size                      | 5-10 min  |
| 2     | `02_career_form_stats.sql`       | Career stats, last 5, ELO, jockey/trainer rates                          | 25-35 min |
| 3     | `03_advanced_features.sql`       | Running style, speed figures, class, sectionals                          | 15-20 min |
| 4     | `04_interactions_validation.sql` | Interaction features, barrier analysis, anti-leakage, validation         | 15-20 min |
| 5     | `05_sectional_backfill.sql`      | Backfill sectional positions from race_results_sectional_times (AU + HK) | 10-15 min |
| 6     | `06_current_form_views.sql`      | Materialized views for predictions (uses complete sectional data)        | 5-10 min  |
| 7     | `07_elo_rebuild_and_sync.sql`    | Rebuild horse_elo_ratings + sync horses table + refresh views (FINAL)    | 30-45 min |

**Total: ~100-150 minutes** (vs 140+ in v1)

## Quick Start

```bash
# Run in order via Supabase SQL Editor
01_base_rebuild.sql
02_career_form_stats.sql
03_advanced_features.sql
04_interactions_validation.sql
05_sectional_backfill.sql  # MUST run before 06 - fills position_800m/400m in race_results
06_current_form_views.sql  # Creates materialized views using complete sectional data
07_elo_rebuild_and_sync.sql # FINAL: Rebuild ELO + sync horses + refresh views
```

Or use the single RPC function:

```sql
SELECT run_training_rebuild_v2();
```

## What Changed From V1

### Consolidated Scripts

- **01-04** → **01**: Base rebuild + track properties + field size
- **05-06** → **02**: Career stats + last 5 + ELO + jockey/trainer
- **07-08** → **03**: Running style + speed figures + class rating + sectionals
- **09-13** → **04**: Interactions + barrier + anti-leakage + validation
- **14** → **05**: Current form views (unchanged, standalone)

### Optimizations

- Single UPDATE per phase (batch all column sets)
- Dropped indexes during bulk updates, recreated at end
- Combined related CTEs to reduce table scans
- Removed redundant backup tables (one master backup)
- Removed per-step validation (final validation only)

## Phase Details

### Phase 1: Base Rebuild (01_base_rebuild.sql)

- Creates backup of existing data
- Truncates and rebuilds from `race_results` + `races`
- Sets location-aware slugs (AU/HK/etc)
- Sets track properties (direction, category, type)
- Sets race class flags (maiden, handicap, age/sex restrictions)
- Calculates total_runners per race

### Phase 2: Career & Form Stats (02_career_form_stats.sql)

- Career stats with anti-leakage (total_races, wins, places, percentages)
- Days since last race
- Last 5 stats (win rate, place rate, avg position, form momentum)
- Horse ELO ratings (location-aware)
- Jockey stats (win rate, place rate)
- Trainer stats (win rate, place rate)
- Cross-region horse flags

### Phase 3: Advanced Features (03_advanced_features.sql)

- Sectional positions (800m, 400m, improvements)
- Running style classification (Leader/Stalker/Midfield/Closer)
- Closing ability scores
- Early speed scores
- Speed figures (raw time, normalized, best/avg)
- Class rating (1-100 scale)
- Field comparison percentiles
- Odds-based features
- Distance preferences
- Value indicators

### Phase 4: Interactions & Validation (04_interactions_validation.sql)

- ELO interactions (x jockey, x trainer, x weight)
- Barrier interactions (x runners, x distance, x direction)
- Form interactions (x freshness, x track)
- Running style interactions (x direction, x barrier)
- Barrier analysis (track-specific advantages)
- Anti-leakage flags (staleness, never placed)
- Model bias columns
- Final validation checks
- Recreate indexes

### Phase 5: Sectional Data Backfill (05_sectional_backfill.sql)

**CRITICAL**: This script backfills `position_800m` and `position_400m` from `race_results_sectional_times`.
**MUST RUN BEFORE Phase 6** - the current form views use this sectional data for running_style calculations.

- **Australian tracks**: Use 200m-interval checkpoints (position_200m, position_400m, position_600m, etc.)
- **Hong Kong tracks**: Use 400m-interval checkpoints (position_400m, position_800m, position_1200m, etc.)
- Maps checkpoint positions to "800m remaining" and "400m remaining" based on race distance
- Includes HK late-replacement matching by horse_number
- Syncs backfilled data to `race_training_dataset`
- Recalculates `running_style` based on updated positions

**Why this is needed**: Without this script, HK races would have NULL position_800m/position_400m
because HK sectional data uses different checkpoint intervals than Australian tracks.

### Phase 6: Current Form Views (06_current_form_views.sql)

Creates 5 materialized views using complete sectional data from Phase 5:

- `horse_current_form` (23 columns) - includes running_style calculated from position_800m
- `jockey_current_form` (11 columns)
- `trainer_current_form` (11 columns)
- `jockey_horse_connection_stats` (9 columns)
- `jockey_trainer_connection_stats` (9 columns)
- Refresh functions for daily updates

### Phase 7: ELO Rebuild & Entity Sync (07_elo_rebuild_and_sync.sql)

**FINAL SCRIPT**: Heavy operations that must run after all views are created.

- **ELO Rebuild**: Processes ALL races chronologically to build accurate ELO progression
  - K-Factor: 32 (base)
  - Win: +25.6 | 2nd: +12.8 | 3rd: +3.2 | Mid-field: -6.4 | Back: -16
  - Group 1 races: 1.5x multiplier
  - Large fields (14+): 1.2x multiplier
- **Horse Sync**: Ensures all horses in race_results exist in horses table
- **Refresh**: Updates horse_current_form with new ELO values

**Functions Created**:

- `rebuild_horse_elo_ratings_for_current_form()` - Full ELO rebuild (30-45 min)
- `sync_missing_horses_from_results()` - Sync horses table

**Why this is separate**: ELO rebuild is expensive and must run AFTER horse_current_form exists.

## Anti-Leakage Pattern

All career/form calculations use:

```sql
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
```

This ensures we only use data from **before** the current race.

## Location-Aware Partitioning

All stats partitioned by `horse_location_slug` to prevent cross-region contamination:

```sql
PARTITION BY horse_location_slug ORDER BY race_date, race_id
```

## Backup & Rollback

Phase 1 creates a single backup:

```sql
-- Rollback command:
TRUNCATE race_training_dataset;
INSERT INTO race_training_dataset SELECT * FROM _backup_training_rebuild_v2;
```

## Column Count

| Category           | Count    |
| ------------------ | -------- |
| Base Race Info     | 15       |
| Location           | 5        |
| Track Properties   | 6        |
| Career Stats       | 10       |
| Last 5 / Form      | 6        |
| ELO                | 2        |
| Jockey/Trainer     | 8        |
| Sectional          | 6        |
| Running Style      | 8        |
| Speed Figures      | 6        |
| Class              | 5        |
| Field Comparison   | 8        |
| Odds Features      | 8        |
| Value Indicators   | 10       |
| Interactions       | 20       |
| Barrier Analysis   | 5        |
| Anti-Leakage Flags | 8        |
| **Total**          | **140+** |
