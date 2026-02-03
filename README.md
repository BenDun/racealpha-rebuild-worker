# RaceAlpha Training Dataset Rebuild Worker

DuckDB-powered weekly rebuild of `race_training_dataset` for ML training.

## Why DuckDB?

- **10x faster** than running on Supabase directly
- **Zero production impact** - doesn't lock Supabase during rebuild
- **Cost efficient** - Railway only charges while running (~$0.30/rebuild)

## How It Works

```
1. Pull source tables from Supabase (races, race_results, sectionals)
2. Run all feature engineering in DuckDB (locally in Railway container)
3. Push final dataset back to Supabase shadow table
4. Atomic swap to production table
```

## Railway Setup

1. Create new Railway project
2. Connect this GitHub repo
3. Add environment variables:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_KEY`
   - `DATABASE_URL` (direct connection string)
4. Set up cron trigger: `0 16 * * 0` (Sunday 2am AEST = 4pm UTC Saturday)

## Manual Trigger

```bash
# Via Railway CLI
railway run python rebuild_worker.py

# Or trigger via Railway dashboard
```

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Copy env file
cp .env.example .env
# Edit .env with your credentials

# Run locally
python rebuild_worker.py
```

## Phases

| Phase     | Description                         | Time           |
| --------- | ----------------------------------- | -------------- |
| 1         | Extract source tables from Supabase | ~2-3 min       |
| 2         | Base rebuild (core fields)          | ~3-5 min       |
| 3         | Career & form stats (anti-leakage)  | ~10-15 min     |
| 4         | Advanced features                   | ~5-8 min       |
| 5         | Sectional backfill (800m/400m)      | ~3-5 min       |
| 6         | Export to Supabase + atomic swap    | ~3-5 min       |
| 7         | Refresh materialized views          | ~2-3 min       |
| **Total** |                                     | **~25-40 min** |

vs ~100-150 min running directly on Supabase!

## Features Calculated (140+)

- Career stats (wins, places, percentages)
- Last 5 race form + momentum
- ELO ratings (location-aware)
- Jockey/Trainer stats with anti-leakage
- Running style classification (leader/stalker/midfield/closer)
- Sectional positions (800m, 400m) backfilled from sectional_times
- Class ratings (Group 1-3, Listed, BM, etc.)
- Field percentiles (ELO, odds)
- Odds features (implied probability, favorite, longshot)
- Cross-region horse flags
- Distance and barrier categorization

## Materialized Views Refreshed

- `horse_current_form` (34 columns with sectional features)
- `jockey_current_form`
- `trainer_current_form`
- `jockey_horse_connection_stats`
- `jockey_trainer_connection_stats`

## Environment Variables

```bash
# Required
DATABASE_URL=postgresql://user:pass@host:5432/db
SUPABASE_SERVICE_KEY=your_service_role_key

# Optional
SUPABASE_URL=https://your-project.supabase.co
TRIGGERED_BY=manual|cron|api
TEST_MODE=false
```

## Test Mode

Run without rebuilding to test status API:

```bash
TEST_MODE=true python rebuild_worker.py
```
