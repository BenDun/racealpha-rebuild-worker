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

## Estimated Runtime

| Phase     | Description                   | Time           |
| --------- | ----------------------------- | -------------- |
| Extract   | Pull 3 tables from Supabase   | ~2-3 min       |
| Transform | Feature engineering in DuckDB | ~10-15 min     |
| Load      | Push to shadow table + swap   | ~3-5 min       |
| **Total** |                               | **~15-25 min** |

vs ~100-150 min running directly on Supabase!

## Features Calculated (140+)

- Career stats (wins, places, percentages)
- Last 5 race form
- ELO ratings (location-aware)
- Jockey/Trainer stats
- Running style classification
- Speed figures
- Class ratings
- Barrier analysis
- Interaction features
- Anti-leakage flags
