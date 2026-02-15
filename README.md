# RaceAlpha Training Dataset Rebuild Worker

DuckDB-powered rebuild system for `race_training_dataset` ML training data.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Railway (Web UI)                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   FastAPI Dashboard                       │    │
│  │  http://your-railway-url.railway.app                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                           │                                      │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐    │
│  │  1. PULL     │   │ 2. TRANSFORM │   │  3. PUSH         │    │
│  │  ─────────   │   │  ──────────  │   │  ────────        │    │
│  │  Supabase →  │   │  DuckDB SQL  │   │  DuckDB →        │    │
│  │  DuckDB      │   │  Phases 1-10 │   │  Supabase        │    │
│  └──────────────┘   └──────────────┘   └──────────────────┘    │
│                           │                                      │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              Local Data Lake (DuckDB)                     │    │
│  │  /app/data_lake/racealpha.duckdb (~2GB)                   │    │
│  │  • races (200K rows)                                       │    │
│  │  • race_results (2.5M rows)                                │    │
│  │  • race_training_dataset (500K rows rebuilt)              │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Why This Architecture?

| Metric            | Old (Direct Supabase) | New (DuckDB Data Lake) |
| ----------------- | --------------------- | ---------------------- |
| **Rebuild Time**  | 2-3 hours             | 15-30 minutes          |
| **Supabase Load** | High (locks tables)   | Zero during rebuild    |
| **Cost**          | $50+/month (compute)  | $5/month (Railway)     |
| **Control**       | None (fire & forget)  | Full UI with rollback  |

## 📦 Files

```
racealpha-rebuild-worker/
├── web_ui.py              # FastAPI web dashboard
├── data_lake.py           # Pull data from Supabase to DuckDB
├── transform_runner.py    # Run SQL phases on DuckDB
├── push_to_supabase.py    # Push rebuilt data back
├── rebuild_worker.py      # Legacy CLI worker (still works)
├── sql/                   # SQL transformation files
│   ├── 01_base_rebuild.sql
│   ├── 02_career_form_stats.sql
│   ├── ...
│   └── 09_remove_leakage_columns.sql
└── data_lake/             # Local DuckDB storage (gitignored)
    └── racealpha.duckdb
```

## 🎮 Web UI

Access the dashboard at your Railway URL:

### Pipeline Steps

1. **📥 Pull** - Download tables from Supabase → DuckDB
2. **⚙️ Transform** - Run 10 SQL phases locally
3. **✅ Validate** - Check data quality
4. **📤 Push** - Upload to Supabase (with auto-backup)

### API Endpoints

```
GET  /              # Web dashboard
GET  /api/status    # System status
POST /api/pull      # Start data pull
POST /api/transform # Start transformations
GET  /api/validate  # Run validation
GET  /api/preview   # Preview push changes
POST /api/push      # Push to Supabase
POST /api/rollback  # Rollback to backup
GET  /api/logs      # View logs
WS   /ws/progress   # Real-time progress
```

## 🛠️ Railway Setup

1. Create Railway project
2. Connect this GitHub repo
3. Environment variables:
   ```
   DATABASE_URL=postgresql://postgres:xxx@db.xxx.supabase.co:5432/postgres
   SUPABASE_URL=https://xxx.supabase.co
   SUPABASE_SERVICE_KEY=eyJ...
   PORT=8000
   ```
4. Deploy - web UI auto-starts

## 💻 Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Copy env
cp .env.example .env

# Run web UI
python web_ui.py
# Open http://localhost:8000

# Or run CLI tools directly:
python data_lake.py status
python data_lake.py pull
python transform_runner.py run
python push_to_supabase.py preview
python push_to_supabase.py push-copy
```

## 🔄 Transformation Phases

| Phase | File                              | Description                        | Est. Time |
| ----- | --------------------------------- | ---------------------------------- | --------- |
| 1     | 01_base_rebuild.sql               | Base dataset from race_results     | 10 min    |
| 2     | 02_career_form_stats.sql          | Career stats, last 5, ELO          | 25 min    |
| 3     | 03_advanced_features.sql          | Sectional positions, running style | 15 min    |
| 4     | 04_interactions_validation.sql    | Feature interactions               | 10 min    |
| 5     | 05_sectional_backfill.sql         | Backfill missing sectionals        | 10 min    |
| 6     | 06_sectional_pattern_features.sql | Sectional patterns                 | 10 min    |
| 7     | 07_current_form_views.sql         | Form views (Supabase only)         | Skip      |
| 8     | 08_elo_rebuild_and_sync.sql       | ELO ratings rebuild                | 15 min    |
| 9     | 08b_weather_features.sql          | Weather preferences                | 5 min     |
| 10    | 09_remove_leakage_columns.sql     | Remove leakage                     | 2 min     |

## 🔒 Safety Features

- **Auto-backup**: Creates `_backup_training_push` before every push
- **Rollback**: One-click restore from backup
- **Validation**: Data quality checks before push
- **Column matching**: Only pushes matching columns

## 📊 Data Lake Benefits

- **Persistent storage**: DuckDB file survives restarts
- **Parquet export**: Can export for external analysis
- **Instant queries**: Sub-second analytics on 500K rows
- **No network latency**: All transformations local

## 🐛 Troubleshooting

### Pull fails

```bash
# Check Supabase connection
python data_lake.py status
```

### Transform fails

```bash
# Test specific phase
python transform_runner.py test 2
```

### Push fails

```bash
# Preview changes first
python push_to_supabase.py preview

# Rollback if needed
python push_to_supabase.py rollback
```
