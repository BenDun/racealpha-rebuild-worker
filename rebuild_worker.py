"""
RaceAlpha Training Dataset Rebuild Worker - V2
===============================================
Executes rebuild_v2 SQL scripts directly on Supabase PostgreSQL.

Run: python rebuild_worker.py
Schedule: Weekly Sunday 2am AEST (via Railway cron)

Phases (from rebuild_v2):
  1. Base Rebuild (01_base_rebuild.sql)
  2. Career & Form Stats (02_career_form_stats.sql)
  3. Advanced Features (03_advanced_features.sql)
  4. Interactions & Validation (04_interactions_validation.sql)
  5. Sectional Backfill (05_sectional_backfill.sql)
  6. Sectional Pattern Features (06_sectional_pattern_features.sql)
  7. ELO Rebuild & Sync (07_elo_rebuild_and_sync.sql)
  8. Current Form Views (06_current_form_views.sql)
  9. Validation & Cleanup

Approach: Runs native PostgreSQL SQL directly on Supabase via psycopg2.
SQL files stored in sql/ directory within this repo.
"""

import os
import sys
import time
import uuid
import re
import requests
import psycopg2
import threading
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://pabqrixgzcnqttrkwkil.supabase.co")
DATABASE_URL = os.getenv("DATABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
TRIGGERED_BY = os.getenv("TRIGGERED_BY", "manual")

# DRY_RUN mode - creates test table without affecting production
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
TEST_TABLE_NAME = "race_training_dataset_test"
PROD_TABLE_NAME = "race_training_dataset"

# SQL files directory
SQL_DIR = Path(__file__).parent / "sql"

# Total phases for progress tracking
TOTAL_PHASES = 11

# Global run ID
RUN_ID = f"rebuild_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Progress bar settings
PROGRESS_BAR_WIDTH = 40
SPINNER_CHARS = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']


class ProgressSpinner:
    """Background spinner with elapsed time for long-running operations"""
    def __init__(self, message: str):
        self.message = message
        self.running = False
        self.thread = None
        self.start_time = None
        
    def _spin(self):
        idx = 0
        while self.running:
            elapsed = time.time() - self.start_time
            mins, secs = divmod(int(elapsed), 60)
            spinner = SPINNER_CHARS[idx % len(SPINNER_CHARS)]
            status = f"\r{spinner} {self.message} [{mins:02d}:{secs:02d}]"
            sys.stdout.write(status)
            sys.stdout.flush()
            idx += 1
            time.sleep(0.1)
    
    def start(self):
        self.running = True
        self.start_time = time.time()
        self.thread = threading.Thread(target=self._spin, daemon=True)
        self.thread.start()
    
    def stop(self, final_message: str = None):
        self.running = False
        if self.thread:
            self.thread.join(timeout=0.5)
        elapsed = time.time() - self.start_time
        mins, secs = divmod(int(elapsed), 60)
        if final_message:
            sys.stdout.write(f"\r✓ {final_message} [{mins:02d}:{secs:02d}]\n")
        else:
            sys.stdout.write(f"\r✓ {self.message} [{mins:02d}:{secs:02d}]\n")
        sys.stdout.flush()


def progress_bar(current: int, total: int, prefix: str = "", suffix: str = "") -> str:
    """Generate a progress bar string"""
    percent = current / total if total > 0 else 0
    filled = int(PROGRESS_BAR_WIDTH * percent)
    bar = '█' * filled + '░' * (PROGRESS_BAR_WIDTH - filled)
    return f"{prefix} [{bar}] {current}/{total} {suffix}"


def print_phase_progress(phase_num: int, total_phases: int, phase_name: str):
    """Print overall phase progress"""
    bar = progress_bar(phase_num, total_phases, prefix="Overall Progress:", suffix=f"- Phase {phase_num}: {phase_name}")
    print(f"\n{bar}", flush=True)


def log(message: str, level: str = "INFO"):
    """Timestamped logging"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}", flush=True)


def update_status(status: str, phase: str = None, phase_number: int = 0,
                  message: str = None, rows_processed: int = 0,
                  error_message: str = None, duration_seconds: float = None):
    """Update rebuild status in Supabase"""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return

    try:
        url = f"{SUPABASE_URL}/rest/v1/rebuild_status?on_conflict=run_id"
        headers = {
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates"
        }

        data = {
            "run_id": RUN_ID,
            "status": status,
            "phase": phase,
            "phase_number": phase_number,
            "total_phases": TOTAL_PHASES,
            "message": message,
            "rows_processed": rows_processed,
            "updated_at": datetime.utcnow().isoformat(),
            "triggered_by": TRIGGERED_BY,
        }

        if error_message:
            data["error_message"] = error_message
        if duration_seconds:
            data["duration_seconds"] = round(duration_seconds, 2)
        if status == "completed" or status == "failed":
            data["completed_at"] = datetime.utcnow().isoformat()

        response = requests.post(url, headers=headers, json=data)
        if response.status_code not in [200, 201]:
            log(f"Warning: Failed to update status: {response.text}", "WARN")
    except Exception as e:
        log(f"Warning: Could not update status: {e}", "WARN")


def get_db_connection():
    """Get psycopg2 connection to Supabase with keepalive settings"""
    # Connection options for long-running queries
    connect_options = {
        "keepalives": 1,
        "keepalives_idle": 30,      # Send keepalive after 30s idle
        "keepalives_interval": 10,  # Retry every 10s
        "keepalives_count": 5,      # Give up after 5 retries
        "connect_timeout": 30,
    }
    
    if DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL, **connect_options)
    else:
        db_host = os.getenv("DB_HOST", "db.pabqrixgzcnqttrkwkil.supabase.co")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "postgres")
        db_user = os.getenv("DB_USER", "postgres")
        db_pass = SUPABASE_SERVICE_KEY or os.getenv("DB_PASSWORD", "")

        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_pass,
            **connect_options
        )

    conn.autocommit = True
    return conn


def read_sql_file(filename: str) -> str:
    """Read SQL file from sql/ directory"""
    sql_path = SQL_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read()


def replace_table_name(sql: str, from_table: str, to_table: str) -> str:
    """Replace table name references in SQL for DRY_RUN mode."""
    # Simple global replacement - works for most cases
    result = sql.replace(from_table, to_table)
    # Also replace index naming patterns
    result = result.replace("idx_rtd_", "idx_rtdtest_")
    return result


def execute_sql(conn, sql: str, description: str = None) -> int:
    """Execute SQL and return rows affected"""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.rowcount if cursor.rowcount > 0 else 0
        return rows
    except Exception as e:
        log(f"SQL Error in {description}: {e}", "ERROR")
        raise
    finally:
        cursor.close()


def query_one(conn, sql: str):
    """Execute a query and return single result"""
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None


def query_count(conn, table_name: str) -> int:
    """Get row count of a table"""
    return query_one(conn, f"SELECT COUNT(*) FROM {table_name}") or 0


def run_phase(conn, phase_num: int, sql_file: str, description: str, target_table: str, total_phases: int = TOTAL_PHASES) -> dict:
    """Execute a phase SQL file with progress indicator"""
    phase_start = time.time()
    
    # Print phase header and progress bar
    print_phase_progress(phase_num, total_phases, description)
    log(f"{'=' * 60}")
    log(f"PHASE {phase_num}: {description.upper()}")
    log(f"{'=' * 60}")
    update_status("transforming", f"Phase {phase_num}: {description}", phase_num, f"Running {sql_file}...")

    try:
        sql = read_sql_file(sql_file)
        log(f"Loaded {sql_file} ({len(sql):,} bytes)")

        # In DRY_RUN mode, replace table references
        if DRY_RUN:
            sql = replace_table_name(sql, PROD_TABLE_NAME, target_table)
            log(f"DRY_RUN: Replaced '{PROD_TABLE_NAME}' -> '{target_table}'")

        # Start spinner for SQL execution
        spinner = ProgressSpinner(f"Executing {sql_file}")
        spinner.start()
        
        # Execute the SQL
        try:
            rows = execute_sql(conn, sql, description)
        finally:
            spinner.stop(f"Executed {sql_file}")

        duration = time.time() - phase_start

        # Get current row count
        row_count = query_count(conn, target_table)
        
        # Print success summary
        mins, secs = divmod(int(duration), 60)
        log(f"✅ Phase {phase_num} complete in {mins}m {secs}s ({row_count:,} rows)")

        update_status("transforming", f"Phase {phase_num}: {description}", phase_num,
                      f"Complete: {row_count:,} rows", row_count, duration_seconds=duration)

        return {"phase": phase_num, "duration": duration, "rows": row_count, "success": True}

    except Exception as e:
        duration = time.time() - phase_start
        mins, secs = divmod(int(duration), 60)
        log(f"❌ Phase {phase_num} failed after {mins}m {secs}s: {e}", "ERROR")
        update_status("failed", f"Phase {phase_num}: {description}", phase_num,
                      error_message=str(e), duration_seconds=duration)
        return {"phase": phase_num, "duration": duration, "error": str(e), "success": False}


def setup_test_table(conn) -> int:
    """Create test table from production for DRY_RUN mode"""
    log("")
    log("=" * 60)
    log("DRY_RUN: Setting up test table")
    log("=" * 60)

    cursor = conn.cursor()

    # Drop existing test table
    cursor.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_NAME} CASCADE")
    log(f"Dropped existing {TEST_TABLE_NAME} (if any)")

    # Create test table as copy of production (2020+ data only)
    log(f"Creating {TEST_TABLE_NAME} from production (2020+ data)...")
    cursor.execute(f"""
        CREATE TABLE {TEST_TABLE_NAME} AS
        SELECT * FROM {PROD_TABLE_NAME}
        WHERE race_date >= '2020-01-01'
    """)

    test_count = query_count(conn, TEST_TABLE_NAME)
    log(f"Created {TEST_TABLE_NAME} with {test_count:,} rows")

    # Add unique constraint for upsert operations
    cursor.execute(f"""
        ALTER TABLE {TEST_TABLE_NAME}
        ADD CONSTRAINT {TEST_TABLE_NAME}_race_horse_key
        UNIQUE (race_id, horse_slug)
    """)
    log("Added unique constraint (race_id, horse_slug)")

    # Create essential indexes
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TEST_TABLE_NAME}_race_id ON {TEST_TABLE_NAME}(race_id)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TEST_TABLE_NAME}_horse_slug ON {TEST_TABLE_NAME}(horse_slug)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TEST_TABLE_NAME}_race_date ON {TEST_TABLE_NAME}(race_date)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TEST_TABLE_NAME}_horse_loc ON {TEST_TABLE_NAME}(horse_location_slug)")
    log("Created indexes")

    cursor.close()
    return test_count


def run_rebuild():
    """Main rebuild function"""
    start_time = time.time()
    log("=" * 60)
    log("RACEALPHA TRAINING DATASET REBUILD V2 - STARTING")
    log(f"Run ID: {RUN_ID}")
    log(f"DRY_RUN: {DRY_RUN}")
    if DRY_RUN:
        log(f"DRY RUN MODE: Operating on {TEST_TABLE_NAME}")
    log("=" * 60)

    # Determine target table
    target_table = TEST_TABLE_NAME if DRY_RUN else PROD_TABLE_NAME

    # Verify SQL files exist
    required_sql_files = [
        ("01_base_rebuild.sql", "Base Rebuild"),
        ("02_career_form_stats.sql", "Career & Form Stats"),
        ("03_advanced_features.sql", "Advanced Features"),
        ("04_interactions_validation.sql", "Interactions & Validation"),
        ("05_sectional_backfill.sql", "Sectional Backfill"),
        ("06_sectional_pattern_features.sql", "Sectional Pattern Features"),
        ("07_current_form_views.sql", "Current Form Views"),
        ("08_elo_rebuild_and_sync.sql", "ELO Rebuild & Sync"),
        ("08b_weather_features.sql", "Weather Track Preferences"),
        ("09_remove_leakage_columns.sql", "Remove Leakage Columns"),
    ]

    log(f"Checking SQL files in {SQL_DIR}...")
    missing_files = []
    for f, _ in required_sql_files:
        if not (SQL_DIR / f).exists():
            missing_files.append(f)

    if missing_files:
        log(f"Missing SQL files: {missing_files}", "ERROR")
        update_status("failed", "Initialization", 0, error_message=f"Missing SQL files: {missing_files}")
        return {"status": "error", "message": f"Missing SQL files: {missing_files}"}

    log(f"All {len(required_sql_files)} SQL files found")

    # Initial status update
    update_status("starting", "Initializing", 0, "Starting rebuild worker...")

    # Connect to Supabase
    try:
        log("Connecting to Supabase PostgreSQL...")
        conn = get_db_connection()
        log("Connected to Supabase")
        update_status("starting", "Initializing", 0, "Connected to Supabase")

        # Set session parameters for long-running queries
        cursor = conn.cursor()
        cursor.execute("SET statement_timeout = '0'")
        cursor.execute("SET lock_timeout = '0'")
        cursor.execute("SET work_mem = '512MB'")
        cursor.execute("SET maintenance_work_mem = '1GB'")
        cursor.close()
        log("Session parameters set (no timeouts, 512MB work_mem)")

    except Exception as e:
        log(f"Failed to connect to Supabase: {e}", "ERROR")
        update_status("failed", "Initializing", 0, error_message=str(e))
        return {"status": "error", "message": str(e)}

    # Setup test table for DRY_RUN
    if DRY_RUN:
        try:
            setup_test_table(conn)
        except Exception as e:
            log(f"Failed to setup test table: {e}", "ERROR")
            update_status("failed", "DRY_RUN Setup", 0, error_message=str(e))
            conn.close()
            return {"status": "error", "message": str(e)}

    # Execute phases
    results = []

    for i, (sql_file, description) in enumerate(required_sql_files, start=1):
        # Skip views in DRY_RUN (they reference production table)
        if DRY_RUN and "views" in sql_file.lower():
            log(f"Skipping Phase {i} ({description}) in DRY_RUN mode")
            results.append({"phase": i, "skipped": True, "success": True})
            continue

        result = run_phase(conn, i, sql_file, description, target_table)
        results.append(result)

        if not result["success"]:
            # For non-critical phases (ELO, Views), log warning and continue
            if i >= 7:
                log(f"Warning: Phase {i} failed but continuing...", "WARN")
            else:
                conn.close()
                return {"status": "error", "phase": i, "error": result.get("error")}

    # Final validation
    log("")
    log("=" * 60)
    log("PHASE 9: FINAL VALIDATION")
    log("=" * 60)
    update_status("validating", "Phase 9: Validation", 9, "Running final validation...")

    cursor = conn.cursor()

    # Get final counts and stats
    final_count = query_count(conn, target_table)

    cursor.execute(f"SELECT COUNT(DISTINCT horse_slug) FROM {target_table}")
    unique_horses = cursor.fetchone()[0]

    cursor.execute(f"SELECT MIN(race_date), MAX(race_date) FROM {target_table}")
    date_range = cursor.fetchone()

    cursor.execute(f"""
        SELECT
            ROUND(COUNT(*) FILTER (WHERE position_800m IS NOT NULL) * 100.0 / NULLIF(COUNT(*), 0), 1) as sectional_coverage,
            ROUND(COUNT(*) FILTER (WHERE running_style IS NOT NULL AND running_style != 'unknown') * 100.0 / NULLIF(COUNT(*), 0), 1) as running_style_coverage
        FROM {target_table}
    """)
    coverage = cursor.fetchone()

    # Count columns
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = '{target_table}'
    """)
    column_count = cursor.fetchone()[0]

    cursor.close()

    log(f"Final row count: {final_count:,}")
    log(f"Unique horses: {unique_horses:,}")
    log(f"Date range: {date_range[0]} to {date_range[1]}")
    log(f"Sectional coverage: {coverage[0]}%")
    log(f"Running style coverage: {coverage[1]}%")
    log(f"Total columns: {column_count}")

    # Complete
    total_duration = time.time() - start_time
    total_mins, total_secs = divmod(int(total_duration), 60)

    # Print final summary with phase breakdown
    print("\n")
    print("=" * 60)
    print("🏁 REBUILD COMPLETE!")
    print("=" * 60)
    
    if DRY_RUN:
        print(f"📋 Mode: DRY RUN (test table: {TEST_TABLE_NAME})")
    else:
        print(f"📋 Mode: PRODUCTION")
    
    print(f"\n📊 Final Statistics:")
    print(f"   • Total rows:      {final_count:,}")
    print(f"   • Total columns:   {column_count}")
    print(f"   • Unique horses:   {unique_horses:,}")
    print(f"   • Date range:      {date_range[0]} to {date_range[1]}")
    print(f"   • Sectionals:      {coverage[0]}%")
    print(f"   • Running style:   {coverage[1]}%")
    
    print(f"\n⏱️  Phase Timing:")
    for r in results:
        if r.get("skipped"):
            print(f"   Phase {r['phase']:2d}: ⏭️  Skipped")
        elif r.get("success"):
            mins, secs = divmod(int(r.get('duration', 0)), 60)
            print(f"   Phase {r['phase']:2d}: ✅ {mins:2d}m {secs:02d}s ({r.get('rows', 0):,} rows)")
        else:
            print(f"   Phase {r['phase']:2d}: ❌ Failed - {r.get('error', 'Unknown')[:50]}")
    
    print(f"\n⏱️  Total Duration: {total_mins}m {total_secs}s")
    print(f"🔑 Run ID: {RUN_ID}")
    print("=" * 60)

    update_status("completed", "Complete", TOTAL_PHASES,
                  f"Rebuild complete: {final_count:,} rows, {column_count} columns",
                  final_count, duration_seconds=total_duration)

    conn.close()

    return {
        "status": "success",
        "dry_run": DRY_RUN,
        "table": target_table,
        "rows": final_count,
        "columns": column_count,
        "unique_horses": unique_horses,
        "date_range": [str(date_range[0]), str(date_range[1])],
        "sectional_coverage": coverage[0],
        "running_style_coverage": coverage[1],
        "duration_minutes": round(total_duration / 60, 1),
        "run_id": RUN_ID
    }


def run_test_mode():
    """Test mode - validates connection and SQL files without running rebuild"""
    log("=" * 60)
    log("TEST MODE - Validating Setup")
    log(f"Run ID: {RUN_ID}")
    log("=" * 60)

    # Check SQL files
    required_sql_files = [
        "01_base_rebuild.sql",
        "02_career_form_stats.sql",
        "03_advanced_features.sql",
        "04_interactions_validation.sql",
        "05_sectional_backfill.sql",
        "06_sectional_pattern_features.sql",
        "07_current_form_views.sql",
        "08_elo_rebuild_and_sync.sql",
        "08b_weather_features.sql",
        "09_remove_leakage_columns.sql",
    ]

    log(f"SQL Directory: {SQL_DIR}")
    all_found = True
    total_lines = 0

    for f in required_sql_files:
        path = SQL_DIR / f
        if path.exists():
            with open(path, 'r') as file:
                lines = len(file.readlines())
                total_lines += lines
            log(f"  {f} ({lines} lines)")
        else:
            log(f"  {f} MISSING", "ERROR")
            all_found = False

    if not all_found:
        log("")
        log("Some SQL files are missing!", "ERROR")
        return {"status": "error", "message": "Missing SQL files"}

    log(f"")
    log(f"Total SQL: {total_lines:,} lines")

    # Test database connection
    log("")
    log("Testing database connection...")
    try:
        conn = get_db_connection()

        # Get production table info
        prod_count = query_count(conn, PROD_TABLE_NAME)

        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = '{PROD_TABLE_NAME}'
        """)
        col_count = cursor.fetchone()[0]
        cursor.close()

        conn.close()

        log(f"Connected to Supabase")
        log(f"Production table: {prod_count:,} rows, {col_count} columns")

    except Exception as e:
        log(f"Connection failed: {e}", "ERROR")
        return {"status": "error", "message": str(e)}

    log("")
    log("All checks passed - ready for rebuild")

    return {"status": "success", "sql_lines": total_lines, "production_rows": prod_count, "production_columns": col_count}


if __name__ == "__main__":
    mode = os.getenv("MODE", "rebuild").lower()

    if mode == "test":
        result = run_test_mode()
    else:
        result = run_rebuild()

    print(f"\n{'='*60}")
    print(f"RESULT: {result}")
    print(f"{'='*60}")

    # Exit with appropriate code
    if result.get("status") == "error":
        sys.exit(1)
    sys.exit(0)
