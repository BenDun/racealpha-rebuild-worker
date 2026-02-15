"""
RaceAlpha DuckDB Data Lake
==========================
Simple app to pull data from Supabase into a local DuckDB database.

Usage:
    python app.py pull          # Pull all tables from Supabase
    python app.py status        # Show what's in DuckDB
    python app.py clear         # Clear the local DuckDB
    python app.py serve         # Start web UI
"""

import os
import duckdb
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# Config
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", os.path.join(_SCRIPT_DIR, "racealpha.duckdb"))
SUPABASE_DB_URL = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")

# Tables to pull
TABLES = [
    "race_training_dataset",
    "races",
    "race_meetings",
    "race_results",
    "race_results_sectional_times",
    "races_derived_sectional_margins",
    "horse_elo_ratings",
]


def get_supabase_connection():
    """Connect to Supabase PostgreSQL"""
    if not SUPABASE_DB_URL:
        raise ValueError("SUPABASE_DB_URL not set in .env")
    
    conn = psycopg2.connect(
        SUPABASE_DB_URL,
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    return conn


def get_duckdb_connection():
    """Connect to local DuckDB"""
    return duckdb.connect(DUCKDB_PATH)


def pull_table(table_name: str) -> dict:
    """Pull a single table from Supabase into DuckDB"""
    print(f"\n📥 Pulling {table_name}...")
    
    # Connect to Supabase
    pg_conn = get_supabase_connection()
    pg_cursor = pg_conn.cursor()
    
    # Get row count first
    pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = pg_cursor.fetchone()[0]
    print(f"   Source rows: {total_rows:,}")
    
    # Get column info
    pg_cursor.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    columns = pg_cursor.fetchall()
    print(f"   Columns: {len(columns)}")
    
    # Fetch data in batches with progress bar
    batch_size = 50000
    all_rows = []
    column_names = None
    
    with tqdm(total=total_rows, desc="   Fetching", unit=" rows", ncols=80) as pbar:
        pg_cursor.execute(f"SELECT * FROM {table_name}")
        column_names = [desc[0] for desc in pg_cursor.description]
        
        while True:
            rows = pg_cursor.fetchmany(batch_size)
            if not rows:
                break
            all_rows.extend(rows)
            pbar.update(len(rows))
    
    pg_cursor.close()
    pg_conn.close()
    
    # Insert into DuckDB
    duck = get_duckdb_connection()
    
    # Drop existing table
    duck.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Create table and insert data
    if all_rows:
        import pandas as pd
        from decimal import Decimal
        
        print("   Loading into DuckDB...")
        
        # Step 1: Create DataFrame
        df = pd.DataFrame(all_rows, columns=column_names)
        
        # Step 2: Convert ALL object columns with Decimals to float in one go
        # This is faster than checking each column individually
        for col in df.select_dtypes(include=['object']).columns:
            try:
                # Try to convert to numeric - will handle Decimal and strings
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        
        # Step 3: Load into DuckDB
        duck.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        # Verify
        result = duck.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        loaded_rows = result[0]
        print(f"   ✅ Loaded {loaded_rows:,} rows into DuckDB")
    else:
        duck.execute(f"CREATE TABLE {table_name} (empty_placeholder INT)")
        loaded_rows = 0
        print(f"   ⚠️ Table is empty")
    
    duck.close()
    
    return {
        "table": table_name,
        "source_rows": total_rows,
        "loaded_rows": loaded_rows,
        "columns": len(columns),
    }


def pull_all():
    """Pull all configured tables"""
    print("=" * 60)
    print("🏇 RaceAlpha - Pull from Supabase to DuckDB")
    print("=" * 60)
    
    results = []
    for table in TABLES:
        try:
            result = pull_table(table)
            results.append(result)
        except Exception as e:
            print(f"   ❌ Error: {e}")
            results.append({"table": table, "error": str(e)})
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Summary")
    print("=" * 60)
    total_rows = 0
    for r in results:
        if "error" in r:
            print(f"   ❌ {r['table']}: {r['error']}")
        else:
            print(f"   ✅ {r['table']}: {r['loaded_rows']:,} rows")
            total_rows += r['loaded_rows']
    
    print(f"\n   Total: {total_rows:,} rows")
    
    # File size
    if os.path.exists(DUCKDB_PATH):
        size_mb = os.path.getsize(DUCKDB_PATH) / (1024 * 1024)
        print(f"   DuckDB file: {size_mb:.1f} MB")
    
    return results


def show_status():
    """Show what's currently in DuckDB"""
    print("=" * 60)
    print("🦆 DuckDB Status")
    print("=" * 60)
    
    if not os.path.exists(DUCKDB_PATH):
        print("   No DuckDB file found. Run 'pull' first.")
        return
    
    duck = get_duckdb_connection()
    
    # Get all tables
    tables = duck.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
    """).fetchall()
    
    if not tables:
        print("   No tables in DuckDB")
        duck.close()
        return
    
    print(f"\n   Found {len(tables)} tables:\n")
    
    total_rows = 0
    for (table_name,) in tables:
        # Get row count
        count = duck.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        # Get column count
        cols = duck.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """).fetchone()[0]
        
        print(f"   📋 {table_name}")
        print(f"      Rows: {count:,}")
        print(f"      Columns: {cols}")
        total_rows += count
    
    print(f"\n   Total rows: {total_rows:,}")
    
    # File size
    size_mb = os.path.getsize(DUCKDB_PATH) / (1024 * 1024)
    print(f"   File size: {size_mb:.1f} MB")
    
    duck.close()


def push_table(table_name: str, allowed_tables=None, progress_callback=None) -> dict:
    """Push a single table from DuckDB back to Supabase PostgreSQL.
    
    Args:
        progress_callback: optional callable(rows_sent, total_rows) for live progress
    """
    PUSH_ALLOWED = allowed_tables or ["race_training_dataset"]
    if table_name not in PUSH_ALLOWED:
        raise ValueError(f"Push not allowed for '{table_name}'. Allowed: {PUSH_ALLOWED}")

    print(f"\n📤 Pushing {table_name} to Supabase...")

    # Read from DuckDB
    duck = get_duckdb_connection()
    df = duck.execute(f"SELECT * FROM {table_name}").fetchdf()
    duck.close()

    local_rows = len(df)
    print(f"   Local rows: {local_rows:,}")

    if local_rows == 0:
        return {"table": table_name, "status": "skipped", "reason": "empty table"}

    # Connect to Supabase
    pg_conn = get_supabase_connection()
    pg_cursor = pg_conn.cursor()

    def _pg_type_for_series(series) -> str:
        """Map pandas dtype to a safe PostgreSQL type for schema sync."""
        dtype_str = str(series.dtype).lower()
        if 'bool' in dtype_str:
            return 'BOOLEAN'
        if 'int' in dtype_str:
            return 'BIGINT'
        if 'float' in dtype_str or 'double' in dtype_str:
            return 'DOUBLE PRECISION'
        if 'datetime' in dtype_str or 'timestamp' in dtype_str:
            return 'TIMESTAMP'
        return 'TEXT'

    # Get current remote count
    pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    remote_before = pg_cursor.fetchone()[0]
    print(f"   Remote rows (before): {remote_before:,}")

    # Truncate remote table
    pg_cursor.execute(f"TRUNCATE TABLE {table_name}")
    pg_conn.commit()
    print(f"   ✓ Truncated remote table")

    # Get column names and types from remote (including numeric precision)
    pg_cursor.execute(f"""
        SELECT column_name, data_type, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    remote_col_info = pg_cursor.fetchall()
    remote_columns = {r[0]: r[1] for r in remote_col_info}

    # Schema sync: add any new local columns to remote table before push
    missing_remote_cols = [c for c in df.columns if c not in remote_columns]
    if missing_remote_cols:
        print(f"   ⚙️ Adding {len(missing_remote_cols)} missing remote columns...")
        for col in missing_remote_cols:
            pg_type = _pg_type_for_series(df[col])
            pg_cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{col}" {pg_type}')
            print(f"      + {col} ({pg_type})")
        pg_conn.commit()

        # Refresh remote schema after ALTER TABLE
        pg_cursor.execute(f"""
            SELECT column_name, data_type, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        remote_col_info = pg_cursor.fetchall()
        remote_columns = {r[0]: r[1] for r in remote_col_info}
    # Build precision map for numeric columns: col -> max absolute value
    numeric_limits = {}
    for col_name, dtype, prec, scale in remote_col_info:
        if dtype == 'numeric' and prec is not None and scale is not None:
            numeric_limits[col_name] = 10 ** (prec - scale) - 10 ** (-scale)

    # Only push columns that exist in both local and remote
    common_cols = [c for c in df.columns if c in remote_columns]
    df = df[common_cols]
    print(f"   Pushing {len(common_cols)} columns")

    # Cast columns to match Supabase types (DuckDB/pandas stores ints as float due to NaN)
    import numpy as np
    int_types = {'integer', 'bigint', 'smallint'}
    bool_types = {'boolean'}
    casted = 0
    for col in common_cols:
        rtype = remote_columns[col]
        if rtype in int_types and df[col].dtype in ('float64', 'float32', 'Float64'):
            # Use pandas nullable Int64 to preserve NaN as \N in CSV
            df[col] = df[col].astype('Int64')
            casted += 1
        elif rtype in bool_types:
            # Convert 0/1/True/False/NaN to proper booleans
            df[col] = df[col].astype('boolean')
            casted += 1
    if casted:
        print(f"   ✓ Cast {casted} columns to match Supabase types")

    # Clamp numeric columns: NULL out values that exceed Supabase NUMERIC precision
    clamped_total = 0
    for col in common_cols:
        if col in numeric_limits:
            max_abs = numeric_limits[col]
            mask = df[col].abs() > max_abs
            n_bad = mask.sum()
            if n_bad > 0:
                df.loc[mask, col] = np.nan
                clamped_total += n_bad
                print(f"   ⚠ {col}: {n_bad} values exceeded NUMERIC limit (|x| > {max_abs:.4f}) → set to NULL")
    if clamped_total:
        print(f"   ✓ Clamped {clamped_total} overflow values to NULL")

    # Upload in chunks for real progress tracking
    import io
    import csv

    CHUNK_SIZE = 30_000
    cols_str = ', '.join([f'"{c}"' for c in common_cols])
    copy_sql = f"COPY {table_name} ({cols_str}) FROM STDIN WITH CSV NULL '\\N'"
    rows_sent = 0
    
    # Signal start with 0 rows so UI shows total
    if progress_callback:
        progress_callback(0, local_rows)

    with tqdm(total=local_rows, desc="   Uploading", unit=" rows", ncols=80) as pbar:
        for start in range(0, local_rows, CHUNK_SIZE):
            chunk = df.iloc[start:start + CHUNK_SIZE]
            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False, na_rep='\\N', quoting=csv.QUOTE_MINIMAL)
            buffer.seek(0)
            pg_cursor.copy_expert(copy_sql, buffer)
            pg_conn.commit()
            rows_sent += len(chunk)
            pbar.update(len(chunk))
            if progress_callback:
                progress_callback(rows_sent, local_rows)

    # Verify
    pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    remote_after = pg_cursor.fetchone()[0]
    print(f"   ✅ Remote rows (after): {remote_after:,}")

    pg_cursor.close()
    pg_conn.close()

    return {
        "table": table_name,
        "local_rows": local_rows,
        "remote_before": remote_before,
        "remote_after": remote_after,
        "columns_pushed": len(common_cols),
    }


def clear_database():
    """Delete the DuckDB file"""
    if os.path.exists(DUCKDB_PATH):
        os.remove(DUCKDB_PATH)
        print(f"✅ Deleted {DUCKDB_PATH}")
    else:
        print("No DuckDB file to delete")


def serve():
    """Start the web UI"""
    from web_ui import app
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    print(f"\n🌐 Starting web UI on http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)
    
    command = sys.argv[1].lower()
    
    if command == "pull":
        pull_all()
    elif command == "status":
        show_status()
    elif command == "clear":
        clear_database()
    elif command == "serve":
        serve()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
