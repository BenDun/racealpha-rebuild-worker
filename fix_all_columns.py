#!/usr/bin/env python3
"""
Comprehensive column checker - finds ALL missing columns across all SQL files
and adds them to the database in one go.
"""

import os
import re
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Connect to Supabase
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

print("=" * 60)
print("COMPREHENSIVE COLUMN CHECKER")
print("=" * 60)

# Step 1: Get all existing columns
cur.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'race_training_dataset'
""")
existing_columns = set(row[0] for row in cur.fetchall())
print(f"\n✓ Found {len(existing_columns)} existing columns in race_training_dataset")

# Step 2: Scan all SQL files for column references
sql_dir = os.path.join(os.path.dirname(__file__), 'sql')
all_referenced_columns = set()

# Patterns to find column names
patterns = [
    # SET column = ...
    r'\bSET\s+([a-z_][a-z0-9_]*)\s*=',
    # UPDATE ... SET column = 
    r',\s*([a-z_][a-z0-9_]*)\s*=',
    # ADD COLUMN column_name
    r'ADD\s+COLUMN(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-z_][a-z0-9_]*)',
    # column_name = (in updates)
    r'^\s+([a-z_][a-z0-9_]*)\s*=\s*(?:CASE|COALESCE|NULL|[0-9]|\()',
]

# Reserved words and config to skip
skip_words = {
    'set', 'statement_timeout', 'lock_timeout', 'work_mem', 'from', 'where',
    'and', 'or', 'not', 'null', 'true', 'false', 'case', 'when', 'then',
    'else', 'end', 'as', 'on', 'join', 'left', 'right', 'inner', 'outer',
    'select', 'update', 'insert', 'delete', 'create', 'alter', 'drop',
    'table', 'index', 'column', 'constraint', 'primary', 'key', 'foreign',
    'references', 'unique', 'check', 'default', 'if', 'exists', 'not',
    'in', 'is', 'like', 'between', 'having', 'group', 'order', 'by',
    'asc', 'desc', 'limit', 'offset', 'union', 'all', 'distinct',
    'count', 'sum', 'avg', 'min', 'max', 'coalesce', 'nullif', 'cast',
    'extract', 'date_part', 'to_char', 'to_date', 'now', 'current_date',
    'row_number', 'rank', 'dense_rank', 'lag', 'lead', 'first_value',
    'last_value', 'over', 'partition', 'rows', 'preceding', 'following',
    'unbounded', 'current', 'row', 'range', 'exclude', 'no', 'others',
    'ties', 'groups', 'filter', 'within', 'ordinality', 'lateral',
    'using', 'natural', 'cross', 'full', 'semi', 'anti', 'for', 'share',
    'nowait', 'skip', 'locked', 'returning', 'conflict', 'do', 'nothing',
    'excluded', 'values', 'view', 'materialized', 'temp', 'temporary',
    'begin', 'commit', 'rollback', 'savepoint', 'release', 'declare',
    'fetch', 'close', 'move', 'cursor', 'prepare', 'execute', 'deallocate',
    'explain', 'analyze', 'verbose', 'costs', 'buffers', 'timing', 'format',
    'notice', 'raise', 'info', 'warning', 'debug', 'log', 'exception',
    'type', 'numeric', 'integer', 'bigint', 'smallint', 'real', 'double',
    'precision', 'decimal', 'boolean', 'text', 'varchar', 'char', 'bytea',
    'timestamp', 'date', 'time', 'interval', 'json', 'jsonb', 'uuid', 'xml',
    'array', 'record', 'void', 'trigger', 'function', 'procedure', 'language',
    'plpgsql', 'sql', 'immutable', 'stable', 'volatile', 'parallel', 'safe',
    'unsafe', 'restricted', 'called', 'input', 'strict', 'security', 'definer',
    'invoker', 'external', 'returns', 'setof', 'variadic', 'inout', 'out',
}

print(f"\nScanning SQL files in {sql_dir}...")

for filename in sorted(os.listdir(sql_dir)):
    if not filename.endswith('.sql'):
        continue
    
    filepath = os.path.join(sql_dir, filename)
    with open(filepath, 'r') as f:
        content = f.read()
    
    file_columns = set()
    
    for pattern in patterns:
        matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            col = match.lower().strip()
            if col and col not in skip_words and len(col) > 2:
                file_columns.add(col)
    
    # Also find columns in specific patterns
    # pattern: column_name NUMERIC, column_name INTEGER, etc
    type_pattern = r'([a-z_][a-z0-9_]*)\s+(?:NUMERIC|INTEGER|BIGINT|BOOLEAN|VARCHAR|TEXT|REAL|DOUBLE)'
    matches = re.findall(type_pattern, content, re.IGNORECASE)
    for match in matches:
        col = match.lower().strip()
        if col and col not in skip_words and len(col) > 2:
            file_columns.add(col)
    
    all_referenced_columns.update(file_columns)
    print(f"  {filename}: found {len(file_columns)} column references")

print(f"\n✓ Found {len(all_referenced_columns)} total column references in SQL files")

# Step 3: Find missing columns
missing_columns = all_referenced_columns - existing_columns
# Filter out obvious non-columns
missing_columns = {c for c in missing_columns if not c.startswith('pg_') and len(c) > 3}

print(f"\n{'=' * 60}")
print(f"MISSING COLUMNS: {len(missing_columns)}")
print("=" * 60)

if missing_columns:
    for col in sorted(missing_columns):
        print(f"  - {col}")

# Step 4: Determine column types by looking at SQL context
def guess_column_type(col_name, sql_content):
    """Guess the column type based on naming conventions and SQL context."""
    col_lower = col_name.lower()
    
    # Check for explicit type in ADD COLUMN statements
    pattern = rf'ADD\s+COLUMN(?:\s+IF\s+NOT\s+EXISTS)?\s+{col_name}\s+(\w+(?:\([^)]+\))?)'
    match = re.search(pattern, sql_content, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    
    # Naming conventions
    if col_lower.endswith('_flag') or col_lower.startswith('is_') or col_lower.startswith('has_'):
        return 'BOOLEAN DEFAULT FALSE'
    if col_lower.endswith('_count') or col_lower.endswith('_runs') or col_lower.endswith('_starts'):
        return 'INTEGER DEFAULT 0'
    if col_lower.endswith('_days') or col_lower.endswith('_position') or col_lower.endswith('_level'):
        return 'INTEGER'
    if col_lower.endswith('_rate') or col_lower.endswith('_pct') or col_lower.endswith('_percentage'):
        return 'NUMERIC(10,4)'
    if col_lower.endswith('_score') or col_lower.endswith('_rating') or col_lower.endswith('_figure'):
        return 'NUMERIC(12,4)'
    if col_lower.endswith('_improvement') or col_lower.endswith('_change'):
        return 'NUMERIC(10,4)'
    if col_lower.endswith('_bias') or col_lower.endswith('_error'):
        return 'NUMERIC(10,4)'
    if col_lower.endswith('_advantage') or col_lower.endswith('_preference'):
        return 'NUMERIC(10,4)'
    if col_lower.endswith('_time') or col_lower.endswith('_seconds'):
        return 'NUMERIC(10,3)'
    if col_lower.endswith('_style') or col_lower.endswith('_type') or col_lower.endswith('_category'):
        return 'VARCHAR(50)'
    
    # Default to NUMERIC for most features
    return 'NUMERIC(12,4)'

# Build all SQL content for context
all_sql = ""
for filename in os.listdir(sql_dir):
    if filename.endswith('.sql'):
        with open(os.path.join(sql_dir, filename), 'r') as f:
            all_sql += f.read() + "\n"

# Step 5: Generate and execute ALTER statements
if missing_columns:
    print(f"\n{'=' * 60}")
    print("ADDING MISSING COLUMNS...")
    print("=" * 60)
    
    alter_statements = []
    for col in sorted(missing_columns):
        col_type = guess_column_type(col, all_sql)
        stmt = f"ADD COLUMN IF NOT EXISTS {col} {col_type}"
        alter_statements.append(stmt)
        print(f"  + {col} {col_type}")
    
    # Execute in batches of 20 to avoid statement too long
    batch_size = 20
    for i in range(0, len(alter_statements), batch_size):
        batch = alter_statements[i:i+batch_size]
        sql = "ALTER TABLE race_training_dataset\n" + ",\n".join(batch) + ";"
        try:
            cur.execute(sql)
            conn.commit()
            print(f"  ✓ Added columns {i+1}-{min(i+batch_size, len(alter_statements))}")
        except Exception as e:
            print(f"  ✗ Error adding columns: {e}")
            conn.rollback()
    
    # Update backup too
    print("\nUpdating backup table...")
    try:
        cur.execute("DROP TABLE IF EXISTS _backup_training_rebuild_v2")
        cur.execute("CREATE TABLE _backup_training_rebuild_v2 AS SELECT * FROM race_training_dataset")
        conn.commit()
        print("  ✓ Backup updated with new columns")
    except Exception as e:
        print(f"  ✗ Error updating backup: {e}")
        conn.rollback()

# Final summary
cur.execute("""
    SELECT COUNT(*) 
    FROM information_schema.columns 
    WHERE table_name = 'race_training_dataset'
""")
final_count = cur.fetchone()[0]

print(f"\n{'=' * 60}")
print("SUMMARY")
print("=" * 60)
print(f"  Columns before: {len(existing_columns)}")
print(f"  Missing found:  {len(missing_columns)}")
print(f"  Columns after:  {final_count}")
print("=" * 60)

cur.close()
conn.close()
print("\nDone! You can now redeploy the rebuild worker.")
