#!/usr/bin/env python3
"""
Rebuild Runner for race_training_dataset_new
Executes the 3-phase SQL rebuild pipeline in DuckDB.
"""
import duckdb
import time
import os
import sys
import json
from pathlib import Path
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "racealpha.duckdb")
REBUILD_DIR = os.path.join(os.path.dirname(__file__), "rebuild")

# Intermediate columns Phase 2 creates/uses but Phase 3 drops as leakage.
# These must exist in race_training_dataset_new before Phase 2 runs.
INTERMEDIATE_COLUMNS = [
    'position_800m', 'position_400m', 'position_at_800', 'position_at_400', 'position_at_end',
    'sectional_position_800m', 'sectional_position_400m', 'sectional_position_200m',
    'pos_improvement_800_finish', 'pos_improvement_400_finish', 'pos_improvement_800_400',
    'best_late_improvement',
    'position_change_800_400', 'position_change_400_finish',
    'closing_ability_score', 'early_speed_score', 'sustained_run_score',
    'closing_power_score', 'finishing_kick_consistency',
    'speed_figure', 'speed_rating', 'early_speed_pct', 'strong_finish_pct',
    'pos_volatility_800',
    'early_speed_rating', 'finish_speed_rating',
    # Feature maturity columns (cold-start improvement - Feb 2026)
    'elo_races_count', 'feature_maturity_score',
    # Rail position features (Feb 2026 - genuine new signal)
    'rail_out_metres', 'is_rail_true', 'rail_out_x_barrier',
    'effective_barrier', 'barrier_advantage_rail_adjusted',
]

PHASES = [
    {"file": "phase1_base_career.sql", "name": "Base Insert + Career Stats", "description": "Insert from race_results+races, track props, career stats, ELO, jockey/trainer rates"},
    {"file": "phase2_features.sql", "name": "Advanced Features & Interactions", "description": "Sectionals, running style, speed, class, odds, connections, barrier analysis, specialist features"},
    {"file": "phase3_validate_clean.sql", "name": "Validation + Leakage Removal", "description": "Remove leakage columns (final_position, margin, in-race positions, derived scores)"},
]

# Global status for web UI polling
_status = {
    "state": "idle",  # idle, running, completed, error
    "current_phase": 0,
    "total_phases": len(PHASES),
    "current_phase_name": "",
    "started_at": None,
    "completed_at": None,
    "error": None,
    "phase_results": [],
    "row_count": 0,
    "col_count": 0,
}


def get_status():
    """Return current rebuild status."""
    return _status.copy()


def reset_status():
    """Reset status to idle."""
    global _status
    _status = {
        "state": "idle",
        "current_phase": 0,
        "total_phases": len(PHASES),
        "current_phase_name": "",
        "started_at": None,
        "completed_at": None,
        "error": None,
        "phase_results": [],
        "row_count": 0,
        "col_count": 0,
    }


def split_sql_statements(sql_text: str) -> list[str]:
    """Split SQL text into individual statements, handling edge cases."""
    statements = []
    current = []
    in_string = False
    string_char = None

    for line in sql_text.split('\n'):
        stripped = line.strip()
        # Skip empty lines and comments
        if not stripped or stripped.startswith('--'):
            continue

        current.append(line)

        # Track string literals to avoid splitting on semicolons inside strings
        for i, char in enumerate(stripped):
            if char in ("'", '"') and (i == 0 or stripped[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False

        # If line ends with semicolon outside of a string, it's end of statement
        if stripped.endswith(';') and not in_string:
            stmt = '\n'.join(current).strip()
            if stmt and not all(l.strip().startswith('--') or not l.strip() for l in stmt.split('\n')):
                statements.append(stmt)
            current = []

    # Any remaining content
    if current:
        stmt = '\n'.join(current).strip()
        if stmt and not all(l.strip().startswith('--') or not l.strip() for l in stmt.split('\n')):
            statements.append(stmt)

    return statements


def run_phase(con: duckdb.DuckDBPyConnection, phase_idx: int, phase: dict) -> dict:
    """Execute a single rebuild phase."""
    sql_path = os.path.join(REBUILD_DIR, phase["file"])
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path, 'r') as f:
        sql_text = f.read()

    statements = split_sql_statements(sql_text)
    print(f"\n{'='*60}")
    print(f"Phase {phase_idx+1}/{len(PHASES)}: {phase['name']}")
    print(f"  {phase['description']}")
    print(f"  Statements: {len(statements)}")
    print(f"{'='*60}")

    _status["current_phase"] = phase_idx + 1
    _status["current_phase_name"] = phase["name"]

    phase_start = time.time()
    errors = []
    completed = 0

    for i, stmt in enumerate(statements):
        stmt_start = time.time()
        try:
            # Show first 80 chars of statement for progress
            preview = stmt.replace('\n', ' ')[:80].strip()
            print(f"  [{i+1}/{len(statements)}] {preview}...")
            con.execute(stmt)
            elapsed = time.time() - stmt_start
            if elapsed > 2:
                print(f"    ⏱ {elapsed:.1f}s")
            completed += 1
        except Exception as e:
            error_msg = str(e)
            # Some errors are acceptable (e.g., column already exists for DROP IF EXISTS)
            if "does not exist" in error_msg.lower() and "DROP COLUMN" in stmt.upper():
                print(f"    ⚠ Column already dropped, skipping")
                completed += 1
                continue
            print(f"    ❌ Error: {error_msg}")
            errors.append({"statement": i+1, "error": error_msg, "sql_preview": stmt[:200]})
            # Continue executing remaining statements
            continue

    phase_elapsed = time.time() - phase_start
    result = {
        "phase": phase_idx + 1,
        "name": phase["name"],
        "statements_total": len(statements),
        "statements_completed": completed,
        "errors": errors,
        "elapsed_seconds": round(phase_elapsed, 1),
    }

    status_emoji = "✅" if not errors else "⚠️"
    print(f"\n{status_emoji} Phase {phase_idx+1} complete: {completed}/{len(statements)} statements in {phase_elapsed:.1f}s")
    if errors:
        print(f"   {len(errors)} error(s)")

    return result


def run_rebuild(skip_phases: list[int] = None):
    """Execute the full rebuild pipeline."""
    global _status
    reset_status()
    _status["state"] = "running"
    _status["started_at"] = datetime.now().isoformat()

    print(f"\n{'#'*60}")
    print(f"# REBUILD race_training_dataset_new")
    print(f"# Started: {_status['started_at']}")
    print(f"# Phases: {len(PHASES)}")
    print(f"{'#'*60}")

    try:
        con = duckdb.connect(DB_PATH)

        # Pre-check: ensure source tables exist
        tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        required = ['race_results', 'races', 'race_training_dataset']
        missing = [t for t in required if t not in tables]
        if missing:
            raise RuntimeError(f"Missing required tables: {missing}")

        # Create or recreate race_training_dataset_new from the current schema
        if 'race_training_dataset_new' in tables:
            con.execute("DROP TABLE race_training_dataset_new")
        con.execute("CREATE TABLE race_training_dataset_new AS SELECT * FROM race_training_dataset WHERE 1=0")
        new_cols = con.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='race_training_dataset_new'"
        ).fetchone()[0]
        print(f"\n  ✓ Created race_training_dataset_new with {new_cols} columns")

        # Add intermediate columns that Phase 2 needs (Phase 3 drops them later)
        existing_cols = set(r[0] for r in con.execute("DESCRIBE race_training_dataset_new").fetchall())
        added = 0
        for col in INTERMEDIATE_COLUMNS:
            if col not in existing_cols:
                con.execute(f"ALTER TABLE race_training_dataset_new ADD COLUMN {col} DOUBLE")
                added += 1
        if added:
            final_cols = con.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='race_training_dataset_new'"
            ).fetchone()[0]
            print(f"  ✓ Added {added} intermediate columns → {final_cols} total")
        else:
            print(f"  ✓ All intermediate columns already present")

        # Execute phases
        for idx, phase in enumerate(PHASES):
            if skip_phases and (idx + 1) in skip_phases:
                print(f"\n⏭ Skipping Phase {idx+1}: {phase['name']}")
                _status["phase_results"].append({
                    "phase": idx + 1, "name": phase["name"],
                    "statements_total": 0, "statements_completed": 0,
                    "errors": [], "elapsed_seconds": 0, "skipped": True
                })
                continue

            result = run_phase(con, idx, phase)
            _status["phase_results"].append(result)

            # If phase 1 (base insert) has errors, we might want to stop
            if idx == 0 and result["errors"]:
                fatal_errors = [e for e in result["errors"] if "INSERT" in e.get("sql_preview", "").upper()]
                if fatal_errors:
                    raise RuntimeError(f"Fatal error in base insert: {fatal_errors[0]['error']}")

        # Final stats
        row_count = con.execute("SELECT COUNT(*) FROM race_training_dataset_new").fetchone()[0]
        col_count = len(con.execute("DESCRIBE race_training_dataset_new").fetchall())
        _status["row_count"] = row_count
        _status["col_count"] = col_count

        print(f"\n{'#'*60}")
        print(f"# REBUILD COMPLETE")
        print(f"# Rows: {row_count:,}")
        print(f"# Columns: {col_count}")
        total_errors = sum(len(r.get("errors", [])) for r in _status["phase_results"])
        print(f"# Errors: {total_errors}")
        total_time = sum(r.get("elapsed_seconds", 0) for r in _status["phase_results"])
        print(f"# Total time: {total_time:.1f}s")
        print(f"{'#'*60}")

        # Validation queries
        print("\n📊 Validation Summary:")
        try:
            stats = con.execute("""
                SELECT COUNT(*) as total,
                    COUNT(DISTINCT race_id) as races,
                    COUNT(DISTINCT horse_slug) as horses,
                    COUNT(DISTINCT track_name) as tracks,
                    MIN(race_date) as earliest,
                    MAX(race_date) as latest
                FROM race_training_dataset_new
            """).fetchone()
            print(f"  Records: {stats[0]:,} | Races: {stats[1]:,} | Horses: {stats[2]:,} | Tracks: {stats[3]}")
            print(f"  Date range: {stats[4]} → {stats[5]}")
        except Exception as e:
            print(f"  ⚠ Validation query error: {e}")

        # Check for remaining leakage columns
        try:
            cols = [row[0] for row in con.execute("DESCRIBE race_training_dataset_new").fetchall()]
            leakage = [c for c in cols if c in [
                'final_position', 'margin', 'raw_time_seconds',
                'position_800m', 'position_400m', 'position_at_800', 'position_at_400', 'position_at_end',
                'pos_improvement_800_finish', 'pos_improvement_400_finish', 'pos_improvement_800_400',
                'best_late_improvement', 'closing_ability_score', 'early_speed_score',
                'sustained_run_score', 'closing_power_score',
                'speed_figure', 'speed_rating',
                # NOTE: finishing_kick_consistency, early_speed_pct, strong_finish_pct, pos_volatility_800
                # are now HISTORICAL (anti-leakage) — they use w_prior window, so they stay.
            ]]
            if leakage:
                print(f"  ⚠ Remaining leakage columns: {leakage}")
            else:
                print(f"  ✅ No leakage columns found")
        except Exception as e:
            print(f"  ⚠ Leakage check error: {e}")

        con.close()
        _status["state"] = "completed"
        _status["completed_at"] = datetime.now().isoformat()

    except Exception as e:
        _status["state"] = "error"
        _status["error"] = str(e)
        _status["completed_at"] = datetime.now().isoformat()
        print(f"\n❌ REBUILD FAILED: {e}")
        raise


def compare_tables():
    """Compare race_training_dataset vs race_training_dataset_new."""
    con = duckdb.connect(DB_PATH)

    old_count = con.execute("SELECT COUNT(*) FROM race_training_dataset").fetchone()[0]
    new_count = con.execute("SELECT COUNT(*) FROM race_training_dataset_new").fetchone()[0]

    old_cols = set(row[0] for row in con.execute("DESCRIBE race_training_dataset").fetchall())
    new_cols = set(row[0] for row in con.execute("DESCRIBE race_training_dataset_new").fetchall())

    print(f"\n{'='*60}")
    print(f"TABLE COMPARISON")
    print(f"{'='*60}")
    print(f"  race_training_dataset:     {old_count:>10,} rows  |  {len(old_cols)} columns")
    print(f"  race_training_dataset_new: {new_count:>10,} rows  |  {len(new_cols)} columns")
    print(f"\n  Columns only in OLD: {old_cols - new_cols or 'none'}")
    print(f"  Columns only in NEW: {new_cols - old_cols or 'none'}")

    con.close()
    return {"old_rows": old_count, "new_rows": new_count, "old_cols": len(old_cols), "new_cols": len(new_cols)}


def swap_tables():
    """Swap race_training_dataset_new → race_training_dataset."""
    con = duckdb.connect(DB_PATH)

    new_count = con.execute("SELECT COUNT(*) FROM race_training_dataset_new").fetchone()[0]
    if new_count == 0:
        raise RuntimeError("race_training_dataset_new is empty — cannot swap!")

    print(f"Swapping tables (new has {new_count:,} rows)...")
    # Drop old backup if it exists from a previous swap
    con.execute("DROP TABLE IF EXISTS race_training_dataset_old")
    con.execute("ALTER TABLE race_training_dataset RENAME TO race_training_dataset_old")
    con.execute("ALTER TABLE race_training_dataset_new RENAME TO race_training_dataset")
    print("✅ Swap complete. Old table available as race_training_dataset_old")

    con.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "rebuild":
            run_rebuild()
        elif cmd == "compare":
            compare_tables()
        elif cmd == "swap":
            swap_tables()
        elif cmd == "status":
            print(json.dumps(get_status(), indent=2))
        else:
            print(f"Unknown command: {cmd}")
            print("Usage: python rebuild_runner.py [rebuild|compare|swap|status]")
            sys.exit(1)
    else:
        print("Usage: python rebuild_runner.py [rebuild|compare|swap|status]")
        sys.exit(1)
