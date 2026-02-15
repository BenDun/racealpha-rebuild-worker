"""
Simple Web UI for viewing DuckDB data + Rebuild pipeline
"""

import os
import time
import duckdb
import threading
import psycopg2
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv
import rebuild_runner

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", os.path.join(_SCRIPT_DIR, "racealpha.duckdb"))

# Default sync time (5th Feb 2026 at 11:51 PM)
DEFAULT_SYNC_TIME = "2026-02-05T23:51:00"

app = FastAPI(title="RaceAlpha DuckDB Viewer")

# Sync status tracking
sync_status = {
    "running": False,
    "current_table": None,
    "tables_done": [],
    "tables_total": [],
    "progress": "",
    "progress_pct": 0,
    "last_sync": None,
    "error": None
}


def get_db():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def get_db_write():
    return duckdb.connect(DUCKDB_PATH, read_only=False)


def init_sync_metadata():
    """Initialize the sync metadata table if it doesn't exist"""
    if not os.path.exists(DUCKDB_PATH):
        return
    try:
        db = get_db_write()
        db.execute("""
            CREATE TABLE IF NOT EXISTS _sync_metadata (
                table_name VARCHAR PRIMARY KEY,
                last_synced TIMESTAMP
            )
        """)
        db.close()
    except:
        pass


def load_sync_times():
    """Load sync times from DuckDB"""
    from app import TABLES
    result = {}
    
    if not os.path.exists(DUCKDB_PATH):
        # Return defaults for all tables
        for table in TABLES:
            result[table] = DEFAULT_SYNC_TIME
        return result
    
    try:
        db = get_db()
        # Check if metadata table exists
        tables = db.execute("SELECT table_name FROM information_schema.tables WHERE table_name = '_sync_metadata'").fetchall()
        if tables:
            rows = db.execute("SELECT table_name, last_synced FROM _sync_metadata").fetchall()
            for table_name, last_synced in rows:
                result[table_name] = last_synced.isoformat() if last_synced else DEFAULT_SYNC_TIME
        db.close()
    except:
        pass
    
    # Fill in defaults for tables not in DB
    for table in TABLES:
        if table not in result:
            result[table] = DEFAULT_SYNC_TIME
    
    return result


def save_sync_time(table_name: str):
    """Save sync time for a table to DuckDB"""
    from datetime import datetime
    try:
        init_sync_metadata()
        db = get_db_write()
        now = datetime.now()
        db.execute("""
            INSERT OR REPLACE INTO _sync_metadata (table_name, last_synced)
            VALUES (?, ?)
        """, [table_name, now])
        db.close()
        return now.isoformat()
    except Exception as e:
        print(f"Error saving sync time: {e}")
        return datetime.now().isoformat()


def run_sync(tables=None):
    """Background sync task"""
    global sync_status
    sync_status["running"] = True
    sync_status["error"] = None
    sync_status["tables_done"] = []
    sync_status["progress_pct"] = 0
    
    try:
        from app import pull_table, TABLES
        
        tables_to_sync = tables if tables else TABLES
        sync_status["tables_total"] = list(tables_to_sync)
        
        for i, table in enumerate(tables_to_sync):
            sync_status["current_table"] = table
            sync_status["progress"] = f"Pulling {table}..."
            sync_status["progress_pct"] = int((i / len(tables_to_sync)) * 100)
            
            try:
                pull_table(table)
                sync_status["tables_done"].append(table)
                save_sync_time(table)
            except Exception as e:
                sync_status["error"] = f"{table}: {e}"
                break
        
        sync_status["progress_pct"] = 100
        sync_status["progress"] = "Complete!"
        sync_status["current_table"] = None
        from datetime import datetime
        sync_status["last_sync"] = datetime.now().isoformat()
    except Exception as e:
        sync_status["error"] = str(e)
        sync_status["progress"] = f"Error: {e}"
    finally:
        sync_status["running"] = False


# Push status tracking
push_status = {
    "running": False,
    "current_table": None,
    "tables_done": [],
    "tables_total": [],
    "progress": "",
    "progress_pct": 0,
    "error": None,
    "results": [],
    "rows_pushed": 0,
    "total_rows": 0,
}


# ELO rebuild status tracking
elo_rebuild_status = {
    "running": False,
    "phase": "",          # 'wiping', 'processing', 'done'
    "current_year": None,
    "years_done": [],
    "years_total": list(range(2017, 2027)),
    "progress_pct": 0,
    "log": [],             # list of log lines
    "error": None,
    "started_at": None,
    "finished_at": None,
    "total_processed": 0
}


def run_elo_rebuild():
    """Background task: wipe ELO in Supabase then rebuild year-by-year"""
    global elo_rebuild_status
    db_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not db_url:
        elo_rebuild_status["error"] = "No DATABASE_URL configured"
        elo_rebuild_status["running"] = False
        return

    elo_rebuild_status["running"] = True
    elo_rebuild_status["error"] = None
    elo_rebuild_status["years_done"] = []
    elo_rebuild_status["log"] = []
    elo_rebuild_status["total_processed"] = 0
    elo_rebuild_status["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    elo_rebuild_status["finished_at"] = None
    years = list(range(2017, 2027))
    elo_rebuild_status["years_total"] = years

    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()

        # Phase 1: Wipe
        elo_rebuild_status["phase"] = "wiping"
        elo_rebuild_status["log"].append("🗑️ Wiping horse_elo_ratings...")
        cur.execute("SELECT * FROM wipe_horse_elo_ratings();")
        deleted = cur.fetchone()
        deleted_count = deleted[0] if deleted else 0
        elo_rebuild_status["log"].append(f"   Deleted {deleted_count:,} existing records")

        # Phase 2: Rebuild year by year
        elo_rebuild_status["phase"] = "processing"
        for i, year in enumerate(years):
            elo_rebuild_status["current_year"] = year
            elo_rebuild_status["progress_pct"] = int((i / len(years)) * 100)
            elo_rebuild_status["log"].append(f"⚡ Processing {year}...")

            t0 = time.time()
            cur.execute("SELECT * FROM process_elo_by_year(%s);", (year,))
            row = cur.fetchone()
            count = int(row[0]) if row else 0
            elapsed = time.time() - t0

            elo_rebuild_status["total_processed"] += count
            elo_rebuild_status["years_done"].append(year)
            elo_rebuild_status["log"].append(
                f"   ✅ {year}: {count:,} records ({elapsed:.1f}s)"
            )

        cur.close()
        conn.close()

        elo_rebuild_status["phase"] = "done"
        elo_rebuild_status["progress_pct"] = 100
        elo_rebuild_status["current_year"] = None
        elo_rebuild_status["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        elo_rebuild_status["log"].append(
            f"🏁 Complete! {elo_rebuild_status['total_processed']:,} total records rebuilt"
        )

    except Exception as e:
        elo_rebuild_status["error"] = str(e)
        elo_rebuild_status["log"].append(f"❌ Error: {e}")
        elo_rebuild_status["phase"] = "error"
    finally:
        elo_rebuild_status["running"] = False


def run_push(tables=None):
    """Background push task - push DuckDB tables to Supabase"""
    global push_status
    push_status["running"] = True
    push_status["error"] = None
    push_status["tables_done"] = []
    push_status["results"] = []
    push_status["progress_pct"] = 0
    push_status["rows_pushed"] = 0
    push_status["total_rows"] = 0
    
    def _progress_cb(rows_sent, total):
        push_status["rows_pushed"] = rows_sent
        push_status["total_rows"] = total
        push_status["progress_pct"] = int((rows_sent / total) * 100) if total else 0
        push_status["progress"] = f"Uploading... {rows_sent:,} / {total:,} rows"
    
    try:
        from app import push_table
        
        PUSHABLE = ["race_training_dataset"]
        tables_to_push = tables if tables else PUSHABLE
        # Only allow pushable tables
        tables_to_push = [t for t in tables_to_push if t in PUSHABLE]
        push_status["tables_total"] = list(tables_to_push)
        
        for i, table in enumerate(tables_to_push):
            push_status["current_table"] = table
            push_status["progress"] = f"Pushing {table} to Supabase..."
            push_status["rows_pushed"] = 0
            push_status["total_rows"] = 0
            
            try:
                result = push_table(table, progress_callback=_progress_cb)
                push_status["tables_done"].append(table)
                push_status["results"].append(result)
            except Exception as e:
                push_status["error"] = f"{table}: {e}"
                push_status["results"].append({"table": table, "error": str(e)})
                break
        
        push_status["progress_pct"] = 100
        push_status["progress"] = "Push complete!"
        push_status["current_table"] = None
    except Exception as e:
        push_status["error"] = str(e)
        push_status["progress"] = f"Error: {e}"
    finally:
        push_status["running"] = False


@app.get("/", response_class=HTMLResponse)
async def home():
    # Get available tables from app.py
    from app import TABLES
    tables_json = str(list(TABLES)).replace("'", '"')
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>RaceAlpha DuckDB</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            .progress-bar {{ transition: width 0.3s ease; }}
        </style>
    </head>
    <body class="bg-gray-900 text-gray-100 p-8">
        <div class="max-w-4xl mx-auto">
            <div class="flex justify-between items-center mb-6">
                <h1 class="text-3xl font-bold">🦆 RaceAlpha DuckDB</h1>
                <button id="syncAllBtn" onclick="startSync()" class="bg-green-600 hover:bg-green-700 px-4 py-2 rounded-lg font-medium transition flex items-center gap-2">
                    <span>🔄</span>
                    <span>Sync All Tables</span>
                </button>
            </div>
            
            <!-- Progress Section -->
            <div id="syncStatus" class="hidden mb-6 p-4 rounded-lg bg-gray-800 border border-gray-700">
                <div class="flex justify-between items-center mb-2">
                    <span id="syncProgress" class="font-medium">Syncing...</span>
                    <span id="syncPct" class="text-gray-400">0%</span>
                </div>
                <div class="w-full bg-gray-700 rounded-full h-3">
                    <div id="progressBar" class="progress-bar bg-green-500 h-3 rounded-full" style="width: 0%"></div>
                </div>
                <div id="tableStatus" class="mt-2 text-sm text-gray-400"></div>
            </div>
            
            <!-- File Info -->
            <div id="fileInfo" class="mb-4 text-gray-400"></div>
            
            <!-- Rebuild Section -->
            <div class="mb-6 p-4 rounded-lg bg-gray-800 border border-yellow-600/30">
                <div class="flex justify-between items-center mb-3">
                    <h2 class="text-lg font-bold text-yellow-400">🔨 Rebuild Pipeline</h2>
                    <div class="flex gap-2">
                        <button id="rebuildBtn" onclick="startRebuild()" class="bg-yellow-600 hover:bg-yellow-700 px-4 py-2 rounded-lg font-medium transition text-sm">
                            ▶️ Run Rebuild
                        </button>
                        <button onclick="compareRebuild()" class="bg-gray-600 hover:bg-gray-700 px-3 py-2 rounded-lg text-sm transition">
                            📊 Compare
                        </button>
                        <button onclick="swapRebuild()" class="bg-red-600 hover:bg-red-700 px-3 py-2 rounded-lg text-sm transition">
                            🔄 Swap Tables
                        </button>
                    </div>
                </div>
                <div id="rebuildStatus" class="text-sm text-gray-400">
                    <span>Status: Idle</span>
                </div>
                <div id="rebuildProgress" class="hidden mt-3">
                    <div class="flex justify-between text-xs text-gray-400 mb-1">
                        <span id="rebuildPhaseName">Phase 1/3</span>
                        <span id="rebuildPhaseNum"></span>
                    </div>
                    <div class="w-full bg-gray-700 rounded-full h-2">
                        <div id="rebuildBar" class="bg-yellow-500 h-2 rounded-full transition-all" style="width: 0%"></div>
                    </div>
                    <div id="rebuildPhases" class="mt-2 text-xs space-y-1"></div>
                </div>
                <div id="rebuildResult" class="hidden mt-3 text-sm p-3 rounded bg-gray-900"></div>
            </div>
            
            <!-- ELO Summary Section -->
            <div class="mb-6 p-4 rounded-lg bg-gray-800 border border-purple-600/30">
                <div class="flex justify-between items-center mb-3">
                    <h2 class="text-lg font-bold text-purple-400">⚡ Horse ELO Ratings <span class="text-xs font-normal text-gray-500">(rebuilt in Supabase → synced down)</span></h2>
                    <button id="eloRefreshBtn" onclick="loadEloSummary()" class="bg-purple-600 hover:bg-purple-700 px-3 py-1.5 rounded text-sm font-medium transition">
                        🔄 Refresh
                    </button>
                </div>
                <div id="eloSummary" class="text-sm text-gray-400">Loading...</div>
            </div>
            
            <!-- ELO Rebuild Section -->
            <div class="mb-6 p-4 rounded-lg bg-gray-800 border border-orange-600/30">
                <div class="flex justify-between items-center mb-3">
                    <h2 class="text-lg font-bold text-orange-400">🔨 Rebuild ELO in Supabase <span class="text-xs font-normal text-gray-500">(wipe → process_elo_by_year × 10 years)</span></h2>
                    <button id="eloRebuildBtn" onclick="startEloRebuild()" class="bg-orange-600 hover:bg-orange-700 px-3 py-1.5 rounded text-sm font-medium transition">
                        🔨 Rebuild ELO
                    </button>
                </div>
                <div id="eloRebuildStatus" class="text-sm text-gray-400">
                    <span>Wipes <code class="text-orange-300">horse_elo_ratings</code> in Supabase, then runs <code class="text-orange-300">process_elo_by_year()</code> for 2017–2026. Takes ~10-15 minutes.</span>
                </div>
                <div id="eloRebuildLog" class="hidden mt-3 bg-gray-900 rounded p-3 max-h-64 overflow-y-auto font-mono text-xs text-gray-300">
                </div>
                <div id="eloRebuildProgress" class="hidden mt-3">
                    <div class="flex justify-between text-xs text-gray-400 mb-1">
                        <span id="eloRebuildProgressText">Starting...</span>
                        <span id="eloRebuildProgressPct">0%</span>
                    </div>
                    <div class="w-full bg-gray-700 rounded-full h-2">
                        <div id="eloRebuildBar" class="bg-orange-500 h-2 rounded-full transition-all duration-500" style="width: 0%"></div>
                    </div>
                </div>
            </div>

            <!-- Push to Supabase Section -->
            <div class="mb-6 p-4 rounded-lg bg-gray-800 border border-emerald-600/30">
                <div class="flex justify-between items-center mb-3">
                    <h2 class="text-lg font-bold text-emerald-400">📤 Push to Supabase</h2>
                    <div class="flex gap-2 items-center">
                        <span id="pushConfirmMsg" class="hidden text-xs text-red-400 animate-pulse">⚠️ Click again to confirm TRUNCATE + push</span>
                        <button id="pushTrainingBtn" onclick="pushTable('race_training_dataset')" class="bg-emerald-600 hover:bg-emerald-700 px-3 py-1.5 rounded text-sm font-medium transition">
                            📤 Push Training Dataset
                        </button>
                    </div>
                </div>
                <div id="pushStatus" class="text-sm text-gray-400">
                    <span>Push rebuilt training dataset to Supabase (truncates remote & replaces). ELO is rebuilt in Supabase and synced down — never pushed.</span>
                </div>
                <div id="pushProgress" class="hidden mt-3">
                    <div class="flex justify-between text-xs text-gray-400 mb-1">
                        <span id="pushProgressText">Pushing...</span>
                        <span id="pushPct">0%</span>
                    </div>
                    <div class="w-full bg-gray-700 rounded-full h-2">
                        <div id="pushBar" class="bg-emerald-500 h-2 rounded-full transition-all" style="width: 0%"></div>
                    </div>
                </div>
                <div id="pushResult" class="hidden mt-3 text-sm p-3 rounded bg-gray-900"></div>
            </div>
            
            <!-- Tables Grid -->
            <div id="content" class="space-y-3">Loading...</div>
        </div>
        
        <script>
            const ALL_TABLES = {tables_json};
            
            function loadStatus() {{
                fetch('/api/status')
                    .then(r => r.json())
                    .then(data => {{
                        if (data.error && data.tables.length === 0) {{
                            document.getElementById('fileInfo').innerHTML = data.error;
                        }} else {{
                            let info = `File: ${{data.file_size_mb}} MB`;
                            if (data.last_sync) {{
                                info += ` • Last sync: ${{new Date(data.last_sync).toLocaleString()}}`;
                            }}
                            document.getElementById('fileInfo').innerHTML = info;
                        }}
                        
                        // Tables - show all configured tables + any extra local tables
                        const existingTables = new Map((data.tables || []).map(t => [t.name, t]));
                        
                        let html = '';
                        
                        // 1. Show sync-able tables (from Supabase)
                        html += '<div class="text-xs text-gray-500 uppercase tracking-wider mb-2 mt-2">📥 Supabase Sync Tables</div>';
                        ALL_TABLES.forEach(tableName => {{
                            const t = existingTables.get(tableName);
                            const exists = !!t;
                            
                            const syncTime = data.table_sync_times ? data.table_sync_times[tableName] : null;
                            const syncTimeStr = syncTime ? new Date(syncTime).toLocaleString() : '';
                            
                            html += `
                                <div class="bg-gray-800 p-4 rounded-lg flex justify-between items-center">
                                    <div>
                                        <div class="font-bold text-lg flex items-center gap-2">
                                            ${{exists ? '✅' : '⚪'}} ${{tableName}}
                                        </div>
                                        <div class="text-gray-400 text-sm">
                                            ${{exists ? `${{t.rows.toLocaleString()}} rows, ${{t.columns}} columns` : 'Not synced yet'}}
                                            ${{syncTime ? `<span class="ml-2 text-gray-500">• Synced: ${{syncTimeStr}}</span>` : ''}}
                                        </div>
                                        ${{exists ? `<a href="/table/${{tableName}}" class="text-blue-400 hover:underline text-sm">View sample →</a>` : ''}}
                                    </div>
                                    <button onclick="syncTable('${{tableName}}')" class="bg-blue-600 hover:bg-blue-700 px-3 py-1.5 rounded text-sm font-medium transition" id="btn-${{tableName}}">
                                        🔄 Sync
                                    </button>
                                </div>
                            `;
                        }});
                        
                        // 2. Show local-only tables (rebuild targets, etc.)
                        const localTables = (data.tables || []).filter(t => !ALL_TABLES.includes(t.name) && !t.name.startsWith('_'));
                        if (localTables.length > 0) {{
                            html += '<div class="text-xs text-gray-500 uppercase tracking-wider mb-2 mt-6">🔧 Local / Rebuild Tables</div>';
                            localTables.forEach(t => {{
                                html += `
                                    <div class="bg-gray-800 p-4 rounded-lg flex justify-between items-center border border-gray-700">
                                        <div>
                                            <div class="font-bold text-lg flex items-center gap-2">
                                                🆕 ${{t.name}}
                                            </div>
                                            <div class="text-gray-400 text-sm">
                                                ${{t.rows.toLocaleString()}} rows, ${{t.columns}} columns
                                            </div>
                                            <a href="/table/${{t.name}}" class="text-blue-400 hover:underline text-sm">View sample →</a>
                                        </div>
                                        <button onclick="clearLocalTable('${{t.name}}')" class="bg-orange-600 hover:bg-orange-700 px-3 py-1.5 rounded text-sm font-medium transition">
                                            🧹 Clear
                                        </button>
                                    </div>
                                `;
                            }});
                        }}
                        
                        document.getElementById('content').innerHTML = html;
                    }});
            }}
            
            function checkSyncStatus() {{
                fetch('/api/sync/status')
                    .then(r => r.json())
                    .then(data => {{
                        const statusDiv = document.getElementById('syncStatus');
                        const syncAllBtn = document.getElementById('syncAllBtn');
                        
                        if (data.running) {{
                            statusDiv.classList.remove('hidden');
                            document.getElementById('syncProgress').textContent = data.progress || 'Syncing...';
                            document.getElementById('syncPct').textContent = data.progress_pct + '%';
                            document.getElementById('progressBar').style.width = data.progress_pct + '%';
                            
                            // Show table status
                            let tableHtml = '';
                            (data.tables_total || []).forEach(t => {{
                                const done = (data.tables_done || []).includes(t);
                                const current = data.current_table === t;
                                tableHtml += `<span class="${{done ? 'text-green-400' : current ? 'text-yellow-400' : 'text-gray-500'}}">${{done ? '✅' : current ? '⏳' : '⬜'}} ${{t}}</span> `;
                            }});
                            document.getElementById('tableStatus').innerHTML = tableHtml;
                            
                            // Disable all sync buttons
                            syncAllBtn.disabled = true;
                            syncAllBtn.classList.add('opacity-50');
                            ALL_TABLES.forEach(t => {{
                                const btn = document.getElementById('btn-' + t);
                                if (btn) {{ btn.disabled = true; btn.classList.add('opacity-50'); }}
                            }});
                            
                            setTimeout(checkSyncStatus, 500);
                        }} else {{
                            // Enable all buttons
                            syncAllBtn.disabled = false;
                            syncAllBtn.classList.remove('opacity-50');
                            ALL_TABLES.forEach(t => {{
                                const btn = document.getElementById('btn-' + t);
                                if (btn) {{ btn.disabled = false; btn.classList.remove('opacity-50'); }}
                            }});
                            
                            if (data.error) {{
                                document.getElementById('progressBar').classList.remove('bg-green-500');
                                document.getElementById('progressBar').classList.add('bg-red-500');
                                document.getElementById('syncProgress').textContent = '❌ ' + data.error;
                            }} else if ((data.tables_done || []).length > 0) {{
                                setTimeout(() => {{
                                    statusDiv.classList.add('hidden');
                                }}, 2000);
                            }} else {{
                                statusDiv.classList.add('hidden');
                            }}
                            
                            loadStatus();
                        }}
                    }});
            }}
            
            function startSync(tables = null) {{
                const body = tables ? JSON.stringify({{ tables: [tables] }}) : '{{}}';
                fetch('/api/sync', {{ 
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: body
                }})
                    .then(r => r.json())
                    .then(data => {{
                        if (data.status === 'started') {{
                            document.getElementById('progressBar').classList.remove('bg-red-500');
                            document.getElementById('progressBar').classList.add('bg-green-500');
                            checkSyncStatus();
                        }}
                    }});
            }}
            
            function syncTable(tableName) {{
                startSync(tableName);
            }}
            
            function clearLocalTable(tableName) {{
                fetch('/api/table/' + tableName + '/clear', {{ method: 'POST' }})
                    .then(r => r.json())
                    .then(data => {{
                        if (data.status === 'cleared') loadStatus();
                    }});
            }}
            
            loadStatus();
            checkSyncStatus();
            checkRebuildStatus();
            loadEloSummary();
            
            function loadEloSummary() {{
                const btn = document.getElementById('eloRefreshBtn');
                const div = document.getElementById('eloSummary');
                
                // Show loading state
                btn.disabled = true;
                btn.classList.add('opacity-50');
                btn.innerHTML = '⏳ Querying DuckDB...';
                div.innerHTML = '<span class="text-purple-300 animate-pulse">🔄 Querying horse_elo_ratings in DuckDB...</span>';
                
                fetch('/api/elo/summary')
                    .then(r => r.json())
                    .then(data => {{
                        if (data.error) {{
                            div.innerHTML = `<span class="text-red-400">❌ ${{data.error}}</span>`;
                            return;
                        }}
                        if (!data.years || data.years.length === 0) {{
                            div.innerHTML = '<span class="text-gray-500">No ELO data in DuckDB. Sync horse_elo_ratings first.</span>';
                            return;
                        }}
                        
                        let html = `
                            <div class="mb-3 flex gap-4 text-xs">
                                <span class="text-purple-300">Total: <strong>${{data.total_records?.toLocaleString()}}</strong> records</span>
                                <span>Horses: <strong>${{data.unique_horses?.toLocaleString()}}</strong></span>
                                <span>Range: <strong>${{data.min_elo}} – ${{data.max_elo}}</strong></span>
                                <span>Avg: <strong>${{data.avg_elo}}</strong></span>
                                <span>σ: <strong>${{data.stddev_elo}}</strong></span>
                            </div>
                            <div class="overflow-x-auto">
                                <table class="w-full text-xs">
                                    <thead>
                                        <tr class="border-b border-gray-700 text-gray-400">
                                            <th class="text-left py-1 pr-3">Year</th>
                                            <th class="text-right py-1 pr-3">Records</th>
                                            <th class="text-right py-1 pr-3">Horses</th>
                                            <th class="text-right py-1 pr-3">ELO Range</th>
                                            <th class="text-right py-1">Avg ELO</th>
                                        </tr>
                                    </thead>
                                    <tbody>`;
                        
                        data.years.forEach(y => {{
                            html += `
                                        <tr class="border-b border-gray-800 hover:bg-gray-700/50">
                                            <td class="py-1 pr-3 font-medium">${{y.year}}</td>
                                            <td class="text-right py-1 pr-3">${{y.records.toLocaleString()}}</td>
                                            <td class="text-right py-1 pr-3">${{y.horses.toLocaleString()}}</td>
                                            <td class="text-right py-1 pr-3">${{y.min_elo}} – ${{y.max_elo}}</td>
                                            <td class="text-right py-1">${{y.avg_elo}}</td>
                                        </tr>`;
                        }});
                        
                        html += `</tbody></table></div>`;
                        const now = new Date().toLocaleTimeString();
                        html += `<div class="mt-2 text-xs text-gray-600">Last refreshed: ${{now}} (from local DuckDB)</div>`;
                        div.innerHTML = html;
                    }})
                    .catch(err => {{
                        div.innerHTML = `<span class="text-red-400">❌ Error: ${{err}}</span>`;
                    }})
                    .finally(() => {{
                        btn.disabled = false;
                        btn.classList.remove('opacity-50');
                        btn.innerHTML = '🔄 Refresh';
                    }});
            }}
            
            let eloRebuildConfirmed = false;
            let eloConfirmTimer = null;
            function startEloRebuild() {{
                const btn = document.getElementById('eloRebuildBtn');
                
                if (!eloRebuildConfirmed) {{
                    eloRebuildConfirmed = true;
                    btn.classList.remove('bg-orange-600', 'hover:bg-orange-700');
                    btn.classList.add('bg-red-600', 'hover:bg-red-700');
                    btn.textContent = '\u26a0\ufe0f CONFIRM REBUILD';
                    clearTimeout(eloConfirmTimer);
                    eloConfirmTimer = setTimeout(() => {{
                        eloRebuildConfirmed = false;
                        btn.classList.remove('bg-red-600', 'hover:bg-red-700');
                        btn.classList.add('bg-orange-600', 'hover:bg-orange-700');
                        btn.textContent = '\U0001f528 Rebuild ELO';
                    }}, 5000);
                    return;
                }}
                eloRebuildConfirmed = false;
                clearTimeout(eloConfirmTimer);
                
                btn.disabled = true;
                btn.classList.add('opacity-50');
                btn.innerHTML = '⏳ Rebuilding...';
                
                document.getElementById('eloRebuildLog').classList.remove('hidden');
                document.getElementById('eloRebuildLog').innerHTML = '<span class="text-orange-300 animate-pulse">Starting ELO rebuild in Supabase...</span>';
                document.getElementById('eloRebuildProgress').classList.remove('hidden');
                
                fetch('/api/elo/rebuild', {{ method: 'POST' }})
                    .then(r => r.json())
                    .then(data => {{
                        if (data.status === 'started') {{
                            pollEloRebuild();
                        }} else if (data.status === 'already_running') {{
                            pollEloRebuild();
                        }} else {{
                            document.getElementById('eloRebuildLog').innerHTML = `<span class="text-red-400">Error: ${{JSON.stringify(data)}}</span>`;
                            btn.disabled = false;
                            btn.classList.remove('opacity-50');
                            btn.innerHTML = '🔨 Rebuild ELO';
                        }}
                    }})
                    .catch(err => {{
                        document.getElementById('eloRebuildLog').innerHTML = `<span class="text-red-400">Error: ${{err}}</span>`;
                        btn.disabled = false;
                        btn.classList.remove('opacity-50');
                        btn.innerHTML = '🔨 Rebuild ELO';
                    }});
            }}
            
            function pollEloRebuild() {{
                fetch('/api/elo/rebuild/status')
                    .then(r => r.json())
                    .then(data => {{
                        const logDiv = document.getElementById('eloRebuildLog');
                        const btn = document.getElementById('eloRebuildBtn');
                        
                        // Update log
                        if (data.log && data.log.length > 0) {{
                            logDiv.innerHTML = data.log.map(line => {{
                                if (line.startsWith('❌')) return `<div class="text-red-400">${{line}}</div>`;
                                if (line.startsWith('✅') || line.includes('✅')) return `<div class="text-green-400">${{line}}</div>`;
                                if (line.startsWith('🏁')) return `<div class="text-yellow-300 font-bold">${{line}}</div>`;
                                if (line.startsWith('⚡')) return `<div class="text-orange-300">${{line}}</div>`;
                                if (line.startsWith('🗑️')) return `<div class="text-gray-400">${{line}}</div>`;
                                return `<div>${{line}}</div>`;
                            }}).join('');
                            logDiv.scrollTop = logDiv.scrollHeight;
                        }}
                        
                        // Update progress bar
                        document.getElementById('eloRebuildBar').style.width = data.progress_pct + '%';
                        document.getElementById('eloRebuildProgressPct').textContent = data.progress_pct + '%';
                        
                        if (data.current_year) {{
                            document.getElementById('eloRebuildProgressText').textContent = `Processing ${{data.current_year}}...`;
                        }} else if (data.phase === 'wiping') {{
                            document.getElementById('eloRebuildProgressText').textContent = 'Wiping existing records...';
                        }} else if (data.phase === 'done') {{
                            document.getElementById('eloRebuildProgressText').textContent = `Done! ${{data.total_processed?.toLocaleString()}} records`;
                        }}
                        
                        if (data.running) {{
                            setTimeout(pollEloRebuild, 2000);
                        }} else {{
                            btn.disabled = false;
                            btn.classList.remove('opacity-50');
                            btn.innerHTML = data.error ? '❌ Failed — Retry' : '✅ Rebuild Complete';
                            // Auto-refresh ELO summary after rebuild finishes
                            if (!data.error) {{
                                setTimeout(() => {{
                                    btn.innerHTML = '🔨 Rebuild ELO';
                                }}, 5000);
                            }}
                        }}
                    }})
                    .catch(err => {{
                        setTimeout(pollEloRebuild, 3000);
                    }});
            }}

            let pushConfirmed = false;
            let pushConfirmTimer = null;
            function pushTable(tableName) {{
                const btn = document.getElementById(tableName === 'race_training_dataset' ? 'pushTrainingBtn' : 'pushEloBtn');
                const confirmMsg = document.getElementById('pushConfirmMsg');
                
                if (!pushConfirmed) {{
                    pushConfirmed = true;
                    btn.classList.remove('bg-emerald-600', 'hover:bg-emerald-700');
                    btn.classList.add('bg-red-600', 'hover:bg-red-700');
                    btn.textContent = '⚠️ CONFIRM PUSH';
                    if (confirmMsg) confirmMsg.classList.remove('hidden');
                    clearTimeout(pushConfirmTimer);
                    pushConfirmTimer = setTimeout(() => {{
                        pushConfirmed = false;
                        btn.classList.remove('bg-red-600', 'hover:bg-red-700');
                        btn.classList.add('bg-emerald-600', 'hover:bg-emerald-700');
                        btn.textContent = '📤 Push Training Dataset';
                        if (confirmMsg) confirmMsg.classList.add('hidden');
                    }}, 5000);
                    return;
                }}
                
                pushConfirmed = false;
                clearTimeout(pushConfirmTimer);
                if (confirmMsg) confirmMsg.classList.add('hidden');
                btn.disabled = true;
                btn.classList.add('opacity-50');
                btn.textContent = '⏳ Pushing...';
                
                document.getElementById('pushResult').classList.add('hidden');
                document.getElementById('pushProgress').classList.remove('hidden');
                document.getElementById('pushProgressText').textContent = `Pushing ${{tableName}}...`;
                document.getElementById('pushPct').textContent = '0%';
                document.getElementById('pushBar').style.width = '0%';
                
                fetch('/api/push', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ tables: [tableName] }})
                }})
                    .then(r => r.json())
                    .then(data => {{
                        // Start polling immediately
                        setTimeout(checkPushStatus, 1000);
                    }})
                    .catch(err => {{
                        btn.disabled = false;
                        btn.classList.remove('opacity-50');
                        btn.textContent = '📤 Push Training Dataset';
                        document.getElementById('pushProgress').classList.add('hidden');
                        document.getElementById('pushResult').innerHTML = `<span class="text-red-400">❌ Error: ${{err}}</span>`;
                        document.getElementById('pushResult').classList.remove('hidden');
                    }});
            }}
            
            function checkPushStatus() {{
                fetch('/api/push/status')
                    .then(r => r.json())
                    .then(data => {{
                        const progressDiv = document.getElementById('pushProgress');
                        const resultDiv = document.getElementById('pushResult');
                        const btn = document.getElementById('pushTrainingBtn');
                        
                        if (data.running) {{
                            progressDiv.classList.remove('hidden');
                            const pText = data.rows_pushed && data.total_rows
                                ? `Uploading... ${{Number(data.rows_pushed).toLocaleString()}} / ${{Number(data.total_rows).toLocaleString()}} rows`
                                : (data.progress || 'Pushing...');
                            document.getElementById('pushProgressText').textContent = pText;
                            document.getElementById('pushPct').textContent = data.progress_pct + '%';
                            document.getElementById('pushBar').style.width = data.progress_pct + '%';
                            setTimeout(checkPushStatus, 1000);
                        }} else {{
                            progressDiv.classList.add('hidden');
                            btn.disabled = false;
                            btn.classList.remove('opacity-50');
                            btn.classList.remove('bg-red-600', 'hover:bg-red-700');
                            btn.classList.add('bg-emerald-600', 'hover:bg-emerald-700');
                            btn.textContent = '📤 Push Training Dataset';
                            
                            if (data.results && data.results.length > 0) {{
                                let html = '';
                                data.results.forEach(r => {{
                                    if (r.error) {{
                                        html += `<div class="text-red-400">❌ ${{r.table}}: ${{r.error}}</div>`;
                                    }} else {{
                                        html += `<div class="text-emerald-400">✅ ${{r.table}}: ${{r.local_rows?.toLocaleString()}} rows pushed (${{r.remote_before?.toLocaleString()}} → ${{r.remote_after?.toLocaleString()}})</div>`;
                                    }}
                                }});
                                if (data.error) {{
                                    html += `<div class="text-red-400 mt-1">❌ ${{data.error}}</div>`;
                                }}
                                resultDiv.innerHTML = html;
                                resultDiv.classList.remove('hidden');
                            }} else if (data.error) {{
                                resultDiv.innerHTML = `<span class="text-red-400">❌ ${{data.error}}</span>`;
                                resultDiv.classList.remove('hidden');
                            }}
                            
                            // Re-enable button
                            const pushBtn = document.getElementById('pushTrainingBtn');
                            if (pushBtn) {{ pushBtn.disabled = false; pushBtn.classList.remove('opacity-50'); }}
                        }}
                    }});
            }}
            
            function startRebuild() {{
                const btn = document.getElementById('rebuildBtn');
                btn.disabled = true;
                btn.classList.add('opacity-50');
                btn.textContent = '⏳ Starting...';
                fetch('/api/rebuild/start', {{ method: 'POST' }})
                    .then(r => r.json())
                    .then(data => {{
                        if (data.status === 'started') {{
                            document.getElementById('rebuildResult').classList.add('hidden');
                            checkRebuildStatus();
                        }} else if (data.status === 'already_running') {{
                            btn.textContent = '⚠️ Already Running';
                            checkRebuildStatus();
                        }} else {{
                            btn.disabled = false;
                            btn.classList.remove('opacity-50');
                            btn.textContent = '🔨 Run Rebuild';
                        }}
                    }})
                    .catch(err => {{
                        btn.disabled = false;
                        btn.classList.remove('opacity-50');
                        btn.textContent = '🔨 Run Rebuild';
                        document.getElementById('rebuildStatus').innerHTML = '<span class="text-red-400">❌ Failed to start: ' + err + '</span>';
                    }});
            }}
            
            function checkRebuildStatus() {{
                fetch('/api/rebuild/status')
                    .then(r => r.json())
                    .then(data => {{
                        const statusDiv = document.getElementById('rebuildStatus');
                        const progressDiv = document.getElementById('rebuildProgress');
                        const resultDiv = document.getElementById('rebuildResult');
                        const btn = document.getElementById('rebuildBtn');
                        
                        if (data.state === 'running') {{
                            btn.disabled = true;
                            btn.classList.add('opacity-50');
                            progressDiv.classList.remove('hidden');
                            
                            const pct = Math.round((data.current_phase / data.total_phases) * 100);
                            document.getElementById('rebuildBar').style.width = pct + '%';
                            document.getElementById('rebuildPhaseName').textContent = data.current_phase_name || 'Starting...';
                            document.getElementById('rebuildPhaseNum').textContent = `Phase ${{data.current_phase}}/${{data.total_phases}}`;
                            statusDiv.innerHTML = `<span class="text-yellow-400">⏳ Running...</span>` + 
                                (data.started_at ? ` <span class="text-gray-500">Started: ${{new Date(data.started_at).toLocaleTimeString()}}</span>` : '');
                            
                            let phasesHtml = '';
                            (data.phase_results || []).forEach(p => {{
                                const emoji = p.errors && p.errors.length > 0 ? '⚠️' : '✅';
                                phasesHtml += `<div>${{emoji}} Phase ${{p.phase}}: ${{p.name}} (${{p.elapsed_seconds}}s, ${{p.statements_completed}}/${{p.statements_total}} stmts)</div>`;
                            }});
                            document.getElementById('rebuildPhases').innerHTML = phasesHtml;
                            
                            setTimeout(checkRebuildStatus, 2000);
                        }} else if (data.state === 'completed') {{
                            btn.disabled = false;
                            btn.classList.remove('opacity-50');
                            progressDiv.classList.add('hidden');
                            statusDiv.innerHTML = `<span class="text-green-400">✅ Complete!</span> ${{data.row_count?.toLocaleString() || '?'}} rows, ${{data.col_count || '?'}} columns`;
                            
                            let phasesHtml = '';
                            (data.phase_results || []).forEach(p => {{
                                const emoji = p.errors && p.errors.length > 0 ? '⚠️' : '✅';
                                phasesHtml += `<div>${{emoji}} Phase ${{p.phase}}: ${{p.name}} — ${{p.elapsed_seconds}}s (${{p.statements_completed}}/${{p.statements_total}} stmts${{p.errors?.length ? ', ' + p.errors.length + ' errors' : ''}})</div>`;
                            }});
                            resultDiv.innerHTML = phasesHtml;
                            resultDiv.classList.remove('hidden');
                            
                            loadStatus();
                        }} else if (data.state === 'error') {{
                            btn.disabled = false;
                            btn.classList.remove('opacity-50');
                            progressDiv.classList.add('hidden');
                            statusDiv.innerHTML = `<span class="text-red-400">❌ Error: ${{data.error}}</span>`;
                            
                            let phasesHtml = '';
                            (data.phase_results || []).forEach(p => {{
                                const emoji = p.errors && p.errors.length > 0 ? '⚠️' : '✅';
                                phasesHtml += `<div>${{emoji}} Phase ${{p.phase}}: ${{p.name}} — ${{p.elapsed_seconds}}s</div>`;
                            }});
                            resultDiv.innerHTML = phasesHtml;
                            resultDiv.classList.remove('hidden');
                        }} else {{
                            btn.disabled = false;
                            btn.classList.remove('opacity-50');
                            statusDiv.innerHTML = '<span>Status: Idle</span>';
                        }}
                    }});
            }}
            
            function compareRebuild() {{
                fetch('/api/rebuild/compare', {{ method: 'POST' }})
                    .then(r => r.json())
                    .then(data => {{
                        if (data.error) {{ alert('Error: ' + data.error); return; }}
                        const resultDiv = document.getElementById('rebuildResult');
                        resultDiv.innerHTML = `
                            <div class="font-bold mb-1">📊 Table Comparison</div>
                            <div>race_training_dataset: ${{data.old_rows?.toLocaleString()}} rows, ${{data.old_cols}} cols</div>
                            <div>race_training_dataset_new: ${{data.new_rows?.toLocaleString()}} rows, ${{data.new_cols}} cols</div>
                        `;
                        resultDiv.classList.remove('hidden');
                    }});
            }}
            
            function swapRebuild() {{
                const swapBtn = document.querySelector('button[onclick="swapRebuild()"]');
                swapBtn.disabled = true;
                swapBtn.textContent = '⏳ Swapping...';
                fetch('/api/rebuild/swap', {{ method: 'POST' }})
                    .then(r => r.json())
                    .then(data => {{
                        swapBtn.disabled = false;
                        swapBtn.textContent = '🔄 Swap Tables';
                        if (data.error) {{ 
                            document.getElementById('rebuildResult').innerHTML = '<span class="text-red-400">❌ Swap error: ' + data.error + '</span>';
                            return; 
                        }}
                        const rebuildTime = data.rebuild_time_seconds ? `${{data.rebuild_time_seconds}}s` : 'N/A';
                        const rows = data.row_count ? data.row_count.toLocaleString() : '?';
                        const cols = data.col_count || '?';
                        const swapTime = data.swap_time ? new Date(data.swap_time).toLocaleString() : new Date().toLocaleString();
                        document.getElementById('rebuildResult').innerHTML = `
                            <span class="text-green-400">✅ Tables swapped!</span>
                            <div class="mt-1 text-xs text-gray-400">
                                <span>📊 ${{rows}} rows, ${{cols}} cols</span>
                                <span class="ml-3">⏱ Rebuild: ${{rebuildTime}}</span>
                                <span class="ml-3">🕐 Updated: ${{swapTime}}</span>
                            </div>
                            <div class="text-xs text-gray-500 mt-1">Old table available as race_training_dataset_old</div>
                        `;
                        document.getElementById('rebuildResult').classList.remove('hidden');
                        loadStatus();
                    }});
            }}
        </script>
    </body>
    </html>
    """
    return html


@app.post("/api/sync")
async def start_sync_endpoint(request: Request):
    """Start a background sync from Supabase"""
    if sync_status["running"]:
        return {"status": "already_running"}
    
    tables = None
    try:
        body = await request.json()
        if body and "tables" in body:
            tables = body["tables"]
    except:
        pass
    
    # Run in background thread
    thread = threading.Thread(target=run_sync, args=(tables,))
    thread.start()
    
    return {"status": "started"}


@app.get("/api/sync/status")
async def get_sync_status():
    """Get current sync status"""
    return sync_status


@app.get("/api/status")
async def status():
    if not os.path.exists(DUCKDB_PATH):
        return {"error": "No DuckDB file. Click 'Sync All Tables' to start.", "tables": [], "file_size_mb": 0}
    
    try:
        db = get_db()
        tables = db.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()
        
        result = []
        for (name,) in tables:
            rows = db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            cols = db.execute(f"""
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = '{name}'
            """).fetchone()[0]
            result.append({"name": name, "rows": rows, "columns": cols})
        
        db.close()
        
        size_mb = os.path.getsize(DUCKDB_PATH) / (1024 * 1024)
        return {
            "tables": result, 
            "file_size_mb": round(size_mb, 1),
            "last_sync": sync_status.get("last_sync"),
            "table_sync_times": load_sync_times()
        }
    
    except Exception as e:
        return {"error": str(e), "tables": [], "file_size_mb": 0}


@app.delete("/api/table/{name}")
async def drop_table(name: str):
    """Drop a local-only table from DuckDB"""
    from app import TABLES
    if name in TABLES:
        return {"error": "Cannot drop a sync table"}
    try:
        db = get_db_write()
        db.execute(f"DROP TABLE IF EXISTS {name}")
        db.close()
        return {"status": "dropped", "table": name}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/table/{name}/clear")
async def clear_table(name: str):
    """Truncate (clear all rows from) a local table"""
    from app import TABLES
    if name in TABLES:
        return {"error": "Cannot clear a sync table from here"}
    try:
        db = get_db_write()
        db.execute(f"DELETE FROM {name}")
        db.close()
        return {"status": "cleared", "table": name}
    except Exception as e:
        return {"error": str(e)}


# ============================================================================
# ELO SUMMARY API
# ============================================================================

@app.get("/api/elo/summary")
async def elo_summary():
    """Get ELO ratings summary by year from DuckDB"""
    if not os.path.exists(DUCKDB_PATH):
        return {"error": "No DuckDB file"}
    try:
        db = get_db()
        # Check table exists
        tables = [r[0] for r in db.execute("SHOW TABLES").fetchall()]
        if 'horse_elo_ratings' not in tables:
            db.close()
            return {"error": "horse_elo_ratings not in DuckDB", "years": []}

        # Overall stats
        overall = db.execute("""
            SELECT COUNT(*), COUNT(DISTINCT horse_slug),
                   ROUND(MIN(elo_after)::NUMERIC, 1), ROUND(MAX(elo_after)::NUMERIC, 1),
                   ROUND(AVG(elo_after)::NUMERIC, 2), ROUND(STDDEV(elo_after)::NUMERIC, 2)
            FROM horse_elo_ratings
        """).fetchone()

        # Per year
        rows = db.execute("""
            SELECT EXTRACT(YEAR FROM race_date)::INT as year,
                   COUNT(*) as records,
                   COUNT(DISTINCT horse_slug) as horses,
                   ROUND(MIN(elo_after)::NUMERIC, 1) as min_elo,
                   ROUND(MAX(elo_after)::NUMERIC, 1) as max_elo,
                   ROUND(AVG(elo_after)::NUMERIC, 1) as avg_elo
            FROM horse_elo_ratings
            GROUP BY 1 ORDER BY 1
        """).fetchall()

        db.close()

        return {
            "total_records": overall[0],
            "unique_horses": overall[1],
            "min_elo": float(overall[2]) if overall[2] else None,
            "max_elo": float(overall[3]) if overall[3] else None,
            "avg_elo": float(overall[4]) if overall[4] else None,
            "stddev_elo": float(overall[5]) if overall[5] else None,
            "years": [
                {"year": r[0], "records": r[1], "horses": r[2],
                 "min_elo": float(r[3]), "max_elo": float(r[4]), "avg_elo": float(r[5])}
                for r in rows
            ]
        }
    except Exception as e:
        return {"error": str(e), "years": []}


# ============================================================================
# ELO REBUILD IN SUPABASE API
# ============================================================================

@app.post("/api/elo/rebuild")
async def start_elo_rebuild():
    """Start full ELO rebuild in Supabase (wipe + process_elo_by_year)"""
    if elo_rebuild_status["running"]:
        return {"status": "already_running"}
    thread = threading.Thread(target=run_elo_rebuild, daemon=True)
    thread.start()
    return {"status": "started"}


@app.get("/api/elo/rebuild/status")
async def get_elo_rebuild_status():
    """Get current ELO rebuild progress"""
    return elo_rebuild_status


# ============================================================================
# PUSH TO SUPABASE API
# ============================================================================

@app.post("/api/push")
async def start_push(request: Request):
    """Push DuckDB tables to Supabase"""
    if push_status["running"]:
        return {"status": "already_running"}

    tables = None
    try:
        body = await request.json()
        if body and "tables" in body:
            tables = body["tables"]
    except:
        pass

    thread = threading.Thread(target=run_push, args=(tables,), daemon=True)
    thread.start()

    return {"status": "started"}


@app.get("/api/push/status")
async def get_push_status():
    """Get current push status"""
    return push_status


# ============================================================================
# REBUILD API ENDPOINTS
# ============================================================================

def run_rebuild_background():
    """Run rebuild in background thread."""
    try:
        rebuild_runner.run_rebuild()
    except Exception as e:
        print(f"Rebuild error: {e}")


@app.post("/api/rebuild/start")
async def start_rebuild():
    """Start the rebuild pipeline."""
    status = rebuild_runner.get_status()
    if status["state"] == "running":
        return {"status": "already_running"}
    thread = threading.Thread(target=run_rebuild_background, daemon=True)
    thread.start()
    return {"status": "started"}


@app.get("/api/rebuild/status")
async def get_rebuild_status():
    """Get current rebuild status."""
    return rebuild_runner.get_status()


@app.post("/api/rebuild/compare")
async def compare_rebuild():
    """Compare old vs new table."""
    try:
        result = rebuild_runner.compare_tables()
        return {"status": "ok", **result}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/rebuild/swap")
async def swap_rebuild():
    """Swap race_training_dataset_new → race_training_dataset."""
    try:
        rebuild_runner.swap_tables()
        # Update sync timestamp for the dataset
        swap_time = save_sync_time('race_training_dataset')
        # Get rebuild timing from last rebuild status
        status = rebuild_runner.get_status()
        total_time = sum(r.get("elapsed_seconds", 0) for r in status.get("phase_results", []))
        return {
            "status": "swapped",
            "swap_time": swap_time,
            "rebuild_time_seconds": round(total_time, 1),
            "row_count": status.get("row_count", 0),
            "col_count": status.get("col_count", 0),
            "rebuilt_at": status.get("completed_at"),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/table/{name}", response_class=HTMLResponse)
async def view_table(name: str):
    try:
        db = get_db()
        
        # Get sample data
        df = db.execute(f"SELECT * FROM {name} LIMIT 100").fetchdf()
        
        # Get total count
        total = db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        
        db.close()
        
        # Build HTML table
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{name} - RaceAlpha</title>
            <script src="https://cdn.tailwindcss.com"></script>
            <style>
                table {{ font-size: 12px; }}
                th, td {{ padding: 4px 8px; white-space: nowrap; }}
            </style>
        </head>
        <body class="bg-gray-900 text-gray-100 p-8">
            <div class="max-w-full mx-auto">
                <a href="/" class="text-blue-400 hover:underline">← Back</a>
                <h1 class="text-2xl font-bold my-4">{name}</h1>
                <p class="text-gray-400 mb-4">
                    Showing 100 of {total:,} rows
                </p>
                <div class="overflow-x-auto">
                    <table class="bg-gray-800 rounded">
                        <thead>
                            <tr class="border-b border-gray-700">
        """
        
        for col in df.columns:
            html += f'<th class="text-left text-gray-400">{col}</th>'
        
        html += "</tr></thead><tbody>"
        
        for _, row in df.iterrows():
            html += "<tr class='border-b border-gray-800 hover:bg-gray-700'>"
            for val in row:
                display = str(val)[:50] if val is not None else "-"
                html += f"<td>{display}</td>"
            html += "</tr>"
        
        html += """
                        </tbody>
                    </table>
                </div>
            </div>
        </body>
        </html>
        """
        return html
    
    except Exception as e:
        return f"<html><body class='bg-gray-900 text-red-400 p-8'>Error: {e}</body></html>"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
