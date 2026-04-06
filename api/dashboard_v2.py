"""
Universal Contact Form Filler — Web Dashboard v2
Flask backend with all REST API endpoints + Scraper integration.
Run: python dashboard_v2.py
"""

import os
import sys
import csv
import json
import time
import signal
import sqlite3
import hashlib
import secrets
import subprocess
import threading
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import (
    Flask, request, jsonify, session, redirect, url_for,
    render_template, Response, send_from_directory, g
)
from werkzeug.utils import secure_filename

# ============================================================
#   APP SETUP
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_FOLDER = BASE_DIR / "uploads"
UPLOAD_FOLDER.mkdir(exist_ok=True)
DB_PATH = BASE_DIR / "automation.db"
CONFIG_PATH = BASE_DIR / "config.json"
OUTREACH_SCRIPT = BASE_DIR.parent / "Outreach(1).py"  # one level up
SCRAPER_SCRIPT = BASE_DIR.parent / "scraper.py"        # one level up

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = secrets.token_hex(32)
app.config["UPLOAD_FOLDER"] = str(UPLOAD_FOLDER)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB max upload

# ============================================================
#   GLOBAL STATE — active subprocess (outreach)
# ============================================================

active_run = {
    "process": None,
    "run_id": None,
    "logs": [],
    "status": "idle",       # idle | running | completed | stopped | error
    "total_leads": 0,
    "processed": 0,
    "csv_filename": "",
    "start_time": None,
}
run_lock = threading.Lock()

# ============================================================
#   GLOBAL STATE — scraper subprocess
# ============================================================

scraper_state = {
    "process": None,
    "logs": [],
    "status": "idle",       # idle | running | completed | stopped | error
    "total_urls": 0,
    "processed": 0,
    "csv_filename": "",
    "start_time": None,
    "results": [],
}
scraper_lock = threading.Lock()

# ============================================================
#   CONFIG HELPERS
# ============================================================

DEFAULT_CONFIG = {
    "MY_FIRST_NAME": "Hemant",
    "MY_LAST_NAME": "Bansal",
    "MY_FULL_NAME": "Hemant Bansal",
    "MY_EMAIL": "info@hyperstaff.co",
    "MY_PHONE": "7011613319",
    "MY_PHONE_INTL": "+917011613319",
    "MY_COMPANY": "HyperStaff",
    "MY_WEBSITE": "https://hyperstaff.co",
    "OPENAI_API_KEY": "",
    "NOPECHA_API_KEY": "",
    "SPREADSHEET_ID": "",
    "CREDS_FILE": "google_credentials.json",
    "PARALLEL_COUNT": 10,
    "NOPECHA_HARD_TIMEOUT": 300,
    "DASHBOARD_USER": "admin",
    "DASHBOARD_PASS_HASH": hashlib.sha256("admin123".encode()).hexdigest(),
}

def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f:
            saved = json.load(f)
        merged = {**DEFAULT_CONFIG, **saved}
        return merged
    return dict(DEFAULT_CONFIG)

def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

# ============================================================
#   DATABASE
# ============================================================

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(str(DB_PATH))
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()

def init_db():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TEXT NOT NULL,
            end_time TEXT,
            csv_filename TEXT,
            total_leads INTEGER DEFAULT 0,
            successful INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            total_cost REAL DEFAULT 0.0,
            total_tokens INTEGER DEFAULT 0,
            captchas_solved INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            company_name TEXT,
            contact_url TEXT,
            submitted TEXT,
            submission_assurance TEXT,
            captcha_status TEXT,
            proxy_used TEXT,
            bandwidth_kb TEXT,
            run_timestamp TEXT,
            api_calls TEXT,
            input_tokens TEXT,
            output_tokens TEXT,
            total_tokens TEXT,
            est_cost TEXT,
            avg_tokens_call TEXT,
            fields_filled TEXT,
            submission_status TEXT,
            confirmation_msg TEXT,
            message_sent TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (run_id) REFERENCES runs(id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Migration: add message_sent column if missing
    try:
        c.execute("SELECT message_sent FROM results LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE results ADD COLUMN message_sent TEXT")
    conn.commit()

    # Cleanup: mark any stale 'running' runs as 'error' (from crashed sessions)
    stale = c.execute("SELECT COUNT(*) FROM runs WHERE status='running'").fetchone()[0]
    if stale > 0:
        c.execute("UPDATE runs SET status='error', end_time=? WHERE status='running'",
                  (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
        conn.commit()
        print(f"[Dashboard] Cleaned up {stale} stale 'running' runs -> marked as 'error'")

    conn.close()

# ============================================================
#   AUTH HELPERS
# ============================================================

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            if request.is_json or request.headers.get("Accept") == "application/json":
                return jsonify({"error": "Unauthorized"}), 401
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated

# ============================================================
#   ROUTES — Pages
# ============================================================

@app.route("/")
def index():
    return render_template("index_v2.html")

# ============================================================
#   AUTH ROUTES
# ============================================================

@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json() or {}
    username = data.get("username", "")
    password = data.get("password", "")
    cfg = load_config()
    pass_hash = hashlib.sha256(password.encode()).hexdigest()
    if username == cfg.get("DASHBOARD_USER") and pass_hash == cfg.get("DASHBOARD_PASS_HASH"):
        session["logged_in"] = True
        session["username"] = username
        return jsonify({"success": True})
    return jsonify({"success": False, "error": "Invalid credentials"}), 401

@app.route("/api/logout")
def api_logout():
    session.clear()
    return jsonify({"success": True})

@app.route("/api/auth-status")
def auth_status():
    return jsonify({"logged_in": bool(session.get("logged_in")), "username": session.get("username", "")})

# ============================================================
#   DASHBOARD STATS
# ============================================================

@app.route("/api/dashboard-stats")
@login_required
def dashboard_stats():
    db = get_db()
    total_runs = db.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
    total_leads = db.execute("SELECT COALESCE(SUM(total_leads), 0) FROM runs").fetchone()[0]
    total_successful = db.execute("SELECT COALESCE(SUM(successful), 0) FROM runs").fetchone()[0]
    total_failed = db.execute("SELECT COALESCE(SUM(failed), 0) FROM runs").fetchone()[0]
    total_cost = db.execute("SELECT COALESCE(SUM(total_cost), 0.0) FROM runs").fetchone()[0]
    total_tokens = db.execute("SELECT COALESCE(SUM(total_tokens), 0) FROM runs").fetchone()[0]
    captchas = db.execute("SELECT COALESCE(SUM(captchas_solved), 0) FROM runs").fetchone()[0]
    success_rate = round((total_successful / total_leads * 100), 1) if total_leads > 0 else 0

    # Per-run breakdown for chart
    runs_chart = db.execute(
        "SELECT id, start_time, successful, failed FROM runs ORDER BY id DESC LIMIT 20"
    ).fetchall()
    chart_data = [{"run_id": r["id"], "date": r["start_time"], "successful": r["successful"], "failed": r["failed"]} for r in runs_chart]

    # Recent results
    recent = db.execute("""
        SELECT r.company_name, r.contact_url, r.submitted, r.submission_status,
               r.captcha_status, r.est_cost, r.created_at
        FROM results r ORDER BY r.id DESC LIMIT 10
    """).fetchall()
    recent_list = [dict(row) for row in recent]

    return jsonify({
        "total_runs": total_runs,
        "total_leads": total_leads,
        "success_rate": success_rate,
        "total_cost": round(total_cost, 4),
        "total_tokens": total_tokens,
        "captchas_solved": captchas,
        "chart_data": chart_data,
        "recent_activity": recent_list,
    })

# ============================================================
#   CSV UPLOAD
# ============================================================

@app.route("/api/upload-csv", methods=["POST"])
@login_required
def upload_csv():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["file"]
    if not f.filename or not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files are allowed"}), 400

    filename = secure_filename(f.filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{filename}"
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    f.save(filepath)

    # Parse CSV for preview
    rows = []
    total = 0
    try:
        with open(filepath, "r", encoding="utf-8-sig") as csvf:
            reader = csv.DictReader(csvf)
            for row in reader:
                total += 1
                company = row.get("Company Name", row.get("company_name", ""))
                url = row.get("Contact Form URL", row.get("Contact URL Found", row.get("contact_url", "")))
                rows.append({"company_name": company, "contact_url": url})
    except Exception as e:
        return jsonify({"error": f"Failed to parse CSV: {str(e)}"}), 400

    preview = rows[:10]
    return jsonify({
        "filename": filename,
        "total_leads": total,
        "preview": preview,
    })

# ============================================================
#   START / STOP AUTOMATION (Outreach)
# ============================================================

def _parse_result_line(line):
    """Try to parse structured JSON result lines from Outreach.py stdout."""
    if "[RESULT]" in line:
        try:
            json_str = line.split("[RESULT]", 1)[1].strip()
            return json.loads(json_str)
        except Exception:
            pass
    return None

def _run_subprocess(run_id, csv_path, cfg):
    """Background thread that runs Outreach.py and captures output."""
    global active_run

    env = os.environ.copy()
    for key in ["OPENAI_API_KEY", "NOPECHA_API_KEY", "SPREADSHEET_ID", "CREDS_FILE",
                "MY_FIRST_NAME", "MY_LAST_NAME", "MY_FULL_NAME", "MY_EMAIL", "MY_PHONE",
                "MY_PHONE_INTL", "MY_COMPANY", "MY_WEBSITE"]:
        if cfg.get(key):
            env[key] = str(cfg[key])
    env["PARALLEL_COUNT"] = str(cfg.get("PARALLEL_COUNT", 10))
    env["NOPECHA_HARD_TIMEOUT"] = str(cfg.get("NOPECHA_HARD_TIMEOUT", 300))

    try:
        proc = subprocess.Popen(
            [sys.executable, str(OUTREACH_SCRIPT), csv_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
            cwd=str(OUTREACH_SCRIPT.parent),
        )
        with run_lock:
            active_run["process"] = proc

        conn = sqlite3.connect(str(DB_PATH))
        successful = 0
        failed = 0
        total_cost = 0.0
        total_tokens_sum = 0
        captchas = 0
        processed = 0

        for line in proc.stdout:
            line = line.rstrip("\n\r")
            with run_lock:
                active_run["logs"].append(line)
                if len(active_run["logs"]) > 2000:
                    active_run["logs"] = active_run["logs"][-1500:]

            result = _parse_result_line(line)
            if result:
                processed += 1
                with run_lock:
                    active_run["processed"] = processed

                sub_status = result.get("submission_status", "")
                if sub_status and "success" in sub_status.lower():
                    successful += 1
                elif sub_status and "fail" in sub_status.lower():
                    failed += 1

                cost_str = result.get("est_cost", "0")
                try:
                    total_cost += float(cost_str)
                except (ValueError, TypeError):
                    pass

                tokens_str = result.get("total_tokens", "0")
                try:
                    total_tokens_sum += int(tokens_str)
                except (ValueError, TypeError):
                    pass

                cap = result.get("captcha_status", "")
                if cap and "solved" in cap.lower():
                    captchas += 1

                try:
                    conn.execute("""
                        INSERT INTO results (run_id, company_name, contact_url, submitted,
                            submission_assurance, captcha_status, proxy_used, bandwidth_kb,
                            run_timestamp, api_calls, input_tokens, output_tokens, total_tokens,
                            est_cost, avg_tokens_call, fields_filled, submission_status, confirmation_msg,
                            message_sent)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        run_id,
                        result.get("company_name", ""),
                        result.get("contact_url", ""),
                        result.get("submitted", ""),
                        result.get("submission_assurance", ""),
                        result.get("captcha_status", ""),
                        result.get("proxy_used", ""),
                        result.get("bandwidth_kb", ""),
                        result.get("run_timestamp", ""),
                        result.get("api_calls", ""),
                        result.get("input_tokens", ""),
                        result.get("output_tokens", ""),
                        result.get("total_tokens", ""),
                        result.get("est_cost", ""),
                        result.get("avg_tokens_call", ""),
                        result.get("fields_filled", ""),
                        result.get("submission_status", ""),
                        result.get("confirmation_msg", ""),
                        result.get("message_sent", ""),
                    ))
                    conn.commit()
                except Exception as e:
                    print(f"[Dashboard] DB insert error: {e}")

            elif "Processing" in line and "of" in line:
                import re
                m = re.search(r'(\d+)\s+of\s+(\d+)', line)
                if m:
                    with run_lock:
                        active_run["processed"] = int(m.group(1))

            elif "[Sheets] OK:" in line and not result:
                processed += 1
                with run_lock:
                    active_run["processed"] = processed

        proc.wait()
        exit_code = proc.returncode

        with run_lock:
            if active_run["status"] == "running":
                active_run["status"] = "completed" if exit_code == 0 else "error"
            active_run["process"] = None

        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final_status = active_run["status"]
        conn.execute("""
            UPDATE runs SET end_time=?, successful=?, failed=?, total_cost=?,
                total_tokens=?, captchas_solved=?, status=?
            WHERE id=?
        """, (end_time, successful, failed, round(total_cost, 6),
              total_tokens_sum, captchas, final_status, run_id))
        conn.commit()
        conn.close()

    except Exception as e:
        with run_lock:
            active_run["status"] = "error"
            active_run["logs"].append(f"[Dashboard Error] {str(e)}")
            active_run["process"] = None
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("UPDATE runs SET status='error', end_time=? WHERE id=?",
                         (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), run_id))
            conn.commit()
            conn.close()
        except Exception:
            pass


@app.route("/api/start-run", methods=["POST"])
@login_required
def start_run():
    global active_run
    with run_lock:
        if active_run["status"] == "running":
            return jsonify({"error": "A run is already in progress"}), 409

    data = request.get_json() or {}
    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "No CSV filename specified"}), 400

    csv_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    if not os.path.exists(csv_path):
        return jsonify({"error": "CSV file not found"}), 404

    total_leads = 0
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        total_leads = sum(1 for _ in csv.DictReader(f))

    cfg = load_config()
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    db = get_db()
    cur = db.execute(
        "INSERT INTO runs (start_time, csv_filename, total_leads, status) VALUES (?,?,?,?)",
        (start_time, filename, total_leads, "running")
    )
    db.commit()
    run_id = cur.lastrowid

    with run_lock:
        active_run = {
            "process": None,
            "run_id": run_id,
            "logs": [],
            "status": "running",
            "total_leads": total_leads,
            "processed": 0,
            "csv_filename": filename,
            "start_time": start_time,
        }

    t = threading.Thread(target=_run_subprocess, args=(run_id, csv_path, cfg), daemon=True)
    t.start()

    return jsonify({"run_id": run_id, "total_leads": total_leads, "status": "running"})


@app.route("/api/stop-run", methods=["POST"])
@login_required
def stop_run():
    global active_run
    with run_lock:
        if active_run["status"] != "running" or not active_run["process"]:
            return jsonify({"error": "No active run to stop"}), 400
        proc = active_run["process"]
        active_run["status"] = "stopped"
        active_run["logs"].append("[Dashboard] Run stopped by user.")

    try:
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        proc.kill()

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("UPDATE runs SET status='stopped', end_time=? WHERE id=?",
                     (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), active_run["run_id"]))
        conn.commit()
        conn.close()
    except Exception:
        pass

    return jsonify({"success": True, "status": "stopped"})


@app.route("/api/run-status")
@login_required
def run_status():
    with run_lock:
        results = []
        if active_run["run_id"]:
            try:
                conn = sqlite3.connect(str(DB_PATH))
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT * FROM results WHERE run_id=? ORDER BY id", (active_run["run_id"],)
                ).fetchall()
                results = [dict(r) for r in rows]
                conn.close()
            except Exception:
                pass

        return jsonify({
            "run_id": active_run["run_id"],
            "status": active_run["status"],
            "total_leads": active_run["total_leads"],
            "processed": active_run["processed"],
            "csv_filename": active_run["csv_filename"],
            "start_time": active_run["start_time"],
            "results": results,
        })

# ============================================================
#   LIVE LOGS — SSE
# ============================================================

@app.route("/api/logs/stream")
@login_required
def log_stream():
    def generate():
        last_idx = 0
        while True:
            with run_lock:
                current_logs = active_run["logs"]
                status = active_run["status"]

            if last_idx < len(current_logs):
                for line in current_logs[last_idx:]:
                    yield f"data: {json.dumps({'line': line, 'status': status})}\n\n"
                last_idx = len(current_logs)

            if status in ("completed", "stopped", "error", "idle"):
                yield f"data: {json.dumps({'line': '', 'status': status, 'done': True})}\n\n"
                break

            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/api/logs/current")
@login_required
def logs_current():
    with run_lock:
        return jsonify({
            "logs": active_run["logs"][-200:],
            "status": active_run["status"],
        })

# ============================================================
#   RUN HISTORY
# ============================================================

@app.route("/api/runs")
@login_required
def list_runs():
    db = get_db()
    runs = db.execute("SELECT * FROM runs ORDER BY id DESC").fetchall()
    return jsonify([dict(r) for r in runs])

@app.route("/api/runs/<int:run_id>")
@login_required
def run_detail(run_id):
    db = get_db()
    run = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    if not run:
        return jsonify({"error": "Run not found"}), 404
    results = db.execute("SELECT * FROM results WHERE run_id=? ORDER BY id", (run_id,)).fetchall()
    return jsonify({
        "run": dict(run),
        "results": [dict(r) for r in results],
    })

@app.route("/api/runs/<int:run_id>/export")
@login_required
def export_run(run_id):
    db = get_db()
    results = db.execute("SELECT * FROM results WHERE run_id=? ORDER BY id", (run_id,)).fetchall()
    if not results:
        return jsonify({"error": "No results found"}), 404

    import io
    output = io.StringIO()
    writer = csv.writer(output)
    cols = ["Company Name", "Contact URL", "Submitted", "Submission Status",
            "Captcha Status", "Fields Filled", "Est. Cost", "Total Tokens", "Confirmation Msg"]
    writer.writerow(cols)
    for r in results:
        writer.writerow([
            r["company_name"], r["contact_url"], r["submitted"],
            r["submission_status"], r["captcha_status"], r["fields_filled"],
            r["est_cost"], r["total_tokens"], r["confirmation_msg"]
        ])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=run_{run_id}_export.csv"}
    )

# ============================================================
#   SETTINGS
# ============================================================

SECRET_KEYS = {"OPENAI_API_KEY", "NOPECHA_API_KEY"}

@app.route("/api/settings", methods=["GET"])
@login_required
def get_settings():
    cfg = load_config()
    safe = {}
    for k, v in cfg.items():
        if k in SECRET_KEYS and v:
            safe[k] = v[:8] + "••••••••" + v[-4:] if len(str(v)) > 12 else "••••••••"
        elif k == "DASHBOARD_PASS_HASH":
            safe[k] = "••••••••"
        else:
            safe[k] = v
    return jsonify(safe)

@app.route("/api/settings", methods=["POST"])
@login_required
def update_settings():
    data = request.get_json() or {}
    cfg = load_config()

    if "new_password" in data and data["new_password"]:
        cfg["DASHBOARD_PASS_HASH"] = hashlib.sha256(data["new_password"].encode()).hexdigest()
    if "DASHBOARD_USER" in data and data["DASHBOARD_USER"]:
        cfg["DASHBOARD_USER"] = data["DASHBOARD_USER"]

    for key in ["MY_FIRST_NAME", "MY_LAST_NAME", "MY_FULL_NAME", "MY_EMAIL", "MY_PHONE",
                "MY_PHONE_INTL", "MY_COMPANY", "MY_WEBSITE", "SPREADSHEET_ID", "CREDS_FILE",
                "PARALLEL_COUNT", "NOPECHA_HARD_TIMEOUT"]:
        if key in data:
            cfg[key] = data[key]

    for key in SECRET_KEYS:
        if key in data and data[key] and "••••" not in str(data[key]):
            cfg[key] = data[key]

    save_config(cfg)
    return jsonify({"success": True})

@app.route("/api/settings/upload-creds", methods=["POST"])
@login_required
def upload_creds():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["file"]
    if not f.filename.endswith(".json"):
        return jsonify({"error": "Only JSON files allowed"}), 400
    dest = BASE_DIR.parent / "google_credentials.json"
    f.save(str(dest))
    cfg = load_config()
    cfg["CREDS_FILE"] = str(dest)
    save_config(cfg)
    return jsonify({"success": True, "path": str(dest)})

@app.route("/api/settings/test-sheets")
@login_required
def test_sheets():
    cfg = load_config()
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_path = cfg.get("CREDS_FILE", "google_credentials.json")
        if not os.path.isabs(creds_path):
            creds_path = str(BASE_DIR.parent / creds_path)
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(cfg.get("SPREADSHEET_ID", "")).sheet1
        title = sheet.spreadsheet.title
        return jsonify({"success": True, "title": title, "rows": sheet.row_count})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

# ============================================================
#   GOOGLE SHEET VIEWER
# ============================================================

@app.route("/api/sheet-data")
@login_required
def sheet_data():
    cfg = load_config()
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_path = cfg.get("CREDS_FILE", "google_credentials.json")
        if not os.path.isabs(creds_path):
            creds_path = str(BASE_DIR.parent / creds_path)
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(cfg.get("SPREADSHEET_ID", "")).sheet1
        all_data = sheet.get_all_records()

        total_rows = len(all_data)
        success_count = sum(1 for r in all_data if str(r.get("Submission Status", "")).lower() == "successful")
        success_pct = round(success_count / total_rows * 100, 1) if total_rows > 0 else 0
        costs = []
        for r in all_data:
            try:
                costs.append(float(r.get("Est. Cost (USD)", 0)))
            except (ValueError, TypeError):
                pass
        avg_cost = round(sum(costs) / len(costs), 6) if costs else 0

        return jsonify({
            "rows": all_data,
            "total_rows": total_rows,
            "success_pct": success_pct,
            "avg_cost": avg_cost,
        })
    except Exception as e:
        return jsonify({"error": str(e), "rows": []}), 400

# ============================================================
#   SCRAPER (pt.py) — API ENDPOINTS
# ============================================================

def _run_scraper_subprocess(csv_path, workers):
    """Background thread that runs pt.py and captures output."""
    global scraper_state

    try:
        cmd = [sys.executable, str(SCRAPER_SCRIPT)]
        if csv_path:
            cmd.append(csv_path)
        if workers:
            cmd.extend(["--workers", str(workers)])

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(SCRAPER_SCRIPT.parent),
        )
        with scraper_lock:
            scraper_state["process"] = proc

        processed = 0
        results = []

        for line in proc.stdout:
            line = line.rstrip("\n\r")
            with scraper_lock:
                scraper_state["logs"].append(line)
                if len(scraper_state["logs"]) > 2000:
                    scraper_state["logs"] = scraper_state["logs"][-1500:]

            # Parse progress lines like "[W1] [3/10] ✓ FORM" or "[W1] [3/10] — no form"
            import re
            progress_match = re.search(r'\[(\d+)/(\d+)\]', line)
            if progress_match:
                current = int(progress_match.group(1))
                total = int(progress_match.group(2))
                with scraper_lock:
                    scraper_state["processed"] = current
                    scraper_state["total_urls"] = total

            # Detect completion markers
            if "✓ FORM" in line or "— no form" in line:
                processed += 1

        proc.wait()
        exit_code = proc.returncode

        # Read results from the output CSV
        output_csv = str(SCRAPER_SCRIPT.parent / "contact_finder_results_v2.csv")
        if os.path.exists(output_csv):
            try:
                with open(output_csv, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    results = [dict(row) for row in reader]
            except Exception as e:
                print(f"[Scraper] CSV read error: {e}")

        with scraper_lock:
            if scraper_state["status"] == "running":
                scraper_state["status"] = "completed" if exit_code == 0 else "error"
            scraper_state["process"] = None
            scraper_state["results"] = results

    except Exception as e:
        with scraper_lock:
            scraper_state["status"] = "error"
            scraper_state["logs"].append(f"[Scraper Error] {str(e)}")
            scraper_state["process"] = None


@app.route("/api/scraper/upload-csv", methods=["POST"])
@login_required
def scraper_upload_csv():
    """Upload a CSV for the scraper (company websites)."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["file"]
    if not f.filename or not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files are allowed"}), 400

    filename = secure_filename(f.filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"scraper_{timestamp}_{filename}"
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    f.save(filepath)

    # Parse for preview
    rows = []
    total = 0
    try:
        with open(filepath, "r", encoding="utf-8-sig") as csvf:
            reader = csv.DictReader(csvf)
            for row in reader:
                total += 1
                company = row.get("Company Name", row.get("company_name", row.get("company", "")))
                url = row.get("Website URL", row.get("website_url", row.get("company_website",
                      row.get("website", row.get("url", "")))))
                if not company and url:
                    try:
                        from urllib.parse import urlparse
                        company = urlparse(url).netloc.lstrip("www.")
                    except Exception:
                        company = f"Row {total}"
                rows.append({"company_name": company, "website_url": url})
    except Exception as e:
        return jsonify({"error": f"Failed to parse CSV: {str(e)}"}), 400

    preview = rows[:10]
    return jsonify({
        "filename": filename,
        "total_urls": total,
        "preview": preview,
    })


@app.route("/api/scraper/start", methods=["POST"])
@login_required
def scraper_start():
    """Start the scraper (pt.py) as a subprocess."""
    global scraper_state
    with scraper_lock:
        if scraper_state["status"] == "running":
            return jsonify({"error": "Scraper is already running"}), 409

    data = request.get_json() or {}
    filename = data.get("filename")
    workers = data.get("workers", 4)
    urls_text = data.get("urls_text", "")  # direct text input

    csv_path = None
    total_urls = 0

    if urls_text and urls_text.strip():
        # Create a temporary CSV from text input
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_filename = f"scraper_manual_{timestamp}.csv"
        csv_path = os.path.join(app.config["UPLOAD_FOLDER"], temp_filename)

        lines = [l.strip() for l in urls_text.strip().splitlines() if l.strip()]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # Check if lines contain company names (name → url or name, url)
            if any("→" in l or "\t" in l for l in lines):
                writer.writerow(["Company Name", "Website URL"])
                for line in lines:
                    if "→" in line:
                        parts = [p.strip() for p in line.split("→", 1)]
                    else:
                        parts = [p.strip() for p in line.split("\t", 1)]
                    if len(parts) == 2:
                        writer.writerow(parts)
                        total_urls += 1
                    elif parts[0].startswith("http"):
                        writer.writerow(["", parts[0]])
                        total_urls += 1
            else:
                writer.writerow(["company_website"])
                for line in lines:
                    if line:
                        writer.writerow([line])
                        total_urls += 1

    elif filename:
        csv_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        if not os.path.exists(csv_path):
            return jsonify({"error": "CSV file not found"}), 404
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            total_urls = sum(1 for line in f if line.strip()) - 1  # minus header
        if total_urls < 0:
            total_urls = 0
    else:
        # Use default URLs built into pt.py
        total_urls = 7  # default list in pt.py

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with scraper_lock:
        scraper_state = {
            "process": None,
            "logs": [],
            "status": "running",
            "total_urls": total_urls,
            "processed": 0,
            "csv_filename": filename or "manual_input",
            "start_time": start_time,
            "results": [],
        }

    t = threading.Thread(
        target=_run_scraper_subprocess,
        args=(csv_path, workers),
        daemon=True
    )
    t.start()

    return jsonify({"total_urls": total_urls, "status": "running"})


@app.route("/api/scraper/stop", methods=["POST"])
@login_required
def scraper_stop():
    """Stop the running scraper."""
    global scraper_state
    with scraper_lock:
        if scraper_state["status"] != "running" or not scraper_state["process"]:
            return jsonify({"error": "No active scraper to stop"}), 400
        proc = scraper_state["process"]
        scraper_state["status"] = "stopped"
        scraper_state["logs"].append("[Dashboard] Scraper stopped by user.")

    try:
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        proc.kill()

    # Read partial results
    output_csv = str(SCRAPER_SCRIPT.parent / "contact_finder_results_v2.csv")
    partial_results = []
    if os.path.exists(output_csv):
        try:
            with open(output_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                partial_results = [dict(row) for row in reader]
        except Exception:
            pass

    with scraper_lock:
        scraper_state["results"] = partial_results

    return jsonify({"success": True, "status": "stopped"})


@app.route("/api/scraper/status")
@login_required
def scraper_status():
    """Get scraper status and results."""
    with scraper_lock:
        return jsonify({
            "status": scraper_state["status"],
            "total_urls": scraper_state["total_urls"],
            "processed": scraper_state["processed"],
            "csv_filename": scraper_state["csv_filename"],
            "start_time": scraper_state["start_time"],
            "results": scraper_state["results"],
        })


@app.route("/api/scraper/logs")
@login_required
def scraper_logs():
    """Get scraper logs."""
    with scraper_lock:
        return jsonify({
            "logs": scraper_state["logs"][-300:],
            "status": scraper_state["status"],
        })


@app.route("/api/scraper/logs/stream")
@login_required
def scraper_log_stream():
    """SSE stream for scraper logs."""
    def generate():
        last_idx = 0
        while True:
            with scraper_lock:
                current_logs = scraper_state["logs"]
                status = scraper_state["status"]

            if last_idx < len(current_logs):
                for line in current_logs[last_idx:]:
                    yield f"data: {json.dumps({'line': line, 'status': status})}\n\n"
                last_idx = len(current_logs)

            if status in ("completed", "stopped", "error", "idle"):
                yield f"data: {json.dumps({'line': '', 'status': status, 'done': True})}\n\n"
                break

            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/scraper/results")
@login_required
def scraper_results():
    """Read scraper results from the output CSV file."""
    output_csv = str(SCRAPER_SCRIPT.parent / "contact_finder_results_v2.csv")
    results = []
    if os.path.exists(output_csv):
        try:
            with open(output_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                results = [dict(row) for row in reader]
        except Exception as e:
            return jsonify({"error": str(e), "results": []}), 400

    # Compute stats
    total = len(results)
    with_form = sum(1 for r in results if r.get("Has Form", "").lower() == "yes")
    with_captcha = sum(1 for r in results if r.get("Has Captcha", "").lower() == "yes")
    with_contact = sum(1 for r in results if r.get("Contact URL Found", ""))

    return jsonify({
        "results": results,
        "total": total,
        "with_form": with_form,
        "with_captcha": with_captcha,
        "with_contact": with_contact,
    })


@app.route("/api/scraper/export")
@login_required
def scraper_export():
    """Download the scraper results CSV."""
    output_csv = SCRAPER_SCRIPT.parent / "contact_finder_results_v2.csv"
    if not output_csv.exists():
        return jsonify({"error": "No results file found"}), 404
    return send_from_directory(
        str(output_csv.parent),
        output_csv.name,
        mimetype="text/csv",
        as_attachment=True,
        download_name="contact_finder_results.csv"
    )


@app.route("/api/scraper/send-to-outreach", methods=["POST"])
@login_required
def scraper_send_to_outreach():
    """Copy scraper results (with forms) into a CSV suitable for Outreach."""
    output_csv = str(SCRAPER_SCRIPT.parent / "contact_finder_results_v2.csv")
    if not os.path.exists(output_csv):
        return jsonify({"error": "No scraper results found"}), 404

    results = []
    try:
        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            results = [dict(row) for row in reader]
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    # Filter: only rows that have a form
    with_form = [r for r in results if r.get("Has Form", "").lower() == "yes"]
    if not with_form:
        return jsonify({"error": "No companies with contact forms found"}), 400

    # Create outreach CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outreach_filename = f"outreach_from_scraper_{timestamp}.csv"
    outreach_path = os.path.join(app.config["UPLOAD_FOLDER"], outreach_filename)

    with open(outreach_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Company Name", "Contact Form URL"])
        for r in with_form:
            writer.writerow([
                r.get("Company Name", ""),
                r.get("Contact URL Found", ""),
            ])

    return jsonify({
        "success": True,
        "filename": outreach_filename,
        "total_leads": len(with_form),
    })


# ============================================================
#   MAIN
# ============================================================

if __name__ == "__main__":
    init_db()
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
    print("=" * 60)
    print("  Universal Contact Form Filler — Dashboard v2")
    print("  Open http://localhost:5000")
    print("  Default login: admin / admin123")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
