from __future__ import annotations

import csv
import os
import json
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import PyMongoError


BASE_DIR = Path(__file__).resolve().parent
OUTREACH_SCRIPT = BASE_DIR / "Outreach(1).py"
LOG_BUFFER_SIZE = 1000
RESULT_PREFIX = "[RESULT]"


DEFAULT_MONGODB_URI = "mongodb://127.0.0.1:27017"
DEFAULT_MONGODB_DB = "outreach"


def _resolve_mongodb_uri() -> str:
	for key in ("MONGODB_URI", "MONGODB_URL", "MONGO_URL", "DATABASE_URL"):
		candidate = str(os.environ.get(key, "") or "").strip()
		if not candidate:
			continue
		if candidate.startswith("mongodb://") or candidate.startswith("mongodb+srv://"):
			return candidate
	return DEFAULT_MONGODB_URI


def _resolve_mongodb_db_name(uri: str) -> str:
	explicit_name = str(os.environ.get("MONGODB_DB", "") or "").strip()
	if explicit_name:
		return explicit_name

	parsed = urlparse(uri)
	db_name_from_path = parsed.path.lstrip("/").split("/", 1)[0]
	return db_name_from_path or DEFAULT_MONGODB_DB


MONGODB_URI = _resolve_mongodb_uri()
MONGODB_DB_NAME = _resolve_mongodb_db_name(MONGODB_URI)
MONGODB_SCHEME = MONGODB_URI.split("://", 1)[0] if "://" in MONGODB_URI else "mongodb"

_db_available = False
_db_init_error: str | None = None
_mongo_client: MongoClient | None = None
_runs_collection = None
_logs_collection = None


def _init_db() -> None:
	global _db_available, _db_init_error, _mongo_client, _runs_collection, _logs_collection
	try:
		_mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
		_mongo_client.admin.command("ping")
		db = _mongo_client[MONGODB_DB_NAME]
		_runs_collection = db["outreach_runs"]
		_logs_collection = db["outreach_logs"]
		_runs_collection.create_index("run_id", unique=True)
		_runs_collection.create_index("started_at")
		_logs_collection.create_index([("run_id", 1), ("created_at", -1)])
		_db_available = True
		_db_init_error = None
	except Exception as exc:
		if _mongo_client is not None:
			try:
				_mongo_client.close()
			except Exception:
				pass
		_mongo_client = None
		_runs_collection = None
		_logs_collection = None
		_db_available = False
		_db_init_error = str(exc)
		print(f"[DB] Initialization failed: {exc}")


def _db_record_run_start(run_id: str, pid: int, csv_path: str | None, started_at: str) -> None:
	if not _db_available or _runs_collection is None:
		return
	try:
		_runs_collection.update_one(
			{"run_id": run_id},
			{
				"$set": {
					"run_id": run_id,
					"status": "running",
					"pid": pid,
					"csv_path": csv_path,
					"started_at": started_at,
					"finished_at": None,
					"exit_code": None,
				}
			},
			upsert=True,
		)
	except PyMongoError as exc:
		print(f"[DB] Failed to record run start: {exc}")


def _db_update_run_state(run_id: str | None, *, status: str, finished_at: str | None = None, exit_code: int | None = None) -> None:
	if not _db_available or not run_id or _runs_collection is None:
		return
	try:
		_runs_collection.update_one(
			{"run_id": run_id},
			{
				"$set": {
					"status": status,
					"finished_at": finished_at,
					"exit_code": exit_code,
				}
			},
		)
	except PyMongoError as exc:
		print(f"[DB] Failed to update run state: {exc}")


def _db_append_log(run_id: str | None, line: str) -> None:
	if not _db_available or not run_id or _logs_collection is None:
		return
	try:
		_logs_collection.insert_one(
			{
				"run_id": run_id,
				"line": line,
				"created_at": _utc_now_iso(),
			}
		)
	except PyMongoError as exc:
		print(f"[DB] Failed to append log: {exc}")


def _db_get_latest_run() -> dict[str, Any] | None:
	if not _db_available or _runs_collection is None:
		return None
	try:
		doc = _runs_collection.find_one({}, sort=[("started_at", -1)])
		if not doc:
			return None
		return {
			"run_id": doc.get("run_id"),
			"status": doc.get("status"),
			"pid": doc.get("pid"),
			"csv_path": doc.get("csv_path"),
			"started_at": doc.get("started_at"),
			"finished_at": doc.get("finished_at"),
			"exit_code": doc.get("exit_code"),
		}
	except PyMongoError:
		return None


def _db_get_logs(run_id: str, tail: int) -> list[str]:
	if not _db_available or _logs_collection is None:
		return []
	try:
		cursor = _logs_collection.find(
			{"run_id": run_id},
			{"_id": 0, "line": 1},
		).sort("created_at", -1).limit(int(tail))
		rows = [str(row.get("line", "")) for row in cursor]
		return [line for line in reversed(rows) if line]
	except PyMongoError:
		return []


def _materialize_google_credentials_file() -> None:
	raw = str(os.environ.get("GOOGLE_CREDENTIALS_JSON", "") or "").strip()
	if not raw:
		return

	creds_path = BASE_DIR / "google_credentials.json"
	if creds_path.exists():
		return

	try:
		parsed = json.loads(raw)
		creds_path.write_text(json.dumps(parsed), encoding="utf-8")
	except Exception:
		try:
			creds_path.write_text(raw, encoding="utf-8")
		except Exception:
			pass


_materialize_google_credentials_file()
_init_db()

app = FastAPI(
	title="Outreach FastAPI Backend",
	version="1.0.0",
	description="API endpoints to start and monitor Outreach(1).py runs.",
)

_state_lock = threading.Lock()
_process: subprocess.Popen | None = None
_run_id: str | None = None
_started_at: str | None = None
_finished_at: str | None = None
_exit_code: int | None = None
_csv_path: str | None = None
_logs: deque[str] = deque(maxlen=LOG_BUFFER_SIZE)
_total_leads: int = 0
_processed_leads: int = 0
_current_lead: str = "-"
_results: list[dict[str, Any]] = []
_duplicates_skipped: int = 0
_generated_csv_path: str | None = None


class OutreachStartRequest(BaseModel):
	csv_path: str | None = Field(
		default=None,
		description="Optional CSV path. Relative paths are resolved from project root.",
	)
	leads: list[dict[str, str]] | None = Field(
		default=None,
		description="Optional leads payload. If provided, backend builds a run CSV automatically.",
	)
	persona: dict[str, Any] | None = Field(
		default=None,
		description="Optional persona payload used to set runtime environment values.",
	)


def _utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def _parse_cost(value: Any) -> float:
	numeric = str(value or "")
	filtered = "".join(char for char in numeric if char.isdigit() or char in {".", "-"})
	try:
		return float(filtered)
	except Exception:
		return 0.0


def _status_from_result(submitted: str, captcha_status: str, submission_status: str, assurance: str) -> str:
	if str(submitted or "").strip().lower() == "yes":
		return "success"

	combined = f"{captcha_status} {submission_status} {assurance}".lower()
	if (
		"timeout" in combined
		or "captcha" in combined
		or "warning" in combined
		or "not found" in combined
	):
		return "warning"

	return "fail"


def _map_result_payload(payload: dict[str, Any]) -> dict[str, Any]:
	submitted_raw = str(payload.get("submitted") or "No")
	captcha_status = str(payload.get("captcha_status") or "n/a")
	submission_status = str(payload.get("submission_status") or "")
	assurance = str(payload.get("submission_assurance") or "")

	return {
		"companyName": str(payload.get("company_name") or "Unknown"),
		"contactUrl": str(payload.get("contact_url") or ""),
		"submitted": "Yes" if submitted_raw.strip().lower() == "yes" else "No",
		"status": _status_from_result(submitted_raw, captcha_status, submission_status, assurance),
		"captchaStatus": captcha_status,
		"confirmationMsg": str(payload.get("confirmation_msg") or assurance or "-") or "-",
		"estCostUsd": _parse_cost(payload.get("est_cost")),
	}


def _parse_result_line(line: str) -> dict[str, Any] | None:
	if not line.startswith(RESULT_PREFIX):
		return None

	try:
		raw_payload = json.loads(line[len(RESULT_PREFIX) :].strip())
		if not isinstance(raw_payload, dict):
			return None
		return _map_result_payload(raw_payload)
	except Exception:
		return None


def _normalize_url_key(raw_url: str) -> str:
	normalized = str(raw_url or "").strip()
	if not normalized:
		return ""

	try:
		candidate = normalized if normalized.lower().startswith(("http://", "https://")) else f"https://{normalized}"
		parsed = urlparse(candidate)
		host = parsed.hostname.replace("www.", "", 1).lower() if parsed.hostname else ""
		path_name = parsed.path.rstrip("/") or "/"
		return f"{host}{path_name}" if host else normalized.lower()
	except Exception:
		return normalized.lower()


def _prepare_csv_from_leads(leads: list[dict[str, str]], run_id: str) -> tuple[str, int, int]:
	runs_dir = BASE_DIR / ".outreach-runs"
	runs_dir.mkdir(parents=True, exist_ok=True)
	csv_path = runs_dir / f"run-{run_id}.csv"

	seen: set[str] = set()
	duplicates_skipped = 0
	rows: list[tuple[str, str]] = []

	for index, lead in enumerate(leads):
		company_name = str((lead or {}).get("companyName") or "").strip() or f"Lead {index + 1}"
		contact_url = str((lead or {}).get("contactUrl") or "").strip()
		if not contact_url:
			continue

		url_key = _normalize_url_key(contact_url)
		if not url_key:
			continue
		if url_key in seen:
			duplicates_skipped += 1
			continue

		seen.add(url_key)
		rows.append((company_name, contact_url))

	if not rows:
		raise HTTPException(status_code=422, detail="No valid leads were provided")

	with csv_path.open("w", encoding="utf-8", newline="") as handle:
		writer = csv.writer(handle)
		writer.writerow(["Company Name", "Contact URL Found"])
		for row in rows:
			writer.writerow(row)

	return str(csv_path), len(rows), duplicates_skipped


def _count_csv_rows(csv_path: str | None) -> int:
	if not csv_path:
		return 0

	try:
		path_obj = Path(csv_path)
		with path_obj.open("r", encoding="utf-8") as handle:
			lines = [line for line in handle.read().splitlines() if line.strip()]
		if len(lines) <= 1:
			return 0
		return max(0, len(lines) - 1)
	except Exception:
		return 0


def _build_persona_env(persona: dict[str, Any] | None) -> dict[str, str]:
	if not isinstance(persona, dict):
		return {}

	mapping: dict[str, str] = {
		"firstName": "MY_FIRST_NAME",
		"lastName": "MY_LAST_NAME",
		"professionalEmail": "MY_EMAIL",
		"verifiedPhone": "MY_PHONE",
		"company": "MY_COMPANY",
		"website": "MY_WEBSITE",
		"zipCode": "MY_PIN_CODE",
		"jobTitle": "MY_JOB_TITLE",
		"pitchMessage": "PITCH_MESSAGE",
		"id": "CAMPAIGN_ID",
		"title": "CAMPAIGN_TITLE",
		"aiInstruction": "AI_INSTRUCTION",
	}

	env: dict[str, str] = {}
	for key, env_key in mapping.items():
		value = persona.get(key)
		if value is None:
			continue
		text = str(value).strip()
		if text:
			env[env_key] = text

	max_daily = persona.get("maxDailySubmissions")
	if isinstance(max_daily, (int, float)) and int(max_daily) > 0:
		env["OUTREACH_MAX_DAILY_SUBMISSIONS"] = str(int(max_daily))

	full_name = f"{env.get('MY_FIRST_NAME', '')} {env.get('MY_LAST_NAME', '')}".strip()
	if full_name:
		env["MY_FULL_NAME"] = full_name

	return env


def _append_log(line: str) -> None:
	global _processed_leads, _current_lead
	clean = line.rstrip("\r\n")
	if not clean:
		return
	current_run_id = None
	parsed_result = _parse_result_line(clean)
	with _state_lock:
		_logs.append(clean)
		current_run_id = _run_id
		if parsed_result is not None:
			_results.append(parsed_result)
			_processed_leads = len(_results)
			current_lead = str(parsed_result.get("contactUrl") or "").strip() or str(parsed_result.get("companyName") or "-")
			_current_lead = current_lead
	_db_append_log(current_run_id, clean)


def _stream_process_output(proc: subprocess.Popen) -> None:
	if proc.stdout is None:
		return
	for line in proc.stdout:
		_append_log(line)
	proc.stdout.close()


def _refresh_process_state() -> None:
	global _exit_code, _finished_at, _generated_csv_path
	current_run_id = None
	new_exit_code = None
	new_finished_at = None
	generated_csv_path = None
	with _state_lock:
		if _process is None:
			return
		code = _process.poll()
		if code is None:
			return
		if _exit_code is None:
			_exit_code = int(code)
			_finished_at = _utc_now_iso()
			current_run_id = _run_id
			new_exit_code = _exit_code
			new_finished_at = _finished_at
			generated_csv_path = _generated_csv_path
			_generated_csv_path = None

	if new_exit_code is not None:
		status = "completed" if int(new_exit_code) == 0 else "failed"
		_db_update_run_state(current_run_id, status=status, finished_at=new_finished_at, exit_code=new_exit_code)

	if generated_csv_path:
		try:
			Path(generated_csv_path).unlink(missing_ok=True)
		except Exception:
			pass


def _resolve_csv_path(csv_path: str | None) -> str | None:
	if not csv_path:
		return None

	candidate = Path(csv_path).expanduser()
	if not candidate.is_absolute():
		candidate = (BASE_DIR / candidate).resolve()

	if not candidate.exists():
		raise HTTPException(status_code=400, detail=f"CSV file not found: {candidate}")
	if candidate.is_dir():
		raise HTTPException(status_code=400, detail=f"CSV path is a directory: {candidate}")

	return str(candidate)


def _validate_ping_url(url: str) -> str:
	parsed = urlparse(url)
	if parsed.scheme not in {"http", "https"} or not parsed.netloc:
		raise HTTPException(status_code=400, detail="Use a valid http/https URL")
	return url


@app.get("/")
def root() -> dict:
	return {
		"service": "Outreach FastAPI Backend",
		"docs": "/docs",
		"start_endpoint": "/outreach/start",
	}


@app.get("/health")
def health() -> dict:
	_refresh_process_state()
	return {
		"status": "ok",
		"db_connected": _db_available,
		"db_engine": MONGODB_SCHEME,
	}


@app.get("/db/status")
def db_status() -> dict:
	return {
		"db_connected": _db_available,
		"db_engine": MONGODB_SCHEME,
		"db_name": MONGODB_DB_NAME,
		"db_init_error": _db_init_error,
	}


@app.get("/ping")
def ping() -> dict:
	_refresh_process_state()
	with _state_lock:
		running = _process is not None and _process.poll() is None
		return {
			"status": "ok",
			"checked_at": _utc_now_iso(),
			"outreach_running": running,
			"run_id": _run_id,
			"db_connected": _db_available,
		}


@app.get("/endpoint/ping")
def ping_endpoint(
	url: str = Query(..., description="Full http/https URL to ping"),
	timeout: float = Query(default=8.0, ge=1.0, le=30.0),
) -> dict:
	target = _validate_ping_url(url)
	request = urlrequest.Request(target, method="GET", headers={"User-Agent": "OutreachFastAPI/1.0"})
	start = time.perf_counter()

	try:
		with urlrequest.urlopen(request, timeout=timeout) as response:
			status_code = int(response.status)
			reason = str(getattr(response, "reason", ""))
			ok = 200 <= status_code < 400
	except urlerror.HTTPError as exc:
		status_code = int(exc.code)
		reason = str(exc.reason)
		ok = False
	except Exception as exc:
		elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
		return {
			"target": target,
			"ok": False,
			"status_code": None,
			"reason": str(exc),
			"response_time_ms": elapsed_ms,
			"checked_at": _utc_now_iso(),
		}

	elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
	return {
		"target": target,
		"ok": ok,
		"status_code": status_code,
		"reason": reason,
		"response_time_ms": elapsed_ms,
		"checked_at": _utc_now_iso(),
	}


@app.post("/outreach/start")
def start_outreach(payload: OutreachStartRequest) -> dict:
	global _process, _run_id, _started_at, _finished_at, _exit_code, _csv_path
	global _total_leads, _processed_leads, _current_lead, _results, _duplicates_skipped, _generated_csv_path

	if not OUTREACH_SCRIPT.exists():
		raise HTTPException(status_code=500, detail=f"Script not found: {OUTREACH_SCRIPT}")

	requested_csv_path = _resolve_csv_path(payload.csv_path)
	persona_env = _build_persona_env(payload.persona)
	_refresh_process_state()

	with _state_lock:
		if _process is not None and _process.poll() is None:
			raise HTTPException(status_code=409, detail="Outreach run is already in progress")

		run_id = uuid.uuid4().hex[:12]
		csv_arg = requested_csv_path
		total_leads = _count_csv_rows(csv_arg)
		duplicates_skipped = 0
		generated_csv_path = None

		if isinstance(payload.leads, list) and payload.leads:
			generated_csv_path, total_leads, duplicates_skipped = _prepare_csv_from_leads(payload.leads, run_id)
			csv_arg = generated_csv_path

		cmd = [sys.executable, str(OUTREACH_SCRIPT)]
		if csv_arg:
			cmd.append(csv_arg)

		spawn_env = os.environ.copy()
		if persona_env:
			spawn_env.update(persona_env)

		try:
			proc = subprocess.Popen(
				cmd,
				cwd=str(BASE_DIR),
				env=spawn_env,
				stdout=subprocess.PIPE,
				stderr=subprocess.STDOUT,
				text=True,
				bufsize=1,
			)
		except Exception as exc:
			raise HTTPException(status_code=500, detail=f"Failed to start Outreach script: {exc}") from exc

		_process = proc
		_run_id = run_id
		_started_at = _utc_now_iso()
		_finished_at = None
		_exit_code = None
		_csv_path = csv_arg
		_total_leads = int(total_leads)
		_processed_leads = 0
		_current_lead = "-"
		_results = []
		_duplicates_skipped = int(duplicates_skipped)
		_generated_csv_path = generated_csv_path
		_logs.clear()
		_logs.append(f"[{_started_at}] Started: {' '.join(cmd)}")
		if _duplicates_skipped > 0:
			_logs.append(f"[{_started_at}] Skipped {_duplicates_skipped} duplicate lead(s) before execution")
		_db_record_run_start(_run_id, proc.pid, _csv_path, _started_at)

		reader = threading.Thread(target=_stream_process_output, args=(proc,), daemon=True)
		reader.start()

		return {
			"status": "started",
			"run_id": _run_id,
			"pid": proc.pid,
			"csv_path": _csv_path,
			"total_leads": _total_leads,
			"processed_leads": _processed_leads,
			"duplicates_skipped": _duplicates_skipped,
			"started_at": _started_at,
		}


@app.get("/outreach/status")
def outreach_status() -> dict:
	_refresh_process_state()
	current_snapshot = None
	with _state_lock:
		running = _process is not None and _process.poll() is None
		if _total_leads > 0:
			progress = int(round((_processed_leads / _total_leads) * 100))
		elif _processed_leads > 0 and not running:
			progress = 100
		else:
			progress = 0

		current_snapshot = {
			"running": running,
			"run_id": _run_id,
			"pid": _process.pid if _process else None,
			"csv_path": _csv_path,
			"started_at": _started_at,
			"finished_at": _finished_at,
			"exit_code": _exit_code,
			"total_leads": _total_leads,
			"processed_leads": _processed_leads,
			"progress": max(0, min(100, progress)),
			"current_lead": _current_lead,
			"results": list(_results),
			"duplicates_skipped": _duplicates_skipped,
			"captcha_credits_used_today": 0,
			"captcha_credits_limit": 0,
			"captcha_credits_remaining": 0,
			"status": "running" if running else ("completed" if _exit_code == 0 else ("failed" if _exit_code is not None else "idle")),
		}

	if current_snapshot and current_snapshot["run_id"]:
		return current_snapshot

	latest = _db_get_latest_run()
	if latest is not None:
		return {
			"running": False,
			"run_id": latest.get("run_id"),
			"pid": latest.get("pid"),
			"csv_path": latest.get("csv_path"),
			"started_at": latest.get("started_at"),
			"finished_at": latest.get("finished_at"),
			"exit_code": latest.get("exit_code"),
			"total_leads": 0,
			"processed_leads": 0,
			"progress": 0,
			"current_lead": "-",
			"results": [],
			"duplicates_skipped": 0,
			"captcha_credits_used_today": 0,
			"captcha_credits_limit": 0,
			"captcha_credits_remaining": 0,
			"status": latest.get("status") or "unknown",
		}

	return current_snapshot or {
		"running": False,
		"run_id": None,
		"pid": None,
		"csv_path": None,
		"started_at": None,
		"finished_at": None,
		"exit_code": None,
		"total_leads": 0,
		"processed_leads": 0,
		"progress": 0,
		"current_lead": "-",
		"results": [],
		"duplicates_skipped": 0,
		"captcha_credits_used_today": 0,
		"captcha_credits_limit": 0,
		"captcha_credits_remaining": 0,
		"status": "idle",
	}


@app.get("/outreach/logs")
def outreach_logs(
	tail: int = Query(default=200, ge=1, le=1000),
	run_id: str | None = Query(default=None, description="Optional run_id to fetch historical logs"),
) -> dict:
	_refresh_process_state()
	target_run_id = run_id
	fallback_lines: list[str] = []
	with _state_lock:
		if target_run_id is None:
			target_run_id = _run_id
			fallback_lines = list(_logs)[-tail:]

	db_lines = _db_get_logs(target_run_id, tail) if target_run_id else []
	lines = db_lines or fallback_lines
	return {
		"run_id": target_run_id,
		"line_count": len(lines),
		"logs": lines,
	}


@app.post("/outreach/stop")
def stop_outreach() -> dict:
	_refresh_process_state()

	with _state_lock:
		if _process is None or _process.poll() is not None:
			raise HTTPException(status_code=409, detail="No running Outreach process")

		_process.terminate()
		_logs.append(f"[{_utc_now_iso()}] Stop requested")
		_db_update_run_state(_run_id, status="stopping")
		return {
			"status": "stopping",
			"run_id": _run_id,
			"pid": _process.pid,
		}


if __name__ == "__main__":
	import uvicorn

	uvicorn.run("Back:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
