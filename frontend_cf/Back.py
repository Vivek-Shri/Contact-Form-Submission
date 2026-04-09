from __future__ import annotations

import csv
import os
import json
import re
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

import psycopg2
import psycopg2.pool
import psycopg2.extras


BASE_DIR = Path(__file__).resolve().parent
OUTREACH_SCRIPT = BASE_DIR / "Outreach(1).py"
LOG_BUFFER_SIZE = 1000
RESULT_PREFIX = "[RESULT]"
SOCIAL_URL_DETAIL = "Social media URLs are not allowed for outreach leads"
SOCIAL_MEDIA_DOMAINS = {
	"facebook.com",
	"fb.com",
	"linkedin.com",
	"instagram.com",
	"twitter.com",
	"x.com",
	"t.co",
	"youtube.com",
	"youtu.be",
	"tiktok.com",
	"pinterest.com",
	"reddit.com",
	"snapchat.com",
	"whatsapp.com",
	"wa.me",
	"telegram.me",
	"t.me",
	"discord.com",
}

# ---------------------------------------------------------------------------
# Supabase PostgreSQL configuration
# ---------------------------------------------------------------------------
_PG_HOST = os.environ.get("PG_HOST", "db.rhmqhrjbknazyflmbwbv.supabase.co")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PG_DB = os.environ.get("PG_DB", "postgres")
_PG_USER = os.environ.get("PG_USER", "postgres")
_PG_PASSWORD = os.environ.get("PG_PASSWORD", "6?9H#@Dv5W+VTEZ")

_db_available = False
_db_init_error: str | None = None
_pg_pool: psycopg2.pool.ThreadedConnectionPool | None = None


@contextmanager
def _get_conn():
	"""Get a connection from the pool with automatic commit/rollback."""
	if _pg_pool is None:
		raise HTTPException(status_code=503, detail="Database pool not available")
	conn = _pg_pool.getconn()
	try:
		yield conn
		conn.commit()
	except Exception:
		conn.rollback()
		raise
	finally:
		_pg_pool.putconn(conn)


def _dict_cursor(conn):
	"""Create a RealDictCursor for dict-like row access."""
	return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS outreach_runs (
	id SERIAL PRIMARY KEY,
	run_id TEXT UNIQUE NOT NULL,
	status TEXT DEFAULT 'running',
	pid INTEGER,
	csv_path TEXT,
	started_at TEXT,
	finished_at TEXT,
	exit_code INTEGER,
	campaign_id TEXT,
	campaign_title TEXT,
	total_leads INTEGER DEFAULT 0,
	processed_leads INTEGER DEFAULT 0,
	duplicates_skipped INTEGER DEFAULT 0,
	resume_skipped_leads INTEGER DEFAULT 0,
	social_skipped_leads INTEGER DEFAULT 0,
	resumed_from_run_id TEXT
);

CREATE TABLE IF NOT EXISTS outreach_logs (
	id SERIAL PRIMARY KEY,
	run_id TEXT NOT NULL,
	line TEXT,
	created_at TEXT
);

CREATE TABLE IF NOT EXISTS campaigns (
	id SERIAL PRIMARY KEY,
	campaign_id TEXT UNIQUE NOT NULL,
	name TEXT,
	description TEXT DEFAULT '',
	status TEXT DEFAULT 'draft',
	ai_instruction TEXT DEFAULT '',
	max_daily_submissions INTEGER DEFAULT 100,
	search_for_form BOOLEAN DEFAULT FALSE,
	steps JSONB DEFAULT '[]'::jsonb,
	created_at TEXT,
	updated_at TEXT
);

CREATE TABLE IF NOT EXISTS campaign_contacts (
	id SERIAL PRIMARY KEY,
	contact_id TEXT UNIQUE NOT NULL,
	campaign_id TEXT NOT NULL,
	company_name TEXT,
	contact_url TEXT,
	domain TEXT,
	url_key TEXT,
	location TEXT DEFAULT '',
	industry TEXT DEFAULT '',
	notes TEXT DEFAULT '',
	is_interested BOOLEAN DEFAULT FALSE,
	created_at TEXT,
	updated_at TEXT,
	UNIQUE(campaign_id, url_key)
);

CREATE INDEX IF NOT EXISTS idx_runs_started_at ON outreach_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_runs_campaign ON outreach_runs(campaign_id);
CREATE INDEX IF NOT EXISTS idx_logs_run_id ON outreach_logs(run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_campaigns_updated_at ON campaigns(updated_at);
CREATE INDEX IF NOT EXISTS idx_contacts_campaign_id ON campaign_contacts(campaign_id);
CREATE INDEX IF NOT EXISTS idx_contacts_created_at ON campaign_contacts(created_at);
CREATE INDEX IF NOT EXISTS idx_contacts_url_key ON campaign_contacts(url_key);
"""


def _init_db() -> None:
	global _db_available, _db_init_error, _pg_pool
	try:
		_pg_pool = psycopg2.pool.ThreadedConnectionPool(
			minconn=1,
			maxconn=10,
			host=_PG_HOST,
			port=_PG_PORT,
			dbname=_PG_DB,
			user=_PG_USER,
			password=_PG_PASSWORD,
			sslmode="require",
			connect_timeout=10,
		)
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(_CREATE_TABLES_SQL)
		_db_available = True
		_db_init_error = None
		print(f"[DB] Connected to Supabase PostgreSQL at {_PG_HOST}")
	except Exception as exc:
		if _pg_pool is not None:
			try:
				_pg_pool.closeall()
			except Exception:
				pass
		_pg_pool = None
		_db_available = False
		_db_init_error = str(exc)
		print(f"[DB] Initialization failed: {exc}")


def _require_db():
	if not _db_available or _pg_pool is None:
		raise HTTPException(status_code=503, detail="Database is not connected")


# ---------------------------------------------------------------------------
# DB helper functions  (PostgreSQL)
# ---------------------------------------------------------------------------

def _db_record_run_start(
	run_id: str,
	pid: int,
	csv_path: str | None,
	started_at: str,
	*,
	campaign_id: str | None,
	campaign_title: str | None,
	total_leads: int,
	duplicates_skipped: int,
	resume_skipped_leads: int,
	social_skipped_leads: int,
	resumed_from_run_id: str | None,
) -> None:
	if not _db_available or _pg_pool is None:
		return
	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(
					"""INSERT INTO outreach_runs (
						run_id, status, pid, csv_path, started_at,
						campaign_id, campaign_title, total_leads, processed_leads,
						duplicates_skipped, resume_skipped_leads, social_skipped_leads,
						resumed_from_run_id, finished_at, exit_code
					) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
					ON CONFLICT (run_id) DO UPDATE SET
						status=EXCLUDED.status, pid=EXCLUDED.pid, csv_path=EXCLUDED.csv_path,
						started_at=EXCLUDED.started_at, campaign_id=EXCLUDED.campaign_id,
						campaign_title=EXCLUDED.campaign_title, total_leads=EXCLUDED.total_leads,
						processed_leads=EXCLUDED.processed_leads,
						duplicates_skipped=EXCLUDED.duplicates_skipped,
						resume_skipped_leads=EXCLUDED.resume_skipped_leads,
						social_skipped_leads=EXCLUDED.social_skipped_leads,
						resumed_from_run_id=EXCLUDED.resumed_from_run_id,
						finished_at=EXCLUDED.finished_at, exit_code=EXCLUDED.exit_code
					""",
					(run_id, "running", pid, csv_path, started_at,
					 campaign_id, campaign_title, int(total_leads), 0,
					 int(duplicates_skipped), int(resume_skipped_leads),
					 int(social_skipped_leads), resumed_from_run_id, None, None),
				)
	except Exception as exc:
		print(f"[DB] Failed to record run start: {exc}")


def _db_update_run_state(
	run_id: str | None,
	*,
	status: str,
	finished_at: str | None = None,
	exit_code: int | None = None,
	processed_leads: int | None = None,
	total_leads: int | None = None,
	duplicates_skipped: int | None = None,
	resume_skipped_leads: int | None = None,
	social_skipped_leads: int | None = None,
	resumed_from_run_id: str | None = None,
) -> None:
	if not _db_available or not run_id or _pg_pool is None:
		return
	try:
		set_parts = ["status = %s"]
		params: list[Any] = [status]

		if finished_at is not None:
			set_parts.append("finished_at = %s")
			params.append(finished_at)
		if exit_code is not None:
			set_parts.append("exit_code = %s")
			params.append(int(exit_code))
		if processed_leads is not None:
			set_parts.append("processed_leads = %s")
			params.append(max(0, int(processed_leads)))
		if total_leads is not None:
			set_parts.append("total_leads = %s")
			params.append(max(0, int(total_leads)))
		if duplicates_skipped is not None:
			set_parts.append("duplicates_skipped = %s")
			params.append(max(0, int(duplicates_skipped)))
		if resume_skipped_leads is not None:
			set_parts.append("resume_skipped_leads = %s")
			params.append(max(0, int(resume_skipped_leads)))
		if social_skipped_leads is not None:
			set_parts.append("social_skipped_leads = %s")
			params.append(max(0, int(social_skipped_leads)))
		if resumed_from_run_id is not None:
			set_parts.append("resumed_from_run_id = %s")
			params.append(_safe_trim(resumed_from_run_id))

		params.append(run_id)
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(
					f"UPDATE outreach_runs SET {', '.join(set_parts)} WHERE run_id = %s",
					params,
				)
	except Exception as exc:
		print(f"[DB] Failed to update run state: {exc}")


def _db_append_log(run_id: str | None, line: str) -> None:
	if not _db_available or not run_id or _pg_pool is None:
		return
	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(
					"INSERT INTO outreach_logs (run_id, line, created_at) VALUES (%s, %s, %s)",
					(run_id, line, _utc_now_iso()),
				)
	except Exception as exc:
		print(f"[DB] Failed to append log: {exc}")


def _db_get_latest_run() -> dict[str, Any] | None:
	if not _db_available or _pg_pool is None:
		return None
	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute("SELECT * FROM outreach_runs ORDER BY started_at DESC LIMIT 1")
				doc = cur.fetchone()
				if not doc:
					return None
				return {
					"run_id": doc.get("run_id"),
					"status": doc.get("status"),
					"pid": doc.get("pid"),
					"csv_path": doc.get("csv_path"),
					"campaign_id": doc.get("campaign_id"),
					"campaign_title": doc.get("campaign_title"),
					"started_at": doc.get("started_at"),
					"finished_at": doc.get("finished_at"),
					"exit_code": doc.get("exit_code"),
					"total_leads": doc.get("total_leads") or 0,
					"processed_leads": doc.get("processed_leads") or 0,
					"duplicates_skipped": doc.get("duplicates_skipped") or 0,
					"resume_skipped_leads": doc.get("resume_skipped_leads") or 0,
					"social_skipped_leads": doc.get("social_skipped_leads") or 0,
					"resumed_from_run_id": doc.get("resumed_from_run_id"),
				}
	except Exception:
		return None


def _db_get_run(run_id: str) -> dict[str, Any] | None:
	if not _db_available or _pg_pool is None:
		return None
	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"SELECT run_id, campaign_id, status, started_at FROM outreach_runs WHERE run_id = %s LIMIT 1",
					(run_id,),
				)
				return cur.fetchone()
	except Exception:
		return None


def _db_get_latest_resumable_run(campaign_id: str) -> dict[str, Any] | None:
	if not _db_available or _pg_pool is None:
		return None
	if not campaign_id:
		return None
	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"""SELECT run_id, campaign_id, status, started_at
					   FROM outreach_runs
					   WHERE campaign_id = %s AND status NOT IN ('running', 'stopping', 'queued')
					   ORDER BY started_at DESC LIMIT 1""",
					(campaign_id,),
				)
				return cur.fetchone()
	except Exception:
		return None


def _db_get_latest_resumable_run_any() -> dict[str, Any] | None:
	if not _db_available or _pg_pool is None:
		return None
	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"""SELECT run_id, campaign_id, status, started_at
					   FROM outreach_runs
					   WHERE status NOT IN ('running', 'stopping', 'queued')
					   ORDER BY started_at DESC LIMIT 1"""
				)
				return cur.fetchone()
	except Exception:
		return None


def _db_get_processed_url_keys(run_id: str) -> set[str]:
	if not _db_available or _pg_pool is None or not run_id:
		return set()

	keys: set[str] = set()
	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"SELECT line FROM outreach_logs WHERE run_id = %s ORDER BY created_at ASC",
					(run_id,),
				)
				for row in cur:
					line = str(row.get("line") or "")
					parsed = _parse_result_line(line)
					if parsed is None:
						continue
					url_key = _normalize_url_key(str(parsed.get("contactUrl") or ""))
					if url_key:
						keys.add(url_key)
	except Exception:
		return set()

	return keys


def _db_get_logs(run_id: str, tail: int) -> list[str]:
	if not _db_available or _pg_pool is None:
		return []
	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"""SELECT line FROM outreach_logs
					   WHERE run_id = %s
					   ORDER BY created_at DESC LIMIT %s""",
					(run_id, int(tail)),
				)
				rows = [str(row.get("line", "")) for row in cur]
				return [line for line in reversed(rows) if line]
	except Exception:
		return []


# ---------------------------------------------------------------------------
# Google credentials helper (unchanged)
# ---------------------------------------------------------------------------

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
_active_campaign_id: str | None = None
_active_campaign_title: str | None = None
_resume_skipped_leads: int = 0
_social_skipped_leads: int = 0
_resumed_from_run_id: str | None = None


class OutreachStartRequest(BaseModel):
	csv_path: str | None = Field(
		default=None,
		description="Optional CSV path. Relative paths are resolved from project root.",
	)
	leads: list[dict[str, Any]] | None = Field(
		default=None,
		description="Optional leads payload. If provided, backend builds a run CSV automatically.",
	)
	persona: dict[str, Any] | None = Field(
		default=None,
		description="Optional persona payload used to set runtime environment values.",
	)
	resume: bool = Field(
		default=True,
		description="Resume from the latest non-running run for this campaign when possible.",
	)
	resume_from_run_id: str | None = Field(
		default=None,
		description="Optional specific run_id to use as resume bookmark.",
	)
	dedupe_by_domain: bool = Field(
		default=True,
		description="If true, keep only a limited number of URLs per domain while building run CSV.",
	)
	max_urls_per_domain: int = Field(
		default=1,
		ge=1,
		le=20,
		description="Maximum number of URLs to keep per domain when dedupe_by_domain is enabled.",
	)


class CampaignCreateRequest(BaseModel):
	name: str = Field(min_length=1, max_length=140)
	aiInstruction: str = Field(default="", max_length=30000)
	status: str = Field(default="draft", max_length=20)
	maxDailySubmissions: int = Field(default=100, ge=1, le=100000)
	searchForForm: bool = Field(default=False)
	steps: list[str] = Field(default_factory=list)


class CampaignUpdateRequest(BaseModel):
	name: str | None = Field(default=None, min_length=1, max_length=140)
	aiInstruction: str | None = Field(default=None, max_length=30000)
	status: str | None = Field(default=None, max_length=20)
	maxDailySubmissions: int | None = Field(default=None, ge=1, le=100000)
	searchForForm: bool | None = Field(default=None)
	steps: list[str] | None = Field(default=None)


class CampaignContactCreateRequest(BaseModel):
	companyName: str = Field(min_length=1, max_length=200)
	contactUrl: str = Field(min_length=1, max_length=2000)
	location: str | None = Field(default=None, max_length=180)
	industry: str | None = Field(default=None, max_length=180)
	notes: str | None = Field(default=None, max_length=2000)


class ContactUpdateRequest(BaseModel):
	companyName: str | None = Field(default=None, min_length=1, max_length=200)
	isInterested: bool | None = Field(default=None)


class CheckExistsRequest(BaseModel):
	urls: list[str]


class BulkContactsCreateRequest(BaseModel):
	contacts: list[dict[str, Any]]
	campaign_id: Optional[str] = None


def _safe_trim(value: Any) -> str:
	return str(value or "").strip()


def _normalize_campaign_status(raw_status: str) -> str:
	value = _safe_trim(raw_status).lower()
	allowed = {"draft", "active", "paused", "archived"}
	return value if value in allowed else "draft"


def _is_social_domain(domain: str) -> bool:
	host = _safe_trim(domain).lower().replace("www.", "", 1)
	if not host:
		return False
	return any(host == blocked or host.endswith(f".{blocked}") for blocked in SOCIAL_MEDIA_DOMAINS)


def _build_search_clause(search_text: str | None, columns: list[str]) -> tuple[str, list[Any]]:
	"""Build a SQL WHERE clause for ILIKE search across multiple columns."""
	query_text = _safe_trim(search_text)
	if not query_text:
		return "", []
	pattern = f"%{query_text}%"
	clauses = [f"COALESCE({col}, '') ILIKE %s" for col in columns]
	params: list[Any] = [pattern] * len(columns)
	return f"({' OR '.join(clauses)})", params


def _build_pagination_meta(page: int, limit: int, total: int) -> dict[str, int]:
	safe_page = max(1, int(page))
	safe_limit = max(1, int(limit))
	safe_total = max(0, int(total))
	total_pages = max(1, (safe_total + safe_limit - 1) // safe_limit) if safe_total else 1
	return {
		"page": safe_page,
		"limit": safe_limit,
		"total": safe_total,
		"total_pages": total_pages,
	}


def _normalize_contact_url(raw_url: str) -> tuple[str, str, str]:
	value = _safe_trim(raw_url).strip("\"'")
	if not value:
		raise HTTPException(status_code=422, detail="Contact URL is required")

	candidate = value if value.lower().startswith(("http://", "https://")) else f"https://{value.lstrip('/')}"
	parsed = urlparse(candidate)

	if parsed.scheme not in {"http", "https"} or not parsed.netloc:
		raise HTTPException(status_code=422, detail="Use a valid http/https URL")

	host = (parsed.hostname or "").replace("www.", "", 1).lower()
	if not host:
		raise HTTPException(status_code=422, detail="Unable to resolve URL domain")
	if _is_social_domain(host):
		raise HTTPException(status_code=422, detail=SOCIAL_URL_DETAIL)

	path_name = parsed.path.rstrip("/") or "/"
	query = f"?{parsed.query}" if parsed.query else ""
	normalized_url = f"{parsed.scheme}://{parsed.netloc.lower()}{path_name}{query}"
	url_key = f"{host}{path_name}"
	return normalized_url, host, url_key


def _map_campaign_document(
	doc: dict[str, Any],
	*,
	contact_count: int = 0,
	last_run: dict[str, Any] | None = None,
) -> dict[str, Any]:
	return {
		"id": _safe_trim(doc.get("campaign_id")),
		"name": _safe_trim(doc.get("name")),
		"status": _safe_trim(doc.get("status")) or "draft",
		"aiInstruction": _safe_trim(doc.get("ai_instruction")),
		"maxDailySubmissions": int(doc.get("max_daily_submissions") or 100),
		"searchForForm": bool(doc.get("search_for_form") or False),
		"steps": doc.get("steps") or [],
		"contactCount": int(contact_count),
		"createdAt": _safe_trim(doc.get("created_at")),
		"updatedAt": _safe_trim(doc.get("updated_at")),
		"lastRun": last_run,
	}


def _map_contact_document(doc: dict[str, Any]) -> dict[str, Any]:
	return {
		"id": _safe_trim(doc.get("contact_id")),
		"campaignId": _safe_trim(doc.get("campaign_id")),
		"companyName": _safe_trim(doc.get("company_name")),
		"contactUrl": _safe_trim(doc.get("contact_url")),
		"domain": _safe_trim(doc.get("domain")),
		"location": _safe_trim(doc.get("location")),
		"industry": _safe_trim(doc.get("industry")),
		"notes": _safe_trim(doc.get("notes")),
		"isInterested": bool(doc.get("is_interested") or False),
		"createdAt": _safe_trim(doc.get("created_at")),
		"updatedAt": _safe_trim(doc.get("updated_at")),
	}


def _campaign_last_run(campaign_id: str) -> dict[str, Any] | None:
	if not _db_available or _pg_pool is None:
		return None
	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"""SELECT run_id, status, started_at, finished_at, exit_code,
					          total_leads, processed_leads, duplicates_skipped
					   FROM outreach_runs
					   WHERE campaign_id = %s
					   ORDER BY started_at DESC LIMIT 1""",
					(campaign_id,),
				)
				doc = cur.fetchone()
				if not doc:
					return None
				return {
					"runId": _safe_trim(doc.get("run_id")),
					"status": _safe_trim(doc.get("status")) or "unknown",
					"startedAt": _safe_trim(doc.get("started_at")),
					"finishedAt": _safe_trim(doc.get("finished_at")),
					"exitCode": doc.get("exit_code"),
					"totalLeads": int(doc.get("total_leads") or 0),
					"processedLeads": int(doc.get("processed_leads") or 0),
					"duplicatesSkipped": int(doc.get("duplicates_skipped") or 0),
				}
	except Exception:
		return None


def _ensure_campaign_exists(campaign_id: str) -> dict[str, Any]:
	_require_db()
	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"SELECT * FROM campaigns WHERE campaign_id = %s LIMIT 1",
					(campaign_id,),
				)
				doc = cur.fetchone()
				if not doc:
					raise HTTPException(status_code=404, detail="Campaign not found")
				return dict(doc)
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"DB error: {exc}") from exc


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


def _extract_lead_info(lead_data: dict[str, Any]) -> tuple[str, str]:
	if not isinstance(lead_data, dict):
		return "", ""
	normalized = {str(key or "").strip().lower(): _safe_trim(value) for key, value in lead_data.items() if value}
	raw_values = [value for value in normalized.values() if value]

	contact_url = ""
	# 1. Exact popular key matches
	for candidate in ("contact url found", "contact_url_found", "contact url", "contact_url", "contacturl", "url", "website", "site", "domain", "link"):
		if candidate in normalized:
			contact_url = normalized[candidate]
			break

	# 2. Fuzzy key matches
	if not contact_url:
		for key, value in normalized.items():
			if "url" in key or "link" in key or "website" in key or "domain" in key:
				contact_url = value
				break

	# 3. Value-based heuristics
	if not contact_url:
		for value in raw_values:
			lowered = value.lower()
			if lowered.startswith(("http://", "https://")) or ("." in lowered and " " not in value):
				contact_url = value
				break

	company_name = ""
	# 1. Exact popular key matches
	for candidate in ("company name", "company_name", "companyname", "company", "name", "business", "organization", "organisation"):
		if candidate in normalized:
			company_name = normalized[candidate]
			break

	# 2. Value-based heuristics (grab the first text that isn't the URL)
	if not company_name and raw_values:
		for value in raw_values:
			if value != contact_url:
				company_name = value
				break
		if not company_name:
			company_name = raw_values[0]

	return company_name, contact_url


def _read_leads_from_csv(csv_path: str) -> list[dict[str, Any]]:
	path_obj = Path(csv_path)
	if not path_obj.exists() or path_obj.is_dir():
		raise HTTPException(status_code=400, detail=f"CSV file not found: {csv_path}")

	leads: list[dict[str, Any]] = []
	try:
		with path_obj.open("r", encoding="utf-8-sig", newline="") as handle:
			reader = csv.DictReader(handle)
			if not reader.fieldnames:
				return leads

			for row in reader:
				company_name, contact_url = _extract_lead_info(row)
				if contact_url:
					leads.append({"companyName": company_name, "contactUrl": contact_url})
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"Unable to parse CSV leads: {exc}") from exc

	return leads


def _prepare_csv_from_leads(
	leads: list[dict[str, Any]],
	run_id: str,
	*,
	skip_url_keys: set[str] | None = None,
	skip_domains: set[str] | None = None,
	dedupe_by_domain: bool = True,
	max_urls_per_domain: int = 1,
) -> tuple[str, int, int, int, int, int]:
	runs_dir = BASE_DIR / ".outreach-runs"
	runs_dir.mkdir(parents=True, exist_ok=True)
	csv_path = runs_dir / f"run-{run_id}.csv"

	seen: set[str] = set()
	domain_counts: dict[str, int] = {}
	duplicates_skipped = 0
	social_skipped = 0
	resume_skipped = 0
	invalid_skipped = 0
	resume_keys = skip_url_keys or set()
	resume_domain_set = {str(item or "").strip().lower() for item in (skip_domains or set()) if str(item or "").strip()}
	max_domain_rows = max(1, int(max_urls_per_domain))
	rows: list[tuple[str, str]] = []

	for index, lead in enumerate(leads):
		company_name, contact_url = _extract_lead_info(lead or {})
		if not company_name:
			company_name = f"Lead {index + 1}"

		if not contact_url:
			invalid_skipped += 1
			continue

		try:
			normalized_url, domain, url_key = _normalize_contact_url(contact_url)
		except HTTPException as exc:
			detail_text = _safe_trim(getattr(exc, "detail", ""))
			if detail_text == SOCIAL_URL_DETAIL:
				social_skipped += 1
			else:
				invalid_skipped += 1
			continue

		if url_key in seen:
			duplicates_skipped += 1
			continue

		seen.add(url_key)
		if resume_keys and url_key in resume_keys:
			resume_skipped += 1
			continue
		if resume_domain_set and domain in resume_domain_set:
			resume_skipped += 1
			continue

		if dedupe_by_domain:
			domain_count = domain_counts.get(domain, 0)
			if domain_count >= max_domain_rows:
				duplicates_skipped += 1
				continue
			domain_counts[domain] = domain_count + 1

		rows.append((company_name, normalized_url))

	if not rows:
		if resume_skipped > 0:
			raise HTTPException(
				status_code=409,
				detail="Resume bookmark already covers all provided leads; nothing new to process.",
			)
		raise HTTPException(status_code=422, detail="No valid leads were provided")

	with csv_path.open("w", encoding="utf-8", newline="") as handle:
		writer = csv.writer(handle)
		writer.writerow(["Company Name", "Contact URL Found"])
		for row in rows:
			writer.writerow(row)

	return str(csv_path), len(rows), duplicates_skipped, social_skipped, resume_skipped, invalid_skipped


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
	processed_leads = None
	parsed_result = _parse_result_line(clean)
	with _state_lock:
		_logs.append(clean)
		current_run_id = _run_id
		if parsed_result is not None:
			if _active_campaign_id and not parsed_result.get("campaignId"):
				parsed_result["campaignId"] = _active_campaign_id
			if _active_campaign_title and not parsed_result.get("campaignTitle"):
				parsed_result["campaignTitle"] = _active_campaign_title
			_results.append(parsed_result)
			_processed_leads = len(_results)
			processed_leads = _processed_leads
			current_lead = str(parsed_result.get("contactUrl") or "").strip() or str(parsed_result.get("companyName") or "-")
			_current_lead = current_lead
	_db_append_log(current_run_id, clean)
	if current_run_id and processed_leads is not None:
		_db_update_run_state(
			current_run_id,
			status="running",
			processed_leads=processed_leads,
			total_leads=_total_leads,
			duplicates_skipped=_duplicates_skipped,
			resume_skipped_leads=_resume_skipped_leads,
			social_skipped_leads=_social_skipped_leads,
		)


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
	processed_leads = None
	total_leads = None
	duplicates_skipped = None
	resume_skipped_leads = None
	social_skipped_leads = None
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
			processed_leads = _processed_leads
			total_leads = _total_leads
			duplicates_skipped = _duplicates_skipped
			resume_skipped_leads = _resume_skipped_leads
			social_skipped_leads = _social_skipped_leads
			generated_csv_path = _generated_csv_path
			_generated_csv_path = None

	if new_exit_code is not None:
		status = "completed" if int(new_exit_code) == 0 else "failed"
		_db_update_run_state(
			current_run_id,
			status=status,
			finished_at=new_finished_at,
			exit_code=new_exit_code,
			processed_leads=processed_leads,
			total_leads=total_leads,
			duplicates_skipped=duplicates_skipped,
			resume_skipped_leads=resume_skipped_leads,
			social_skipped_leads=social_skipped_leads,
		)

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


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.get("/")
def root() -> dict:
	return {
		"service": "Outreach FastAPI Backend",
		"docs": "/docs",
		"start_endpoint": "/outreach/start",
		"start_endpoint_aliases": ["/api/outreach/start", "/api/start-run"],
	}


@app.get("/health")
def health() -> dict:
	_refresh_process_state()
	return {
		"status": "ok",
		"db_connected": _db_available,
		"db_engine": "postgresql",
	}


@app.get("/db/status")
def db_status() -> dict:
	return {
		"db_connected": _db_available,
		"db_engine": "postgresql",
		"db_name": _PG_DB,
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


# ---------------------------------------------------------------------------
# Campaign endpoints
# ---------------------------------------------------------------------------

@app.get("/campaigns")
@app.get("/api/campaigns")
def list_campaigns(
	q: str | None = Query(default=None),
	page: int = Query(default=1, ge=1),
	limit: int = Query(default=25, ge=1, le=200),
) -> dict:
	_require_db()
	offset = (int(page) - 1) * int(limit)

	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				# Build search filter
				search_clause, search_params = _build_search_clause(
					q, ["campaign_id", "name", "description", "status"]
				)
				where_sql = f"WHERE {search_clause}" if search_clause else ""

				# Count total
				cur.execute(f"SELECT COUNT(*) as cnt FROM campaigns {where_sql}", search_params)
				total = cur.fetchone()["cnt"]

				# Fetch page
				cur.execute(
					f"SELECT * FROM campaigns {where_sql} ORDER BY updated_at DESC OFFSET %s LIMIT %s",
					search_params + [offset, int(limit)],
				)
				docs = cur.fetchall()

				# Get contact counts per campaign
				campaign_ids = [_safe_trim(doc.get("campaign_id")) for doc in docs if _safe_trim(doc.get("campaign_id"))]
				contact_counts: dict[str, int] = {}
				if campaign_ids:
					placeholders = ",".join(["%s"] * len(campaign_ids))
					cur.execute(
						f"""SELECT campaign_id, COUNT(*) as cnt
						    FROM campaign_contacts
						    WHERE campaign_id IN ({placeholders})
						    GROUP BY campaign_id""",
						campaign_ids,
					)
					for row in cur:
						ckey = _safe_trim(row.get("campaign_id"))
						if ckey:
							contact_counts[ckey] = int(row.get("cnt") or 0)

				items: list[dict[str, Any]] = []
				for doc in docs:
					cid = _safe_trim(doc.get("campaign_id"))
					items.append(
						_map_campaign_document(
							doc,
							contact_count=contact_counts.get(cid, 0),
							last_run=_campaign_last_run(cid),
						)
					)

				return {
					"campaigns": items,
					"pagination": _build_pagination_meta(page, limit, total),
					"query": {"q": _safe_trim(q)},
				}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to list campaigns: {exc}") from exc


@app.post("/campaigns")
@app.post("/api/campaigns")
def create_campaign(payload: CampaignCreateRequest) -> dict:
	_require_db()
	now = _utc_now_iso()
	campaign_id = f"cmp-{uuid.uuid4().hex[:10]}"

	doc = {
		"campaign_id": campaign_id,
		"name": _safe_trim(payload.name),
		"status": _normalize_campaign_status(payload.status),
		"ai_instruction": _safe_trim(payload.aiInstruction),
		"max_daily_submissions": int(payload.maxDailySubmissions),
		"search_for_form": bool(payload.searchForForm),
		"steps": payload.steps or [],
		"created_at": now,
		"updated_at": now,
	}

	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(
					"""INSERT INTO campaigns (
						campaign_id, name, status, ai_instruction,
						max_daily_submissions, search_for_form, steps,
						created_at, updated_at
					) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
					(
						doc["campaign_id"], doc["name"], doc["status"],
						doc["ai_instruction"], doc["max_daily_submissions"],
						doc["search_for_form"],
						psycopg2.extras.Json(doc["steps"]),
						doc["created_at"], doc["updated_at"],
					),
				)
		return _map_campaign_document(doc, contact_count=0, last_run=None)
	except psycopg2.IntegrityError as exc:
		raise HTTPException(status_code=409, detail="Campaign ID collision, please retry") from exc
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to create campaign: {exc}") from exc


@app.get("/campaigns/{campaign_id}")
@app.get("/api/campaigns/{campaign_id}")
def get_campaign(campaign_id: str) -> dict:
	doc = _ensure_campaign_exists(campaign_id)

	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"SELECT COUNT(*) as cnt FROM campaign_contacts WHERE campaign_id = %s",
					(campaign_id,),
				)
				count = cur.fetchone()["cnt"]
		return _map_campaign_document(doc, contact_count=count, last_run=_campaign_last_run(campaign_id))
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to fetch campaign: {exc}") from exc


@app.put("/campaigns/{campaign_id}")
@app.put("/api/campaigns/{campaign_id}")
def update_campaign(campaign_id: str, payload: CampaignUpdateRequest) -> dict:
	_require_db()
	_ensure_campaign_exists(campaign_id)

	set_parts: list[str] = []
	params: list[Any] = []
	raw = payload.model_dump(exclude_unset=True)
	for key, value in raw.items():
		if key == "name":
			set_parts.append("name = %s")
			params.append(_safe_trim(value))
		elif key == "aiInstruction":
			set_parts.append("ai_instruction = %s")
			params.append(_safe_trim(value))
		elif key == "status":
			set_parts.append("status = %s")
			params.append(_normalize_campaign_status(str(value)))
		elif key == "maxDailySubmissions" and value is not None:
			set_parts.append("max_daily_submissions = %s")
			params.append(int(value))
		elif key == "searchForForm" and value is not None:
			set_parts.append("search_for_form = %s")
			params.append(bool(value))
		elif key == "steps" and value is not None:
			set_parts.append("steps = %s")
			params.append(psycopg2.extras.Json(value))

	if set_parts:
		set_parts.append("updated_at = %s")
		params.append(_utc_now_iso())
		params.append(campaign_id)
		try:
			with _get_conn() as conn:
				with conn.cursor() as cur:
					cur.execute(
						f"UPDATE campaigns SET {', '.join(set_parts)} WHERE campaign_id = %s",
						params,
					)
		except Exception as exc:
			raise HTTPException(status_code=500, detail=f"Unable to update campaign: {exc}") from exc

	return get_campaign(campaign_id)


@app.delete("/campaigns/{campaign_id}")
@app.delete("/api/campaigns/{campaign_id}")
def delete_campaign(campaign_id: str) -> dict:
	_require_db()
	_ensure_campaign_exists(campaign_id)

	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute("DELETE FROM campaign_contacts WHERE campaign_id = %s", (campaign_id,))
				deleted_contacts = cur.rowcount
				cur.execute("DELETE FROM campaigns WHERE campaign_id = %s", (campaign_id,))
		return {
			"status": "deleted",
			"campaign_id": campaign_id,
			"deleted_contacts": int(deleted_contacts),
		}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to delete campaign: {exc}") from exc


# ---------------------------------------------------------------------------
# Campaign contacts endpoints
# ---------------------------------------------------------------------------

@app.get("/campaigns/{campaign_id}/contacts")
@app.get("/api/campaigns/{campaign_id}/contacts")
def list_campaign_contacts(
	campaign_id: str,
	q: str | None = Query(default=None),
	page: int = Query(default=1, ge=1),
	limit: int = Query(default=5000, ge=1, le=5000),
) -> dict:
	_ensure_campaign_exists(campaign_id)
	offset = (int(page) - 1) * int(limit)

	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				# Build WHERE
				where_parts = ["campaign_id = %s"]
				params: list[Any] = [campaign_id]

				search_clause, search_params = _build_search_clause(
					q, ["company_name", "contact_url", "domain", "location", "industry", "notes"]
				)
				if search_clause:
					where_parts.append(search_clause)
					params.extend(search_params)

				where_sql = "WHERE " + " AND ".join(where_parts)

				# Count
				cur.execute(f"SELECT COUNT(*) as cnt FROM campaign_contacts {where_sql}", params)
				total = cur.fetchone()["cnt"]

				# Fetch
				cur.execute(
					f"SELECT * FROM campaign_contacts {where_sql} ORDER BY updated_at DESC OFFSET %s LIMIT %s",
					params + [offset, int(limit)],
				)
				docs = cur.fetchall()

				return {
					"contacts": [_map_contact_document(doc) for doc in docs],
					"pagination": _build_pagination_meta(page, limit, total),
					"query": {"q": _safe_trim(q), "campaign_id": campaign_id},
				}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to list contacts: {exc}") from exc


@app.post("/campaigns/{campaign_id}/contacts")
@app.post("/api/campaigns/{campaign_id}/contacts")
def create_campaign_contact(campaign_id: str, payload: CampaignContactCreateRequest) -> dict:
	_ensure_campaign_exists(campaign_id)
	normalized_url, domain, url_key = _normalize_contact_url(payload.contactUrl)
	now = _utc_now_iso()
	doc = {
		"contact_id": f"lead-{uuid.uuid4().hex[:10]}",
		"campaign_id": campaign_id,
		"company_name": _safe_trim(payload.companyName),
		"contact_url": normalized_url,
		"domain": domain,
		"url_key": url_key,
		"location": _safe_trim(payload.location),
		"industry": _safe_trim(payload.industry),
		"notes": _safe_trim(payload.notes),
		"created_at": now,
		"updated_at": now,
	}

	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(
					"""INSERT INTO campaign_contacts (
						contact_id, campaign_id, company_name, contact_url,
						domain, url_key, location, industry, notes,
						created_at, updated_at
					) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
					(
						doc["contact_id"], doc["campaign_id"], doc["company_name"],
						doc["contact_url"], doc["domain"], doc["url_key"],
						doc["location"], doc["industry"], doc["notes"],
						doc["created_at"], doc["updated_at"],
					),
				)
		return _map_contact_document(doc)
	except psycopg2.IntegrityError as exc:
		raise HTTPException(status_code=409, detail="Contact URL already exists in this campaign") from exc
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to create contact: {exc}") from exc


@app.post("/campaigns/{campaign_id}/contacts/bulk")
@app.post("/api/campaigns/{campaign_id}/contacts/bulk")
def create_bulk_campaign_contacts(campaign_id: str, payload: BulkContactsCreateRequest) -> dict:
	_ensure_campaign_exists(campaign_id)
	_require_db()

	rows_to_insert = []
	now = _utc_now_iso()
	seen_urls: set[str] = set()

	for item in payload.contacts:
		company_name, contact_url = _extract_lead_info(item)
		if not contact_url:
			continue

		try:
			normalized_url, domain, url_key = _normalize_contact_url(contact_url)
		except HTTPException:
			continue

		if url_key in seen_urls:
			continue
		seen_urls.add(url_key)

		rows_to_insert.append((
			f"lead-{uuid.uuid4().hex[:10]}", campaign_id,
			_safe_trim(company_name) or "Unknown", normalized_url,
			domain, url_key,
			_safe_trim(item.get("location")),
			_safe_trim(item.get("industry")),
			_safe_trim(item.get("notes")),
			now, now,
		))

	inserted = 0
	if rows_to_insert:
		try:
			with _get_conn() as conn:
				with conn.cursor() as cur:
					psycopg2.extras.execute_values(
						cur,
						"""INSERT INTO campaign_contacts (
							contact_id, campaign_id, company_name, contact_url,
							domain, url_key, location, industry, notes,
							created_at, updated_at
						) VALUES %s ON CONFLICT DO NOTHING""",
						rows_to_insert,
					)
					inserted = cur.rowcount
		except Exception:
			pass

	return {"message": f"Successfully processed {len(rows_to_insert)} contacts. Inserted {inserted}."}


@app.delete("/campaigns/{campaign_id}/contacts")
@app.delete("/api/campaigns/{campaign_id}/contacts")
def delete_all_campaign_contacts(campaign_id: str) -> dict:
	_ensure_campaign_exists(campaign_id)

	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute("DELETE FROM campaign_contacts WHERE campaign_id = %s", (campaign_id,))
				deleted_count = cur.rowcount
		return {
			"status": "deleted",
			"deleted_count": deleted_count,
			"campaign_id": campaign_id,
		}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to delete contacts: {exc}") from exc


@app.delete("/campaigns/{campaign_id}/contacts/{contact_id}")
@app.delete("/api/campaigns/{campaign_id}/contacts/{contact_id}")
def delete_campaign_contact(campaign_id: str, contact_id: str) -> dict:
	_ensure_campaign_exists(campaign_id)

	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(
					"DELETE FROM campaign_contacts WHERE campaign_id = %s AND contact_id = %s",
					(campaign_id, contact_id),
				)
				if cur.rowcount == 0:
					raise HTTPException(status_code=404, detail="Contact not found")
		return {
			"status": "deleted",
			"campaign_id": campaign_id,
			"contact_id": contact_id,
		}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to delete contact: {exc}") from exc


@app.patch("/campaigns/{campaign_id}/contacts/{contact_id}")
@app.patch("/api/campaigns/{campaign_id}/contacts/{contact_id}")
def update_campaign_contact(campaign_id: str, contact_id: str, payload: ContactUpdateRequest) -> dict:
	_ensure_campaign_exists(campaign_id)

	set_parts: list[str] = []
	params: list[Any] = []
	if payload.companyName is not None:
		set_parts.append("company_name = %s")
		params.append(_safe_trim(payload.companyName))
	if payload.isInterested is not None:
		set_parts.append("is_interested = %s")
		params.append(bool(payload.isInterested))

	if not set_parts:
		return {"status": "no changes"}

	set_parts.append("updated_at = %s")
	params.append(_utc_now_iso())
	params.extend([campaign_id, contact_id])

	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					f"UPDATE campaign_contacts SET {', '.join(set_parts)} WHERE campaign_id = %s AND contact_id = %s RETURNING *",
					params,
				)
				result = cur.fetchone()
				if not result:
					raise HTTPException(status_code=404, detail="Contact not found")
				return _map_contact_document(result)
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to update contact: {exc}") from exc


# ---------------------------------------------------------------------------
# Global contact endpoints
# ---------------------------------------------------------------------------

@app.get("/contacts")
@app.get("/api/contacts")
def list_all_contacts(
	campaign_id: str | None = Query(default=None),
	q: str | None = Query(default=None),
	page: int = Query(default=1, ge=1),
	limit: int = Query(default=50, ge=1, le=200000),
) -> dict:
	_require_db()
	offset = (int(page) - 1) * int(limit)

	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				# Build campaign name map
				cur.execute("SELECT campaign_id, name FROM campaigns")
				campaign_name_map = {
					_safe_trim(row.get("campaign_id")): _safe_trim(row.get("name")) for row in cur
				}

				# Build WHERE with optional join for campaign name search
				where_parts: list[str] = []
				params: list[Any] = []
				query_text = _safe_trim(q)

				if campaign_id:
					where_parts.append("cc.campaign_id = %s")
					params.append(campaign_id)

				if query_text:
					pattern = f"%{query_text}%"
					search_clauses = [
						"COALESCE(cc.company_name, '') ILIKE %s",
						"COALESCE(cc.contact_url, '') ILIKE %s",
						"COALESCE(cc.domain, '') ILIKE %s",
						"COALESCE(cc.location, '') ILIKE %s",
						"COALESCE(cc.industry, '') ILIKE %s",
						"COALESCE(cc.notes, '') ILIKE %s",
						"COALESCE(c.name, '') ILIKE %s",
					]
					where_parts.append(f"({' OR '.join(search_clauses)})")
					params.extend([pattern] * 7)

				where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

				# Count
				cur.execute(
					f"""SELECT COUNT(*) as cnt
					    FROM campaign_contacts cc
					    LEFT JOIN campaigns c ON cc.campaign_id = c.campaign_id
					    {where_sql}""",
					params,
				)
				total = cur.fetchone()["cnt"]

				# Fetch
				cur.execute(
					f"""SELECT cc.*
					    FROM campaign_contacts cc
					    LEFT JOIN campaigns c ON cc.campaign_id = c.campaign_id
					    {where_sql}
					    ORDER BY cc.updated_at DESC OFFSET %s LIMIT %s""",
					params + [offset, int(limit)],
				)
				contact_docs = cur.fetchall()

				items: list[dict[str, Any]] = []
				for doc in contact_docs:
					mapped = _map_contact_document(doc)
					mapped["campaignName"] = campaign_name_map.get(mapped["campaignId"], "")
					items.append(mapped)

				return {
					"contacts": items,
					"pagination": _build_pagination_meta(page, limit, total),
					"query": {"q": _safe_trim(q), "campaign_id": _safe_trim(campaign_id)},
				}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to list contacts: {exc}") from exc


@app.delete("/api/contacts/{contact_id}")
def delete_contact_global(contact_id: str) -> dict:
	_require_db()
	contact_id_clean = _safe_trim(contact_id)
	if not contact_id_clean:
		raise HTTPException(status_code=400, detail="Invalid contact ID")
	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute("DELETE FROM campaign_contacts WHERE contact_id = %s", (contact_id_clean,))
				if cur.rowcount == 0:
					raise HTTPException(status_code=404, detail="Contact not found")
		return {"message": "Contact deleted successfully"}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to delete contact: {exc}") from exc


@app.delete("/api/contacts")
def delete_all_contacts() -> dict:
	_require_db()
	try:
		with _get_conn() as conn:
			with conn.cursor() as cur:
				cur.execute("DELETE FROM campaign_contacts")
				deleted = cur.rowcount
		return {"message": f"Successfully deleted {deleted} contacts"}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to delete contacts: {exc}") from exc


@app.post("/api/contacts/check-exists")
def check_contacts_exist(payload: CheckExistsRequest) -> dict:
	_require_db()
	query_keys: list[str] = []
	original_map: dict[str, list[str]] = {}

	for url in payload.urls:
		if not url:
			continue
		try:
			_, _, url_key = _normalize_contact_url(str(url))
			query_keys.append(url_key)
			if url_key not in original_map:
				original_map[url_key] = []
			original_map[url_key].append(url)
		except Exception:
			pass

	if not query_keys:
		return {"existing_urls": []}

	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				placeholders = ",".join(["%s"] * len(query_keys))
				cur.execute(
					f"SELECT DISTINCT url_key FROM campaign_contacts WHERE url_key IN ({placeholders})",
					query_keys,
				)
				existing_original: list[str] = []
				for row in cur:
					k = row.get("url_key")
					if k in original_map:
						existing_original.extend(original_map[k])
						del original_map[k]
				return {"existing_urls": existing_original}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to check contacts: {exc}") from exc


@app.post("/api/contacts/bulk")
def create_bulk_contacts(payload: BulkContactsCreateRequest) -> dict:
	_require_db()
	rows_to_insert = []
	now = _utc_now_iso()

	for item in payload.contacts:
		company_name, contact_url = _extract_lead_info(item)
		if not contact_url:
			continue

		try:
			normalized_url, domain, url_key = _normalize_contact_url(contact_url)
		except HTTPException:
			continue

		rows_to_insert.append((
			f"lead-{uuid.uuid4().hex[:10]}",
			_safe_trim(payload.campaign_id) or "",
			_safe_trim(company_name) or "Unknown",
			normalized_url, domain, url_key,
			"", "", "",
			now, now,
		))

	if rows_to_insert:
		try:
			with _get_conn() as conn:
				with conn.cursor() as cur:
					psycopg2.extras.execute_values(
						cur,
						"""INSERT INTO campaign_contacts (
							contact_id, campaign_id, company_name, contact_url,
							domain, url_key, location, industry, notes,
							created_at, updated_at
						) VALUES %s ON CONFLICT DO NOTHING""",
						rows_to_insert,
					)
		except Exception:
			pass

	return {"message": f"Successfully processed {len(rows_to_insert)} contacts"}


# ---------------------------------------------------------------------------
# Campaign runs endpoint
# ---------------------------------------------------------------------------

@app.get("/campaigns/{campaign_id}/runs")
@app.get("/api/campaigns/{campaign_id}/runs")
def list_campaign_runs(
	campaign_id: str,
	limit: int = Query(default=25, ge=1, le=200),
) -> dict:
	_ensure_campaign_exists(campaign_id)
	_require_db()

	try:
		with _get_conn() as conn:
			with _dict_cursor(conn) as cur:
				cur.execute(
					"""SELECT run_id, status, started_at, finished_at, exit_code,
					          total_leads, processed_leads, duplicates_skipped
					   FROM outreach_runs
					   WHERE campaign_id = %s
					   ORDER BY started_at DESC LIMIT %s""",
					(campaign_id, int(limit)),
				)
				items = [
					{
						"runId": _safe_trim(doc.get("run_id")),
						"status": _safe_trim(doc.get("status")) or "unknown",
						"startedAt": _safe_trim(doc.get("started_at")),
						"finishedAt": _safe_trim(doc.get("finished_at")),
						"exitCode": doc.get("exit_code"),
						"totalLeads": int(doc.get("total_leads") or 0),
						"processedLeads": int(doc.get("processed_leads") or 0),
						"duplicatesSkipped": int(doc.get("duplicates_skipped") or 0),
					}
					for doc in cur
				]

				return {"runs": items}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Unable to list campaign runs: {exc}") from exc


# ---------------------------------------------------------------------------
# Outreach run management endpoints
# ---------------------------------------------------------------------------

@app.post("/outreach/start")
@app.post("/api/outreach/start")
@app.post("/api/start-run")
def start_outreach(payload: OutreachStartRequest) -> dict:
	global _process, _run_id, _started_at, _finished_at, _exit_code, _csv_path
	global _total_leads, _processed_leads, _current_lead, _results, _duplicates_skipped, _generated_csv_path
	global _active_campaign_id, _active_campaign_title
	global _resume_skipped_leads, _social_skipped_leads, _resumed_from_run_id

	if not OUTREACH_SCRIPT.exists():
		raise HTTPException(status_code=500, detail=f"Script not found: {OUTREACH_SCRIPT}")

	requested_csv_path = _resolve_csv_path(payload.csv_path)
	persona_env = _build_persona_env(payload.persona)
	_refresh_process_state()

	with _state_lock:
		if _process is not None and _process.poll() is None:
			raise HTTPException(status_code=409, detail="Outreach run is already in progress")

		persona_payload = payload.persona if isinstance(payload.persona, dict) else {}
		campaign_id = _safe_trim(persona_payload.get("id"))
		campaign_title = _safe_trim(persona_payload.get("title"))
		resume_enabled = bool(payload.resume)
		resume_from_run_id = _safe_trim(payload.resume_from_run_id)
		dedupe_by_domain = bool(payload.dedupe_by_domain)
		max_urls_per_domain = max(1, int(payload.max_urls_per_domain or 1))

		run_id = uuid.uuid4().hex[:12]
		csv_arg = requested_csv_path
		total_leads = _count_csv_rows(csv_arg)
		duplicates_skipped = 0
		resume_skipped_leads = 0
		social_skipped_leads = 0
		invalid_skipped_leads = 0
		resumed_from_run_id = None
		generated_csv_path = None
		resume_skip_keys: set[str] = set()
		resume_skip_domains: set[str] = set()

		if resume_enabled:
			resume_source = None
			if resume_from_run_id:
				resume_source = _db_get_run(resume_from_run_id)
			elif campaign_id:
				resume_source = _db_get_latest_resumable_run(campaign_id)
			else:
				resume_source = _db_get_latest_resumable_run_any()
			if resume_from_run_id and resume_source is None:
				raise HTTPException(status_code=404, detail=f"Resume run not found: {resume_from_run_id}")
			if resume_source is not None:
				candidate_run_id = _safe_trim(resume_source.get("run_id"))
				if candidate_run_id and candidate_run_id != run_id:
					resume_skip_keys = _db_get_processed_url_keys(candidate_run_id)
					resume_skip_domains = {
						_safe_trim(key).split("/", 1)[0].lower()
						for key in resume_skip_keys
						if _safe_trim(key)
					}
					resume_skip_domains.discard("")
					if resume_skip_keys:
						resumed_from_run_id = candidate_run_id

		input_leads: list[dict[str, Any]] = []
		if isinstance(payload.leads, list) and payload.leads:
			input_leads = payload.leads
		elif requested_csv_path:
			input_leads = _read_leads_from_csv(requested_csv_path)

		if input_leads:
			(
				generated_csv_path,
				total_leads,
				duplicates_skipped,
				social_skipped_leads,
				resume_skipped_leads,
				invalid_skipped_leads,
			) = _prepare_csv_from_leads(
				input_leads,
				run_id,
				skip_url_keys=resume_skip_keys,
				skip_domains=resume_skip_domains,
				dedupe_by_domain=dedupe_by_domain,
				max_urls_per_domain=max_urls_per_domain,
			)
			csv_arg = generated_csv_path
		elif requested_csv_path:
			raise HTTPException(status_code=422, detail="No readable leads found in provided CSV")

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
		_csv_path = requested_csv_path or csv_arg
		_total_leads = int(total_leads)
		_processed_leads = 0
		_current_lead = "-"
		_results = []
		_duplicates_skipped = int(duplicates_skipped)
		_resume_skipped_leads = int(resume_skipped_leads)
		_social_skipped_leads = int(social_skipped_leads)
		_resumed_from_run_id = resumed_from_run_id or None
		_generated_csv_path = generated_csv_path
		_active_campaign_id = campaign_id or None
		_active_campaign_title = campaign_title or None
		_logs.clear()
		_logs.append(f"[{_started_at}] Started: {' '.join(cmd)}")
		if dedupe_by_domain:
			_logs.append(
				f"[{_started_at}] Domain-level dedupe enabled: max {max_urls_per_domain} URL(s) per domain"
			)
		if _duplicates_skipped > 0:
			_logs.append(f"[{_started_at}] Skipped {_duplicates_skipped} duplicate lead(s) before execution")
		if _social_skipped_leads > 0:
			_logs.append(f"[{_started_at}] Skipped {_social_skipped_leads} social-media lead(s) before execution")
		if _resume_skipped_leads > 0:
			bookmark = _resumed_from_run_id or "latest bookmark"
			_logs.append(f"[{_started_at}] Resume bookmark {bookmark} skipped {_resume_skipped_leads} processed lead(s)")
		if invalid_skipped_leads > 0:
			_logs.append(f"[{_started_at}] Skipped {invalid_skipped_leads} invalid lead row(s) before execution")
		_db_record_run_start(
			_run_id,
			proc.pid,
			_csv_path,
			_started_at,
			campaign_id=_active_campaign_id,
			campaign_title=_active_campaign_title,
			total_leads=_total_leads,
			duplicates_skipped=_duplicates_skipped,
			resume_skipped_leads=_resume_skipped_leads,
			social_skipped_leads=_social_skipped_leads,
			resumed_from_run_id=_resumed_from_run_id,
		)

		reader = threading.Thread(target=_stream_process_output, args=(proc,), daemon=True)
		reader.start()

		return {
			"status": "started",
			"run_id": _run_id,
			"campaign_id": _active_campaign_id,
			"campaign_title": _active_campaign_title,
			"pid": proc.pid,
			"csv_path": _csv_path,
			"total_leads": _total_leads,
			"processed_leads": _processed_leads,
			"duplicates_skipped": _duplicates_skipped,
			"resume_skipped_leads": _resume_skipped_leads,
			"social_skipped_leads": _social_skipped_leads,
			"resumed_from_run_id": _resumed_from_run_id,
			"dedupe_by_domain": dedupe_by_domain,
			"max_urls_per_domain": max_urls_per_domain,
			"started_at": _started_at,
		}


@app.get("/outreach/status")
@app.get("/api/outreach/status")
@app.get("/api/run-status")
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
			"campaign_id": _active_campaign_id,
			"campaign_title": _active_campaign_title,
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
			"resume_skipped_leads": _resume_skipped_leads,
			"social_skipped_leads": _social_skipped_leads,
			"resumed_from_run_id": _resumed_from_run_id,
			"captcha_credits_used_today": 0,
			"captcha_credits_limit": 0,
			"captcha_credits_remaining": 0,
			"status": "running" if running else ("completed" if _exit_code == 0 else ("failed" if _exit_code is not None else "idle")),
		}

	if current_snapshot and current_snapshot["run_id"]:
		return current_snapshot

	latest = _db_get_latest_run()
	if latest is not None:
		total_leads = int(latest.get("total_leads") or 0)
		processed_leads = int(latest.get("processed_leads") or 0)
		progress = int(round((processed_leads / total_leads) * 100)) if total_leads > 0 else 0
		return {
			"running": False,
			"run_id": latest.get("run_id"),
			"campaign_id": latest.get("campaign_id"),
			"campaign_title": latest.get("campaign_title"),
			"pid": latest.get("pid"),
			"csv_path": latest.get("csv_path"),
			"started_at": latest.get("started_at"),
			"finished_at": latest.get("finished_at"),
			"exit_code": latest.get("exit_code"),
			"total_leads": total_leads,
			"processed_leads": processed_leads,
			"progress": max(0, min(100, progress)),
			"current_lead": "-",
			"results": [],
			"duplicates_skipped": int(latest.get("duplicates_skipped") or 0),
			"resume_skipped_leads": int(latest.get("resume_skipped_leads") or 0),
			"social_skipped_leads": int(latest.get("social_skipped_leads") or 0),
			"resumed_from_run_id": latest.get("resumed_from_run_id"),
			"captcha_credits_used_today": 0,
			"captcha_credits_limit": 0,
			"captcha_credits_remaining": 0,
			"status": latest.get("status") or "unknown",
		}

	return current_snapshot or {
		"running": False,
		"run_id": None,
		"campaign_id": None,
		"campaign_title": None,
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
		"resume_skipped_leads": 0,
		"social_skipped_leads": 0,
		"resumed_from_run_id": None,
		"captcha_credits_used_today": 0,
		"captcha_credits_limit": 0,
		"captcha_credits_remaining": 0,
		"status": "idle",
	}


@app.get("/outreach/logs")
@app.get("/api/outreach/logs")
@app.get("/api/run-logs")
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
@app.post("/api/outreach/stop")
@app.post("/api/stop-run")
def stop_outreach() -> dict:
	_refresh_process_state()

	with _state_lock:
		if _process is None or _process.poll() is not None:
			raise HTTPException(status_code=409, detail="No running Outreach process")

		_process.terminate()
		_logs.append(f"[{_utc_now_iso()}] Stop requested")
		_db_update_run_state(
			_run_id,
			status="stopping",
			processed_leads=_processed_leads,
			total_leads=_total_leads,
			duplicates_skipped=_duplicates_skipped,
			resume_skipped_leads=_resume_skipped_leads,
			social_skipped_leads=_social_skipped_leads,
		)
		return {
			"status": "stopping",
			"run_id": _run_id,
			"pid": _process.pid,
		}


if __name__ == "__main__":
	import uvicorn

	uvicorn.run("Back:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
