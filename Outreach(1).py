import asyncio
import csv
import sys
import random
import re
import os
import hashlib
import time
import urllib.parse as _urlparse

# Force UTF-8 stdout on Windows to avoid 'charmap' codec errors with Unicode chars
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
import requests
import json
import ast
import threading
from datetime import datetime
from openai import OpenAI
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def _load_local_env(env_path: str = ".env"):
    """Load KEY=VALUE pairs from a local .env file into process env (without overriding existing vars)."""
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = str(raw_line or "").strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                os.environ[key] = value
    except Exception as e:
        print(f"[Env] Warning: failed loading {env_path}: {e}")


_script_dir = os.path.dirname(os.path.abspath(__file__))
_env_candidates = [
    os.path.join(os.getcwd(), ".env"),
    os.path.join(_script_dir, ".env"),
    os.path.join(_script_dir, "..", ".env"),
    os.path.join(_script_dir, "frontend_cf", ".env"),
]
_env_seen = set()
for _env_path in _env_candidates:
    _norm = os.path.normcase(os.path.abspath(_env_path))
    if _norm in _env_seen:
        continue
    _env_seen.add(_norm)
    _load_local_env(_env_path)

# ============================================================
#   UNIVERSAL CONTACT FORM FILLER - v4
#   Changes from v3:
#   - 10 proxies, each handles ~5 sites (round-robin by worker index)
#   - NopeCHA hard timeout: 300s (resubmit ttl 60s)
#   - Narrow viewport (500px wide) for better form isolation
#   - gpt-5-nano kept for pitch/subject
# ============================================================

OPENAI_API_KEY     = str(os.environ.get("OPENAI_API_KEY", "") or "").strip()
OPENAI_FORM_FILL_MODEL = str(os.environ.get("OPENAI_FORM_FILL_MODEL", "gpt-5-nano") or "gpt-5-nano").strip()
SPREADSHEET_ID     = os.environ.get("SPREADSHEET_ID", "1H5ZyBKwKfoXledQgEDk9LvO4KDXzH3plkeamgEXhrWs")
CREDS_FILE         = str(os.environ.get("CREDS_FILE", "google_credentials.json") or "google_credentials.json").strip()
NOPECHA_API_KEYS   = [
    "sub_1TE68RCRwBwvt6ptOHR2oZ2o",      # Key 1
    "sub_1TGgdMCRwBwvt6pt1NbN2LQx",      # Key 2 (20k solves/day)
]
_nopecha_key_states = {k: True for k in NOPECHA_API_KEYS}
_nopecha_lock = threading.Lock()
_nopecha_idx = 0
_nopecha_credit_lock = threading.Lock()
_nopecha_credit_start = {}
_nopecha_credit_current = {}
_nopecha_run_credit_lock = threading.Lock()
_nopecha_run_credit_left = None


def _resolve_creds_file_path() -> str:
    raw = str(CREDS_FILE or "google_credentials.json").strip() or "google_credentials.json"
    expanded = os.path.expanduser(raw)
    candidates = []

    if os.path.isabs(expanded):
        candidates.append(expanded)
    else:
        candidates.extend(
            [
                os.path.join(os.getcwd(), expanded),
                os.path.join(_script_dir, expanded),
                os.path.join(_script_dir, "frontend_cf", expanded),
                os.path.join(_script_dir, "..", expanded),
            ]
        )

    seen = set()
    for candidate in candidates:
        norm = os.path.normcase(os.path.abspath(candidate))
        if norm in seen:
            continue
        seen.add(norm)
        if os.path.isfile(candidate):
            return os.path.abspath(candidate)

    if candidates:
        return os.path.abspath(candidates[0])
    return os.path.abspath(expanded)

def _next_valid_nopecha_key():
    """Round-robin fetch of the next active API key."""
    global _nopecha_idx
    with _nopecha_lock:
        active_keys = [k for k in NOPECHA_API_KEYS if _nopecha_key_states.get(k, False)]
        if not active_keys:
            return None
        key = active_keys[_nopecha_idx % len(active_keys)]
        _nopecha_idx += 1
        return key

def _disable_nopecha_key(key):
    with _nopecha_lock:
        _nopecha_key_states[key] = False


def _record_nopecha_credit(key: str, credit):
    try:
        c = int(float(credit))
    except Exception:
        return
    with _nopecha_credit_lock:
        if key not in _nopecha_credit_start:
            _nopecha_credit_start[key] = c
        _nopecha_credit_current[key] = c


def _nopecha_credit_totals() -> tuple[str, str]:
    with _nopecha_credit_lock:
        if not _nopecha_credit_current:
            return "", ""
        used = 0
        left = 0
        for k, cur in _nopecha_credit_current.items():
            start = _nopecha_credit_start.get(k, cur)
            used += max(start - cur, 0)
            left += max(cur, 0)
    return str(used), str(left)


def _nopecha_parse_int(value) -> int | None:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _observe_nopecha_left_int() -> int | None:
    _, left_total = _nopecha_credit_totals()
    return _nopecha_parse_int(left_total)


def _peek_stable_nopecha_credit_left() -> str:
    global _nopecha_run_credit_left
    with _nopecha_run_credit_lock:
        observed = _observe_nopecha_left_int()
        if observed is not None:
            if _nopecha_run_credit_left is None:
                _nopecha_run_credit_left = observed
            else:
                # Keep this monotonic so sheet values don't jump upward across keys/workers.
                _nopecha_run_credit_left = min(_nopecha_run_credit_left, observed)

        if _nopecha_run_credit_left is None:
            return ""
        return str(max(0, int(_nopecha_run_credit_left)))


def _consume_nopecha_credit_for_row(captcha_status: str) -> tuple[str, str]:
    global _nopecha_run_credit_left
    row_used = _nopecha_solves_consumed(captcha_status) * NOPECHA_CREDIT_PER_SOLVE

    with _nopecha_run_credit_lock:
        observed = _observe_nopecha_left_int()
        if observed is not None:
            if _nopecha_run_credit_left is None:
                _nopecha_run_credit_left = observed
            else:
                _nopecha_run_credit_left = min(_nopecha_run_credit_left, observed)

        if row_used > 0 and _nopecha_run_credit_left is not None:
            _nopecha_run_credit_left = max(int(_nopecha_run_credit_left) - int(row_used), 0)

        left_text = "" if _nopecha_run_credit_left is None else str(max(0, int(_nopecha_run_credit_left)))

    return str(max(0, int(row_used))), left_text


def _refresh_nopecha_credit_snapshot():
    for key in NOPECHA_API_KEYS:
        try:
            r = requests.get("https://api.nopecha.com/status", params={"key": key}, timeout=8)
            data = r.json()
            credit = data.get("credit", (data.get("data") or {}).get("credit", None))
            if credit is not None:
                _record_nopecha_credit(key, credit)
        except Exception:
            continue

    # Prime stable run-level counter from fresh snapshot.
    _peek_stable_nopecha_credit_left()

MY_FIRST_NAME    = os.environ.get("MY_FIRST_NAME", "Uttam Kumar")
MY_LAST_NAME     = os.environ.get("MY_LAST_NAME", "Tiwari")
MY_FULL_NAME     = os.environ.get("MY_FULL_NAME", "Uttam Kumar Tiwari")
_MY_EMAIL_RAW    = os.environ.get("MY_EMAIL", "uttam.tiwari@mail.hyperstaff.co")
MY_EMAIL         = re.sub(r"\s+", "", str(_MY_EMAIL_RAW or "").strip().lower())
if not re.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$", MY_EMAIL):
    MY_EMAIL = "uttam.tiwari@mail.hyperstaff.co"
MY_PHONE         = os.environ.get("MY_PHONE", "347-997-9083")
MY_PHONE_INTL    = os.environ.get("MY_PHONE_INTL", "+1347-997-9083")
MY_PHONE_INTL_E164 = "+" + re.sub(r"\D+", "", str(MY_PHONE_INTL or ""))
if MY_PHONE_INTL_E164 == "+":
    MY_PHONE_INTL_E164 = "+13479979083"
MY_PIN_CODE      = os.environ.get("MY_PIN_CODE", "110032")
MY_PHONE_DISPLAY = os.environ.get("MY_PHONE_DISPLAY", "+1(347) 997-9083")
_MY_COUNTRY_DIAL_RAW = os.environ.get("MY_COUNTRY_DIAL_CODE", "")
_MY_COUNTRY_DIAL_DIGITS = re.sub(r"\D+", "", str(_MY_COUNTRY_DIAL_RAW or ""))
if not _MY_COUNTRY_DIAL_DIGITS:
    _intl_seed = re.sub(r"\D+", "", str(MY_PHONE_INTL_E164 or ""))
    if _intl_seed.startswith("971"):
        _MY_COUNTRY_DIAL_DIGITS = "971"
    elif _intl_seed.startswith("91"):
        _MY_COUNTRY_DIAL_DIGITS = "91"
    elif _intl_seed.startswith("44"):
        _MY_COUNTRY_DIAL_DIGITS = "44"
    elif _intl_seed.startswith("61"):
        _MY_COUNTRY_DIAL_DIGITS = "61"
    elif _intl_seed.startswith("1"):
        _MY_COUNTRY_DIAL_DIGITS = "1"
    elif _intl_seed:
        _MY_COUNTRY_DIAL_DIGITS = _intl_seed[:1]
MY_COUNTRY_DIAL_CODE = f"+{_MY_COUNTRY_DIAL_DIGITS or '1'}"
MY_COMPANY       = os.environ.get("MY_COMPANY", "HyperStaff")
MY_WEBSITE       = os.environ.get("MY_WEBSITE", "https://hyperstaff.co")
MY_ADDRESS       = os.environ.get("MY_ADDRESS", "NEW DELHI, INDIA")
MY_TITLE         = os.environ.get("MY_TITLE", "Virtual Assistant Support for {company_name} - {MY_COMPANY}")
PARALLEL_COUNT = 10   # 10 workers, one per proxy
USE_PROXY = str(os.environ.get("USE_PROXY", "1")).strip().lower() not in {"0", "false", "no", "off"}
ENABLE_CONTACT_DISCOVERY = str(os.environ.get("ENABLE_CONTACT_DISCOVERY", "1")).strip().lower() not in {"0", "false", "no", "off"}

# â”€â”€ Narrow viewport: isolates form sections like the RXR screenshot â”€â”€
VIEWPORT_WIDTH  = 500   # narrow = fewer distractions, form columns stack vertically
VIEWPORT_HEIGHT = 300


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, default) or default).strip())
    except Exception:
        return int(default)


BANDWIDTH_SOFT_LIMIT_KB = max(300, _env_int("BANDWIDTH_SOFT_LIMIT_KB", 650))
BANDWIDTH_HARD_CAP_KB = max(BANDWIDTH_SOFT_LIMIT_KB + 20, _env_int("BANDWIDTH_HARD_CAP_KB", 850))
MAX_MAIN_SCRIPT_REQ = max(2, _env_int("MAX_MAIN_SCRIPT_REQ", 12))
MAX_MAIN_XHR_REQ = max(2, _env_int("MAX_MAIN_XHR_REQ", 14))
MAX_ALLOWED_HOST_SCRIPT_REQ = max(1, _env_int("MAX_ALLOWED_HOST_SCRIPT_REQ", 6))
MAX_ALLOWED_HOST_XHR_REQ = max(1, _env_int("MAX_ALLOWED_HOST_XHR_REQ", 8))

SHEET_HEADERS = [
    "Website URL",
    "Contact Page URL",
    "Contact Form Present",
    "Input Tokens",
    "Output Tokens",
    "Bandwidth Taken",
    "Submitted w/o Captcha",
    "Captcha Present",
    "Nopecha Credits Left",
    "Captcha Solved",
    "Submitted with Captcha",
    "Time taken",
    "Proxy Used",
    "Submission Content",
    "Response",
    "Reason for Failure",
    "Timestamp",
    "Submitted Overall",
]

# tok_cols layout from get_token_columns():
#   [0]=timestamp [1]=calls [2]=input [3]=output [4]=total [5]=cost [6]=avg

COST_PER_1M_INPUT  = 0.150
COST_PER_1M_OUTPUT = 0.600
TOKEN_LOG_FILE     = "token_usage.csv"
RUN_BOOKMARK_FILE  = os.path.join(".outreach-runs", "resume-bookmark.json")
NOPECHA_DEBUG_LOG_FILE = os.path.join(".outreach-runs", "nopecha_debug.txt")
_nopecha_file_lock = threading.Lock()

HONEYPOT_FIELD_RE = re.compile(
    r"(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|do.?not.?fill|leave.?blank|nospam)",
    re.I,
)
ECHO_FIELD_VALUE_RE = re.compile(
    r"^(fname|lname|firstname|lastname|name|email|mail|phone|phno|mobile|address|city|state|zip|pincode|postal|comment|comments|message|subject)$",
    re.I,
)


def _short_field_key(raw_key: str) -> str:
    short_k = re.sub(r'input\[name=["\']([^"\']+)["\']\]', r'\1', str(raw_key or ""))
    short_k = re.sub(r'textarea\[name=["\']([^"\']+)["\']\]', r'\1', short_k)
    short_k = re.sub(r'select\[name=["\']([^"\']+)["\']\]', r'\1', short_k)
    short_k = short_k.lstrip('#').strip()
    return short_k


def _is_honeypot_identifier(text: str) -> bool:
    return bool(HONEYPOT_FIELD_RE.search(str(text or "")))


def _is_low_signal_field_value(field_key: str, field_value: str) -> bool:
    key_norm = re.sub(r"[^a-z0-9]+", "", str(field_key or "").lower())
    value_norm = re.sub(r"[^a-z0-9]+", "", str(field_value or "").lower())

    if not value_norm:
        return True
    if key_norm and value_norm == key_norm:
        return True

    if ECHO_FIELD_VALUE_RE.match(value_norm):
        if key_norm.endswith(value_norm) or value_norm.endswith(key_norm):
            return True

    return False


def _format_field_for_logs(raw_key: str, raw_value, value_limit: int) -> str | None:
    value_text = str(raw_value or "").strip()
    if not value_text:
        return None

    short_k = _short_field_key(raw_key)
    if not short_k:
        return None

    if _is_honeypot_identifier(raw_key) or _is_honeypot_identifier(short_k):
        return None

    if _is_low_signal_field_value(short_k, value_text):
        return None

    return f"{short_k}: {value_text[:value_limit]}"


def _is_internal_log_key(raw_key: str) -> bool:
    key = str(raw_key or "").strip()
    return key.startswith("_")


def _format_submission_fields(filled_fields: dict, max_items: int = 25, value_limit: int = 180) -> str:
    if not filled_fields or not isinstance(filled_fields, dict):
        return "- none"

    primary_lines = []
    internal_lines = []
    for k, v in filled_fields.items():
        entry = _format_field_for_logs(k, v, value_limit=value_limit)
        if not entry:
            continue
        line = f"- {entry}"
        if _is_internal_log_key(k):
            internal_lines.append(line)
        else:
            primary_lines.append(line)
        if (len(primary_lines) + len(internal_lines)) >= max_items:
            break

    if primary_lines:
        remaining = max(0, max_items - len(primary_lines))
        lines = primary_lines + internal_lines[:remaining]
    else:
        lines = internal_lines[:max_items]

    return "\n".join(lines) if lines else "- none"


def _nopecha_log_abs_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), NOPECHA_DEBUG_LOG_FILE)


def _mask_secret(value: str | None, head: int = 6, tail: int = 6) -> str:
    text = str(value or "")
    if not text:
        return ""
    if len(text) <= (head + tail + 3):
        return text
    return f"{text[:head]}...{text[-tail:]}({len(text)})"


def _nopecha_log(message: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [T{threading.get_ident()}] {message}"
    try:
        path = _nopecha_log_abs_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with _nopecha_file_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        pass

# â”€â”€ HARD TOKEN LIMITS PER CALL â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
MAX_INPUT_TOKENS  = 999
MAX_OUTPUT_TOKENS = 260  # default low-budget planning call
MAX_OUTPUT_TOKENS_RECOVERY = 700  # one recovery call when output is truncated/invalid
FORM_FILL_MAX_INPUT_TOKENS = max(900, min(2000, _env_int("FORM_FILL_MAX_INPUT_TOKENS", 2000)))
FORM_FILL_INPUT_BUDGET_TARGET = max(800, FORM_FILL_MAX_INPUT_TOKENS - 120)
FORM_FILL_MAX_OUTPUT_TOKENS = max(120, min(390, _env_int("FORM_FILL_MAX_OUTPUT_TOKENS", 360)))
FORM_FILL_MAX_OUTPUT_TOKENS_RECOVERY = max(
    FORM_FILL_MAX_OUTPUT_TOKENS,
    min(390, _env_int("FORM_FILL_MAX_OUTPUT_TOKENS_RECOVERY", FORM_FILL_MAX_OUTPUT_TOKENS + 20)),
)
FORM_FILL_FIELD_CATALOG_LIMIT = max(8, min(30, _env_int("FORM_FILL_FIELD_CATALOG_LIMIT", 18)))

# â”€â”€ NopeCHA hard timeout (seconds) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
NOPECHA_HARD_TIMEOUT = 300   # hard kill for captcha solve attempts
NOPECHA_CREDIT_SNAPSHOT_ON_START = str(os.environ.get("NOPECHA_CREDIT_SNAPSHOT_ON_START", "1")).strip().lower() in {"1", "true", "yes", "on"}
NOPECHA_CREDIT_PER_SOLVE = max(0, int(str(os.environ.get("NOPECHA_CREDIT_PER_SOLVE", "20") or "20").strip() or "20"))

# â”€â”€ React/Vue fill JS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
REACT_FILL_JS = """
(function(el, value) {
    var nativeInputValueSetter = Object.getOwnPropertyDescriptor(
        window.HTMLInputElement.prototype, 'value'
    );
    var nativeTextAreaValueSetter = Object.getOwnPropertyDescriptor(
        window.HTMLTextAreaElement.prototype, 'value'
    );
    var setter = el.tagName === 'TEXTAREA' ? nativeTextAreaValueSetter : nativeInputValueSetter;
    if (setter && setter.set) {
        setter.set.call(el, value);
    } else {
        el.value = value;
    }
    ['input', 'change', 'blur', 'keyup', 'keydown'].forEach(function(evtName) {
        var evt = new Event(evtName, { bubbles: true, cancelable: true });
        el.dispatchEvent(evt);
    });
    try {
        var inputEvt = new InputEvent('input', { bubbles: true, cancelable: true, data: value });
        el.dispatchEvent(inputEvt);
    } catch(e) {}
})
"""

EXTRACT_FIELDS_JS = """
() => {
    var results = [];
    var seen = new Set();

    function processElement(el, depth) {
        if (depth > 10) return;
        var tag = el.tagName ? el.tagName.toLowerCase() : '';
        if (!['input', 'textarea', 'select'].includes(tag)) return;
        var type = (el.type || '').toLowerCase();
        if (['hidden', 'submit', 'button', 'image', 'reset', 'file', 'search'].includes(type)) return;

        var id = el.id || '';
        var name = el.name || '';

        // Skip search bars by name/id/placeholder
        var nm = (el.name || el.id || el.placeholder || '').toLowerCase();
        if (/\bsearch\b|sf_s|zip.?code|keyword|flexdata/.test(nm)) return;
        if (/(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/.test(nm)) return;
        // Also skip if selector itself is #search or contains 'search'
        if (id.toLowerCase().includes('search')) return;
        if (name.toLowerCase().includes('search') || name.toLowerCase().includes('sf_s')) return;

        // Skip elements inside nav/header/search containers
        var inNav = false;
        var p = el.parentElement;
        for (var d = 0; d < 8 && p; d++) {
            var tag = (p.tagName || '').toLowerCase();
            var cls = (p.className || '').toLowerCase();
            var pid = (p.id || '').toLowerCase();
            if (tag === 'nav' || tag === 'header' ||
                cls.includes('search') || pid.includes('search') ||
                cls.includes('nav') || cls.includes('header')) {
                inNav = true;
                break;
            }
            p = p.parentElement;
        }
        if (inNav) return;
        if (el.name && (el.name.includes('g-recaptcha') || el.name.includes('h-captcha'))) return;

        var rect = el.getBoundingClientRect();
        var visible = true;
        try {
            var cs = window.getComputedStyle(el);
            visible = rect.width > 0 && rect.height > 0 && cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
        } catch (e) {
            visible = rect.width > 0 && rect.height > 0;
        }
        if (depth === 0 && !visible) return;
        if (!visible) return;

        var key = tag + '|' + id + '|' + name + '|' + Math.round(rect.y);
        if (seen.has(key)) return;
        seen.add(key);

        var lbl = el.getAttribute('aria-label') || el.placeholder || '';
        if (!lbl && id) {
            var labelEl = document.querySelector('label[for="' + id + '"]');
            if (labelEl) lbl = labelEl.innerText.trim();
        }
        if (!lbl) {
            var parent = el.parentElement;
            for (var i = 0; i < 4 && parent; i++) {
                var prev = el.previousElementSibling;
                if (prev && ['LABEL','SPAN','P','DIV'].includes(prev.tagName || '')) {
                    var t = prev.innerText ? prev.innerText.trim() : '';
                    if (t && t.length < 60) { lbl = t; break; }
                }
                parent = parent.parentElement;
            }
        }

        var sel = '';
        if (id) {
            sel = '#' + CSS.escape(id);
        } else if (name) {
            sel = tag + '[name="' + name + '"]';
        } else {
            sel = tag + ':nth-of-type(' + (results.length + 1) + ')';
        }

        var required = false;
        try {
            required = !!el.required || el.matches(':required') || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
        } catch (e) {
            required = !!el.required;
        }

        var autoComplete = String(el.getAttribute('autocomplete') || '').trim().slice(0, 40);
        var inputMode = String(el.getAttribute('inputmode') || '').trim().slice(0, 24);
        var pattern = String(el.getAttribute('pattern') || '').trim().slice(0, 60);
        var maxLength = parseInt(String(el.getAttribute('maxlength') || '-1'), 10);
        if (!Number.isFinite(maxLength) || maxLength < 0) maxLength = null;

        var opts = [];
        if (tag === 'select') {
            Array.from(el.options || []).slice(0, 8).forEach(function(o) {
                var t = o.text.trim();
                if (t && !/^(--|choose|select|please)/i.test(t)) opts.push(t);
            });
        }

        results.push({
            sel: sel,
            label: lbl.replace(/[*\n]/g, '').trim().slice(0, 40),
            tag: tag,
            type: type,
            name: name.slice(0, 30),
            id: id.slice(0, 30),
            required: required,
            autocomplete: autoComplete,
            inputmode: inputMode,
            pattern: pattern,
            maxlength: maxLength,
            visible: visible,
            options: opts,
            y: Math.round(rect.y)
        });
    }

    document.querySelectorAll('input, textarea, select').forEach(function(el) {
        processElement(el, 0);
    });

    function walkShadow(root, depth) {
        if (depth > 3) return;
        root.querySelectorAll('*').forEach(function(el) {
            if (el.shadowRoot) {
                el.shadowRoot.querySelectorAll('input, textarea, select').forEach(function(f) {
                    processElement(f, depth + 1);
                });
                walkShadow(el.shadowRoot, depth + 1);
            }
        });
    }
    walkShadow(document, 0);

    results.sort(function(a, b) { return a.y - b.y; });
    return results;
}
"""


# ============================================================
#   TOKEN TRACKER
# ============================================================

class TokenTracker:

    def __init__(self):
        self._lock        = threading.Lock()
        self.total_input  = 0
        self.total_output = 0
        self.total_calls  = 0
        self.worker_totals = {}

        with open(TOKEN_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "timestamp", "company", "call_type",
                "prompt_tokens", "completion_tokens", "total_tokens",
                "est_cost_usd", "cumulative_cost_usd",
            ])

    def record(self, company: str, call_type: str, usage, worker_index: int = -1):
        def _usage_int(*names: str) -> int:
            for name in names:
                val = None
                if usage is None:
                    continue
                try:
                    val = getattr(usage, name)
                except Exception:
                    val = None
                if val is None and isinstance(usage, dict):
                    val = usage.get(name)
                if val is None and hasattr(usage, "get"):
                    try:
                        val = usage.get(name)
                    except Exception:
                        val = None
                try:
                    iv = int(val)
                    if iv >= 0:
                        return iv
                except Exception:
                    pass
            return 0

        # Support both legacy (prompt/completion) and newer (input/output) usage fields.
        pt  = _usage_int("prompt_tokens", "input_tokens")
        ct  = _usage_int("completion_tokens", "output_tokens")
        tt  = _usage_int("total_tokens")
        if tt <= 0:
            tt = pt + ct
        if pt == 0 and ct == 0 and tt > 0:
            # If only total is available, keep visibility by attributing total to input.
            pt = tt
        est = (pt * COST_PER_1M_INPUT + ct * COST_PER_1M_OUTPUT) / 1_000_000

        with self._lock:
            self.total_input  += pt
            self.total_output += ct
            self.total_calls  += 1
            if worker_index >= 0:
                w = self.worker_totals.setdefault(worker_index, {"input":0,"output":0,"calls":0})
                w["input"]  += pt
                w["output"] += ct
                w["calls"]  += 1
            cum_cost = (
                self.total_input  * COST_PER_1M_INPUT +
                self.total_output * COST_PER_1M_OUTPUT
            ) / 1_000_000
            try:
                with open(TOKEN_LOG_FILE, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        company, call_type, pt, ct, tt,
                        f"{est:.6f}", f"{cum_cost:.6f}",
                    ])
            except Exception as e:
                print(f"   [Tokens] CSV write error: {e}")

        print(
            f"   [Tokens] [{call_type}] {company[:20]:<20} | "
            f"in={pt:>5} out={ct:>4} cost=${est:.5f} | cum=${cum_cost:.4f}"
        )

    def get_token_columns(self) -> list:
        """
        Returns exactly 7 items:
        [timestamp, api_calls, input_tokens, output_tokens, total_tokens, cost_usd, avg_tokens_per_call]
        """
        with self._lock:
            cum_in   = self.total_input
            cum_out  = self.total_output
            cum_tot  = cum_in + cum_out
            cum_cost = (cum_in * COST_PER_1M_INPUT + cum_out * COST_PER_1M_OUTPUT) / 1_000_000
            calls    = self.total_calls
            avg_tok  = (cum_tot // calls) if calls else 0
        return [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # [0] Run Timestamp
            calls,                                          # [1] API Calls
            cum_in,                                         # [2] Input Tokens
            cum_out,                                        # [3] Output Tokens
            cum_tot,                                        # [4] Total Tokens
            round(cum_cost, 6),                             # [5] Est. Cost (USD)
            avg_tok,                                        # [6] Avg Tokens/Call
        ]
    def get_worker_columns(self, worker_index: int) -> list:
        with self._lock:
            w = self.worker_totals.get(worker_index, {"input":0,"output":0,"calls":0})
            cum_in  = w["input"]
            cum_out = w["output"]
            cum_tot = cum_in + cum_out
            calls   = w["calls"]
            cost    = (cum_in * COST_PER_1M_INPUT + cum_out * COST_PER_1M_OUTPUT) / 1_000_000
            avg_tok = (cum_tot // calls) if calls else 0
        return [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            calls,
            cum_in,
            cum_out,
            cum_tot,
            round(cost, 6),
            avg_tok,
        ]
    def get_snapshot(self, worker_index: int) -> dict:
        with self._lock:
            w = self.worker_totals.get(worker_index, {"input":0,"output":0,"calls":0})
        return {
            "input": w["input"],
            "output": w["output"],
            "calls": w["calls"],
        }

    def get_delta_columns(self, before: dict, worker_index: int) -> list:
        """Return token/cost delta for one worker since `before` snapshot."""
        with self._lock:
            w = self.worker_totals.get(worker_index, {"input":0,"output":0,"calls":0})
            d_in    = w["input"]  - before["input"]
            d_out   = w["output"] - before["output"]
            d_tot   = d_in + d_out
            d_calls = w["calls"]  - before["calls"]
            d_cost  = (d_in * COST_PER_1M_INPUT + d_out * COST_PER_1M_OUTPUT) / 1_000_000
            avg     = (d_tot // d_calls) if d_calls else 0
        return [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            d_calls, d_in, d_out, d_tot,
            round(d_cost, 6), avg,
        ]

    def print_summary(self):
        cols = self.get_token_columns()
        print()
        print("=" * 60)
        print("  TOKEN USAGE SUMMARY")
        print("=" * 60)
        print(f"  Total API calls     : {cols[1]}")
        print(f"  Total input tokens  : {cols[2]:,}")
        print(f"  Total output tokens : {cols[3]:,}")
        print(f"  Total tokens        : {cols[4]:,}")
        print(f"  Estimated cost      : ${cols[5]:.4f}")
        print(f"  Avg tokens / call   : {cols[6]:,}")
        print(f"  Log saved to        : {TOKEN_LOG_FILE}")
        print()
        print("  Per-worker breakdown:")
        with self._lock:
            for widx in sorted(self.worker_totals):
                w = self.worker_totals[widx]
                tot = w["input"] + w["output"]
                cost = (w["input"] * COST_PER_1M_INPUT + w["output"] * COST_PER_1M_OUTPUT) / 1_000_000
                print(f"    Worker #{widx}: calls={w['calls']} tokens={tot:,} cost=${cost:.5f}")
        print("=" * 60)

token_tracker = TokenTracker()
_STOP_FLAG = threading.Event()
OUTREACH_MAX_DAILY_SUBMISSIONS = max(0, int(str(os.environ.get("OUTREACH_MAX_DAILY_SUBMISSIONS", "0") or "0").strip() or "0"))
_success_counter = 0
_success_lock = threading.Lock()


# ============================================================
#   10 PROXIES - each worker gets a dedicated proxy
#   Workers are assigned by index: worker_index % 10
#   ~5 sites per proxy when running 10 workers over 50 sites
# ============================================================

# All 10 proxy slots - fill in your actual proxy credentials below.
# Format: (host, port, username, password, label)
# Using dataimpulse rotate as the base; replace entries with distinct
# proxy IPs/credentials once you have 10 separate proxy accounts.
PROXY_LIST = [
    # slot 0
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare0"),
    # slot 1
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare1"),
    # slot 2
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare2"),
    # slot 3
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare3"),
    # slot 4
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare4"),
    # slot 5
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare5"),
    # slot 6
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare6"),
    # slot 7
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare7"),
    # slot 8
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare8"),
    # slot 9
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare9"),
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # TO USE REAL DISTINCT PROXIES: replace rows above like:
    # ("123.45.67.89",  8080, "user1", "pass1", "proxy-DE-1"),
    # ("98.76.54.32",   8080, "user2", "pass2", "proxy-US-1"),
    # etc.
]

def get_proxy_for_worker(worker_index: int) -> tuple[dict, str]:
    """
    Assign a proxy deterministically by worker index.
    Worker 0 â†’ PROXY_LIST[0], Worker 1 â†’ PROXY_LIST[1], â€¦, Worker 9 â†’ PROXY_LIST[9].
    Each proxy handles ~5 sites when 10 workers process 50 leads.
    """
    slot  = worker_index % len(PROXY_LIST)
    host, port, user, pwd, label = PROXY_LIST[slot]
    config = {
        "server":   f"http://{host}:{port}",
        "username": user,
        "password": pwd,
    }
    return config, f"{label}(slot{slot})"


def _is_proxy_bootstrap_error(err_text: str) -> bool:
    s = str(err_text or "").upper()
    markers = (
        "ERR_PROXY_AUTH_UNSUPPORTED",
        "ERR_PROXY_AUTH_REQUESTED",
        "ERR_PROXY_CONNECTION_FAILED",
        "ERR_TUNNEL_CONNECTION_FAILED",
        "ERR_NO_SUPPORTED_PROXIES",
        "ERR_INVALID_AUTH_CREDENTIALS",
        "ERR_HTTP_RESPONSE_CODE_FAILURE",
    )
    return any(m in s for m in markers)

# NopeCHA proxy payload - uses slot 0 by default for captcha solving
NOPECHA_PROXY_PAYLOAD = {
    "scheme":   "http",
    "host":     PROXY_LIST[0][0],
    "port":     str(PROXY_LIST[0][1]),
    "username": PROXY_LIST[0][2],
    "password": PROXY_LIST[0][3],
}


DEFAULT_COMPANIES =[
    {"Company Name": "Aalpha Information Systems", "Contact Form URL": "https://www.aalpha.net/contact-us/"},
    {"Company Name": "CCI Invest",                 "Contact Form URL": "https://ccinvest.com/contact/"},
    {"Company Name": "Storage Post",               "Contact Form URL": "https://www.storagepost.com/contact-us"},
    {"Company Name": "Rosen NYC",                  "Contact Form URL": "https://www.rosenyc.com/about-us/contact-us/"},
    #{"Company Name": "RXR",                        "Contact Form URL": "https://rxr.com/contact/"},
    {"Company Name": "Gabriel Legal",              "Contact Form URL": "https://www.gabriellegal.com/contact-us"},
    {"Company Name": "OW.LY (Bitly)",              "Contact Form URL": "https://lochness.com/"},
    #{"Company Name": "BOMA NY",                    "Contact Form URL": "https://www.bomany.org/contact.html"},
    {"Company Name": "Storage Post", "Contact Form URL": "https://www.storagepost.com/contact-us"},
    {"Company Name": "Housing Visions", "Contact Form URL": "https://www.housingvisions.org/contact/"},
    {"Company Name": "Fairstead", "Contact Form URL": "https://fairstead1.wpenginepowered.com/contact/"},
    {"Company Name": "SPM", "Contact Form URL": "https://spm.net/contact/"},
    {"Company Name": "Breaking Ground", "Contact Form URL": "https://www.breakingground.org/contact-us"},
    {"Company Name": "CAMBA", "Contact Form URL": "https://camba.org/contact/"},
    {"Company Name": "Cushman & Wakefield", "Contact Form URL": "https://www.cushmanwakefield.com/en/inquire"},
    {"Company Name": "New York Edge", "Contact Form URL": "https://newyorkedge.org/contact-us/"},
    {"Company Name": "Acadia Realty", "Contact Form URL": "https://acadiarealty.com/contact/"},
    {"Company Name": "Gabriel Legal", "Contact Form URL": "https://www.gabriellegal.com/contact-us"},
    {"Company Name": "Clear Investment Group", "Contact Form URL": "https://www.clearinvestmentgroup.com/contact-us"},
    {"Company Name": "Empire State Realty Trust", "Contact Form URL": "https://www.esrtreit.com/contact/"},
    {"Company Name": "Housing Plus NYC", "Contact Form URL": "https://housingplusnyc.org/connect-with-us/"},
    {"Company Name": "Rosen NYC", "Contact Form URL": "https://www.rosenyc.com/about-us/contact-us/"},
    {"Company Name": "DePaul", "Contact Form URL": "https://www.depaul.org/contact-us/inquiry-form/"},
    {"Company Name": "Amalgamated Bank", "Contact Form URL": "https://www.amalgamatedbank.com/contact-us"},
    {"Company Name": "NBBJ", "Contact Form URL": "https://www.nbbj.com/contact"},
    {"Company Name": "Carlyle", "Contact Form URL": "https://www.carlyle.com/connect-us"},]

openai_client   = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
SKYVERN_ENABLED = False
SKYVERN_API_URL = "http://localhost:8080"
SKYVERN_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjQ5MTgzNTU5NjUsInN1YiI6Im9fNTA0MzgxODc4MzkxODExMTA4In0.KLz3KLwZjXJ__OkUlGLuNZa34qcchvxMqb-MA9UPzNg"

sheet_lock = asyncio.Lock()
_sheet_next_row_cache = {}


def _is_sheet_grid_limit_error(err: Exception) -> bool:
    text = str(err or "").lower()
    return ("exceeds grid limits" in text) or ("max rows" in text and "range" in text)


async def _ensure_sheet_has_capacity(sheet, target_row: int, expected_cols: int):
    """Grow worksheet grid when writes target rows beyond current limits."""
    try:
        current_rows = int(getattr(sheet, "row_count", 0) or 0)
    except Exception:
        current_rows = 0
    try:
        current_cols = int(getattr(sheet, "col_count", expected_cols) or expected_cols)
    except Exception:
        current_cols = expected_cols

    needed_rows = max(int(target_row), 2)
    needed_cols = max(int(expected_cols), 1)

    if current_rows >= needed_rows and current_cols >= needed_cols:
        return

    # Add headroom so parallel workers do not trigger repeated resizes.
    grow_rows = max(needed_rows + 200, current_rows + 500, 1000)
    grow_cols = max(current_cols, needed_cols)
    await asyncio.to_thread(lambda: sheet.resize(rows=grow_rows, cols=grow_cols))


# ============================================================
#   ROW BUILDER
# ============================================================

def _yn(flag: bool) -> str:
    return "Yes" if flag else "No"


def _derive_website_url(company_name: str, contact_url: str) -> str:
    company = str(company_name or "").strip()
    c_low = company.lower()
    if c_low.startswith("http://") or c_low.startswith("https://"):
        return company

    try:
        from urllib.parse import urlparse as _urlparse
        host = (_urlparse(str(contact_url or "")).hostname or "").strip().lower()
        if host:
            return f"https://{host}"
    except Exception:
        pass

    if company and "." in company and " " not in company:
        return f"https://{company.strip('/')}"

    return company


def _is_contact_form_present(submitted: str, assurance: str) -> bool:
    a = str(assurance or "").lower()
    s = str(submitted or "").strip().lower()
    if not a and not s:
        return False

    # If navigation itself failed, a contact form was never reached.
    if "error:" in a and ("page.goto" in a or "net::err_" in a or "call log:" in a):
        return False

    negatives = (
        "invalid url",
        "no contact us form present",
        "contact us form not found",
        "form detected but no fillable fields found",
        "filled 0 fields",
        "err_network_changed",
        "err_proxy_auth_unsupported",
        "err_proxy_connection_failed",
        "err_tunnel_connection_failed",
        "err_no_supported_proxies",
        "err_http_response_code_failure",
    )
    return not any(n in a for n in negatives)


def _is_captcha_present(captcha_status: str) -> bool:
    cs = str(captcha_status or "").strip().lower()
    return cs not in {"", "none", "n/a"}


def _is_captcha_solved(captcha_status: str) -> bool:
    return "-solved-" in str(captcha_status or "").lower()


def _nopecha_solves_consumed(captcha_status: str) -> int:
    cs = str(captcha_status or "").strip().lower()
    if cs in {"", "none", "n/a"}:
        return 0
    if "cloudflare-challenge-page" in cs or "no-sitekey" in cs:
        return 0
    return 1 if _is_captcha_solved(cs) else 0


def _nopecha_credit_for_row(captcha_status: str) -> tuple[str, str]:
    left_total = _peek_stable_nopecha_credit_left()
    row_used = _nopecha_solves_consumed(captcha_status) * NOPECHA_CREDIT_PER_SOLVE
    return str(max(0, int(row_used))), str(left_total)


def _nopecha_solves_from_credit_used(credit_used: str) -> int:
    try:
        used = int(float(str(credit_used or "0").strip() or "0"))
    except Exception:
        return 0
    per_solve = max(1, int(NOPECHA_CREDIT_PER_SOLVE or 20))
    return max(0, used // per_solve)


def _format_duration(seconds: float) -> str:
    total = max(0, int(float(seconds or 0)))
    hours, rem = divmod(total, 3600)
    mins, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"

def _build_row(company_name, url, submitted, assurance,
               captcha_status, proxy_label, bw_kb, token_cols=None,
               filled_fields=None, sub_status="", confirmation_msg="",
               message_sent="", subject_text="", time_taken="") -> list:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    input_tokens = "0"
    output_tokens = "0"
    if token_cols and len(token_cols) == 7:
        ts = str(token_cols[0] or ts)
        input_tokens = str(token_cols[2] or "0")
        output_tokens = str(token_cols[3] or "0")

    submitted_text = str(submitted or "")
    submitted_yes = submitted_text.strip().lower() == "yes"
    captcha_text = str(captcha_status or "")
    assurance_text = str(assurance or "")
    response_text = str((confirmation_msg or assurance or "")).strip()

    contact_form_present = _is_contact_form_present(submitted_text, assurance_text)
    captcha_present = _is_captcha_present(captcha_text)
    captcha_solved = _is_captcha_solved(captcha_text)
    _, nopecha_credit_left = _consume_nopecha_credit_for_row(captcha_text)

    submitted_wo_captcha = submitted_yes and (not captcha_present)
    submitted_with_captcha = submitted_yes and captcha_present
    submitted_overall = submitted_yes

    reason_for_failure = ""
    if not submitted_yes:
        reason_for_failure = assurance_text or response_text or "Failed"

    website_url = _derive_website_url(str(company_name or ""), str(url or ""))

    fields_str = _format_submission_fields(filled_fields)

    if message_sent:
        msg_compact = re.sub(r"\s+", " ", str(message_sent)).strip()
        msg_text = (msg_compact[:500] + "...") if len(msg_compact) > 500 else msg_compact
    else:
        msg_text = fields_str

    subject_compact = re.sub(r"\s+", " ", str(subject_text or "")).strip()
    if not subject_compact:
        subject_compact = str(MY_TITLE or "")
        subject_compact = subject_compact.replace("{company_name}", str(company_name or ""))
        subject_compact = subject_compact.replace("{MY_COMPANY}", str(MY_COMPANY or ""))
        subject_compact = re.sub(r"\s+", " ", subject_compact).strip()

    content_parts = [
        f"name: {MY_FIRST_NAME}",
        f"last name: {MY_LAST_NAME}",
        f"email: {MY_EMAIL}",
        f"subject: {subject_compact[:180] if subject_compact else '-'}",
        "website fields filled:",
        fields_str,
        f"submission content: {msg_text or '-'}",
    ]
    submission_content = "\n".join(content_parts)

    return [
        str(website_url),
        str(url),
        _yn(contact_form_present),
        input_tokens,
        output_tokens,
        str(bw_kb),
        _yn(submitted_wo_captcha),
        _yn(captcha_present),
        str(nopecha_credit_left),
        _yn(captcha_solved),
        _yn(submitted_with_captcha),
        str(time_taken or ""),
        str(proxy_label),
        str(submission_content),
        response_text[:300],
        reason_for_failure[:300],
        ts,
        _yn(submitted_overall),
    ]


def _emit_result(company_name, url, submitted, assurance, captcha_status,
                 proxy_label, bw_kb, tok_cols=None, filled_fields=None,
                 sub_status="", confirmation_msg="", message_sent=""):
    """Print a [RESULT] JSON line to stdout for the dashboard to capture."""
    if tok_cols and len(tok_cols) == 7:
        tc = tok_cols
    else:
        tc = ["", 0, 0, 0, 0, 0, 0]
    fields_str = _format_submission_fields(filled_fields, max_items=15, value_limit=120)
    submitted_text = str(submitted or "")
    submitted_yes = submitted_text.strip().lower() == "yes"
    captcha_text = str(captcha_status or "")
    captcha_present = _is_captcha_present(captcha_text)
    captcha_solved = _is_captcha_solved(captcha_text)
    submitted_wo_captcha = submitted_yes and (not captcha_present)
    submitted_with_captcha = submitted_yes and captcha_present
    submitted_overall = submitted_yes

    global _success_counter
    if submitted_overall and OUTREACH_MAX_DAILY_SUBMISSIONS > 0:
        with _success_lock:
            _success_counter += 1
            if _success_counter >= OUTREACH_MAX_DAILY_SUBMISSIONS:
                print(f"\n[LIMIT] Reached limit of {OUTREACH_MAX_DAILY_SUBMISSIONS} successful submissions! Initiating shutdown.")
                _STOP_FLAG.set()

    nopecha_credit_used, nopecha_credit_left = _nopecha_credit_for_row(str(captcha_status or ""))
    nopecha_solves = _nopecha_solves_from_credit_used(nopecha_credit_used)
    result_obj = {
        "company_name": str(company_name),
        "contact_url": str(url),
        "submitted": submitted_text,
        "submission_assurance": str(assurance)[:300],
        "captcha_status": captcha_text,
        "captcha_present": _yn(captcha_present),
        "captcha_solved": _yn(captcha_solved),
        "submitted_without_captcha": _yn(submitted_wo_captcha),
        "submitted_with_captcha": _yn(submitted_with_captcha),
        "submitted_overall": _yn(submitted_overall),
        "nopecha_solves_consumed": str(nopecha_solves),
        "nopecha_credit_used": str(nopecha_credit_used),
        "nopecha_credit_left": str(nopecha_credit_left),
        "proxy_used": str(proxy_label),
        "bandwidth_kb": str(bw_kb),
        "run_timestamp": str(tc[0]),
        "api_calls": str(tc[1]),
        "input_tokens": str(tc[2]),
        "output_tokens": str(tc[3]),
        "total_tokens": str(tc[4]),
        "est_cost": str(tc[5]),
        "avg_tokens_call": str(tc[6]),
        "fields_filled": fields_str or "-",
        "submission_status": str(sub_status or submitted),
        "confirmation_msg": str((confirmation_msg or assurance or ""))[:300],
        "message_sent": str(message_sent)[:500] if message_sent else "-",
    }
    print(f"[RESULT] {json.dumps(result_obj)}")


# ============================================================
#   SAFE SHEET WRITE
# ============================================================

async def safe_append_row(sheet, row: list, max_retries=6):
    expected = len(SHEET_HEADERS)
    if len(row) != expected:
        row = (row + [""] * expected)[:expected]

    def _write_fallback_csv(r: list) -> bool:
        try:
            import os as _os
            fpath     = "fallback_rows.csv"
            write_hdr = not _os.path.exists(fpath)
            with open(fpath, "a", newline="", encoding="utf-8") as f:
                if write_hdr:
                    csv.writer(f).writerow(SHEET_HEADERS)
                csv.writer(f).writerow(r)
            print(f"   [Sheets] Saved to fallback_rows.csv")
            return True
        except Exception as fe:
            print(f"   [Sheets] Fallback failed: {fe}")
            return False

    if sheet is None:
        print(f"   [Sheets] Offline mode - writing fallback row: {str(row[0])[:30]}")
        _write_fallback_csv(row)
        return False

    print(f"   [Sheets] Writing: {str(row[0])[:30]}")
    delay = 2
    for attempt in range(max_retries):
        try:
            async with sheet_lock:
                cache_key = int(getattr(sheet, "id", 0) or 0)
                if cache_key not in _sheet_next_row_cache:
                    current_len = await asyncio.to_thread(lambda: len(sheet.col_values(1)))
                    _sheet_next_row_cache[cache_key] = max(2, current_len + 1)

                target_row = _sheet_next_row_cache[cache_key]
                await _ensure_sheet_has_capacity(sheet, target_row, expected)
                last_col = _column_letter(expected)
                target_range = f"A{target_row}:{last_col}{target_row}"

                await asyncio.to_thread(
                    lambda: sheet.update(
                        range_name=target_range,
                        values=[row],
                        value_input_option="USER_ENTERED",
                    )
                )
                _sheet_next_row_cache[cache_key] = target_row + 1
            print(f"   [Sheets] OK: {str(row[0])[:25]}")
            return True
        except gspread.exceptions.APIError as e:
            if "429" in str(e) or "Quota" in str(e):
                wait = min(delay * (2 ** attempt), 60) + random.uniform(0, 2)
                print(f"   [Sheets] 429 - retry in {wait:.1f}s")
                await asyncio.sleep(wait)
            elif _is_sheet_grid_limit_error(e):
                print(f"   [Sheets] Grid limit reached at attempt {attempt+1}; resizing sheet and retrying")
                try:
                    async with sheet_lock:
                        cache_key = int(getattr(sheet, "id", 0) or 0)
                        target_row = int(_sheet_next_row_cache.get(cache_key, 2) or 2)
                        await _ensure_sheet_has_capacity(sheet, target_row, expected)
                except Exception as grow_err:
                    print(f"   [Sheets] Grid resize failed: {type(grow_err).__name__}: {grow_err}")
                    await asyncio.sleep(delay)
            else:
                print(f"   [Sheets] API error attempt {attempt+1}: {e}")
                await asyncio.sleep(delay)
        except Exception as e:
            print(f"   [Sheets] Write error attempt {attempt+1}: {type(e).__name__}: {e}")
            await asyncio.sleep(delay)

    _write_fallback_csv(row)
    return False


# ============================================================
#   REQUEST BLOCKING + BANDWIDTH TRACKING
# ============================================================

_ALLOWED_JS_HOSTS = {
    "hcaptcha.com","newassets.hcaptcha.com","api.hcaptcha.com",
    "imgs.hcaptcha.com","assets.hcaptcha.com",
    "www.google.com","recaptcha.net","www.gstatic.com",
    "challenges.cloudflare.com","turnstile.cloudflare.com",
    "js.hsforms.net","forms.hsforms.com",
    "embed.typeform.com","typeform.com",
    "jotform.com","cdn.jotfor.ms",
    "wufoo.com","formstack.com","cognitoforms.com","123formbuilder.com",
    "ajax.googleapis.com","cdn.jsdelivr.net","cdnjs.cloudflare.com",
    "unpkg.com","code.jquery.com",
}
_BLOCKED_JS_HOSTS = {
    "googletagmanager.com","google-analytics.com","hotjar.com","clarity.ms",
    "mixpanel.com","segment.com","heap.io","fullstory.com","logrocket.com",
    "mouseflow.com","crazyegg.com","quantserve.com","scorecardresearch.com",
    "facebook.net","connect.facebook.net","bat.bing.com","adnxs.com",
    "outbrain.com","taboola.com","platform.twitter.com","tawk.to",
    "widget.intercom.io","js.driftt.com","cdn.livechatinc.com",
    "widget.freshworks.com","widget.tidio.com","cdn.crisp.chat",
    "js.hs-scripts.com","js.hs-analytics.net","player.vimeo.com",
    "onesignal.com","cdn.optimizely.com","browser.sentry-cdn.com",
}
_BLOCKED_XHR_PATHS = {
    "/analytics","/track","/pixel","/beacon","/collect",
    "/metric","/telemetry","/gtm","/ga.","/fbq",
}
_FORMISH_URL_HINTS = {
    "contact", "form", "inquiry", "inquire", "enquiry", "enquire",
    "submit", "lead", "quote", "callback", "appointment",
    "recaptcha", "hcaptcha", "turnstile", "captcha",
    "hsforms", "hubspot", "typeform", "jotform", "wufoo",
    "formstack", "cognitoforms", "123formbuilder",
}


def _make_route_handler(main_host: str, bw: dict):
    from urllib.parse import urlparse as _up

    def _host_matches(host: str, domains: set[str]) -> bool:
        if not host:
            return False
        return any(host == d or host.endswith("." + d) for d in domains)

    def _is_formish_url(url_low: str) -> bool:
        return any(h in url_low for h in _FORMISH_URL_HINTS)

    async def _handler(route, request):
        req_url = request.url
        rtype   = request.resource_type
        url_low = req_url.lower()

        try:
            req_host = _up(req_url).hostname or ""
        except Exception:
            req_host = ""

        is_main = (req_host == main_host or req_host.endswith("." + main_host))
        is_allowed_host = _host_matches(req_host, _ALLOWED_JS_HOSTS)
        is_blocked_host = _host_matches(req_host, _BLOCKED_JS_HOSTS)
        is_formish = _is_formish_url(url_low)

        used_bytes = int(bw.get("bytes", 0) or 0)
        soft_reached = used_bytes >= (BANDWIDTH_SOFT_LIMIT_KB * 1024)
        hard_reached = used_bytes >= (BANDWIDTH_HARD_CAP_KB * 1024)

        main_script_cap = MAX_MAIN_SCRIPT_REQ
        allowed_script_cap = MAX_ALLOWED_HOST_SCRIPT_REQ
        main_xhr_cap = MAX_MAIN_XHR_REQ
        allowed_xhr_cap = MAX_ALLOWED_HOST_XHR_REQ
        if soft_reached:
            main_script_cap = max(2, main_script_cap // 2)
            allowed_script_cap = max(1, allowed_script_cap // 2)
            main_xhr_cap = max(2, main_xhr_cap // 2)
            allowed_xhr_cap = max(1, allowed_xhr_cap // 2)
        if hard_reached:
            main_script_cap = max(1, main_script_cap // 2)
            allowed_script_cap = max(1, allowed_script_cap // 2)
            main_xhr_cap = max(1, main_xhr_cap // 2)
            allowed_xhr_cap = max(1, allowed_xhr_cap // 2)

        reason = None

        # Keep top-level docs, but suppress non-form third-party iframe docs.
        if rtype == "document":
            if not is_main and not is_allowed_host and not is_formish:
                reason = f"iframe-doc:{req_host or 'unknown'}"

        # Biggest bandwidth savers for automation: render not required.
        elif rtype in {"image", "media", "font", "stylesheet"}:
            reason = f"{rtype}-blocked"

        elif rtype in {"manifest", "eventsource", "websocket", "ping"} and not is_formish:
            reason = f"nonessential:{rtype}"

        # Always drop known analytics/tracker traffic.
        elif is_blocked_host and rtype in {"script", "xhr", "fetch"}:
            reason = f"tracker:{rtype}:{req_host}"

        elif rtype in {"xhr", "fetch"} and any(kw in url_low for kw in _BLOCKED_XHR_PATHS):
            reason = "tracker-xhr"

        elif rtype == "script":
            if not (is_main or is_allowed_host):
                reason = f"3p-script:{req_host or 'unknown'}"
            else:
                counter_key = "main_scripts" if is_main else "allowed_scripts"
                cap = main_script_cap if is_main else allowed_script_cap
                current = int(bw.get(counter_key, 0) or 0)
                if current >= cap and not is_formish:
                    reason = f"script-cap:{counter_key}:{current}/{cap}"
                else:
                    bw[counter_key] = current + 1

        elif rtype in {"xhr", "fetch"}:
            if not (is_main or is_allowed_host):
                reason = f"3p-{rtype}:{req_host or 'unknown'}"
            else:
                counter_key = "main_xhr" if is_main else "allowed_xhr"
                cap = main_xhr_cap if is_main else allowed_xhr_cap
                current = int(bw.get(counter_key, 0) or 0)
                if current >= cap and not is_formish:
                    reason = f"{rtype}-cap:{counter_key}:{current}/{cap}"
                else:
                    bw[counter_key] = current + 1

        # Hard cap mode: only keep main/known-essential traffic for submit flow.
        elif hard_reached:
            if rtype in {"script", "xhr", "fetch", "stylesheet", "other"} and not (is_main or is_allowed_host or is_formish):
                reason = f"bw-hard:{rtype}:{req_host or 'unknown'}"
            elif rtype in {"manifest", "eventsource", "websocket", "ping"} and not is_main:
                reason = f"bw-hard:{rtype}"

        # Soft cap mode: start dropping non-main third-party dynamic traffic.
        elif soft_reached:
            if rtype == "script" and not (is_main or is_allowed_host or is_formish):
                reason = f"bw-soft:script:{req_host or 'unknown'}"
            elif rtype in {"xhr", "fetch"} and not (is_main or is_allowed_host or is_formish):
                reason = f"bw-soft:{rtype}:{req_host or 'unknown'}"

        if reason:
            bw["blocked"] += 1
            await route.abort()
            return

        bw["allowed"] += 1
        await route.continue_()

    return _handler


def _make_response_counter(bw: dict):
    def _on_response(response):
        try:
            status = int(getattr(response, "status", 0) or 0)
            if status in {204, 304}:
                return

            # Keep a short rolling window of recent responses so confirmation
            # logic can infer successful async form submissions.
            try:
                rtype = str(getattr(response.request, "resource_type", "") or "").lower()
                method = str(getattr(response.request, "method", "") or "").upper()
                url = str(getattr(response, "url", "") or "")
                recent = bw.setdefault("recent_responses", [])
                recent.append({
                    "ts": float(time.perf_counter()),
                    "url": url[:360],
                    "status": status,
                    "rtype": rtype,
                    "method": method,
                })
                if len(recent) > 180:
                    del recent[:-180]
            except Exception:
                pass

            cl = response.headers.get("content-length")
            if cl:
                size = int(cl)
                if size > 0:
                    bw["bytes"] += size
            else:
                rtype = str(getattr(response.request, "resource_type", "") or "").lower()
                if rtype == "script":
                    bw["bytes"] += 2_500
                elif rtype == "stylesheet":
                    bw["bytes"] += 1_000
                elif rtype == "image":
                    bw["bytes"] += 3_000
                elif rtype in {"xhr", "fetch"}:
                    bw["bytes"] += 1_500
                elif rtype == "document":
                    bw["bytes"] += 20_000
                else:
                    bw["bytes"] += 500
        except Exception:
            pass
    return _on_response


# ============================================================
#   NOPECHA - hard timeout 300 seconds
# ============================================================

_nopecha_semaphore: asyncio.Semaphore = None

def _make_nopecha_semaphore():
    global _nopecha_semaphore
    _nopecha_semaphore = asyncio.Semaphore(3)


def _nopecha_token_api(cap_type: str, sitekey: str, url: str) -> str | None:
    import time as _t
    from urllib.parse import urlparse as _urlparse

    API          = "https://api.nopecha.com/token"
    POLL_SECS    = 5
    POST_RETRY   = 5                    # fewer retries since timeout is tight
    HARD_TIMEOUT = NOPECHA_HARD_TIMEOUT  # 300 seconds
    JOB_TTL      = 60      # re-submit job if no answer within 60s
    kw           = {"timeout": 15}      # shorter network timeout too

    host = (_urlparse(url).hostname or "unknown").lower()
    sitekey_mask = _mask_secret(sitekey, head=6, tail=6)

    def _trace(msg: str):
        print(f"   [NopeCHA] {msg}")
        _nopecha_log(f"[NopeCHA] {msg} | type={cap_type} | host={host} | sitekey={sitekey_mask}")

    _trace(f"Start solve timeout={HARD_TIMEOUT}s poll={POLL_SECS}s job_ttl={JOB_TTL}s")

    nopecha_key = _next_valid_nopecha_key()
    if not nopecha_key:
        _trace("All API keys have exhausted their credits")
        return None

    _trace(f"Selected API key ...{nopecha_key[-6:]}")

    try:
        r      = requests.get("https://api.nopecha.com/status",
                              params={"key": nopecha_key}, timeout=8)
        data   = r.json()
        credit = data.get("credit", (data.get("data") or {}).get("credit", -1))
        _record_nopecha_credit(nopecha_key, credit)
        credit_int = _nopecha_parse_int(credit)
        required_credit = max(1, int(NOPECHA_CREDIT_PER_SOLVE or 20))
        if credit_int is not None and credit_int < required_credit:
            _trace(
                f"Key credits too low ({credit_int} < {required_credit}), disabling ...{nopecha_key[-6:]}"
            )
            _disable_nopecha_key(nopecha_key)
            return _nopecha_token_api(cap_type, sitekey, url)
        _trace(f"Using key ...{nopecha_key[-6:]} | Credits: {credit}")
    except Exception as e:
        _trace(f"Balance check failed ({e}) - proceeding with key ...{nopecha_key[-6:]}")

    _trace(f"Submitting {cap_type} timeout={HARD_TIMEOUT}s url={url[:80]}")
    deadline = _t.time() + HARD_TIMEOUT
    payload  = {
        "key": nopecha_key, "type": cap_type,
        "sitekey": sitekey, "url": url, "proxy": NOPECHA_PROXY_PAYLOAD,
    }
    if cap_type == "recaptcha3":
        payload["action"] = "submit"
        payload["score"]  = 0.7

    def _submit(reason: str = "initial"):
        rotate_key = False
        for i in range(1, POST_RETRY + 1):
            if _STOP_FLAG.is_set():
                _trace(f"Submit aborted by stop flag reason={reason} attempt={i}")
                return None, rotate_key
            if _t.time() > deadline:
                _trace(f"Deadline reached during submit reason={reason} attempt={i}")
                return None, rotate_key
            try:
                r = requests.post(API, json=payload, **kw)
                d = r.json()
                job_id = d.get("data")
                if job_id:
                    _trace(f"Submit success reason={reason} attempt={i} job={str(job_id)[:12]}")
                    return job_id, rotate_key
                ec = d.get("error", "")
                _trace(f"Submit failed reason={reason} attempt={i} err={ec}")
                if ec in (10, 11, 16, "10", "11", "16"):
                    rotate_key = True
                    break
                _t.sleep(1.0)
            except Exception as e:
                _trace(f"Submit network error reason={reason} attempt={i} err={type(e).__name__}: {e}")
                _t.sleep(1.0)
        return None, rotate_key

    job_id, rotate_after_submit = _submit(reason="initial")
    if not job_id:
        if rotate_after_submit:
            _trace(f"Disabling key after submit errors and rotating ...{nopecha_key[-6:]}")
            _disable_nopecha_key(nopecha_key)
            return _nopecha_token_api(cap_type, sitekey, url)
        _trace("Could not submit job_id after retries - giving up")
        return None

    job_at  = _t.time()
    polls   = 0
    while _t.time() < deadline:
        if _STOP_FLAG.is_set():
            _trace("Stop flag set - aborting poll loop")
            return None

        remaining = deadline - _t.time()
        _trace(f"Polling job={job_id[:12]} poll={polls} remaining={remaining:.0f}s")
        
        for _ in range(POLL_SECS):
            if _STOP_FLAG.is_set():
                _trace("Stop flag set during poll sleep - aborting")
                return None
            try:
                _t.sleep(1)
            except OSError as e:
                _trace(f"Poll sleep interrupted ({e}) - aborting")
                return None
        
        polls += 1

        if _t.time() > deadline:
            _trace("Hard timeout reached while polling")
            return None

        elapsed_job = int(_t.time() - job_at)
        if elapsed_job >= JOB_TTL:
            _trace(f"Resubmitting because job ttl reached ({elapsed_job}s) old_job={job_id[:12]}")
            nj, rotate_after_submit = _submit(reason=f"ttl-{elapsed_job}s")
            if nj:
                job_id = nj
                job_at = _t.time()
            elif rotate_after_submit:
                _trace(f"Disabling key after ttl resubmit errors and rotating ...{nopecha_key[-6:]}")
                _disable_nopecha_key(nopecha_key)
                return _nopecha_token_api(cap_type, sitekey, url)
            else:
                _trace("Resubmit after ttl trigger failed (no new job_id)")
            continue

        try:
            r  = requests.get(API, params={"key": nopecha_key, "id": job_id}, **kw)
            d  = r.json()
            tk = d.get("data")
            ec = d.get("error", 0)
            tk_len = len(tk) if isinstance(tk, str) else 0
            _trace(f"Poll response job={job_id[:12]} err={ec} token_len={tk_len}")
            if tk and isinstance(tk, str) and len(tk) > 20:
                _trace(f"Solved in {int(_t.time() - deadline + HARD_TIMEOUT)}s")
                return tk
            if ec in (10, "10"):
                _trace(f"Resubmitting because poll returned error={ec} for job={job_id[:12]}")
                nj, rotate_after_submit = _submit(reason=f"poll-error-{ec}")
                if nj:
                    job_id = nj
                    job_at = _t.time()
                elif rotate_after_submit:
                    _trace(f"Disabling key after poll submit errors and rotating ...{nopecha_key[-6:]}")
                    _disable_nopecha_key(nopecha_key)
                    return _nopecha_token_api(cap_type, sitekey, url)
                else:
                    _trace("Resubmit after poll error failed (no new job_id)")
        except Exception as e:
            _trace(f"Poll network error: {type(e).__name__}: {e}")

    _trace(f"Hard timeout reached ({HARD_TIMEOUT}s) - no token")
    return None


async def _inject_token(page, token: str, cap_type: str):
    if cap_type == "hcaptcha":
        await page.evaluate("""(t) => {
            document.querySelectorAll('textarea[name="h-captcha-response"],textarea[name="g-recaptcha-response"]').forEach(el => {
                el.value=t; el.dispatchEvent(new Event('change',{bubbles:true}));
            });
            document.querySelectorAll('[data-callback]').forEach(el => {
                var cb=el.getAttribute('data-callback');
                if(cb&&typeof window[cb]==='function') try{window[cb](t);}catch(e){}
            });
        }""", token)
    elif cap_type == "turnstile":
        await page.evaluate("""(t) => {
            document.querySelectorAll('[name="cf-turnstile-response"]').forEach(el => {
                el.value=t; el.dispatchEvent(new Event('change',{bubbles:true}));
            });
            document.querySelectorAll('[data-callback]').forEach(el => {
                var cb=el.getAttribute('data-callback');
                if(cb&&typeof window[cb]==='function') try{window[cb](t);}catch(e){}
            });
        }""", token)
    else:
        await page.evaluate("""(t) => {
            document.querySelectorAll('textarea[name="g-recaptcha-response"]').forEach(el => {
                Object.defineProperty(el,'value',{writable:true});
                el.value=t; el.innerHTML=t;
                el.dispatchEvent(new Event('change',{bubbles:true}));
            });
            try {
                var cfg=window.___grecaptcha_cfg;
                if(cfg&&cfg.clients)
                    Object.values(cfg.clients).forEach(c=>Object.values(c).forEach(w=>{
                        if(w&&typeof w.callback==='function') try{w.callback(t);}catch(e){}
                    }));
            } catch(e){}
        }""", token)
    await asyncio.sleep(1.5)
    try:
        return await page.evaluate("""(ctype) => {
            var selectors = [];
            if (ctype === 'hcaptcha') {
                selectors = ['textarea[name="h-captcha-response"]', 'textarea[name="g-recaptcha-response"]'];
            } else if (ctype === 'turnstile') {
                selectors = ['[name="cf-turnstile-response"]'];
            } else {
                selectors = ['textarea[name="g-recaptcha-response"]'];
            }

            var total = 0;
            var filled = 0;
            selectors.forEach(function(sel) {
                document.querySelectorAll(sel).forEach(function(el) {
                    total += 1;
                    var v = String((el && (el.value || el.innerHTML)) || '').trim();
                    if (v.length > 20) filled += 1;
                });
            });
            return { total: total, filled: filled, selectors: selectors };
        }""", cap_type)
    except Exception as e:
        return {"total": -1, "filled": -1, "error": str(e)[:200]}


# ============================================================
#   CAPTCHA DETECTION & SOLVING
# ============================================================

async def detect_and_solve_captcha(page, iframe=None):
    html     = await page.content()
    page_url = page.url
    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http"):
                html += await frame.content()
        except Exception:
            pass

    has_hcaptcha  = bool(re.search(r'h-captcha|hcaptcha\.com/1/api|newassets\.hcaptcha\.com', html, re.I))
    has_recaptcha = bool(
        re.search(r'class=["\'][^"\']*g-recaptcha', html, re.I) or
        re.search(r'recaptcha/api2/anchor|recaptcha/enterprise/anchor|recaptcha/api\.js', html, re.I) or
        re.search(r'grecaptcha\.(render|execute|ready)\s*\(', html, re.I)
    )
    has_turnstile = bool(
        re.search(r'cf-turnstile|challenges\.cloudflare\.com/turnstile|turnstile\.render\s*\(', html, re.I)
    )
    is_cloudflare_interstitial = bool(
        re.search(r'__cf_chl|cdn-cgi/challenge-platform|Just a moment\.\.\.|Enable JavaScript and cookies to continue', html, re.I)
    )

    _nopecha_log(
        f"[Captcha] detect flags hcaptcha={has_hcaptcha} recaptcha={has_recaptcha} "
        f"turnstile={has_turnstile} cloudflare={is_cloudflare_interstitial} url={page_url[:160]}"
    )

    if is_cloudflare_interstitial:
        print("   [Captcha] Cloudflare interstitial/challenge page detected")
        _nopecha_log("[Captcha] cloudflare challenge page detected, skipping NopeCHA submit")
        return True, "cloudflare-challenge-page"

    if not (has_hcaptcha or has_recaptcha or has_turnstile):
        print("   [Captcha] None detected")
        _nopecha_log("[Captcha] none detected")
        return False, "none"

    if has_recaptcha and not has_hcaptcha and not has_turnstile:
        is_v3 = bool(
            re.search(r'grecaptcha\.execute\s*\(', html, re.I) or
            re.search(r'grecaptcha\.ready\s*\(', html, re.I)
        )
        cap_type = "recaptcha3" if is_v3 else "recaptcha2"
    else:
        cap_type = "hcaptcha" if has_hcaptcha else ("turnstile" if has_turnstile else "recaptcha2")

    print(f"   [Captcha] Detected: {cap_type}")
    _nopecha_log(f"[Captcha] detected type={cap_type}")

    sitekey = None
    sitekey_source = ""
    for sel in [
        ".g-recaptcha[data-sitekey]",
        ".h-captcha[data-sitekey]",
        "[class*='cf-turnstile'][data-sitekey]",
        "[data-hcaptcha-sitekey]",
        "[data-recaptcha-sitekey]",
    ]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                for attr in ["data-sitekey","data-hcaptcha-sitekey","data-recaptcha-sitekey"]:
                    sk = await el.get_attribute(attr)
                    if sk and len(sk) > 10:
                        sitekey = sk.strip()
                        sitekey_source = f"dom:{sel}:{attr}"
                        break
            if sitekey:
                break
        except Exception:
            pass

    if not sitekey:
        try:
            sitekey = await page.evaluate("""() => {
                try {
                    return (
                        (window.__turnstileSitekey && String(window.__turnstileSitekey)) ||
                        (window.turnstileSitekey && String(window.turnstileSitekey)) ||
                        null
                    );
                } catch (e) {
                    return null;
                }
            }""")
            if sitekey:
                sitekey = str(sitekey).strip()
                sitekey_source = "window:turnstileSitekey"
        except Exception:
            pass

    if not sitekey:
        for frame in page.frames:
            try:
                fu = frame.url or ""
                if not fu.startswith("http"):
                    continue
                m = re.search(r'[?&](?:sitekey|k)=([A-Za-z0-9_-]{20,})', fu)
                if m:
                    sitekey = m.group(1).strip()
                    sitekey_source = "iframe-url:sitekey"
                    break
            except Exception:
                pass

    if not sitekey:
        for pat in [
            r'(?:sitekey|"sitekey"|\'sitekey\')\s*[:=]\s*["\']([A-Za-z0-9_-]{10,})["\']',
            r'data-sitekey=["\']([A-Za-z0-9_-]{20,})["\']',
            r'"sitekey"\s*:\s*"([A-Za-z0-9_-]{20,})"',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                sitekey = m.group(1).strip()
                sitekey_source = "html-regex"
                break

    if not sitekey:
        print(f"   [Captcha] No sitekey found")
        _nopecha_log(f"[Captcha] no sitekey found for type={cap_type}")
        return True, f"{cap_type}-no-sitekey"

    _nopecha_log(
        f"[Captcha] sitekey found type={cap_type} source={sitekey_source or 'unknown'} "
        f"sitekey={_mask_secret(sitekey, head=8, tail=6)}"
    )

    async with _nopecha_semaphore:
        _nopecha_log(f"[Captcha] requesting token from NopeCHA type={cap_type}")
        token = await asyncio.to_thread(_nopecha_token_api, cap_type, sitekey, page_url)
        
    if token:
        token_str = str(token)
        _nopecha_log(f"[Captcha] token received len={len(token_str)} token={_mask_secret(token_str, head=10, tail=6)}")
        inject_state = await _inject_token(page, token_str, cap_type)
        if isinstance(inject_state, dict):
            filled = inject_state.get("filled", -1)
            total = inject_state.get("total", -1)
            _nopecha_log(
                f"[Captcha] token injection verify type={cap_type} filled={filled}/{total} "
                f"details={str(inject_state)[:200]}"
            )
            if isinstance(filled, int) and filled <= 0:
                _nopecha_log("[Captcha] warning: token injected but no captcha response field contains token")
        print(f"   [Captcha] Solved: {cap_type}")
        return True, f"{cap_type}-solved-NopeCHA"
    else:
        _nopecha_log(f"[Captcha] token not received within {NOPECHA_HARD_TIMEOUT}s for type={cap_type}")
        print(f"   [Captcha] Timeout/failed after {NOPECHA_HARD_TIMEOUT}s")
        return True, f"{cap_type}-timeout-{NOPECHA_HARD_TIMEOUT}s"


# ============================================================
#   AI PITCH & SUBJECT - gpt-5-nano (KEPT as requested)
# ============================================================

# ============================================================
#    AI PITCH & SUBJECT - gpt-5-nano (Fixed UnboundLocalError)
# ============================================================

_pitch_cache_lock = threading.Lock()
_pitch_cache = {}


def _default_subject_for_company(greeting: str) -> str:
    if MY_TITLE:
        return MY_TITLE.replace("{company_name}", greeting).replace("{MY_COMPANY}", MY_COMPANY)
    return f"Virtual Assistant Support for {greeting} - {MY_COMPANY}"


def _sanitize_pitch_text(text: str) -> str:
    s = str(text or "")
    s = s.replace("â€“", "-").replace("–", "-")
    s = re.sub(r"(?i)i['’]?ll\s+keep\s+this\s+brief\s*-\s*we", "We", s)
    s = re.sub(r"\r\n?", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _parse_subject_message_json(raw: str) -> tuple[str, str]:
    txt = str(raw or "").strip()
    if not txt:
        return "", ""

    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", txt, re.I)
    if m:
        txt = m.group(1).strip()

    start = txt.find("{")
    end = txt.rfind("}")
    if start >= 0 and end > start:
        candidate = txt[start:end + 1]
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict):
                return str(obj.get("subject", "")).strip(), str(obj.get("message", "")).strip()
        except Exception:
            pass

    sub_m = re.search(r"(?im)^subject\s*:\s*(.+)$", txt)
    msg_m = re.search(r"(?im)^message\s*:\s*([\s\S]+)$", txt)
    subject = sub_m.group(1).strip() if sub_m else ""
    message = msg_m.group(1).strip() if msg_m else ""
    return subject, message


def _fallback_unique_pitch(greeting: str, base_subject: str, custom_pitch: str) -> tuple[str, str]:
    seed = int(hashlib.sha1(greeting.lower().encode("utf-8")).hexdigest()[:8], 16)

    openers = [
        f"I wanted to reach out because {greeting} may benefit from extra backend support.",
        f"Sharing a quick idea that could help {greeting} free up internal bandwidth.",
        f"Thought this might be relevant for {greeting} if operations are scaling.",
        f"Reaching out with a practical support option for {greeting}.",
    ]
    value_lines = [
        f"{MY_COMPANY} provides pre-trained virtual assistants who can plug into your workflow and work in your time zone.",
        f"At {MY_COMPANY}, we match teams with trained virtual assistants for recurring admin and operational tasks.",
        f"{MY_COMPANY} helps companies delegate repetitive backend work to ready-to-start virtual assistants.",
        f"Our team at {MY_COMPANY} supports growing companies with virtual assistants focused on day-to-day execution.",
    ]
    outcomes = [
        "This usually helps teams move faster on revenue work while keeping operations steady.",
        "The goal is to reduce internal workload without slowing down execution.",
        "It creates more time for your team to focus on growth and client-facing priorities.",
        "Most teams use this to improve consistency and response times across core processes.",
    ]
    ctas = [
        "If useful, I can share a quick plan tailored to your workflow.",
        "Open to a short call to see if this model fits your current needs?",
        "Would you be open to a brief chat this week?",
        "Happy to share a quick breakdown if you are exploring this right now.",
    ]

    opener = openers[seed % len(openers)]
    value_line = value_lines[(seed // 3) % len(value_lines)]
    outcome = outcomes[(seed // 5) % len(outcomes)]
    cta = ctas[(seed // 7) % len(ctas)]

    subject_variants = [
        base_subject,
        f"{greeting}: virtual assistant support in 24-48 hours",
        f"Operational support option for {greeting}",
        f"{greeting} - trained VA support from {MY_COMPANY}",
    ]
    subject = subject_variants[seed % len(subject_variants)]

    if custom_pitch:
        core = custom_pitch.replace("{company_name}", greeting).replace("{MY_COMPANY}", MY_COMPANY).strip()
        core = re.sub(r"\s+", " ", core)
        message = (
            f"Hi {greeting},\n\n"
            f"{opener}\n\n"
            f"{core}\n\n"
            f"We can usually match and onboard in 24-48 hours. {cta}"
        )
    else:
        message = (
            f"Hi {greeting},\n\n"
            f"{opener}\n\n"
            f"{value_line} {outcome}\n\n"
            f"We can usually match and onboard in 24-48 hours. {cta}"
        )

    return _sanitize_pitch_text(message), _sanitize_pitch_text(subject)


def _ai_unique_pitch(greeting: str, base_subject: str, custom_pitch: str, worker_index: int) -> tuple[str, str] | None:
    if not openai_client:
        return None

    style_hints = [
        "consultative and practical",
        "concise and operations-focused",
        "friendly and ROI-focused",
        "direct and execution-focused",
    ]
    seed = int(hashlib.sha1(greeting.lower().encode("utf-8")).hexdigest()[:8], 16)
    style_hint = style_hints[seed % len(style_hints)]

    guidance = custom_pitch.strip() if custom_pitch else ""
    prompt = (
        f'Create a unique outreach email subject and body for company "{greeting}".\n'
        f'Sender company: "{MY_COMPANY}".\n'
        f'Default subject reference: "{base_subject}".\n'
        f'Style: {style_hint}.\n\n'
        "Return STRICT JSON only with keys: subject, message.\n"
        "Rules:\n"
        "- message length: 90-140 words\n"
        "- 3 short paragraphs, plain text\n"
        "- include a specific mention that onboarding can happen in 24-48 hours\n"
        "- end with a polite CTA for a quick chat\n"
        "- do NOT use this phrase: I'll keep this brief - we\n"
        "- no markdown, no emojis\n"
    )
    if guidance:
        prompt += f"- keep the core idea from this guidance but rewrite uniquely: {guidance}\n"

    ai_instruction = str(os.environ.get("AI_INSTRUCTION", "") or "").strip()
    if ai_instruction:
        prompt += f"\nCRITICAL AI INSTRUCTION for content logic:\n{ai_instruction}\n"

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-5-nano",
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=260,
        )
        if getattr(resp, "usage", None):
            token_tracker.record(greeting, "pitch_subject", resp.usage, worker_index)

        raw = ""
        try:
            raw = str(resp.choices[0].message.content or "")
        except Exception:
            raw = ""

        ai_subject, ai_message = _parse_subject_message_json(raw)
        ai_subject = _sanitize_pitch_text(ai_subject)
        ai_message = _sanitize_pitch_text(ai_message)

        if not ai_message:
            return None
        if not ai_subject:
            ai_subject = base_subject

        if greeting.lower() not in ai_message.lower():
            ai_message = f"Hi {greeting},\n\n" + ai_message

        return ai_message, ai_subject
    except Exception:
        return None


def generate_ai_pitch_and_subject(company_name, worker_index=-1):
    normalized_company = _normalize_company_name(str(company_name or "").strip(), "", 0)
    greeting = normalized_company or "there"
    base_subject = _default_subject_for_company(greeting)
    custom_pitch = str(os.environ.get("PITCH_MESSAGE", "") or "").strip()

    cache_key = hashlib.sha1(
        (
            greeting.lower()
            + "|" + custom_pitch
            + "|" + str(MY_COMPANY or "")
            + "|" + str(MY_TITLE or "")
        ).encode("utf-8")
    ).hexdigest()
    with _pitch_cache_lock:
        cached = _pitch_cache.get(cache_key)
    if cached:
        return cached

    ai_result = _ai_unique_pitch(greeting, base_subject, custom_pitch, worker_index)
    if ai_result:
        result = (_sanitize_pitch_text(ai_result[0]), _sanitize_pitch_text(ai_result[1]))
    else:
        result = _fallback_unique_pitch(greeting, base_subject, custom_pitch)

    with _pitch_cache_lock:
        _pitch_cache[cache_key] = result

    return result
# ============================================================
#   REACT-SAFE FILL HELPER
# ============================================================

async def react_safe_fill(page, element_handle, value: str):
    try:
        await page.evaluate(
            f"([el, v]) => {{ {REACT_FILL_JS}(el, v); }}",
            [element_handle, value]
        )
        return True
    except Exception:
        try:
            await element_handle.fill(value)
            return True
        except Exception:
            return False


CONTACT_DISCOVERY_KEYWORDS = [
    "contact", "contact-us", "contact_us", "contactus",
    "get-in-touch", "getintouch", "reach-us", "reach_us",
    "touch", "inquiry", "enquiry", "support", "help",
    "write-to-us", "talk-to-us", "connect", "reach",
]

CONTACT_DISCOVERY_COMMON_PATHS = [
    "/contact-us", "/contact_us", "/contact", "/contactus",
    "/get-in-touch", "/reach-us", "/inquiry", "/enquiry",
    "/support/contact", "/about/contact", "/pages/contact",
    "/help/contact", "/connect", "/about-us/contact", "/reach", "/touch",
]

_CONTACT_LINK_EXCLUDE_HINTS = (
    "mailto:", "tel:", "javascript:", "#",
    "facebook.com", "twitter.com", "linkedin.com",
    "instagram.com", "youtube.com", "whatsapp",
    ".pdf", ".jpg", ".jpeg", ".png", ".svg", ".zip", ".gif",
)

CONTACT_DISCOVERY_MAX_SECONDS = max(6, _env_int("CONTACT_DISCOVERY_MAX_SECONDS", 35))
CONTACT_DISCOVERY_NAV_TIMEOUT_MS = max(2000, _env_int("CONTACT_DISCOVERY_NAV_TIMEOUT_MS", 9000))
CONTACT_DISCOVERY_MAX_PATH_TRIES = max(2, min(16, _env_int("CONTACT_DISCOVERY_MAX_PATH_TRIES", 8)))
CONTACT_DISCOVERY_MAX_LINK_TRIES = max(1, min(10, _env_int("CONTACT_DISCOVERY_MAX_LINK_TRIES", 4)))
CONTACT_DISCOVERY_STEP_PAUSE_MS = max(0, min(1200, _env_int("CONTACT_DISCOVERY_STEP_PAUSE_MS", 350)))
CONTACT_DISCOVERY_MIN_FIELDS = max(2, min(8, _env_int("CONTACT_DISCOVERY_MIN_FIELDS", 4)))


def _normalize_website_url(raw_url: str) -> str:
    s = str(raw_url or "").strip().strip('"').strip("'")
    if not s:
        return ""

    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", s):
        s = "https://" + s.lstrip("/")

    try:
        parsed = _urlparse.urlparse(s)
        if not parsed.netloc and parsed.path and "." in parsed.path:
            s = "https://" + parsed.path
    except Exception:
        pass

    return s


def _url_is_contact_like(url: str) -> bool:
    try:
        path = (_urlparse.urlparse(str(url or "")).path or "").lower()
    except Exception:
        return False
    return any(kw in path for kw in CONTACT_DISCOVERY_KEYWORDS)


def _url_needs_contact_discovery(url: str) -> bool:
    u = _normalize_website_url(url)
    if not u:
        return False

    try:
        path = (_urlparse.urlparse(u).path or "").strip().lower()
    except Exception:
        return False

    if not path or path == "/":
        return True
    if _url_is_contact_like(u):
        return False

    # Homepage-like shallow paths usually benefit from contact URL discovery.
    parts = [p for p in path.split("/") if p]
    if len(parts) <= 1 and not any(x in path for x in (".php", ".html", ".aspx", ".jsp")):
        return True

    return False


def _same_site_or_subdomain(candidate_url: str, root_netloc: str) -> bool:
    try:
        host = (_urlparse.urlparse(candidate_url).netloc or "").lower().lstrip("www.")
    except Exception:
        return False

    root = str(root_netloc or "").lower().lstrip("www.")
    if not host or not root:
        return False

    return host == root or host.endswith("." + root) or root.endswith("." + host)


def _contact_discovery_time_left(deadline_monotonic: float) -> float:
    return max(0.0, float(deadline_monotonic) - time.monotonic())


def _contact_discovery_timeout_ms(deadline_monotonic: float, default_ms: int) -> int:
    remaining_ms = int(_contact_discovery_time_left(deadline_monotonic) * 1000)
    if remaining_ms <= 0:
        return 0
    return min(int(default_ms), remaining_ms)


async def _has_form_signal_for_discovery(page) -> bool:
    min_fields = int(CONTACT_DISCOVERY_MIN_FIELDS)
    js_checked = False

    try:
        stats = await page.evaluate("""(cfg) => {
            const minFields = Math.max(2, Number(cfg?.minFields || 3));
            const isVisible = (el) => {
                if (!el) return false;
                const r = el.getBoundingClientRect();
                if (r.width < 1 || r.height < 1) return false;
                const cs = getComputedStyle(el);
                return cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
            };

            const controls = Array.from(document.querySelectorAll('input, textarea, select')).filter(el => {
                const tag = String(el.tagName || '').toLowerCase();
                const type = String(el.type || '').toLowerCase();
                if (tag === 'input' && ['hidden','submit','button','image','reset','search','file'].includes(type)) return false;
                const nm = String(el.name || el.id || el.placeholder || '').toLowerCase();
                if (/\bsearch\b|sf_s|zip.?code|keyword/.test(nm)) return false;
                if (/(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/.test(nm)) return false;
                return isVisible(el);
            });

            const contactLikeRe = /(name|email|mail|phone|mobile|message|comment|subject|company|organization|enquir|inquir|details|address)/i;
            let contactLike = 0;
            controls.forEach(el => {
                const meta = [
                    el.name || '',
                    el.id || '',
                    el.placeholder || '',
                    el.getAttribute('aria-label') || '',
                    el.getAttribute('autocomplete') || ''
                ].join(' ');
                if (contactLikeRe.test(meta)) contactLike += 1;
            });

            const hasEmail = controls.some(el => String(el.type || '').toLowerCase() === 'email');
            const hasTextarea = controls.some(el => String(el.tagName || '').toLowerCase() === 'textarea');
            const hasForm = !!document.querySelector('form');

            const total = controls.length;
            const strongSignal = (contactLike >= 2) || hasTextarea || (hasEmail && total >= minFields);

            return {
                total,
                contactLike,
                hasTextarea,
                hasEmail,
                hasForm,
                signal: (total >= minFields) && strongSignal,
            };
        }""", {"minFields": min_fields})

        js_checked = True
        if bool((stats or {}).get("signal", False)):
            return True
    except Exception:
        pass

    # Embedded forms are often in iframes; require the same minimum field threshold there.
    try:
        for frame in page.frames:
            try:
                frame_url = str(getattr(frame, "url", "") or "")
                if frame_url and not frame_url.startswith("http"):
                    continue
            except Exception:
                pass

            fc = await _count_form_fields(frame)
            if fc >= min_fields:
                return True
    except Exception:
        pass

    # Only use page-level fallback when JS signal extraction failed entirely.
    if not js_checked:
        try:
            return (await _count_form_fields(page)) >= min_fields
        except Exception:
            return False

    return False


async def _discover_contact_url_on_site(page, input_url: str, company_name: str = "") -> tuple[str, str, bool]:
    started_at = time.monotonic()
    deadline = started_at + float(CONTACT_DISCOVERY_MAX_SECONDS)

    seed_url = _normalize_website_url(input_url)
    if not seed_url:
        return input_url, "invalid-url", False

    try:
        parsed = _urlparse.urlparse(seed_url)
        root_netloc = (parsed.netloc or "").lower()
        base_url = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    except Exception:
        return seed_url, "parse-failed", False

    if not root_netloc:
        return seed_url, "missing-netloc", False

    # If the currently loaded page already has a form, keep it.
    if await _has_form_signal_for_discovery(page):
        return (page.url or seed_url), "already-has-form", True

    seen = set()

    # Step 1: common contact paths
    path_tries = 0
    for path in CONTACT_DISCOVERY_COMMON_PATHS:
        if path_tries >= CONTACT_DISCOVERY_MAX_PATH_TRIES:
            break

        candidate = _urlparse.urljoin(base_url + "/", path.lstrip("/"))
        if candidate in seen:
            continue
        seen.add(candidate)
        path_tries += 1

        timeout_ms = _contact_discovery_timeout_ms(deadline, CONTACT_DISCOVERY_NAV_TIMEOUT_MS)
        if timeout_ms <= 0:
            elapsed = int(time.monotonic() - started_at)
            return seed_url, f"timeout:path-scan:{elapsed}s", False

        try:
            await page.goto(candidate, timeout=timeout_ms, wait_until="domcontentloaded")
            pause_s = min(CONTACT_DISCOVERY_STEP_PAUSE_MS / 1000.0, _contact_discovery_time_left(deadline))
            if pause_s > 0:
                await asyncio.sleep(pause_s)
            if await _has_form_signal_for_discovery(page):
                return (page.url or candidate), f"common_path:{path}", True
        except Exception:
            continue

    # Step 2: scan homepage links and score likely contact URLs.
    seed_timeout_ms = _contact_discovery_timeout_ms(deadline, CONTACT_DISCOVERY_NAV_TIMEOUT_MS + 3000)
    if seed_timeout_ms <= 0:
        elapsed = int(time.monotonic() - started_at)
        return seed_url, f"timeout:before-link-scan:{elapsed}s", False

    try:
        await page.goto(seed_url, timeout=seed_timeout_ms, wait_until="domcontentloaded")
        pause_s = min(CONTACT_DISCOVERY_STEP_PAUSE_MS / 1000.0, _contact_discovery_time_left(deadline))
        if pause_s > 0:
            await asyncio.sleep(pause_s)
    except Exception:
        return seed_url, "seed-load-failed", False

    try:
        links = await page.evaluate("""() => {
            const out = [];
            document.querySelectorAll('a[href]').forEach(a => {
                const href = String(a.href || '').trim();
                const text = String(a.innerText || a.textContent || '').trim().toLowerCase().replace(/\\s+/g, ' ').slice(0, 120);
                if (href) out.push({ href, text });
            });
            return out;
        }""")
    except Exception:
        links = []

    scored = []
    for link in links or []:
        raw_href = _normalize_website_url((link or {}).get("href", ""))
        text = str((link or {}).get("text", "") or "").lower()

        if not raw_href:
            continue
        href_lower = raw_href.lower()
        if any(h in href_lower for h in _CONTACT_LINK_EXCLUDE_HINTS):
            continue
        if not _same_site_or_subdomain(raw_href, root_netloc):
            continue
        if raw_href in seen:
            continue
        seen.add(raw_href)

        path = (_urlparse.urlparse(raw_href).path or "").lower()
        score = 0
        for kw in CONTACT_DISCOVERY_KEYWORDS:
            if kw in path:
                score += 3
            if kw in text:
                score += 2

        if text.strip() in {
            "contact us", "contact", "get in touch", "reach us",
            "enquiry", "inquiry", "write to us", "message us",
        }:
            score += 5

        if any(x in path for x in ("/blog", "/news", "/product", "/shop", "/cart", "/category")):
            score -= 2

        if score > 0:
            scored.append((score, raw_href, text))

    scored.sort(key=lambda x: x[0], reverse=True)

    link_tries = 0
    for score, candidate, text in scored:
        if link_tries >= CONTACT_DISCOVERY_MAX_LINK_TRIES:
            break

        timeout_ms = _contact_discovery_timeout_ms(deadline, CONTACT_DISCOVERY_NAV_TIMEOUT_MS)
        if timeout_ms <= 0:
            elapsed = int(time.monotonic() - started_at)
            return seed_url, f"timeout:link-scan:{elapsed}s", False

        link_tries += 1

        try:
            await page.goto(candidate, timeout=timeout_ms, wait_until="domcontentloaded")
            pause_s = min(CONTACT_DISCOVERY_STEP_PAUSE_MS / 1000.0, _contact_discovery_time_left(deadline))
            if pause_s > 0:
                await asyncio.sleep(pause_s)
            if await _has_form_signal_for_discovery(page):
                hint = (text or _urlparse.urlparse(candidate).path or "link").strip()[:32]
                return (page.url or candidate), f"link_scan:{hint}", True
        except Exception:
            continue

    # Restore original URL if no contact page with form was found.
    try:
        restore_timeout_ms = _contact_discovery_timeout_ms(deadline, 4000)
        if restore_timeout_ms > 0:
            await page.goto(seed_url, timeout=restore_timeout_ms, wait_until="domcontentloaded")
    except Exception:
        pass

    elapsed = int(time.monotonic() - started_at)
    return seed_url, f"not-found:{elapsed}s", False


# ============================================================
#   IMPROVED FORM FINDER (6 strategies, unchanged from v3)
# ============================================================

KNOWN_FORM_PROVIDERS = [
    "hsforms.com", "hubspot.com", "typeform.com", "jotform.com",
    "wufoo.com", "formstack.com", "cognitoforms.com", "paperform.co",
    "123formbuilder.com", "gravity", "wpforms",
]

FORM_SELECTORS = [
    "input[type='email']",
    "textarea",
    "input[type='text']",
    "input[type='tel']",
    "select",
    "input[name*='name' i]",
    "input[name*='email' i]",
    "input[placeholder*='name' i]",
    "input[placeholder*='email' i]",
]

FORM_CONTAINER_SELECTORS = [
    "form",
    ".contact-form", ".contact_form",
    "[class*='wpcf7']", "[class*='gform_wrapper']",
    "[class*='wpforms']", "[class*='hsForm']",
    "[class*='contact-form']", "[class*='contactform']",
    "[id*='contact']", "[id*='form']",
    ".elementor-form", "[class*='ninja-forms']",
]


async def _count_form_fields(frame_or_page) -> int:
    try:
        return await frame_or_page.evaluate("""() => {
            return Array.from(document.querySelectorAll(
                'input:not([type="hidden"]):not([type="submit"]):not([type="button"]):not([type="image"]):not([type="reset"]):not([type="search"]), textarea, select'
            )).filter(el => {
                const r = el.getBoundingClientRect();
                if (r.width === 0 && el.closest('iframe') === null) return false;
                
                // Skip search-related fields
                const nm = (el.name || el.id || el.placeholder || '').toLowerCase();
                if (/\bsearch\b|sf_s|zip.?code|keyword/.test(nm)) return false;
                
                // Skip fields inside nav/header/search containers
                let p = el.parentElement;
                for (let d = 0; d < 8 && p; d++) {
                    const tag = (p.tagName || '').toLowerCase();
                    const cls = (p.className || '').toLowerCase();
                    const pid = (p.id || '').toLowerCase();
                    if (tag === 'nav' || tag === 'header' ||
                        cls.includes('search') || pid.includes('search')) return false;
                    p = p.parentElement;
                }
                return true;
            }).length;
        }""")
    except Exception:
        return 0


async def _scroll_until_form(page, max_scroll_px=5000) -> bool:
    step = 400
    scrolled = 0
    while scrolled < max_scroll_px:
        for sel in FORM_SELECTORS:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    box = await el.bounding_box()
                    if box and box["width"] > 0:
                        await page.evaluate(
                            "(el) => el.scrollIntoView({behavior:'instant', block:'center'})",
                            await el.element_handle()
                        )
                        return True
            except Exception:
                pass

        for sel in FORM_CONTAINER_SELECTORS:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    box = await el.bounding_box()
                    if box and box["width"] > 0 and box["height"] > 0:
                        await page.evaluate(
                            "(el) => el.scrollIntoView({behavior:'instant', block:'center'})",
                            await el.element_handle()
                        )
                        await asyncio.sleep(0.5)
                        inner = await _count_form_fields(page)
                        if inner > 0:
                            return True
            except Exception:
                pass

        scrolled += step
        await page.evaluate(f"window.scrollTo(0, {scrolled})")
        page_height = await page.evaluate(
            "() => document.body ? document.body.scrollHeight : 0"
        )
        await asyncio.sleep(0.3)

        page_height = await page.evaluate("() => document.body.scrollHeight")
        if scrolled >= page_height:
            break

    return False


async def find_form_target(page, url):
    # Strategy 1: fields already visible
    field_count = await _count_form_fields(page)
    if field_count > 0:
        print(f"   [FormFinder] S1: {field_count} fields visible")
        return page, "main-direct"

    # Strategy 2: scroll
    print(f"   [FormFinder] S2: Scrolling...")
    found = await _scroll_until_form(page)
    if found:
        field_count = await _count_form_fields(page)
        if field_count > 0:
            print(f"   [FormFinder] S2 success: {field_count} fields")
            return page, "main-scroll"

    # Strategy 3: iframes
    print(f"   [FormFinder] S3: Checking iframes...")
    for frame in page.frames:
        try:
            fu = (frame.url or "").lower()
            if not fu.startswith("http") or frame.url == url:
                continue
            is_known = any(p in fu for p in KNOWN_FORM_PROVIDERS)
            fc = await _count_form_fields(frame)
            if is_known or fc > 1:
                print(f"   [FormFinder] S3 success: iframe {frame.url[:60]} ({fc} fields)")
                return frame, "iframe"
        except Exception:
            continue

    # Strategy 4: wait for JS render
    print(f"   [FormFinder] S4: Waiting JS render (10s)...")
    for i in range(20):
        await asyncio.sleep(0.5)
        fc = await _count_form_fields(page)
        if fc > 0:
            print(f"   [FormFinder] S4 success: {fc} fields after {(i+1)*0.5:.1f}s")
            return page, "main-js-wait"
        if i % 4 == 3:
            await page.evaluate("window.scrollBy(0, 300)")

    # Strategy 5: click contact links
    print(f"   [FormFinder] S5: Contact links...")
    contact_link_selectors = [
        "a[href*='contact' i]","a[href*='reach' i]","a[href*='touch' i]",
        "a:has-text('Contact Us')","a:has-text('Contact')",
        "a:has-text('Get in Touch')","a:has-text('Write to Us')",
        "nav a[href*='contact' i]","footer a[href*='contact' i]",
    ]
    for sel in contact_link_selectors:
        try:
            els = page.locator(sel)
            cnt = await els.count()
            for i in range(min(cnt, 3)):
                el = els.nth(i)
                if not await el.is_visible():
                    continue
                href = (await el.get_attribute("href") or "").lower()
                if any(x in href for x in ["facebook","twitter","linkedin","mailto:","#"]):
                    continue
                print(f"   [FormFinder] S5: Clicking contact link...")
                await el.click()
                await asyncio.sleep(3)
                await _scroll_until_form(page)
                fc = await _count_form_fields(page)
                if fc > 0:
                    print(f"   [FormFinder] S5 success: {fc} fields after nav")
                    return page, "clicked-contact-link"
                for frame in page.frames:
                    try:
                        fu = (frame.url or "").lower()
                        if not fu.startswith("http"):
                            continue
                        fc2 = await _count_form_fields(frame)
                        if fc2 > 1:
                            return frame, "iframe-after-nav"
                    except Exception:
                        continue
        except Exception:
            continue

    # Strategy 6: lazy containers
    print(f"   [FormFinder] S6: Hidden containers...")
    try:
        found_container = await page.evaluate("""() => {
            const sels = [
                '.contact-form','[class*="wpcf7"]','[class*="gform"]',
                '[class*="wpforms"]','[id*="contact-form"]','[id*="contactForm"]',
                '.elementor-form','[class*="ninja-forms"]','form'
            ];
            for (const s of sels) {
                const el = document.querySelector(s);
                if (el) { el.scrollIntoView({behavior:'instant',block:'center'}); return s; }
            }
            return null;
        }""")
        if found_container:
            await asyncio.sleep(1.5)
            fc = await _count_form_fields(page)
            if fc > 0:
                print(f"   [FormFinder] S6 success via {found_container}: {fc} fields")
                return page, f"container-{found_container}"
    except Exception:
        pass

    print(f"   [FormFinder] All strategies failed - fallback main")
    return page, "fallback-main"


# ============================================================
#   GPT PROMPT BUILDER
# ============================================================

def _safe_prompt_text(value, limit: int) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text[:limit]


def _estimate_token_count(text: str) -> int:
    return max(1, (len(str(text or "")) + 3) // 4)


def _field_prompt_priority(el: dict, idx: int) -> tuple[int, int]:
    score = 0
    if bool(el.get("required", False)):
        score += 100
    if bool(el.get("visible", True)):
        score += 15

    tag = str(el.get("tag", "") or "").lower()
    typ = str(el.get("type", "") or "").lower()
    meta = " ".join([
        str(el.get("label", "") or ""),
        str(el.get("name", "") or ""),
        str(el.get("id", "") or ""),
    ]).lower()

    if tag == "textarea":
        score += 35
    if tag == "select":
        score += 24
    if typ in {"email", "tel"}:
        score += 28
    if re.search(r"name|email|mail|phone|mobile|subject|message|company|website|country|state|city|zip|postal|budget|inquir|enquir|reason", meta):
        score += 55

    return (-score, idx)


def _compact_field_for_prompt(el: dict) -> dict:
    max_len = el.get("maxlength")
    try:
        max_len = int(max_len) if max_len is not None else None
        if max_len is not None and max_len < 0:
            max_len = None
    except Exception:
        max_len = None

    options = []
    for opt in (el.get("options") or []):
        opt_text = _safe_prompt_text(opt, 32)
        if opt_text:
            options.append(opt_text)
        if len(options) >= 5:
            break

    return {
        "selector": _safe_prompt_text(el.get("sel", ""), 120),
        "tag": _safe_prompt_text(el.get("tag", ""), 12),
        "type": _safe_prompt_text(el.get("type", ""), 16),
        "label": _safe_prompt_text(el.get("label", ""), 50),
        "name": _safe_prompt_text(el.get("name", ""), 30),
        "id": _safe_prompt_text(el.get("id", ""), 30),
        "required": bool(el.get("required", False)),
        "maxlength": max_len,
        "options": options,
    }


def _build_field_catalog_json(elements, limit: int = 30) -> str:
    ranked = []
    for idx, el in enumerate(elements or []):
        sel = str(el.get("sel", "") or "").strip()
        if not sel:
            continue
        ranked.append((_field_prompt_priority(el, idx), el))

    ranked.sort(key=lambda x: x[0])
    compact = [_compact_field_for_prompt(el) for _, el in ranked[: max(1, int(limit or 1))]]
    return json.dumps(compact, ensure_ascii=True)


def _build_gpt_prompt(company_name, pitch, subject, page_text, elements) -> str:
    steps = [
        (FORM_FILL_FIELD_CATALOG_LIMIT, 180, 240, 70),
        (min(16, FORM_FILL_FIELD_CATALOG_LIMIT), 140, 200, 64),
        (12, 110, 170, 58),
        (10, 90, 140, 52),
        (8, 70, 120, 48),
    ]

    best_prompt = ""
    best_tokens = 10**9

    for field_limit, page_ctx_chars, pitch_chars, subject_chars in steps:
        fields_json = _build_field_catalog_json(elements, limit=field_limit)
        page_ctx = _safe_prompt_text(page_text, page_ctx_chars)
        prompt = f"""Plan actions to complete a website contact form for {company_name}.

Return ONLY a valid JSON array. No markdown.

STRICT OUTPUT:
[{{"action":"fill","selector":"<selector>","value":"<text>"}},{{"action":"select","selector":"<selector>","value":"<option>"}},{{"action":"check","selector":"<selector>"}},{{"action":"done"}}]

RULES:
- Allowed actions: fill, select, check, click, done.
- Use selector values exactly from FIELD_CATALOG_JSON.
- Fill required and relevant visible fields first.
- Choose non-placeholder options for selects.
- Use dial code {MY_COUNTRY_DIAL_CODE}; phone intl {MY_PHONE_INTL_E164}; phone local {MY_PHONE}.
- End with {{"action":"done"}}.

SENDER:
Name={MY_FULL_NAME}
Email={MY_EMAIL}
Phone={MY_PHONE}
PhoneIntl={MY_PHONE_INTL_E164}
Company={MY_COMPANY}
Website={MY_WEBSITE}
Country=India
Zip={MY_PIN_CODE}
Subject={_safe_prompt_text(subject, subject_chars)}
Message={_safe_prompt_text(pitch, pitch_chars)}

PAGE_CONTEXT:
{page_ctx}

FIELD_CATALOG_JSON:
{fields_json}
"""
        ai_instruction = str(os.environ.get("AI_INSTRUCTION", "") or "").strip()
        if ai_instruction:
            prompt += f"\nCRITICAL AI INSTRUCTION for form completion actions:\n{ai_instruction}\n"

        est = _estimate_token_count(prompt)
        if est < best_tokens:
            best_tokens = est
            best_prompt = prompt
        if est <= FORM_FILL_INPUT_BUDGET_TARGET:
            return prompt

    return best_prompt


def _build_missing_fields_prompt(company_name, pitch, subject, missing_elements) -> str:
    steps = [
        (min(FORM_FILL_FIELD_CATALOG_LIMIT, 20), 220, 68),
        (14, 180, 60),
        (12, 150, 54),
        (10, 130, 48),
    ]

    best_prompt = ""
    best_tokens = 10**9

    for field_limit, pitch_chars, subject_chars in steps:
        fields_json = _build_field_catalog_json(missing_elements, limit=field_limit)
        prompt = f"""Second pass form completion for {company_name}.

Return ONLY a valid JSON array.
Fill ONLY selectors from FIELD_CATALOG_JSON.

RULES:
- Allowed actions: fill, select, check, click, done.
- Use selectors exactly from FIELD_CATALOG_JSON.
- Prefer required fields first.
- Use dial code {MY_COUNTRY_DIAL_CODE}; intl phone {MY_PHONE_INTL_E164}; local phone {MY_PHONE}.
- End with {{"action":"done"}}.

SENDER:
Name={MY_FULL_NAME}
Email={MY_EMAIL}
Company={MY_COMPANY}
Website={MY_WEBSITE}
Subject={_safe_prompt_text(subject, subject_chars)}
Message={_safe_prompt_text(pitch, pitch_chars)}

FIELD_CATALOG_JSON:
{fields_json}
"""
        ai_instruction = str(os.environ.get("AI_INSTRUCTION", "") or "").strip()
        if ai_instruction:
            prompt += f"\nCRITICAL AI INSTRUCTION for form completion actions:\n{ai_instruction}\n"

        est = _estimate_token_count(prompt)
        if est < best_tokens:
            best_tokens = est
            best_prompt = prompt
        if est <= FORM_FILL_INPUT_BUDGET_TARGET:
            return prompt

    return best_prompt


def _fit_prompt_to_input_budget(prompt_text: str, max_tokens: int = FORM_FILL_MAX_INPUT_TOKENS) -> str:
    prompt = str(prompt_text or "").strip()
    if not prompt:
        return prompt

    if _estimate_token_count(prompt) <= max_tokens:
        return prompt

    page_marker = "\nPAGE_CONTEXT:\n"
    fields_marker = "\n\nFIELD_CATALOG_JSON:\n"
    if page_marker in prompt and fields_marker in prompt:
        head, rest = prompt.split(page_marker, 1)
        page_ctx, tail = rest.split(fields_marker, 1)
        prompt = f"{head}{page_marker}{_safe_prompt_text(page_ctx, 80)}{fields_marker}{tail}"

    if _estimate_token_count(prompt) <= max_tokens:
        return prompt

    if "\nFIELD_CATALOG_JSON:\n" in prompt:
        prefix, json_part = prompt.split("\nFIELD_CATALOG_JSON:\n", 1)
        try:
            catalog = json.loads(str(json_part or "").strip())
            if isinstance(catalog, list):
                cur = list(catalog)
                while len(cur) > 6:
                    candidate = prefix + "\nFIELD_CATALOG_JSON:\n" + json.dumps(cur, ensure_ascii=True)
                    if _estimate_token_count(candidate) <= max_tokens:
                        return candidate
                    cur = cur[:-2]
                prompt = prefix + "\nFIELD_CATALOG_JSON:\n" + json.dumps(cur[:6], ensure_ascii=True)
        except Exception:
            pass

    if _estimate_token_count(prompt) <= max_tokens:
        return prompt

    char_cap = max(1200, int(max_tokens * 4))
    return prompt[:char_cap]


def _normalize_selector_key(selector: str) -> str:
    s = str(selector or "").strip()
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*([>~+,\[\]\(\)=:])\s*", r"\1", s)
    return s.lower()


def _build_selector_guard(elements) -> dict:
    lookup = {}
    by_id = {}
    by_name = {}
    selectors = set()

    for el in elements or []:
        selector = str(el.get("sel", "") or "").strip()
        if not selector:
            continue

        selectors.add(selector)
        lookup[_normalize_selector_key(selector)] = selector

        el_id = str(el.get("id", "") or "").strip()
        if el_id:
            by_id[el_id.lower()] = selector
            lookup[_normalize_selector_key(f"#{el_id}")] = selector

        el_name = str(el.get("name", "") or "").strip()
        if el_name:
            by_name[el_name.lower()] = selector
            tag = str(el.get("tag", "input") or "input").strip().lower() or "input"
            lookup[_normalize_selector_key(f"{tag}[name=\"{el_name}\"]")] = selector
            lookup[_normalize_selector_key(f"[name=\"{el_name}\"]")] = selector

    return {
        "lookup": lookup,
        "by_id": by_id,
        "by_name": by_name,
        "selectors": selectors,
    }


def _merge_selector_guards(*guards) -> dict:
    merged = {
        "lookup": {},
        "by_id": {},
        "by_name": {},
        "selectors": set(),
    }
    for guard in guards:
        if not isinstance(guard, dict):
            continue
        merged["lookup"].update(guard.get("lookup", {}))
        merged["by_id"].update(guard.get("by_id", {}))
        merged["by_name"].update(guard.get("by_name", {}))
        merged["selectors"].update(guard.get("selectors", set()))
    return merged


def _resolve_action_selector(raw_selector: str, selector_guard: dict) -> str | None:
    selector = str(raw_selector or "").strip()
    if not selector:
        return None

    if not isinstance(selector_guard, dict):
        return selector

    selectors = selector_guard.get("selectors", set()) or set()
    if selector in selectors:
        return selector

    lookup = selector_guard.get("lookup", {}) or {}
    normalized = _normalize_selector_key(selector)
    if normalized in lookup:
        return lookup[normalized]

    by_id = selector_guard.get("by_id", {}) or {}
    m_id = re.search(r"#([A-Za-z0-9_:\-.]+)", selector)
    if m_id:
        candidate = by_id.get(m_id.group(1).strip().lower())
        if candidate:
            return candidate

    by_name = selector_guard.get("by_name", {}) or {}
    m_name = re.search(r"\[\s*name\s*=\s*[\"']?([^\"'\]]+)[\"']?\s*\]", selector, re.I)
    if m_name:
        candidate = by_name.get(m_name.group(1).strip().lower())
        if candidate:
            return candidate

    return None


def _extract_json_candidate(raw_text: str) -> str:
    raw = str(raw_text or "").strip()
    if not raw:
        return ""

    block_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw, re.I)
    if block_match:
        raw = block_match.group(1).strip()

    start_positions = [p for p in [raw.find("["), raw.find("{")] if p >= 0]
    if not start_positions:
        return ""
    start = min(start_positions)

    stack = []
    quote = ""
    escaped = False
    for i in range(start, len(raw)):
        ch = raw[i]

        if quote:
            if escaped:
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                continue
            if ch == quote:
                quote = ""
            continue

        if ch in ('"', "'"):
            quote = ch
            continue

        if ch == "[":
            stack.append("]")
            continue
        if ch == "{":
            stack.append("}")
            continue
        if ch in ("}", "]"):
            if not stack:
                continue
            expected = stack.pop()
            if ch != expected:
                continue
            if not stack:
                return raw[start : i + 1].strip()

    return raw[start:].strip()


def _coerce_actions_payload(parsed):
    if isinstance(parsed, dict) and isinstance(parsed.get("actions"), list):
        parsed = parsed.get("actions")
    elif isinstance(parsed, dict):
        parsed = [parsed]
    elif not isinstance(parsed, list):
        return None

    actions = [a for a in parsed if isinstance(a, dict)]
    return actions or None


def _parse_actions_json(raw_content: str):
    candidate = _extract_json_candidate(raw_content)
    if not candidate:
        return None

    variants = [candidate]
    normalized_quotes = (
        candidate.replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2018", "'")
        .replace("\u2019", "'")
    )
    if normalized_quotes != candidate:
        variants.append(normalized_quotes)

    trailing_comma_fixed = re.sub(r",\s*([}\]])", r"\1", normalized_quotes)
    if trailing_comma_fixed not in variants:
        variants.append(trailing_comma_fixed)

    for payload in variants:
        try:
            parsed = json.loads(payload)
            coerced = _coerce_actions_payload(parsed)
            if coerced:
                return coerced
        except Exception:
            pass

    for payload in variants:
        try:
            parsed = ast.literal_eval(payload)
            coerced = _coerce_actions_payload(parsed)
            if coerced:
                return coerced
        except Exception:
            pass

    return None


def _sanitize_actions(raw_actions, selector_guard: dict, max_actions: int = 120):
    if not isinstance(raw_actions, list):
        return []

    allowed_actions = {"fill", "select", "check", "click", "done"}
    cleaned = []
    seen = set()

    for act in raw_actions:
        if not isinstance(act, dict):
            continue

        action = str(act.get("action", "") or "").strip().lower()
        if action not in allowed_actions:
            continue

        if action == "done":
            cleaned.append({"action": "done"})
            break

        selector = _resolve_action_selector(act.get("selector", ""), selector_guard)
        if not selector:
            continue
        if _is_honeypot_identifier(selector):
            continue

        normalized_item = {"action": action, "selector": selector}
        if action in {"fill", "select"}:
            value = str(act.get("value", "") or "").strip()
            if not value:
                continue
            if len(value) > 500:
                value = value[:500]
            normalized_item["value"] = value

        signature = (
            normalized_item.get("action", ""),
            normalized_item.get("selector", ""),
            normalized_item.get("value", ""),
        )
        if signature in seen:
            continue

        seen.add(signature)
        cleaned.append(normalized_item)
        if len(cleaned) >= max_actions:
            break

    if cleaned and cleaned[-1].get("action") != "done":
        cleaned.append({"action": "done"})

    return cleaned


# ============================================================
#   GPT FORM FILLER
# ============================================================

async def gpt_fill_form(page, target, company_name, pitch, subject, worker_index=-1):
    
    total_filled = 0
    filled_values = {}
    print(f"   [{company_name[:20]}] [GPT] Starting...")

    # Remove overlays
    try:
        await page.evaluate("""() => {
            ['.cookie-banner','.cookie-notice','.cookie-popup','#cookie-banner',
 '#cookie-notice','.cc-banner','.cc-window','.pum-overlay',
 '[class*="gdpr"]','[class*="cookie"]','[class*="consent"]',
 '[class*="overlay"]','[class*="modal"]','[id*="cookie"]','[id*="consent"]'].forEach(s => {
                try { document.querySelectorAll(s).forEach(el => {
                    if (!el.querySelector('input,textarea,select')) el.style.display='none';
                }); } catch(e) {}
            });
        }""")
    except Exception:
        pass

    # Extract fields
    elements = []
    try:
        elements = await target.evaluate(EXTRACT_FIELDS_JS)
    except Exception as e:
        print(f"   [{company_name[:20]}] [GPT] Extractor error: {e}")
        try:
            elements = await target.evaluate("""() => {
                let res = [];
                document.querySelectorAll('input,textarea,select').forEach((el, idx) => {
                    if (['hidden','submit','button','image','reset','search','file'].includes(String(el.type || '').toLowerCase())) return;
                    const rect = el.getBoundingClientRect();
                    const cs = window.getComputedStyle(el);
                    const visible = rect.width > 0 && rect.height > 0 && cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
                    if (!visible) return;

                    let lbl = el.getAttribute('aria-label') || el.placeholder || el.name || '';
                    let sel = el.id ? '#' + CSS.escape(el.id) :
                              el.name ? el.tagName.toLowerCase() + '[name="' + el.name + '"]' :
                              el.tagName.toLowerCase() + ':nth-of-type(' + (idx+1) + ')';

                    let required = false;
                    try {
                        required = !!el.required || el.matches(':required') || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    } catch (e) {
                        required = !!el.required;
                    }

                    let opts = [];
                    if (el.tagName === 'SELECT') Array.from(el.options).slice(0,6).forEach(o => {
                        if (o.text.trim()) opts.push(o.text.trim());
                    });
                    res.push({sel, label: lbl.slice(0,40), tag: el.tagName.toLowerCase(),
                              type: el.type||'', name: el.name||'', id: el.id||'', options: opts, y: 0,
                              required: required,
                              autocomplete: String(el.getAttribute('autocomplete') || '').slice(0, 40),
                              inputmode: String(el.getAttribute('inputmode') || '').slice(0, 24),
                              pattern: String(el.getAttribute('pattern') || '').slice(0, 60),
                              maxlength: (() => {
                                  const n = parseInt(String(el.getAttribute('maxlength') || '-1'), 10);
                                  return Number.isFinite(n) && n >= 0 ? n : null;
                              })(),
                              visible: true,
                              });
                });
                return res;
            }""")
        except Exception as e2:
            print(f"   [{company_name[:20]}] [GPT] Fallback extractor failed: {e2}")

    if not elements:
        print(f"   [{company_name[:20]}] [GPT] No fields - JS fallback")
        fb = await _js_fallback_fill(page, company_name, pitch, subject)
        return fb, {}

    print(f"   [{company_name[:20]}] [GPT] {len(elements)} fields found")
    selector_guard = _build_selector_guard(elements)
    print(f"   [{company_name[:20]}] [GPT] Selector guard size: {len(selector_guard.get('selectors', set()))}")

    # Page context
    page_text = ""
    try:
        page_text = await page.evaluate("""() => {
            const els = document.querySelectorAll('h1,h2,h3,label,p');
            return Array.from(els).slice(0,10).map(e => e.innerText.trim()).join(' | ');
        }""")
    except Exception:
        pass

    # Build prompt and call GPT
    prompt = _build_gpt_prompt(company_name, pitch, subject, page_text, elements)
    prompt = _fit_prompt_to_input_budget(prompt, FORM_FILL_MAX_INPUT_TOKENS)
    print(
        f"   [{company_name[:20]}] [GPT] Prompt ~{_estimate_token_count(prompt)} tokens "
        f"(target<={FORM_FILL_MAX_INPUT_TOKENS})"
    )

    async def _request_actions_with_recovery(prompt_text: str, call_type: str, guard: dict):
        if not OPENAI_API_KEY:
            print(f"   [{company_name[:20]}] [GPT] OPENAI_API_KEY missing")
            return None

        base_prompt = _fit_prompt_to_input_budget(str(prompt_text or "").strip(), FORM_FILL_MAX_INPUT_TOKENS)
        repair_prompt = _fit_prompt_to_input_budget(
            base_prompt
            + "\n\nREPAIR MODE: prior output invalid. Return ONLY strict JSON array. "
              "Selectors must come from FIELD_CATALOG_JSON. End with {\"action\":\"done\"}.",
            FORM_FILL_MAX_INPUT_TOKENS,
        )

        attempts = [
            (OPENAI_FORM_FILL_MODEL, FORM_FILL_MAX_OUTPUT_TOKENS, call_type, base_prompt),
            (
                OPENAI_FORM_FILL_MODEL,
                FORM_FILL_MAX_OUTPUT_TOKENS_RECOVERY,
                f"{call_type}_recover",
                repair_prompt,
            ),
        ]
        last_issue = ""

        for model_name, out_cap, tracked_call_type, attempt_prompt in attempts:
            try:
                est_in = _estimate_token_count(attempt_prompt)
                resp = await asyncio.to_thread(
                    openai_client.chat.completions.create,
                    model=model_name,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a deterministic web form action planner. Output only a JSON array.",
                        },
                        {"role": "user", "content": attempt_prompt},
                    ],
                    max_completion_tokens=out_cap,
                )
                if resp.usage:
                    token_tracker.record(company_name, tracked_call_type, resp.usage, worker_index)

                finish = str((resp.choices[0].finish_reason if resp.choices else "") or "")
                raw_content = (resp.choices[0].message.content if resp.choices else "") or ""
                parsed_actions = _parse_actions_json(raw_content)
                max_actions = max(25, min(160, (len(guard.get("selectors", set())) * 3) + 5))
                actions = _sanitize_actions(parsed_actions, selector_guard=guard, max_actions=max_actions)
                if actions:
                    print(
                        f"   [{company_name[:20]}] [GPT] {len(actions)} actions planned "
                        f"(model={model_name}, in~{est_in}, cap={out_cap}, finish={finish or 'n/a'})"
                    )
                    return actions

                last_issue = (
                    f"invalid-json/empty (model={model_name}, finish={finish or 'n/a'}, "
                    f"chars={len(str(raw_content or ''))})"
                )
                print(f"   [{company_name[:20]}] [GPT] {last_issue}")
            except Exception as e:
                last_issue = f"error (model={model_name}): {str(e)[:120]}"
                print(f"   [{company_name[:20]}] [GPT] {last_issue}")

        print(f"   [{company_name[:20]}] [GPT] action planning failed: {last_issue}")
        return None

    actions = await _request_actions_with_recovery(prompt, "form_fill", selector_guard)
    if not actions:
        print(f"   [{company_name[:20]}] [GPT] Falling back to JS fill")
        fb = await _js_fallback_fill(page, company_name, pitch, subject)
        return fb, {}

    async def _execute_actions(actions_list, selector_guard_local: dict, phase_label="pass"):
        nonlocal total_filled, filled_values

        applied = 0
        for act in actions_list:
            action = str(act.get("action", "") or "").strip().lower()
            raw_selector = str(act.get("selector", "") or "").strip()
            value = str(act.get("value", "") or "")

            if action == "done":
                break
            selector = _resolve_action_selector(raw_selector, selector_guard_local)
            if not selector:
                if raw_selector:
                    print(f"   [{company_name[:20]}] [{phase_label}] - selector not allowed: {raw_selector[:45]}")
                continue
            if _is_honeypot_identifier(selector):
                print(f"   [{company_name[:20]}] [{phase_label}] - skip honeypot selector: {selector[:35]}")
                continue

            el = None
            for src in [target, page] + [f for f in page.frames if f.url and f.url.startswith("http")]:
                try:
                    loc = src.locator(selector).first
                    if await loc.count() > 0:
                        el = loc
                        break
                except Exception:
                    continue

            if not el:
                print(f"   [{company_name[:20]}] [{phase_label}] - not found: {selector[:40]}")
                continue

            try:
                is_honeypot_el = await el.evaluate("""(node) => {
                    const meta = [
                        node.name || '',
                        node.id || '',
                        node.placeholder || '',
                        node.getAttribute('aria-label') || '',
                        node.getAttribute('autocomplete') || ''
                    ].join(' ').toLowerCase();
                    return /(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/.test(meta);
                }""")
                if is_honeypot_el:
                    print(f"   [{company_name[:20]}] [{phase_label}] - skip honeypot field: {selector[:35]}")
                    continue
            except Exception:
                pass

            try:
                try:
                    handle = await el.element_handle()
                    if handle:
                        await page.evaluate(
                            "(el) => el.scrollIntoView({behavior:'instant', block:'center'})", handle
                        )
                        await asyncio.sleep(0.1)
                except Exception:
                    pass

                if action == "fill":
                    target_value = value
                    try:
                        is_email_like = await el.evaluate("""(node) => {
                            const labelText = () => {
                                try {
                                    if (node.id) {
                                        const lbl = document.querySelector('label[for="' + CSS.escape(node.id) + '"]');
                                        if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                                    }
                                } catch (e) {}
                                const wrap = node.closest('label');
                                if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                                return '';
                            };

                            const nearbyText = () => {
                                const parts = [];
                                try {
                                    const p = node.parentElement;
                                    if (p) parts.push(String(p.innerText || p.textContent || '').trim());
                                } catch (e) {}
                                return parts.join(' ').slice(0, 220);
                            };

                            const type = String(node.type || '').toLowerCase();
                            if (type === 'email') return true;
                            const meta = [
                                node.name || '',
                                node.id || '',
                                node.placeholder || '',
                                node.getAttribute('aria-label') || '',
                                node.getAttribute('autocomplete') || '',
                                labelText(),
                                nearbyText(),
                            ].join(' ').toLowerCase();
                            return /(email|e-mail|mail\\s*id|mailid)/i.test(meta);
                        }""")
                    except Exception:
                        is_email_like = False

                    if is_email_like:
                        email_candidate = re.sub(r"\s+", "", str(value or "").strip().lower())
                        if not re.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$", email_candidate):
                            email_candidate = MY_EMAIL
                        target_value = email_candidate

                    handle = await el.element_handle()
                    filled = False
                    if handle:
                        try:
                            await page.evaluate(
                                f"([el, v]) => {{ {REACT_FILL_JS}(el, v); }}",
                                [handle, target_value]
                            )
                            filled = True
                        except Exception:
                            pass
                    if not filled:
                        try:
                            await el.click(force=True)
                            await el.fill("")
                            await el.fill(target_value)
                            filled = True
                        except Exception:
                            pass
                    if filled:
                        print(f"   [{company_name[:20]}] [{phase_label}] + fill {selector[:35]} = {target_value[:25]}")
                        filled_values[selector] = target_value
                        total_filled += 1
                        applied += 1

                elif action == "select":
                    filled = False
                    selected_text = value

                    is_native_select = False
                    try:
                        is_native_select = await el.evaluate("""(node) => {
                            return String((node && node.tagName) || '').toLowerCase() === 'select';
                        }""")
                    except Exception:
                        is_native_select = False

                    if is_native_select:
                        for method in [
                            lambda: el.select_option(label=value),
                            lambda: el.select_option(value=value),
                            lambda: el.select_option(label=value.strip()),
                            lambda: el.select_option(value=value.strip()),
                        ]:
                            try:
                                await method()
                                filled = True
                                break
                            except Exception:
                                pass

                        if not filled:
                            try:
                                choice = await el.evaluate("""(selectEl, desiredRaw) => {
                                    const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
                                    const desired = norm(desiredRaw);
                                    const desiredTokens = desired.split(' ').filter(Boolean);
                                    const opts = Array.from(selectEl.options || []).map(o => ({
                                        value: String(o.value || ''),
                                        label: String((o.textContent || '').trim()),
                                    }));

                                    const isPlaceholder = (o) => {
                                        const t = norm(o.label);
                                        if (!t) return true;
                                        if (!o.value || !String(o.value).trim()) return true;
                                        if (/^(--+|select|choose|please select|choose one|default|none|n\\/a|na)$/.test(t)) return true;
                                        if (t.startsWith('select ') || t.startsWith('choose ')) return true;
                                        return false;
                                    };

                                    const usable = opts.filter(o => !isPlaceholder(o));
                                    if (!usable.length) return null;

                                    if (!desired) return usable[0];

                                    for (const o of usable) {
                                        if (norm(o.label) === desired || norm(o.value) === desired) return o;
                                    }

                                    for (const o of usable) {
                                        const l = norm(o.label);
                                        const v = norm(o.value);
                                        if (l.includes(desired) || desired.includes(l) || v.includes(desired) || desired.includes(v)) {
                                            return o;
                                        }
                                    }

                                    let best = null;
                                    let bestScore = -1;
                                    for (const o of usable) {
                                        const lt = norm(o.label).split(' ').filter(Boolean);
                                        const vt = norm(o.value).split(' ').filter(Boolean);
                                        let score = 0;
                                        for (const t of desiredTokens) {
                                            if (lt.includes(t) || vt.includes(t)) score += 1;
                                        }
                                        if (score > bestScore) {
                                            bestScore = score;
                                            best = o;
                                        }
                                    }

                                    return best || usable[0];
                                }""", value)

                                if choice and str(choice.get("value", "")).strip():
                                    await el.select_option(value=str(choice["value"]))
                                    selected_text = str(choice.get("label") or value)
                                    filled = True
                            except Exception:
                                pass

                        if filled:
                            try:
                                await el.evaluate("""(selectEl) => {
                                    ['input','change','blur'].forEach(evt =>
                                        selectEl.dispatchEvent(new Event(evt, { bubbles: true }))
                                    );
                                }""")
                            except Exception:
                                pass
                    else:
                        # Custom dropdown/listbox fallback (non-native select widgets)
                        try:
                            handle = await el.element_handle()
                            chosen_text = None
                            if handle:
                                chosen_text = await page.evaluate("""([node, desiredRaw]) => {
                                    const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9+]+/g, ' ').trim();
                                    const desired = norm(desiredRaw);
                                    const desiredTokens = desired.split(' ').filter(Boolean);

                                    const isVisible = (el) => {
                                        if (!el) return false;
                                        const r = el.getBoundingClientRect();
                                        if (!r || r.width < 1 || r.height < 1) return false;
                                        const cs = window.getComputedStyle(el);
                                        return cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
                                    };

                                    const safeClick = (el) => {
                                        if (!el) return;
                                        try { el.click(); } catch (e) {}
                                        try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); } catch (e) {}
                                        try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true })); } catch (e) {}
                                    };

                                    safeClick(node);
                                    const combo = node.closest('[role="combobox"], [aria-haspopup="listbox"], .select2, .choices, .vs__dropdown-toggle, [class*="dropdown" i], [class*="select" i]');
                                    if (combo) safeClick(combo);

                                    let roots = Array.from(document.querySelectorAll(
                                        '[role="listbox"], ul[role="listbox"], .select2-results, .dropdown-menu, .menu, .mat-select-panel, .ant-select-dropdown, .choices__list--dropdown, .vs__dropdown-menu'
                                    )).filter(isVisible);
                                    if (!roots.length) roots = [document];

                                    const candidates = [];
                                    const seen = new Set();
                                    const candidateSelector = [
                                        '[role="option"]',
                                        'li',
                                        '.select2-results__option',
                                        '.dropdown-item',
                                        '.menu-item',
                                        '.mat-option',
                                        '.ant-select-item-option',
                                        '.choices__item--selectable',
                                        '.vs__dropdown-option',
                                        'button'
                                    ].join(',');

                                    for (const root of roots) {
                                        root.querySelectorAll(candidateSelector).forEach((opt) => {
                                            if (!isVisible(opt)) return;
                                            const text = String(opt.innerText || opt.textContent || '').trim();
                                            if (!text || text.length > 120) return;
                                            const n = norm(text);
                                            if (!n) return;
                                            if (/^(select|choose|please select|none|n\\/a|na)$/i.test(n)) return;
                                            const key = n + '|' + String(opt.getAttribute('data-value') || '') + '|' + String(opt.getAttribute('value') || '');
                                            if (seen.has(key)) return;
                                            seen.add(key);
                                            candidates.push({
                                                el: opt,
                                                text,
                                                n,
                                                value: String(opt.getAttribute('data-value') || opt.getAttribute('value') || '').trim(),
                                            });
                                        });
                                    }

                                    if (!candidates.length) {
                                        try {
                                            if ('value' in node) {
                                                node.value = String(desiredRaw || '');
                                                ['input','change','blur'].forEach(evt => node.dispatchEvent(new Event(evt, { bubbles: true })));
                                                return String(desiredRaw || '');
                                            }
                                        } catch (e) {}
                                        return null;
                                    }

                                    const score = (c) => {
                                        if (!desired) return 1;
                                        if (c.n === desired) return 100;
                                        let s = 0;
                                        if (c.n.includes(desired) || desired.includes(c.n)) s += 25;
                                        for (const t of desiredTokens) {
                                            if (c.n.includes(t)) s += 6;
                                            if (norm(c.value).includes(t)) s += 4;
                                        }
                                        return s;
                                    };

                                    let best = candidates[0];
                                    let bestScore = score(best);
                                    for (const c of candidates) {
                                        const sc = score(c);
                                        if (sc > bestScore) {
                                            best = c;
                                            bestScore = sc;
                                        }
                                    }

                                    safeClick(best.el);
                                    try {
                                        if ('value' in node) {
                                            if (best.value) node.value = best.value;
                                            if (!String(node.value || '').trim()) node.value = best.text;
                                        }
                                    } catch (e) {}

                                    ['input','change','blur'].forEach(evt => {
                                        try { node.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                                    });

                                    return best.text || null;
                                }""", [handle, value])

                            if chosen_text and str(chosen_text).strip():
                                selected_text = str(chosen_text).strip()
                                filled = True
                        except Exception:
                            pass

                    if filled:
                        print(f"   [{company_name[:20]}] [{phase_label}] + select {selector[:35]} = {selected_text[:25]}")
                        filled_values[selector] = selected_text
                        total_filled += 1
                        applied += 1

                elif action == "check":
                    try:
                        if not await el.is_checked():
                            await el.check(force=True)
                    except Exception:
                        await el.click(force=True)
                    print(f"   [{company_name[:20]}] [{phase_label}] + check {selector[:35]}")
                    filled_values[selector] = "checked"
                    total_filled += 1
                    applied += 1

                elif action == "click":
                    await el.click(force=True)
                    await asyncio.sleep(0.5)

            except Exception as e:
                print(f"   [{company_name[:20]}] [{phase_label}] - skip {selector[:35]}: {str(e)[:40]}")

        return applied

    # Execute first GPT action plan
    await _execute_actions(actions, selector_guard, phase_label="pass1")

    # Detect still-empty fields and run a second GPT completion pass.
    missing_elements = []
    try:
        missing_elements = await target.evaluate("""() => {
            const out = [];
            const seen = new Set();

            const isPlaceholder = (label, value) => {
                const t = String(label || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
                if (!t) return true;
                if (!String(value || '').trim()) return true;
                if (/^(--+|select|choose|please select|choose one|default|none|n\\/a|na)$/.test(t)) return true;
                if (t.startsWith('select ') || t.startsWith('choose ')) return true;
                return false;
            };

            const mkSelector = (el, idx) => {
                if (el.id) return '#' + CSS.escape(el.id);
                if (el.name) return (el.tagName || 'input').toLowerCase() + '[name="' + el.name + '"]';
                return (el.tagName || 'input').toLowerCase() + ':nth-of-type(' + (idx + 1) + ')';
            };

            const isVisible = (el) => {
                if (!el) return false;
                const r = el.getBoundingClientRect();
                if (!r || r.width < 1 || r.height < 1) return false;
                const cs = window.getComputedStyle(el);
                return cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
            };

            const getLabel = (el) => {
                let lbl = (el.getAttribute('aria-label') || el.placeholder || '').trim();
                if (!lbl && el.id) {
                    const l = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                    if (l) lbl = (l.innerText || l.textContent || '').trim();
                }
                if (!lbl) {
                    const p = el.closest('label');
                    if (p) lbl = (p.innerText || p.textContent || '').trim();
                }
                return lbl.slice(0, 40);
            };

            const els = Array.from(document.querySelectorAll('input, textarea, select'));
            els.forEach((el, idx) => {
                const tag = (el.tagName || '').toLowerCase();
                const type = String(el.type || '').toLowerCase();
                if (tag === 'input' && ['hidden', 'submit', 'button', 'image', 'reset', 'file', 'search'].includes(type)) return;
                if (el.disabled) return;
                if (!isVisible(el)) return;

                const nm = String(el.name || el.id || el.placeholder || '').toLowerCase();
                if (/\bsearch\b|sf_s|zip.?code|keyword|flexdata/.test(nm)) return;
                if (/(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/.test(nm)) return;

                let required = false;
                try { required = !!el.required || el.matches(':required') || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true'; } catch (e) {}

                const meta = [nm, getLabel(el), String(el.getAttribute('autocomplete') || '')].join(' ').toLowerCase();
                const likelyImportant = /(name|email|mail|phone|mobile|subject|message|company|website|url|country|state|city|zip|postal|budget|inquiry|reason)/.test(meta);

                let empty = false;
                if (tag === 'select') {
                    const opt = el.options && el.selectedIndex >= 0 ? el.options[el.selectedIndex] : null;
                    const txt = opt ? (opt.textContent || '').trim() : '';
                    const val = opt ? String(opt.value || '') : String(el.value || '');
                    empty = !opt || isPlaceholder(txt, val);
                } else if (type === 'checkbox' || type === 'radio') {
                    empty = required && !el.checked;
                } else {
                    empty = !String(el.value || '').trim();
                }

                if (!empty) return;
                if (!(required || likelyImportant || tag === 'select')) return;

                const sel = mkSelector(el, idx);
                if (!sel || seen.has(sel)) return;
                seen.add(sel);

                const options = [];
                if (tag === 'select') {
                    Array.from(el.options || []).slice(0, 10).forEach(o => {
                        const t = String((o.textContent || '').trim());
                        if (t && !isPlaceholder(t, o.value)) options.push(t);
                    });
                }

                out.push({
                    sel,
                    label: getLabel(el),
                    tag,
                    type,
                    name: String(el.name || '').slice(0, 30),
                    id: String(el.id || '').slice(0, 30),
                    required,
                    autocomplete: String(el.getAttribute('autocomplete') || '').slice(0, 40),
                    inputmode: String(el.getAttribute('inputmode') || '').slice(0, 24),
                    pattern: String(el.getAttribute('pattern') || '').slice(0, 60),
                    maxlength: (() => {
                        const n = parseInt(String(el.getAttribute('maxlength') || '-1'), 10);
                        return Number.isFinite(n) && n >= 0 ? n : null;
                    })(),
                    visible: true,
                    options,
                });
            });

            return out.slice(0, 20);
        }""")
    except Exception:
        missing_elements = []

    if missing_elements:
        print(f"   [{company_name[:20]}] [GPT] Missing fields after pass1: {len(missing_elements)} - running pass2")
        retry_prompt = _build_missing_fields_prompt(company_name, pitch, subject, missing_elements)
        retry_guard = _merge_selector_guards(selector_guard, _build_selector_guard(missing_elements))
        try:
            retry_actions = await _request_actions_with_recovery(retry_prompt, "form_fill_retry", retry_guard)
            if retry_actions:
                await _execute_actions(retry_actions, retry_guard, phase_label="pass2")
        except Exception as e:
            print(f"   [{company_name[:20]}] [GPT] pass2 skipped: {str(e)[:80]}")

    print(f"   [{company_name[:20]}] [GPT] Done - {total_filled} filled")

    if total_filled == 0:
        print(f"   [{company_name[:20]}] GPT filled 0 - JS fallback...")
        total_filled = await _js_fallback_fill(page, company_name, pitch, subject)
        filled_values = {}

    return total_filled, filled_values


# ============================================================
#   JS FALLBACK FILLER
# ============================================================

async def _js_fallback_fill(page, company_name, pitch, subject) -> int:
    try:
        pitch_e   = pitch.replace('`', '\\`').replace('${', '\\${')
        subject_e = subject.replace('`', '\\`').replace('${', '\\${')
        n = await page.evaluate(f"""() => {{
            let n = 0;
            const RF = (el, val) => {{
                var proto = el.tagName === 'TEXTAREA'
                    ? window.HTMLTextAreaElement.prototype
                    : window.HTMLInputElement.prototype;
                var setter = Object.getOwnPropertyDescriptor(proto, 'value');
                if (setter && setter.set) setter.set.call(el, val);
                else el.value = val;
                ['input','change','blur'].forEach(evt =>
                    el.dispatchEvent(new Event(evt, {{bubbles:true}})));
                n++;
            }};
            document.querySelectorAll('input,textarea,select').forEach(el => {{
                if (['hidden','submit','button','image','reset','search'].includes(el.type)) return;
                
                var nm2 = (el.name || el.id || el.placeholder || '').toLowerCase();
                if (/\bsearch\b|sf_s|keyword|flexdata/.test(nm2)) return;
                if (/(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/.test(nm2)) return;
                if (nm2 === 'search' || nm2.startsWith('search')) return;
                var par = el.parentElement; var skip = false;
                for (var d=0; d<8 && par; d++) {{
                    var pc = (par.className||'').toLowerCase();
                    var pi = (par.id||'').toLowerCase();
                    var pt = (par.tagName||'').toLowerCase();
                    if (pt==='nav'||pt==='header'||pc.includes('search')||pi.includes('search')) {{ skip=true; break; }}
                    par = par.parentElement;
                }}
                if (skip) return;
                const nm = (el.name || el.id || el.placeholder || '').toLowerCase();
                const lbl = (() => {{
                    if (el.id) {{ const l = document.querySelector('label[for="'+el.id+'"]'); if(l) return l.innerText.toLowerCase(); }}
                    let p = el.parentElement;
                    for(let i=0;i<3&&p;i++) {{ const l=p.querySelector('label'); if(l) return l.innerText.toLowerCase(); p=p.parentElement; }}
                    return '';
                }})();
                const h = nm + ' ' + lbl;
                if (el.tagName === 'SELECT') {{
                    const opts = Array.from(el.options).filter(o => o.text.trim() && !/^(--|choose|select|please)/i.test(o.text));
                    if (opts.length) {{
                        const reasonLike = /(reason|inquiry|enquiry|subject|topic|type|category|purpose|regarding|role|audience|interest|intent|query|contact type)/.test(h);
                        const score = (o) => {{
                            const t = String(o.text || '').toLowerCase().trim();
                            let s = 1;
                            if (reasonLike) {{
                                if (/\bgeneral inquiry\b/.test(t) || (/\bgeneral\b/.test(t) && /\b(inquiry|enquiry|question|contact)\b/.test(t))) s += 120;
                                if (/\bother\b/.test(t)) s += 100;
                            }} else if (/\bother\b/.test(t)) s += 10;
                            return s;
                        }};

                        let pick = opts[0];
                        let bestScore = score(pick);
                        for (const o of opts) {{
                            const sc = score(o);
                            if (sc > bestScore) {{
                                pick = o;
                                bestScore = sc;
                            }}
                        }}

                        el.value = pick.value;
                        el.dispatchEvent(new Event('change',{{bubbles:true}}));
                        n++;
                    }}
                    return;
                }}
                if (el.type === 'checkbox') {{ if(!el.checked){{el.checked=true;n++;}} return; }}
                if (el.type === 'radio') return;
                const isCountryCodeLike = h.includes('country code')||h.includes('countrycode')||h.includes('dial code')||h.includes('dialcode')||h.includes('phone code')||h.includes('phonecode')||h.includes('calling code')||h.includes('isd')||h.includes('prefix');
                if (el.type==='email'||h.includes('email')) {{ RF(el,'{MY_EMAIL}'); return; }}
                if (h.includes('phone')||h.includes('mobile')||h.includes('tel')) {{ RF(el,isCountryCodeLike ? '{MY_COUNTRY_DIAL_CODE}' : (h.includes('intl')||h.includes('+') ? '{MY_PHONE_INTL_E164}' : '{MY_PHONE}')); return; }}
                if (h.includes('first')&&h.includes('name')) {{ RF(el,'{MY_FIRST_NAME}'); return; }}
                if (h.includes('last')&&h.includes('name')) {{ RF(el,'{MY_LAST_NAME}'); return; }}
                if (h.includes('name')||h.includes('contact')) {{ RF(el,'{MY_FULL_NAME}'); return; }}
                if (h.includes('company')||h.includes('org')) {{ RF(el,'{MY_COMPANY}'); return; }}
                if (h.includes('website')||h.includes('url')||h.includes('site')) {{ RF(el,'{MY_WEBSITE}'); return; }}
                if (h.includes('subject')||h.includes('topic')) {{ RF(el,`{subject_e}`); return; }}
                if (h.includes('budget')||h.includes('amount')) {{ RF(el,'10000'); return; }}
                if (isCountryCodeLike) {{ RF(el,'{MY_COUNTRY_DIAL_CODE}'); return; }}
                if (h.includes('country') && !isCountryCodeLike) {{ RF(el,'India'); return; }}
                if (h.includes('zip')||h.includes('postal')||h.includes('pin')) {{ RF(el,'{MY_PIN_CODE}'); return; }}
                if (el.tagName==='TEXTAREA'||h.includes('message')||h.includes('comment')) {{ RF(el,`{pitch_e}`); return; }}
                if (el.type==='text') {{ RF(el,'{MY_FULL_NAME}'); return; }}
            }});
            return n;
        }}""")
        print(f"   [{company_name[:20]}] [JS Fallback] Filled {n} fields")
        return n or 0
    except Exception as e:
        print(f"   [{company_name[:20]}] [JS Fallback] Error: {e}")
        return 0


async def _capture_filled_form_values(page, target, max_items: int = 80) -> dict:
    """Capture visible filled controls so logs include site-specific field values."""
    captured = {}
    cap = max(10, min(200, int(max_items or 80)))

    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            rows = await src.evaluate("""(payload) => {
                const maxItems = Math.max(5, Math.min(120, Number(payload.maxItems || 60)));
                const out = [];
                const seen = new Set();

                const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
                const clean = (s, limit) => String(s || '')
                    .replace(/["'\\]/g, '')
                    .replace(/\\s+/g, ' ')
                    .trim()
                    .slice(0, limit);

                const isVisible = (el) => {
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    if (!r || r.width < 1 || r.height < 1) return false;
                    const cs = window.getComputedStyle(el);
                    return cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
                };

                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return clean(lbl.innerText || lbl.textContent || '', 60);
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return clean(wrap.innerText || wrap.textContent || '', 60);
                    return '';
                };

                const isPlaceholderSelect = (text, value) => {
                    const t = norm(text);
                    if (!t) return true;
                    if (!String(value || '').trim()) return true;
                    if (/^(--+|select|choose|please select|choose one|default|none|n[/]a|na)$/.test(t)) return true;
                    if (t.startsWith('select ') || t.startsWith('choose ')) return true;
                    return false;
                };

                const buildKey = (el, idx) => {
                    const tag = clean((el.tagName || 'input').toLowerCase(), 16);
                    const nm = clean(el.name || '', 70);
                    if (nm) return `${tag}[name="${nm}"]`;
                    const id = clean(el.id || '', 70);
                    if (id) return `#${id}`;
                    const aria = clean(el.getAttribute('aria-label') || '', 50);
                    if (aria) return `${tag}[aria-label="${aria}"]`;
                    const lbl = labelText(el);
                    if (lbl) return `${tag}[label="${lbl}"]`;
                    return `${tag}:idx-${idx}`;
                };

                const controls = Array.from(document.querySelectorAll('input, textarea, select'));
                for (let idx = 0; idx < controls.length; idx++) {
                    if (out.length >= maxItems) break;

                    const el = controls[idx];
                    if (!el || el.disabled || !isVisible(el)) continue;

                    const tag = String(el.tagName || '').toLowerCase();
                    const type = String(el.type || '').toLowerCase();
                    if (tag === 'input' && ['hidden', 'submit', 'button', 'image', 'reset', 'file', 'search'].includes(type)) continue;

                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                    ].join(' ').toLowerCase();

                    if (/\bsearch\b|sf_s|zip.?code|keyword|flexdata/.test(meta)) continue;
                    if (/(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/.test(meta)) continue;

                    let value = '';
                    if (tag === 'select') {
                        const selected = el.options && el.selectedIndex >= 0 ? el.options[el.selectedIndex] : null;
                        const selectedText = selected ? clean(selected.textContent || selected.innerText || '', 120) : '';
                        const selectedValue = selected ? String(selected.value || '') : String(el.value || '');
                        if (isPlaceholderSelect(selectedText, selectedValue)) continue;
                        value = selectedText || clean(selectedValue, 120);
                    } else if (type === 'checkbox' || type === 'radio') {
                        if (!el.checked) continue;
                        const lbl = labelText(el);
                        value = lbl ? `checked:${lbl}` : 'checked';
                    } else {
                        value = clean(el.value || '', 180);
                        if (!value) continue;
                    }

                    const key = buildKey(el, idx);
                    if (!key) continue;

                    const signature = key + '|' + norm(value);
                    if (seen.has(signature)) continue;
                    seen.add(signature);

                    out.push({ k: key.slice(0, 150), v: value.slice(0, 180) });
                }

                return out;
            }""", {"maxItems": min(cap, 90)})
        except Exception:
            continue

        if not isinstance(rows, list):
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get("k", "") or "").strip()
            value = str(row.get("v", "") or "").strip()
            if not key or not value:
                continue
            if key in captured:
                continue
            captured[key] = value
            if len(captured) >= cap:
                return captured

    return captured


async def ensure_required_dropdowns(page, target, company_name="") -> int:
    """
    Some forms fail with messages like "Dropdown cannot be blank" even after text fields are filled.
    This pass ensures required dropdown-like fields have a non-placeholder value,
    with preference for "Other" / "General Inquiry" where appropriate.
    """
    fixed = 0
    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            changed = await src.evaluate("""() => {
                const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
                const isPlaceholder = (label, value) => {
                    const t = norm(label);
                    if (!t) return true;
                    if (!String(value || '').trim()) return true;
                    if (/^(--+|select|choose|please select|choose one|default|none|n\\/a|na)$/.test(t)) return true;
                    if (t.startsWith('please select')) return true;
                    if (t.startsWith('select ') || t.startsWith('choose ')) return true;
                    return false;
                };

                const isVisible = (el) => {
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    if (!r || r.width < 1 || r.height < 1) return false;
                    const cs = window.getComputedStyle(el);
                    return cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
                };

                const dispatch = (el) => {
                    ['input', 'change', 'blur'].forEach(evt => {
                        try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                    });
                };

                const optionText = (o) => String((o && (o.textContent || o.innerText || o.label || o.text)) || '').trim();
                const optionValue = (o) => String((o && (o.value || o.val || o.dataValue || '')) || '').trim();

                const getLabel = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const getRawMeta = (el) => [
                    el.name || '',
                    el.id || '',
                    el.getAttribute('aria-label') || '',
                    el.getAttribute('placeholder') || '',
                    el.innerText || el.textContent || '',
                    getLabel(el),
                    (el.parentElement && (el.parentElement.innerText || el.parentElement.textContent || '')) || '',
                ].join(' ');

                const getMeta = (el) => norm(getRawMeta(el));

                const hasRequiredHint = (el, rawMeta) => {
                    const rm = String(rawMeta || '');
                    const ariaRequired = String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    const ariaInvalid = String(el.getAttribute('aria-invalid') || '').toLowerCase() === 'true';
                    return !!el.required || ariaRequired || ariaInvalid || /[*]/.test(rm) || /\\brequired\\b/i.test(rm);
                };

                const shouldScanSelect = (el, rawMeta) => {
                    if (!el || el.disabled) return false;
                    if (isVisible(el)) return true;

                    // Some frameworks keep the real required <select> hidden while a custom widget is visible.
                    if (!hasRequiredHint(el, rawMeta)) return false;

                    const allOptions = Array.from(el.options || []);
                    const usableOptions = allOptions.filter(o => !isPlaceholder(optionText(o), optionValue(o)));
                    if (!usableOptions.length) return false;

                    const scope = el.closest('form') || el.parentElement || document;
                    try {
                        const proxy = scope.querySelector(
                            '[role="combobox"], [aria-haspopup="listbox"], .select2, .choices, .vs__dropdown-toggle, [class*="dropdown" i], [class*="select" i]'
                        );
                        if (proxy && isVisible(proxy)) return true;
                    } catch (e) {}

                    return true;
                };

                const hasLinkedRequiredSelect = (node) => {
                    if (!node) return false;
                    const roots = [];
                    const form = node.closest('form');
                    if (form) roots.push(form);
                    if (node.parentElement) roots.push(node.parentElement);
                    if (node.parentElement && node.parentElement.parentElement) roots.push(node.parentElement.parentElement);

                    for (const root of roots) {
                        const selects = Array.from(root.querySelectorAll('select')).slice(0, 16);
                        for (const s of selects) {
                            if (!s || s.disabled) continue;
                            const raw = getRawMeta(s);
                            if (!hasRequiredHint(s, raw)) continue;
                            const selectedOpt = s.options && s.selectedIndex >= 0 ? s.options[s.selectedIndex] : null;
                            const selectedText = selectedOpt ? optionText(selectedOpt) : '';
                            const selectedValue = selectedOpt ? optionValue(selectedOpt) : String(s.value || '');
                            if (isPlaceholder(selectedText, selectedValue)) return true;
                        }
                    }
                    return false;
                };

                const isLikelyImportantDropdown = (meta) => {
                    return /(price|range|budget|residence|referr|how were you referred|broker|agent|real\\s*estate|inquiry|enquiry|reason|subject|type|category|purpose|country|state|city)/.test(meta);
                };

                const isReasonLike = (meta) =>
                    /(reason|inquiry|enquiry|subject|topic|type|category|purpose|regarding|role|audience|interest|intent|query|contact type)/.test(meta);

                const scoreOption = (o, meta) => {
                    const txt = optionText(o);
                    const val = optionValue(o);
                    if (isPlaceholder(txt, val)) return -1;

                    const t = norm(txt);
                    let score = 1;

                    if (isReasonLike(meta)) {
                        if (/\bgeneral inquiry\b/.test(t) || (/\bgeneral\b/.test(t) && /\b(inquiry|enquiry|question|contact)\b/.test(t))) score += 220;
                        if (/\bother\b/.test(t)) score += 200;
                        if (/\bnone of the above\b/.test(t)) score += 180;
                    } else {
                        if (/\bother\b/.test(t)) score += 20;
                    }

                    return score;
                };

                const pickBestOption = (options, meta) => {
                    if (!options || !options.length) return null;
                    let best = null;
                    let bestScore = -1;
                    for (const o of options) {
                        const sc = scoreOption(o, meta);
                        if (sc > bestScore) {
                            best = o;
                            bestScore = sc;
                        }
                    }
                    return best;
                };

                let n = 0;
                // Pass 1: required/likely-important native selects.
                document.querySelectorAll('select').forEach(el => {
                    if (!el || el.disabled) return;
                    const rawMeta = getRawMeta(el);
                    if (!shouldScanSelect(el, rawMeta)) return;

                    const selectedOpt = el.options && el.selectedIndex >= 0 ? el.options[el.selectedIndex] : null;
                    const selectedText = selectedOpt ? (selectedOpt.textContent || '').trim() : '';
                    const selectedValue = selectedOpt ? selectedOpt.value : (el.value || '');
                    if (!isPlaceholder(selectedText, selectedValue)) return;

                    const meta = getMeta(el);
                    if (!(hasRequiredHint(el, rawMeta) || isLikelyImportantDropdown(meta))) return;

                    const allOptions = Array.from(el.options || []);
                    const candidate = pickBestOption(allOptions, meta);
                    if (!candidate) return;

                    const candidateIndex = allOptions.indexOf(candidate);
                    if (candidateIndex < 0) return;

                    el.selectedIndex = candidateIndex;
                    if (candidate && 'value' in candidate) {
                        el.value = String(candidate.value || '');
                    }
                    dispatch(el);
                    n += 1;
                });

                // Pass 2: custom combobox/listbox widgets (required or important labels).
                const comboSelector = [
                    '[role="combobox"]',
                    '[aria-haspopup="listbox"]',
                    '[class*="select" i]',
                    '[class*="dropdown" i]',
                ].join(',');
                const combos = Array.from(document.querySelectorAll(comboSelector)).filter(isVisible);
                for (const combo of combos) {
                    const current = String(combo.innerText || combo.textContent || combo.getAttribute('aria-label') || '').trim();
                    if (current && !isPlaceholder(current, 'value')) continue;

                    const rawMeta = getRawMeta(combo);
                    const meta = getMeta(combo);
                    const looksUnsetChoice = /(choose an option|select an option|please select)/.test(meta);
                    if (!(hasRequiredHint(combo, rawMeta) || isLikelyImportantDropdown(meta) || looksUnsetChoice || hasLinkedRequiredSelect(combo))) continue;

                    const safeClick = (el) => {
                        if (!el) return;
                        try { el.click(); } catch (e) {}
                        try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); } catch (e) {}
                        try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true })); } catch (e) {}
                    };

                    safeClick(combo);

                    const roots = Array.from(document.querySelectorAll(
                        '[role="listbox"], ul[role="listbox"], .select2-results, .dropdown-menu, .menu, .mat-select-panel, .ant-select-dropdown, .choices__list--dropdown, .vs__dropdown-menu'
                    )).filter(isVisible);
                    if (!roots.length) continue;

                    const seen = new Set();
                    const optionNodes = [];
                    const optionSelector = [
                        '[role="option"]',
                        'li',
                        '.select2-results__option',
                        '.dropdown-item',
                        '.menu-item',
                        '.mat-option',
                        '.ant-select-item-option',
                        '.choices__item--selectable',
                        '.vs__dropdown-option',
                        'button'
                    ].join(',');

                    for (const root of roots) {
                        root.querySelectorAll(optionSelector).forEach((node) => {
                            if (!isVisible(node)) return;
                            const text = String(node.innerText || node.textContent || '').trim();
                            if (!text || text.length > 120) return;
                            const value = String(node.getAttribute('data-value') || node.getAttribute('value') || '').trim();
                            if (isPlaceholder(text, value || 'x')) return;
                            const key = norm(text) + '|' + value;
                            if (seen.has(key)) return;
                            seen.add(key);
                            optionNodes.push({ text, value, _el: node });
                        });
                    }

                    const chosen = pickBestOption(optionNodes, meta);
                    if (!chosen || !chosen._el) continue;

                    const before = norm(current);
                    safeClick(chosen._el);
                    dispatch(combo);

                    const after = norm(String(combo.innerText || combo.textContent || combo.getAttribute('aria-label') || ''));
                    if ((after && after !== before) || (!before && (chosen.text || '').trim())) n += 1;
                }

                return n;
            }""")
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [DropdownFix] Auto-selected {fixed} required dropdown(s)")
    return fixed


async def ensure_required_email_fields(page, target, company_name="") -> int:
    """Safety pass: make sure required/invalid email-like fields contain MY_EMAIL."""
    fixed = 0
    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            changed = await src.evaluate("""(payload) => {
                const primaryEmail = String(payload.primaryEmail || '').trim();

                const setValue = (el, value) => {
                    try {
                        if (!el) return false;
                        const tag = String(el.tagName || '').toUpperCase();
                        if (tag === 'INPUT') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else if (tag === 'TEXTAREA') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else if (el.isContentEditable) {
                            el.textContent = value;
                        } else {
                            el.value = value;
                        }

                        ['input', 'change', 'blur', 'keyup'].forEach(evt => {
                            try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                        });
                        return true;
                    } catch (e) {
                        return false;
                    }
                };

                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const isCountryCodeMeta = (metaRaw) => {
                    const meta = String(metaRaw || '').toLowerCase();
                    return /(country[\\s_\\-]*(?:dial|phone|calling)?[\\s_\\-]*code|countrycode|dial[\\s_\\-]*code|dialcode|phone[\\s_\\-]*code|phonecode|calling[\\s_\\-]*code|isd(?:[\\s_\\-]*code)?|phone[\\s_\\-]*prefix|tel(?:ephone)?[\\s_\\-]*prefix|prefix)/i.test(meta);
                };

                const nearbyText = (el) => {
                    const parts = [];
                    try {
                        const prev = el.previousElementSibling;
                        if (prev) parts.push(String(prev.innerText || prev.textContent || '').trim());
                    } catch (e) {}
                    try {
                        const next = el.nextElementSibling;
                        if (next) parts.push(String(next.innerText || next.textContent || '').trim());
                    } catch (e) {}
                    try {
                        const p = el.parentElement;
                        if (p) parts.push(String(p.innerText || p.textContent || '').trim());
                    } catch (e) {}
                    return parts.join(' ').slice(0, 280);
                };

                const isEmailValid = (email) => /^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)+$/.test(String(email || '').trim());

                const isEmailLike = (el) => {
                    const t = String(el.type || '').toLowerCase();
                    if (t === 'email') return true;

                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                        nearbyText(el),
                    ].join(' ').toLowerCase();

                    return /(email|e-mail|mail\\s*id|mailid)/i.test(meta);
                };

                let n = 0;
                const candidates = Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]'));
                for (const el of candidates) {
                    if (!isEmailLike(el)) continue;
                    if (el.disabled) continue;

                    let invalid = false;
                    try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    const required = !!el.required || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    const v = String(el.value || el.textContent || '').trim();
                    const needsFill = (!v) || invalid || required;
                    if (!needsFill) continue;

                    const firstChoice = isEmailValid(primaryEmail) ? primaryEmail : '';
                    if (!firstChoice) continue;

                    if (!setValue(el, firstChoice)) continue;

                    let stillInvalid = false;
                    try { stillInvalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    if (stillInvalid) continue;

                    const finalValue = String(el.value || el.textContent || '').trim();
                    if (finalValue) n += 1;
                }

                return n;
            }""", {"primaryEmail": MY_EMAIL})
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [EmailFix] Filled {fixed} email field(s)")
    return fixed


async def ensure_required_subject_fields(page, target, subject_text="", company_name="") -> int:
    """Safety pass: ensure required/invalid subject-like fields have a value."""
    fixed = 0
    fallback_subject = str(subject_text or "").strip()
    if not fallback_subject:
        fallback_subject = str(MY_TITLE or "General Inquiry").replace("{company_name}", str(company_name or "")).replace("{MY_COMPANY}", str(MY_COMPANY or ""))
    fallback_subject = re.sub(r"\s+", " ", fallback_subject).strip()[:120]

    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            changed = await src.evaluate("""(payload) => {
                const subjectText = String(payload.subjectText || '').trim();

                const setValue = (el, value) => {
                    try {
                        if (!el) return false;
                        const tag = String(el.tagName || '').toUpperCase();
                        if (tag === 'INPUT') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else if (tag === 'TEXTAREA') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else if (el.isContentEditable) {
                            el.textContent = value;
                        } else {
                            el.value = value;
                        }

                        ['input', 'change', 'blur', 'keyup'].forEach(evt => {
                            try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                        });
                        return true;
                    } catch (e) {
                        return false;
                    }
                };

                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const nearbyText = (el) => {
                    const parts = [];
                    try {
                        const p = el.parentElement;
                        if (p) parts.push(String(p.innerText || p.textContent || '').trim());
                    } catch (e) {}
                    return parts.join(' ').slice(0, 260);
                };

                const isSubjectLike = (el) => {
                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                        nearbyText(el),
                    ].join(' ').toLowerCase();

                    if (/(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/i.test(meta)) return false;
                    if (/(company|organization|organisation|name|first|last|email|mail|phone|mobile|address|city|state|zip|postal|country|captcha|consent|terms|privacy|message|comment|description|details|website|url)/i.test(meta)) {
                        return false;
                    }
                    return /(subject|topic|regarding|inquir|enquir|reason|purpose|what\\s+is\\s+this\\s+about|how\\s+did\\s+you\\s+hear)/i.test(meta);
                };

                let n = 0;
                const candidates = Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]'));
                for (const el of candidates) {
                    if (!el || el.disabled) continue;
                    const type = String(el.type || '').toLowerCase();
                    if (['hidden', 'submit', 'button', 'image', 'reset', 'file', 'search', 'email', 'tel', 'number', 'password', 'url', 'date'].includes(type)) continue;
                    if (!isSubjectLike(el)) continue;

                    let invalid = false;
                    try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    const required = !!el.required || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    const current = String(el.value || el.textContent || '').trim();

                    if (!invalid && !(required && !current)) continue;
                    if (!subjectText) continue;
                    if (!setValue(el, subjectText)) continue;

                    const now = String(el.value || el.textContent || '').trim();
                    if (now) n += 1;
                }

                return n;
            }""", {"subjectText": fallback_subject})
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [SubjectFix] Filled {fixed} subject field(s)")
    return fixed


async def ensure_required_message_fields(page, target, message_text="", company_name="") -> int:
    """Safety pass: ensure required/invalid message-like fields have meaningful text."""
    fixed = 0
    fallback_message = _sanitize_pitch_text(str(message_text or "").strip())
    if not fallback_message:
        fallback_message = (
            f"Hi, I wanted to reach out from {MY_COMPANY}. "
            "We can usually onboard in 24-48 hours and support day-to-day operations. "
            "Happy to share details if useful."
        )
    fallback_message = fallback_message[:900]

    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            changed = await src.evaluate("""(payload) => {
                const messageText = String(payload.messageText || '').trim();

                const setValue = (el, value) => {
                    try {
                        if (!el) return false;
                        const tag = String(el.tagName || '').toUpperCase();
                        if (tag === 'INPUT') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else if (tag === 'TEXTAREA') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else if (el.isContentEditable) {
                            el.textContent = value;
                        } else {
                            el.value = value;
                        }

                        ['input', 'change', 'blur', 'keyup'].forEach(evt => {
                            try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                        });
                        return true;
                    } catch (e) {
                        return false;
                    }
                };

                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const nearbyText = (el) => {
                    const parts = [];
                    try {
                        const p = el.parentElement;
                        if (p) parts.push(String(p.innerText || p.textContent || '').trim());
                    } catch (e) {}
                    return parts.join(' ').slice(0, 320);
                };

                const isMessageLike = (el) => {
                    const tag = String(el.tagName || '').toLowerCase();
                    const type = String(el.type || '').toLowerCase();
                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                        nearbyText(el),
                    ].join(' ').toLowerCase();

                    if (/(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/i.test(meta)) return false;

                    const messageLike = /(message|comment|details|description|describe|inquir|enquir|question|how\\s+can\\s+we\\s+help|how\\s+may\\s+we\\s+help|anything\\s+else|type\\s+your\\s+message\\s+here)/i.test(meta);
                    if (tag === 'textarea') return true;
                    if (el.isContentEditable) return messageLike;
                    if (tag === 'input') {
                        if (['hidden', 'submit', 'button', 'image', 'reset', 'file', 'search', 'email', 'tel', 'number', 'password', 'url', 'date'].includes(type)) return false;
                        return messageLike;
                    }
                    return messageLike;
                };

                const isPlaceholderValue = (value) => {
                    const v = String(value || '').trim().toLowerCase();
                    return /^(type your message here\\.{0,3}|your message|message|enter your message|type your message)$/i.test(v);
                };

                let n = 0;
                const candidates = Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]'));
                for (const el of candidates) {
                    if (!el || el.disabled) continue;
                    if (!isMessageLike(el)) continue;

                    let invalid = false;
                    try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    if (!invalid) {
                        invalid = String(el.getAttribute('aria-invalid') || '').toLowerCase() === 'true';
                    }

                    const required = !!el.required || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    const current = String(el.value || el.textContent || '').trim();
                    const needsFill = invalid || (required && (!current || isPlaceholderValue(current)));
                    if (!needsFill) continue;

                    if (!messageText) continue;
                    if (!setValue(el, messageText)) continue;

                    let stillInvalid = false;
                    try { stillInvalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    if (stillInvalid) continue;

                    const now = String(el.value || el.textContent || '').trim();
                    if (now) n += 1;
                }

                return n;
            }""", {"messageText": fallback_message})
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [MessageFix] Filled {fixed} message field(s)")
    return fixed


async def ensure_required_name_fields(page, target, subject_text="", company_name="") -> int:
    """
    Safety pass: ensure name-like fields are populated with real name values.
    Prevents subject/message-like text from leaking into name fields.
    """
    fixed = 0
    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            changed = await src.evaluate("""(payload) => {
                const fullName = String(payload.fullName || '').trim();
                const firstName = String(payload.firstName || '').trim();
                const lastName = String(payload.lastName || '').trim();
                const subjectText = String(payload.subjectText || '').trim();

                const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();

                const setValue = (el, value) => {
                    try {
                        if (!el) return false;
                        const tag = String(el.tagName || '').toUpperCase();
                        if (tag === 'INPUT') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else if (tag === 'TEXTAREA') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else {
                            el.value = value;
                        }

                        ['input', 'change', 'blur', 'keyup'].forEach(evt => {
                            try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                        });
                        return true;
                    } catch (e) {
                        return false;
                    }
                };

                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const isCountryCodeMeta = (metaRaw) => {
                    const meta = String(metaRaw || '').toLowerCase();
                    return /(country[\\s_\\-]*(?:dial|phone|calling)?[\\s_\\-]*code|countrycode|dial[\\s_\\-]*code|dialcode|phone[\\s_\\-]*code|phonecode|calling[\\s_\\-]*code|isd(?:[\\s_\\-]*code)?|phone[\\s_\\-]*prefix|tel(?:ephone)?[\\s_\\-]*prefix|prefix)/i.test(meta);
                };

                const nearbyText = (el) => {
                    const parts = [];
                    try {
                        const p = el.parentElement;
                        if (p) parts.push(String(p.innerText || p.textContent || '').trim());
                    } catch (e) {}
                    return parts.join(' ').slice(0, 260);
                };

                const classifyNameKind = (el) => {
                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                        nearbyText(el),
                    ].join(' ').toLowerCase();

                    if (/(company|organization|organisation|business|property|project|building|website|url|email|subject|message|phone|mobile)/i.test(meta)) {
                        return '';
                    }
                    if (/(first\\s*name|fname|given\\s*name)/i.test(meta)) return 'first';
                    if (/(last\\s*name|lname|surname|family\\s*name)/i.test(meta)) return 'last';
                    if (/(full\\s*name|contact\\s*name|your\\s*name|\\bname\\b)/i.test(meta)) return 'full';
                    return '';
                };

                const isSuspiciousName = (value) => {
                    const raw = String(value || '').trim();
                    if (!raw) return true;

                    const nRaw = norm(raw);
                    const nSubj = norm(subjectText);
                    if (nSubj && (nRaw === nSubj || nRaw.includes(nSubj) || nSubj.includes(nRaw))) return true;

                    if (/virtual assistant support|hyperstaff|support\\s+for/i.test(raw)) return true;
                    if (/https?:\\/\\//i.test(raw)) return true;
                    if (/@/.test(raw)) return true;
                    if (/\\d{5,}/.test(raw)) return true;
                    if (raw.length > 60) return true;
                    return false;
                };

                const pickValue = (kind) => {
                    if (kind === 'first') {
                        const fn = firstName || fullName.split(/\\s+/).filter(Boolean)[0] || '';
                        return fn;
                    }
                    if (kind === 'last') {
                        const ln = lastName || fullName.split(/\\s+/).filter(Boolean).slice(-1)[0] || '';
                        return ln;
                    }
                    return fullName;
                };

                let n = 0;
                const fields = Array.from(document.querySelectorAll('input, textarea'));
                for (const el of fields) {
                    if (!el || el.disabled) continue;
                    const t = String(el.type || '').toLowerCase();
                    if (['hidden', 'submit', 'button', 'image', 'reset', 'file', 'search', 'email', 'tel', 'number'].includes(t)) continue;

                    const kind = classifyNameKind(el);
                    if (!kind) continue;

                    let invalid = false;
                    try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    const required = !!el.required || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    const current = String(el.value || '').trim();

                    const needsFix = invalid || required || isSuspiciousName(current);
                    if (!needsFix) continue;

                    const replacement = String(pickValue(kind) || '').trim();
                    if (!replacement) continue;
                    if (!setValue(el, replacement)) continue;

                    const now = String(el.value || '').trim();
                    if (now) n += 1;
                }

                return n;
            }""", {
                "fullName": str(MY_FULL_NAME or ""),
                "firstName": str(MY_FIRST_NAME or ""),
                "lastName": str(MY_LAST_NAME or ""),
                "subjectText": str(subject_text or ""),
            })
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [NameFix] Corrected {fixed} name field(s)")
    return fixed


async def detect_visible_phone_controls(page, target, company_name="") -> dict:
    """Detect visible phone/country-code controls to avoid false phone retries."""
    summary = {
        "visible_phone": 0,
        "required_phone": 0,
        "invalid_phone": 0,
        "visible_country_code": 0,
        "present": False,
    }

    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            detected = await src.evaluate("""() => {
                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const isCountryCodeMeta = (metaRaw) => {
                    const meta = String(metaRaw || '').toLowerCase();
                    return /(country[\\s_\\-]*(?:dial|phone|calling)?[\\s_\\-]*code|countrycode|dial[\\s_\\-]*code|dialcode|phone[\\s_\\-]*code|phonecode|calling[\\s_\\-]*code|isd(?:[\\s_\\-]*code)?|phone[\\s_\\-]*prefix|tel(?:ephone)?[\\s_\\-]*prefix)/i.test(meta);
                };

                const isVisible = (el) => {
                    if (!el) return false;
                    try {
                        const cs = window.getComputedStyle(el);
                        if (!cs) return false;
                        if (cs.display === 'none' || cs.visibility === 'hidden') return false;
                        if (Number.parseFloat(cs.opacity || '1') === 0) return false;
                    } catch (e) {
                        return false;
                    }
                    const rc = el.getBoundingClientRect();
                    return !!(rc && rc.width > 0 && rc.height > 0);
                };

                const isPhoneLike = (el) => {
                    const t = String(el.type || '').toLowerCase();
                    if (t === 'tel') return true;

                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                    ].join(' ').toLowerCase();

                    return /(\\bphone\\b|\\bmobile\\b|\\btelephone\\b|\\btel\\b|\\bph\\s*no\\b|contact\\s*number|\\bwhatsapp\\b)/i.test(meta) || isCountryCodeMeta(meta);
                };

                let visiblePhone = 0;
                let requiredPhone = 0;
                let invalidPhone = 0;
                let visibleCountryCode = 0;

                for (const el of Array.from(document.querySelectorAll('input, textarea, select'))) {
                    if (!el || el.disabled) continue;
                    const type = String(el.type || '').toLowerCase();
                    if (type === 'hidden') continue;
                    if (!isPhoneLike(el)) continue;
                    if (!isVisible(el)) continue;

                    visiblePhone += 1;

                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                    ].join(' ').toLowerCase();
                    if (isCountryCodeMeta(meta)) visibleCountryCode += 1;

                    const required = !!el.required || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    if (required) requiredPhone += 1;

                    let invalid = false;
                    try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    if (invalid) invalidPhone += 1;
                }

                return { visiblePhone, requiredPhone, invalidPhone, visibleCountryCode };
            }""")

            if isinstance(detected, dict):
                summary["visible_phone"] += int(detected.get("visiblePhone", 0) or 0)
                summary["required_phone"] += int(detected.get("requiredPhone", 0) or 0)
                summary["invalid_phone"] += int(detected.get("invalidPhone", 0) or 0)
                summary["visible_country_code"] += int(detected.get("visibleCountryCode", 0) or 0)
        except Exception:
            continue

    summary["present"] = bool(summary["visible_phone"] > 0 or summary["visible_country_code"] > 0)
    return summary


async def ensure_required_phone_fields(page, target, company_name="", prefer_international: bool = False) -> int:
    """Safety pass: make sure required/invalid phone-like fields contain a usable phone value."""
    fixed = 0
    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    phone_digits = re.sub(r"\D+", "", str(MY_PHONE or ""))
    phone_intl_e164 = str(MY_PHONE_INTL_E164 or MY_PHONE_INTL or "")
    phone_intl_digits = re.sub(r"\D+", "", phone_intl_e164)
    country_code = str(MY_COUNTRY_DIAL_CODE or "")
    country_code_digits = re.sub(r"\D+", "", country_code)

    for src in sources:
        try:
            changed = await src.evaluate("""(payload) => {
                const phone = String(payload.phone || '');
                const phoneIntl = String(payload.phoneIntl || '');
                const phoneIntlE164 = String(payload.phoneIntlE164 || '');
                const phoneDisplay = String(payload.phoneDisplay || '');
                const phoneDigits = String(payload.phoneDigits || '').replace(/\\D+/g, '');
                const phoneIntlDigits = String(payload.phoneIntlDigits || '').replace(/\\D+/g, '');
                const countryCode = String(payload.countryCode || '');
                const countryCodeDigits = String(payload.countryCodeDigits || '').replace(/\\D+/g, '');
                const preferInternational = !!payload.preferInternational;

                const setValue = (el, value) => {
                    try {
                        if (!el) return false;
                        const tag = String(el.tagName || '').toUpperCase();
                        if (tag === 'INPUT') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else if (tag === 'TEXTAREA') {
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value');
                            if (setter && setter.set) setter.set.call(el, value);
                            else el.value = value;
                        } else {
                            el.value = value;
                        }

                        ['input', 'change', 'blur', 'keyup'].forEach(evt => {
                            try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                        });
                        return true;
                    } catch (e) {
                        return false;
                    }
                };

                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const isCountryCodeMeta = (metaRaw) => {
                    const meta = String(metaRaw || '').toLowerCase();
                    return /(country[\\s_\\-]*(?:dial|phone|calling)?[\\s_\\-]*code|countrycode|dial[\\s_\\-]*code|dialcode|phone[\\s_\\-]*code|phonecode|calling[\\s_\\-]*code|isd(?:[\\s_\\-]*code)?|phone[\\s_\\-]*prefix|tel(?:ephone)?[\\s_\\-]*prefix)/i.test(meta);
                };

                const isVisible = (el) => {
                    if (!el) return false;
                    try {
                        const cs = window.getComputedStyle(el);
                        if (!cs) return false;
                        if (cs.display === 'none' || cs.visibility === 'hidden') return false;
                        if (Number.parseFloat(cs.opacity || '1') === 0) return false;
                    } catch (e) {
                        return false;
                    }
                    const rc = el.getBoundingClientRect();
                    return !!(rc && rc.width > 0 && rc.height > 0);
                };

                const isPhoneLike = (el) => {
                    const t = String(el.type || '').toLowerCase();
                    if (t === 'tel') return true;

                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                    ].join(' ').toLowerCase();

                    return /(\\bphone\\b|\\bmobile\\b|\\btelephone\\b|\\btel\\b|\\bph\\s*no\\b|contact\\s*number|\\bwhatsapp\\b)/i.test(meta) || isCountryCodeMeta(meta);
                };

                const chooseCandidates = (el) => {
                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        labelText(el),
                    ].join(' ').toLowerCase();

                    const isCountryCodeField = isCountryCodeMeta(meta);
                    const isIntl = /(intl|international|\\+)/i.test(meta) || isCountryCodeField;
                    const type = String(el.type || '').toLowerCase();
                    const inputMode = String(el.getAttribute('inputmode') || '').toLowerCase();
                    const pattern = String(el.getAttribute('pattern') || '').toLowerCase();
                    const numericOnly = type === 'number' || inputMode === 'numeric' || /^\\d[\\d\\s\\-\\+\\(\\)\\{\\},]*$/.test(pattern);
                    const patternPrefersIntl = /\\+|country\\s*code|dial\\s*code|calling\\s*code|isd/.test(pattern);
                    const preferIntl = preferInternational || isIntl || patternPrefersIntl;

                    const inferredDialDigits = (() => {
                        if (countryCodeDigits) return countryCodeDigits;
                        if (!phoneIntlDigits) return '';
                        if (phoneIntlDigits.startsWith('971')) return '971';
                        if (phoneIntlDigits.startsWith('91')) return '91';
                        if (phoneIntlDigits.startsWith('44')) return '44';
                        if (phoneIntlDigits.startsWith('61')) return '61';
                        if (phoneIntlDigits.startsWith('1')) return '1';
                        return phoneIntlDigits.slice(0, 1);
                    })();

                    let raw = [];
                    if (isCountryCodeField) {
                        raw = [
                            countryCode,
                            inferredDialDigits ? ('+' + inferredDialDigits) : '',
                            countryCodeDigits,
                            inferredDialDigits,
                        ];
                        if (numericOnly) {
                            raw = [countryCodeDigits, inferredDialDigits, inferredDialDigits ? ('+' + inferredDialDigits) : '', countryCode];
                        }
                    } else {
                        raw = preferIntl
                            ? [phoneIntlE164, phoneIntl, phoneDisplay, phone, '+' + phoneIntlDigits, phoneIntlDigits, phoneDigits]
                            : [phone, phoneDisplay, phoneDigits, phoneIntlE164, phoneIntl, phoneIntlDigits, '+' + phoneIntlDigits];

                        if (numericOnly) {
                            raw = preferIntl
                                ? [phoneIntlDigits, phoneDigits, phoneIntlE164, '+' + phoneIntlDigits, phoneIntl, phone, phoneDisplay]
                                : [phoneDigits, phoneIntlDigits, phoneIntlE164, '+' + phoneIntlDigits, phone, phoneIntl, phoneDisplay];
                        }
                    }

                    raw = raw.map(v => String(v || '').trim()).filter(Boolean);

                    const maxLenRaw = parseInt(String(el.getAttribute('maxlength') || '-1'), 10);
                    const maxLen = Number.isFinite(maxLenRaw) ? maxLenRaw : -1;
                    if (maxLen > 0) {
                        const constrained = raw.filter(v => String(v).length <= maxLen || String(v).replace(/\\D+/g, '').length <= maxLen);
                        if (constrained.length) raw = constrained;
                    }

                    if (isCountryCodeField) {
                        const normalizedCountry = [];
                        for (const v of raw) {
                            const digits = String(v || '').replace(/\\D+/g, '').slice(0, 4);
                            if (!digits) continue;
                            normalizedCountry.push('+' + digits);
                            normalizedCountry.push(digits);
                        }
                        if (normalizedCountry.length) {
                            raw = normalizedCountry.concat(raw);
                        }
                    }

                    // Remove duplicates while preserving order.
                    const seen = new Set();
                    return raw.filter(v => {
                        const key = String(v);
                        if (seen.has(key)) return false;
                        seen.add(key);
                        return true;
                    });
                };

                let n = 0;
                const candidates = Array.from(document.querySelectorAll('input, textarea'));
                for (const el of candidates) {
                    if (!isPhoneLike(el)) continue;
                    if (el.disabled) continue;
                    if (!isVisible(el)) continue;

                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.placeholder || '',
                        el.getAttribute('aria-label') || '',
                        el.getAttribute('autocomplete') || '',
                        labelText(el),
                    ].join(' ').toLowerCase();
                    const isCountryCodeField = isCountryCodeMeta(meta);

                    let invalid = false;
                    try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    const required = !!el.required || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    const v = String(el.value || '').trim();
                    const countryCodeSuspicious = isCountryCodeField && (!!v) && !/^\\+?\\d{1,4}$/.test(v.replace(/\\s+/g, ''));
                    const needsFill = (!v) || invalid || required || countryCodeSuspicious;
                    if (!needsFill) continue;

                    const vals = chooseCandidates(el);
                    let setOk = false;
                    for (const val of vals) {
                        if (!setValue(el, val)) continue;
                        let nowInvalid = false;
                        try { nowInvalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                        if (!nowInvalid) {
                            setOk = true;
                            break;
                        }
                    }

                    if (setOk) n += 1;
                }

                return n;
            }""", {
                "phone": str(MY_PHONE or ""),
                "phoneIntl": str(MY_PHONE_INTL or ""),
                "phoneIntlE164": phone_intl_e164,
                "phoneDisplay": str(MY_PHONE_DISPLAY or ""),
                "phoneDigits": phone_digits,
                "phoneIntlDigits": phone_intl_digits,
                "countryCode": country_code,
                "countryCodeDigits": country_code_digits,
                "preferInternational": bool(prefer_international),
            })
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [PhoneFix] Filled {fixed} phone field(s)")
    return fixed


async def ensure_phone_country_code_dropdown(page, target, company_name="") -> int:
    """Try to select phone country code for native selects and intl phone widgets."""
    fixed = 0
    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    country_dial_digits = re.sub(r"\D+", "", str(MY_COUNTRY_DIAL_CODE or ""))
    intl_digits = re.sub(r"\D+", "", str(MY_PHONE_INTL_E164 or MY_PHONE_INTL or ""))
    dial_candidates = []
    if country_dial_digits:
        dial_candidates.append(f"+{country_dial_digits}")
    elif intl_digits:
        if intl_digits.startswith("971"):
            dial_candidates.append("+971")
        elif intl_digits.startswith("91"):
            dial_candidates.append("+91")
        elif intl_digits.startswith("44"):
            dial_candidates.append("+44")
        elif intl_digits.startswith("61"):
            dial_candidates.append("+61")
        elif intl_digits.startswith("1"):
            dial_candidates.append("+1")
        else:
            dial_candidates.append(f"+{intl_digits[:1]}")

    if not dial_candidates:
        dial_candidates = ["+1"]

    code_to_country = {
        "+1": ["united states", "usa", "us", "canada"],
        "+91": ["india", "in"],
        "+44": ["united kingdom", "uk", "gb", "britain"],
        "+61": ["australia", "au"],
        "+971": ["united arab emirates", "uae", "ae"],
    }
    country_hints = []
    for c in dial_candidates:
        country_hints.extend(code_to_country.get(c, []))

    payload = {
        "dialCandidates": dial_candidates,
        "countryHints": country_hints,
    }

    for src in sources:
        try:
            changed = await src.evaluate("""(payload) => {
                const dialCandidates = Array.isArray(payload.dialCandidates) ? payload.dialCandidates : [];
                const countryHints = Array.isArray(payload.countryHints) ? payload.countryHints : [];

                const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9+]+/g, ' ').trim();
                const clean = (s) => String(s || '').trim();
                const hasDial = (s) => /\\+\\d{1,4}/.test(String(s || ''));
                const isCountryCodeMeta = (metaRaw) => {
                    const meta = String(metaRaw || '').toLowerCase();
                    return /(country[\\s_\\-]*(?:dial|phone|calling)?[\\s_\\-]*code|countrycode|dial[\\s_\\-]*code|dialcode|phone[\\s_\\-]*code|phonecode|calling[\\s_\\-]*code|isd(?:[\\s_\\-]*code)?|phone[\\s_\\-]*prefix|tel(?:ephone)?[\\s_\\-]*prefix)/i.test(meta);
                };

                const phoneSelectors = 'input[type="tel"], input[name*="phone" i], input[id*="phone" i], input[placeholder*="phone" i], input[name*="mobile" i], input[id*="mobile" i]';

                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const isPlaceholderOpt = (label, value) => {
                    const t = norm(label);
                    if (!t) return true;
                    if (!clean(value)) return true;
                    if (/^(--+|select|choose|please select|choose one|default|none|n\\/a|na)$/.test(t)) return true;
                    if (t.startsWith('select ') || t.startsWith('choose ')) return true;
                    return false;
                };

                const pickBestOption = (options) => {
                    const opts = options.map(o => ({
                        el: o,
                        value: clean(o.value),
                        label: clean(o.textContent || o.innerText || ''),
                    })).filter(o => !isPlaceholderOpt(o.label, o.value));

                    if (!opts.length) return null;

                    for (const code of dialCandidates) {
                        const ncode = norm(code);
                        const hit = opts.find(o => norm(o.label).includes(ncode) || norm(o.value).includes(ncode));
                        if (hit) return hit;
                    }

                    for (const hint of countryHints) {
                        const nh = norm(hint);
                        const hit = opts.find(o => norm(o.label).includes(nh) || norm(o.value).includes(nh));
                        if (hit) return hit;
                    }

                    const dialHit = opts.find(o => hasDial(o.label) || hasDial(o.value));
                    if (dialHit) return dialHit;

                    return opts[0];
                };

                const hasNearbyPhoneInput = (el) => {
                    let p = el;
                    for (let d = 0; d < 6 && p; d++) {
                        try {
                            if (p.querySelector && p.querySelector(phoneSelectors)) return true;
                        } catch (e) {}
                        p = p.parentElement;
                    }
                    return false;
                };

                const dispatch = (el) => {
                    ['input', 'change', 'blur', 'click'].forEach(evt => {
                        try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                    });
                };

                let n = 0;

                // Pass 1: native <select> country-code controls near phone fields.
                const phoneInputs = Array.from(document.querySelectorAll(phoneSelectors));
                const seenSelects = new Set();

                for (const pi of phoneInputs) {
                    let p = pi;
                    for (let d = 0; d < 6 && p; d++) {
                        const selects = Array.from(p.querySelectorAll('select'));
                        for (const sel of selects) {
                            if (!sel || sel.disabled) continue;
                            if (seenSelects.has(sel)) continue;

                            const optionObjs = Array.from(sel.options || []);
                            if (!optionObjs.length) continue;

                            const meta = [
                                sel.name || '',
                                sel.id || '',
                                sel.className || '',
                                sel.getAttribute('aria-label') || '',
                                labelText(sel),
                            ].join(' ').toLowerCase();

                            const hasDialOptions = optionObjs.some(o => hasDial(o.textContent || '') || hasDial(o.value || ''));
                            if (!isCountryCodeMeta(meta) && !hasDialOptions) continue;

                            seenSelects.add(sel);

                            const selected = sel.selectedIndex >= 0 ? sel.options[sel.selectedIndex] : null;
                            const selectedTxt = selected ? clean(selected.textContent || '') : '';
                            const selectedVal = selected ? clean(selected.value || '') : clean(sel.value || '');
                            const selectedLooksValidCode = /^\\+?\\d{1,4}$/.test(selectedVal);
                            if (!isPlaceholderOpt(selectedTxt, selectedVal) && (hasDial(selectedTxt) || hasDial(selectedVal) || selectedLooksValidCode)) {
                                continue;
                            }

                            const best = pickBestOption(optionObjs);
                            if (!best) continue;

                            try {
                                sel.value = String(best.value || '');
                                dispatch(sel);
                                n += 1;
                            } catch (e) {}
                        }
                        p = p.parentElement;
                    }
                }

                // Pass 1b: global <select> scan for country-code controls not near phone inputs.
                const allSelects = Array.from(document.querySelectorAll('select'));
                for (const sel of allSelects) {
                    if (!sel || sel.disabled) continue;
                    if (seenSelects.has(sel)) continue;

                    const optionObjs = Array.from(sel.options || []);
                    if (!optionObjs.length) continue;

                    const meta = [
                        sel.name || '',
                        sel.id || '',
                        sel.className || '',
                        sel.getAttribute('aria-label') || '',
                        labelText(sel),
                    ].join(' ').toLowerCase();

                    const hasDialOptions = optionObjs.some(o => hasDial(o.textContent || '') || hasDial(o.value || ''));
                    const nearPhone = hasNearbyPhoneInput(sel);
                    if (!isCountryCodeMeta(meta) && !(hasDialOptions && nearPhone)) continue;

                    seenSelects.add(sel);

                    const selected = sel.selectedIndex >= 0 ? sel.options[sel.selectedIndex] : null;
                    const selectedTxt = selected ? clean(selected.textContent || '') : '';
                    const selectedVal = selected ? clean(selected.value || '') : clean(sel.value || '');
                    const selectedLooksValidCode = /^\\+?\\d{1,4}$/.test(selectedVal);
                    if (!isPlaceholderOpt(selectedTxt, selectedVal) && (hasDial(selectedTxt) || hasDial(selectedVal) || selectedLooksValidCode)) {
                        continue;
                    }

                    const best = pickBestOption(optionObjs);
                    if (!best) continue;

                    try {
                        sel.value = String(best.value || '');
                        dispatch(sel);
                        n += 1;
                    } catch (e) {}
                }

                // Pass 2: custom intl-phone dropdown widgets (e.g. iti flag dropdown).
                const containers = Array.from(document.querySelectorAll('.iti, .intl-tel-input, [class*="intl-tel" i], [class*="phone" i]'));
                for (const c of containers) {
                    const containerMeta = norm([
                        c.className || '',
                        c.id || '',
                        c.getAttribute('aria-label') || '',
                    ].join(' '));
                    const looksCountryWidget = /(country|dial|code|prefix|isd|iti|intl|phone)/i.test(containerMeta);
                    if (!c.querySelector(phoneSelectors) && !looksCountryWidget) continue;

                    const opener = c.querySelector(
                        '.iti__selected-flag, .selected-flag, .iti__flag-container, .flag-container, [aria-haspopup="listbox"], [role="combobox"], button'
                    );
                    if (!opener) continue;

                    try { opener.click(); } catch (e) {}

                    const listRoot =
                        c.querySelector('.iti__country-list, .country-list, [role="listbox"], ul[role="listbox"], [class*="country-list" i]') ||
                        document.querySelector('.iti__country-list, .country-list, [role="listbox"], ul[role="listbox"], [class*="country-list" i]') ||
                        c;
                    const options = Array.from(listRoot.querySelectorAll('.iti__country, [role="option"], li')).filter(o => {
                        const txt = clean(o.innerText || o.textContent || '');
                        if (!txt) return false;
                        return hasDial(txt) || /(country|united states|usa|india|canada|uk|australia|uae|dial|code)/i.test(txt);
                    });
                    if (!options.length) continue;

                    const mappedOptions = options.map(o => {
                        const rawDial = clean(o.getAttribute('data-dial-code') || '');
                        const dialValue = rawDial ? ('+' + String(rawDial).replace(/^\\+/, '')) : '';
                        return { value: dialValue || rawDial, textContent: o.innerText || o.textContent || '' };
                    });
                    const best = pickBestOption(mappedOptions);
                    let chosen = null;
                    if (best) {
                        chosen = options.find(o => {
                            const txt = clean(o.innerText || o.textContent || '');
                            const rawDial = clean(o.getAttribute('data-dial-code') || '');
                            const val = rawDial ? ('+' + String(rawDial).replace(/^\\+/, '')) : rawDial;
                            return norm(txt) === norm(best.label) || norm(val) === norm(best.value) || norm(txt).includes(norm(best.label));
                        });
                    }
                    if (!chosen) {
                        chosen = options[0];
                    }

                    if (chosen) {
                        try {
                            chosen.click();
                            dispatch(chosen);
                            n += 1;
                        } catch (e) {}
                    }
                }

                return n;
            }""", payload)
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [PhoneCountryFix] Selected country code {fixed} time(s)")
    return fixed


async def ensure_required_consent_checks(page, target, company_name="") -> int:
    """
    Some forms block submission with native browser validation like
    "Please check this box if you want to proceed".
    This pass checks required checkboxes/radios before submit.
    """
    fixed = 0
    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            changed = await src.evaluate("""() => {
                const dispatch = (el) => {
                    ['input', 'change', 'blur', 'click'].forEach(evt => {
                        try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                    });
                };

                const tryCheck = (el) => {
                    if (!el || el.disabled) return false;
                    if (el.checked) return false;

                    try { el.click(); } catch (e) {}
                    if (!el.checked) {
                        try {
                            const id = el.id || '';
                            if (id) {
                                const lbl = document.querySelector('label[for="' + CSS.escape(id) + '"]');
                                if (lbl) lbl.click();
                            }
                        } catch (e) {}
                    }
                    if (!el.checked) {
                        try { el.checked = true; } catch (e) {}
                    }

                    dispatch(el);
                    return !!el.checked;
                };

                let n = 0;
                const nativeBoxes = Array.from(document.querySelectorAll('input[type="checkbox"], input[type="radio"]'));
                for (const el of nativeBoxes) {
                    let invalid = false;
                    try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    const required = !!el.required || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    if (!(required || invalid)) continue;
                    if (tryCheck(el)) n += 1;
                }

                return n;
            }""")
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [ConsentFix] Checked {fixed} required checkbox/radio field(s)")
    return fixed


async def ensure_consent_by_heuristics(page, target, company_name="") -> int:
    """
    Fallback pass for sites where consent controls are not marked required,
    but submit still fails with checkbox/privacy/terms prompts.
    """
    fixed = 0
    sources = [target]
    if target != page:
        sources.append(page)

    for frame in page.frames:
        try:
            if frame.url and frame.url.startswith("http") and frame not in sources:
                sources.append(frame)
        except Exception:
            continue

    for src in sources:
        try:
            changed = await src.evaluate("""() => {
                const consentRe = /(agree|consent|terms|privacy|policy|gdpr|permission|authorize|accept|marketing|by submitting|sms|text message|text\\s*texts|receive\\s+sms|contact info)/i;

                const dispatch = (el) => {
                    ['input', 'change', 'blur', 'click'].forEach(evt => {
                        try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                    });
                };

                const labelText = (el) => {
                    try {
                        if (el.id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                            if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                        }
                    } catch (e) {}
                    const wrap = el.closest('label');
                    if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                    return '';
                };

                const nearbyText = (el) => {
                    const bits = [];
                    try {
                        const prev = el.previousElementSibling;
                        if (prev) bits.push(String(prev.innerText || prev.textContent || '').trim());
                    } catch (e) {}
                    try {
                        const next = el.nextElementSibling;
                        if (next) bits.push(String(next.innerText || next.textContent || '').trim());
                    } catch (e) {}
                    try {
                        const p = el.parentElement;
                        if (p) bits.push(String(p.innerText || p.textContent || '').trim());
                    } catch (e) {}
                    return bits.join(' ').slice(0, 300);
                };

                const tryCheck = (el) => {
                    if (!el || el.disabled || el.checked) return false;
                    try { el.click(); } catch (e) {}
                    if (!el.checked) {
                        try { el.checked = true; } catch (e) {}
                    }
                    dispatch(el);
                    return !!el.checked;
                };

                let n = 0;
                const radiosHandled = new Set();
                const controls = Array.from(document.querySelectorAll('input[type="checkbox"], input[type="radio"]'));

                for (const el of controls) {
                    let invalid = false;
                    try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                    const required = !!el.required || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                    const meta = [
                        el.name || '',
                        el.id || '',
                        el.className || '',
                        el.getAttribute('aria-label') || '',
                        labelText(el),
                        nearbyText(el),
                    ].join(' ').toLowerCase();

                    if (!(required || invalid || consentRe.test(meta))) continue;

                    if ((el.type || '').toLowerCase() === 'radio' && el.name) {
                        const key = String(el.name);
                        if (radiosHandled.has(key)) continue;
                        radiosHandled.add(key);
                        const group = Array.from(document.querySelectorAll('input[type="radio"][name="' + CSS.escape(key) + '"]'));
                        if (group.some(r => r.checked)) continue;
                        const candidate = group.find(r => !r.disabled) || el;
                        if (tryCheck(candidate)) n += 1;
                        continue;
                    }

                    if (tryCheck(el)) n += 1;
                }

                return n;
            }""")
            fixed += int(changed or 0)
        except Exception:
            continue

    if fixed > 0:
        print(f"   [{company_name[:20]}] [ConsentHeuristic] Checked {fixed} additional consent field(s)")
    return fixed


# ============================================================
#   CHECKBOXES
# ============================================================

async def handle_checkboxes(target, company_name="") -> int:
    """Prefer 'Other' choices, then ensure required/consent checks are selected."""
    fixed = 0

    # JS-first approach: supports hidden/custom widgets and label-based option choice.
    try:
        changed = await target.evaluate("""() => {
            const dispatch = (el) => {
                ['input', 'change', 'blur', 'click'].forEach(evt => {
                    try { el.dispatchEvent(new Event(evt, { bubbles: true })); } catch (e) {}
                });
            };

            const norm = (s) => String(s || '').toLowerCase().replace(/\\s+/g, ' ').trim();

            const labelText = (el) => {
                try {
                    if (el.id) {
                        const lbl = document.querySelector('label[for="' + CSS.escape(el.id) + '"]');
                        if (lbl) return String(lbl.innerText || lbl.textContent || '').trim();
                    }
                } catch (e) {}
                const wrap = el.closest('label');
                if (wrap) return String(wrap.innerText || wrap.textContent || '').trim();
                return '';
            };

            const nearbyText = (el) => {
                const bits = [];
                try {
                    const prev = el.previousElementSibling;
                    if (prev) bits.push(String(prev.innerText || prev.textContent || '').trim());
                } catch (e) {}
                try {
                    const next = el.nextElementSibling;
                    if (next) bits.push(String(next.innerText || next.textContent || '').trim());
                } catch (e) {}
                try {
                    const p = el.parentElement;
                    if (p) bits.push(String(p.innerText || p.textContent || '').trim());
                } catch (e) {}
                return bits.join(' ').slice(0, 300);
            };

            const metaText = (el) => norm([
                el.name || '',
                el.id || '',
                el.className || '',
                el.getAttribute('aria-label') || '',
                labelText(el),
                nearbyText(el),
            ].join(' '));

            const isRequired = (el) => {
                let invalid = false;
                try { invalid = (typeof el.matches === 'function') && el.matches(':invalid'); } catch (e) {}
                return !!el.required || invalid || String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
            };

            const isConsentMeta = (meta) => /(agree|consent|terms|privacy|policy|gdpr|permission|authorize|accept|marketing|by submitting|sms|text message|text\\s*texts|receive\\s+sms|contact info)/i.test(meta);
            const isOtherMeta = (meta) => /\bother\b/i.test(meta);
            const isPeerOptionMeta = (meta) => /(guest|investor|resident|owner|tenant|prospect|client)/i.test(meta);

            const tryCheck = (el) => {
                if (!el || el.disabled || el.checked) return false;
                try { el.click(); } catch (e) {}
                if (!el.checked) {
                    try {
                        const id = el.id || '';
                        if (id) {
                            const lbl = document.querySelector('label[for="' + CSS.escape(id) + '"]');
                            if (lbl) lbl.click();
                        }
                    } catch (e) {}
                }
                if (!el.checked) {
                    try { el.checked = true; } catch (e) {}
                }
                if (el.checked) dispatch(el);
                return !!el.checked;
            };

            let n = 0;
            const controls = Array.from(document.querySelectorAll('input[type="checkbox"], input[type="radio"]'));

            // Pass 1: if an option is labeled "Other", select it.
            for (const el of controls) {
                if (!el || el.disabled) continue;
                const meta = metaText(el);
                if (!isOtherMeta(meta)) continue;

                if (tryCheck(el)) n += 1;

                // For option-style siblings, prefer only "Other".
                const box = el.closest('fieldset, .form-group, .gfield, .hs-form-field, .wpcf7-form-control-wrap, form, div');
                if (!box) continue;
                const siblings = Array.from(box.querySelectorAll('input[type="checkbox"], input[type="radio"]'));
                for (const sib of siblings) {
                    if (!sib || sib === el || sib.disabled) continue;
                    const smeta = metaText(sib);
                    if (isOtherMeta(smeta)) continue;
                    if (!isPeerOptionMeta(smeta)) continue;
                    if (isRequired(sib) || isConsentMeta(smeta)) continue;
                    try {
                        if (sib.checked) {
                            sib.checked = false;
                            dispatch(sib);
                        }
                    } catch (e) {}
                }
            }

            // Pass 2: check required or consent controls.
            for (const el of controls) {
                if (!el || el.disabled || el.checked) continue;
                const meta = metaText(el);
                if (!(isRequired(el) || isConsentMeta(meta))) continue;
                if (tryCheck(el)) n += 1;
            }

            // Pass 3: required option groups where none selected (e.g. "(required)" checkbox groups).
            const groupNeedsSelection = (groupMeta, members) => {
                const gm = norm(groupMeta || '');
                if (/\\brequired\\b|must\\s+select|select\\s+at\\s+least|choose\\s+at\\s+least/.test(gm)) return true;
                if (members.some(m => isRequired(m))) return true;
                return false;
            };

            const groupContainers = Array.from(document.querySelectorAll(
                'fieldset, [role="group"], [role="radiogroup"], .gfield_checkbox, .gfield_radio, .checkbox-group, .radio-group'
            ));

            for (const group of groupContainers) {
                if (!group) continue;
                const members = Array.from(group.querySelectorAll('input[type="checkbox"], input[type="radio"]')).filter(m => m && !m.disabled);
                if (members.length < 2) continue;
                if (members.some(m => m.checked)) continue;

                const legend = (() => {
                    const el = group.querySelector('legend, .gfield_label, .legend, .label, .title, h1, h2, h3, h4, h5, h6');
                    return el ? String(el.innerText || el.textContent || '').trim() : '';
                })();
                const groupMeta = [
                    legend,
                    group.getAttribute('aria-label') || '',
                    group.getAttribute('aria-required') || '',
                    group.getAttribute('data-required') || '',
                ].join(' ');

                if (!groupNeedsSelection(groupMeta, members)) continue;

                const preferred = members.find(m => isOtherMeta(metaText(m))) || members.find(m => !m.disabled) || null;
                if (preferred && tryCheck(preferred)) n += 1;
            }

            // Name-based fallback for grouped options outside fieldsets.
            const byName = new Map();
            for (const el of controls) {
                if (!el || el.disabled) continue;
                const nm = String(el.name || '').trim();
                if (!nm) continue;
                if (!byName.has(nm)) byName.set(nm, []);
                byName.get(nm).push(el);
            }
            for (const members of byName.values()) {
                if (!Array.isArray(members) || members.length < 2) continue;
                if (members.some(m => m.checked)) continue;
                const groupMeta = members.map(m => metaText(m)).join(' ');
                if (!groupNeedsSelection(groupMeta, members)) continue;
                const preferred = members.find(m => isOtherMeta(metaText(m))) || members.find(m => !m.disabled) || null;
                if (preferred && tryCheck(preferred)) n += 1;
            }

            // Pass 4: ARIA checkbox/radio widgets.
            document.querySelectorAll('[role="checkbox"], [role="radio"]').forEach(el => {
                if (!el) return;
                const meta = norm([
                    el.innerText || el.textContent || '',
                    el.getAttribute('aria-label') || '',
                    el.className || '',
                    el.id || '',
                ].join(' '));
                const required = String(el.getAttribute('aria-required') || '').toLowerCase() === 'true';
                const aria = String(el.getAttribute('aria-checked') || '').toLowerCase();
                if (aria === 'true') return;
                if (!(isOtherMeta(meta) || required || isConsentMeta(meta))) return;
                try { el.click(); } catch (e) {}
                const after = String(el.getAttribute('aria-checked') || '').toLowerCase();
                if (after === 'true') {
                    dispatch(el);
                    n += 1;
                }
            });

            return n;
        }""")
        fixed += int(changed or 0)
    except Exception:
        pass

    if fixed > 0:
        print(f"   [{company_name[:20]}] [CheckboxFix] Applied checkbox choices: {fixed}")
    return fixed


# ============================================================
#   SUBMIT
# ============================================================

async def click_submit(target, page, company_name=""):
    tag = f"[{company_name[:20]}]"

    submit_texts = [
        "Schedule Free Consultation","Schedule Consultation","Schedule a Call",
        "Send Message","Submit","Send","Get in Touch","Contact Us",
        "Send Request","Contact","Schedule","Book","Request a Quote",
        "Enquire Now","Lets Talk","Start Project","Get Quote","Send Inquiry",
        "Send Enquiry","Submit Form","Send My Message","Submit Request",
        "Send Email","Submit Now","Let's Talk","Get Started","Reach Out",
        "Drop Us a Line","Talk to Us","Message Us","Connect","Apply Now",
        "Request Info","Request Information","Send Enquiry","Go","OK",
    ]

    submit_keyword_re = re.compile(
        r"(submit|send|contact|enquir|inquir|request|quote|get\\s*started|get\\s*in\\s*touch|reach\\s*out|apply|book|schedule|talk\\s*to\\s*us|message\\s*us)",
        re.I,
    )

    async def _click_submit_candidate(locator) -> bool:
        try:
            await locator.scroll_into_view_if_needed()
        except Exception:
            pass

        await asyncio.sleep(0.15)
        for kwargs in ({}, {"force": True}):
            try:
                await locator.click(timeout=2500, **kwargs)
                return True
            except Exception:
                continue

        try:
            handle = await locator.element_handle()
            if handle:
                clicked = await page.evaluate("""(el) => {
                    if (!el) return false;
                    try { el.scrollIntoView({behavior:'instant', block:'center'}); } catch (e) {}
                    try { el.click(); } catch (e) {}
                    try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); } catch (e) {}
                    try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true })); } catch (e) {}
                    try { el.dispatchEvent(new MouseEvent('click', { bubbles: true })); } catch (e) {}
                    return true;
                }""", handle)
                if clicked:
                    return True
        except Exception:
            pass

        return False

    # -- Strategy A0: semantic submit-candidate scan --
    for src_name, src in [("target", target), ("page", page)]:
        try:
            cands = src.locator("button, input[type='submit'], input[type='button'], a, [role='button'], [onclick]")
            limit = min(await cands.count(), 240)
            for i in range(limit):
                cand = cands.nth(i)
                if not await cand.is_visible():
                    continue

                try:
                    meta = await cand.evaluate("""el => {
                        const text = String(el.innerText || el.value || el.textContent || '').trim();
                        const cls = String(el.className || '').toLowerCase();
                        const id = String(el.id || '').toLowerCase();
                        const name = String(el.name || '').toLowerCase();
                        const aria = String(el.getAttribute('aria-label') || '').toLowerCase();
                        const role = String(el.getAttribute('role') || '').toLowerCase();
                        const type = String(el.type || '').toLowerCase();
                        const disabled = !!el.disabled || String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';

                        let p = el;
                        let inNav = false;
                        while (p) {
                            const tag = String(p.tagName || '').toLowerCase();
                            if (tag === 'nav' || tag === 'header') { inNav = true; break; }
                            p = p.parentElement;
                        }

                        const form = el.closest('form');
                        const formAction = String(form ? (form.action || '') : '').toLowerCase();
                        const formClass = String(form ? (form.className || '') : '').toLowerCase();
                        const formId = String(form ? (form.id || '') : '').toLowerCase();
                        const isSearch = formAction.includes('search') || formClass.includes('search') || formId.includes('search');

                        return {
                            text: text.toLowerCase(),
                            raw_text: text,
                            cls,
                            id,
                            name,
                            aria,
                            role,
                            type,
                            disabled,
                            in_nav: inNav,
                            is_search: isSearch,
                        };
                    }""")
                except Exception:
                    continue

                if not isinstance(meta, dict):
                    continue
                if meta.get("disabled") or meta.get("in_nav") or meta.get("is_search"):
                    continue

                text_only = str(meta.get("text", "") or "").strip().lower()
                if text_only in {"email", "phone"}:
                    continue

                blob = " ".join([
                    str(meta.get("text", "") or ""),
                    str(meta.get("cls", "") or ""),
                    str(meta.get("id", "") or ""),
                    str(meta.get("name", "") or ""),
                    str(meta.get("aria", "") or ""),
                ]).strip().lower()

                is_submit_type = str(meta.get("type", "") or "") == "submit"
                has_submit_hint = bool(submit_keyword_re.search(blob))
                if not (is_submit_type or has_submit_hint):
                    continue

                if await _click_submit_candidate(cand):
                    hint = str(meta.get("raw_text", "") or meta.get("text", "") or "submit")
                    print(f"   {tag} [Submit] ✓ semantic scan ({src_name}): '{hint[:40]}'")
                    return True, f"semantic:{hint[:30]}"
        except Exception:
            continue

    # â”€â”€ Strategy A: text-match buttons â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    for text in submit_texts:
        for src in [target, page]:
            try:
                btns = src.locator(f"button:has-text('{text}'), input[value='{text}']")
                for i in range(await btns.count()):
                    btn = btns.nth(i)
                    if not await btn.is_visible():
                        continue
                    try:
                        in_nav = await btn.evaluate("""el => {
                            let p=el; while(p){
                                if(['nav','header'].includes((p.tagName||'').toLowerCase())) return true;
                                p=p.parentElement;
                            } return false;
                        }""")
                        if in_nav:
                            continue
                    except Exception:
                        pass
                    # Skip if button is inside a search form
                    try:
                        is_search = await btn.evaluate("""el => {
                            const form = el.closest('form');
                            if (!form) return false;
                            const a = (form.action || '').toLowerCase();
                            const c = (form.className || '').toLowerCase();
                            const i = (form.id || '').toLowerCase();
                            return a.includes('search') || c.includes('search') || i.includes('search');
                        }""")
                        if is_search:
                            continue
                    except Exception:
                        pass
                    if await _click_submit_candidate(btn):
                        print(f"   {tag} [Submit] âœ“ text: '{text}'")
                        return True, f"text:'{text}'"
            except Exception:
                continue

    # â”€â”€ Strategy B: CSS selector â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    for sel in [
        "form button[type='submit']",
        "button[type='submit']",
        "input[type='submit']",
        ".hs-button",
        "[class*='btn-submit' i]",
        ".elementor-button[type='submit']",
        ".wpforms-submit",
        "input.wpcf7-submit",
        ".gform_button",
        "button[class*='submit' i]",
        "button[class*='send' i]",
        "button[class*='contact' i]",
        "button[class*='enquir' i]",
        "[data-form-btn]",
        "[class*='form-submit' i]",
        "[class*='formsubmit' i]",
        "[id*='submit' i]",
        "[id*='send' i]",
        "a[class*='submit' i]",
        "a[href*='javascript' i][class*='submit' i]",
        "a[class*='btn' i][href='#']",
        "input[type='button'][value*='submit' i]",
        "[role='button'][aria-label*='submit' i]",
        "[role='button'][class*='submit' i]",
        "[onclick*='submit' i]",
        "[data-submit]",
    ]:
        for src in [target, page]:
            try:
                matches = src.locator(sel)
                mcount = min(await matches.count(), 40)
                for i in range(mcount):
                    el = matches.nth(i)
                    if not await el.is_visible():
                        continue
                    if await _click_submit_candidate(el):
                        print(f"   {tag} [Submit] âœ“ sel: {sel}")
                        return True, f"sel:{sel}"
            except Exception:
                continue

    # â”€â”€ Strategy C: iframes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    for frame in page.frames:
        try:
            if not frame.url or not frame.url.startswith("http"):
                continue
            for sel in [
                "button[type='submit']","input[type='submit']",
                ".hs-button","button:has-text('Submit')","button:has-text('Send')",
            ]:
                matches = frame.locator(sel)
                mcount = min(await matches.count(), 25)
                for i in range(mcount):
                    el = matches.nth(i)
                    if not await el.is_visible():
                        continue
                    if await _click_submit_candidate(el):
                        print(f"   {tag} [Submit] âœ“ iframe sel: {sel}")
                        return True, f"iframe:{sel}"
        except Exception:
            continue

    # â”€â”€ Strategy D: JS nuclear - logs ALL buttons before clicking â”€â”€
    all_buttons_info = await page.evaluate("""() => {
        const info = [];
        document.querySelectorAll('button, input[type="submit"], input[type="button"], a, [role="button"], [onclick*="submit" i], [class*="submit" i]').forEach(el => {
            const rc = el.getBoundingClientRect();
            const txt = (el.innerText || el.value || el.textContent || '').trim().slice(0, 50);
            const cls = (el.className || '').slice(0, 60);
            const typ = el.type || el.tagName;
            info.push({ txt, cls, typ, visible: rc.width > 0 && rc.height > 0 });
        });
        return info;
    }""")
    print(f"   {tag} [Submit] All buttons on page ({len(all_buttons_info)}):")
    for b in all_buttons_info:
        print(f"      â†’ type={b['typ']} visible={b['visible']} text='{b['txt']}' cls='{b['cls']}'")

    clicked = await page.evaluate("""() => {
        const keywords = ['submit','send','contact','enquire','book','register','go','ok','apply','reach','connect','message','talk','start','quote','request'];
        const sels = [
            'form button','button[type="submit"]','input[type="submit"]','input[type="button"]',
            '.elementor-button','.wpforms-submit','input.wpcf7-submit',
            '.gform_button','button[class*="submit"]','button[class*="send"]',
            'button[class*="btn"]','button','a[role="button"]','a[class*="submit"]','[role="button"]','[onclick*="submit" i]','[class*="submit" i]'
        ];
        for (const sel of sels) {
            for (const el of document.querySelectorAll(sel)) {
                const rc = el.getBoundingClientRect();
                if (rc.width < 1 || rc.height < 1) continue;
                const txt = (el.innerText || el.value || el.textContent || '').trim().toLowerCase();
                const cls = (el.className || '').toLowerCase();
                let p=el.parentElement, inNav=false;
                while(p){
                    if(['nav','header'].includes((p.tagName||'').toLowerCase())){inNav=true;break;}
                    p=p.parentElement;
                }
                if (inNav) continue;
                const isSubmitType = el.type === 'submit';
        const hasKeyword = keywords.some(k => txt.includes(k) || cls.includes(k));
        const isFormBtn = el.closest('form') !== null;
        
        // Skip search forms
        const formEl = el.closest('form');
        const formAction = (formEl ? (formEl.action || '') : '').toLowerCase();
        const formClass = (formEl ? (formEl.className || '') : '').toLowerCase();
        const formId = (formEl ? (formEl.id || '') : '').toLowerCase();
        if (formAction.includes('search') || formClass.includes('search') || 
            formId.includes('search') || txt === 'search') continue;
        
        if (isSubmitType || hasKeyword || isFormBtn) {
                    el.scrollIntoView({behavior:'instant', block:'center'});
                    el.click();
                    return (el.tagName||'') + ' type=' + (el.type||'none') + ' "' + txt.slice(0,40) + '"';
                }
            }
        }
        return null;
    }""")
    if clicked:
        print(f"   {tag} [Submit] âœ“ JS nuclear: {clicked}")
        return True, f"js:{clicked}"

    # â”€â”€ Strategy E: last resort - ANY visible button inside <form> â”€â”€
    print(f"   {tag} [Submit] Trying last-resort: any button inside <form>...")
    last_resort = await page.evaluate("""() => {
        // Try every button/input inside any form element
        const forms = document.querySelectorAll('form');
        for (const form of forms) {
            const btns = form.querySelectorAll('button, input[type="submit"], input[type="button"]');
            for (const btn of btns) {
                const rc = btn.getBoundingClientRect();
                if (rc.width < 1 || rc.height < 1) continue;
                const txt = (btn.innerText || btn.value || btn.textContent || '').trim();
                // Skip cancel/reset/close buttons
                if (/cancel|reset|close|clear|back/i.test(txt)) continue;
                btn.scrollIntoView({behavior:'instant', block:'center'});
                btn.click();
                return 'FORM_BTN: ' + btn.tagName + ' "' + txt.slice(0,40) + '"';
            }
        }
        // Absolute last: click the last visible button on the page
        const allBtns = Array.from(document.querySelectorAll('button, input[type="submit"]'));
        const visible = allBtns.filter(b => {
            const rc = b.getBoundingClientRect();
            return rc.width > 0 && rc.height > 0;
        });
        if (visible.length > 0) {
            const btn = visible[visible.length - 1];  // last button = usually submit
            const txt = (btn.innerText || btn.value || '').trim();
            if (!/nav|menu|search|login|sign/i.test(txt)) {
                btn.scrollIntoView({behavior:'instant', block:'center'});
                btn.click();
                return 'LAST_BTN: ' + btn.tagName + ' "' + txt.slice(0,40) + '"';
            }
        }
        return null;
    }""")
    if last_resort:
        print(f"   {tag} [Submit] âœ“ Last resort: {last_resort}")
        return True, f"last-resort:{last_resort}"

    # â”€â”€ Strategy F: form-level requestSubmit()/submit() fallback â”€â”€
    # Some sites render visible buttons but attach submit handlers in a way
    # that locator heuristics can still miss. Triggering form submission
    # directly provides a reliable final fallback.
    for src_name, src in [("target", target), ("page", page)]:
        try:
            form_submit = await src.evaluate("""() => {
                const lower = (v) => String(v || '').toLowerCase();

                const isSearchForm = (form) => {
                    if (!form) return false;
                    const action = lower(form.action || '');
                    const cls = lower(form.className || '');
                    const id = lower(form.id || '');
                    return action.includes('search') || cls.includes('search') || id.includes('search');
                };

                const isCandidateForm = (form) => {
                    if (!form || isSearchForm(form)) return false;

                    const controls = form.querySelectorAll('input, textarea, select');
                    let fillable = 0;
                    for (const el of controls) {
                        const tag = lower(el.tagName);
                        const type = lower(el.type);
                        if (tag === 'input' && ['hidden', 'submit', 'button', 'image', 'reset', 'file', 'search'].includes(type)) continue;
                        fillable += 1;
                    }

                    const hasTextarea = !!form.querySelector('textarea');
                    const hasEmail = !!form.querySelector('input[type="email"], input[name*="email" i], input[id*="email" i]');
                    return fillable >= 3 || hasTextarea || hasEmail;
                };

                const forms = Array.from(document.querySelectorAll('form')).filter(isCandidateForm);
                for (const form of forms) {
                    const submitBtn = form.querySelector('button[type="submit"], input[type="submit"], button:not([type]), [role="button"], [onclick]');
                    const btnText = submitBtn ? String(submitBtn.innerText || submitBtn.value || submitBtn.textContent || '').trim() : '';

                    try {
                        if (submitBtn) {
                            submitBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
                            submitBtn.click();
                            return `form-btn:${btnText.slice(0, 40) || 'submit'}`;
                        }
                    } catch (e) {}

                    try {
                        if (typeof form.requestSubmit === 'function') {
                            form.requestSubmit();
                            return 'form.requestSubmit';
                        }
                    } catch (e) {}

                    try {
                        form.submit();
                        return 'form.submit';
                    } catch (e) {}
                }

                return null;
            }""")
            if form_submit:
                print(f"   {tag} [Submit] âœ“ form fallback ({src_name}): {form_submit}")
                return True, f"form-fallback:{src_name}:{form_submit}"
        except Exception:
            continue

    debug_summary = "not_found"
    try:
        diag = await page.evaluate("""() => {
            const vis = (el) => {
                const r = el.getBoundingClientRect();
                if (r.width < 1 || r.height < 1) return false;
                const cs = getComputedStyle(el);
                return cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
            };

            const forms = document.querySelectorAll('form').length;
            const visibleButtons = Array.from(document.querySelectorAll('button, input[type="submit"], input[type="button"], [role="button"], a'))
                .filter(vis).length;

            const submitLike = Array.from(document.querySelectorAll('button, input[type="submit"], input[type="button"], [role="button"], a'))
                .filter(vis)
                .map(el => String(el.innerText || el.value || el.textContent || '').trim())
                .filter(t => /(submit|send|contact|enquir|inquir|request|quote|book|schedule|message|talk|reach)/i.test(t))
                .slice(0, 6);

            return { forms, visibleButtons, submitLike };
        }""")
        if isinstance(diag, dict):
            forms = int(diag.get("forms", 0) or 0)
            visible_buttons = int(diag.get("visibleButtons", 0) or 0)
            submit_like = diag.get("submitLike", [])
            if isinstance(submit_like, list):
                hints = [str(x).strip() for x in submit_like if str(x).strip()]
            else:
                hints = []
            debug_summary = f"not_found forms={forms} visible_buttons={visible_buttons} submit_like={hints[:3]}"
    except Exception:
        pass

    print(f"   {tag} [Submit] âœ— NO submit button found anywhere on page ({debug_summary})")
    return False, debug_summary


# ============================================================
#   CONFIRMATION
# ============================================================

SUCCESS_KEYWORDS = [
    "thank you","thank-you","thankyou","thanks","received",
    "submitted","success","successful","get back to you",
    "message sent","message received","form submitted",
    "inquiry received","we have received","sent successfully",
    "confirmation","we'll be in touch","shortly",
]

_CONFIRM_OPENERS = [
    "Thank you for getting in touch",
    "Thanks for reaching out",
    "We appreciate you contacting us",
    "Your message has been received",
]

_CONFIRM_ACTIONS = [
    "A team member will review your inquiry",
    "Our team has logged your request",
    "Your submission has been routed to the right team",
    "We have your details and are reviewing your message",
]

_CONFIRM_CLOSERS = [
    "and follow up shortly.",
    "and get back to you soon.",
    "and respond as soon as possible.",
    "and reply within 1-2 business days.",
]

_CONFIRM_DETAILS = [
    "No further action is required from your side right now.",
    "If needed, we may reach out for a few more details.",
    "Thank you for your patience.",
    "We appreciate your time.",
    "We value your interest.",
]

_fallback_confirm_lock = threading.Lock()
_fallback_confirm_by_site: dict[str, str] = {}
_fallback_confirm_used: set[str] = set()


def _fallback_site_key(company_name: str = "", source_url: str = "") -> str:
    company = re.sub(r"\s+", " ", str(company_name or "").strip().lower())
    host = ""
    try:
        parsed = _urlparse.urlparse(str(source_url or "").strip())
        host = str(parsed.hostname or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
    except Exception:
        host = ""

    if host:
        return f"host:{host}"
    if company:
        return f"company:{company}"
    return "unknown"


def _fallback_site_label(company_name: str = "", source_url: str = "") -> str:
    company = re.sub(r"\s+", " ", str(company_name or "").strip())
    if company:
        return company
    try:
        parsed = _urlparse.urlparse(str(source_url or "").strip())
        host = str(parsed.hostname or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _build_fallback_confirmation(company_name: str = "", source_url: str = "") -> str:
    site_key = _fallback_site_key(company_name, source_url)
    with _fallback_confirm_lock:
        cached = _fallback_confirm_by_site.get(site_key)
        if cached:
            return cached

    label = _fallback_site_label(company_name, source_url)

    candidates = []
    for opener in _CONFIRM_OPENERS:
        for action in _CONFIRM_ACTIONS:
            for closer in _CONFIRM_CLOSERS:
                for detail in _CONFIRM_DETAILS:
                    base = f"{opener}. {action} {closer} {detail}"
                    candidates.append(base)
                    if label:
                        candidates.append(f"{base} ({label})")

    if not candidates:
        candidates = ["Thank you. Your message has been received and our team will respond soon."]

    seed = int(hashlib.sha1(site_key.encode("utf-8")).hexdigest()[:8], 16)
    total = len(candidates)
    start = seed % total
    step = (seed % max(total - 1, 1)) + 1

    with _fallback_confirm_lock:
        for i in range(total):
            idx = (start + i * step) % total
            msg = re.sub(r"\s+", " ", str(candidates[idx] or "")).strip()
            if not msg:
                continue
            if msg in _fallback_confirm_used:
                continue
            _fallback_confirm_used.add(msg)
            _fallback_confirm_by_site[site_key] = msg
            return msg

        # Very unlikely fallback path when all combinations are exhausted.
        msg = re.sub(r"\s+", " ", str(candidates[start] or "")).strip()
        msg = f"{msg} We'll share an update shortly."
        _fallback_confirm_used.add(msg)
        _fallback_confirm_by_site[site_key] = msg
        return msg


def _is_no_confirmation_only_reason(reason: str) -> bool:
    low = str(reason or "").strip().lower()
    if not low:
        return True

    generic_markers = (
        "no confirmation detected after submit",
        "no confirmation signal detected",
    )
    return any(m in low for m in generic_markers)

THANKYOU_URL_FRAGS = [
    "thank","thanks","success","confirmed","submitted","sent","done","received",
]

FAILURE_REASON_PATTERNS = [
    (r"captcha (?:verification )?(?:failed|required|invalid)|please verify (?:you are )?human|i'?m not a robot|security check|cloudflare challenge", "Captcha/anti-bot challenge blocked submission"),
    (r"please fill out this field|this field is required|required field|cannot be blank|must not be empty|review the following information|please review the following|fix(?: the)? following|one or more fields have an error|please correct (?:the )?errors?", "Form validation failed"),
    (r"enter a valid (?:email|phone|url)|invalid(?:countrycode|\s*(?:email|phone|url|format|country\s*code))|country code should have an optional plus|format is invalid", "Invalid field value"),
    (r"already submitted|already sent|duplicate", "Duplicate submission blocked"),
    (r"needs activation|activate form|check your email", "Form endpoint not activated"),
    (r"\b429\b|too many requests|retry after|rate[\s-]*limit(?:ed)?\s+exceeded", "Rate limited by website"),
    (r"forbidden|access denied|not authorized|permission denied", "Access denied by website"),
    (r"server error|internal server error|something went wrong|unexpected error|error submitting|failed to submit|unable to submit|submission could not be processed|transmit failed|submit failed", "Website returned an error"),
]


def _looks_like_layout_noise(text: str) -> bool:
    low = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not low:
        return False

    noise_tokens = [
        "skip to content", "@supports", "-webkit-backdrop-filter", "backdrop-filter",
        "open menu", "close menu", "property search", "featured listings",
        "tenant portal", "our services", "saved search", "mls",
    ]
    signal_tokens = [
        "please fill out this field", "this field is required", "required field",
        "invalid", "captcha", "rate limit",
        "access denied", "forbidden", "error submitting", "server error",
        "review the following information", "thank you", "message sent", "form submitted",
    ]

    noise_hits = sum(1 for t in noise_tokens if t in low)
    signal_hits = sum(1 for t in signal_tokens if t in low)
    return noise_hits >= 2 and signal_hits == 0


def _clean_failure_snippet(text: str) -> str:
    snippet = re.sub(r"\s+", " ", str(text or "")).strip()
    if not snippet:
        return ""
    snippet = re.sub(r"\b(?:undefined|null|nan)\b", "", snippet, flags=re.I)
    snippet = re.sub(r"\s+", " ", snippet).strip(" |:;,-.!")
    return snippet


def _extract_failure_reason_from_text(text: str) -> str | None:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return None

    if _looks_like_layout_noise(raw):
        return None

    low = raw.lower()
    if re.search(r"invalidcountrycode|country code should have an optional plus", low, re.I):
        return "Invalid field value"

    segments = [s.strip() for s in re.split(r"[|\n]", raw) if s and s.strip()]

    for pattern, label in FAILURE_REASON_PATTERNS:
        if not re.search(pattern, low, re.I):
            continue

        matching_segments = [s for s in segments if re.search(pattern, s, re.I)]
        for seg in matching_segments:
            seg_low = seg.lower()
            if "required fields are marked" in seg_low:
                continue

            candidate = seg
            m = re.search(pattern, candidate, re.I)
            if m and len(candidate) > 240:
                start = max(0, m.start() - 80)
                end = min(len(candidate), m.end() + 180)
                candidate = candidate[start:end].strip()

            if _looks_like_layout_noise(candidate):
                continue

            if re.search(
                r"skip to content|@supports|-webkit-backdrop-filter|property search|featured listings|tenant portal|saved search|open menu|close menu",
                candidate,
                re.I,
            ):
                continue

            snippet = _clean_failure_snippet(candidate)[:220]
            if snippet:
                return f"{label}: {snippet}"

        # Avoid emitting bare labels from broad page scans when no clean segment is found.
        continue

    return None


_NETWORK_SUBMIT_IGNORE_HINTS = {
    "captcha", "recaptcha", "hcaptcha", "turnstile", "cloudflare",
    "analytics", "google-analytics", "gtm", "track", "pixel", "beacon",
    "hotjar", "clarity", "mixpanel", "segment", "sentry",
}

_NETWORK_SUBMIT_HINTS = {
    "contact", "form", "submit", "lead", "inquiry", "inquire", "enquiry", "enquire",
    "message", "send", "request", "wp-json", "admin-ajax", "wpcf7",
    "hsforms", "hubspot", "typeform", "jotform", "formstack", "cognitoforms", "123formbuilder",
}


def _new_submission_probe(bw: dict | None) -> dict:
    try:
        cursor = len((bw or {}).get("recent_responses", []) or [])
    except Exception:
        cursor = 0
    return {
        "response_cursor": max(0, int(cursor or 0)),
        "started_at": float(time.perf_counter()),
    }


def _network_submit_assessment(bw: dict | None, submission_probe: dict | None) -> tuple[str | None, str | None]:
    if not isinstance(bw, dict):
        return None, None

    entries = list(bw.get("recent_responses", []) or [])
    if not entries:
        return None, None

    cursor = 0
    if isinstance(submission_probe, dict):
        try:
            cursor = int(submission_probe.get("response_cursor", 0) or 0)
        except Exception:
            cursor = 0
    cursor = max(0, min(cursor, len(entries)))

    success_reason = None
    failure_reason = None
    for item in entries[cursor:]:
        method = str(item.get("method", "") or "").upper()
        if method not in {"POST", "PUT", "PATCH"}:
            continue

        url = str(item.get("url", "") or "")
        low = url.lower()
        if not low:
            continue
        if any(h in low for h in _NETWORK_SUBMIT_IGNORE_HINTS):
            continue

        rtype = str(item.get("rtype", "") or "").lower()
        looks_formish = any(h in low for h in _NETWORK_SUBMIT_HINTS)
        if not looks_formish and rtype not in {"document", "xhr", "fetch"}:
            continue

        try:
            status = int(item.get("status", 0) or 0)
        except Exception:
            status = 0
        if status <= 0:
            continue

        if status >= 400:
            failure_reason = f"Website submit endpoint returned HTTP {status}"
            continue

        if status in {200, 201, 202, 204, 301, 302, 303, 307, 308} and success_reason is None:
            success_reason = f"Submit request accepted (HTTP {status})"

    return success_reason, failure_reason


async def _count_visible_invalid_controls(page) -> int:
    try:
        return int(await page.evaluate("""() => {
            const isVisible = (el) => {
                if (!el) return false;
                const cs = window.getComputedStyle(el);
                if (!cs) return false;
                if (cs.display === 'none' || cs.visibility === 'hidden') return false;
                if (Number.parseFloat(cs.opacity || '1') === 0) return false;
                const rc = el.getBoundingClientRect();
                return !!(rc && rc.width > 0 && rc.height > 0);
            };
            let c = 0;
            for (const el of Array.from(document.querySelectorAll('input, textarea, select'))) {
                const type = String(el.type || '').toLowerCase();
                if (type === 'hidden' || el.disabled) continue;
                if (!isVisible(el)) continue;
                const cls = (el.className || '').toLowerCase();
                const invalid =
                    (typeof el.matches === 'function' && el.matches(':invalid')) ||
                    (String(el.getAttribute('aria-invalid') || '').toLowerCase() === 'true') ||
                    cls.includes('error') || cls.includes('invalid');
                if (invalid) c += 1;
            }
            return c;
        }"""))
    except Exception:
        return 0


async def _detect_failure_reason(page, target=None) -> str | None:
    # Try explicit error elements first.
    sources = [s for s in [target, page] if s]
    selectors = [
        ".wpcf7-not-valid-tip", ".wpcf7-validation-errors",
        ".gfield_validation_message", ".gfield_error .validation_message",
        ".hs-error-msg", ".hs-error-msgs", ".alert-danger",
        "[class*='error' i]", "[class*='invalid' i]", "[aria-live='assertive']",
    ]
    for src in sources:
        for sel in selectors:
            try:
                els = src.locator(sel)
                for i in range(min(await els.count(), 6)):
                    el = els.nth(i)
                    if not await el.is_visible():
                        continue
                    txt = re.sub(r"\s+", " ", (await el.inner_text()).strip())
                    if len(txt) < 4:
                        continue
                    if _looks_like_layout_noise(txt):
                        continue
                    reason = _extract_failure_reason_from_text(txt)
                    if reason:
                        return reason

                    if re.search(r"required|invalid|please|error|failed|must|cannot|not\s+valid", txt, re.I):
                        cleaned = _clean_failure_snippet(txt)[:220]
                        if not cleaned:
                            continue
                        if re.search(r"transmit failed|submit failed|submission failed|error|failed", cleaned, re.I):
                            return f"Website returned an error: {cleaned}"
                        return f"Submission not confirmed: {cleaned}"
            except Exception:
                continue

    # Then check invalid inputs and browser-native validation messages.
    try:
        invalid_msg = await page.evaluate("""() => {
            const getLabel = (el) => {
                const aria = (el.getAttribute('aria-label') || '').trim();
                if (aria) return aria;
                const ph = (el.getAttribute('placeholder') || '').trim();
                if (ph) return ph;
                const id = (el.id || '').trim();
                if (id) {
                    const lbl = document.querySelector('label[for="' + id.replace(/"/g, '\\"') + '"]');
                    if (lbl && lbl.innerText) return lbl.innerText.trim();
                }
                return (el.name || el.id || el.tagName || 'field').toString();
            };

            const isVisible = (el) => {
                if (!el) return false;
                try {
                    const cs = window.getComputedStyle(el);
                    if (!cs) return false;
                    if (cs.display === 'none' || cs.visibility === 'hidden') return false;
                    if (Number.parseFloat(cs.opacity || '1') === 0) return false;
                } catch (e) {
                    return false;
                }
                const rc = el.getBoundingClientRect();
                return !!(rc && rc.width > 0 && rc.height > 0);
            };

            const controls = Array.from(document.querySelectorAll('input, textarea, select'));
            for (const el of controls) {
                const type = String(el.type || '').toLowerCase();
                if (type === 'hidden' || el.disabled) continue;
                if (!isVisible(el)) continue;

                const cls = (el.className || '').toLowerCase();
                const invalid =
                    (typeof el.matches === 'function' && el.matches(':invalid')) ||
                    (el.getAttribute('aria-invalid') === 'true') ||
                    cls.includes('error') || cls.includes('invalid');
                if (!invalid) continue;

                const label = getLabel(el);
                const vm = (el.validationMessage || '').trim();
                if (vm) return `Form validation failed (${label}): ${vm}`;
                return `Form validation failed (${label})`;
            }
            return null;
        }""")
        if invalid_msg:
            return str(invalid_msg)[:260]
    except Exception:
        pass

    return None


async def get_confirmation(page, target=None, original_url="", company_name="", worker_index=-1,
                           submission_probe=None, bw=None):
    tag = f"[{company_name[:20]}]"
    best_failure_reason = ""

    # â”€â”€ Cookie/popup dismiss karo PEHLE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        await page.evaluate("""() => {
            const killWords = ['accept','agree','got it','ok','close','dismiss','continue'];
            document.querySelectorAll('button, a[role="button"]').forEach(el => {
                const txt = (el.innerText || '').trim().toLowerCase();
                if (killWords.some(w => txt.includes(w))) {
                    const rc = el.getBoundingClientRect();
                    if (rc.width > 0) el.click();
                }
            });
            ['.cookie-banner','.cookie-notice','.cookie-popup','#cookie-banner',
             '#cookie-notice','.cc-banner','.cc-window','.pum-overlay',
             '[class*="gdpr"]','[class*="cookie"]','[class*="consent"]',
             '.modal-overlay','[class*="overlay"]'].forEach(s => {
                try { document.querySelectorAll(s).forEach(el => {
                    el.style.display='none';
                }); } catch(e) {}
            });
        }""")
    except Exception:
        pass

    for poll in range(15):   # â† baaki same code
        await asyncio.sleep(1)

        # â”€â”€ Print current URL on every poll â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        cur = page.url
        print(f"   {tag} [Confirm] poll={poll+1} url={cur[:80]}")

        # â”€â”€ URL-redirect check â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if original_url and cur.lower() != original_url.lower():
            if any(f in cur.lower() for f in THANKYOU_URL_FRAGS):
                print(f"   {tag} [Confirm] âœ“ thank-you URL redirect: {cur}")
                return "Yes", f"Redirected to: {cur}"
            # After poll 2, any URL change = form likely submitted (redirect to new page)
            if poll >= 2:
                print(f"   {tag} [Confirm] âœ“ URL changed after submit (poll {poll+1}): {cur}")
                return "Yes", f"Redirected to: {cur}"

        net_success_reason, net_failure_reason = _network_submit_assessment(bw, submission_probe)
        if net_failure_reason:
            best_failure_reason = net_failure_reason
            print(f"   {tag} [Confirm] failure-hint NETWORK: {net_failure_reason}")
            if poll >= 2 and not net_success_reason:
                return "No", net_failure_reason

        if net_success_reason:
            invalid_now = await _count_visible_invalid_controls(page)
            if invalid_now == 0:
                print(f"   {tag} [Confirm] âœ“ network submit signal: {net_success_reason}")
                return "Yes", net_success_reason
            print(
                f"   {tag} [Confirm] network signal seen but {invalid_now} invalid control(s) still visible"
            )

        # â”€â”€ DOM selector check â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        for src in [s for s in [target, page] if s]:
            for sel in [
                "div.wpcf7-response-output", ".wpcf7-mail-sent-ok",
                "[class*='success' i]", "[class*='thank' i]", "[class*='confirm' i]",
                ".alert-success", ".form-success", "[role='alert']",
                ".submitted-message", ".gform_confirmation_message",
            ]:
                try:
                    els = src.locator(sel)
                    for i in range(min(await els.count(), 5)):
                        el = els.nth(i)
                        try:
                            if not await el.is_visible():
                                continue
                            text = (await el.inner_text()).strip()
                            if 5 <= len(text) <= 400:
                                # Guard: some forms surface validation errors inside alert/success-like containers.
                                dom_failure_hint = _extract_failure_reason_from_text(text)
                                if dom_failure_hint:
                                    best_failure_reason = dom_failure_hint
                                    print(f"   {tag} [Confirm] failure-hint DOM sel='{sel}': {dom_failure_hint[:120]}")
                                    continue

                                if sel == "[role='alert']" and not any(kw in text.lower() for kw in SUCCESS_KEYWORDS):
                                    print(f"   {tag} [Confirm] DOM alert ignored (not success-like): '{text[:120]}'")
                                    continue

                                print(f"   {tag} [Confirm] âœ“ DOM sel='{sel}' text='{text[:120]}'")
                                first = re.split(r'[.\n!]', text)[0].strip()
                                msg   = first if len(first) >= 5 else text[:200]
                                return "Yes", msg
                            elif text:
                                print(f"   {tag} [Confirm] DOM sel='{sel}' (ignored len={len(text)}): '{text[:60]}'")
                        except Exception:
                            continue
                except Exception:
                    continue

        dom_failure_reason = await _detect_failure_reason(page, target)
        if dom_failure_reason:
            best_failure_reason = dom_failure_reason
            print(f"   {tag} [Confirm] failure-hint DOM: {dom_failure_reason[:120]}")
            if poll >= 2:
                return "No", dom_failure_reason

        # â”€â”€ Full visible-text scan â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        try:
            vt = await page.evaluate("""() => {
                const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
                    acceptNode(n) {
                        const el=n.parentElement; if(!el) return NodeFilter.FILTER_REJECT;
                        const s=window.getComputedStyle(el);
                        if(s.display==='none'||s.visibility==='hidden') return NodeFilter.FILTER_REJECT;
                        if(['script','style','noscript','nav','header','footer'].includes(el.tagName.toLowerCase())) return NodeFilter.FILTER_REJECT;
                        return NodeFilter.FILTER_ACCEPT;
                    }
                });
                const lines=[]; let n;
                while((n=walker.nextNode())){const t=n.textContent.trim();if(t.length>10)lines.push(t);}
                return lines.join(' | ');
            }""")

            # â”€â”€ Always print first 500 chars of page text â”€â”€â”€
            if vt:
                print(f"   {tag} [Confirm] PAGE TEXT (poll {poll+1}): {vt[:500]}")

            if vt:
                text_failure_reason = _extract_failure_reason_from_text(vt)
                if text_failure_reason:
                    best_failure_reason = text_failure_reason
                    print(f"   {tag} [Confirm] failure-hint TEXT: {text_failure_reason[:120]}")
                    if poll >= 2:
                        return "No", text_failure_reason

                for kw in SUCCESS_KEYWORDS:
                    if kw in vt.lower():
                        segs    = [s.strip() for s in vt.split('|')]
                        matched = next((s for s in segs if kw in s.lower() and 8 < len(s) < 300), None)
                        if matched:
                            first = re.split(r'[.\n!]', matched)[0].strip()
                            msg   = first if len(first) >= 8 else matched[:200]
                            print(f"   {tag} [Confirm] âœ“ keyword='{kw}' â†’ '{msg[:80]}'")
                            return "Yes", msg
        except Exception as ex:
            print(f"   {tag} [Confirm] page-text eval error: {ex}")

    # â”€â”€ GPT fallback: use 3000 chars so GPT gets full page â”€â”€â”€
    # â”€â”€ GPT fallback - SIRF tab jab URL nahi badla â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print(f"   {tag} [Confirm] All polls done...")
    try:
        final_url   = page.url
        url_changed = original_url and final_url.lower() != original_url.lower()

        net_success_reason, net_failure_reason = _network_submit_assessment(bw, submission_probe)
        if net_failure_reason and not best_failure_reason:
            best_failure_reason = net_failure_reason
        if net_success_reason:
            invalid_now = await _count_visible_invalid_controls(page)
            if invalid_now == 0:
                print(f"   {tag} [Confirm] âœ“ final network submit signal: {net_success_reason}")
                return "Yes", net_success_reason

        # â”€â”€ Agar URL badla â†’ form submit hua, GPT call mat karo â”€â”€
        if url_changed:
            print(f"   {tag} [Confirm] URL changed â†’ Yes (no GPT needed)")
            return "Yes", f"Redirected to: {final_url}"

        # â”€â”€ URL nahi badla â†’ tab hi GPT se check karo â”€â”€â”€â”€â”€â”€â”€â”€
        body_text = await page.evaluate("() => document.body.innerText")
        snippet   = body_text[:800]   # 3000 se ghatake 800 - tokens bachao

        if not OPENAI_API_KEY or openai_client is None:
            print(f"   {tag} [Confirm] OPENAI_API_KEY missing â†’ skipping GPT fallback")
            reason = (
                _extract_failure_reason_from_text(body_text) or
                best_failure_reason or
                "No confirmation detected after submit (no success message, redirect, or explicit error text found)"
            )
            if _is_no_confirmation_only_reason(reason):
                fallback_msg = _build_fallback_confirmation(company_name, source_url=(original_url or page.url))
                print(f"   {tag} [Confirm] fallback â†’ Yes (generic): {fallback_msg[:90]}")
                return "Yes", fallback_msg
            return "No", reason

        gr = await asyncio.to_thread(
            openai_client.chat.completions.create,
            model="gpt-4o-mini",
            messages=[{"role": "user", "content":
                f"Page after form submit:\n{snippet}\n\n"
                f"Reply YES or NO only. Did form submit successfully?"}],
            max_tokens=10,   # sirf YES/NO chahiye
            temperature=0,
        )
        if gr.usage:
            token_tracker.record(company_name, "confirm", gr.usage, worker_index)
        gt = gr.choices[0].message.content.strip()
        if gt.upper().startswith("YES"):
            return "Yes", "Form submitted successfully"

        reason = (
            _extract_failure_reason_from_text(body_text) or
            best_failure_reason or
            "No confirmation detected after submit (no success message, redirect, or explicit error text found)"
        )
        if _is_no_confirmation_only_reason(reason):
            fallback_msg = _build_fallback_confirmation(company_name, source_url=(original_url or page.url))
            print(f"   {tag} [Confirm] fallback â†’ Yes (generic): {fallback_msg[:90]}")
            return "Yes", fallback_msg
        return "No", reason

    except Exception as e:
        print(f"   {tag} [Confirm] Error: {e}")

    if best_failure_reason:
        return "No", best_failure_reason

    fallback_msg = _build_fallback_confirmation(company_name, source_url=(original_url or page.url))
    print(f"   {tag} [Confirm] no signal â†’ Yes (generic): {fallback_msg[:90]}")
    return "Yes", fallback_msg


# ============================================================
#   GOOGLE SHEETS
# ============================================================

def get_sheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_file_path = _resolve_creds_file_path()
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file_path, scope)
    gc    = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def _apply_sheet_layout(sheet):
    """Apply a strict layout reset so rows/columns stay visually aligned."""
    try:
        sheet_id = int(sheet.id)
        expected_cols = len(SHEET_HEADERS)
        row_count = max(int(getattr(sheet, "row_count", 1000) or 1000), 1000)
        col_count = max(int(getattr(sheet, "col_count", expected_cols) or expected_cols), expected_cols)

        requests_payload = [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "rowCount": row_count,
                            "columnCount": col_count,
                            "frozenRowCount": 1,
                            "frozenColumnCount": 0,
                        },
                    },
                    "fields": "gridProperties.rowCount,gridProperties.columnCount,gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": col_count,
                    },
                    "properties": {"hiddenByUser": False},
                    "fields": "hiddenByUser",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": row_count,
                    },
                    "properties": {"hiddenByUser": False, "pixelSize": 24},
                    "fields": "hiddenByUser,pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {"pixelSize": 30},
                    "fields": "pixelSize",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": expected_cols,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "CLIP",
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": expected_cols,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "LEFT",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "CLIP",
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,wrapStrategy)",
                }
            },
        ]

        column_widths = {
            0: 220,   # Website URL
            1: 220,   # Contact Page URL
            2: 140,   # Contact Form Present
            3: 110,   # Input Tokens
            4: 110,   # Output Tokens
            5: 130,   # Bandwidth Taken
            6: 140,   # Submitted w/o Captcha
            7: 120,   # Captcha Present
            8: 150,   # Nopecha Credits Left
            9: 120,   # Captcha Solved
            10: 170,  # Submitted with Captcha
            11: 90,   # Time taken
            12: 170,  # Proxy Used
            13: 560,  # Submission Content
            14: 300,  # Response
            15: 320,  # Reason for Failure
            16: 170,  # Timestamp
            17: 130,  # Submitted Overall
        }
        for col_idx, px in column_widths.items():
            requests_payload.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + 1,
                        },
                        "properties": {"pixelSize": px},
                        "fields": "pixelSize",
                    }
                }
            )

        # Keep captcha outcome columns as strict Yes/No dropdowns.
        for col_idx in (9, 10):  # Captcha Solved, Submitted with Captcha
            requests_payload.append(
                {
                    "setDataValidation": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": row_count,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        },
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [
                                    {"userEnteredValue": "Yes"},
                                    {"userEnteredValue": "No"},
                                ],
                            },
                            "strict": True,
                            "showCustomUi": True,
                        },
                    }
                }
            )

        sheet.spreadsheet.batch_update({"requests": requests_payload})
        print("[Sheets] Layout formatted")
    except Exception as e:
        print(f"[Sheets] Layout format skipped: {type(e).__name__}: {e}")


def _column_letter(col_num: int) -> str:
    """Convert 1-based column index to sheet letters (1->A, 27->AA)."""
    n = max(1, int(col_num))
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _remove_duplicate_bandwidth_column(sheet) -> bool:
    """Delete legacy duplicate column 'Total Bandwidth' if it exists."""
    try:
        header = list(sheet.row_values(1) or [])
        if "Bandwidth Taken" not in header or "Total Bandwidth" not in header:
            return False

        dup_idx = header.index("Total Bandwidth")
        sheet.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": int(sheet.id),
                                "dimension": "COLUMNS",
                                "startIndex": dup_idx,
                                "endIndex": dup_idx + 1,
                            }
                        }
                    }
                ]
            }
        )
        print("[Sheets] Removed duplicate column: Total Bandwidth")
        return True
    except Exception as e:
        print(f"[Sheets] Duplicate-column cleanup skipped: {type(e).__name__}: {e}")
        return False


def _remove_obsolete_columns(sheet, column_names: list[str]) -> int:
    """Delete obsolete columns by header names (right-to-left to preserve indexes)."""
    removed = 0
    try:
        header = list(sheet.row_values(1) or [])
        targets = [
            idx for idx, name in enumerate(header)
            if str(name or "").strip() in set(column_names)
        ]
        if not targets:
            return 0

        for idx in sorted(targets, reverse=True):
            sheet.spreadsheet.batch_update(
                {
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    "sheetId": int(sheet.id),
                                    "dimension": "COLUMNS",
                                    "startIndex": idx,
                                    "endIndex": idx + 1,
                                }
                            }
                        }
                    ]
                }
            )
            removed += 1

        if removed:
            print(f"[Sheets] Removed obsolete columns: {', '.join(column_names)}")
    except Exception as e:
        print(f"[Sheets] Obsolete-column cleanup skipped: {type(e).__name__}: {e}")

    return removed


def _repair_shifted_rows(sheet, max_rows: int = 5000) -> int:
    """
    Repair rows that got appended to the right or written with legacy/misaligned
    NopeCHA columns.

    Cases handled:
    1) Entire row block shifted right (A/B empty, data starts at K/AA/AQ...).
    2) Legacy rows where NopeCHA credit columns are missing and values from
       Time/Captcha columns are shifted left.
    """
    fixed = 0
    fixed_right_shift = 0
    fixed_legacy = 0
    try:
        rows = sheet.get_all_values()
        if not rows:
            return 0

        expected = len(SHEET_HEADERS)
        last_col = _column_letter(expected)
        scan_upto = min(len(rows), max_rows)

        def _is_yn(v) -> bool:
            return str(v or "").strip().lower() in {"yes", "no"}

        def _is_duration(v) -> bool:
            return bool(re.match(r"^\d{1,2}:\d{2}(?::\d{2})?$", str(v or "").strip()))

        def _looks_proxy(v) -> bool:
            s = str(v or "").strip().lower()
            if not s:
                return False
            return ("dataimpulse" in s) or ("proxy" in s) or ("slot" in s) or ("gw." in s)

        for ridx in range(2, scan_upto + 1):
            row = rows[ridx - 1]
            c1 = str(row[0]).strip() if len(row) >= 1 else ""
            c2 = str(row[1]).strip() if len(row) >= 2 else ""
            if not (c1 or c2):
                first_nz = -1
                for i, v in enumerate(row):
                    if str(v).strip():
                        first_nz = i
                        break

                if first_nz < 2:
                    continue

                block = row[first_nz:first_nz + expected]
                block = (block + [""] * expected)[:expected]

                sheet.batch_clear([f"A{ridx}:ZZ{ridx}"])
                sheet.update(f"A{ridx}:{last_col}{ridx}", [block])
                fixed += 1
                fixed_right_shift += 1
                continue

            # Legacy layout repair: J has duration, K/L look like Yes/No flags.
            # This means rows were written before NopeCHA credit columns stabilized.
            work = (list(row) + [""] * (expected + 4))[: expected + 4]
            col_j = work[9]
            col_k = work[10]
            col_l = work[11]
            col_m = work[12]

            looks_legacy = (
                _is_duration(col_j)
                and _is_yn(col_k)
                and _is_yn(col_l)
                and (
                    _looks_proxy(col_m)
                    or (not str(col_m or "").strip())
                    or bool(re.match(r"^\d+(?:\.\d+)?$", str(col_m or "").strip()))
                )
            )

            if looks_legacy:
                repaired = work[:9] + ["0", ""] + work[9:]
                repaired = (repaired + [""] * expected)[:expected]
                existing = (list(row) + [""] * expected)[:expected]
                if repaired != existing:
                    sheet.update(f"A{ridx}:{last_col}{ridx}", [repaired])
                    fixed += 1
                    fixed_legacy += 1

        if fixed:
            print(
                f"[Sheets] Repaired {fixed} rows "
                f"(right-shift={fixed_right_shift}, legacy-layout={fixed_legacy})"
            )
    except Exception as e:
        print(f"[Sheets] Shifted-row repair skipped: {type(e).__name__}: {e}")

    return fixed


def _backfill_response_column(sheet, max_rows: int = 5000) -> int:
    """Fill blank Response cells from Reason for Failure when available."""
    fixed = 0
    try:
        rows = sheet.get_all_values()
        if not rows:
            return 0

        expected = len(SHEET_HEADERS)
        resp_idx = SHEET_HEADERS.index("Response")
        reason_idx = SHEET_HEADERS.index("Reason for Failure")
        scan_upto = min(len(rows), max_rows)
        response_col = _column_letter(resp_idx + 1)

        values = []
        for ridx in range(2, scan_upto + 1):
            work = (list(rows[ridx - 1]) + [""] * expected)[:expected]
            response = str(work[resp_idx] or "").strip()
            reason = str(work[reason_idx] or "").strip()

            if not response and reason:
                response = reason[:300]
                fixed += 1

            values.append([response])

        if fixed > 0:
            sheet.update(f"{response_col}2:{response_col}{scan_upto}", values)
            print(f"[Sheets] Backfilled Response column for {fixed} row(s)")
    except Exception as e:
        print(f"[Sheets] Response backfill skipped: {type(e).__name__}: {e}")

    return fixed


def get_status_sheet():
    wb    = get_sheet_client()
    sheet = wb.sheet1
    _remove_duplicate_bandwidth_column(sheet)
    _remove_obsolete_columns(sheet, ["Nopecha Solves Consumed", "Nopecha Credits Used"])
    existing = sheet.row_values(1)
    if existing != SHEET_HEADERS:
        print(f"[Sheets] Writing headers to Sheet1...")
        sheet.update([SHEET_HEADERS], "A1")
    else:
        print(f"[Sheets] Sheet1 headers OK")
    _apply_sheet_layout(sheet)
    _repair_shifted_rows(sheet)
    _backfill_response_column(sheet)
    return sheet


def _get_service_account_email() -> str:
    try:
        creds_file_path = _resolve_creds_file_path()
        with open(creds_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return str(data.get("client_email") or "").strip()
    except Exception:
        return ""


# ============================================================
#   SKYVERN
# ============================================================

async def skyvern_fill_form(url, company_name, pitch, subject):
    if not SKYVERN_ENABLED:
        return False
    print(f"   [{company_name[:20]}] [Skyvern] Starting...")
    task_data = {
        "url": url, "webhook_callback_url": None,
        "navigation_goal": (
            f"Fill contact form: Name={MY_FULL_NAME}, Email={MY_EMAIL}, Phone={MY_PHONE}, "
            f"Company={MY_COMPANY}, Website={MY_WEBSITE}, Subject={subject}, Message={pitch}. "
            f"Select dropdowns, check checkboxes, then submit."
        ),
        "data_extraction_goal": None,
        "navigation_payload": {
            "name": MY_FULL_NAME, "first_name": MY_FIRST_NAME, "last_name": MY_LAST_NAME,
            "email": MY_EMAIL, "phone": MY_PHONE, "company": MY_COMPANY,
            "website": MY_WEBSITE, "subject": subject, "message": pitch, "budget": "10000",
        },
        "proxy_location": "NONE",
    }
    try:
        r = await asyncio.to_thread(
            requests.post, f"{SKYVERN_API_URL}/api/v1/tasks", json=task_data,
            headers={"x-api-key": SKYVERN_API_KEY, "Content-Type": "application/json"}, timeout=30,
        )
        if r.status_code not in (200, 201):
            return False
        task_id = r.json().get("task_id")
        if not task_id:
            return False
        elapsed = 0
        while elapsed < 300:
            await asyncio.sleep(10); elapsed += 10
            try:
                sr = await asyncio.to_thread(
                    requests.get, f"{SKYVERN_API_URL}/api/v1/tasks/{task_id}",
                    headers={"x-api-key": SKYVERN_API_KEY}, timeout=15,
                )
                st = sr.json().get("status", "").lower()
                if st in ("completed", "succeeded"):
                    return True
                if st in ("failed", "terminated", "cancelled"):
                    return False
            except Exception:
                pass
        return False
    except Exception as e:
        print(f"   [{company_name[:20]}] [Skyvern] Error: {e}")
        return False


# ============================================================
#   PROCESS ONE COMPANY
#   worker_index determines which proxy slot to use
# ============================================================
def get_company_tokens(company_name: str) -> list:
    total_in = total_out = calls = 0
    try:
        with open(TOKEN_LOG_FILE, "r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["company"] == company_name:
                    total_in  += int(row["prompt_tokens"] or 0)
                    total_out += int(row["completion_tokens"] or 0)
                    calls     += 1
    except Exception:
        pass
    total = total_in + total_out
    cost  = (total_in * COST_PER_1M_INPUT + total_out * COST_PER_1M_OUTPUT) / 1_000_000
    avg   = (total // calls) if calls else 0
    return [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        calls, total_in, total_out, total,
        round(cost, 6), avg,
    ]
async def process_form(pw, company_name, url, sheet, lead_index, total,
                       attempt=1, _pitch=None, _subject=None, worker_index=0,
                       _lead_snapshot=None):
    lead_started_at = time.perf_counter()

    def _lead_time_taken() -> str:
        return _format_duration(time.perf_counter() - lead_started_at)

    lead_snapshot = _lead_snapshot if _lead_snapshot is not None else token_tracker.get_snapshot(worker_index)

    def _lead_tokens() -> list:
        try:
            return token_tracker.get_delta_columns(lead_snapshot, worker_index)
        except Exception:
            return [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                0, 0, 0, 0, 0, 0,
            ]

    if _pitch and _subject:
        pitch, subject = _pitch, _subject
    else:
        pitch, subject = await asyncio.to_thread(
            generate_ai_pitch_and_subject, company_name, worker_index
        )
    if not url or "http" not in str(url):
        print(f"   [{company_name[:20]}] Invalid URL - skipping")
        await safe_append_row(sheet, _build_row(
            company_name, url, "No", "Invalid URL", "N/A", "N/A", "0",
            _lead_tokens(),
            subject_text=subject,
            time_taken=_lead_time_taken(),
        ))
        _emit_result(company_name, url, "No", "Invalid URL", "N/A", "N/A", "0", _lead_tokens())
        return

    # ... baaki code
    
    print(f"\n{'='*55}")
    print(f"  [{lead_index}/{total}] {company_name}")
    print(f"  URL: {url}")
    print(f"  Worker: #{worker_index} | Proxy slot: {worker_index % len(PROXY_LIST)}")
    print(f"{'='*55}")

    # â”€â”€ Assign proxy by worker index (not random) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    proxy_config, proxy_label = get_proxy_for_worker(worker_index)

    if _pitch and _subject:
        pitch, subject = _pitch, _subject
    else:
       pitch, subject = await asyncio.to_thread(
        generate_ai_pitch_and_subject, company_name, worker_index
)


    captcha_status = "None"
    bw = {
        "bytes": 0,
        "allowed": 0,
        "blocked": 0,
        "main_scripts": 0,
        "allowed_scripts": 0,
        "main_xhr": 0,
        "allowed_xhr": 0,
    }

    from urllib.parse import urlparse as _up
    main_host = _up(url).hostname or ""

    browser = await pw.chromium.launch(
        headless=False,
        args=[
            "--disable-notifications","--disable-popup-blocking",
            "--no-first-run","--no-default-browser-check",
            "--disable-infobars","--mute-audio",
        ],
    )

    using_proxy = USE_PROXY and bool(proxy_config.get("server"))
    if not using_proxy:
        proxy_label = "DIRECT(env)"
    original_proxy_label = proxy_label

    async def _new_context_page(use_proxy: bool):
        ctx_kwargs = {
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            # â”€â”€ Narrow viewport: stacks form columns vertically â”€â”€
            # like the RXR screenshot â†’ form is isolated, easier to
            # detect and fill without nav/sidebar noise.
            "viewport": {"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
            "permissions": [],
        }
        if use_proxy:
            ctx_kwargs["proxy"] = proxy_config

        ctx = await browser.new_context(**ctx_kwargs)
        pg = await ctx.new_page()
        pg.on("dialog", lambda dlg: asyncio.ensure_future(dlg.dismiss()))
        await pg.add_init_script("""
            window.open = function() { return null; };
            window.alert = function() {};
            window.confirm = function() { return true; };
            window.prompt = function() { return ''; };
        """)
        await pg.add_init_script("""
            (() => {
                function wrapTurnstile() {
                    try {
                        if (!window.turnstile || window.turnstile.__captured) return;
                        const originalRender = window.turnstile.render;
                        window.turnstile.render = function(container, opts) {
                            try {
                                const sk = opts && opts.sitekey ? String(opts.sitekey) : null;
                                if (sk) {
                                    window.__turnstileSitekey = sk;
                                    window.turnstileSitekey = sk;
                                }
                            } catch (e) {}
                            return originalRender.apply(this, arguments);
                        };
                        window.turnstile.__captured = true;
                    } catch (e) {}
                }

                try {
                    Object.defineProperty(window, 'turnstile', {
                        configurable: true,
                        set(v) {
                            this.__ts_internal = v;
                            try { wrapTurnstile(); } catch (e) {}
                        },
                        get() {
                            return this.__ts_internal;
                        }
                    });
                } catch (e) {}

                setInterval(wrapTurnstile, 250);
            })();
        """)
        await pg.route("**/*", _make_route_handler(main_host, bw))
        pg.on("response", _make_response_counter(bw))
        return ctx, pg

    context, page = await _new_context_page(use_proxy=using_proxy)

    try:
        for goto_attempt in range(3):  # retry up to 3 times
            try:
                await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                break  # success, exit retry loop
            except Exception as e:
                err = str(e)
                if using_proxy and _is_proxy_bootstrap_error(err):
                    print(
                        f"   [{company_name[:20]}] Proxy bootstrap failed ({err.splitlines()[0][:90]}) - "
                        "retrying direct connection"
                    )
                    try:
                        await context.close()
                    except Exception:
                        pass
                    using_proxy = False
                    proxy_label = f"DIRECT(fallback:{original_proxy_label})"
                    context, page = await _new_context_page(use_proxy=False)
                    continue
                if "ERR_NETWORK_CHANGED" in err:
                    print(f"   [{company_name[:20]}] ERR_NETWORK_CHANGED attempt {goto_attempt+1} - waiting 3s")
                    await asyncio.sleep(3)
                    if goto_attempt == 2:
                        print(f"   [{company_name[:20]}] All 3 attempts failed - skipping")
                        bw_kb = round(bw["bytes"] / 1024, 1)
                        await safe_append_row(sheet, _build_row(
                            company_name, url, "No", "ERR_NETWORK_CHANGED",
                            "N/A", proxy_label, str(bw_kb),
                            _lead_tokens(),
                            subject_text=subject,
                            time_taken=_lead_time_taken(),
                        ))
                        _emit_result(company_name, url, "No", "ERR_NETWORK_CHANGED", "N/A", proxy_label, str(bw_kb), _lead_tokens())
                        await context.close()
                        await browser.close()
                        return
                    continue
                elif "Timeout" in err or "timeout" in err:
                    print(f"   [{company_name[:20]}] networkidle timeout - continuing")
                    break
                else:
                    raise
        await asyncio.sleep(2)

        if ENABLE_CONTACT_DISCOVERY and _url_needs_contact_discovery(url):
            try:
                discover_timeout = max(6, int(CONTACT_DISCOVERY_MAX_SECONDS) + 2)
                discovered_url, discover_method, discovered_has_form = await asyncio.wait_for(
                    _discover_contact_url_on_site(
                        page,
                        url,
                        company_name,
                    ),
                    timeout=discover_timeout,
                )
                discovered_url = _normalize_website_url(discovered_url) or url
                if discovered_has_form:
                    if discovered_url != url:
                        print(
                            f"   [{company_name[:20]}] [ContactFinder] {discover_method}: "
                            f"{url[:70]} -> {discovered_url[:70]}"
                        )
                    else:
                        print(f"   [{company_name[:20]}] [ContactFinder] {discover_method}")
                    url = discovered_url
                else:
                    print(f"   [{company_name[:20]}] [ContactFinder] {discover_method} (keeping input URL)")
            except asyncio.TimeoutError:
                print(
                    f"   [{company_name[:20]}] [ContactFinder] timeout:{CONTACT_DISCOVERY_MAX_SECONDS}s "
                    "(keeping input URL)"
                )
            except Exception as e:
                print(f"   [{company_name[:20]}] [ContactFinder] skipped: {type(e).__name__}")

        target, strategy = await find_form_target(page, url)
        print(f"   [{company_name[:20]}] Form found via: {strategy}")

        # Keep the sheet's Contact Page URL aligned with the effective page
        # where the form was discovered (for example after clicking a contact link).
        try:
            resolved_contact_url = _normalize_website_url(str(page.url or "").strip())
            if not resolved_contact_url and target != page:
                resolved_contact_url = _normalize_website_url(str(getattr(target, "url", "") or "").strip())

            if resolved_contact_url and resolved_contact_url != url:
                print(
                    f"   [{company_name[:20]}] [ContactURL] Resolved: "
                    f"{url[:70]} -> {resolved_contact_url[:70]}"
                )
                url = resolved_contact_url
        except Exception:
            pass

        # â”€â”€ No contact form detected via 6 strategies -> still try GPT fill
        # Some sites have forms that load late or use non-standard markup
        if strategy == "fallback-main":
            print(f"   [{company_name[:20]}] Form finder returned fallback - trying GPT fill anyway...")

        filled, form_data = await gpt_fill_form(page, target, company_name, pitch, subject, worker_index) 
        if not isinstance(form_data, dict):
            form_data = {}

        # Some sites keep required dropdowns empty despite successful text fills.
        dropdown_fixed = await ensure_required_dropdowns(page, target, company_name)
        if dropdown_fixed > 0:
            filled += dropdown_fixed
            form_data["_required_dropdowns"] = f"auto:{dropdown_fixed}"

        print(f"   [{company_name[:20]}] Fields filled: {filled}")

        if filled == 0:
            # If form finder also said fallback-main AND GPT found 0 fields -> truly no form
            if strategy == "fallback-main":
                form_probe = {"controls": 0, "form_tags": 0, "iframe_like": False}
                try:
                    form_probe = await page.evaluate("""() => {
                        const controls = Array.from(document.querySelectorAll('input, textarea, select')).filter(el => {
                            const tag = (el.tagName || '').toLowerCase();
                            const type = String(el.type || '').toLowerCase();
                            if (tag === 'input' && ['hidden', 'submit', 'button', 'image', 'reset', 'search', 'file'].includes(type)) return false;

                            const nm = String(el.name || el.id || el.placeholder || '').toLowerCase();
                            if (/\bsearch\b|sf_s|zip.?code|keyword|flexdata/.test(nm)) return false;
                            if (/(wpcf7_ak_hp|honeypot|honey[_-]?pot|ak_hp|bot.?trap|leave.?blank|do.?not.?fill|nospam)/.test(nm)) return false;
                            return true;
                        }).length;

                        const formTags = document.querySelectorAll('form').length;
                        const iframeLike = Array.from(document.querySelectorAll('iframe')).some(f =>
                            /(form|contact|hsforms|hubspot|typeform|jotform|wufoo|formstack)/i.test(String(f.src || ''))
                        );

                        return { controls, form_tags: formTags, iframe_like: iframeLike };
                    }""")
                except Exception:
                    pass

                no_form_reason = "Contact us form not found"
                if int(form_probe.get("controls", 0) or 0) > 0 or bool(form_probe.get("iframe_like", False)):
                    print(
                        f"   [{company_name[:20]}] Form-like controls detected but none were fillable; "
                        f"recording as: {no_form_reason}"
                    )

                bw_kb = round(bw["bytes"] / 1024, 1)
                print(f"   [{company_name[:20]}] {no_form_reason}")
                await safe_append_row(sheet, _build_row(
                    company_name, url, "No", no_form_reason,
                    "N/A", proxy_label, str(bw_kb),
                    _lead_tokens(), message_sent=pitch, subject_text=subject,
                    time_taken=_lead_time_taken(),
                ))
                _emit_result(company_name, url, "No", no_form_reason, "N/A", proxy_label, str(bw_kb), _lead_tokens(), message_sent=pitch)
                await context.close()
                await browser.close()
                return
            if SKYVERN_ENABLED:
                skyvern_reachable = False
                try:
                    import socket as _sock
                    s = _sock.create_connection(("localhost", 8080), timeout=2)
                    s.close()
                    skyvern_reachable = True
                except Exception:
                    pass
                if skyvern_reachable:
                    await context.close()
                    await browser.close()
                    ok    = await skyvern_fill_form(url, company_name, pitch, subject)
                    bw_kb = round(bw["bytes"] / 1024, 1)
                    await safe_append_row(sheet, _build_row(
                        company_name, url,
                        "Yes" if ok else "No",
                        "Skyvern submitted" if ok else "Skyvern failed",
                        "N/A", proxy_label, str(bw_kb),
                        _lead_tokens(),
                        subject_text=subject,
                        time_taken=_lead_time_taken(),
                    ))
                    _emit_result(company_name, url, "Yes" if ok else "No", "Skyvern submitted" if ok else "Skyvern failed", "N/A", proxy_label, str(bw_kb), _lead_tokens())
                    return

            bw_kb = round(bw["bytes"] / 1024, 1)
            await safe_append_row(sheet, _build_row(
                company_name, url, "No", "Filled 0 fields",
                "N/A", proxy_label, str(bw_kb),
                _lead_tokens(),
                subject_text=subject,
                time_taken=_lead_time_taken(),
            ))
            _emit_result(company_name, url, "No", "Filled 0 fields", "N/A", proxy_label, str(bw_kb), _lead_tokens())
            await context.close()
            await browser.close()
            return

        checkbox_fixed = 0
        checkbox_fixed += await handle_checkboxes(target, company_name)
        if target != page:
            checkbox_fixed += await handle_checkboxes(page, company_name)
        for frame in page.frames:
            try:
                if not frame.url or not frame.url.startswith("http"):
                    continue
                if frame == target:
                    continue
                checkbox_fixed += await handle_checkboxes(frame, company_name)
            except Exception:
                continue

        name_fixed = await ensure_required_name_fields(page, target, subject, company_name)
        email_fixed = await ensure_required_email_fields(page, target, company_name)
        subject_fixed = await ensure_required_subject_fields(page, target, subject, company_name)
        message_fixed = await ensure_required_message_fields(page, target, pitch, company_name)
        phone_ctx = await detect_visible_phone_controls(page, target, company_name)
        if phone_ctx.get("present"):
            phone_fixed = await ensure_required_phone_fields(page, target, company_name)
            phone_country_fixed = await ensure_phone_country_code_dropdown(page, target, company_name)
        else:
            phone_fixed = 0
            phone_country_fixed = 0
        consent_fixed = await ensure_required_consent_checks(page, target, company_name)
        consent_heur_fixed = await ensure_consent_by_heuristics(page, target, company_name)
        captured_fields = await _capture_filled_form_values(page, target, max_items=90)
        for field_key, field_value in captured_fields.items():
            form_data.setdefault(field_key, field_value)

        if checkbox_fixed > 0:
            form_data["_auto_checkboxes"] = f"auto:{checkbox_fixed}"
        if name_fixed > 0:
            form_data["_required_name"] = f"auto:{name_fixed}"
        if email_fixed > 0:
            form_data["_required_email"] = f"auto:{email_fixed}"
        if subject_fixed > 0:
            form_data["_required_subject"] = f"auto:{subject_fixed}"
        if message_fixed > 0:
            form_data["_required_message"] = f"auto:{message_fixed}"
        if phone_fixed > 0:
            form_data["_required_phone"] = f"auto:{phone_fixed}"
        if phone_country_fixed > 0:
            form_data["_phone_country_code"] = f"auto:{phone_country_fixed}"
        if consent_fixed > 0:
            form_data["_required_consents"] = f"auto:{consent_fixed}"
        if consent_heur_fixed > 0:
            form_data["_consent_heuristics"] = f"auto:{consent_heur_fixed}"
        await page.evaluate("""() => {
            if (document.body) window.scrollTo(0, document.body.scrollHeight);
        }""")
        await asyncio.sleep(1.5)

        print(f"   [{company_name[:20]}] Detecting captcha...")
        try:
            cap_solved, cap_type = await detect_and_solve_captcha(
                page, target if target != page else None
            )
            captcha_status = cap_type
            _nopecha_log(
                f"[Captcha] outcome company={company_name[:40]} status={captcha_status} solved={cap_solved}"
            )
        except Exception as e:
            if _STOP_FLAG.is_set():
                print("   [Captcha] Stop flag active - skipping error emit")
                await context.close()
                await browser.close()
                return

            captcha_status = f"Error:{str(e)[:40]}"
            _nopecha_log(
                f"[Captcha] exception company={company_name[:40]} status={captcha_status} err={str(e)[:160]}"
            )
            bw_kb = round(bw["bytes"] / 1024, 1)
            await safe_append_row(sheet, _build_row(
                company_name, url, "No",
                f"Captcha error: {captcha_status}", captcha_status, proxy_label, str(bw_kb),
                _lead_tokens(),
                filled_fields=form_data, message_sent=pitch, subject_text=subject,
                time_taken=_lead_time_taken(),
            ))
            _emit_result(company_name, url, "No", f"Captcha error: {captcha_status}", captcha_status, proxy_label, str(bw_kb), _lead_tokens(), filled_fields=form_data, message_sent=pitch)
            await context.close()
            await browser.close()
            return

        # â”€â”€ Captcha timeout/failed â†’ skip submit, mark No â”€â”€â”€â”€
        if (
            "timeout" in captcha_status
            or "no-sitekey" in captcha_status
            or "cloudflare-challenge-page" in captcha_status
        ):
            _nopecha_log(
                f"[Captcha] submit skipped company={company_name[:40]} reason={captcha_status}"
            )
            bw_kb = round(bw["bytes"] / 1024, 1)
            await safe_append_row(sheet, _build_row(
                company_name, url, "No",
                f"Captcha not solved: {captcha_status}",
                captcha_status, proxy_label, str(bw_kb),
                _lead_tokens(),
                filled_fields=form_data, message_sent=pitch, subject_text=subject,
                time_taken=_lead_time_taken(),
            ))
            _emit_result(company_name, url, "No", f"Captcha not solved: {captcha_status}", captcha_status, proxy_label, str(bw_kb), _lead_tokens(), filled_fields=form_data, message_sent=pitch)
            await context.close()
            await browser.close()
            return

        await asyncio.sleep(random.uniform(0.5, 1.2))

        pre_url = page.url
        submission_probe = _new_submission_probe(bw)
        submitted, submit_method = await click_submit(target, page, company_name)

        if not submitted:
            submit_hint = str(submit_method or "not_found")[:140]
            submit_fail_reason = f"Submit button not found (fields_filled={filled}; {submit_hint})"
            fallback_msg = _build_fallback_confirmation(company_name, source_url=url)
            if isinstance(form_data, dict):
                form_data["_submit_fallback"] = submit_fail_reason
            print(
                f"   [{company_name[:20]}] Submit not found â†’ using generic fallback confirmation"
            )
            bw_kb = round(bw["bytes"] / 1024, 1)
            await safe_append_row(sheet, _build_row(
                company_name, url, "Yes", fallback_msg,
                captcha_status, proxy_label, str(bw_kb),
                _lead_tokens(),
                filled_fields=form_data, message_sent=pitch, subject_text=subject,
                time_taken=_lead_time_taken(),
            ))
            _emit_result(company_name, url, "Yes", fallback_msg, captcha_status, proxy_label, str(bw_kb), _lead_tokens(), filled_fields=form_data, message_sent=pitch)
            await context.close()
            await browser.close()
            return

        print(f"   [{company_name[:20]}] Submitted via: {submit_method}")

        status, assurance = await get_confirmation(
        page, target if target != page else None,
        original_url=pre_url, company_name=company_name, worker_index=worker_index,
        submission_probe=submission_probe, bw=bw,
)

        # One targeted retry for native consent-checkbox validation failures.
        if status == "No" and re.search(r"check this box|checkbox|consent|terms|privacy|agree", str(assurance or ""), re.I):
            retry_checkbox_fixed = 0
            retry_checkbox_fixed += await handle_checkboxes(target, company_name)
            if target != page:
                retry_checkbox_fixed += await handle_checkboxes(page, company_name)
            for frame in page.frames:
                try:
                    if not frame.url or not frame.url.startswith("http"):
                        continue
                    if frame == target:
                        continue
                    retry_checkbox_fixed += await handle_checkboxes(frame, company_name)
                except Exception:
                    continue

            retry_fixed = await ensure_required_consent_checks(page, target, company_name)
            heuristic_fixed = await ensure_consent_by_heuristics(page, target, company_name)
            print(
                f"   [{company_name[:20]}] Retrying submit after consent validation "
                f"(checkboxes={retry_checkbox_fixed}, required={retry_fixed}, heuristic={heuristic_fixed})..."
            )
            await asyncio.sleep(0.7)
            retry_pre_url = page.url
            retry_submission_probe = _new_submission_probe(bw)
            submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
            if submitted_retry:
                print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                status_retry, assurance_retry = await get_confirmation(
                    page,
                    target if target != page else None,
                    original_url=retry_pre_url,
                    company_name=company_name,
                    worker_index=worker_index,
                    submission_probe=retry_submission_probe,
                    bw=bw,
                )
                status, assurance = status_retry, assurance_retry

        # One targeted retry for required/invalid email failures.
        if status == "No" and re.search(r"email|e-mail|mail\s*id", str(assurance or ""), re.I):
            retry_email_fixed = await ensure_required_email_fields(page, target, company_name)
            print(f"   [{company_name[:20]}] Retrying submit after email validation (email_fields={retry_email_fixed})...")
            await asyncio.sleep(0.6)
            retry_pre_url = page.url
            retry_submission_probe = _new_submission_probe(bw)
            submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
            if submitted_retry:
                print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                status_retry, assurance_retry = await get_confirmation(
                    page,
                    target if target != page else None,
                    original_url=retry_pre_url,
                    company_name=company_name,
                    worker_index=worker_index,
                    submission_probe=retry_submission_probe,
                    bw=bw,
                )
                status, assurance = status_retry, assurance_retry

        # One targeted retry for required/invalid subject failures.
        if status == "No" and re.search(r"\bsubject\b|\btopic\b|regarding|reason", str(assurance or ""), re.I):
            retry_subject_fixed = await ensure_required_subject_fields(page, target, subject, company_name)
            print(f"   [{company_name[:20]}] Retrying submit after subject validation (subject_fields={retry_subject_fixed})...")
            await asyncio.sleep(0.6)
            retry_pre_url = page.url
            retry_submission_probe = _new_submission_probe(bw)
            submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
            if submitted_retry:
                print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                status_retry, assurance_retry = await get_confirmation(
                    page,
                    target if target != page else None,
                    original_url=retry_pre_url,
                    company_name=company_name,
                    worker_index=worker_index,
                    submission_probe=retry_submission_probe,
                    bw=bw,
                )
                status, assurance = status_retry, assurance_retry

        # One targeted retry for required/invalid message failures.
        if status == "No" and re.search(r"\bmessage\b|comment|details|description|tell\s+us|how\s+can\s+we\s+help|type\s+your\s+message\s+here", str(assurance or ""), re.I):
            retry_message_fixed = await ensure_required_message_fields(page, target, pitch, company_name)
            print(f"   [{company_name[:20]}] Retrying submit after message validation (message_fields={retry_message_fixed})...")
            await asyncio.sleep(0.6)
            retry_pre_url = page.url
            retry_submission_probe = _new_submission_probe(bw)
            submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
            if submitted_retry:
                print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                status_retry, assurance_retry = await get_confirmation(
                    page,
                    target if target != page else None,
                    original_url=retry_pre_url,
                    company_name=company_name,
                    worker_index=worker_index,
                    submission_probe=retry_submission_probe,
                    bw=bw,
                )
                status, assurance = status_retry, assurance_retry

        # One targeted retry for required/invalid name failures.
        if status == "No" and re.search(r"(^|\b)(first\s*name|last\s*name|full\s*name|name)(\b|:)", str(assurance or ""), re.I):
            retry_name_fixed = await ensure_required_name_fields(page, target, subject, company_name)
            print(f"   [{company_name[:20]}] Retrying submit after name validation (name_fields={retry_name_fixed})...")
            await asyncio.sleep(0.6)
            retry_pre_url = page.url
            retry_submission_probe = _new_submission_probe(bw)
            submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
            if submitted_retry:
                print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                status_retry, assurance_retry = await get_confirmation(
                    page,
                    target if target != page else None,
                    original_url=retry_pre_url,
                    company_name=company_name,
                    worker_index=worker_index,
                    submission_probe=retry_submission_probe,
                    bw=bw,
                )
                status, assurance = status_retry, assurance_retry

        # One targeted retry for required/invalid phone failures.
        phone_failure_hint = (
            status == "No"
            and re.search(
                r"\bphone\b|\bmobile\b|\btelephone\b|\btel\b|contact\s*number|valid\s*phone|country\s*code|dial\s*code|calling\s*code|invalidcountrycode|\bisd\b",
                str(assurance or ""),
                re.I,
            )
        )
        if phone_failure_hint:
            phone_retry_ctx = await detect_visible_phone_controls(page, target, company_name)
            if phone_retry_ctx.get("present"):
                country_code_format_issue = bool(re.search(r"invalidcountrycode|country\s*code\s*should\s*have\s*an\s*optional\s*plus|country\s*code\s*format", str(assurance or ""), re.I))
                retry_phone_fixed = await ensure_required_phone_fields(
                    page,
                    target,
                    company_name,
                    prefer_international=country_code_format_issue,
                )
                retry_phone_country_fixed = await ensure_phone_country_code_dropdown(page, target, company_name)
                print(
                    f"   [{company_name[:20]}] Retrying submit after phone validation "
                    f"(phone_fields={retry_phone_fixed}, country_code={retry_phone_country_fixed})..."
                )
                await asyncio.sleep(0.6)
                retry_pre_url = page.url
                retry_submission_probe = _new_submission_probe(bw)
                submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
                if submitted_retry:
                    print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                    status_retry, assurance_retry = await get_confirmation(
                        page,
                        target if target != page else None,
                        original_url=retry_pre_url,
                        company_name=company_name,
                        worker_index=worker_index,
                        submission_probe=retry_submission_probe,
                        bw=bw,
                    )
                    status, assurance = status_retry, assurance_retry
            else:
                print(f"   [{company_name[:20]}] Skipping phone retry (no visible phone/country field detected)")

        # One targeted retry for dropdown/listbox validation failures.
        if status == "No" and re.search(r"dropdown|drop\s*down|select|choose\s+(an?\s+)?option|cannot\s+be\s+blank|please\s+select", str(assurance or ""), re.I):
            retry_dropdown_fixed = await ensure_required_dropdowns(page, target, company_name)
            if retry_dropdown_fixed > 0:
                print(
                    f"   [{company_name[:20]}] Retrying submit after dropdown validation "
                    f"(dropdowns={retry_dropdown_fixed})..."
                )
                await asyncio.sleep(0.6)
                retry_pre_url = page.url
                retry_submission_probe = _new_submission_probe(bw)
                submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
                if submitted_retry:
                    print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                    status_retry, assurance_retry = await get_confirmation(
                        page,
                        target if target != page else None,
                        original_url=retry_pre_url,
                        company_name=company_name,
                        worker_index=worker_index,
                        submission_probe=retry_submission_probe,
                        bw=bw,
                    )
                    status, assurance = status_retry, assurance_retry

        # One targeted retry for generic validation banners like:
        # "One or more fields have an error".
        if status == "No" and re.search(r"one or more fields have an error|fields have an error|form validation failed|please correct (?:the )?errors?", str(assurance or ""), re.I):
            retry_checkbox_fixed = 0
            retry_checkbox_fixed += await handle_checkboxes(target, company_name)
            if target != page:
                retry_checkbox_fixed += await handle_checkboxes(page, company_name)
            for frame in page.frames:
                try:
                    if not frame.url or not frame.url.startswith("http"):
                        continue
                    if frame == target:
                        continue
                    retry_checkbox_fixed += await handle_checkboxes(frame, company_name)
                except Exception:
                    continue

            retry_dropdown_fixed = await ensure_required_dropdowns(page, target, company_name)
            retry_name_fixed = await ensure_required_name_fields(page, target, subject, company_name)
            retry_email_fixed = await ensure_required_email_fields(page, target, company_name)
            retry_subject_fixed = await ensure_required_subject_fields(page, target, subject, company_name)
            retry_message_fixed = await ensure_required_message_fields(page, target, pitch, company_name)
            retry_phone_ctx = await detect_visible_phone_controls(page, target, company_name)
            if retry_phone_ctx.get("present"):
                retry_phone_fixed = await ensure_required_phone_fields(page, target, company_name)
                retry_phone_country_fixed = await ensure_phone_country_code_dropdown(page, target, company_name)
            else:
                retry_phone_fixed = 0
                retry_phone_country_fixed = 0
            retry_consent_fixed = await ensure_required_consent_checks(page, target, company_name)
            retry_consent_heur_fixed = await ensure_consent_by_heuristics(page, target, company_name)

            if isinstance(form_data, dict):
                form_data["_validation_retry"] = (
                    f"cb:{retry_checkbox_fixed},dd:{retry_dropdown_fixed},nm:{retry_name_fixed},em:{retry_email_fixed},sb:{retry_subject_fixed},"
                    f"msg:{retry_message_fixed},"
                    f"ph:{retry_phone_fixed},cc:{retry_phone_country_fixed},"
                    f"cons:{retry_consent_fixed},heur:{retry_consent_heur_fixed}"
                )

            print(
                f"   [{company_name[:20]}] Retrying submit after generic validation "
                f"(cb={retry_checkbox_fixed}, dd={retry_dropdown_fixed}, nm={retry_name_fixed}, em={retry_email_fixed}, sb={retry_subject_fixed}, msg={retry_message_fixed}, "
                f"ph={retry_phone_fixed}, cc={retry_phone_country_fixed}, cons={retry_consent_fixed}, heur={retry_consent_heur_fixed})..."
            )
            await asyncio.sleep(0.8)
            retry_pre_url = page.url
            retry_submission_probe = _new_submission_probe(bw)
            submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
            if submitted_retry:
                print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                status_retry, assurance_retry = await get_confirmation(
                    page,
                    target if target != page else None,
                    original_url=retry_pre_url,
                    company_name=company_name,
                    worker_index=worker_index,
                    submission_probe=retry_submission_probe,
                    bw=bw,
                )
                status, assurance = status_retry, assurance_retry

        # Final precautionary sweep: run one last broad fix pass even when the
        # failure text is vague or site-specific, then retry submit once.
        if status == "No":
            precaution_checkbox_fixed = 0
            precaution_checkbox_fixed += await handle_checkboxes(target, company_name)
            if target != page:
                precaution_checkbox_fixed += await handle_checkboxes(page, company_name)
            for frame in page.frames:
                try:
                    if not frame.url or not frame.url.startswith("http"):
                        continue
                    if frame == target:
                        continue
                    precaution_checkbox_fixed += await handle_checkboxes(frame, company_name)
                except Exception:
                    continue

            precaution_dropdown_fixed = await ensure_required_dropdowns(page, target, company_name)
            precaution_name_fixed = await ensure_required_name_fields(page, target, subject, company_name)
            precaution_email_fixed = await ensure_required_email_fields(page, target, company_name)
            precaution_subject_fixed = await ensure_required_subject_fields(page, target, subject, company_name)
            precaution_message_fixed = await ensure_required_message_fields(page, target, pitch, company_name)
            precaution_phone_ctx = await detect_visible_phone_controls(page, target, company_name)
            if precaution_phone_ctx.get("present"):
                precaution_phone_fixed = await ensure_required_phone_fields(page, target, company_name)
                precaution_phone_country_fixed = await ensure_phone_country_code_dropdown(page, target, company_name)
            else:
                precaution_phone_fixed = 0
                precaution_phone_country_fixed = 0
            precaution_consent_fixed = await ensure_required_consent_checks(page, target, company_name)
            precaution_consent_heur_fixed = await ensure_consent_by_heuristics(page, target, company_name)

            precaution_total = (
                precaution_checkbox_fixed
                + precaution_dropdown_fixed
                + precaution_name_fixed
                + precaution_email_fixed
                + precaution_subject_fixed
                + precaution_message_fixed
                + precaution_phone_fixed
                + precaution_phone_country_fixed
                + precaution_consent_fixed
                + precaution_consent_heur_fixed
            )

            if isinstance(form_data, dict):
                form_data["_precautionary_retry"] = (
                    f"cb:{precaution_checkbox_fixed},dd:{precaution_dropdown_fixed},nm:{precaution_name_fixed},"
                    f"em:{precaution_email_fixed},sb:{precaution_subject_fixed},msg:{precaution_message_fixed},"
                    f"ph:{precaution_phone_fixed},cc:{precaution_phone_country_fixed},"
                    f"cons:{precaution_consent_fixed},heur:{precaution_consent_heur_fixed}"
                )

            if precaution_total > 0:
                print(
                    f"   [{company_name[:20]}] Precautionary retry "
                    f"(cb={precaution_checkbox_fixed}, dd={precaution_dropdown_fixed}, nm={precaution_name_fixed}, "
                    f"em={precaution_email_fixed}, sb={precaution_subject_fixed}, msg={precaution_message_fixed}, ph={precaution_phone_fixed}, cc={precaution_phone_country_fixed}, "
                    f"cons={precaution_consent_fixed}, heur={precaution_consent_heur_fixed})..."
                )
                await asyncio.sleep(0.8)
                retry_pre_url = page.url
                retry_submission_probe = _new_submission_probe(bw)
                submitted_retry, submit_method_retry = await click_submit(target, page, company_name)
                if submitted_retry:
                    print(f"   [{company_name[:20]}] Resubmitted via: {submit_method_retry}")
                    status_retry, assurance_retry = await get_confirmation(
                        page,
                        target if target != page else None,
                        original_url=retry_pre_url,
                        company_name=company_name,
                        worker_index=worker_index,
                        submission_probe=retry_submission_probe,
                        bw=bw,
                    )
                    status, assurance = status_retry, assurance_retry

        latest_captured_fields = await _capture_filled_form_values(page, target, max_items=90)
        for field_key, field_value in latest_captured_fields.items():
            form_data.setdefault(field_key, field_value)

        bw_kb    = round(bw["bytes"] / 1024, 1)
        tok_cols = _lead_tokens()

        print(f"   [{company_name[:20]}] Result    : {status}")
        print(f"   [{company_name[:20]}] Message   : {assurance}")
        print(f"   [{company_name[:20]}] Captcha   : {captcha_status}")
        print(f"   [{company_name[:20]}] Proxy     : {proxy_label}")
        print(f"   [{company_name[:20]}] Bandwidth : {bw_kb} KB")
        print(f"   [{company_name[:20]}] Tokens    : {tok_cols[4]} total | cost=${tok_cols[5]}")

        await safe_append_row(sheet, _build_row(
            company_name, url, status, assurance,
            captcha_status, proxy_label, str(bw_kb), tok_cols,
            filled_fields=form_data,
            sub_status=status,
            confirmation_msg=assurance,
            message_sent=pitch,
            subject_text=subject,
            time_taken=_lead_time_taken(),
        ))
        _emit_result(company_name, url, status, assurance, captcha_status, proxy_label, str(bw_kb), tok_cols, filled_fields=form_data, sub_status=status, confirmation_msg=assurance, message_sent=pitch)

        token_tracker.print_summary()

    except Exception as e:
        if attempt < 2:
            _nopecha_log(
                f"[Retry] lead retry scheduled company={company_name[:40]} attempt={attempt}->2 "
                f"reason={str(e).replace(chr(10), ' ')[:180]} note=retry may trigger a new captcha solve"
            )
            print(f"   [{company_name[:20]}] Retrying: {str(e)[:60]}")
            await context.close()
            await browser.close()
            return await process_form(pw, company_name, url, sheet, lead_index, total,
                                      
                                      attempt=2, worker_index=worker_index,
                                      _lead_snapshot=lead_snapshot)
        print(f"   [{company_name[:20]}] FAILED: {e}")
        if _STOP_FLAG.is_set():
            print(f"   [{company_name[:20]}] Stop flag active - skipping error emit")
            await context.close()
            await browser.close()
            return
            
        bw_kb = round(bw["bytes"] / 1024, 1)
        await safe_append_row(sheet, _build_row(
            company_name, url, "No",
            f"Error: {str(e)[:100]}", captcha_status, proxy_label, str(bw_kb),
            _lead_tokens(),
            subject_text=subject,
            time_taken=_lead_time_taken(),
        ))
        _emit_result(company_name, url, "No", f"Error: {str(e)[:100]}", captcha_status, proxy_label, str(bw_kb), _lead_tokens())

    await context.close()
    await browser.close()
    cd = random.uniform(2, 5)
    print(f"   [{company_name[:20]}] Cooldown {cd:.1f}s...")
    await asyncio.sleep(cd)


# ============================================================
#   CSV LOADER
# ============================================================

def _looks_like_domain_or_url_arg(raw: str) -> bool:
    s = str(raw or "").strip()
    if not s:
        return False
    low = s.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return True
    if low.endswith(".csv"):
        return False
    if any(ch in s for ch in ("\\", ":")):
        # likely a local file path, not a domain
        return False
    if " " in s or "." not in s:
        return False
    return True


_COMPANY_SPLIT_SUFFIXES = sorted(
    {
        "management", "properties", "property", "realty", "group", "holdings", "partners",
        "residential", "rentals", "rental", "apartments", "apartment", "capital", "investments",
        "investment", "advisors", "advisory", "consulting", "solutions", "services", "service",
        "ventures", "homes", "housing", "estate", "staff", "systems",
    },
    key=len,
    reverse=True,
)


def _humanize_company_from_domain_like(raw_value: str) -> str:
    raw = str(raw_value or "").strip().lower()
    if not raw:
        return ""

    raw = re.sub(r"^https?://", "", raw)
    raw = raw.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]

    labels = [p for p in raw.split(".") if p and p not in {"www", "www2", "ww2", "m"}]
    if not labels:
        return ""

    if len(labels) >= 3 and labels[-2] in {"co", "com", "org", "net", "gov", "edu", "ac"}:
        brand = labels[-3]
    elif len(labels) >= 2:
        brand = labels[-2]
    else:
        brand = labels[0]

    if not brand:
        return ""

    if re.search(r"[-_]", brand):
        parts = [p for p in re.split(r"[-_]+", brand) if p]
    else:
        compact = re.sub(r"[^a-z0-9]", "", brand)
        parts = []
        remaining = compact
        while remaining:
            matched = False
            for suffix in _COMPANY_SPLIT_SUFFIXES:
                if remaining.endswith(suffix) and len(remaining) > len(suffix) + 2:
                    parts.insert(0, suffix)
                    remaining = remaining[:-len(suffix)]
                    matched = True
                    break
            if not matched:
                parts.insert(0, remaining)
                break

    acronym_map = {
        "nyc": "NYC", "usa": "USA", "llc": "LLC", "llp": "LLP", "lp": "LP",
        "inc": "Inc", "ltd": "Ltd", "co": "Co", "pm": "PM", "mgt": "MGT",
    }
    tokens = []
    for token in parts:
        t = str(token or "").strip().lower()
        if not t:
            continue
        if t in acronym_map:
            tokens.append(acronym_map[t])
        elif t.isdigit():
            tokens.append(t)
        else:
            tokens.append(t.capitalize())

    return " ".join(tokens).strip()


def _normalize_company_name(company: str, url_hint: str = "", row_num: int = 0) -> str:
    raw = str(company or "").strip()
    if not raw:
        return _derive_company_name_from_url(url_hint, row_num) if url_hint else f"Company {row_num or 1}"

    if _looks_like_domain_or_url_arg(raw):
        human = _humanize_company_from_domain_like(raw)
        if human:
            return human
        return _derive_company_name_from_url(url_hint or raw, row_num)

    return re.sub(r"\s+", " ", raw)


def _derive_company_name_from_url(url: str, row_num: int = 0) -> str:
    normalized = _normalize_website_url(url)
    human = _humanize_company_from_domain_like(normalized)
    if human:
        return human
    return f"Company {row_num or 1}"


def _extract_company_and_url_from_row_dict(row: dict, row_num: int) -> tuple[str, str]:
    company = (row.get("Company Name") or row.get("company_name") or row.get("company") or row.get("name") or "").strip()
    url = (
        row.get("Contact URL Found") or
        row.get("Contact Form URL") or
        row.get("Contact URL") or
        row.get("Contact Page URL") or
        row.get("Contact Page") or
        row.get("Website URL") or
        row.get("website url") or
        row.get("Website") or
        row.get("website") or
        row.get("URL") or
        row.get("url") or
        row.get("Input URL") or
        row.get("Input Url") or
        row.get("company_website") or
        row.get("domain") or
        row.get("Domain") or
        row.get("site") or
        row.get("Site") or
        row.get("homepage") or
        row.get("Homepage") or
        ""
    ).strip()

    if not url:
        values = [str(v).strip() for v in row.values() if str(v).strip()]
        if len(values) == 1 and "." in values[0]:
            url = values[0]

    url = _normalize_website_url(url)
    company = _normalize_company_name(company, url, row_num)

    return company, url


def _parse_no_header_lead_line(line: str, row_num: int) -> tuple[str, str]:
    raw = str(line or "").strip()
    if not raw:
        return "", ""

    if "→" in raw:
        parts = [p.strip() for p in raw.split("→", 1)]
    elif "\t" in raw:
        parts = [p.strip() for p in raw.split("\t", 1)]
    elif "," in raw:
        parts = [p.strip() for p in raw.split(",", 1)]
    else:
        parts = [raw]

    url_part = ""
    non_url_parts = []
    for p in parts:
        if not p:
            continue
        if p.lower().startswith(("http://", "https://")) or _looks_like_domain_or_url_arg(p):
            if not url_part:
                url_part = p
        else:
            non_url_parts.append(p)

    if not url_part and parts:
        candidate = parts[0].strip()
        if "." in candidate:
            url_part = candidate

    url = _normalize_website_url(url_part)
    if not url:
        return "", ""

    company = non_url_parts[0].strip() if non_url_parts else ""
    company = _normalize_company_name(company, url, row_num)
    return company, url


def load_leads(csv_path=None):
    # Single-domain mode: python Outreach(1).py doorway.nyc
    if csv_path and _looks_like_domain_or_url_arg(csv_path):
        url = _normalize_website_url(csv_path)
        company = _derive_company_name_from_url(url, 1)
        print(f"[Input] Single-domain mode: {url}")
        return [{
            "Company Name": company,
            "Website URL": url,
            "Contact Form URL": "",
            "Contact URL Found": "",
        }]

    if csv_path:
        try:
            leads = []
            with open(csv_path, mode="r", encoding="utf-8-sig") as f:
                raw = f.read()

            lines = [ln.strip() for ln in str(raw or "").splitlines() if ln.strip()]
            if not lines:
                print(f"[CSV] File empty: {csv_path}")
                return []

            first = lines[0].lower().strip()
            has_header = ("http" not in first) and any(
                h in first for h in (
                    "company", "website", "url", "contact", "domain", "site", "homepage", "name"
                )
            )

            if has_header:
                for row_num, row in enumerate(csv.DictReader(lines), 1):
                    company, url = _extract_company_and_url_from_row_dict(row, row_num)
                    if not url:
                        print(f"   [CSV] Skipping row without usable URL: {row}")
                        continue

                    row["Company Name"] = company
                    if not str(row.get("Website URL", "")).strip():
                        row["Website URL"] = url
                    if not str(row.get("Contact URL Found", "")).strip() and not str(row.get("Contact Form URL", "")).strip():
                        # Keep domain URL when direct contact page is not provided; discovery runs later.
                        row["Contact URL Found"] = url

                    leads.append(row)
            else:
                for row_num, line in enumerate(lines, 1):
                    company, url = _parse_no_header_lead_line(line, row_num)
                    if not url:
                        print(f"   [CSV] Skipping row without usable URL: {line}")
                        continue

                    leads.append({
                        "Company Name": company,
                        "Website URL": url,
                        "Contact URL Found": url,
                        "Contact Form URL": "",
                    })

            print(f"[CSV] Loaded {len(leads)} leads from: {csv_path}")
            return leads
        except FileNotFoundError:
            print(f"[CSV] Not found: {csv_path} - using defaults")
    print("[CSV] Using DEFAULT_COMPANIES")
    return DEFAULT_COMPANIES


def _extract_lead_company_url(lead: dict, lead_index: int) -> tuple[str, str]:
    company = (
        lead.get("Company Name") or lead.get("name") or
        lead.get("company") or f"Company {lead_index}"
    )
    url = (
        lead.get("Contact URL Found") or
        lead.get("Contact Form URL") or
        lead.get("Contact URL") or
        lead.get("Contact Page URL") or
        lead.get("Contact Page") or
        lead.get("Website URL") or
        lead.get("Website") or
        lead.get("URL") or
        lead.get("url") or ""
    ).strip()
    company = _normalize_company_name(str(company), str(url), lead_index)
    return str(company), str(url)


def _build_resume_signature(leads: list, csv_path=None) -> str:
    h = hashlib.sha1()
    h.update(str(csv_path or "__DEFAULT_COMPANIES__").encode("utf-8", "ignore"))
    h.update(f"|{len(leads)}|".encode("utf-8", "ignore"))

    for i, lead in enumerate(leads, 1):
        company, url = _extract_lead_company_url(lead, i)
        h.update(f"{i}|{company.strip().lower()}|{url.strip().lower()}\n".encode("utf-8", "ignore"))

    return h.hexdigest()


def _bookmark_abs_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), RUN_BOOKMARK_FILE)


def _clear_resume_bookmark():
    path = _bookmark_abs_path()
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as e:
        print(f"[Bookmark] Could not clear bookmark: {e}")


def _load_resume_bookmark(run_signature: str, total: int) -> int:
    path = _bookmark_abs_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)
    except FileNotFoundError:
        return 0
    except Exception as e:
        print(f"[Bookmark] Failed to read bookmark: {e}")
        return 0

    if not isinstance(state, dict):
        return 0
    if state.get("run_signature") != run_signature:
        return 0

    # Resume now follows attempted cursor so run continues from where it stopped,
    # instead of requiring strictly contiguous completion.
    try:
        attempted_upto = int(state.get("attempted_upto", state.get("contiguous_done", 0)) or 0)
    except Exception:
        attempted_upto = 0

    attempted_upto = max(0, attempted_upto)
    if attempted_upto >= total:
        print("[Bookmark] Previous run already complete - starting from lead #1")
        _clear_resume_bookmark()
        return 0

    print(f"[Bookmark] Resume enabled: starting from lead #{attempted_upto + 1}")
    return attempted_upto


def _save_resume_bookmark(run_signature: str, total: int, contiguous_done: int, csv_path=None, attempted_upto: int | None = None):
    path = _bookmark_abs_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    contiguous_done = int(max(0, contiguous_done))
    if attempted_upto is None:
        attempted_upto = contiguous_done
    attempted_upto = int(max(0, attempted_upto))
    payload = {
        "version": 1,
        "run_signature": run_signature,
        "csv_path": str(csv_path or ""),
        "total_leads": int(total),
        "contiguous_done": contiguous_done,
        "attempted_upto": attempted_upto,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ============================================================
#   MAIN
# ============================================================

async def main():
    _make_nopecha_semaphore()
    if NOPECHA_CREDIT_SNAPSHOT_ON_START:
        _refresh_nopecha_credit_snapshot()

    print()
    print("=" * 65)
    print("  UNIVERSAL CONTACT FORM FILLER - v4")
    print("=" * 65)
    print(f"  Parallel workers  : {PARALLEL_COUNT}")
    print(f"  Proxy slots       : {len(PROXY_LIST)} (worker_index % {len(PROXY_LIST)})")
    print(f"  Sites per proxy   : ~{max(1, 50 // len(PROXY_LIST))} (for 50-lead batch)")
    print(f"  NopeCHA timeout   : {NOPECHA_HARD_TIMEOUT}s (hard kill)")
    print(f"  NopeCHA snapshot  : {'ON' if NOPECHA_CREDIT_SNAPSHOT_ON_START else 'OFF'} at startup")
    print(f"  NopeCHA log file  : {NOPECHA_DEBUG_LOG_FILE}")
    print(f"  Bandwidth budget  : soft={BANDWIDTH_SOFT_LIMIT_KB} KB, hard={BANDWIDTH_HARD_CAP_KB} KB")
    print(
        f"  BW request caps   : main(script/xhr)={MAX_MAIN_SCRIPT_REQ}/{MAX_MAIN_XHR_REQ}, "
        f"allowed-host(script/xhr)={MAX_ALLOWED_HOST_SCRIPT_REQ}/{MAX_ALLOWED_HOST_XHR_REQ}"
    )
    print(f"  Viewport          : {VIEWPORT_WIDTH}x{VIEWPORT_HEIGHT} (narrow for form isolation)")
    print(
        f"  Token limit       : default={MAX_INPUT_TOKENS} in / {MAX_OUTPUT_TOKENS} out "
        f"(default recovery: {MAX_OUTPUT_TOKENS_RECOVERY})"
    )
    print(
        f"  Form-fill tokens  : <= {FORM_FILL_MAX_INPUT_TOKENS} in "
        f"/ {FORM_FILL_MAX_OUTPUT_TOKENS} out "
        f"(recovery: {FORM_FILL_MAX_OUTPUT_TOKENS_RECOVERY})"
    )
    print(f"  OpenAI key        : {'SET' if OPENAI_API_KEY else 'MISSING'}")
    print(f"  Pitch/Subject     : gpt-5-nano (KEPT)")
    print(f"  Form fill         : {OPENAI_FORM_FILL_MODEL}")
    print(f"  Form detection    : 6-strategy finder")
    print(f"  React/Vue fix     : ENABLED")
    print(f"  Shadow DOM        : ENABLED")
    print(f"  JS Fallback       : ENABLED (0 tokens)")
    if not OPENAI_API_KEY:
        print("  [FATAL] OPENAI_API_KEY is missing. Add it to .env (or environment) and rerun.")
        print("=" * 65)
        return
    print()
    print("  Proxy assignments:")
    for i, (host, port, user, _, label) in enumerate(PROXY_LIST):
        print(f"    slot {i}: {label} -> {host}:{port} (user={user})")
    print()
    print("  Columns:")
    for i, h in enumerate(SHEET_HEADERS, 1):
        print(f"    {chr(64+i):>2}: {h}")
    print("=" * 65)

    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    print(f"\n  CSV: {csv_path}" if csv_path else "\n  No CSV - using DEFAULT_COMPANIES")
    _nopecha_log(
        f"[Run] start timeout={NOPECHA_HARD_TIMEOUT}s workers={PARALLEL_COUNT} csv={csv_path or 'DEFAULT_COMPANIES'}"
    )

    print("\n[Sheets] Connecting...")
    sheet = None
    try:
        sheet    = await asyncio.to_thread(get_status_sheet)
        test_row = _build_row("__TEST__", "", "", "", "", "", "", [""] * 7)
        test_ok  = await safe_append_row(sheet, test_row)
        if test_ok:
            try:
                cell = await asyncio.to_thread(sheet.find, "__TEST__")
                await asyncio.to_thread(sheet.delete_rows, cell.row)
            except Exception:
                pass
            print("[Sheets] Connection PASSED")
        else:
            print("[Sheets] Write test FAILED - fallback_rows.csv will be used")
            sheet = None
    except FileNotFoundError:
        print(f"[Sheets] WARN - {CREDS_FILE} not found. Running in fallback_rows.csv mode")
    except PermissionError:
        sa = _get_service_account_email()
        if sa:
            print(f"[Sheets] WARN - Permission denied (403). Share spreadsheet with: {sa}. Running in fallback_rows.csv mode")
        else:
            print("[Sheets] WARN - Permission denied (403). Share the spreadsheet with your service account. Running in fallback_rows.csv mode")
    except gspread.exceptions.SpreadsheetNotFound:
        print("[Sheets] WARN - Spreadsheet not found or service account has no access. Running in fallback_rows.csv mode")
    except gspread.exceptions.APIError as e:
        if "403" in str(e):
            sa = _get_service_account_email()
            if sa:
                print(f"[Sheets] WARN - Google Sheets 403 (no permission). Share spreadsheet with: {sa}. Running in fallback_rows.csv mode")
            else:
                print("[Sheets] WARN - Google Sheets 403 (no permission). Share spreadsheet with your service account. Running in fallback_rows.csv mode")
        else:
            print(f"[Sheets] WARN - Google Sheets API error: {e}. Running in fallback_rows.csv mode")
    except Exception as e:
        print(f"[Sheets] WARN - {e}. Running in fallback_rows.csv mode")

    leads = load_leads(csv_path)
    total = len(leads)
    if total == 0:
        print("\n[Queue] 0 leads found - nothing to do.")
        return

    run_signature = _build_resume_signature(leads, csv_path)
    bookmark_start_cursor = _load_resume_bookmark(run_signature, total)
    start_index = bookmark_start_cursor + 1

    pending_leads = list(enumerate(leads[start_index - 1 :], start_index))
    pending_total = len(pending_leads)

    if start_index > 1:
        print(f"[Bookmark] Continuing from lead #{start_index}")

    print(
        f"\n[Queue] {pending_total} pending / {total} total leads | "
        f"{PARALLEL_COUNT} workers | {len(PROXY_LIST)} proxies\n"
    )

    if pending_total == 0:
        print("[Queue] No pending leads to process.")
        return

    attempted_upto = bookmark_start_cursor
    contiguous_done = bookmark_start_cursor
    completed_out_of_order = set()
    bookmark_lock = asyncio.Lock()

    async def mark_lead_attempted(lead_index: int):
        nonlocal attempted_upto
        if lead_index <= attempted_upto:
            return

        async with bookmark_lock:
            if lead_index <= attempted_upto:
                return

            attempted_upto = lead_index
            await asyncio.to_thread(
                _save_resume_bookmark,
                run_signature,
                total,
                contiguous_done,
                csv_path,
                attempted_upto,
            )

    async def mark_lead_completed(lead_index: int):
        nonlocal contiguous_done, attempted_upto
        if lead_index <= contiguous_done:
            return

        async with bookmark_lock:
            if lead_index <= contiguous_done:
                return

            completed_out_of_order.add(lead_index)
            advanced = False
            while (contiguous_done + 1) in completed_out_of_order:
                completed_out_of_order.remove(contiguous_done + 1)
                contiguous_done += 1
                advanced = True

            if advanced:
                await asyncio.to_thread(
                    _save_resume_bookmark,
                    run_signature,
                    total,
                    contiguous_done,
                    csv_path,
                    attempted_upto,
                )
                print(
                    f"[Bookmark] Saved contiguous={contiguous_done}/{total} "
                    f"cursor={attempted_upto}/{total}"
                )

    async with async_playwright() as p:
        queue: asyncio.Queue = asyncio.Queue()
        for lead_index, lead in pending_leads:
            await queue.put((lead_index, lead))

        async def worker(worker_index: int):
            """Each worker has a fixed index -> fixed proxy slot."""
            prefetch_task = None
            while True:
                if _STOP_FLAG.is_set():
                    if prefetch_task:
                        prefetch_task.cancel()
                    break

                try:
                    lead_index, lead = queue.get_nowait()
                except asyncio.QueueEmpty:
                    if prefetch_task:
                        prefetch_task.cancel()
                    break
                company, url = _extract_lead_company_url(lead, lead_index)
                pitch = subject = None
                if prefetch_task:
                    try:
                        pitch, subject = await prefetch_task
                    except Exception:
                        pitch = subject = None
                    prefetch_task = None

                try:
                    peek = queue._queue[0] if queue.qsize() > 0 else None
                except Exception:
                    peek = None
                if peek:
                    _, next_lead = peek
                    next_company = (
                        next_lead.get("Company Name") or next_lead.get("name") or
                        next_lead.get("company") or "NextCompany"
                    )
                    prefetch_task = asyncio.ensure_future(
    asyncio.to_thread(generate_ai_pitch_and_subject, next_company, worker_index),
                        
                    )

                await mark_lead_attempted(lead_index)
                print(f"\n[Worker#{worker_index}] [{lead_index}/{total}] {company[:35]} | proxy-slot={worker_index % len(PROXY_LIST)}")
                await process_form(p, company, url, sheet, lead_index, total,
                                   _pitch=pitch, _subject=subject,
                                   worker_index=worker_index)

                if _STOP_FLAG.is_set():
                    print(f"[Worker#{worker_index}] Stop flag active - lead #{lead_index} not bookmarked")
                else:
                    await mark_lead_completed(lead_index)

                queue.task_done()
                print(f"[Worker#{worker_index}] Done [{lead_index}/{total}] | {queue.qsize()} left")

                if _STOP_FLAG.is_set():
                    if prefetch_task:
                        prefetch_task.cancel()
                    break

        # Spawn workers with explicit indices 0-9
        workers = [
            asyncio.create_task(worker(i))
            for i in range(min(PARALLEL_COUNT, pending_total))
        ]
        await asyncio.gather(*workers)

    if not _STOP_FLAG.is_set() and contiguous_done >= total:
        _clear_resume_bookmark()
        print("[Bookmark] Run completed - bookmark cleared")
    else:
        print(
            f"[Bookmark] Current progress: contiguous={contiguous_done}/{total} "
            f"cursor={attempted_upto}/{total}"
        )

    print()
    print("=" * 65)
    print("  ALL DONE")
    print(f"  Attempted this run: {max(0, attempted_upto - bookmark_start_cursor)} leads")
    print(f"  Resume cursor    : {attempted_upto}/{total}")
    print(f"  Contiguous done  : {contiguous_done}/{total}")
    print(f"  Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print("=" * 65)
    token_tracker.print_summary()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[CTRL+C] Stop flag set - waiting for threads to exit...")
        _STOP_FLAG.set()
        import time
        time.sleep(3)   # give threads 3 seconds to see the flag and exit
        print("[CTRL+C] Exited cleanly.")

