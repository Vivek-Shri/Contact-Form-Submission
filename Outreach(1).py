import asyncio
import csv
import sys
import random
import re

# Force UTF-8 stdout on Windows to avoid 'charmap' codec errors with Unicode chars
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
import requests
import json
import threading
from datetime import datetime
from openai import OpenAI
from playwright.async_api import async_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
#   UNIVERSAL CONTACT FORM FILLER — v4
#   Changes from v3:
#   - 10 proxies, each handles ~5 sites (round-robin by worker index)
#   - NopeCHA hard timeout: 120s (was 3600s)
#   - Narrow viewport (500px wide) for better form isolation
#   - gpt-5-nano kept for pitch/subject
# ============================================================

OPENAI_API_KEY     = "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"
SPREADSHEET_ID     = "1jSfdjqQXgueTfatP10R3mIxr_Kee0zNN0tVEXq59rYE"
CREDS_FILE         = "google_credentials.json"
NOPECHA_API_KEYS   = [
    "sub_1TE68RCRwBwvt6ptOHR2oZ2o",      # Key 1
    "sub_1TGgdMCRwBwvt6pt1NbN2LQx",      # Key 2 (20k solves/day)
]
NOPECHA_API_KEY    = NOPECHA_API_KEYS[0]  # default for balance checks
_nopecha_key_idx   = 0                    # round-robin counter
_nopecha_key_lock  = threading.Lock()

def _next_nopecha_key():
    """Round-robin between NopeCHA API keys."""
    global _nopecha_key_idx
    with _nopecha_key_lock:
        key = NOPECHA_API_KEYS[_nopecha_key_idx % len(NOPECHA_API_KEYS)]
        _nopecha_key_idx += 1
    return key

MY_FIRST_NAME    = "Uttam Kumar"
MY_LAST_NAME     = "Tiwari"
MY_FULL_NAME     = "Uttam Kumar Tiwari"
MY_EMAIL         = "info@hyperstaff.co"
MY_PHONE         = "646-798-9403"
MY_PHONE_INTL    = "+1646-798-9403"
MY_PHONE_DISPLAY = "+1 (646) 798-9403"
MY_COMPANY       = "HyperStaff"
MY_WEBSITE       = "https://hyperstaff.co"

PARALLEL_COUNT = 10   # 10 workers, one per proxy

# ── Narrow viewport: isolates form sections like the RXR screenshot ──
VIEWPORT_WIDTH  = 500   # narrow = fewer distractions, form columns stack vertically
VIEWPORT_HEIGHT = 900

SHEET_HEADERS = [
    "Company Name", "Contact URL", "Submitted", "Submission Assurance",
    "Captcha Status", "Proxy Used", "Bandwidth (KB)",
    "Run Timestamp", "API Calls", "Input Tokens", "Output Tokens",
    "Total Tokens", "Est. Cost (USD)", "Avg Tokens/Call",
    "Fields Filled",
    "Submission Status",
    "Confirmation Msg",
    "Message Sent",
]

# tok_cols layout from get_token_columns():
#   [0]=timestamp [1]=calls [2]=input [3]=output [4]=total [5]=cost [6]=avg
# _build_row base has 7 fields (A-G), then tok_cols[0..6] fills H-N = 7 more = 14 total ✓

COST_PER_1M_INPUT  = 0.150
COST_PER_1M_OUTPUT = 0.600
TOKEN_LOG_FILE     = "token_usage.csv"

# ── HARD TOKEN LIMITS PER CALL ──────────────────────────────
MAX_INPUT_TOKENS  = 5000
MAX_OUTPUT_TOKENS = 500

# ── NopeCHA hard timeout (seconds) ──────────────────────────
NOPECHA_HARD_TIMEOUT = 300   # ← was 3600; now 2 minutes then give up

# ── React/Vue fill JS ────────────────────────────────────────
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

        // Skip search bars by name/id/placeholder
        var nm = (el.name || el.id || el.placeholder || '').toLowerCase();
        if (/\bsearch\b|sf_s|zip.?code|keyword|flexdata/.test(nm)) return;
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
        if (depth === 0 && rect.width === 0 && rect.height === 0) return;

        var id = el.id || '';
        var name = el.name || '';
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
        pt  = getattr(usage, "prompt_tokens",     0) or 0
        ct  = getattr(usage, "completion_tokens", 0) or 0
        tt  = getattr(usage, "total_tokens",      0) or 0
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
    def get_snapshot(self, worker_index: int) -> dict:
        with self._lock:
            w = self.worker_totals.get(worker_index, {"input":0,"output":0,"calls":0})
        return {
            "input": w["input"],
            "output": w["output"],
            "calls": w["calls"],
        }

    def get_delta_columns(self, before: dict) -> list:
        """Sirf is ek lead ke tokens — cumulative nahi"""
        with self._lock:
            d_in    = self.total_input  - before["input"]
            d_out   = self.total_output - before["output"]
            d_tot   = d_in + d_out
            d_calls = self.total_calls  - before["calls"]
            d_cost  = (d_in * COST_PER_1M_INPUT + d_out * COST_PER_1M_OUTPUT) / 1_000_000
            avg     = (d_tot // d_calls) if d_calls else 0
        return [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            d_calls, d_in, d_out, d_tot,
            round(d_cost, 6), avg,
        ]

token_tracker = TokenTracker()
_STOP_FLAG = threading.Event()


# ============================================================
#   10 PROXIES — each worker gets a dedicated proxy
#   Workers are assigned by index: worker_index % 10
#   ~5 sites per proxy when running 10 workers over 50 sites
# ============================================================

# All 10 proxy slots — fill in your actual proxy credentials below.
# Format: (host, port, username, password, label)
# Using Webshare rotate as the base; replace entries with distinct
# proxy IPs/credentials once you have 10 separate proxy accounts.
PROXY_LIST = [
    # slot 0
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-0"),
    # slot 1
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-1"),
    # slot 2
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-2"),
    # slot 3
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-3"),
    # slot 4
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-4"),
    # slot 5
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-5"),
    # slot 6
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-6"),
    # slot 7
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-7"),
    # slot 8
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-8"),
    # slot 9
    ("p.webshare.io", 80, "nehybklk-rotate",  "hgu3dl519zlc", "webshare-9"),
    # ──────────────────────────────────────────────────────────
    # TO USE REAL DISTINCT PROXIES: replace rows above like:
    # ("123.45.67.89",  8080, "user1", "pass1", "proxy-DE-1"),
    # ("98.76.54.32",   8080, "user2", "pass2", "proxy-US-1"),
    # etc.
]

def get_proxy_for_worker(worker_index: int) -> tuple[dict, str]:
    """
    Assign a proxy deterministically by worker index.
    Worker 0 → PROXY_LIST[0], Worker 1 → PROXY_LIST[1], …, Worker 9 → PROXY_LIST[9].
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

# NopeCHA proxy payload — uses slot 0 by default for captcha solving
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

openai_client   = OpenAI(api_key=OPENAI_API_KEY)
SKYVERN_ENABLED = False
SKYVERN_API_URL = "http://localhost:8080"
SKYVERN_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjQ5MTgzNTU5NjUsInN1YiI6Im9fNTA0MzgxODc4MzkxODExMTA4In0.KLz3KLwZjXJ__OkUlGLuNZa34qcchvxMqb-MA9UPzNg"

sheet_lock = asyncio.Lock()


# ============================================================
#   ROW BUILDER
# ============================================================

def _build_row(company_name, url, submitted, assurance,
               captcha_status, proxy_label, bw_kb, token_cols=None,
               filled_fields=None, sub_status="", confirmation_msg="",
               message_sent="") -> list:
    base = [str(company_name), str(url), str(submitted), str(assurance),
            str(captcha_status), str(proxy_label), str(bw_kb)]
    if token_cols and len(token_cols) == 7:
        tok = [str(v) for v in token_cols]
    else:
        tok = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
               "0","0","0","0","0.000000","0"]
    if filled_fields and isinstance(filled_fields, dict) and len(filled_fields) > 0:
        parts = []
        for k, v in filled_fields.items():
            if not v or not str(v).strip():
                continue
            short_k = re.sub(r'input\[name=["\']([^"\']+)["\']\]', r'\1', k)
            short_k = re.sub(r'textarea\[name=["\']([^"\']+)["\']\]', r'\1', short_k)
            short_k = short_k.lstrip('#').strip()
            short_v = str(v)[:200]  # 200 chars per field so messages aren't cut
            parts.append(f"{short_k}: {short_v}")
        fields_str = " | ".join(parts[:15])  # up to 15 fields shown
    else:
        fields_str = "—"
    msg_str = str(message_sent) if message_sent else "—"
    return base + tok + [fields_str, str(sub_status or "—"), str(confirmation_msg or "—")[:300], msg_str]


def _emit_result(company_name, url, submitted, assurance, captcha_status,
                 proxy_label, bw_kb, tok_cols=None, filled_fields=None,
                 sub_status="", confirmation_msg="", message_sent=""):
    """Print a [RESULT] JSON line to stdout for the dashboard to capture."""
    if tok_cols and len(tok_cols) == 7:
        tc = tok_cols
    else:
        tc = ["", 0, 0, 0, 0, 0, 0]
    fields_str = ""
    if filled_fields and isinstance(filled_fields, dict):
        parts = []
        for k, v in filled_fields.items():
            if not v or not str(v).strip():
                continue
            short_k = re.sub(r'input\[name=["\']([^"\']+)["\']\]', r'\1', k)
            short_k = re.sub(r'textarea\[name=["\']([^"\']+)["\']\]', r'\1', short_k)
            short_k = short_k.lstrip('#').strip()
            parts.append(f"{short_k}: {str(v)[:40]}")
        fields_str = " | ".join(parts[:10])
    result_obj = {
        "company_name": str(company_name),
        "contact_url": str(url),
        "submitted": str(submitted),
        "submission_assurance": str(assurance)[:300],
        "captcha_status": str(captcha_status),
        "proxy_used": str(proxy_label),
        "bandwidth_kb": str(bw_kb),
        "run_timestamp": str(tc[0]),
        "api_calls": str(tc[1]),
        "input_tokens": str(tc[2]),
        "output_tokens": str(tc[3]),
        "total_tokens": str(tc[4]),
        "est_cost": str(tc[5]),
        "avg_tokens_call": str(tc[6]),
        "fields_filled": fields_str or "—",
        "submission_status": str(sub_status or submitted),
        "confirmation_msg": str(confirmation_msg or assurance)[:300],
        "message_sent": str(message_sent)[:500] if message_sent else "—",
    }
    print(f"[RESULT] {json.dumps(result_obj)}")


# ============================================================
#   SAFE SHEET WRITE
# ============================================================

async def safe_append_row(sheet, row: list, max_retries=6):
    expected = len(SHEET_HEADERS)
    if len(row) != expected:
        row = (row + [""] * expected)[:expected]

    print(f"   [Sheets] Writing: {str(row[0])[:30]}")
    delay = 2
    for attempt in range(max_retries):
        try:
            async with sheet_lock:
                await asyncio.to_thread(sheet.append_row, row, value_input_option="USER_ENTERED")
            print(f"   [Sheets] OK: {str(row[0])[:25]}")
            return True
        except gspread.exceptions.APIError as e:
            if "429" in str(e) or "Quota" in str(e):
                wait = min(delay * (2 ** attempt), 60) + random.uniform(0, 2)
                print(f"   [Sheets] 429 — retry in {wait:.1f}s")
                await asyncio.sleep(wait)
            else:
                print(f"   [Sheets] API error attempt {attempt+1}: {e}")
                await asyncio.sleep(delay)
        except Exception as e:
            print(f"   [Sheets] Write error attempt {attempt+1}: {type(e).__name__}: {e}")
            await asyncio.sleep(delay)

    try:
        import os as _os
        fpath     = "fallback_rows.csv"
        write_hdr = not _os.path.exists(fpath)
        with open(fpath, "a", newline="", encoding="utf-8") as f:
            if write_hdr:
                csv.writer(f).writerow(SHEET_HEADERS)
            csv.writer(f).writerow(row)
        print(f"   [Sheets] Saved to fallback_rows.csv")
    except Exception as fe:
        print(f"   [Sheets] Fallback failed: {fe}")
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


def _make_route_handler(main_host: str, bw: dict):
    from urllib.parse import urlparse as _up

    async def _handler(route, request):
        req_url = request.url
        rtype   = request.resource_type
        url_low = req_url.lower()

        # Always allow documents (HTML pages), stylesheets, and fonts
        if rtype in {"document", "stylesheet", "font"}:
            bw["allowed"] += 1
            await route.continue_()
            return

        try:
            req_host = _up(req_url).hostname or ""
        except Exception:
            req_host = ""

        is_main = (req_host == main_host or req_host.endswith("." + main_host))

        # Always allow everything from the main site
        if is_main:
            bw["allowed"] += 1
            await route.continue_()
            return

        reason = None

        # Only block heavy media (video/audio) — NOT images
        if rtype == "media":
            reason = "media"

        # Block only KNOWN tracker/analytics hosts (not all third-party JS)
        elif rtype == "script":
            blocked = any(req_host == d or req_host.endswith("." + d)
                          for d in _BLOCKED_JS_HOSTS)
            if blocked:
                reason = f"js-tracker:{req_host}"
            # Allow ALL other scripts — forms need them to render

        elif rtype in {"xhr", "fetch"}:
            if any(kw in url_low for kw in _BLOCKED_XHR_PATHS):
                reason = "tracker-xhr"
            else:
                blocked = any(req_host == d or req_host.endswith("." + d)
                              for d in _BLOCKED_JS_HOSTS)
                if blocked:
                    reason = f"xhr:{req_host}"

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
            cl = response.headers.get("content-length")
            if cl:
                bw["bytes"] += int(cl)
            else:
                u = response.url.lower()
                if any(u.endswith(e) for e in (".js", ".mjs")):
                    bw["bytes"] += 30_000
                elif u.endswith(".css"):
                    bw["bytes"] += 15_000
                else:
                    bw["bytes"] += 5_000
        except Exception:
            pass
    return _on_response


# ============================================================
#   NOPECHA — hard timeout 120 seconds
# ============================================================

_nopecha_semaphore: asyncio.Semaphore = None

def _make_nopecha_semaphore():
    global _nopecha_semaphore
    _nopecha_semaphore = asyncio.Semaphore(3)


def _nopecha_token_api(cap_type: str, sitekey: str, url: str) -> str | None:
    import time as _t

    API          = "https://api.nopecha.com/token"
    POLL_SECS    = 5
    POST_RETRY   = 5                    # fewer retries since timeout is tight
    HARD_TIMEOUT = NOPECHA_HARD_TIMEOUT  # ← 120 seconds
    JOB_TTL      = 90                   # re-submit job if no answer within 90s
    kw           = {"timeout": 15}      # shorter network timeout too

    try:
        r      = requests.get("https://api.nopecha.com/status",
                              params={"key": NOPECHA_API_KEY}, timeout=8)
        data   = r.json()
        credit = data.get("credit", (data.get("data") or {}).get("credit", -1))
        if credit == 0:
            print(f"   [NopeCHA] No credits — skipping")
            return None
        print(f"   [NopeCHA] Credits: {credit}")
    except Exception as e:
        print(f"   [NopeCHA] Balance check failed ({e}) — proceeding")

    # Pick a key for this captcha solve via round-robin
    nopecha_key = _next_nopecha_key()
    print(f"   [NopeCHA] Using key ...{nopecha_key[-6:]}")
    print(f"   [NopeCHA] Submitting {cap_type} | timeout={HARD_TIMEOUT}s | url={url[:50]}")
    deadline = _t.time() + HARD_TIMEOUT
    payload  = {
        "key": nopecha_key, "type": cap_type,
        "sitekey": sitekey, "url": url, "proxy": NOPECHA_PROXY_PAYLOAD,
    }
    if cap_type == "recaptcha3":
        payload["action"] = "submit"
        payload["score"]  = 0.7

    def _submit():
        for i in range(1, POST_RETRY + 1):
            if _STOP_FLAG.is_set():           # ← ADD THIS
                return None
            if _t.time() > deadline:
                print(f"   [NopeCHA] Deadline hit during submit attempt {i}")
                return None
            try:
                r = requests.post(API, json=payload, **kw)
                d = r.json()
                if d.get("data"):
                    return d["data"]
                ec = d.get("error", "")
                print(f"   [NopeCHA] POST {i} err={ec}")
                if ec in (10, 11, "10", "11"):
                    return None
                _t.sleep(1.0)
            except Exception as e:
                print(f"   [NopeCHA] POST {i} network error: {e}")
                _t.sleep(1.0)
        return None

    job_id = _submit()
    if not job_id:
        print(f"   [NopeCHA] Could not submit job — giving up")
        return None

    job_at  = _t.time()
    polls   = 0
    while _t.time() < deadline:
        # ← CHECK STOP FLAG — exit immediately on Ctrl+C
        if _STOP_FLAG.is_set():
            print(f"   [NopeCHA] Stop flag set — aborting poll immediately")
            return None

        remaining = deadline - _t.time()
        print(f"   [NopeCHA] Polling job={job_id[:12]}… polls={polls} remaining={remaining:.0f}s")
        
        # Sleep in small chunks so stop flag is checked frequently
        for _ in range(POLL_SECS):
            if _STOP_FLAG.is_set():
                print(f"   [NopeCHA] Stop flag set during sleep — aborting")
                return None
            _t.sleep(1)
        
        polls += 1

        if _t.time() > deadline:
            print(f"   [NopeCHA] Hard timeout — terminating")
            return None

        elapsed_job = int(_t.time() - job_at)
        if elapsed_job >= JOB_TTL:
            print(f"   [NopeCHA] Job TTL hit ({elapsed_job}s) — resubmitting")
            nj = _submit()
            if nj:
                job_id = nj; job_at = _t.time()
            continue

        try:
            r  = requests.get(API, params={"key": nopecha_key, "id": job_id}, **kw)
            d  = r.json()
            tk = d.get("data")
            ec = d.get("error", 0)
            if tk and isinstance(tk, str) and len(tk) > 20:
                print(f"   [NopeCHA] Solved in {int(_t.time()-deadline+HARD_TIMEOUT)}s")
                return tk
            if ec in (10, "10"):
                nj = _submit()
                if nj:
                    job_id = nj; job_at = _t.time()
        except Exception as e:
            print(f"   [NopeCHA] Poll error: {e}")

    print(f"   [NopeCHA] Hard 120s timeout reached — giving up on captcha")
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


# ============================================================
#   CAPTCHA DETECTION & SOLVING
# ============================================================

async def detect_and_solve_captcha(page, iframe=None):
    async with _nopecha_semaphore:
        html     = await page.content()
        page_url = page.url
        for frame in page.frames:
            try:
                if frame.url and frame.url.startswith("http"):
                    html += await frame.content()
            except Exception:
                pass

        has_hcaptcha  = bool(re.search(r'h-captcha|hcaptcha\.com/1/api', html, re.I))
        has_recaptcha = bool(
            re.search(r'class=["\'][^"\']*g-recaptcha', html, re.I) or
            re.search(r'data-sitekey', html, re.I) or
            re.search(r'recaptcha/api2/anchor|recaptcha/enterprise/anchor', html, re.I) or
            re.search(r'grecaptcha\.render\s*\(', html, re.I)
        )
        has_turnstile = bool(re.search(r'cf-turnstile|challenges\.cloudflare\.com/turnstile', html, re.I))

        if not (has_hcaptcha or has_recaptcha or has_turnstile):
            print("   [Captcha] None detected")
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

        sitekey = None
        for sel in ["[data-sitekey]",".g-recaptcha",".h-captcha","[class*='cf-turnstile']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    for attr in ["data-sitekey","data-hcaptcha-sitekey","data-recaptcha-sitekey"]:
                        sk = await el.get_attribute(attr)
                        if sk and len(sk) > 10:
                            sitekey = sk.strip()
                            break
                if sitekey:
                    break
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
                        break
                except Exception:
                    pass

        if not sitekey:
            for pat in [
                r'data-sitekey=["\']([A-Za-z0-9_-]{20,})["\']',
                r'"sitekey"\s*:\s*"([A-Za-z0-9_-]{20,})"',
            ]:
                m = re.search(pat, html, re.I)
                if m:
                    sitekey = m.group(1).strip()
                    break

        if not sitekey:
            print(f"   [Captcha] No sitekey found")
            return True, f"{cap_type}-no-sitekey"

        token = await asyncio.to_thread(_nopecha_token_api, cap_type, sitekey, page_url)
        if token:
            await _inject_token(page, str(token), cap_type)
            print(f"   [Captcha] Solved: {cap_type}")
            return True, f"{cap_type}-solved-NopeCHA"
        else:
            print(f"   [Captcha] Timeout/failed after {NOPECHA_HARD_TIMEOUT}s")
            return True, f"{cap_type}-timeout-{NOPECHA_HARD_TIMEOUT}s"


# ============================================================
#   AI PITCH & SUBJECT — gpt-5-nano (KEPT as requested)
# ============================================================

# ============================================================
#    AI PITCH & SUBJECT — gpt-5-nano (Fixed UnboundLocalError)
# ============================================================

def generate_ai_pitch_and_subject(company_name, worker_index=-1):
    # Use company_name as the greeting; it could be a person name or company name
    greeting = company_name if company_name else "there"

    subject = f"Virtual Assistant Support for {greeting} — {MY_COMPANY}"

    message = (
        f"Hi {greeting},\n\n"
        f"I'm from {MY_COMPANY}, and I'll keep this brief — we provide pre-trained "
        f"Virtual Assistants who are ready to work in your time zone and take your "
        f"backend operations completely off your plate.\n\n"
        f"The result? You get to focus 100% on scaling your business and closing more deals.\n\n"
        f"We can have the right VA matched and ready for you within 24–48 hours.\n\n"
        f"Would love to show you how it works — are you open to a quick chat?\n"
        f"Looking forward to connecting!"
    )

    return message, subject
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

    print(f"   [FormFinder] All strategies failed — fallback main")
    return page, "fallback-main"


# ============================================================
#   GPT PROMPT BUILDER
# ============================================================

def _build_gpt_prompt(company_name, pitch, subject, page_text, elements) -> str:
    page_ctx   = page_text[:200] if page_text else ""
    field_lines = []
    for el in elements[:15]:
        opts = ""
        if el.get("options"):
            opts_str = "|".join(el["options"][:5])
            opts = f" OPTS:[{opts_str}]"
        field_lines.append(
            f"`{el['sel']}` {el['tag']} {el.get('type','')} \"{el['label']}\"{opts}"
        )
    fields_block = "\n".join(field_lines)

    prompt = f"""Fill contact form for {company_name}.

SENDER: Name={MY_FULL_NAME} Email={MY_EMAIL} Phone={MY_PHONE} PhoneIntl={MY_PHONE_INTL} Company={MY_COMPANY} Website={MY_WEBSITE} Subject={subject[:80]} Budget=10000 Country=India

MESSAGE: {pitch[:200]}

PAGE: {page_ctx}

PHONE RULES: label has "intl"/"+"/maxlen>10 → {MY_PHONE_INTL} else → {MY_PHONE}
SELECT RULES: For "reason/inquiry/subject/type" dropdowns → pick most relevant option like "General Inquiry" or "Other". For "country" → "India". For "budget/range" → highest option.
FIELDS (fill ALL):
{fields_block}

Reply ONLY JSON array:
[{{"action":"fill","selector":"...","value":"..."}},{{"action":"select","selector":"...","value":"..."}},{{"action":"check","selector":"..."}},{{"action":"done"}}]"""

    return prompt


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
                    if (['hidden','submit','button'].includes(el.type)) return;
                    let lbl = el.getAttribute('aria-label') || el.placeholder || el.name || '';
                    let sel = el.id ? '#' + CSS.escape(el.id) :
                              el.name ? el.tagName.toLowerCase() + '[name="' + el.name + '"]' :
                              el.tagName.toLowerCase() + ':nth-of-type(' + (idx+1) + ')';
                    let opts = [];
                    if (el.tagName === 'SELECT') Array.from(el.options).slice(0,6).forEach(o => {
                        if (o.text.trim()) opts.push(o.text.trim());
                    });
                    res.push({sel, label: lbl.slice(0,40), tag: el.tagName.toLowerCase(),
                              type: el.type||'', name: el.name||'', options: opts, y: 0});
                });
                return res;
            }""")
        except Exception as e2:
            print(f"   [{company_name[:20]}] [GPT] Fallback extractor failed: {e2}")

    if not elements:
        print(f"   [{company_name[:20]}] [GPT] No fields — JS fallback")
        fb = await _js_fallback_fill(page, company_name, pitch, subject)
        return fb, {}

    print(f"   [{company_name[:20]}] [GPT] {len(elements)} fields found")

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
    print(f"   [{company_name[:20]}] [GPT] Prompt ~{len(prompt)//4} tokens")

    try:
        resp = await asyncio.to_thread(
            openai_client.chat.completions.create,
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=MAX_OUTPUT_TOKENS,
        )
        if resp.usage:
            token_tracker.record(company_name, "form_fill", resp.usage, worker_index)

        raw = resp.choices[0].message.content.strip()
        m = re.search(r'```(?:json)?\s*([\s\S]*?)```', raw)
        if m:
            raw = m.group(1).strip()
        raw = re.sub(r'^[^[{]*', '', raw)
        raw = re.sub(r'[^}\]]*$', '', raw)
        actions = json.loads(raw)
        if not isinstance(actions, list):
            actions = [actions]
        print(f"   [{company_name[:20]}] [GPT] {len(actions)} actions planned")
    except Exception as e:
        print(f"   [{company_name[:20]}] [GPT] Parse error: {e} — JS fallback")
        fb = await _js_fallback_fill(page, company_name, pitch, subject)
        return fb, {}

    # Execute actions
    for act in actions:
        action   = act.get("action", "")
        selector = act.get("selector", "")
        value    = str(act.get("value", ""))

        if action == "done":
            break
        if not selector:
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
            print(f"   [{company_name[:20]}] - not found: {selector[:40]}")
            continue

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
                handle = await el.element_handle()
                filled = False
                if handle:
                    try:
                        await page.evaluate(
                            f"([el, v]) => {{ {REACT_FILL_JS}(el, v); }}",
                            [handle, value]
                        )
                        filled = True
                    except Exception:
                        pass
                if not filled:
                    try:
                        await el.click(force=True)
                        await el.fill("")
                        await el.fill(value)
                        filled = True
                    except Exception:
                        pass
                if filled:
                    print(f"   [{company_name[:20]}] + fill {selector[:35]} = {value[:25]}")
                    filled_values[selector] = value 
                    total_filled += 1

            elif action == "select":
                filled = False
                for method in [
                    lambda: el.select_option(label=value),
                    lambda: el.select_option(value=value),
                ]:
                    try:
                        await method()
                        filled = True
                        break
                    except Exception:
                        pass
                if not filled:
                    try:
                        for opt in await el.locator("option").all():
                            t = (await opt.inner_text()).strip()
                            if t and not re.match(r'^(--|choose|select|please)', t, re.I):
                                await el.select_option(label=t)
                                filled = True
                                break
                    except Exception:
                        pass
                if filled:
                    print(f"   [{company_name[:20]}] + select {selector[:35]} = {value[:25]}")
                    filled_values[selector] = value 
                    total_filled += 1

            elif action == "check":
                try:
                    if not await el.is_checked():
                        await el.check(force=True)
                except Exception:
                    await el.click(force=True)
                print(f"   [{company_name[:20]}] + check {selector[:35]}")
                filled_values[selector] = "checked"   # ← ADD THIS
                total_filled += 1


            elif action == "click":
                await el.click(force=True)
                await asyncio.sleep(0.5)

        except Exception as e:
            print(f"   [{company_name[:20]}] - skip {selector[:35]}: {str(e)[:40]}")

    print(f"   [{company_name[:20]}] [GPT] Done — {total_filled} filled")

    if total_filled == 0:
        print(f"   [{company_name[:20]}] GPT filled 0 — JS fallback...")
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
                if (/\bsearch\b|sf_s|zip.?code|keyword|flexdata/.test(nm2)) return;
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
                    if (opts.length) {{ el.value=opts[0].value; el.dispatchEvent(new Event('change',{{bubbles:true}})); n++; }} return;
                }}
                if (el.type === 'checkbox') {{ if(!el.checked){{el.checked=true;n++;}} return; }}
                if (el.type === 'radio') return;
                if (el.type==='email'||h.includes('email')) {{ RF(el,'{MY_EMAIL}'); return; }}
                if (h.includes('phone')||h.includes('mobile')||h.includes('tel')) {{ RF(el,h.includes('intl')||h.includes('+') ? '{MY_PHONE_INTL}' : '{MY_PHONE}'); return; }}
                if (h.includes('first')&&h.includes('name')) {{ RF(el,'{MY_FIRST_NAME}'); return; }}
                if (h.includes('last')&&h.includes('name')) {{ RF(el,'{MY_LAST_NAME}'); return; }}
                if (h.includes('name')||h.includes('contact')) {{ RF(el,'{MY_FULL_NAME}'); return; }}
                if (h.includes('company')||h.includes('org')) {{ RF(el,'{MY_COMPANY}'); return; }}
                if (h.includes('website')||h.includes('url')||h.includes('site')) {{ RF(el,'{MY_WEBSITE}'); return; }}
                if (h.includes('subject')||h.includes('topic')) {{ RF(el,`{subject_e}`); return; }}
                if (h.includes('budget')||h.includes('amount')) {{ RF(el,'10000'); return; }}
                if (h.includes('country')) {{ RF(el,'India'); return; }}
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


# ============================================================
#   CHECKBOXES
# ============================================================

async def handle_checkboxes(target):
    try:
        cbs = target.locator("input[type='checkbox']")
        cnt = await cbs.count()
        for i in range(cnt):
            cb = cbs.nth(i)
            try:
                if await cb.is_visible() and not await cb.is_checked():
                    await cb.scroll_into_view_if_needed()
                    await cb.check()
            except Exception:
                continue
    except Exception:
        pass


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

    # ── Strategy A: text-match buttons ──────────────────────
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
                                if(['nav','header','footer'].includes((p.tagName||'').toLowerCase())) return true;
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
                    await btn.scroll_into_view_if_needed()
                    await asyncio.sleep(0.3)
                    await btn.click()
                    print(f"   {tag} [Submit] ✓ text: '{text}'")
                    return True, f"text:'{text}'"
            except Exception:
                continue

    # ── Strategy B: CSS selector ─────────────────────────────
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
        "a[class*='btn' i][href='#']",
    ]:
        for src in [target, page]:
            try:
                el = src.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.scroll_into_view_if_needed()
                    await el.click()
                    print(f"   {tag} [Submit] ✓ sel: {sel}")
                    return True, f"sel:{sel}"
            except Exception:
                continue

    # ── Strategy C: iframes ──────────────────────────────────
    for frame in page.frames:
        try:
            if not frame.url or not frame.url.startswith("http"):
                continue
            for sel in [
                "button[type='submit']","input[type='submit']",
                ".hs-button","button:has-text('Submit')","button:has-text('Send')",
            ]:
                el = frame.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.scroll_into_view_if_needed()
                    await el.click()
                    print(f"   {tag} [Submit] ✓ iframe sel: {sel}")
                    return True, f"iframe:{sel}"
        except Exception:
            continue

    # ── Strategy D: JS nuclear — logs ALL buttons before clicking ──
    all_buttons_info = await page.evaluate("""() => {
        const info = [];
        document.querySelectorAll('button, input[type="submit"], a[role="button"]').forEach(el => {
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
        print(f"      → type={b['typ']} visible={b['visible']} text='{b['txt']}' cls='{b['cls']}'")

    clicked = await page.evaluate("""() => {
        const keywords = ['submit','send','contact','enquire','book','register','go','ok','apply','reach','connect','message','talk','start','quote','request'];
        const sels = [
            'form button','button[type="submit"]','input[type="submit"]',
            '.elementor-button','.wpforms-submit','input.wpcf7-submit',
            '.gform_button','button[class*="submit"]','button[class*="send"]',
            'button[class*="btn"]','button','a[role="button"]'
        ];
        for (const sel of sels) {
            for (const el of document.querySelectorAll(sel)) {
                const rc = el.getBoundingClientRect();
                if (rc.width < 1 || rc.height < 1) continue;
                const txt = (el.innerText || el.value || el.textContent || '').trim().toLowerCase();
                const cls = (el.className || '').toLowerCase();
                let p=el.parentElement, inNav=false;
                while(p){
                    if(['nav','header','footer'].includes((p.tagName||'').toLowerCase())){inNav=true;break;}
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
        print(f"   {tag} [Submit] ✓ JS nuclear: {clicked}")
        return True, f"js:{clicked}"

    # ── Strategy E: last resort — ANY visible button inside <form> ──
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
        print(f"   {tag} [Submit] ✓ Last resort: {last_resort}")
        return True, f"last-resort:{last_resort}"

    print(f"   {tag} [Submit] ✗ NO submit button found anywhere on page")
    return False, "not_found"


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
THANKYOU_URL_FRAGS = [
    "thank","thanks","success","confirmed","submitted","sent","done","received",
]


async def get_confirmation(page, target=None, original_url="", company_name="", worker_index=-1):
    tag = f"[{company_name[:20]}]"

    # ── Cookie/popup dismiss karo PEHLE ─────────────────────
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

    for poll in range(15):   # ← baaki same code
        await asyncio.sleep(1)

        # ── Print current URL on every poll ─────────────────
        cur = page.url
        print(f"   {tag} [Confirm] poll={poll+1} url={cur[:80]}")

        # ── URL-redirect check ───────────────────────────────
        if original_url and cur.lower() != original_url.lower():
            if any(f in cur.lower() for f in THANKYOU_URL_FRAGS):
                print(f"   {tag} [Confirm] ✓ thank-you URL redirect: {cur}")
                return "Yes", f"Redirected to: {cur}"
            # After poll 2, any URL change = form likely submitted (redirect to new page)
            if poll >= 2:
                print(f"   {tag} [Confirm] ✓ URL changed after submit (poll {poll+1}): {cur}")
                return "Yes", f"Redirected to: {cur}"

        # ── DOM selector check ───────────────────────────────
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
                                print(f"   {tag} [Confirm] ✓ DOM sel='{sel}' text='{text[:120]}'")
                                first = re.split(r'[.\n!]', text)[0].strip()
                                msg   = first if len(first) >= 5 else text[:200]
                                return "Yes", msg
                            elif text:
                                print(f"   {tag} [Confirm] DOM sel='{sel}' (ignored len={len(text)}): '{text[:60]}'")
                        except Exception:
                            continue
                except Exception:
                    continue

        # ── Full visible-text scan ───────────────────────────
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

            # ── Always print first 500 chars of page text ───
            if vt:
                print(f"   {tag} [Confirm] PAGE TEXT (poll {poll+1}): {vt[:500]}")

            if vt:
                for kw in SUCCESS_KEYWORDS:
                    if kw in vt.lower():
                        segs    = [s.strip() for s in vt.split('|')]
                        matched = next((s for s in segs if kw in s.lower() and 8 < len(s) < 300), None)
                        if matched:
                            first = re.split(r'[.\n!]', matched)[0].strip()
                            msg   = first if len(first) >= 8 else matched[:200]
                            print(f"   {tag} [Confirm] ✓ keyword='{kw}' → '{msg[:80]}'")
                            return "Yes", msg
        except Exception as ex:
            print(f"   {tag} [Confirm] page-text eval error: {ex}")

    # ── GPT fallback: use 3000 chars so GPT gets full page ───
    # ── GPT fallback — SIRF tab jab URL nahi badla ───────────
    print(f"   {tag} [Confirm] All polls done...")
    try:
        final_url   = page.url
        url_changed = original_url and final_url.lower() != original_url.lower()

        # ── Agar URL badla → form submit hua, GPT call mat karo ──
        if url_changed:
            print(f"   {tag} [Confirm] URL changed → Yes (no GPT needed)")
            return "Yes", f"Redirected to: {final_url}"

        # ── URL nahi badla → tab hi GPT se check karo ────────
        body_text = await page.evaluate("() => document.body.innerText")
        snippet   = body_text[:800]   # 3000 se ghatake 800 — tokens bachao

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
        return "No", "No confirmation detected"

    except Exception as e:
        print(f"   {tag} [Confirm] Error: {e}")

    return "No", "No confirmation signal detected"


# ============================================================
#   GOOGLE SHEETS
# ============================================================

def get_sheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    gc    = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def get_status_sheet():
    wb    = get_sheet_client()
    sheet = wb.sheet1
    existing = sheet.row_values(1)
    if existing != SHEET_HEADERS:
        print(f"[Sheets] Writing headers to Sheet1...")
        sheet.update([SHEET_HEADERS], "A1")
    else:
        print(f"[Sheets] Sheet1 headers OK")
    return sheet


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
                       attempt=1, _pitch=None, _subject=None, worker_index=0):
    local_tokens = {"input": 0, "output": 0, "calls": 0}

    lead_snapshot = token_tracker.get_snapshot(worker_index)  # ← LINE 1

    if _pitch and _subject:
        pitch, subject = _pitch, _subject
    else:
        pitch, subject = await asyncio.to_thread(
            generate_ai_pitch_and_subject, company_name, worker_index
        )
    if not url or "http" not in str(url):
        print(f"   [{company_name[:20]}] Invalid URL — skipping")
        await safe_append_row(sheet, _build_row(
            company_name, url, "No", "Invalid URL", "N/A", "N/A", "0",
            get_company_tokens(company_name)
        ))
        _emit_result(company_name, url, "No", "Invalid URL", "N/A", "N/A", "0", get_company_tokens(company_name))
        return

    # ... baaki code
    
    print(f"\n{'='*55}")
    print(f"  [{lead_index}/{total}] {company_name}")
    print(f"  URL: {url}")
    print(f"  Worker: #{worker_index} | Proxy slot: {worker_index % len(PROXY_LIST)}")
    print(f"{'='*55}")

    # ── Assign proxy by worker index (not random) ────────────
    proxy_config, proxy_label = get_proxy_for_worker(worker_index)

    if _pitch and _subject:
        pitch, subject = _pitch, _subject
    else:
       pitch, subject = await asyncio.to_thread(
        generate_ai_pitch_and_subject, company_name, worker_index
)


    captcha_status = "None"
    bw = {"bytes": 0, "allowed": 0, "blocked": 0}

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
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        # ── Narrow viewport: stacks form columns vertically ──
        # like the RXR screenshot → form is isolated, easier to
        # detect and fill without nav/sidebar noise.
        viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
        proxy=proxy_config,
        permissions=[],
    )
    page = await context.new_page()
    page.on("dialog", lambda dlg: asyncio.ensure_future(dlg.dismiss()))
    await page.add_init_script("""
        window.open = function() { return null; };
        window.alert = function() {};
        window.confirm = function() { return true; };
        window.prompt = function() { return ''; };
    """)
    await page.route("**/*", _make_route_handler(main_host, bw))
    page.on("response", _make_response_counter(bw))

    try:
        for goto_attempt in range(3):  # retry up to 3 times
            try:
                await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                break  # success, exit retry loop
            except Exception as e:
                err = str(e)
                if "ERR_NETWORK_CHANGED" in err:
                    print(f"   [{company_name[:20]}] ERR_NETWORK_CHANGED attempt {goto_attempt+1} — waiting 3s")
                    await asyncio.sleep(3)
                    if goto_attempt == 2:
                        print(f"   [{company_name[:20]}] All 3 attempts failed — skipping")
                        bw_kb = round(bw["bytes"] / 1024, 1)
                        await safe_append_row(sheet, _build_row(
                            company_name, url, "No", "ERR_NETWORK_CHANGED",
                            "N/A", proxy_label, str(bw_kb),
                            get_company_tokens(company_name)
                        ))
                        _emit_result(company_name, url, "No", "ERR_NETWORK_CHANGED", "N/A", proxy_label, str(bw_kb), get_company_tokens(company_name))
                        await context.close()
                        await browser.close()
                        return
                    continue
                elif "Timeout" in err or "timeout" in err:
                    print(f"   [{company_name[:20]}] networkidle timeout — continuing")
                    break
                else:
                    raise
        await asyncio.sleep(2)

        target, strategy = await find_form_target(page, url)
        print(f"   [{company_name[:20]}] Form found via: {strategy}")

        # ── No contact form detected via 6 strategies -> still try GPT fill
        # Some sites have forms that load late or use non-standard markup
        if strategy == "fallback-main":
            print(f"   [{company_name[:20]}] Form finder returned fallback - trying GPT fill anyway...")

        filled, form_data = await gpt_fill_form(page, target, company_name, pitch, subject, worker_index) 
        print(f"   [{company_name[:20]}] Fields filled: {filled}")

        if filled == 0:
            # If form finder also said fallback-main AND GPT found 0 fields -> truly no form
            if strategy == "fallback-main":
                bw_kb = round(bw["bytes"] / 1024, 1)
                print(f"   [{company_name[:20]}] No contact us form present")
                await safe_append_row(sheet, _build_row(
                    company_name, url, "No", "No contact us form present",
                    "N/A", proxy_label, str(bw_kb),
                    get_company_tokens(company_name), message_sent=pitch
                ))
                _emit_result(company_name, url, "No", "No contact us form present", "N/A", proxy_label, str(bw_kb), get_company_tokens(company_name), message_sent=pitch)
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
                        get_company_tokens(company_name)
                    ))
                    _emit_result(company_name, url, "Yes" if ok else "No", "Skyvern submitted" if ok else "Skyvern failed", "N/A", proxy_label, str(bw_kb), get_company_tokens(company_name))
                    return

            bw_kb = round(bw["bytes"] / 1024, 1)
            await safe_append_row(sheet, _build_row(
                company_name, url, "No", "Filled 0 fields",
                "N/A", proxy_label, str(bw_kb),
                get_company_tokens(company_name)
            ))
            _emit_result(company_name, url, "No", "Filled 0 fields", "N/A", proxy_label, str(bw_kb), get_company_tokens(company_name))
            await context.close()
            await browser.close()
            return

        await handle_checkboxes(target)
        if target != page:
            await handle_checkboxes(page)
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
        except Exception as e:
            captcha_status = f"Error:{str(e)[:40]}"
            bw_kb = round(bw["bytes"] / 1024, 1)
            await safe_append_row(sheet, _build_row(
                company_name, url, "No",
                f"Captcha error: {captcha_status}", captcha_status, proxy_label, str(bw_kb),
                get_company_tokens(company_name),
                filled_fields=form_data, message_sent=pitch
            ))
            _emit_result(company_name, url, "No", f"Captcha error: {captcha_status}", captcha_status, proxy_label, str(bw_kb), get_company_tokens(company_name), filled_fields=form_data, message_sent=pitch)
            await context.close()
            await browser.close()
            return

        # ── Captcha timeout/failed → skip submit, mark No ────
        if "timeout" in captcha_status or "no-sitekey" in captcha_status:
            bw_kb = round(bw["bytes"] / 1024, 1)
            await safe_append_row(sheet, _build_row(
                company_name, url, "No",
                f"Captcha not solved: {captcha_status}",
                captcha_status, proxy_label, str(bw_kb),
                get_company_tokens(company_name),
                filled_fields=form_data, message_sent=pitch
            ))
            _emit_result(company_name, url, "No", f"Captcha not solved: {captcha_status}", captcha_status, proxy_label, str(bw_kb), get_company_tokens(company_name), filled_fields=form_data, message_sent=pitch)
            await context.close()
            await browser.close()
            return

        await asyncio.sleep(random.uniform(0.5, 1.2))

        pre_url = page.url
        submitted, submit_method = await click_submit(target, page, company_name)

        if not submitted:
            bw_kb = round(bw["bytes"] / 1024, 1)
            await safe_append_row(sheet, _build_row(
                company_name, url, "No", "Submit button not found",
                captcha_status, proxy_label, str(bw_kb),
                get_company_tokens(company_name),
                filled_fields=form_data, message_sent=pitch
            ))
            _emit_result(company_name, url, "No", "Submit button not found", captcha_status, proxy_label, str(bw_kb), get_company_tokens(company_name), filled_fields=form_data, message_sent=pitch)
            await context.close()
            await browser.close()
            return

        print(f"   [{company_name[:20]}] Submitted via: {submit_method}")

        status, assurance = await get_confirmation(
        page, target if target != page else None,
        original_url=pre_url, company_name=company_name, worker_index=worker_index
)

        bw_kb    = round(bw["bytes"] / 1024, 1)
        tok_cols = get_company_tokens(company_name)

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
        ))
        _emit_result(company_name, url, status, assurance, captcha_status, proxy_label, str(bw_kb), tok_cols, filled_fields=form_data, sub_status=status, confirmation_msg=assurance, message_sent=pitch)

        token_tracker.print_summary()

    except Exception as e:
        if attempt < 2:
            print(f"   [{company_name[:20]}] Retrying: {str(e)[:60]}")
            await context.close()
            await browser.close()
            return await process_form(pw, company_name, url, sheet, lead_index, total,
                                      attempt=2, worker_index=worker_index)
        print(f"   [{company_name[:20]}] FAILED: {e}")
        bw_kb = round(bw["bytes"] / 1024, 1)
        await safe_append_row(sheet, _build_row(
            company_name, url, "No",
            f"Error: {str(e)[:100]}", captcha_status, proxy_label, str(bw_kb),
            get_company_tokens(company_name)
        ))
        _emit_result(company_name, url, "No", f"Error: {str(e)[:100]}", captcha_status, proxy_label, str(bw_kb), get_company_tokens(company_name))

    await context.close()
    await browser.close()
    cd = random.uniform(2, 5)
    print(f"   [{company_name[:20]}] Cooldown {cd:.1f}s...")
    await asyncio.sleep(cd)


# ============================================================
#   CSV LOADER
# ============================================================

def load_leads(csv_path=None):
    if csv_path:
        try:
            leads = []
            with open(csv_path, mode="r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    # ── Blank company name ya blank URL wali rows skip karo ──
                    company = (row.get("Company Name") or "").strip()
                    url     = (row.get("Contact URL Found") or row.get("Contact Form URL") or "").strip()
                    if not company or not url:
                        print(f"   [CSV] Skipping blank row: {row}")
                        continue
                    leads.append(row)

            print(f"[CSV] Loaded {len(leads)} leads from: {csv_path}")
            return leads
        except FileNotFoundError:
            print(f"[CSV] Not found: {csv_path} — using defaults")
    print("[CSV] Using DEFAULT_COMPANIES")
    return DEFAULT_COMPANIES


# ============================================================
#   MAIN
# ============================================================

async def main():
    _make_nopecha_semaphore()

    print()
    print("=" * 65)
    print("  UNIVERSAL CONTACT FORM FILLER — v4")
    print("=" * 65)
    print(f"  Parallel workers  : {PARALLEL_COUNT}")
    print(f"  Proxy slots       : {len(PROXY_LIST)} (worker_index % {len(PROXY_LIST)})")
    print(f"  Sites per proxy   : ~{max(1, 50 // len(PROXY_LIST))} (for 50-lead batch)")
    print(f"  NopeCHA timeout   : {NOPECHA_HARD_TIMEOUT}s (hard kill)")
    print(f"  Viewport          : {VIEWPORT_WIDTH}x{VIEWPORT_HEIGHT} (narrow for form isolation)")
    print(f"  Token limit       : {MAX_INPUT_TOKENS} input / {MAX_OUTPUT_TOKENS} output per call")
    print(f"  Pitch/Subject     : gpt-5-nano (KEPT)")
    print(f"  Form fill         : gpt-4o-mini")
    print(f"  Form detection    : 6-strategy finder")
    print(f"  React/Vue fix     : ENABLED")
    print(f"  Shadow DOM        : ENABLED")
    print(f"  JS Fallback       : ENABLED (0 tokens)")
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
    print(f"\n  CSV: {csv_path}" if csv_path else "\n  No CSV — using DEFAULT_COMPANIES")

    print("\n[Sheets] Connecting...")
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
            print("[Sheets] Write test FAILED — fallback_rows.csv will be used")
    except FileNotFoundError:
        print(f"[Sheets] FATAL — {CREDS_FILE} not found!")
        return
    except Exception as e:
        print(f"[Sheets] FATAL — {e}")
        return

    leads = load_leads(csv_path)
    total = len(leads)
    print(f"\n[Queue] {total} leads | {PARALLEL_COUNT} workers | {len(PROXY_LIST)} proxies\n")

    async with async_playwright() as p:
        queue: asyncio.Queue = asyncio.Queue()
        for i, lead in enumerate(leads):
            await queue.put((i + 1, lead))

        async def worker(worker_index: int):
            """Each worker has a fixed index -> fixed proxy slot."""
            prefetch_task = None
            while True:
                try:
                    lead_index, lead = queue.get_nowait()
                except asyncio.QueueEmpty:
                    if prefetch_task:
                        prefetch_task.cancel()
                    break
                company = (
                    lead.get("Company Name") or lead.get("name") or
                    lead.get("company") or f"Company {lead_index}"
                )
                url = (
    lead.get("Contact URL Found") or      # ← tumhara actual CSV column
    lead.get("Contact Form URL") or
    lead.get("Website URL") or
    lead.get("Website") or
    lead.get("URL") or
    lead.get("url") or ""
).strip()
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

                print(f"\n[Worker#{worker_index}] [{lead_index}/{total}] {company[:35]} | proxy-slot={worker_index % len(PROXY_LIST)}")
                await process_form(p, company, url, sheet, lead_index, total,
                                   _pitch=pitch, _subject=subject,
                                   worker_index=worker_index)
                queue.task_done()
                print(f"[Worker#{worker_index}] Done [{lead_index}/{total}] | {queue.qsize()} left")

        # Spawn workers with explicit indices 0-9
        workers = [
            asyncio.create_task(worker(i))
            for i in range(min(PARALLEL_COUNT, total))
        ]
        await asyncio.gather(*workers)

    print()
    print("=" * 65)
    print("  ALL DONE")
    print(f"  Processed: {total} leads")
    print(f"  Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print("=" * 65)
    token_tracker.print_summary()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[CTRL+C] Stop flag set — waiting for threads to exit...")
        _STOP_FLAG.set()
        import time
        time.sleep(3)   # give threads 3 seconds to see the flag and exit
        print("[CTRL+C] Exited cleanly.")
