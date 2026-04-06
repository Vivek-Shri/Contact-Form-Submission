import asyncio
import sys
import csv
import os
import re
import urllib.parse as _urlparse
from playwright.async_api import async_playwright

# ============================================================
#   CONTACT PAGE FINDER v2
#   Upgrades over v1:
#     1. Parallel workers  — process N companies at once
#     2. Form field detection — lists every input field found
#     3. Resume support    — skips already-processed rows
#
#   Usage:
#     Single URL  : py contact_finder_v2.py "https://example.com"
#     CSV file    : py contact_finder_v2.py companies.csv
#     CSV + workers: py contact_finder_v2.py companies.csv --workers 5
#     Default list: py contact_finder_v2.py
#
#   Output: contact_finder_results_v2.csv
#   Resume : re-run the same command — already-done rows are skipped
# ============================================================

DEFAULT_WORKERS = 8        # safe default for 8 GB RAM
MAX_WORKERS     = 8        # hard cap — 8 GB RAM limit
# RAM guide (single shared browser + separate contexts):
#   8  workers -> ~2.0 GB  safe on 8 GB
#   10 workers -> ~2.5 GB  tight
#   15 workers -> ~3.8 GB  will crash on 8 GB

DEFAULT_URLS = [
    {"Company Name": "Aalpha Information Systems", "Website URL": "https://www.aalpha.net/"},
    {"Company Name": "Car Junction",               "Website URL": "https://www.carjunction.com/"},
    {"Company Name": "Jetour Karachi",             "Website URL": "https://jetourkarachi.com/"},
    {"Company Name": "Suzuki Jinnah Avenue",       "Website URL": "https://suzukijinnahavenue.com/"},
    {"Company Name": "Toyota PQ",                  "Website URL": "http://www.toyota-pq.com/"},
    {"Company Name": "Riaz Motors",                "Website URL": "https://riazmotors.com.pk/"},
    {"Company Name": "Tweak Motorsports",          "Website URL": "https://www.tweakmotorsports.pk/"},
]

CONTACT_KEYWORDS = [
    "contact", "contact-us", "contact_us", "contactus",
    "get-in-touch", "getintouch", "reach-us", "reach_us",
    "touch", "inquiry", "enquiry", "support", "help",
    "write-to-us", "talk-to-us", "connect", "reach",
]

COMMON_PATHS = [
    "/contact-us", "/contact_us", "/contact", "/contactus",
    "/get-in-touch", "/reach-us", "/inquiry", "/enquiry",
    "/support/contact", "/about/contact", "/pages/contact",
    "/help/contact", "/connect", "/about-us/contact",
    "/reach", "/touch",
]

OUTPUT_CSV = "contact_finder_results_v2.csv"
OUTPUT_HEADERS = [
    "Company Name",
    "Input URL",
    "Contact URL Found",
    "Has Form",
    "Form Fields",
    "Has Captcha",
    "Emails Found",
    "Phones Found",
    "Method",
]

# ─── print lock so parallel logs don't interleave ────────────
_print_lock = asyncio.Lock() if False else None   # initialised in main()


async def aprint(*args):
    async with _print_lock:
        print(*args)


# ============================================================
#   CSV LOADER
#   Supports all formats:
#     Format A: single column  "company_website" header + URLs
#     Format B: two columns    "Company Name", "Website URL"
#     Format C: URL only       no header, one URL per line
#     Format D: arrow/tab      "Name → URL" or "Name\tURL"
# ============================================================

def _derive_name_from_url(url: str, row_num: int) -> str:
    """Extract a readable name from a domain, e.g. https://ccinvest.com → ccinvest.com"""
    try:
        netloc = _urlparse.urlparse(url).netloc
        return netloc.lstrip("www.") or f"Row {row_num}"
    except Exception:
        return f"Row {row_num}"


def load_from_csv(csv_path: str) -> list:
    leads = []
    try:
        with open(csv_path, mode="r", encoding="utf-8-sig") as f:
            raw_content = f.read()

        lines = [l.strip() for l in raw_content.splitlines() if l.strip()]
        if not lines:
            print(f"   [CSV] File is empty: {csv_path}")
            return []

        first_line = lines[0].lower().strip()
        print(f"   [CSV] First line: {lines[0][:80]}")

        # ── Detect separator ─────────────────────────────────
        if "→" in lines[0]:
            sep = "→"
        elif "\t" in lines[0]:
            sep = "\t"
        else:
            sep = ","

        # ── Detect header ────────────────────────────────────
        has_header = "http" not in first_line and any(kw in first_line for kw in [
            "company", "website", "url", "name", "contact", "site", "domain", "homepage"
        ])

        print(f"   [CSV] has_header={has_header}  sep='{sep}'")

        # ── FORMAT A/B: has header row ────────────────────────
        if has_header:
            reader = csv.DictReader(raw_content.splitlines())
            headers = [h.strip().lower() for h in (reader.fieldnames or [])]
            print(f"   [CSV] Headers detected: {headers}")

            # Single-column URL-only CSV  (e.g. "company_website")
            url_only_headers = {"company_website", "website", "url", "site",
                                 "domain", "homepage", "website url", "link"}
            is_url_only = len(headers) == 1 or (
                len(headers) >= 1 and headers[0] in url_only_headers
                and not any(h in headers for h in ["company name", "company", "name"])
            )

            for i, row in enumerate(reader, 1):
                if is_url_only:
                    # grab the first column value regardless of header name
                    url = list(row.values())[0].strip().strip('"').strip("'")
                    if not url.startswith("http"):
                        url = "https://" + url
                    company = _derive_name_from_url(url, i)
                else:
                    company = (
                        row.get("Company Name") or row.get("company name") or
                        row.get("Company") or row.get("company") or
                        row.get("Name") or row.get("name") or f"Row {i}"
                    ).strip()
                    url = (
                        row.get("Website URL") or row.get("website url") or
                        row.get("Website") or row.get("website") or
                        row.get("URL") or row.get("url") or
                        row.get("company_website") or row.get("Homepage") or
                        row.get("homepage") or row.get("Site") or row.get("site") or ""
                    ).strip().strip('"').strip("'")
                    if not url.startswith("http"):
                        url = "https://" + url

                if url and "." in url:
                    leads.append({
                        "Company Name":     company,
                        "Website URL":      url,
                        "Contact Form URL": "",
                    })

        # ── FORMAT C/D: no header ─────────────────────────────
        else:
            for i, line in enumerate(lines, 1):
                if not line:
                    continue

                if sep == "→":
                    parts = [p.strip() for p in line.split("→", 1)]
                elif sep == "\t":
                    parts = [p.strip() for p in line.split("\t", 1)]
                else:
                    parts = [p.strip() for p in line.split(",", 1)]

                # figure out which part is the URL
                url_part     = next((p for p in parts if p.startswith("http")), None)
                non_url_parts = [p for p in parts if not p.startswith("http")]

                if url_part:
                    url     = url_part.strip().strip('"').strip("'")
                    company = non_url_parts[0].strip() if non_url_parts else _derive_name_from_url(url, i)
                else:
                    # whole line might be a bare domain
                    raw = parts[0].strip().strip('"').strip("'")
                    url = "https://" + raw if not raw.startswith("http") else raw
                    company = _derive_name_from_url(url, i)

                if url and "." in url:
                    leads.append({
                        "Company Name":     company,
                        "Website URL":      url,
                        "Contact Form URL": "",
                    })

        print(f"   [CSV] Loaded {len(leads)} URLs from {csv_path}")
        if leads:
            print(f"   [CSV] Sample → {leads[0]['Company Name']} : {leads[0]['Website URL']}")
        return leads

    except FileNotFoundError:
        print(f"   [CSV] File not found: {csv_path}")
        return []
    except Exception as e:
        print(f"   [CSV] Error: {e}")
        return []


# ============================================================
#   RESUME — load already-done input URLs from output CSV
# ============================================================

def load_done_urls(output_path: str) -> set:
    done = set()
    if not os.path.exists(output_path):
        return done
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("Input URL", "").strip()
                if url:
                    done.add(url)
        print(f"   [Resume] {len(done)} companies already processed — skipping them")
    except Exception as e:
        print(f"   [Resume] Could not read existing output: {e}")
    return done


# ============================================================
#   FORM DETECTION  (upgraded)
# ============================================================

async def _inspect_form(page) -> dict:
    """
    Returns:
      has_form   : bool
      fields     : list of field descriptors  e.g. ["name(text)", "email(email)", "message(textarea)"]
      has_captcha: bool
    """
    try:
        return await page.evaluate("""() => {
            const fields = [];
            const inputs = document.querySelectorAll(
                'input[type="text"], input[type="email"], input[type="tel"], ' +
                'input[type="number"], input[type="url"], input[name], textarea, select'
            );
            inputs.forEach(el => {
                const tag   = el.tagName.toLowerCase();
                const type  = el.type  || tag;
                const name  = (el.name || el.id || el.placeholder || '').toLowerCase().slice(0,30);
                const label = (() => {
                    if (el.id) {
                        const l = document.querySelector('label[for="'+el.id+'"]');
                        if (l) return l.innerText.trim().toLowerCase().slice(0,30);
                    }
                    return '';
                })();
                const desc = label || name || type;
                if (desc && desc !== 'submit' && desc !== 'button' && desc !== 'hidden') {
                    fields.push(desc + '(' + type + ')');
                }
            });

            const hasForm = fields.length > 0 && !!(
                document.querySelector('input[type="email"]') ||
                document.querySelector('textarea') ||
                document.querySelector('form')
            );

            // detect captcha
            const bodyText = document.body.innerHTML.toLowerCase();
            const hasCaptcha = bodyText.includes('recaptcha') ||
                               bodyText.includes('hcaptcha') ||
                               bodyText.includes('cf-turnstile') ||
                               !!document.querySelector('.g-recaptcha, .h-captcha, iframe[src*="captcha"]');

            return { has_form: hasForm, fields: [...new Set(fields)], has_captcha: hasCaptcha };
        }""")
    except Exception:
        return {"has_form": False, "fields": [], "has_captcha": False}


# ============================================================
#   EMAIL + PHONE EXTRACTOR
# ============================================================

async def _extract_contacts(page) -> dict:
    """Extract visible email addresses and phone numbers from page text."""
    try:
        text = await page.evaluate("() => document.body.innerText")
    except Exception:
        return {"emails": [], "phones": []}

    emails = list(set(re.findall(
        r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text
    )))
    emails = [e for e in emails if not e.endswith(('.png', '.jpg', '.svg', '.gif'))][:5]

    phones = list(set(re.findall(
        r'(?:\+?\d[\d\s\-().]{7,}\d)', text
    )))
    phones = [p.strip() for p in phones if len(re.sub(r'\D', '', p)) >= 7][:5]

    return {"emails": emails, "phones": phones}


# ============================================================
#   CORE FINDER  (v2 — uses upgraded form + contact helpers)
# ============================================================

async def find_contact_url(context, company_name: str, company_url: str) -> dict:
    result = {
        "Company Name":      company_name,
        "Input URL":         company_url,
        "Contact URL Found": None,
        "Has Form":          False,
        "Form Fields":       "",
        "Has Captcha":       False,
        "Emails Found":      "",
        "Phones Found":      "",
        "Method":            None,
    }

    if not company_url or "http" not in company_url:
        result["Method"] = "invalid_url"
        return result

    parsed   = _urlparse.urlparse(company_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    page = await context.new_page()
    page.on("console", lambda msg: None)

    try:
        # ── STEP 1: Try common contact URL patterns directly ──
        for path in COMMON_PATHS:
            test_url = base_url + path
            try:
                resp = await page.goto(test_url, timeout=8000, wait_until="domcontentloaded")
                if resp and resp.status in (200, 301, 302):
                    final_url  = page.url
                    form_info  = await _inspect_form(page)
                    if form_info["has_form"]:
                        contacts = await _extract_contacts(page)
                        result.update({
                            "Contact URL Found": final_url,
                            "Has Form":          True,
                            "Form Fields":       " | ".join(form_info["fields"]),
                            "Has Captcha":       form_info["has_captcha"],
                            "Emails Found":      " | ".join(contacts["emails"]),
                            "Phones Found":      " | ".join(contacts["phones"]),
                            "Method":            "common_path",
                        })
                        await page.close()
                        return result
            except asyncio.CancelledError:
                raise   # let CancelledError propagate — do not swallow it
            except Exception:
                pass

        # ── STEP 2: Load page and scan all links ──────────────
        try:
            await page.goto(company_url, timeout=30000, wait_until="domcontentloaded")
            await asyncio.sleep(2)
        except Exception as e:
            result["Method"] = "load_failed"
            await page.close()
            return result

        links = await page.evaluate("""() => {
            const links = [];
            document.querySelectorAll('a[href]').forEach(a => {
                const href = (a.href || '').trim();
                const text = (a.innerText || a.textContent || '')
                              .trim().toLowerCase().replace(/\\s+/g, ' ').slice(0, 80);
                if (href && (href.startsWith('http') || href.startsWith('/')))
                    links.push({ href, text });
            });
            return links;
        }""")

        scored = []
        seen   = set()

        for link in links:
            raw_href = link['href']
            text     = link['text']

            if raw_href.startswith('/'):
                raw_href = base_url + raw_href

            href_lower = raw_href.lower()

            if any(x in href_lower for x in [
                'mailto:', 'tel:', 'javascript:', '#',
                'facebook.com', 'twitter.com', 'linkedin.com',
                'instagram.com', 'youtube.com', 'whatsapp',
                '.pdf', '.jpg', '.png', '.zip',
            ]):
                continue

            link_netloc = _urlparse.urlparse(raw_href).netloc
            if not (link_netloc == parsed.netloc or
                    link_netloc.endswith('.' + parsed.netloc) or
                    parsed.netloc.endswith('.' + link_netloc)):
                continue

            if raw_href in seen:
                continue
            seen.add(raw_href)

            score    = 0
            url_path = _urlparse.urlparse(raw_href).path.lower()

            for kw in CONTACT_KEYWORDS:
                if kw in url_path: score += 3
                if kw in text:     score += 2

            if text.strip() in ("contact us", "contact", "get in touch",
                                  "reach us", "enquiry", "inquiry", "write to us"):
                score += 5

            if any(x in url_path for x in ['/blog', '/news', '/product',
                                              '/shop', '/cart', '/category']):
                score -= 2

            if score > 0:
                scored.append((score, raw_href, text))

        scored.sort(key=lambda x: x[0], reverse=True)

        # ── STEP 3: Verify candidates have a form ─────────────
        for score, href, text in scored[:5]:
            try:
                await page.goto(href, timeout=15000, wait_until="domcontentloaded")
                await asyncio.sleep(1.5)
                form_info = await _inspect_form(page)
                final_url = page.url
                if form_info["has_form"]:
                    contacts = await _extract_contacts(page)
                    result.update({
                        "Contact URL Found": final_url,
                        "Has Form":          True,
                        "Form Fields":       " | ".join(form_info["fields"]),
                        "Has Captcha":       form_info["has_captcha"],
                        "Emails Found":      " | ".join(contacts["emails"]),
                        "Phones Found":      " | ".join(contacts["phones"]),
                        "Method":            "link_scan",
                    })
                    await page.close()
                    return result
            except Exception:
                pass

        # ── STEP 4: Return best candidate even without form ───
        if scored:
            result.update({
                "Contact URL Found": scored[0][1],
                "Has Form":          False,
                "Method":            "link_scan_no_form",
            })
        else:
            result.update({
                "Contact URL Found": company_url,
                "Has Form":          False,
                "Method":            "original_fallback",
            })

        await page.close()
        return result

    except asyncio.CancelledError:
        try:
            await page.close()
        except Exception:
            pass
        raise   # must propagate so worker can catch and recreate context
    except Exception as e:
        try:
            await page.close()
        except Exception:
            pass
        result["Method"] = f"error:{str(e)[:50]}"
        return result


# ============================================================
#   WORKER HELPERS
# ============================================================

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
       "AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/122.0.0.0 Safari/537.36")

async def _make_context(browser):
    return await browser.new_context(
        user_agent=_UA,
        viewport={"width": 1280, "height": 900},
    )

def _empty_result(company, url, method="error:worker_crash"):
    """Safe fallback — guarantees result is never None."""
    return {
        "Company Name":      company,
        "Input URL":         url,
        "Contact URL Found": url,
        "Has Form":          False,
        "Form Fields":       "",
        "Has Captcha":       False,
        "Emails Found":      "",
        "Phones Found":      "",
        "Method":            method,
    }


# ============================================================
#   WORKER  — each worker pulls from a shared queue
# ============================================================

async def worker(worker_id: int, queue: asyncio.Queue, results: list,
                 browser, semaphore: asyncio.Semaphore, total: int,
                 output_path: str, is_append: bool):
    """
    Each worker shares ONE browser but has its OWN context.
    - TargetClosedError  -> context is recreated, company retried once
    - result is NEVER None -> _empty_result() is the guaranteed fallback
    - each result written to CSV immediately (crash-safe)
    """
    context = await _make_context(browser)

    while True:
        try:
            idx, lead = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        company = lead.get("Company Name", f"Company {idx}")
        url     = lead.get("Website URL", "")

        async with semaphore:
            async with _print_lock:
                print(f"   [W{worker_id}] [{idx}/{total}] {company[:40]}")

            result = None
            try:
                result = await find_contact_url(context, company, url)
            except Exception as exc:
                async with _print_lock:
                    print(f"   [W{worker_id}] [{idx}/{total}] WARN {type(exc).__name__} — retrying with fresh context")
                # Recreate context and retry once
                try:
                    await context.close()
                except Exception:
                    pass
                try:
                    context = await _make_context(browser)
                    result  = await find_contact_url(context, company, url)
                except Exception as retry_exc:
                    async with _print_lock:
                        print(f"   [W{worker_id}] [{idx}/{total}] RETRY FAILED: {str(retry_exc)[:60]}")
                    result = _empty_result(company, url, f"error:{type(retry_exc).__name__}")

            # Guarantee result dict is never None
            if result is None:
                result = _empty_result(company, url, "error:none_returned")

            results.append(result)
            save_results_csv([result], output_path, append=True)

            async with _print_lock:
                status = "✓ FORM" if result.get("Has Form") else "— no form"
                info   = result.get("Form Fields") or result.get("Contact URL Found") or ""
                print(f"   [W{worker_id}] [{idx}/{total}] {status}  {str(info)[:55]}")

        queue.task_done()

    try:
        await context.close()
    except Exception:
        pass


# ============================================================
#   SAVE / APPEND RESULTS TO CSV
# ============================================================

def save_results_csv(results: list, output_path: str, append: bool = False):
    mode     = "a" if append else "w"
    add_hdr  = not os.path.exists(output_path) or not append
    try:
        with open(output_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_HEADERS)
            if add_hdr:
                writer.writeheader()
            for r in results:
                writer.writerow({
                    "Company Name":      r.get("Company Name", ""),
                    "Input URL":         r.get("Input URL", ""),
                    "Contact URL Found": r.get("Contact URL Found", ""),
                    "Has Form":          "Yes" if r.get("Has Form") else "No",
                    "Form Fields":       r.get("Form Fields", ""),
                    "Has Captcha":       "Yes" if r.get("Has Captcha") else "No",
                    "Emails Found":      r.get("Emails Found", ""),
                    "Phones Found":      r.get("Phones Found", ""),
                    "Method":            r.get("Method", ""),
                })
    except Exception as e:
        print(f"\n   [Output] Failed to save CSV: {e}")


# ============================================================
#   MAIN
# ============================================================

async def main():
    global _print_lock
    _print_lock = asyncio.Lock()

    # ── Parse args ───────────────────────────────────────────
    args    = sys.argv[1:]
    workers = DEFAULT_WORKERS
    source  = "default"
    leads   = []

    # extract --workers N
    if "--workers" in args:
        idx = args.index("--workers")
        try:
            workers = min(int(args[idx + 1]), MAX_WORKERS)
            args    = args[:idx] + args[idx + 2:]
        except (IndexError, ValueError):
            pass

    if args:
        arg = args[0]
        if arg.endswith(".csv") or os.path.isfile(arg):
            print(f"[Finder] Loading from CSV: {arg}")
            leads  = load_from_csv(arg)
            source = f"csv:{arg}"
        elif arg.startswith("http"):
            leads  = [{"Company Name": "Input Company", "Website URL": arg, "Contact Form URL": ""}]
            source = "single_url"
        else:
            print(f"[Finder] Unknown argument: {arg}")
            return
    else:
        leads  = DEFAULT_URLS
        source = "default"

    if not leads:
        print("[Finder] No leads to process")
        return

    # ── Resume: skip already-done URLs ───────────────────────
    done_urls  = load_done_urls(OUTPUT_CSV)
    todo       = [l for l in leads if l.get("Website URL", "").strip() not in done_urls]
    skipped    = len(leads) - len(todo)

    print()
    print("=" * 60)
    print("  CONTACT PAGE FINDER  v2")
    print(f"  Source   : {source}")
    print(f"  Total    : {len(leads)}  |  Todo: {len(todo)}  |  Skipped: {skipped}")
    print(f"  Workers  : {workers}")
    print(f"  Output   : {OUTPUT_CSV}")
    print("=" * 60)

    if not todo:
        print("  Nothing new to process.")
        return

    # ── Build queue ───────────────────────────────────────────
    queue     = asyncio.Queue()
    for i, lead in enumerate(todo, skipped + 1):
        # pre-filled contact URL — skip discovery
        existing = lead.get("Contact Form URL", "").strip()
        if existing and "http" in existing:
            save_results_csv([{
                "Company Name":      lead.get("Company Name", ""),
                "Input URL":         lead.get("Website URL", ""),
                "Contact URL Found": existing,
                "Has Form":          True,
                "Form Fields":       "",
                "Has Captcha":       False,
                "Emails Found":      "",
                "Phones Found":      "",
                "Method":            "from_csv",
            }], OUTPUT_CSV, append=True)
            continue
        queue.put_nowait((i, lead))

    results   = []
    semaphore = asyncio.Semaphore(workers)
    is_append = skipped > 0  # append if resuming, else fresh file

    # Write header row now (workers append rows immediately after)
    if not is_append or not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            import csv as _csv
            _csv.DictWriter(f, fieldnames=OUTPUT_HEADERS).writeheader()

    async with async_playwright() as pw:
        # ONE shared browser — all workers use separate contexts from it
        # This saves ~150 MB vs launching one browser per worker
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox",
                  "--disable-gpu", "--memory-pressure-off"]
        )

        worker_tasks = [
            asyncio.create_task(
                worker(wid, queue, results, browser, semaphore,
                       len(todo), OUTPUT_CSV, is_append)
            )
            for wid in range(1, workers + 1)
        ]

        await asyncio.gather(*worker_tasks)
        await browser.close()

    # ── Summary ───────────────────────────────────────────────
    all_results = results
    print()
    print("=" * 60)
    print("  SUMMARY  (this run)")
    print("=" * 60)
    found    = sum(1 for r in all_results if r.get("Contact URL Found"))
    has_form = sum(1 for r in all_results if r.get("Has Form"))
    captcha  = sum(1 for r in all_results if r.get("Has Captcha"))

    print(f"  Processed : {len(all_results)}")
    print(f"  Contact found  : {found}/{len(all_results)}")
    print(f"  Form verified  : {has_form}/{len(all_results)}")
    print(f"  Has CAPTCHA    : {captcha}/{len(all_results)}")
    print()
    print(f"  {'Company':<28} {'Form':<5} {'Captcha':<8} {'Fields'}")
    print(f"  {'-'*28} {'-'*5} {'-'*8} {'-'*30}")
    for r in all_results:
        name    = r.get("Company Name", "")[:26]
        form    = "YES" if r.get("Has Form") else "NO"
        cap     = "YES" if r.get("Has Captcha") else "NO"
        fields  = r.get("Form Fields", "—")[:40]
        print(f"  {name:<28} {form:<5} {cap:<8} {fields}")

    print()
    print(f"  Full results saved to: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())