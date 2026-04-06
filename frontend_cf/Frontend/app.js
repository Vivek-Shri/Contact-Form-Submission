/**
 * Universal Contact Form Filler — Dashboard v2 Frontend
 * All client-side logic for the SPA dashboard + Scraper integration.
 */

// ============================================================
//   STATE
// ============================================================

let currentPage = 'dashboard';
let currentSettingsTab = 'identity';
let uploadedFilename = null;
let scraperUploadedFilename = null;
let runPollingInterval = null;
let scraperPollingInterval = null;
let logEventSource = null;
let scraperLogEventSource = null;
let submissionsChart = null;
let currentDetailRunId = null;
let sheetData = [];
let sheetPage = 0;
const SHEET_PAGE_SIZE = 25;

// ============================================================
//   INIT
// ============================================================

document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    setupLoginForm();
    setupCSVDrop();
    setupScraperCSVDrop();
});

async function checkAuth() {
    try {
        const res = await fetch('/api/auth-status');
        const data = await res.json();
        if (data.logged_in) showApp(data.username);
    } catch (e) { }
}

function setupLoginForm() {
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const user = document.getElementById('login-user').value;
        const pass = document.getElementById('login-pass').value;
        const errEl = document.getElementById('login-error');
        errEl.classList.add('hidden');
        try {
            const res = await fetch('/api/login', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: user, password: pass })
            });
            const data = await res.json();
            if (data.success) showApp(user);
            else { errEl.textContent = data.error || 'Invalid credentials'; errEl.classList.remove('hidden'); }
        } catch (e) { errEl.textContent = 'Connection error'; errEl.classList.remove('hidden'); }
    });
}

function showApp(username) {
    document.getElementById('login-screen').classList.add('hidden');
    document.getElementById('app-layout').classList.remove('hidden');
    document.getElementById('user-badge').textContent = username;
    switchPage('dashboard');
}

async function logout() {
    await fetch('/api/logout');
    document.getElementById('app-layout').classList.add('hidden');
    document.getElementById('login-screen').classList.remove('hidden');
    document.getElementById('login-user').value = '';
    document.getElementById('login-pass').value = '';
}

// ============================================================
//   NAVIGATION
// ============================================================

const pageTitles = {
    dashboard: ['Dashboard', 'Overview of your automation runs'],
    scraper: ['Contact Scraper', 'Find contact forms on company websites'],
    run: ['Run Automation', 'Upload CSV and run the contact form filler'],
    history: ['Run History', 'View all past automation runs'],
    logs: ['Live Logs', 'Real-time subprocess output'],
    settings: ['Settings', 'Configure identity, API keys, and automation'],
    sheet: ['Google Sheet', 'Live view of your Google Sheet data'],
};

function switchPage(page) {
    currentPage = page;
    document.querySelectorAll('.page-section').forEach(el => el.classList.add('hidden'));
    document.getElementById('page-' + page)?.classList.remove('hidden');
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.page === page);
    });
    const [title, subtitle] = pageTitles[page] || ['', ''];
    document.getElementById('page-title').textContent = title;
    document.getElementById('page-subtitle').textContent = subtitle;

    if (page === 'dashboard') loadDashboard();
    if (page === 'scraper') { loadScraperResults(); checkScraperActive(); }
    if (page === 'history') loadRunHistory();
    if (page === 'logs') loadLiveLogs();
    if (page === 'settings') { loadSettings(); switchSettingsTab('identity'); }
    if (page === 'sheet') loadSheetData();
    if (page === 'run') checkActiveRun();
}

// ============================================================
//   TOAST
// ============================================================

function toast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const el = document.createElement('div');
    el.className = `toast toast-${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 4000);
}

// ============================================================
//   DASHBOARD
// ============================================================

async function loadDashboard() {
    try {
        const res = await fetch('/api/dashboard-stats');
        const d = await res.json();
        document.getElementById('stat-total-runs').textContent = d.total_runs;
        document.getElementById('stat-total-leads').textContent = d.total_leads.toLocaleString();
        document.getElementById('stat-success-rate').textContent = d.success_rate + '%';
        document.getElementById('stat-total-cost').textContent = '$' + d.total_cost.toFixed(4);
        document.getElementById('stat-total-tokens').textContent = d.total_tokens.toLocaleString();
        document.getElementById('stat-captchas').textContent = d.captchas_solved;
        renderChart(d.chart_data || []);
        renderRecentActivity(d.recent_activity || []);
    } catch (e) { console.error('Dashboard load error:', e); }
}

function renderChart(data) {
    const ctx = document.getElementById('chart-submissions');
    if (!ctx) return;
    if (submissionsChart) submissionsChart.destroy();
    const labels = data.map(d => '#' + d.run_id).reverse();
    const success = data.map(d => d.successful).reverse();
    const failed = data.map(d => d.failed).reverse();
    submissionsChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels, datasets: [
                { label: 'Successful', data: success, backgroundColor: 'rgba(16,185,129,0.7)', borderRadius: 4 },
                { label: 'Failed', data: failed, backgroundColor: 'rgba(239,68,68,0.7)', borderRadius: 4 },
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { labels: { color: '#9ca3af', font: { size: 11 } } } },
            scales: {
                x: { stacked: true, grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#6b7280', font: { size: 10 } } },
                y: { stacked: true, grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#6b7280', font: { size: 10 } }, beginAtZero: true }
            }
        },
    });
}

function renderRecentActivity(rows) {
    const tbody = document.getElementById('recent-activity-body');
    tbody.innerHTML = '';
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="5" class="py-6 text-center text-gray-600">No activity yet</td></tr>'; return; }
    rows.forEach(r => {
        const tr = document.createElement('tr');
        tr.className = 'border-b border-white/5';
        tr.innerHTML = `<td class="py-2.5 text-white font-medium max-w-[160px] truncate">${esc(r.company_name)}</td><td class="py-2.5">${makeBadge(r.submission_status)}</td><td class="py-2.5 text-gray-400">${esc(r.captcha_status || '—')}</td><td class="py-2.5 text-gray-400">${esc(r.est_cost || '—')}</td><td class="py-2.5 text-gray-500 text-xs">${esc(r.created_at || '')}</td>`;
        tbody.appendChild(tr);
    });
}

// ============================================================
//   CSV UPLOAD & RUN AUTOMATION
// ============================================================

function setupCSVDrop() {
    const dropzone = document.getElementById('csv-dropzone');
    const fileInput = document.getElementById('csv-file-input');
    dropzone.addEventListener('click', () => fileInput.click());
    dropzone.addEventListener('dragover', (e) => { e.preventDefault(); dropzone.classList.add('border-accent-500/50'); });
    dropzone.addEventListener('dragleave', () => dropzone.classList.remove('border-accent-500/50'));
    dropzone.addEventListener('drop', (e) => { e.preventDefault(); dropzone.classList.remove('border-accent-500/50'); if (e.dataTransfer.files.length) handleCSVFile(e.dataTransfer.files[0]); });
    fileInput.addEventListener('change', () => { if (fileInput.files.length) handleCSVFile(fileInput.files[0]); });
}

async function handleCSVFile(file) {
    const formData = new FormData(); formData.append('file', file);
    try {
        const res = await fetch('/api/upload-csv', { method: 'POST', body: formData });
        const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
        uploadedFilename = data.filename;
        document.getElementById('csv-total-leads').textContent = data.total_leads;
        const tbody = document.getElementById('csv-preview-body'); tbody.innerHTML = '';
        data.preview.forEach((row, i) => {
            const tr = document.createElement('tr'); tr.className = 'border-b border-white/5';
            tr.innerHTML = `<td class="py-2 text-gray-500">${i + 1}</td><td class="py-2 text-white">${esc(row.company_name)}</td><td class="py-2 text-gray-400 max-w-[300px] truncate"><a href="${esc(row.contact_url)}" target="_blank" class="hover:text-accent-400">${esc(row.contact_url)}</a></td>`;
            tbody.appendChild(tr);
        });
        document.getElementById('csv-preview-section').classList.remove('hidden');
        toast(`Loaded ${data.total_leads} leads from CSV`, 'success');
    } catch (e) { toast('Upload failed: ' + e.message, 'error'); }
}

function clearUpload() { uploadedFilename = null; document.getElementById('csv-preview-section').classList.add('hidden'); document.getElementById('csv-file-input').value = ''; }

async function startRun() {
    if (!uploadedFilename) { toast('Please upload a CSV first', 'error'); return; }
    try {
        const res = await fetch('/api/start-run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ filename: uploadedFilename }) });
        const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
        toast('Automation started!', 'success');
        showRunProgress(data); startRunPolling(); startLogStreaming();
    } catch (e) { toast('Failed to start run: ' + e.message, 'error'); }
}

function showRunProgress(data) {
    document.getElementById('run-progress-section').classList.remove('hidden');
    document.getElementById('run-log-section').classList.remove('hidden');
    document.getElementById('run-results-section').classList.remove('hidden');
    document.getElementById('run-summary-section').classList.add('hidden');
    document.getElementById('run-indicator').classList.remove('hidden');
    document.getElementById('run-indicator').classList.add('flex');
    document.getElementById('btn-start-run').disabled = true;
    document.getElementById('btn-start-run').classList.add('opacity-50');
}

async function checkActiveRun() {
    try {
        const res = await fetch('/api/run-status'); const data = await res.json();
        if (data.status === 'running') { showRunProgress(data); updateRunUI(data); startRunPolling(); startLogStreaming(); }
    } catch (e) { }
}

function startRunPolling() { if (runPollingInterval) clearInterval(runPollingInterval); runPollingInterval = setInterval(pollRunStatus, 2000); }

async function pollRunStatus() {
    try {
        const res = await fetch('/api/run-status'); const data = await res.json();
        updateRunUI(data);
        if (['completed', 'stopped', 'error', 'idle'].includes(data.status)) {
            clearInterval(runPollingInterval); runPollingInterval = null;
            document.getElementById('run-indicator').classList.add('hidden');
            document.getElementById('run-indicator').classList.remove('flex');
            document.getElementById('btn-start-run').disabled = false;
            document.getElementById('btn-start-run').classList.remove('opacity-50');
            if (data.status !== 'idle') { showRunSummary(data); toast(`Run ${data.status}`, data.status === 'completed' ? 'success' : 'error'); }
        }
    } catch (e) { console.error(e); }
}

function updateRunUI(data) {
    const total = data.total_leads || 1, processed = data.processed || 0;
    const pct = Math.min(Math.round((processed / total) * 100), 100);
    document.getElementById('run-progress-text').textContent = `Processing ${processed} of ${total} leads`;
    document.getElementById('run-progress-bar').style.width = pct + '%';
    document.getElementById('run-progress-pct').textContent = pct + '%';
    const tbody = document.getElementById('run-results-body'); const results = data.results || [];
    tbody.innerHTML = '';
    results.forEach((r, i) => {
        const tr = document.createElement('tr'); tr.className = 'border-b border-white/5';
        tr.innerHTML = `<td class="py-2 text-gray-500">${i + 1}</td><td class="py-2 text-white max-w-[140px] truncate">${esc(r.company_name)}</td><td class="py-2 text-gray-400 max-w-[180px] truncate"><a href="${esc(r.contact_url)}" target="_blank" class="hover:text-accent-400">${esc(r.contact_url)}</a></td><td class="py-2">${r.submitted === 'Yes' ? '<span class="badge badge-success">Yes</span>' : '<span class="badge badge-danger">No</span>'}</td><td class="py-2">${makeBadge(r.submission_status)}</td><td class="py-2 text-gray-400 text-xs">${esc(r.captcha_status || '—')}</td><td class="py-2 text-gray-400 text-xs max-w-[150px] truncate" title="${esc(r.fields_filled || '')}">${esc(r.fields_filled || '—')}</td><td class="py-2 text-gray-400 text-xs">${esc(r.est_cost || '—')}</td>`;
        tbody.appendChild(tr);
    });
}

function showRunSummary(data) {
    document.getElementById('run-summary-section').classList.remove('hidden');
    const results = data.results || [];
    const sc = results.filter(r => r.submission_status && r.submission_status.toLowerCase().includes('success')).length;
    const fc = results.filter(r => r.submission_status && r.submission_status.toLowerCase().includes('fail')).length;
    let tc = 0; results.forEach(r => { try { tc += parseFloat(r.est_cost || 0); } catch (e) { } });
    document.getElementById('run-summary-cards').innerHTML = `<div class="bg-dark-700/50 rounded-xl p-4 text-center"><p class="text-xs text-gray-500 uppercase mb-1">Processed</p><p class="text-xl font-bold text-white">${results.length}</p></div><div class="bg-dark-700/50 rounded-xl p-4 text-center"><p class="text-xs text-gray-500 uppercase mb-1">Successful</p><p class="text-xl font-bold text-success-400">${sc}</p></div><div class="bg-dark-700/50 rounded-xl p-4 text-center"><p class="text-xs text-gray-500 uppercase mb-1">Failed</p><p class="text-xl font-bold text-danger-400">${fc}</p></div><div class="bg-dark-700/50 rounded-xl p-4 text-center"><p class="text-xs text-gray-500 uppercase mb-1">Total Cost</p><p class="text-xl font-bold text-warn-400">$${tc.toFixed(4)}</p></div>`;
}

async function stopRun() {
    if (!confirm('Stop the current run?')) return;
    try { const res = await fetch('/api/stop-run', { method: 'POST' }); const data = await res.json(); if (data.success) toast('Run stopped', 'info'); else toast(data.error || 'Failed', 'error'); } catch (e) { toast('Error: ' + e.message, 'error'); }
}

// ============================================================
//   LIVE LOG STREAMING
// ============================================================

function startLogStreaming() {
    if (logEventSource) logEventSource.close();
    logEventSource = new EventSource('/api/logs/stream');
    logEventSource.onmessage = (event) => { try { const data = JSON.parse(event.data); if (data.line) { appendLogLine(data.line, 'run-log-box'); appendLogLine(data.line, 'live-log-box'); } if (data.done) { logEventSource.close(); logEventSource = null; } } catch (e) { } };
    logEventSource.onerror = () => { if (logEventSource) logEventSource.close(); logEventSource = null; };
}

function appendLogLine(line, containerId) {
    const box = document.getElementById(containerId); if (!box) return;
    const div = document.createElement('div'); div.className = classifyLogLine(line); div.textContent = line;
    box.appendChild(div); box.scrollTop = box.scrollHeight;
    while (box.children.length > 500) box.removeChild(box.firstChild);
}

function classifyLogLine(line) {
    const l = line.toLowerCase();
    if (l.includes('error') || l.includes('fail') || l.includes('exception')) return 'log-error';
    if (l.includes('success') || l.includes('[sheets] ok') || l.includes('submitted: yes')) return 'log-success';
    if (l.includes('captcha') || l.includes('warning') || l.includes('retry') || l.includes('nopecha')) return 'log-warn';
    return 'log-info';
}

async function loadLiveLogs() {
    try { const res = await fetch('/api/logs/current'); const data = await res.json(); const box = document.getElementById('live-log-box'); box.innerHTML = ''; (data.logs || []).forEach(line => appendLogLine(line, 'live-log-box')); if (data.status === 'running') startLogStreaming(); } catch (e) { }
}
function clearLogs() { document.getElementById('live-log-box').innerHTML = ''; }

// ============================================================
//   RUN HISTORY
// ============================================================

async function loadRunHistory() {
    try {
        const res = await fetch('/api/runs'); const runs = await res.json();
        const tbody = document.getElementById('history-body'); tbody.innerHTML = '';
        if (!runs.length) { tbody.innerHTML = '<tr><td colspan="9" class="py-6 text-center text-gray-600">No runs yet</td></tr>'; return; }
        runs.forEach(r => {
            const sb = r.status === 'completed' ? '<span class="badge badge-success">completed</span>' : r.status === 'running' ? '<span class="badge badge-warn">running</span>' : r.status === 'stopped' ? '<span class="badge badge-neutral">stopped</span>' : '<span class="badge badge-danger">' + esc(r.status) + '</span>';
            const tr = document.createElement('tr'); tr.className = 'border-b border-white/5 cursor-pointer';
            tr.innerHTML = `<td class="py-3 text-accent-400 font-medium">#${r.id}</td><td class="py-3 text-gray-300 text-xs">${esc(r.start_time)}</td><td class="py-3 text-gray-400 max-w-[140px] truncate text-xs">${esc(r.csv_filename || '—')}</td><td class="py-3 text-white">${r.total_leads}</td><td class="py-3 text-success-400">${r.successful || 0}</td><td class="py-3 text-danger-400">${r.failed || 0}</td><td class="py-3 text-gray-300">$${(r.total_cost || 0).toFixed(4)}</td><td class="py-3">${sb}</td><td class="py-3"><button onclick="viewRunDetail(${r.id})" class="text-accent-400 hover:text-accent-300 text-xs font-medium">View →</button></td>`;
            tbody.appendChild(tr);
        });
    } catch (e) { toast('Failed to load history', 'error'); }
}

async function viewRunDetail(runId) {
    currentDetailRunId = runId;
    try {
        const res = await fetch(`/api/runs/${runId}`); const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
        document.getElementById('detail-run-id').textContent = runId;
        const tbody = document.getElementById('detail-body'); tbody.innerHTML = '';
        (data.results || []).forEach(r => {
            const tr = document.createElement('tr'); tr.className = 'border-b border-white/5';
            tr.innerHTML = `<td class="py-2 text-white max-w-[130px] truncate">${esc(r.company_name)}</td><td class="py-2 text-gray-400 max-w-[160px] truncate"><a href="${esc(r.contact_url)}" target="_blank" class="hover:text-accent-400">${esc(r.contact_url)}</a></td><td class="py-2">${r.submitted === 'Yes' ? '<span class="badge badge-success">Yes</span>' : '<span class="badge badge-danger">No</span>'}</td><td class="py-2">${makeBadge(r.submission_status)}</td><td class="py-2 text-gray-400 text-xs">${esc(r.captcha_status || '—')}</td><td class="py-2 text-gray-400 text-xs max-w-[120px] truncate">${esc(r.fields_filled || '—')}</td><td class="py-2 text-gray-400 text-xs">${esc(r.est_cost || '—')}</td><td class="py-2 text-gray-400 text-xs">${esc(r.total_tokens || '—')}</td><td class="py-2 text-gray-400 text-xs max-w-[140px] truncate">${esc(r.confirmation_msg || '—')}</td>`;
            tbody.appendChild(tr);
        });
        document.getElementById('run-detail-section').classList.remove('hidden');
        document.getElementById('run-detail-section').scrollIntoView({ behavior: 'smooth' });
    } catch (e) { toast('Failed to load run details', 'error'); }
}
function closeRunDetail() { document.getElementById('run-detail-section').classList.add('hidden'); currentDetailRunId = null; }
function exportRun() { if (!currentDetailRunId) return; window.open(`/api/runs/${currentDetailRunId}/export`, '_blank'); }

// ============================================================
//   SETTINGS
// ============================================================

function switchSettingsTab(tab) {
    currentSettingsTab = tab;
    document.querySelectorAll('.settings-panel').forEach(el => el.classList.add('hidden'));
    document.getElementById('stab-' + tab)?.classList.remove('hidden');
    document.querySelectorAll('.settings-tab').forEach(btn => { btn.classList.toggle('active', btn.dataset.stab === tab); });
}

async function loadSettings() {
    try {
        const res = await fetch('/api/settings'); const cfg = await res.json();
        ['MY_FIRST_NAME', 'MY_LAST_NAME', 'MY_FULL_NAME', 'MY_EMAIL', 'MY_PHONE', 'MY_PHONE_INTL', 'MY_COMPANY', 'MY_WEBSITE', 'OPENAI_API_KEY', 'NOPECHA_API_KEY', 'SPREADSHEET_ID', 'PARALLEL_COUNT', 'NOPECHA_HARD_TIMEOUT', 'DASHBOARD_USER'].forEach(key => {
            const el = document.getElementById('set-' + key); if (el) el.value = cfg[key] || '';
        });
    } catch (e) { toast('Failed to load settings', 'error'); }
}

async function saveSettings() {
    const data = {};
    ['MY_FIRST_NAME', 'MY_LAST_NAME', 'MY_FULL_NAME', 'MY_EMAIL', 'MY_PHONE', 'MY_PHONE_INTL', 'MY_COMPANY', 'MY_WEBSITE', 'OPENAI_API_KEY', 'NOPECHA_API_KEY', 'SPREADSHEET_ID', 'PARALLEL_COUNT', 'NOPECHA_HARD_TIMEOUT', 'DASHBOARD_USER'].forEach(key => {
        const el = document.getElementById('set-' + key);
        if (el) { let val = el.value; if (key === 'PARALLEL_COUNT' || key === 'NOPECHA_HARD_TIMEOUT') val = parseInt(val) || 0; data[key] = val; }
    });
    const pw = document.getElementById('set-new_password'); if (pw && pw.value) data.new_password = pw.value;
    try {
        const res = await fetch('/api/settings', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
        const result = await res.json();
        if (result.success) { toast('Settings saved', 'success'); if (pw) pw.value = ''; } else toast('Failed to save', 'error');
    } catch (e) { toast('Error: ' + e.message, 'error'); }
}

async function uploadCreds() {
    const input = document.getElementById('creds-file-input');
    if (!input.files.length) { toast('Select a JSON file first', 'error'); return; }
    const formData = new FormData(); formData.append('file', input.files[0]);
    try { const res = await fetch('/api/settings/upload-creds', { method: 'POST', body: formData }); const data = await res.json(); if (data.success) toast('Credentials uploaded', 'success'); else toast(data.error || 'Failed', 'error'); } catch (e) { toast('Error: ' + e.message, 'error'); }
}

async function testSheets() {
    const el = document.getElementById('sheets-test-result'); el.textContent = 'Testing...'; el.className = 'ml-3 text-sm text-gray-400';
    try { const res = await fetch('/api/settings/test-sheets'); const data = await res.json(); if (data.success) { el.textContent = `✓ Connected — "${data.title}" (${data.rows} rows)`; el.className = 'ml-3 text-sm text-success-400'; } else { el.textContent = `✕ Failed: ${data.error}`; el.className = 'ml-3 text-sm text-danger-400'; } } catch (e) { el.textContent = '✕ Connection error'; el.className = 'ml-3 text-sm text-danger-400'; }
}

// ============================================================
//   GOOGLE SHEET VIEWER
// ============================================================

async function loadSheetData() {
    try {
        const res = await fetch('/api/sheet-data'); const data = await res.json();
        if (data.error) { toast('Sheet error: ' + data.error, 'error'); return; }
        document.getElementById('sheet-total-rows').textContent = data.total_rows || 0;
        document.getElementById('sheet-success-pct').textContent = (data.success_pct || 0) + '%';
        document.getElementById('sheet-avg-cost').textContent = '$' + (data.avg_cost || 0).toFixed(6);
        sheetData = data.rows || []; sheetPage = 0; renderSheetPage();
    } catch (e) { toast('Failed to load sheet data', 'error'); }
}

function renderSheetPage() {
    const filterSubmitted = document.getElementById('sheet-filter-submitted')?.value || '';
    const filterStatus = document.getElementById('sheet-filter-status')?.value || '';
    let filtered = sheetData;
    if (filterSubmitted) filtered = filtered.filter(r => String(r['Submitted'] || '').toLowerCase() === filterSubmitted.toLowerCase());
    if (filterStatus) filtered = filtered.filter(r => String(r['Submission Status'] || '').toLowerCase() === filterStatus.toLowerCase());
    const totalPages = Math.ceil(filtered.length / SHEET_PAGE_SIZE) || 1;
    if (sheetPage >= totalPages) sheetPage = totalPages - 1; if (sheetPage < 0) sheetPage = 0;
    const start = sheetPage * SHEET_PAGE_SIZE; const pageRows = filtered.slice(start, start + SHEET_PAGE_SIZE);
    const cols = ['Company Name', 'Contact URL', 'Submitted', 'Submission Assurance', 'Captcha Status', 'Proxy Used', 'Bandwidth (KB)', 'Run Timestamp', 'API Calls', 'Input Tokens', 'Output Tokens', 'Total Tokens', 'Est. Cost (USD)', 'Avg Tokens/Call', 'Fields Filled', 'Submission Status', 'Confirmation Msg'];
    document.getElementById('sheet-thead').innerHTML = cols.map(c => `<th class="pb-3 text-left font-medium px-2 whitespace-nowrap">${c}</th>`).join('');
    const tbody = document.getElementById('sheet-body'); tbody.innerHTML = '';
    if (!pageRows.length) { tbody.innerHTML = `<tr><td colspan="${cols.length}" class="py-6 text-center text-gray-600">No data</td></tr>`; }
    else {
        pageRows.forEach(row => {
            const status = String(row['Submission Status'] || '').toLowerCase();
            const rc = status.includes('success') ? 'sheet-row-success' : status.includes('fail') ? 'sheet-row-failed' : '';
            const tr = document.createElement('tr'); tr.className = `border-b border-white/5 ${rc}`;
            tr.innerHTML = cols.map(c => { let val = row[c] ?? '—'; if (c === 'Contact URL' && val && val.startsWith('http')) return `<td class="py-1.5 px-2 max-w-[150px] truncate"><a href="${esc(val)}" target="_blank" class="text-accent-400 hover:text-accent-300">${esc(val)}</a></td>`; if (c === 'Submission Status') return `<td class="py-1.5 px-2">${makeBadge(val)}</td>`; return `<td class="py-1.5 px-2 text-gray-300 max-w-[120px] truncate" title="${esc(String(val))}">${esc(String(val))}</td>`; }).join('');
            tbody.appendChild(tr);
        });
    }
    document.getElementById('sheet-page-info').textContent = `Page ${sheetPage + 1} of ${totalPages} (${filtered.length} rows)`;
    document.getElementById('sheet-filter-submitted').onchange = () => { sheetPage = 0; renderSheetPage(); };
    document.getElementById('sheet-filter-status').onchange = () => { sheetPage = 0; renderSheetPage(); };
}
function sheetPrevPage() { sheetPage--; renderSheetPage(); }
function sheetNextPage() { sheetPage++; renderSheetPage(); }

// ============================================================
//   SCRAPER — Frontend Logic
// ============================================================

function switchScraperTab(tab) {
    document.querySelectorAll('.scraper-input-panel').forEach(el => el.classList.add('hidden'));
    document.getElementById('stab2-' + tab)?.classList.remove('hidden');
    document.querySelectorAll('.scraper-input-tab').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.stab2 === tab);
    });
}

function setupScraperCSVDrop() {
    const dropzone = document.getElementById('scraper-csv-dropzone');
    const fileInput = document.getElementById('scraper-csv-input');
    if (!dropzone || !fileInput) return;
    dropzone.addEventListener('click', () => fileInput.click());
    dropzone.addEventListener('dragover', (e) => { e.preventDefault(); dropzone.classList.add('border-accent-500/50'); });
    dropzone.addEventListener('dragleave', () => dropzone.classList.remove('border-accent-500/50'));
    dropzone.addEventListener('drop', (e) => { e.preventDefault(); dropzone.classList.remove('border-accent-500/50'); if (e.dataTransfer.files.length) handleScraperCSV(e.dataTransfer.files[0]); });
    fileInput.addEventListener('change', () => { if (fileInput.files.length) handleScraperCSV(fileInput.files[0]); });
}

async function handleScraperCSV(file) {
    const formData = new FormData(); formData.append('file', file);
    try {
        const res = await fetch('/api/scraper/upload-csv', { method: 'POST', body: formData });
        const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
        scraperUploadedFilename = data.filename;
        document.getElementById('scraper-csv-count').textContent = data.total_urls;
        document.getElementById('scraper-csv-name').textContent = file.name;
        document.getElementById('scraper-csv-info').classList.remove('hidden');
        toast(`Loaded ${data.total_urls} URLs for scraping`, 'success');
    } catch (e) { toast('Upload failed: ' + e.message, 'error'); }
}

async function startScraper() {
    const urlsText = document.getElementById('scraper-urls-text')?.value || '';
    const workers = parseInt(document.getElementById('scraper-workers')?.value) || 4;
    const body = { workers };

    // Determine input source
    const activeTab = document.querySelector('.scraper-input-panel:not(.hidden)');
    if (activeTab && activeTab.id === 'stab2-csv') {
        if (!scraperUploadedFilename) { toast('Please upload a CSV first', 'error'); return; }
        body.filename = scraperUploadedFilename;
    } else {
        if (!urlsText.trim()) { toast('Please enter at least one URL', 'error'); return; }
        body.urls_text = urlsText;
    }

    try {
        const res = await fetch('/api/scraper/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
        toast('Scraper started!', 'success');
        showScraperProgress();
        startScraperPolling();
        startScraperLogStreaming();
    } catch (e) { toast('Failed to start scraper: ' + e.message, 'error'); }
}

function showScraperProgress() {
    document.getElementById('scraper-progress-section').classList.remove('hidden');
    document.getElementById('scraper-log-section').classList.remove('hidden');
    document.getElementById('btn-start-scraper').disabled = true;
    document.getElementById('btn-start-scraper').classList.add('opacity-50');
    document.getElementById('btn-stop-scraper').classList.remove('hidden');
}

async function stopScraper() {
    if (!confirm('Stop the scraper?')) return;
    try { const res = await fetch('/api/scraper/stop', { method: 'POST' }); const data = await res.json(); if (data.success) toast('Scraper stopped', 'info'); else toast(data.error || 'Failed', 'error'); } catch (e) { toast('Error: ' + e.message, 'error'); }
}

function startScraperPolling() {
    if (scraperPollingInterval) clearInterval(scraperPollingInterval);
    scraperPollingInterval = setInterval(pollScraperStatus, 2000);
}

async function pollScraperStatus() {
    try {
        const res = await fetch('/api/scraper/status'); const data = await res.json();
        updateScraperUI(data);
        if (['completed', 'stopped', 'error', 'idle'].includes(data.status)) {
            clearInterval(scraperPollingInterval); scraperPollingInterval = null;
            document.getElementById('btn-start-scraper').disabled = false;
            document.getElementById('btn-start-scraper').classList.remove('opacity-50');
            document.getElementById('btn-stop-scraper').classList.add('hidden');
            if (data.status !== 'idle') { toast(`Scraper ${data.status}`, data.status === 'completed' ? 'success' : 'error'); loadScraperResults(); }
        }
    } catch (e) { console.error(e); }
}

function updateScraperUI(data) {
    const total = data.total_urls || 1, processed = data.processed || 0;
    const pct = Math.min(Math.round((processed / total) * 100), 100);
    document.getElementById('scraper-progress-text').textContent = `Processing ${processed} of ${total} URLs`;
    document.getElementById('scraper-progress-bar').style.width = pct + '%';
    document.getElementById('scraper-progress-pct').textContent = pct + '%';
}

function startScraperLogStreaming() {
    if (scraperLogEventSource) scraperLogEventSource.close();
    scraperLogEventSource = new EventSource('/api/scraper/logs/stream');
    scraperLogEventSource.onmessage = (event) => {
        try { const data = JSON.parse(event.data); if (data.line) appendLogLine(data.line, 'scraper-log-box'); if (data.done) { scraperLogEventSource.close(); scraperLogEventSource = null; } } catch (e) { }
    };
    scraperLogEventSource.onerror = () => { if (scraperLogEventSource) scraperLogEventSource.close(); scraperLogEventSource = null; };
}

async function checkScraperActive() {
    try {
        const res = await fetch('/api/scraper/status'); const data = await res.json();
        if (data.status === 'running') { showScraperProgress(); updateScraperUI(data); startScraperPolling(); startScraperLogStreaming(); }
    } catch (e) { }
}

async function loadScraperResults() {
    try {
        const res = await fetch('/api/scraper/results'); const data = await res.json();
        document.getElementById('scraper-stat-total').textContent = data.total || 0;
        document.getElementById('scraper-stat-forms').textContent = data.with_form || 0;
        document.getElementById('scraper-stat-captcha').textContent = data.with_captcha || 0;
        document.getElementById('scraper-stat-contact').textContent = data.with_contact || 0;

        const tbody = document.getElementById('scraper-results-body'); tbody.innerHTML = '';
        if (!data.results || !data.results.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="py-6 text-center text-gray-600">No results yet. Run the scraper to find contact forms.</td></tr>';
            return;
        }
        data.results.forEach(r => {
            const tr = document.createElement('tr'); tr.className = 'border-b border-white/5';
            const formBadge = r['Has Form'] === 'Yes' ? '<span class="badge badge-form-yes">Yes</span>' : '<span class="badge badge-form-no">No</span>';
            const captchaBadge = r['Has Captcha'] === 'Yes' ? '<span class="badge badge-captcha">Yes</span>' : '<span class="badge badge-neutral">No</span>';
            const method = r['Method'] || '';
            const methodClass = method.startsWith('error') ? 'method-error' : 'method-' + method.replace(/[^a-z_]/g, '');
            tr.innerHTML = `<td class="py-2 text-white max-w-[140px] truncate">${esc(r['Company Name'])}</td><td class="py-2 text-gray-400 max-w-[140px] truncate"><a href="${esc(r['Input URL'])}" target="_blank" class="hover:text-accent-400">${esc(r['Input URL'])}</a></td><td class="py-2 text-gray-400 max-w-[160px] truncate"><a href="${esc(r['Contact URL Found'] || '')}" target="_blank" class="hover:text-accent-400">${esc(r['Contact URL Found'] || '—')}</a></td><td class="py-2">${formBadge}</td><td class="py-2 text-gray-400 text-xs max-w-[140px] truncate" title="${esc(r['Form Fields'] || '')}">${esc(r['Form Fields'] || '—')}</td><td class="py-2">${captchaBadge}</td><td class="py-2 text-gray-400 text-xs max-w-[120px] truncate">${esc(r['Emails Found'] || '—')}</td><td class="py-2 text-xs ${methodClass}">${esc(method)}</td>`;
            tbody.appendChild(tr);
        });
    } catch (e) { console.error('Scraper results error:', e); }
}

function exportScraperResults() { window.open('/api/scraper/export', '_blank'); }

async function sendToOutreach() {
    try {
        const res = await fetch('/api/scraper/send-to-outreach', { method: 'POST' });
        const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
        toast(`${data.total_leads} leads ready for outreach! Switching to Run page...`, 'success');
        uploadedFilename = data.filename;
        // Load the preview on Run page
        setTimeout(() => {
            switchPage('run');
            // Trigger preview load
            fetch('/api/upload-csv', { method: 'POST', body: (() => { const fd = new FormData(); return fd; })() }).catch(() => { });
            document.getElementById('csv-total-leads').textContent = data.total_leads;
            document.getElementById('csv-preview-section').classList.remove('hidden');
            document.getElementById('csv-preview-body').innerHTML = `<tr class="border-b border-white/5"><td colspan="3" class="py-3 text-center text-gray-400">${data.total_leads} leads from scraper results (${data.filename})</td></tr>`;
        }, 500);
    } catch (e) { toast('Error: ' + e.message, 'error'); }
}

// ============================================================
//   UTILITIES
// ============================================================

function esc(str) { if (!str) return ''; const d = document.createElement('div'); d.textContent = String(str); return d.innerHTML; }

function makeBadge(status) {
    if (!status || status === '—') return '<span class="badge badge-neutral">—</span>';
    const s = String(status).toLowerCase();
    if (s.includes('success')) return `<span class="badge badge-success">${esc(status)}</span>`;
    if (s.includes('fail')) return `<span class="badge badge-danger">${esc(status)}</span>`;
    return `<span class="badge badge-neutral">${esc(status)}</span>`;
}
