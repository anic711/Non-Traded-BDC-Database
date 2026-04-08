// Non-Traded BDC Metrics Dashboard

const state = {
    activeTab: 'gross-sales',
    period: 'monthly',
    start: null,
    end: null,
    data: null,
    presetYears: 1,
};

const ENDPOINTS = {
    'gross-sales': '/api/dashboard/gross-sales',
    'redemptions': '/api/dashboard/redemptions',
    'performance': '/api/dashboard/performance',
    'redemption-requests': '/api/dashboard/redemption-requests',
    'net-flows': '/api/dashboard/net-flows',
};

// --- Initialization ---

function init() {
    setDatePreset(1);

    // Tab listeners
    document.querySelectorAll('.tab').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.activeTab = btn.dataset.tab;
            updatePeriodToggle();
            const defaultYears = TAB_DEFAULT_YEARS[state.activeTab] || 1;
            setDatePreset(defaultYears);
            fetchData();
        });
    });

    // Period toggle
    document.querySelectorAll('.period-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.period = btn.dataset.period;
            fetchData();
        });
    });

    // Date inputs
    document.getElementById('start-date').addEventListener('change', e => {
        state.start = e.target.value;
        clearPresetHighlight();
        fetchData();
    });
    document.getElementById('end-date').addEventListener('change', e => {
        state.end = e.target.value;
        clearPresetHighlight();
        fetchData();
    });

    // Preset buttons (1Y, 2Y, 3Y)
    document.querySelectorAll('.preset-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const years = parseInt(btn.dataset.years);
            setDatePreset(years);
            fetchData();
        });
    });

    // Refresh button
    document.getElementById('refresh-btn').addEventListener('click', refreshData);

    // Export button
    document.getElementById('export-btn').addEventListener('click', exportXlsx);

    // Check update status on load
    checkUpdateStatus();

    fetchData();
}

// --- Date Presets ---

function setDatePreset(years) {
    state.presetYears = years;
    const now = new Date();
    const endMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const startDate = new Date(now.getFullYear() - years, now.getMonth(), 1);
    const startMonth = `${startDate.getFullYear()}-${String(startDate.getMonth() + 1).padStart(2, '0')}`;

    document.getElementById('start-date').value = startMonth;
    document.getElementById('end-date').value = endMonth;
    state.start = startMonth;
    state.end = endMonth;

    document.querySelectorAll('.preset-btn').forEach(b => {
        b.classList.toggle('active', parseInt(b.dataset.years) === years);
    });
}

function clearPresetHighlight() {
    state.presetYears = null;
    document.querySelectorAll('.preset-btn').forEach(b => b.classList.remove('active'));
}

// Tabs where data is always quarterly (no period toggle)
const QUARTERLY_ONLY_TABS = ['redemptions', 'redemption-requests', 'net-flows'];

// Default date range per tab
const TAB_DEFAULT_YEARS = {
    'redemptions': 2,
    'redemption-requests': 2,
    'net-flows': 2,
};

function updatePeriodToggle() {
    const toggle = document.getElementById('period-toggle');
    if (QUARTERLY_ONLY_TABS.includes(state.activeTab)) {
        toggle.style.display = 'none';
    } else {
        toggle.style.display = '';
    }
}

// --- Update Status ---

async function checkUpdateStatus() {
    const light = document.getElementById('status-light');
    try {
        const resp = await fetch('/api/update/latest');
        if (!resp.ok) return;
        const data = await resp.json();
        if (!data) return;
        if (data.status === 'running') {
            light.className = 'status-light running';
            light.title = 'Update in progress...';
            // Poll until complete
            setTimeout(checkUpdateStatus, 5000);
        } else if (data.status === 'success' || data.status === 'completed') {
            light.className = 'status-light ok';
            const ts = data.completed_at || data.started_at;
            light.title = ts ? `Last updated: ${new Date(ts).toLocaleString()}` : 'Up to date';
        } else if (data.status === 'error' || data.status === 'failed') {
            light.className = 'status-light error';
            light.title = 'Last update failed';
        } else {
            light.className = 'status-light ok';
            light.title = 'Status: ' + data.status;
        }
    } catch {
        // Ignore errors — status light stays gray
    }
}

// --- Data Refresh ---

async function refreshData() {
    const btn = document.getElementById('refresh-btn');
    const light = document.getElementById('status-light');
    btn.disabled = true;
    btn.classList.add('spinning');
    light.className = 'status-light running';

    try {
        const resp = await fetch('/api/update/trigger', { method: 'POST' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        // Poll for completion
        pollRefreshStatus(btn);
    } catch (err) {
        light.className = 'status-light error';
        light.title = 'Refresh failed';
        btn.classList.remove('spinning');
        btn.disabled = false;
    }
}

async function pollRefreshStatus(btn) {
    try {
        const resp = await fetch('/api/update/latest');
        if (resp.ok) {
            const data = await resp.json();
            if (data && (data.status === 'running' || data.status === 'started')) {
                setTimeout(() => pollRefreshStatus(btn), 3000);
                return;
            }
        }
    } catch { /* ignore */ }

    // Done (success or failure) — refresh data
    btn.classList.remove('spinning');
    btn.disabled = false;
    checkUpdateStatus();
    fetchData();
}

// --- XLSX Export ---

async function exportXlsx() {
    const btn = document.getElementById('export-btn');
    btn.disabled = true;
    const origText = btn.innerHTML;

    try {
        const url = new URL('/api/dashboard/export', window.location.origin);
        if (state.start) url.searchParams.set('start', state.start);
        if (state.end) url.searchParams.set('end', state.end);
        url.searchParams.set('period', state.period);

        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

        const blob = await resp.blob();
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        const cd = resp.headers.get('content-disposition');
        a.download = cd ? cd.split('filename=')[1].replace(/"/g, '') : 'bdc_metrics.xlsx';
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(a.href);
    } catch (err) {
        console.error('Export failed:', err);
    } finally {
        btn.disabled = false;
        btn.innerHTML = origText;
    }
}

// --- Data Fetching ---

async function fetchData() {
    const loading = document.getElementById('loading');
    const container = document.getElementById('grid-container');
    loading.style.display = 'flex';
    container.innerHTML = '';

    const url = new URL(ENDPOINTS[state.activeTab], window.location.origin);
    if (state.start) url.searchParams.set('start', state.start);
    if (state.end) url.searchParams.set('end', state.end);
    url.searchParams.set('period', state.period);

    try {
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        state.data = await resp.json();
        renderGrid();
    } catch (err) {
        container.innerHTML = `<div class="loading">Error: ${err.message}</div>`;
    } finally {
        loading.style.display = 'none';
    }
}

// --- Rendering ---

function renderGrid() {
    const container = document.getElementById('grid-container');
    container.innerHTML = '';

    if (!state.data || !state.data.banks) return;

    const { funds, banks } = state.data;
    const fundRows = [...funds, 'Total'];

    for (const bank of banks) {
        const section = document.createElement('div');
        section.className = 'bank';

        const header = document.createElement('div');
        header.className = 'bank-header';
        header.textContent = bank.name;
        if (bank.subtitle) {
            const sub = document.createElement('span');
            sub.className = 'bank-subtitle';
            sub.textContent = '\u2014 ' + bank.subtitle;
            header.appendChild(sub);
        }
        section.appendChild(header);

        const wrapper = document.createElement('div');
        wrapper.className = 'table-wrapper';

        const table = document.createElement('table');
        table.className = 'data-table';

        const rows = bank.rows || [];

        // Header row: Fund | date1 | date2 | ...
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        headerRow.appendChild(th(''));
        for (const row of rows) {
            headerRow.appendChild(th(formatDateLabel(row.date)));
        }
        thead.appendChild(headerRow);
        table.appendChild(thead);

        // One row per fund + Total
        const tbody = document.createElement('tbody');
        for (const fund of fundRows) {
            const tr = document.createElement('tr');
            if (fund === 'Total') tr.className = 'total-row';
            tr.appendChild(td(fund, 'text'));
            for (const row of rows) {
                tr.appendChild(td(row[fund], bank.format));
            }
            tbody.appendChild(tr);
        }
        table.appendChild(tbody);
        wrapper.appendChild(table);
        section.appendChild(wrapper);
        container.appendChild(section);
    }
}

function th(text) {
    const el = document.createElement('th');
    el.textContent = text;
    return el;
}

function td(value, format) {
    const el = document.createElement('td');
    if (value === null || value === undefined) {
        el.textContent = '-';
        el.className = 'val-na';
        return el;
    }
    if (value === 'N/A') {
        el.textContent = 'N/A';
        el.className = 'val-na';
        return el;
    }
    if (format === 'text') {
        el.textContent = value;
        return el;
    }

    const num = Number(value);
    if (isNaN(num)) {
        el.textContent = value;
        return el;
    }

    if (format === 'currency') {
        el.textContent = formatCurrency(num);
    } else if (format === 'percent') {
        el.textContent = (num * 100).toFixed(0) + '%';
        if (num > 0) el.className = 'val-positive';
        else if (num < 0) el.className = 'val-negative';
    } else if (format === 'percent1') {
        el.textContent = (num * 100).toFixed(1) + '%';
    } else if (format === 'percent1_color') {
        el.textContent = (num * 100).toFixed(1) + '%';
        if (num > 0) el.className = 'val-positive';
        else if (num < 0) el.className = 'val-negative';
    } else if (format === 'number') {
        el.textContent = formatNumber(num);
    } else {
        el.textContent = num.toLocaleString();
    }
    return el;
}

// --- Formatters ---

function addCommas(n) {
    return n.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function formatCurrency(val) {
    return '$' + addCommas((val / 1e6).toFixed(0)) + 'M';
}

function formatNumber(val) {
    if (Math.abs(val) >= 1e6) return addCommas((val / 1e6).toFixed(1)) + 'M';
    if (Math.abs(val) >= 1e3) return addCommas((val / 1e3).toFixed(0)) + 'K';
    return addCommas(val.toFixed(0));
}

function formatDateLabel(dateStr) {
    // "2025-03-31" -> "Mar 2025"
    const d = new Date(dateStr + 'T00:00:00');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return months[d.getMonth()] + ' ' + d.getFullYear();
}

// --- Start ---
document.addEventListener('DOMContentLoaded', init);
