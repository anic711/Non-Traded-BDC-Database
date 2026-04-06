// Non-Traded BDC Metrics Dashboard

const state = {
    activeTab: 'gross-sales',
    period: 'monthly',
    start: null,
    end: null,
    data: null,
};

const ENDPOINTS = {
    'gross-sales': '/api/dashboard/gross-sales',
    'redemptions': '/api/dashboard/redemptions',
    'performance': '/api/dashboard/performance',
    'redemption-requests': '/api/dashboard/redemption-requests',
};

// --- Initialization ---

function init() {
    // Set default dates: last 12 months
    const now = new Date();
    const endMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const startDate = new Date(now.getFullYear() - 1, now.getMonth(), 1);
    const startMonth = `${startDate.getFullYear()}-${String(startDate.getMonth() + 1).padStart(2, '0')}`;

    document.getElementById('start-date').value = startMonth;
    document.getElementById('end-date').value = endMonth;
    state.start = startMonth;
    state.end = endMonth;

    // Tab listeners
    document.querySelectorAll('.tab').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.activeTab = btn.dataset.tab;
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
        fetchData();
    });
    document.getElementById('end-date').addEventListener('change', e => {
        state.end = e.target.value;
        fetchData();
    });

    // Refresh button
    document.getElementById('refresh-btn').addEventListener('click', refreshData);

    fetchData();
}

// --- Data Refresh ---

async function refreshData() {
    const btn = document.getElementById('refresh-btn');
    btn.disabled = true;
    btn.textContent = 'Refreshing...';
    try {
        const resp = await fetch('/api/update/trigger', { method: 'POST' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const result = await resp.json();
        btn.textContent = 'Done!';
        setTimeout(() => {
            btn.textContent = 'Refresh Data';
            btn.disabled = false;
            fetchData();
        }, 2000);
    } catch (err) {
        btn.textContent = 'Error';
        setTimeout(() => {
            btn.textContent = 'Refresh Data';
            btn.disabled = false;
        }, 3000);
    }
}

// --- Data Fetching ---

async function fetchData() {
    const loading = document.getElementById('loading');
    const container = document.getElementById('grid-container');
    loading.style.display = 'block';
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
            sub.textContent = ' ' + bank.subtitle;
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
        el.textContent = formatPercent(num);
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

function formatPercent(val) {
    return (val * 100).toFixed(1) + '%';
}

function formatDateLabel(dateStr) {
    // "2025-03-31" -> "Mar 2025"
    const d = new Date(dateStr + 'T00:00:00');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return months[d.getMonth()] + ' ' + d.getFullYear();
}

// --- Start ---
document.addEventListener('DOMContentLoaded', init);
