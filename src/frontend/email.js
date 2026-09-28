// Email update: builds an ASIF-focused summary email (text + charts) from the
// dashboard API, previews it in a modal, and lets the user copy it or download
// it as an Outlook draft (.eml).

const EMAIL_FOCUS = 'ASIF';
const EMAIL_CHART_START = '2024-01';
const CHART_JS_URL = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js';

const FOCUS_COLOR = '#3b5bdb';
const PEER_COLORS = ['#8b95a7', '#c9a27e', '#8fb3a0', '#b7a6c9', '#c7ced9', '#d9b8a3'];

let emailDraft = null;

// --- Entry point ---

async function openEmailUpdate() {
    const btn = document.getElementById('email-btn');
    btn.disabled = true;
    showEmailModal('<div class="loading"><div class="loading-spinner"></div><span>Building email...</span></div>');
    try {
        emailDraft = await buildEmailDraft();
        renderEmailPreview(emailDraft);
    } catch (err) {
        console.error('Email build failed:', err);
        setModalBody(`<div class="loading">Couldn't build the email: ${escapeHtml(err.message)}</div>`);
    } finally {
        btn.disabled = false;
    }
}

// --- Data ---

function quarterEndIso(d) {
    const qm = Math.floor(d.getMonth() / 3) * 3 + 3;
    const last = new Date(d.getFullYear(), qm, 0);
    return `${last.getFullYear()}-${String(qm).padStart(2, '0')}-${String(last.getDate()).padStart(2, '0')}`;
}

async function fetchDashboard(path, period) {
    const url = new URL(path, window.location.origin);
    url.searchParams.set('start', EMAIL_CHART_START);
    // End of the current quarter, so a just-filed quarter-end tender is included
    url.searchParams.set('end', quarterEndIso(new Date()));
    url.searchParams.set('period', period);
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status} from ${path}`);
    return resp.json();
}

function bankByName(data, name) {
    const bank = data.banks.find(b => b.name === name);
    if (!bank) throw new Error(`Missing "${name}" data`);
    return bank;
}

function num(v) {
    if (v === null || v === undefined || v === 'N/A') return null;
    const n = Number(v);
    return isNaN(n) ? null : n;
}

function rowFor(bank, dateStr) {
    return bank.rows.find(r => r.date === dateStr) || {};
}

async function buildEmailDraft() {
    const [sales, requests] = await Promise.all([
        fetchDashboard(ENDPOINTS['gross-sales'], 'monthly'),
        fetchDashboard(ENDPOINTS['redemption-requests'], 'quarterly'),
    ]);
    const funds = sales.funds;
    const peers = funds.filter(f => f !== EMAIL_FOCUS);

    const salesText = buildSalesSection(sales, peers);
    const requestsText = buildRequestsSection(requests, peers);

    await loadChartJs();
    const salesChart = renderSalesChart(bankByName(sales, 'Gross Sales'), funds);
    const requestsChart = renderRequestsChart(bankByName(requests, '% of Shares O/S (t-1)'), funds);

    const subject = `Non-Traded BDC Update: ${EMAIL_FOCUS} ${salesText.periodLabel} Sales / ${requestsText.periodLabel} Redemption Requests`;

    return {
        subject,
        sections: [salesText, requestsText],
        peers,
        charts: [
            { cid: 'gross-sales-chart', title: `Monthly Gross Sales ($M), Jan '24 – ${salesText.periodLabel}`, dataUrl: salesChart },
            { cid: 'redemption-requests-chart', title: `Quarterly Redemption Requests (% of Shares O/S, t-1), 1Q24 – ${requestsText.periodLabel}`, dataUrl: requestsChart },
        ],
    };
}

// --- Commentary ---

function fmtM(v) {
    return '$' + addCommas(Math.round(v / 1e6).toString()) + 'M';
}

function fmtPct(v, digits = 0) {
    return (v * 100).toFixed(digits) + '%';
}

function fmtChange(v) {
    return (v >= 0 ? '+' : '−') + Math.abs(v * 100).toFixed(0) + '%';
}

function monthLabel(dateStr) {
    return formatDateLabel(dateStr);
}

function shortMonthLabel(dateStr) {
    const [label, year] = formatDateLabel(dateStr).split(' ');
    return `${label} '${year.slice(2)}`;
}

function quarterLabel(dateStr) {
    const d = new Date(dateStr + 'T00:00:00');
    return `${Math.floor(d.getMonth() / 3) + 1}Q${String(d.getFullYear()).slice(2)}`;
}

function ordinal(n) {
    return ['1st', '2nd', '3rd'][n - 1] || `${n}th`;
}

function lastRowWith(bank, fund) {
    for (let i = bank.rows.length - 1; i >= 0; i--) {
        if (num(bank.rows[i][fund]) !== null) return i;
    }
    return -1;
}

function buildSalesSection(data, peers) {
    const gs = bankByName(data, 'Gross Sales');
    const yoy = bankByName(data, 'Y/Y Growth');
    const t3m = bankByName(data, 'Y/Y Growth - 3M Trailing');
    const pctNav = bankByName(data, '% of NAV (t-1)');

    const i = lastRowWith(gs, EMAIL_FOCUS);
    if (i < 0) throw new Error(`No ${EMAIL_FOCUS} gross sales data`);
    const row = gs.rows[i];
    const d = row.date;
    const label = monthLabel(d);
    const bullets = [];

    // ASIF headline
    const val = num(row[EMAIL_FOCUS]);
    let s = `${EMAIL_FOCUS} raised <b>${fmtM(val)}</b> in ${label}`;
    const prev = i > 0 ? num(gs.rows[i - 1][EMAIL_FOCUS]) : null;
    if (prev !== null && prev > 0) {
        s += `, vs. ${fmtM(prev)} in ${monthLabel(gs.rows[i - 1].date)} (${fmtChange(val / prev - 1)} M/M)`;
    }
    const y = num(rowFor(yoy, d)[EMAIL_FOCUS]);
    const t = num(rowFor(t3m, d)[EMAIL_FOCUS]);
    if (y !== null) {
        s += `${s.includes('M/M') ? ' and' : ','} ${fmtChange(y)} Y/Y`;
        if (t !== null) s += ` (${fmtChange(t)} on a trailing 3-month basis)`;
    }
    bullets.push(s + '.');

    // % of NAV vs. group
    const pn = num(rowFor(pctNav, d)[EMAIL_FOCUS]);
    const pnAll = num(rowFor(pctNav, d).Total);
    if (pn !== null) {
        let p = `That equates to ${fmtPct(pn, 1)} of prior-period NAV`;
        if (pnAll !== null) p += `, vs. ${fmtPct(pnAll, 1)} for the five-fund group`;
        bullets.push(p + '.');
    }

    // Rank and share
    const all = [EMAIL_FOCUS, ...peers]
        .map(f => ({ f, v: num(row[f]) }))
        .filter(x => x.v !== null)
        .sort((a, b) => b.v - a.v);
    const total = all.reduce((acc, x) => acc + x.v, 0);
    if (all.length > 1 && total > 0) {
        const rank = all.findIndex(x => x.f === EMAIL_FOCUS) + 1;
        bullets.push(`${EMAIL_FOCUS} ranked ${ordinal(rank)} of ${all.length} funds and accounted for ${fmtPct(val / total)} of group gross sales (${fmtM(total)} total).`);
    }

    // Peer context
    const peerVals = peers.map(f => ({ f, v: num(row[f]) })).filter(x => x.v !== null);
    if (peerVals.length) {
        const peerSum = peerVals.reduce((acc, x) => acc + x.v, 0);
        let p = `Peers (ex-${EMAIL_FOCUS}) raised ${fmtM(peerSum)} combined`;
        const prevRow = gs.rows[i - 12];
        if (prevRow) {
            const prevSum = peerVals.reduce((acc, x) => acc + (num(prevRow[x.f]) || 0), 0);
            if (prevSum > 0) p += ` (${fmtChange(peerSum / prevSum - 1)} Y/Y)`;
        }
        p += ': ' + peerVals.map(x => `${x.f} ${fmtM(x.v)}`).join(', ');
        bullets.push(p + '.');
    }
    const missing = peers.filter(f => num(row[f]) === null);
    if (missing.length) bullets.push(`Not yet reported for ${label}: ${missing.join(', ')}.`);

    return { heading: `Gross Sales – ${label}`, periodLabel: shortMonthLabel(d), bullets };
}

function buildRequestsSection(data, peers) {
    const pctOs = bankByName(data, '% of Shares O/S (t-1)');
    const value = bankByName(data, 'Value of Shares Tendered');
    const fulfilled = bankByName(data, '% Fulfilled');

    const i = lastRowWith(pctOs, EMAIL_FOCUS);
    if (i < 0) throw new Error(`No ${EMAIL_FOCUS} redemption request data`);
    const row = pctOs.rows[i];
    const d = row.date;
    const label = quarterLabel(d);
    const bullets = [];

    const pct = num(row[EMAIL_FOCUS]);
    let s = `${EMAIL_FOCUS} redemption requests were <b>${fmtPct(pct, 1)}</b> of shares outstanding in ${label}`;
    const prev = i > 0 ? num(pctOs.rows[i - 1][EMAIL_FOCUS]) : null;
    if (prev !== null) s += `, vs. ${fmtPct(prev, 1)} in ${quarterLabel(pctOs.rows[i - 1].date)}`;
    const yearAgo = i >= 4 ? num(pctOs.rows[i - 4][EMAIL_FOCUS]) : null;
    if (yearAgo !== null) s += ` and ${fmtPct(yearAgo, 1)} in ${quarterLabel(pctOs.rows[i - 4].date)}`;
    const v = num(rowFor(value, d)[EMAIL_FOCUS]);
    if (v !== null) s += ` (~${fmtM(v)} tendered)`;
    bullets.push(s + '.');

    const f = num(rowFor(fulfilled, d)[EMAIL_FOCUS]);
    if (f !== null) bullets.push(`${fmtPct(f)} of ${EMAIL_FOCUS}'s tendered shares were accepted.`);

    const peerVals = r => peers.map(p => ({ f: p, v: num(r[p]) })).filter(x => x.v !== null);
    const current = peerVals(row);
    const missing = peers.filter(p => num(row[p]) === null);

    if (missing.length === 0) {
        bullets.push(peerComparison(current, pct, label));
    } else {
        if (current.length) {
            bullets.push(`Peers reported so far for ${label}: ${current.map(x => `${x.f} ${fmtPct(x.v, 1)}`).join(', ')} (not yet reported: ${missing.join(', ')}).`);
        } else {
            bullets.push(`No peers have reported ${label} yet (${missing.join(', ')}).`);
        }
        // Compare against the latest quarter where every fund has reported
        for (let j = i - 1; j >= 0; j--) {
            const r = pctOs.rows[j];
            const full = peerVals(r);
            const focusPct = num(r[EMAIL_FOCUS]);
            if (full.length === peers.length && focusPct !== null) {
                bullets.push(`In ${quarterLabel(r.date)}, the latest quarter with all funds reported, ${peerComparison(full, focusPct, quarterLabel(r.date), true)}`);
                break;
            }
        }
    }

    return { heading: `Redemption Requests – ${label}`, periodLabel: label, bullets };
}

function peerComparison(peerVals, focusPct, label, lowercase = false) {
    const avg = peerVals.reduce((acc, x) => acc + x.v, 0) / peerVals.length;
    const sorted = [...peerVals].sort((a, b) => a.v - b.v);
    const lo = sorted[0], hi = sorted[sorted.length - 1];
    let p = `${lowercase ? 'peers' : 'Peers'} averaged ${fmtPct(avg, 1)}`;
    if (peerVals.length > 1) p += ` (range: ${lo.f} ${fmtPct(lo.v, 1)} to ${hi.f} ${fmtPct(hi.v, 1)})`;
    return p + `, vs. ${fmtPct(focusPct, 1)} for ${EMAIL_FOCUS}.`;
}

// --- Charts ---

function loadChartJs() {
    if (window.Chart) return Promise.resolve();
    return new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.src = CHART_JS_URL;
        s.onload = resolve;
        s.onerror = () => reject(new Error('Could not load the chart library'));
        document.head.appendChild(s);
    });
}

const whiteBackground = {
    id: 'whiteBackground',
    beforeDraw(chart) {
        const { ctx, width, height } = chart;
        ctx.save();
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(0, 0, width, height);
        ctx.restore();
    },
};

function fundColor(fund, peers) {
    if (fund === EMAIL_FOCUS) return FOCUS_COLOR;
    return PEER_COLORS[peers.indexOf(fund) % PEER_COLORS.length];
}

function drawChart(config) {
    const canvas = document.createElement('canvas');
    canvas.width = 1280;
    canvas.height = 620;
    const chart = new Chart(canvas.getContext('2d'), {
        ...config,
        plugins: [whiteBackground],
        options: {
            ...config.options,
            responsive: false,
            animation: false,
            devicePixelRatio: 1,
        },
    });
    const url = canvas.toDataURL('image/png');
    chart.destroy();
    return url;
}

const CHART_FONT = { family: 'Arial, sans-serif', size: 20 };

function baseChartOptions(yTickFormat) {
    return {
        layout: { padding: 16 },
        plugins: {
            legend: { position: 'top', labels: { font: CHART_FONT, color: '#1a1d2e', boxWidth: 22, padding: 18 } },
        },
        scales: {
            x: { grid: { display: false }, ticks: { font: { ...CHART_FONT, size: 17 }, color: '#4b5563' } },
            y: {
                grid: { color: '#e5e7eb' },
                ticks: { font: { ...CHART_FONT, size: 17 }, color: '#4b5563', callback: yTickFormat },
            },
        },
    };
}

function renderSalesChart(bank, funds) {
    const peers = funds.filter(f => f !== EMAIL_FOCUS);
    // Trim trailing months with no data for the focus fund
    const end = lastRowWith(bank, EMAIL_FOCUS);
    const rows = bank.rows.slice(0, end + 1);
    // Focus fund at the bottom of each stack so its trend reads cleanly
    const order = [EMAIL_FOCUS, ...peers];
    const opts = baseChartOptions(v => '$' + addCommas(String(v)));
    opts.scales.x.stacked = true;
    opts.scales.y.stacked = true;
    opts.scales.x.ticks.maxRotation = 90;
    opts.scales.x.ticks.minRotation = 90;
    return drawChart({
        type: 'bar',
        data: {
            labels: rows.map(r => shortMonthLabel(r.date)),
            datasets: order.map(f => ({
                label: f,
                data: rows.map(r => {
                    const v = num(r[f]);
                    return v === null ? null : Math.round(v / 1e6);
                }),
                backgroundColor: fundColor(f, peers),
                borderWidth: 0,
                categoryPercentage: 0.8,
                barPercentage: 0.95,
            })),
        },
        options: opts,
    });
}

function renderRequestsChart(bank, funds) {
    const peers = funds.filter(f => f !== EMAIL_FOCUS);
    const end = lastRowWith(bank, EMAIL_FOCUS);
    const rows = bank.rows.slice(0, end + 1);
    const order = [EMAIL_FOCUS, ...peers];
    const opts = baseChartOptions(v => v + '%');
    opts.scales.y.beginAtZero = true;
    return drawChart({
        type: 'line',
        data: {
            labels: rows.map(r => quarterLabel(r.date)),
            datasets: order.map(f => {
                const focus = f === EMAIL_FOCUS;
                return {
                    label: f,
                    data: rows.map(r => {
                        const v = num(r[f]);
                        return v === null ? null : +(v * 100).toFixed(1);
                    }),
                    borderColor: fundColor(f, peers),
                    backgroundColor: fundColor(f, peers),
                    borderWidth: focus ? 5 : 2.5,
                    pointRadius: focus ? 6 : 3,
                    order: focus ? 0 : 1,
                    spanGaps: false,
                };
            }),
        },
        options: opts,
    });
}

// --- Email HTML ---

function emailHtml(draft, imgSrc) {
    const font = "font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#1a1d2e;";
    let html = `<div style="${font}">`;
    html += `<p style="margin:0 0 12px;">Hi,</p>`;
    html += `<p style="margin:0 0 12px;">Below is the latest on ${EMAIL_FOCUS}, with the other non-traded BDCs we track (${draft.peers.join(', ')}) for context.</p>`;
    for (const sec of draft.sections) {
        html += `<p style="margin:16px 0 6px;font-weight:bold;">${sec.heading}</p><ul style="margin:0 0 12px;padding-left:22px;">`;
        for (const b of sec.bullets) html += `<li style="margin:0 0 4px;">${b}</li>`;
        html += `</ul>`;
    }
    html += `<p style="margin:16px 0 12px;">Charts below.</p>`;
    for (const c of draft.charts) {
        html += `<p style="margin:18px 0 6px;font-weight:bold;">${c.title}</p>`;
        html += `<img src="${imgSrc(c)}" width="640" style="width:640px;max-width:100%;height:auto;border:1px solid #e2e5eb;" alt="${escapeHtml(c.title)}">`;
    }
    html += `<p style="margin:16px 0 0;font-size:9pt;color:#6b7280;">Source: SEC filings (8-K, 10-Q/10-K, SC TO-I), compiled in the Non-Traded BDC Metrics dashboard.</p>`;
    html += `</div>`;
    return html;
}

function emailPlainText(draft) {
    const strip = s => s.replace(/<[^>]+>/g, '');
    let t = `Hi,\n\nBelow is the latest on ${EMAIL_FOCUS}, with the other non-traded BDCs we track (${draft.peers.join(', ')}) for context.\n`;
    for (const sec of draft.sections) {
        t += `\n${sec.heading}\n` + sec.bullets.map(b => `- ${strip(b)}`).join('\n') + '\n';
    }
    return t;
}

// --- Modal ---

function showEmailModal(bodyHtml) {
    let overlay = document.getElementById('email-modal');
    if (!overlay) {
        overlay = document.createElement('div');
        overlay.id = 'email-modal';
        overlay.className = 'modal-overlay';
        overlay.innerHTML = `
            <div class="modal" role="dialog" aria-modal="true" aria-labelledby="email-modal-title">
                <div class="modal-header">
                    <div class="modal-title" id="email-modal-title">Email Update</div>
                    <button class="modal-close" aria-label="Close">&times;</button>
                </div>
                <div class="modal-body" id="email-modal-body"></div>
                <div class="modal-footer" id="email-modal-footer"></div>
            </div>`;
        document.body.appendChild(overlay);
        overlay.addEventListener('click', e => { if (e.target === overlay) closeEmailModal(); });
        overlay.querySelector('.modal-close').addEventListener('click', closeEmailModal);
        document.addEventListener('keydown', e => { if (e.key === 'Escape') closeEmailModal(); });
    }
    overlay.style.display = 'flex';
    document.getElementById('email-modal-footer').innerHTML = '';
    setModalBody(bodyHtml);
}

function setModalBody(html) {
    document.getElementById('email-modal-body').innerHTML = html;
}

function closeEmailModal() {
    const overlay = document.getElementById('email-modal');
    if (overlay) overlay.style.display = 'none';
}

function renderEmailPreview(draft) {
    setModalBody(`
        <div class="email-subject-row">
            <span class="email-subject-label">Subject</span>
            <span class="email-subject" id="email-subject">${escapeHtml(draft.subject)}</span>
        </div>
        <div class="email-preview">${emailHtml(draft, c => c.dataUrl)}</div>`);
    const footer = document.getElementById('email-modal-footer');
    footer.innerHTML = `
        <span class="modal-status" id="email-status"></span>
        <button class="action-btn" id="email-copy-subject">Copy subject</button>
        <button class="action-btn" id="email-download">Download for Outlook</button>
        <button class="action-btn primary-btn" id="email-copy">Copy email</button>`;
    document.getElementById('email-copy').addEventListener('click', copyEmail);
    document.getElementById('email-copy-subject').addEventListener('click', copySubject);
    document.getElementById('email-download').addEventListener('click', downloadEml);
}

function setStatus(msg) {
    document.getElementById('email-status').textContent = msg;
}

async function copyEmail() {
    const html = emailHtml(emailDraft, c => c.dataUrl);
    try {
        await navigator.clipboard.write([new ClipboardItem({
            'text/html': new Blob([html], { type: 'text/html' }),
            'text/plain': new Blob([emailPlainText(emailDraft)], { type: 'text/plain' }),
        })]);
        setStatus('Copied — paste into a new email.');
    } catch {
        // Fallback: select the rendered preview and use the legacy copy command
        const range = document.createRange();
        range.selectNodeContents(document.querySelector('.email-preview'));
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
        const ok = document.execCommand('copy');
        sel.removeAllRanges();
        setStatus(ok ? 'Copied — paste into a new email.' : 'Copy failed — try "Download for Outlook".');
    }
}

async function copySubject() {
    try {
        await navigator.clipboard.writeText(emailDraft.subject);
        setStatus('Subject copied.');
    } catch {
        setStatus('Copy failed.');
    }
}

// Build a MIME draft with the charts embedded as inline attachments.
// "X-Unsent: 1" makes Outlook open it as an editable, unsent draft.
function downloadEml() {
    const boundary = '----=_bdc_' + Date.now().toString(36);
    const wrap = s => s.replace(/(.{76})/g, '$1\r\n');
    const b64utf8 = s => btoa(unescape(encodeURIComponent(s)));
    const html = `<!DOCTYPE html><html><head><meta charset="utf-8"></head><body>${emailHtml(emailDraft, c => 'cid:' + c.cid)}</body></html>`;

    const parts = [
        'X-Unsent: 1',
        `Subject: =?UTF-8?B?${b64utf8(emailDraft.subject)}?=`,
        'MIME-Version: 1.0',
        `Content-Type: multipart/related; boundary="${boundary}"; type="text/html"`,
        '',
        `--${boundary}`,
        'Content-Type: text/html; charset="utf-8"',
        'Content-Transfer-Encoding: base64',
        '',
        wrap(b64utf8(html)),
    ];
    for (const c of emailDraft.charts) {
        parts.push(
            `--${boundary}`,
            `Content-Type: image/png; name="${c.cid}.png"`,
            'Content-Transfer-Encoding: base64',
            `Content-ID: <${c.cid}>`,
            `Content-Disposition: inline; filename="${c.cid}.png"`,
            '',
            wrap(c.dataUrl.split(',')[1]),
        );
    }
    parts.push(`--${boundary}--`, '');

    const blob = new Blob([parts.join('\r\n')], { type: 'message/rfc822' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `BDC Update - ${EMAIL_FOCUS} - ${emailDraft.sections[0].periodLabel.replace("'", '')}.eml`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
    setStatus('Downloaded — open the file to get an Outlook draft.');
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
}
