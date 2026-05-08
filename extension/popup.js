const API_BASE = 'https://app.superlinkedin.org';
let cachedAnalytics = null;

document.addEventListener('DOMContentLoaded', () => {
    const loginView = document.getElementById('loginView');
    const statusView = document.getElementById('statusView');
    const loginBtn = document.getElementById('loginBtn');
    const loginError = document.getElementById('loginError');
    const logoutBtn = document.getElementById('logoutBtn');

    chrome.runtime.sendMessage({ type: 'GET_STATUS' }, (response) => {
        if (response && response.authenticated) {
            showStatus(response);
        } else {
            // Silent auto-login: if the user is already signed into the
            // SuperLinkedIn dashboard the cookie endpoint will hand us a
            // token without any input.
            chrome.runtime.sendMessage({ type: 'AUTO_LOGIN' }, (autoResp) => {
                if (autoResp && autoResp.ok) {
                    chrome.runtime.sendMessage({ type: 'GET_STATUS' }, showStatus);
                } else {
                    showLogin();
                    if (autoResp && autoResp.status === 401) {
                        loginError.innerHTML = 'Not signed in. <a href="#" id="openDashLink" style="color:#0a66c2;text-decoration:underline;">Open SuperLinkedIn</a> to connect automatically.';
                        const link = document.getElementById('openDashLink');
                        if (link) link.addEventListener('click', (e) => { e.preventDefault(); openDashboard(); });
                    }
                }
            });
        }
    });

    loginBtn.addEventListener('click', () => {
        const email = document.getElementById('emailInput').value.trim();
        const linkedinId = document.getElementById('linkedinIdInput').value.trim();
        loginError.textContent = '';
        if (!email) { loginError.textContent = 'Please enter your email.'; return; }

        loginBtn.disabled = true;
        loginBtn.textContent = 'Connecting...';

        chrome.runtime.sendMessage({ type: 'LOGIN', email, linkedinId }, (response) => {
            loginBtn.disabled = false;
            loginBtn.textContent = 'Connect Account';
            if (response && response.ok) {
                chrome.runtime.sendMessage({ type: 'GET_STATUS' }, showStatus);
            } else {
                loginError.textContent = (response && response.error) || 'Login failed.';
            }
        });
    });

    logoutBtn.addEventListener('click', () => {
        chrome.runtime.sendMessage({ type: 'LOGOUT' }, () => showLogin());
    });

    // Tab switching
    document.querySelectorAll('.tab[data-tab]').forEach(tab => {
        tab.addEventListener('click', () => switchTab(tab.dataset.tab, tab));
    });

    // Quick Actions
    document.getElementById('btnOpenDashboard').addEventListener('click', openDashboard);
    document.getElementById('btnOpenAiWriter').addEventListener('click', () => {
        switchTab('ai', document.querySelector('[data-tab="ai"]'));
    });
    document.getElementById('btnToggleSidebar').addEventListener('click', toggleLinkedInSidebar);
    document.getElementById('syncBtnOverview').addEventListener('click', doSync);

    // AI buttons
    document.getElementById('btnAiGenerate').addEventListener('click', () => aiAction('generate'));
    document.getElementById('btnAiImprove').addEventListener('click', () => aiAction('improve'));
    document.querySelectorAll('.ai-quick-btn[data-action]').forEach(btn => {
        btn.addEventListener('click', () => aiAction(btn.dataset.action));
    });
    document.getElementById('btnAiCopy').addEventListener('click', copyAiResult);

    // Footer dashboard link
    document.getElementById('dashboardLink').addEventListener('click', (e) => {
        e.preventDefault();
        openDashboard();
    });

    function showLogin() {
        loginView.style.display = 'block';
        statusView.style.display = 'none';
        document.getElementById('headerActions').style.display = 'none';
    }

    function showStatus(data) {
        loginView.style.display = 'none';
        statusView.style.display = 'block';
        document.getElementById('headerActions').style.display = 'flex';
        document.getElementById('userName').textContent = data.userName || '--';

        if (data.lastSync) {
            document.getElementById('lastSyncTime').textContent = new Date(data.lastSync).toLocaleString();
        }

        loadAnalytics();
    }
});

function switchTab(name, el) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    el.classList.add('active');
    const panelId = 'panel' + name.charAt(0).toUpperCase() + name.slice(1);
    const panel = document.getElementById(panelId);
    if (panel) panel.classList.add('active');
}

function openDashboard() {
    chrome.tabs.create({ url: API_BASE + '/app' });
}

function toggleLinkedInSidebar() {
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
        if (tabs[0] && tabs[0].url && tabs[0].url.includes('linkedin.com')) {
            chrome.tabs.sendMessage(tabs[0].id, { type: 'TOGGLE_SIDEBAR' });
        } else {
            chrome.tabs.create({ url: 'https://www.linkedin.com/feed/' });
        }
    });
}

function doSync() {
    const btn = document.getElementById('syncBtnOverview');
    if (btn) { btn.innerHTML = '<span>&#128259;</span> Syncing...'; btn.disabled = true; }

    chrome.runtime.sendMessage({ type: 'FORCE_SYNC' }, (response) => {
        if (btn) { btn.innerHTML = '<span>&#128259;</span> Sync Now'; btn.disabled = false; }
        if (response && response.ok && response.syncedAt) {
            document.getElementById('lastSyncTime').textContent = new Date(response.syncedAt).toLocaleString();
        }
        loadAnalytics();
    });
}

async function loadAnalytics() {
    try {
        const { authToken } = await chrome.storage.local.get('authToken');
        if (!authToken) return;

        const res = await fetch(`${API_BASE}/api/analytics/summary`, {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });

        if (!res.ok) {
            renderDefaultAnalytics();
            return;
        }

        const data = await res.json();
        cachedAnalytics = data;
        renderAnalytics(data);
    } catch {
        renderDefaultAnalytics();
    }
}

function renderDefaultAnalytics() {
    const data = { followers: 0, totalPosts: 0, avgEngagement: 0, totalImpressions: 0,
                   topPosts: [], totalLikes: 0, totalComments: 0, totalReposts: 0,
                   followerHistory: [] };
    renderAnalytics(data);
}

function renderAnalytics(d) {
    document.getElementById('statFollowers').textContent = formatNum(d.followers || 0);
    document.getElementById('statPosts').textContent = formatNum(d.totalPosts || 0);
    document.getElementById('statEngagement').textContent = (d.avgEngagement || 0).toFixed(1) + '%';
    document.getElementById('statImpressions').textContent = formatNum(d.totalImpressions || 0);

    if (d.plan) {
        document.getElementById('planBadge').textContent = d.plan;
    }

    renderTopPosts(d.topPosts || []);
    renderEngagementBars(d);
    renderFollowerChart(d.followerHistory || []);
}

function renderTopPosts(posts) {
    const list = document.getElementById('topPostsList');
    if (!posts.length) {
        list.innerHTML = '<div class="empty-state">Sync your data to see top posts</div>';
        return;
    }
    list.innerHTML = posts.slice(0, 5).map(p => `
        <div class="top-post-item">
            <div class="top-post-text">${escapeHtml(p.text || '')}</div>
            <div class="top-post-metrics">
                <span class="top-post-metric">&#128077; <span>${formatNum(p.likes || 0)}</span></span>
                <span class="top-post-metric">&#128172; <span>${formatNum(p.comments || 0)}</span></span>
                <span class="top-post-metric">&#128257; <span>${formatNum(p.reposts || 0)}</span></span>
                <span class="top-post-metric">&#128065; <span>${formatNum(p.impressions || 0)}</span></span>
            </div>
        </div>
    `).join('');
}

function renderEngagementBars(d) {
    const maxVal = Math.max(d.totalLikes || 0, d.totalComments || 0, d.totalReposts || 0, d.totalImpressions || 0, 1);

    document.getElementById('engLikesBar').style.width = ((d.totalLikes || 0) / maxVal * 100) + '%';
    document.getElementById('engLikesVal').textContent = formatNum(d.totalLikes || 0);

    document.getElementById('engCommentsBar').style.width = ((d.totalComments || 0) / maxVal * 100) + '%';
    document.getElementById('engCommentsVal').textContent = formatNum(d.totalComments || 0);

    document.getElementById('engRepostsBar').style.width = ((d.totalReposts || 0) / maxVal * 100) + '%';
    document.getElementById('engRepostsVal').textContent = formatNum(d.totalReposts || 0);

    document.getElementById('engViewsBar').style.width = ((d.totalImpressions || 0) / maxVal * 100) + '%';
    document.getElementById('engViewsVal').textContent = formatNum(d.totalImpressions || 0);
}

function renderFollowerChart(history) {
    const container = document.getElementById('followerChart');
    if (!history || history.length < 2) {
        container.innerHTML = '<div class="empty-state">Keep syncing to build your growth chart</div>';
        return;
    }

    const maxVal = Math.max(...history.map(h => h.count || 0), 1);
    const recent = history.slice(-14);

    container.innerHTML = `
        <div class="follower-chart-bars">
            ${recent.map(h => {
                const pct = ((h.count || 0) / maxVal * 100);
                return `<div class="fc-bar" style="height:${pct}%"><span class="fc-bar-label">${formatNum(h.count || 0)}</span></div>`;
            }).join('')}
        </div>
        <div class="fc-label-row">
            ${recent.map(h => {
                const lbl = h.date ? new Date(h.date).toLocaleDateString('en', { day: 'numeric' }) : '';
                return `<div class="fc-label-item">${lbl}</div>`;
            }).join('')}
        </div>
    `;
}

async function aiAction(type) {
    const textarea = document.getElementById('aiTextarea');
    const resultDiv = document.getElementById('aiResult');
    const resultText = document.getElementById('aiResultText');
    const loadingDiv = document.getElementById('aiLoading');
    const text = textarea.value.trim();

    if (type !== 'generate' && !text) {
        textarea.style.borderColor = '#ef4444';
        setTimeout(() => textarea.style.borderColor = '', 2000);
        return;
    }

    resultDiv.style.display = 'none';
    loadingDiv.style.display = 'flex';

    try {
        const { authToken } = await chrome.storage.local.get('authToken');
        if (!authToken) {
            loadingDiv.style.display = 'none';
            resultDiv.style.display = 'block';
            resultText.textContent = 'Please connect your account first.';
            return;
        }

        const endpoint = type === 'generate' ? '/api/ai/write' : '/api/ai/improve';
        const body = type === 'generate'
            ? { prompt: text || 'Write me an engaging LinkedIn post about professional growth' }
            : { text, style: type };

        const res = await fetch(`${API_BASE}${endpoint}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${authToken}`,
            },
            body: JSON.stringify(body),
        });

        const data = await res.json();
        loadingDiv.style.display = 'none';
        resultDiv.style.display = 'block';
        resultText.textContent = data.text || data.result || data.error || 'No result';
    } catch (err) {
        loadingDiv.style.display = 'none';
        resultDiv.style.display = 'block';
        resultText.textContent = 'Error: Could not connect to server.';
    }
}

function copyAiResult() {
    const text = document.getElementById('aiResultText').textContent;
    navigator.clipboard.writeText(text).then(() => {
        const btn = document.querySelector('.ai-btn.small');
        btn.textContent = 'Copied!';
        setTimeout(() => btn.innerHTML = '&#128203; Copy', 1500);
    });
}

function formatNum(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
}

function escapeHtml(str) {
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
}
