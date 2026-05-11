const API_BASE = 'https://app.superlinkedin.org';
let cachedAnalytics = null;

/** Same keys as sidebar upgrade panel (`content.js`): drives `chrome.storage.local.slUpTheme` */
const POPUP_THEME_SET = new Set(['light', 'dark', 'dim', 'system']);

function normalizePopupTheme(theme) {
    return POPUP_THEME_SET.has(theme) ? theme : 'light';
}

function applyStoredPopupTheme(theme) {
    document.body.dataset.popupTheme = normalizePopupTheme(theme || 'light');
}

function refreshPopupThemeButtonSelection(selected) {
    const s = normalizePopupTheme(selected);
    document.querySelectorAll('.popup-theme-btn[data-theme]').forEach((b) => {
        b.classList.toggle('selected', b.dataset.theme === s);
    });
}

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

    chrome.storage.local.get(['slUpTheme'], (res) => {
        applyStoredPopupTheme(res && res.slUpTheme);
    });

    try {
        chrome.storage.onChanged.addListener((changes, area) => {
            if (area !== 'local' || !changes.slUpTheme) return;
            const nv = changes.slUpTheme.newValue;
            applyStoredPopupTheme(nv);
            refreshPopupThemeButtonSelection(nv);
        });
    } catch { /* extension context */ }

    const settingsBtn = document.getElementById('settingsBtn');
    const settingsBackBtn = document.getElementById('settingsBackBtn');
    const toggleSidebarHdr = document.getElementById('toggleSidebarBtn');
    if (settingsBtn) settingsBtn.addEventListener('click', openSettingsPanel);
    if (settingsBackBtn) settingsBackBtn.addEventListener('click', closeSettingsPanel);
    if (toggleSidebarHdr) toggleSidebarHdr.addEventListener('click', toggleLinkedInSidebar);

    document.querySelectorAll('.popup-theme-btn[data-theme]').forEach((btn) => {
        btn.addEventListener('click', () => {
            const theme = normalizePopupTheme(btn.dataset.theme || 'light');
            chrome.storage.local.set({ slUpTheme: theme }, () => {
                applyStoredPopupTheme(theme);
                refreshPopupThemeButtonSelection(theme);
            });
        });
    });

    const adv = document.getElementById('settingsAdvLink');
    if (adv) {
        adv.addEventListener('click', (e) => {
            e.preventDefault();
            chrome.tabs.create({ url: `${API_BASE}/app#settings` });
        });
    }

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
        statusView.classList.remove('in-settings');
    }

    function showStatus(data) {
        loginView.style.display = 'none';
        statusView.style.display = 'block';
        document.getElementById('headerActions').style.display = 'flex';
        chrome.storage.local.get(['slUpTheme'], (res) => {
            const theme = normalizePopupTheme(res && res.slUpTheme);
            applyStoredPopupTheme(theme);
        });
        document.getElementById('userName').textContent = data.userName || '--';

        if (data.lastSync) {
            document.getElementById('lastSyncTime').textContent = new Date(data.lastSync).toLocaleString();
        }

        loadAnalytics();
    }

    function openSettingsPanel() {
        if (!statusView || statusView.style.display === 'none') return;
        statusView.classList.add('in-settings');
        chrome.storage.local.get(['slUpTheme'], (res) => {
            refreshPopupThemeButtonSelection(res && res.slUpTheme);
        });
    }

    function closeSettingsPanel() {
        statusView.classList.remove('in-settings');
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
                   topPosts: [], socialEngagementsDashboard: null,
                   likesFromPosts: 0, totalComments: 0, totalReposts: 0,
                   followerHistory: [], totalLikes: 0 };
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
    const dash = d.socialEngagementsDashboard;
    const hasDash = dash != null && dash !== '' && !Number.isNaN(Number(dash));
    const dashNum = hasDash ? Number(dash) : 0;
    const likesP = d.likesFromPosts != null ? d.likesFromPosts : (d.totalLikes || 0);
    const cmt = d.totalComments || 0;
    const rps = d.totalReposts || 0;
    const imp = d.totalImpressions || 0;

    const maxVal = Math.max(dashNum, likesP, cmt, rps, imp, 1);

    const dashRow = document.getElementById('engDashSocialRow');
    const dashBar = document.getElementById('engDashSocialBar');
    const dashVal = document.getElementById('engDashSocialVal');
    if (dashRow && dashBar && dashVal) {
        if (hasDash) {
            dashRow.style.display = '';
            dashBar.style.width = ((dashNum / maxVal) * 100) + '%';
            dashVal.textContent = formatNum(dashNum);
        } else {
            dashRow.style.display = 'none';
        }
    }

    document.getElementById('engLikesBar').style.width = ((likesP / maxVal) * 100) + '%';
    document.getElementById('engLikesVal').textContent = formatNum(likesP);

    document.getElementById('engCommentsBar').style.width = ((cmt / maxVal) * 100) + '%';
    document.getElementById('engCommentsVal').textContent = formatNum(cmt);

    document.getElementById('engRepostsBar').style.width = ((rps / maxVal) * 100) + '%';
    document.getElementById('engRepostsVal').textContent = formatNum(rps);

    document.getElementById('engViewsBar').style.width = ((imp / maxVal) * 100) + '%';
    document.getElementById('engViewsVal').textContent = formatNum(imp);
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
    const loadingDiv = document.getElementById('aiLoading');
    const loadingLabel = document.getElementById('aiLoadingLabel');
    const statusEl = document.getElementById('aiAssistStatus');
    const text = textarea.value.trim();

    if (statusEl) statusEl.textContent = '';

    if (type !== 'generate' && !text) {
        textarea.style.borderColor = '#ef4444';
        setTimeout(() => { textarea.style.borderColor = ''; }, 2000);
        return;
    }

    loadingDiv.style.display = 'flex';
    if (loadingLabel) loadingLabel.textContent = type === 'generate' ? 'Generating...' : 'Improving...';
    textarea.setAttribute('aria-busy', 'true');

    try {
        const { authToken } = await chrome.storage.local.get('authToken');
        if (!authToken) {
            loadingDiv.style.display = 'none';
            textarea.removeAttribute('aria-busy');
            if (statusEl) statusEl.textContent = 'Please connect your account first.';
            return;
        }

        const endpoint = type === 'generate' ? '/api/ai/write' : '/api/ai/improve';
        const improveAction = type === 'improve' ? 'engaging' : type;
        const body = type === 'generate'
            ? { tone: 'auto', prompt: text || 'Write me an engaging LinkedIn post about professional growth' }
            : { text, action: improveAction };

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
        textarea.removeAttribute('aria-busy');

        // /api/ai/write returns { post }; /api/ai/improve returns { text }
        const generated = (data.post || data.text || data.result || '').trim();
        const errMsg = (data.error || '').trim();

        if (generated) {
            textarea.value = generated;
            if (statusEl) statusEl.textContent = errMsg || '';
            textarea.focus();
        } else if (errMsg) {
            if (statusEl) statusEl.textContent = errMsg;
        } else {
            if (statusEl) statusEl.textContent = 'No result returned. Try again.';
        }
    } catch (err) {
        loadingDiv.style.display = 'none';
        textarea.removeAttribute('aria-busy');
        if (statusEl) statusEl.textContent = 'Could not reach the server. Check your connection and try again.';
    }
}

function copyAiResult() {
    const text = document.getElementById('aiTextarea').value;
    navigator.clipboard.writeText(text).then(() => {
        const btn = document.getElementById('btnAiCopy');
        const prev = btn.innerHTML;
        btn.innerHTML = '&#10003;';
        btn.title = 'Copied';
        setTimeout(() => { btn.innerHTML = prev; btn.title = 'Copy post'; }, 1500);
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
