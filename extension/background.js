const API_BASE = 'https://app.superlinkedin.org';
const SYNC_ALARM = 'superlinkedin-sync';

function extractLinkedinActivityId(url) {
    const s = String(url || '');
    let m = s.match(/activity[:\-](\d{10,})/i);
    if (m) return m[1];
    m = s.match(/ugcPost[:\-](\d{10,})/i);
    if (m) return m[1];
    if (/\/feed\/update\//i.test(s) || /\/posts\//i.test(s) || /\/embed\//i.test(s)) {
        m = s.match(/(\d{15,20})/);
        if (m) return m[1];
    }
    return '';
}

/**
 * Serialized into LinkedIn tabs via chrome.scripting — must NOT close over
 * variables from this service worker file (Chrome copies the function only).
 */
function engageInjectCapture(activityId) {
    const id = String(activityId || '');
    if (!id) return;

    function decodeHref() {
        try {
            return decodeURIComponent(String(window.location.href || ''));
        } catch (_e) {
            return String(window.location.href || '');
        }
    }

    let href = String(window.location.href || '');
    let dec = decodeHref();
    let urlOk =
        href.indexOf(id) !== -1 ||
        dec.indexOf(id) !== -1 ||
        href.indexOf('activity%3A' + id) !== -1 ||
        href.indexOf('urn%3Ali%3Aactivity%3A' + id) !== -1;
    try {
        if (!urlOk && /\/feed\/update\//i.test(href)) {
            let m =
                dec.match(/activity(?:%3A|:)(\d{10,})/i) ||
                dec.match(/urn%3Ali%3Aactivity%3A(\d{10,})/i);
            if (m && String(m[1]) === id) urlOk = true;
        }
    } catch (_e) {}
    if (!urlOk) return;

    function scrapeAuthor(post) {
        if (!post) return '';
        const authorEl = post.querySelector(
            '.update-components-actor__name .visually-hidden, ' +
                '.update-components-actor__meta-link .visually-hidden, ' +
                '.update-components-actor__meta a span[aria-hidden="true"], ' +
                '.feed-shared-actor__name span[aria-hidden="true"], ' +
                '.feed-shared-actor__name'
        );
        return authorEl ? authorEl.textContent.replace(/\s+/g, ' ').trim() : '';
    }

    function longestFromRoot(root) {
        if (!root) return '';
        const sels =
            '.update-components-update-v2__commentary, ' +
            '.feed-shared-update-v2__description, ' +
            '.feed-shared-inline-show-more-text, ' +
            '.update-components-text, ' +
            '.feed-shared-text, ' +
            '.attributed-text-segment-list__content, ' +
            'span.break-words[dir="ltr"]';
        let best = '';
        try {
            root.querySelectorAll(sels).forEach(function (el) {
                const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
                if (/commented\s+on\s+this|^see more$/i.test(t)) return;
                if (t.length < 16) return;
                if (el.closest('.comments-comments-list')) return;
                if (el.closest('[class*="social-details-social-counts"]')) return;
                if (t.length > best.length) best = t;
            });
        } catch (_e) {}
        return best;
    }

    const rootsOrdered = [];
    try {
        document.querySelectorAll('[data-urn]').forEach(function (el) {
            const urn = String(el.getAttribute('data-urn') || '');
            if (urn.indexOf(id) === -1) return;
            const sel =
                '.feed-shared-update-v2, [class*="feed-shared-update"], article, div[data-view-name*="feed-detail"]';
            const card =
                el.closest(sel) || el.closest('main article') || (el.parentElement && el.parentElement.closest('main'));
            if (card && rootsOrdered.indexOf(card) === -1) rootsOrdered.push(card);
        });
        [
            document.querySelector('main .feed-shared-update-v2'),
            document.querySelector('[data-view-name="feed-detail-update"] article'),
            document.querySelector('main article'),
            document.querySelector('.scaffold-layout__detail main'),
        ].forEach(function (n) {
            if (n && rootsOrdered.indexOf(n) === -1) rootsOrdered.push(n);
        });
        document
            .querySelectorAll(
                '.feed-shared-update-v2, [data-urn*="activity"], article[data-id], div[data-view-name*="feed-detail"] article'
            )
            .forEach(function (n) {
                if (rootsOrdered.indexOf(n) === -1) rootsOrdered.push(n);
            });
    } catch (_e) {}

    let bestText = '';
    let bestAuthor = '';
    rootsOrdered.forEach(function (post) {
        const txt = longestFromRoot(post);
        if (txt.length > bestText.length) {
            bestText = txt;
            bestAuthor = scrapeAuthor(post);
        }
    });

    if (bestText.length < 35) {
        try {
            const main = document.querySelector('main.scaffold-layout__main, main[role="main"], main');
            if (main) {
                main.querySelectorAll('span[dir="ltr"], p').forEach(function (el) {
                    const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
                    if (t.length < 55) return;
                    if (/sign in|cookie|try again|© 20/i.test(t)) return;
                    if (el.closest('nav, footer, form[action*="search"], .global-nav')) return;
                    if (t.length > bestText.length) bestText = t;
                });
            }
        } catch (_e) {}
    }

    if (!bestText || bestText.length < 25) return;

    try {
        chrome.runtime.sendMessage({
            type: 'ENGAGE_POST_CAPTURED',
            payload: {
                author: bestAuthor || '',
                text: bestText.substring(0, 12000),
                url: href,
            },
        });
    } catch (_e) {}
}

function tabUrlLikelyShowsActivity(tabUrl, activityId) {
    const u = String(tabUrl || '');
    const id = String(activityId || '');
    if (!u || !id) return false;
    if (u.includes(id)) return true;
    try {
        const dec = decodeURIComponent(u);
        if (dec.includes(id)) return true;
    } catch (_e) {}
    if (u.includes('activity%3A' + id) || u.includes('urn%3Ali%3Aactivity%3A' + id)) return true;
    if (/\/feed\/update\//i.test(u) || /\/posts\//i.test(u)) {
        try {
            const dec = decodeURIComponent(u);
            const m = dec.match(/activity(?:%3A|:)(\d{10,})/i) || dec.match(/urn:?li:?activity:?(\d{10,})/i);
            if (m && String(m[1]) === id) return true;
        } catch (_e) {}
    }
    return false;
}

let pendingData = { followers: null, posts: [], profile: null, dashboardStats: null, dms: null, feedPosts: [] };

/** Inject scraper + nudge content script while Engage fetch is active (slow SPA / selector drift). */
function armEngageLinkedInTabPoll(activityId) {
    let n = 0;
    const max = 52;
    const isLinkedInTab = (u) => /^https:\/\/([^/]*\.)?linkedin\.com\//i.test(String(u || ''));
    const tick = () => {
        chrome.tabs.query({}, (allTabs) => {
            (allTabs || []).forEach((tab) => {
                const u = tab.url || '';
                if (!tab.id || !isLinkedInTab(u) || !tabUrlLikelyShowsActivity(u, activityId)) return;
                chrome.scripting
                    .executeScript({
                        target: { tabId: tab.id },
                        func: engageInjectCapture,
                        args: [activityId],
                    })
                    .catch(() => {});
                chrome.tabs.sendMessage(tab.id, { type: 'ENGAGE_FORCE_SCRAPE' }).catch(() => {});
            });
        });
        if (++n < max) setTimeout(tick, 2000);
    };
    setTimeout(tick, 400);
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    if (msg.type === 'ANALYTICS_DATA') {
        mergeData(msg.payload);
        syncToServer();
    }

    if (msg.type === 'GET_STATUS') {
        chrome.storage.local.get(['authToken', 'lastSync', 'userName', 'plan'], (result) => {
            sendResponse({
                authenticated: !!result.authToken,
                lastSync: result.lastSync || null,
                userName: result.userName || null,
                plan: result.plan || 'Pro',
            });
        });
        return true;
    }

    if (msg.type === 'LOGIN') {
        login(msg.email, msg.linkedinId).then(sendResponse);
        return true;
    }

    if (msg.type === 'AUTO_LOGIN') {
        autoLoginViaCookie().then(sendResponse);
        return true;
    }

    if (msg.type === 'STORE_TOKEN') {
        storeTokenFromPage(msg.data).then(sendResponse);
        return true;
    }

    if (msg.type === 'LOGOUT') {
        chrome.storage.local.remove(['authToken', 'lastSync', 'userName', 'plan', 'ownerLinkedinId', 'ownerName', 'slConnectionsWatermark']);
        sendResponse({ ok: true });
    }

    if (msg.type === 'FORCE_SYNC') {
        syncToServer().then(sendResponse);
        return true;
    }

    if (msg.type === 'GET_ANALYTICS') {
        getAnalyticsSummary().then(sendResponse);
        return true;
    }

    if (msg.type === 'ENGAGE_REGISTER_URL') {
        const url = String(msg.url || '');
        const activityId = extractLinkedinActivityId(url);
        chrome.storage.session.set(
            { engageCapture: { url, activityId, ts: Date.now() } },
            () => {
                sendResponse({ success: !!activityId, activityId: activityId || null, error: activityId ? null : 'Could not read post id from URL' });
                if (activityId) armEngageLinkedInTabPoll(activityId);
            }
        );
        return true;
    }

    if (msg.type === 'ENGAGE_POST_CAPTURED') {
        const payload = msg.payload || {};
        chrome.storage.session.remove('engageCapture', () => {});
        chrome.tabs.query({}, (tabs) => {
            tabs.forEach((tab) => {
                const u = tab.url || '';
                if (u.indexOf('superlinkedin.org') === -1 && u.indexOf('awsapprunner.com') === -1) return;
                chrome.tabs.sendMessage(tab.id, { type: 'ENGAGE_POST_TO_PAGE', payload }).catch(() => {});
            });
        });
        sendResponse({ ok: true });
        return true;
    }

    if (msg.type === 'ENGAGE_POST_COMMENT') {
        const text = String(msg.text || '').trim();
        const activityId = String(msg.activityId || '').trim();
        const postUrl = String(msg.postUrl || '').trim();
        if (!text || !activityId) {
            sendResponse({ success: false, error: 'Missing comment text or post id.' });
            return false;
        }
        const urlMatchesActivity = (u) => {
            if (!u) return false;
            if (u.indexOf(activityId) !== -1) return true;
            try {
                return decodeURIComponent(u).indexOf(activityId) !== -1;
            } catch (_e) {
                return false;
            }
        };
        const normPath = (u) => {
            try {
                return new URL(String(u).split(/[?#]/)[0]).pathname;
            } catch (_e) {
                return '';
            }
        };
        const postPath = postUrl ? normPath(postUrl) : '';

        chrome.tabs.query({}, (tabs) => {
            const liTabs = (tabs || []).filter((t) => /^https:\/\/([^/]+\.)?linkedin\.com\//i.test(t.url || ''));
            const target =
                liTabs.find((t) => urlMatchesActivity(t.url || '')) ||
                (postPath ? liTabs.find((t) => normPath(t.url || '') === postPath) : null);

            if (!target || !target.id) {
                sendResponse({
                    success: false,
                    error:
                        'No LinkedIn tab found for this post. Open it in Chrome (same profile as the extension), wait until the post loads fully, then try again.',
                });
                return;
            }
            chrome.tabs.update(target.id, { active: true }, () => {});
            chrome.tabs.sendMessage(target.id, { type: 'ENGAGE_POST_COMMENT', text, activityId, postUrl }, (response) => {
                if (chrome.runtime.lastError) {
                    sendResponse({ success: false, error: chrome.runtime.lastError.message });
                } else {
                    sendResponse(response || { success: false, error: 'No response from LinkedIn tab.' });
                }
            });
        });
        return true;
    }

    if (msg.type === 'DM_SEND_REPLY') {
        // Prefer a tab already on /messaging/ since the content script can
        // skip navigation. Fall back to any linkedin.com tab — the content
        // script will navigate to messaging itself. If neither exists, open
        // messaging in a background tab so future bulk sends just work.
        chrome.tabs.query({ url: '*://*.linkedin.com/*' }, (tabs) => {
            const messagingTab = tabs.find(t => /\/messaging(\/|$)/.test(t.url || ''));
            const liTab = messagingTab || tabs[0];
            const dispatch = (tabId) => {
                chrome.tabs.sendMessage(tabId, {
                    type: 'DM_SEND_REPLY',
                    recipientName: msg.recipientName,
                    text: msg.text,
                }, (response) => {
                    if (chrome.runtime.lastError) {
                        sendResponse({ success: false, error: 'LinkedIn page not ready. Reload linkedin.com and try again.' });
                    } else {
                        sendResponse(response || { success: false, error: 'No response from LinkedIn page' });
                    }
                });
            };
            if (liTab) {
                dispatch(liTab.id);
            } else {
                chrome.tabs.create({ url: 'https://www.linkedin.com/messaging/', active: false }, (tab) => {
                    // Give the new tab a moment to boot the content script.
                    setTimeout(() => dispatch(tab.id), 6000);
                });
            }
        });
        return true;
    }
});

function mergeData(payload) {
    if (payload.followers !== undefined && payload.followers !== null) {
        const nf = Number(payload.followers);
        const pf = pendingData.followers;
        let reject = false;
        if (!Number.isFinite(nf) || nf < 5) reject = true;
        else if (pf !== null && Number.isFinite(pf) && pf >= 35 && nf <= pf &&
            nf < pf * 0.72 && pf - nf > 55) {
            reject = true;
            console.log('[SuperLinkedIn] Pending queue: rejecting cliff follower scrape', nf, 'vs queued', pf);
        }
        if (!reject) pendingData.followers = nf;
    }
    if (payload.profile) {
        pendingData.profile = payload.profile;
    }
    if (payload.posts && payload.posts.length > 0) {
        const existingTexts = new Set(pendingData.posts.map(p => p.text));
        payload.posts.forEach(p => {
            if (!existingTexts.has(p.text)) {
                pendingData.posts.push(p);
                existingTexts.add(p.text);
            }
        });
        if (pendingData.posts.length > 100) {
            pendingData.posts = pendingData.posts.slice(-100);
        }
    }
    if (payload.dashboardStats) {
        const prev = pendingData.dashboardStats || {};
        const patch = payload.dashboardStats;
        const next = { ...prev, ...patch };
        if (patch && Object.prototype.hasOwnProperty.call(patch, 'postImpressions')) {
            const a = Number(prev.postImpressions);
            const b = Number(patch.postImpressions);
            if (Number.isFinite(a) && a >= 0 && Number.isFinite(b) && b >= 0) {
                next.postImpressions = Math.max(a, b);
            }
        }
        pendingData.dashboardStats = next;
    }
    if (payload.dms) {
        pendingData.dms = payload.dms;
    }
    if (payload.feedPosts && payload.feedPosts.length > 0) {
        const existingTexts = new Set(pendingData.feedPosts.map(p => (p.text || '').substring(0, 80).toLowerCase()));
        payload.feedPosts.forEach(p => {
            const key = (p.text || '').substring(0, 80).toLowerCase();
            if (key && !existingTexts.has(key)) {
                pendingData.feedPosts.push(p);
                existingTexts.add(key);
            }
        });
        if (pendingData.feedPosts.length > 50) {
            pendingData.feedPosts = pendingData.feedPosts.slice(-50);
        }
    }
}

async function login(email, linkedinId) {
    try {
        const res = await fetch(`${API_BASE}/api/extension/auth`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email, linkedinId }),
        });
        const data = await res.json();
        if (data.token) {
            await chrome.storage.local.set({
                authToken: data.token,
                userName: data.name || email,
                ownerLinkedinId: data.linkedinId || '',
                ownerName: data.name || '',
                plan: data.plan || 'Pro',
            });
            return { ok: true, name: data.name };
        }
        return { ok: false, error: data.error || 'Authentication failed' };
    } catch (err) {
        return { ok: false, error: 'Could not connect to server' };
    }
}

// Persist a token handed to us directly from a logged-in dashboard tab.
// This is the primary auto-connect path: the page does a same-origin fetch
// (cookie travels reliably) and forwards the resulting token to us.
async function storeTokenFromPage(data) {
    if (!data || !data.token) return { ok: false, error: 'No token' };
    await chrome.storage.local.set({
        authToken: data.token,
        userName: data.name || data.email || '',
        ownerLinkedinId: data.linkedinId || '',
        ownerName: data.name || '',
        plan: data.plan || 'Pro',
    });
    console.log('[SuperLinkedIn] Token stored from dashboard auto-login');
    return { ok: true, name: data.name };
}

// Cookie-based silent login (fallback). Works when the user is already
// signed into the SuperLinkedIn dashboard in the same browser profile.
// SameSite=Lax may prevent the session cookie from being sent on this
// cross-site fetch, in which case we fall through to the manual login flow
// or rely on the dashboard tab pushing us a token via STORE_TOKEN.
async function autoLoginViaCookie() {
    try {
        const res = await fetch(`${API_BASE}/api/extension/issue-token`, {
            method: 'GET',
            credentials: 'include',
            headers: { 'Accept': 'application/json' },
        });
        if (!res.ok) {
            return { ok: false, status: res.status, error: res.status === 401 ? 'Not signed in to SuperLinkedIn' : 'Auto-login failed' };
        }
        const data = await res.json();
        if (!data.token) return { ok: false, error: 'No token returned' };
        await chrome.storage.local.set({
            authToken: data.token,
            userName: data.name || data.email || '',
            ownerLinkedinId: data.linkedinId || '',
            ownerName: data.name || '',
            plan: data.plan || 'Pro',
        });
        return { ok: true, name: data.name, autoLogin: true };
    } catch (err) {
        return { ok: false, error: 'Could not connect to server' };
    }
}

async function syncToServer() {
    const { authToken } = await chrome.storage.local.get('authToken');
    if (!authToken) return { ok: false, error: 'Not authenticated' };

    const dataToSend = {
        followers: pendingData.followers,
        posts: pendingData.posts.slice(),
        profile: pendingData.profile,
        dashboardStats: pendingData.dashboardStats,
        dms: pendingData.dms,
        feedPosts: pendingData.feedPosts.slice(),
    };

    if (dataToSend.followers === null && dataToSend.posts.length === 0 && !dataToSend.dashboardStats && !dataToSend.dms && dataToSend.feedPosts.length === 0) {
        return { ok: true, message: 'Nothing to sync' };
    }

    try {
        const res = await fetch(`${API_BASE}/api/analytics/sync`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${authToken}`,
            },
            body: JSON.stringify(dataToSend),
        });

        const result = await res.json();
        console.log('[SuperLinkedIn] Sync response:', res.status, JSON.stringify(result));

        if (res.ok) {
            pendingData = { followers: null, posts: [], profile: null, dashboardStats: null, dms: null, feedPosts: [] };
            const now = new Date().toISOString();
            await chrome.storage.local.set({ lastSync: now });
            return { ok: true, syncedAt: now };
        }

        if (res.status === 401) {
            await chrome.storage.local.remove(['authToken']);
            return { ok: false, error: 'Session expired. Please log in again.' };
        }

        return { ok: false, error: result.error || 'Sync failed' };
    } catch (err) {
        return { ok: false, error: 'Could not connect to server' };
    }
}

async function getAnalyticsSummary() {
    const { authToken } = await chrome.storage.local.get('authToken');
    if (!authToken) return { ok: false, error: 'Not authenticated' };

    try {
        const res = await fetch(`${API_BASE}/api/analytics/summary`, {
            headers: { 'Authorization': `Bearer ${authToken}` },
        });
        if (!res.ok) return { ok: false, error: 'Failed to fetch' };
        const data = await res.json();
        return { ok: true, data };
    } catch {
        return { ok: false, error: 'Could not connect to server' };
    }
}

chrome.alarms.create(SYNC_ALARM, { periodInMinutes: 15 });

chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name === SYNC_ALARM) {
        syncToServer();
    }
});
