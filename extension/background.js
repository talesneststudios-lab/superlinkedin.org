const API_BASE = 'https://rwz9r5zqtw.us-east-1.awsapprunner.com';
const SYNC_ALARM = 'superlinkedin-sync';

let pendingData = { followers: null, posts: [] };

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    if (msg.type === 'ANALYTICS_DATA') {
        mergeData(msg.payload);
        syncToServer();
    }

    if (msg.type === 'GET_STATUS') {
        chrome.storage.local.get(['authToken', 'lastSync', 'userName'], (result) => {
            sendResponse({
                authenticated: !!result.authToken,
                lastSync: result.lastSync || null,
                userName: result.userName || null,
            });
        });
        return true;
    }

    if (msg.type === 'LOGIN') {
        login(msg.email, msg.linkedinId).then(sendResponse);
        return true;
    }

    if (msg.type === 'LOGOUT') {
        chrome.storage.local.remove(['authToken', 'lastSync', 'userName']);
        sendResponse({ ok: true });
    }

    if (msg.type === 'FORCE_SYNC') {
        syncToServer().then(sendResponse);
        return true;
    }
});

function mergeData(payload) {
    if (payload.followers !== undefined && payload.followers !== null) {
        pendingData.followers = payload.followers;
    }
    if (payload.posts && payload.posts.length > 0) {
        const existingTexts = new Set(pendingData.posts.map(p => p.text));
        payload.posts.forEach(p => {
            if (!existingTexts.has(p.text)) {
                pendingData.posts.push(p);
                existingTexts.add(p.text);
            }
        });
        if (pendingData.posts.length > 50) {
            pendingData.posts = pendingData.posts.slice(-50);
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
            });
            return { ok: true, name: data.name };
        }
        return { ok: false, error: data.error || 'Authentication failed' };
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
    };

    if (dataToSend.followers === null && dataToSend.posts.length === 0) {
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

        if (res.ok) {
            pendingData = { followers: null, posts: [] };
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

chrome.alarms.create(SYNC_ALARM, { periodInMinutes: 15 });

chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name === SYNC_ALARM) {
        syncToServer();
    }
});
