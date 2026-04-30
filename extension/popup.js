document.addEventListener('DOMContentLoaded', () => {
    const loginView = document.getElementById('loginView');
    const statusView = document.getElementById('statusView');
    const loginBtn = document.getElementById('loginBtn');
    const loginError = document.getElementById('loginError');
    const syncBtn = document.getElementById('syncBtn');
    const syncMessage = document.getElementById('syncMessage');
    const logoutBtn = document.getElementById('logoutBtn');

    function showStatus(data) {
        loginView.style.display = 'none';
        statusView.style.display = 'block';
        document.getElementById('userName').textContent = data.userName || '--';
        if (data.lastSync) {
            const d = new Date(data.lastSync);
            document.getElementById('lastSyncTime').textContent = d.toLocaleString();
        } else {
            document.getElementById('lastSyncTime').textContent = 'Never';
        }
    }

    function showLogin() {
        loginView.style.display = 'block';
        statusView.style.display = 'none';
    }

    chrome.runtime.sendMessage({ type: 'GET_STATUS' }, (response) => {
        if (response && response.authenticated) {
            showStatus(response);
        } else {
            showLogin();
        }
    });

    loginBtn.addEventListener('click', () => {
        const email = document.getElementById('emailInput').value.trim();
        const linkedinId = document.getElementById('linkedinIdInput').value.trim();
        loginError.textContent = '';

        if (!email) {
            loginError.textContent = 'Please enter your email.';
            return;
        }

        loginBtn.disabled = true;
        loginBtn.textContent = 'Connecting...';

        chrome.runtime.sendMessage(
            { type: 'LOGIN', email, linkedinId },
            (response) => {
                loginBtn.disabled = false;
                loginBtn.textContent = 'Connect Account';

                if (response && response.ok) {
                    chrome.runtime.sendMessage({ type: 'GET_STATUS' }, showStatus);
                } else {
                    loginError.textContent = (response && response.error) || 'Login failed.';
                }
            }
        );
    });

    syncBtn.addEventListener('click', () => {
        syncBtn.disabled = true;
        syncBtn.textContent = 'Syncing...';
        syncMessage.textContent = '';

        chrome.runtime.sendMessage({ type: 'FORCE_SYNC' }, (response) => {
            syncBtn.disabled = false;
            syncBtn.textContent = 'Sync Now';

            if (response && response.ok) {
                syncMessage.textContent = 'Synced successfully!';
                if (response.syncedAt) {
                    document.getElementById('lastSyncTime').textContent =
                        new Date(response.syncedAt).toLocaleString();
                }
            } else {
                syncMessage.textContent = (response && response.error) || 'Sync failed.';
                syncMessage.style.color = '#ef4444';
            }

            setTimeout(() => { syncMessage.textContent = ''; syncMessage.style.color = ''; }, 4000);
        });
    });

    logoutBtn.addEventListener('click', () => {
        chrome.runtime.sendMessage({ type: 'LOGOUT' }, () => {
            showLogin();
        });
    });
});
