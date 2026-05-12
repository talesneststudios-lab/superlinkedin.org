(function () {
    try {
        document.documentElement.setAttribute('data-superlinkedin-ext', '1');
        document.documentElement.setAttribute('data-superlinkedin-ext-version', chrome.runtime.getManifest().version || '0');
    } catch (e) {}

    function reply(requestId, response) {
        try {
            window.postMessage({
                source: 'superlinkedin-ext',
                requestId: requestId,
                response: response,
            }, '*');
        } catch (e) {}
    }

    window.addEventListener('message', function (event) {
        if (event.source !== window) return;
        var msg = event.data;
        if (!msg || msg.source !== 'superlinkedin-page') return;
        var requestId = msg.requestId;

        if (msg.type === 'PING') {
            reply(requestId, { success: true, version: chrome.runtime.getManifest().version || '0' });
            return;
        }

        if (msg.type === 'AUTO_LOGIN') {
            try {
                chrome.runtime.sendMessage({ type: 'AUTO_LOGIN' }, function (resp) {
                    if (chrome.runtime.lastError) {
                        reply(requestId, { success: false, error: chrome.runtime.lastError.message || 'Extension error' });
                        return;
                    }
                    reply(requestId, resp || { success: false, error: 'No response from extension' });
                });
            } catch (e) {
                reply(requestId, { success: false, error: 'Extension error: ' + (e && e.message ? e.message : String(e)) });
            }
            return;
        }

        if (msg.type === 'STORE_TOKEN') {
            try {
                chrome.runtime.sendMessage({ type: 'STORE_TOKEN', data: msg.data }, function (resp) {
                    if (chrome.runtime.lastError) {
                        reply(requestId, { success: false, error: chrome.runtime.lastError.message || 'Extension error' });
                        return;
                    }
                    reply(requestId, resp || { success: true });
                });
            } catch (e) {
                reply(requestId, { success: false, error: 'Extension error: ' + (e && e.message ? e.message : String(e)) });
            }
            return;
        }

        if (msg.type === 'DM_SEND_REPLY') {
            try {
                chrome.runtime.sendMessage({
                    type: 'DM_SEND_REPLY',
                    recipientName: msg.recipientName,
                    text: msg.text,
                }, function (resp) {
                    if (chrome.runtime.lastError) {
                        reply(requestId, { success: false, error: chrome.runtime.lastError.message || 'Extension error' });
                        return;
                    }
                    reply(requestId, resp || { success: false, error: 'No response from extension' });
                });
            } catch (e) {
                reply(requestId, { success: false, error: 'Extension error: ' + (e && e.message ? e.message : String(e)) });
            }
            return;
        }

        if (msg.type === 'ENGAGE_POST_COMMENT') {
            try {
                chrome.runtime.sendMessage(
                    {
                        type: 'ENGAGE_POST_COMMENT',
                        text: msg.text,
                        activityId: msg.activityId,
                        postUrl: msg.postUrl,
                    },
                    function (resp) {
                        if (chrome.runtime.lastError) {
                            reply(requestId, { success: false, error: chrome.runtime.lastError.message || 'Extension error' });
                            return;
                        }
                        reply(requestId, resp || { success: false, error: 'No response from extension' });
                    }
                );
            } catch (e) {
                reply(requestId, { success: false, error: 'Extension error: ' + (e && e.message ? e.message : String(e)) });
            }
            return;
        }

        if (msg.type === 'ENGAGE_REGISTER_URL') {
            try {
                chrome.runtime.sendMessage({ type: 'ENGAGE_REGISTER_URL', url: msg.url }, function (resp) {
                    if (chrome.runtime.lastError) {
                        reply(requestId, { success: false, error: chrome.runtime.lastError.message || 'Extension error' });
                        return;
                    }
                    reply(requestId, resp || { success: false, error: 'No response' });
                });
            } catch (e) {
                reply(requestId, { success: false, error: 'Extension error: ' + (e && e.message ? e.message : String(e)) });
            }
            return;
        }
    });

    chrome.runtime.onMessage.addListener(function (msg) {
        if (!msg || msg.type !== 'ENGAGE_POST_TO_PAGE') return;
        try {
            window.postMessage({
                source: 'superlinkedin-ext',
                type: 'ENGAGE_POST_READY',
                payload: msg.payload || {},
            }, '*');
        } catch (e) {}
    });
})();
