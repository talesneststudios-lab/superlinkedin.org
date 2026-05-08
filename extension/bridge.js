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
        }
    });
})();
