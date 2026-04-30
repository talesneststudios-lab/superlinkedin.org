(function () {
    'use strict';

    const SCRAPE_DELAY = 3000;
    let lastUrl = '';

    function parseNumber(text) {
        if (!text) return 0;
        text = text.trim().replace(/,/g, '');
        const m = text.match(/([\d.]+)\s*(K|M|B)?/i);
        if (!m) return 0;
        let n = parseFloat(m[1]);
        if (m[2]) {
            const mult = { K: 1e3, M: 1e6, B: 1e9 };
            n *= mult[m[2].toUpperCase()] || 1;
        }
        return Math.round(n);
    }

    function scrapeFollowers() {
        const selectors = [
            '.pv-top-card--list-bullet .t-bold',
            '.pvs-header__subtitle',
            '[data-test-id="follower-count"]',
        ];

        for (const sel of selectors) {
            const els = document.querySelectorAll(sel);
            for (const el of els) {
                const text = el.textContent || '';
                if (/follower/i.test(text)) {
                    return parseNumber(text);
                }
            }
        }

        const allText = document.body.innerText;
        const followerMatch = allText.match(/([\d,.]+[KMB]?)\s+followers/i);
        if (followerMatch) return parseNumber(followerMatch[1]);

        const connectionMatch = allText.match(/([\d,.]+)\s+connections/i);
        if (connectionMatch) return parseNumber(connectionMatch[1]);

        return null;
    }

    function scrapePostMetrics() {
        const posts = [];

        const feedPosts = document.querySelectorAll(
            '.feed-shared-update-v2, [data-urn*="activity"], .occludable-update'
        );

        feedPosts.forEach(post => {
            const authorEl = post.querySelector(
                '.update-components-actor__name .visually-hidden, ' +
                '.update-components-actor__title .visually-hidden, ' +
                '.feed-shared-actor__name'
            );
            const authorName = authorEl ? authorEl.textContent.trim() : '';

            const textEl = post.querySelector(
                '.feed-shared-text, .update-components-text, .break-words'
            );
            const postText = textEl ? textEl.textContent.trim().substring(0, 100) : '';

            const likesEl = post.querySelector(
                '.social-details-social-counts__reactions-count, ' +
                '[data-test-id="social-actions__reaction-count"]'
            );
            const likes = likesEl ? parseNumber(likesEl.textContent) : 0;

            const commentsEl = post.querySelector(
                'button[aria-label*="comment" i], ' +
                '.social-details-social-counts__comments'
            );
            let comments = 0;
            if (commentsEl) {
                const cText = commentsEl.textContent || commentsEl.getAttribute('aria-label') || '';
                comments = parseNumber(cText);
            }

            const repostsEl = post.querySelector(
                'button[aria-label*="repost" i], ' +
                '.social-details-social-counts__item--with-social-proof'
            );
            let reposts = 0;
            if (repostsEl) {
                const rText = repostsEl.textContent || repostsEl.getAttribute('aria-label') || '';
                reposts = parseNumber(rText);
            }

            const impressionsEl = post.querySelector(
                '.analytics-entry-point, ' +
                '[data-test-id="impression-count"], ' +
                '.ca-entry-point'
            );
            let impressions = 0;
            if (impressionsEl) {
                impressions = parseNumber(impressionsEl.textContent);
            }

            if (postText) {
                posts.push({
                    text: postText,
                    likes,
                    comments,
                    reposts,
                    impressions,
                    scrapedAt: new Date().toISOString(),
                });
            }
        });

        return posts;
    }

    function sendToBackground(data) {
        chrome.runtime.sendMessage({
            type: 'ANALYTICS_DATA',
            payload: data,
        });
    }

    function runScrape() {
        const url = window.location.href;
        const data = { url, timestamp: new Date().toISOString() };

        if (url.includes('/in/')) {
            const followers = scrapeFollowers();
            if (followers !== null) {
                data.followers = followers;
            }
        }

        if (url.includes('/feed') || url.includes('/in/') || url.includes('/posts/')) {
            const posts = scrapePostMetrics();
            if (posts.length > 0) {
                data.posts = posts;
            }
        }

        if (data.followers !== undefined || (data.posts && data.posts.length > 0)) {
            sendToBackground(data);
        }
    }

    function init() {
        setTimeout(runScrape, SCRAPE_DELAY);

        const observer = new MutationObserver(() => {
            const currentUrl = window.location.href;
            if (currentUrl !== lastUrl) {
                lastUrl = currentUrl;
                setTimeout(runScrape, SCRAPE_DELAY);
            }
        });

        observer.observe(document.body, { childList: true, subtree: true });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
