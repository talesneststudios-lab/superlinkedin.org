(function () {
    'use strict';

    const API_BASE = 'https://rwz9r5zqtw.us-east-1.awsapprunner.com';
    const SCRAPE_DELAY = 5000;
    let lastUrl = '';
    let sidebarOpen = false;
    let sidebarData = { followers: 0, posts: [], topPosts: [], totalLikes: 0,
        totalComments: 0, totalReposts: 0, totalImpressions: 0 };

    // ── Number parsing ──
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

    // ── Scraping ──
    // Returns the user's CONNECTIONS count (the field is named `followers`
    // for backwards compatibility with the database schema and existing
    // dashboard code, but we now look for connections first since that's
    // what most LinkedIn users actually grow).
    //
    // The function only trusts numbers from pages where the LOGGED-IN user's
    // own profile data is visible — primarily the My Network page or their
    // own /in/<vanity>/ profile. On the feed and other pages we'd otherwise
    // pick up "1 follower" widgets ("Add to your feed → Pages"), which is
    // exactly the bug that was producing the spurious "1 followers" reading.
    function scrapeFollowers() {
        const url = location.href;
        const path = location.pathname;

        const onMyNetwork = /\/(mynetwork|my-network)(\/|$)/i.test(path);
        const onOwnProfile = /\/in\/[^/]+\/?(\?|$)/i.test(path);
        // The settings/feed pages still surface "X connections" in the user
        // card on the left; allow it ONLY when the number looks plausible.
        const onFeedOrSettings = /\/(feed|notifications|jobs|messaging)(\/|$)/i.test(path);

        // 1) My Network page — most reliable: the "Connections" tile shows
        //    the total count directly.
        if (onMyNetwork) {
            const tile = document.querySelector('a[href*="/mynetwork/invite-connect/connections/"], a[href*="/mynetwork/network-manager/"]');
            const tileText = (tile && tile.textContent) || '';
            const tileMatch = tileText.match(/([\d][\d,.]*\s*[KMB]?)/);
            if (tileMatch) {
                const n = parseNumber(tileMatch[1]);
                if (n >= 1) return n;
            }
            // Generic page-level fallback: "Your connections … 730"
            const pageText = (document.querySelector('main')?.innerText || '').substring(0, 4000);
            const pageMatch = pageText.match(/([\d][\d,.]{1,}[KMB]?)\s+connections?\b/i);
            if (pageMatch) {
                const n = parseNumber(pageMatch[1]);
                if (n >= 5) return n;
            }
        }

        // 2) Own profile page — "X connections" inline in the profile header.
        if (onOwnProfile) {
            const header = document.querySelector('.pv-top-card, .scaffold-layout__main, main') || document.body;
            const headerText = (header.innerText || '').substring(0, 6000);
            const cm = headerText.match(/([\d][\d,.]*\s*[KMB]?)\s+connections?\b/i);
            if (cm) {
                const n = parseNumber(cm[1]);
                // 500+ is LinkedIn's display cap, real value lives elsewhere
                if (n >= 5) return n;
            }
            const fm = headerText.match(/([\d][\d,.]*\s*[KMB]?)\s+followers?\b/i);
            if (fm) {
                const n = parseNumber(fm[1]);
                if (n >= 5) return n;
            }
        }

        // 3) Feed / settings / messaging — the left-rail "user card" shows
        //    a profile summary that occasionally includes a count. Only
        //    accept it when it's specifically labelled CONNECTIONS and is
        //    >= 10, otherwise we'll keep grabbing "1 follower" from the
        //    "Add to your feed" Pages widget.
        if (onFeedOrSettings) {
            const sidebars = document.querySelectorAll(
                '.feed-identity-module, .scaffold-layout__sidebar, .global-nav__me, aside'
            );
            for (const side of sidebars) {
                const text = (side.innerText || '').substring(0, 2000);
                const cm = text.match(/([\d][\d,.]*\s*[KMB]?)\s+connections?\b/i);
                if (cm) {
                    const n = parseNumber(cm[1]);
                    if (n >= 10) return n;
                }
            }
        }

        // No trustworthy number on this page — leave the existing value
        // alone rather than overwriting it with garbage.
        return null;
    }

    // ── Feed sidebar stats ──
    // The LinkedIn feed page's left identity card shows two metrics that
    // mirror the analytics dashboard: "Profile viewers" and "Post
    // impressions". Scrape them so the extension popup stays fresh without
    // requiring the user to visit /analytics/.
    function scrapeFeedSidebarStats() {
        const result = {};
        const card = document.querySelector(
            '.feed-identity-module, .feed-identity-module__profile-card, .scaffold-layout__sidebar'
        ) || document.body;
        if (!card) return null;
        const text = (card.innerText || '').replace(/\u00a0/g, ' ').substring(0, 4000);

        // Pattern: "Profile viewers\n18" or "Profile viewers 18"
        const pv = text.match(/Profile\s+viewers?[\s:]*(\d[\d,.]*[KMB]?)/i);
        if (pv) {
            const n = parseNumber(pv[1]);
            if (n > 0) result.profileViews = n;
        }
        // Pattern: "Post impressions\n24" or "Post impressions 24"
        const pi = text.match(/Post\s+impressions?[\s:]*(\d[\d,.]*[KMB]?)/i);
        if (pi) {
            const n = parseNumber(pi[1]);
            if (n > 0) result.postImpressions = n;
        }

        return Object.keys(result).length ? result : null;
    }

    async function scrapePostMetrics() {
        const posts = [];
        const feedPosts = document.querySelectorAll(
            '.feed-shared-update-v2, [data-urn*="activity"], .occludable-update'
        );

        let ownerName = '';
        try {
            const stored = await chrome.storage.local.get('ownerName');
            ownerName = (stored.ownerName || '').toLowerCase().replace(/[^a-z\s]/g, '').trim();
        } catch (e) {}

        const isOwnProfilePage = window.location.href.includes('/in/') || window.location.href.includes('/posts/');

        feedPosts.forEach(post => {
            const authorEl = post.querySelector(
                '.update-components-actor__name .visually-hidden, ' +
                '.update-components-actor__title .visually-hidden, ' +
                '.feed-shared-actor__name'
            );
            const authorName = authorEl ? authorEl.textContent.trim() : '';

            // On the feed, only count the logged-in user's own posts
            if (!isOwnProfilePage && ownerName && authorName) {
                const normalAuthor = authorName.toLowerCase().replace(/[^a-z\s]/g, '').trim();
                if (!normalAuthor.includes(ownerName) && !ownerName.includes(normalAuthor)) {
                    return;
                }
            }

            const textEl = post.querySelector(
                '.feed-shared-text, .update-components-text, .break-words'
            );
            const postText = textEl ? textEl.textContent.trim().substring(0, 200) : '';

            const likesEl = post.querySelector(
                '.social-details-social-counts__reactions-count, ' +
                '[data-test-id="social-actions__reaction-count"]'
            );
            const likes = likesEl ? parseNumber(likesEl.textContent) : 0;

            const commentsEl = post.querySelector(
                'button[aria-label*="comment" i], .social-details-social-counts__comments'
            );
            let comments = 0;
            if (commentsEl) {
                comments = parseNumber(commentsEl.textContent || commentsEl.getAttribute('aria-label') || '');
            }

            const repostsEl = post.querySelector(
                'button[aria-label*="repost" i], .social-details-social-counts__item--with-social-proof'
            );
            let reposts = 0;
            if (repostsEl) {
                reposts = parseNumber(repostsEl.textContent || repostsEl.getAttribute('aria-label') || '');
            }

            const impressionsEl = post.querySelector(
                '.analytics-entry-point, [data-test-id="impression-count"], .ca-entry-point'
            );
            let impressions = 0;
            if (impressionsEl) {
                impressions = parseNumber(impressionsEl.textContent);
            }

            if (postText) {
                posts.push({
                    text: postText,
                    author: authorName,
                    likes, comments, reposts, impressions,
                    engagement: likes + comments + reposts,
                    scrapedAt: new Date().toISOString(),
                });
            }
        });

        return posts;
    }

    async function scrapeFeedPosts() {
        if (!window.location.href.includes('/feed')) return [];
        const posts = [];
        const feedPosts = document.querySelectorAll(
            '.feed-shared-update-v2, [data-urn*="activity"], .occludable-update'
        );

        let ownerName = '';
        try {
            const stored = await chrome.storage.local.get('ownerName');
            ownerName = (stored.ownerName || '').toLowerCase().replace(/[^a-z\s]/g, '').trim();
        } catch (e) {}

        const seen = new Set();
        feedPosts.forEach(post => {
            const authorEl = post.querySelector(
                '.update-components-actor__name .visually-hidden, ' +
                '.update-components-actor__title .visually-hidden, ' +
                '.feed-shared-actor__name'
            );
            const authorName = authorEl ? authorEl.textContent.trim() : '';
            if (!authorName) return;

            const normalAuthor = authorName.toLowerCase().replace(/[^a-z\s]/g, '').trim();
            if (ownerName && (normalAuthor.includes(ownerName) || ownerName.includes(normalAuthor))) return;

            const textEl = post.querySelector(
                '.feed-shared-text, .update-components-text, .break-words'
            );
            const postText = textEl ? textEl.textContent.trim().substring(0, 300) : '';
            if (!postText || postText.length < 10) return;

            const key = postText.substring(0, 80).toLowerCase();
            if (seen.has(key)) return;
            seen.add(key);

            const likesEl = post.querySelector(
                '.social-details-social-counts__reactions-count, [data-test-id="social-actions__reaction-count"]'
            );
            const likes = likesEl ? parseNumber(likesEl.textContent) : 0;

            const commentsEl = post.querySelector(
                'button[aria-label*="comment" i], .social-details-social-counts__comments'
            );
            const comments = commentsEl ? parseNumber(commentsEl.textContent || commentsEl.getAttribute('aria-label') || '') : 0;

            const repostsEl = post.querySelector(
                'button[aria-label*="repost" i], .social-details-social-counts__item--with-social-proof'
            );
            const reposts = repostsEl ? parseNumber(repostsEl.textContent || repostsEl.getAttribute('aria-label') || '') : 0;

            posts.push({
                text: postText,
                author: authorName,
                likes, comments, reposts,
                engagement: likes + comments + reposts,
                scrapedAt: new Date().toISOString(),
            });
        });

        return posts.slice(0, 30);
    }

    function scrapeProfileInfo() {
        const nameEl = document.querySelector('.text-heading-xlarge, .pv-top-card--list li:first-child');
        const headlineEl = document.querySelector('.text-body-medium, .pv-top-card--experience-list-text');
        return {
            name: nameEl ? nameEl.textContent.trim() : '',
            headline: headlineEl ? headlineEl.textContent.trim() : '',
        };
    }

    function isOwnProfileByDOM() {
        const ownIndicators = [
            'button[aria-label*="Edit intro"]',
            'button[aria-label*="Edit profile"]',
            'button[aria-label*="Open to"]',
            '.pv-top-card--edit',
            '#profile-edit-btn',
            'a[href*="/in/me/"]',
            '.profile-edit-btn',
            'button[aria-label*="Add profile section"]',
            '.pv-dashboard-section',
            '.creator-mode-toggle',
        ];
        for (const sel of ownIndicators) {
            if (document.querySelector(sel)) return true;
        }
        const bodyText = document.body.innerText || '';
        if (/\bEdit (public )?profile\b/i.test(bodyText) && /\bOpen to\b/i.test(bodyText)) return true;
        return false;
    }

    async function isOwnProfile() {
        if (isOwnProfileByDOM()) return true;

        try {
            const { ownerName } = await chrome.storage.local.get('ownerName');
            if (ownerName) {
                const pageProfile = scrapeProfileInfo();
                if (pageProfile.name && ownerName) {
                    const pageName = pageProfile.name.toLowerCase().replace(/[^a-z\s]/g, '').trim();
                    const storedName = ownerName.toLowerCase().replace(/[^a-z\s]/g, '').trim();
                    if (pageName && storedName && pageName === storedName) return true;
                }
            }
        } catch (e) {
            console.log('[SuperLinkedIn] Could not check owner name:', e.message);
        }

        return false;
    }

    function sendToBackground(data) {
        if (!isExtensionValid()) return;
        try {
            chrome.runtime.sendMessage({ type: 'ANALYTICS_DATA', payload: data });
        } catch (e) { /* extension context invalidated */ }
    }

    function scrapeAnalyticsDashboard() {
        const stats = {};
        const bodyText = document.body.innerText || '';

        // === STRATEGY A: Walk DOM leaf nodes to find standalone numbers near labels ===
        const labelMap = {
            'impression': 'postImpressions',
            'follower': 'followers',
            'profile view': 'profileViews',
            'search appear': 'searchAppearances',
            'member': 'membersReached',
            'reached': 'membersReached',
            'social engagement': 'socialEngagements',
        };

        document.querySelectorAll('p, span, div, h1, h2, h3, h4, strong, b, li, td').forEach(el => {
            const text = (el.textContent || '').trim();
            if (!/^\d[\d,.]*[KMB]?$/.test(text)) return;
            if (el.querySelector('p, span, div, h1, h2, h3, h4')) return;

            const num = parseNumber(text);
            if (num <= 0) return;

            const contexts = [];
            let walk = el;
            for (let depth = 0; depth < 4 && walk; depth++) {
                walk = walk.parentElement;
                if (walk) contexts.push((walk.textContent || '').toLowerCase());
            }
            const ns = el.nextElementSibling;
            if (ns) contexts.push((ns.textContent || '').toLowerCase());
            const ps = el.previousElementSibling;
            if (ps) contexts.push((ps.textContent || '').toLowerCase());

            const combined = contexts.join(' ');
            for (const [keyword, key] of Object.entries(labelMap)) {
                if (combined.includes(keyword) && !stats[key]) {
                    // 'followers' values < 5 are almost always widget noise
                    // (e.g. a "1 new follower" panel). Skip them.
                    if (key === 'followers' && num < 5) break;
                    stats[key] = num;
                    break;
                }
            }
        });

        // === STRATEGY B: Regex on full page text with flexible whitespace ===
        const flexWS = '[\\s\\u00a0\\n\\r]{0,15}';
        const fwdPatterns = [
            { key: 'postImpressions', re: new RegExp('(\\d[\\d,.]*[KMB]?)' + flexWS + '(?:Post )?[Ii]mpressions?', 'i') },
            { key: 'followers', re: new RegExp('(\\d[\\d,.]*[KMB]?)' + flexWS + '(?:Total )?[Ff]ollowers?', 'i') },
            { key: 'profileViews', re: new RegExp('(\\d[\\d,.]*[KMB]?)' + flexWS + '[Pp]rofile viewers?', 'i') },
            { key: 'searchAppearances', re: new RegExp('(\\d[\\d,.]*[KMB]?)' + flexWS + '[Ss]earch appear', 'i') },
            { key: 'membersReached', re: new RegExp('(\\d[\\d,.]*[KMB]?)' + flexWS + '[Mm]embers?' + flexWS + 'reached', 'i') },
            { key: 'socialEngagements', re: new RegExp('(\\d[\\d,.]*[KMB]?)' + flexWS + '[Ss]ocial engagements?', 'i') },
        ];
        fwdPatterns.forEach(({ key, re }) => {
            if (!stats[key]) {
                const m = bodyText.match(re);
                if (m) {
                    const v = parseNumber(m[1]);
                    // 'followers' is heavily polluted by widgets like "1 new
                    // follower this week" — require >= 5 to consider it the
                    // real total. Other stats can be any positive value.
                    const minVal = (key === 'followers') ? 5 : 1;
                    if (v >= minVal) stats[key] = v;
                }
            }
        });

        // === STRATEGY C: Reverse patterns (label before number) ===
        const revPatterns = [
            { key: 'postImpressions', re: new RegExp('[Ii]mpressions?' + flexWS + '(\\d[\\d,.]*[KMB]?)', 'i') },
            { key: 'membersReached', re: new RegExp('[Mm]embers?' + flexWS + 'reached' + flexWS + '(\\d[\\d,.]*[KMB]?)', 'i') },
            { key: 'socialEngagements', re: new RegExp('[Ss]ocial engagements?' + flexWS + '(\\d[\\d,.]*[KMB]?)', 'i') },
        ];
        revPatterns.forEach(({ key, re }) => {
            if (!stats[key]) {
                const m = bodyText.match(re);
                if (m) {
                    const v = parseNumber(m[1]);
                    if (v > 0) stats[key] = v;
                }
            }
        });

        // === STRATEGY D: Content performance header value ===
        const cpMatch = bodyText.match(/Content performance[\s\S]{0,200}?(\d[\d,.]*[KMB]?)\s/);
        if (cpMatch && !stats.postImpressions) {
            const v = parseNumber(cpMatch[1]);
            if (v > 0) stats.postImpressions = v;
        }

        // === STRATEGY E: Top performing posts ===
        const topPosts = [];
        const tpRe = /(\d[\d,.]*[KMB]?)\s*impressions?\s*[•·\-]\s*(\d[\d,.]*[KMB]?)\s*engagements?/gi;
        let tpM;
        while ((tpM = tpRe.exec(bodyText)) !== null) {
            topPosts.push({ impressions: parseNumber(tpM[1]), engagements: parseNumber(tpM[2]) });
        }
        if (topPosts.length > 0) stats.topPerformingPosts = topPosts;

        console.log('[SuperLinkedIn] Dashboard scrape result:', JSON.stringify(stats));
        if (Object.keys(stats).length === 0) {
            console.log('[SuperLinkedIn] Page text (first 1000 chars):', bodyText.substring(0, 1000));
        }

        return Object.keys(stats).length > 0 ? stats : null;
    }

    function isExtensionValid() {
        try {
            const url = chrome.runtime.getURL('');
            return url && !url.includes('invalid');
        } catch (e) {
            return false;
        }
    }

    async function runScrape(retryCount) {
        if (!isExtensionValid()) {
            if (_scrapeInterval) { clearInterval(_scrapeInterval); _scrapeInterval = null; }
            if (_urlObserver) { _urlObserver.disconnect(); _urlObserver = null; }
            console.log('[SuperLinkedIn] Extension context invalidated, all timers stopped.');
            return;
        }
        try {
        retryCount = retryCount || 0;
        const url = window.location.href;
        const data = { url, timestamp: new Date().toISOString() };

        // ── Connections / followers ──
        // scrapeFollowers() handles its own page-eligibility check (mynetwork
        // is preferred, /in/<vanity>/ is next, then a strict feed/sidebar
        // fallback). Always call it so the stat refreshes when the user
        // visits any of those pages, not only /in/.
        if (url.includes('/in/')) {
            const ownProfile = await isOwnProfile();
            if (ownProfile) {
                const followers = scrapeFollowers();
                if (followers !== null) {
                    data.followers = followers;
                    sidebarData.followers = followers;
                }
                const profile = scrapeProfileInfo();
                data.profile = profile;
                console.log('[SuperLinkedIn] Own profile detected, scraped followers:', followers);
            } else {
                console.log('[SuperLinkedIn] Visiting another profile, skipping follower/stats sync');
            }
        } else if (url.includes('/mynetwork') || url.includes('/feed') || url.includes('/notifications') || url.includes('/jobs') || url.includes('/messaging')) {
            const followers = scrapeFollowers();
            if (followers !== null) {
                data.followers = followers;
                sidebarData.followers = followers;
                console.log('[SuperLinkedIn] Connections scraped from', url.substring(0, 80), '=>', followers);
            }
        }

        // ── Feed identity sidebar — Profile viewers / Post impressions ──
        // The feed page's left-rail user card shows the same metrics LinkedIn
        // surfaces on its own analytics dashboard, just for the last 7 days.
        // Scrape them here so the popup updates without waiting for the user
        // to visit the analytics dashboard.
        if (url.includes('/feed')) {
            const feedStats = scrapeFeedSidebarStats();
            if (feedStats && (feedStats.postImpressions || feedStats.profileViews)) {
                data.dashboardStats = Object.assign({}, data.dashboardStats || {}, feedStats);
                if (feedStats.postImpressions) sidebarData.totalImpressions = feedStats.postImpressions;
                console.log('[SuperLinkedIn] Feed sidebar stats scraped:', feedStats);
            }
        }

        const isAnalyticsPage = url.includes('/dashboard') || url.includes('/analytics') || url.includes('/creator');
        if (isAnalyticsPage) {
            const dashboardStats = scrapeAnalyticsDashboard();
            if (dashboardStats) {
                data.dashboardStats = Object.assign({}, data.dashboardStats || {}, dashboardStats);
                if (dashboardStats.followers) sidebarData.followers = dashboardStats.followers;
                if (dashboardStats.postImpressions) sidebarData.totalImpressions = dashboardStats.postImpressions;
                if (dashboardStats.socialEngagements) sidebarData.totalLikes = dashboardStats.socialEngagements;
            } else if (retryCount < 3) {
                console.log('[SuperLinkedIn] Analytics page detected but no data found, retry', retryCount + 1, 'of 3 in 5s...');
                setTimeout(() => runScrape(retryCount + 1), 5000);
                return;
            }
        }

        const shouldScrapePosts = url.includes('/feed') || url.includes('/posts/') || (url.includes('/in/') && data.profile);
        if (shouldScrapePosts) {
            const posts = await scrapePostMetrics();
            if (posts.length > 0) {
                data.posts = posts;
                updateSidebarData(posts);
            }
        }

        if (url.includes('/feed')) {
            const networkPosts = await scrapeFeedPosts();
            if (networkPosts.length > 0) {
                data.feedPosts = networkPosts;
                console.log('[SuperLinkedIn] Feed posts scraped for Discover:', networkPosts.length);
            }
        }

        if (url.includes('/messaging')) {
            const dmData = await scrapeMessaging();
            if (dmData && dmData.conversations.length > 0) {
                data.dms = dmData;
                console.log('[SuperLinkedIn] DM data scraped:', dmData.conversations.length, 'conversations');
            } else if (retryCount < 3) {
                console.log('[SuperLinkedIn] Messaging page detected but no DMs found, retry', retryCount + 1, 'of 3 in 5s...');
                setTimeout(() => runScrape(retryCount + 1), 5000);
                return;
            }
        }

        if (data.followers !== undefined || (data.posts && data.posts.length > 0) || data.dashboardStats || data.dms || data.feedPosts) {
            console.log('[SuperLinkedIn] Scraped data:', JSON.stringify(data.dashboardStats || {}), 'posts:', (data.posts || []).length);
            sendToBackground(data);
        } else {
            console.log('[SuperLinkedIn] No data found on this page:', url.substring(0, 80));
        }

        if (sidebarOpen) updateSidebarUI();
        } catch (err) {
            console.log('[SuperLinkedIn] Scrape error:', err.message);
        }
    }

    function updateSidebarData(posts) {
        const existing = new Set(sidebarData.posts.map(p => p.text));
        posts.forEach(p => {
            if (!existing.has(p.text)) {
                sidebarData.posts.push(p);
                existing.add(p.text);
            }
        });
        if (sidebarData.posts.length > 100) sidebarData.posts = sidebarData.posts.slice(-100);

        sidebarData.topPosts = [...sidebarData.posts]
            .sort((a, b) => b.engagement - a.engagement)
            .slice(0, 5);

        sidebarData.totalLikes = sidebarData.posts.reduce((s, p) => s + p.likes, 0);
        sidebarData.totalComments = sidebarData.posts.reduce((s, p) => s + p.comments, 0);
        sidebarData.totalReposts = sidebarData.posts.reduce((s, p) => s + p.reposts, 0);
        sidebarData.totalImpressions = sidebarData.posts.reduce((s, p) => s + p.impressions, 0);
    }

    // ── Sidebar UI ──
    function createSidebar() {
        if (document.getElementById('sl-sidebar')) return;

        const toggle = document.createElement('button');
        toggle.id = 'sl-toggle-btn';
        toggle.title = 'SuperLinkedIn';
        toggle.innerHTML = '<span class="sl-toggle-text">SL</span>';
        toggle.addEventListener('click', () => toggleUpgradePanel());
        document.body.appendChild(toggle);

        const sb = document.createElement('div');
        sb.id = 'sl-sidebar';
        sb.innerHTML = `
            <div class="sl-sb-header">
                <div class="sl-sb-logo">SL</div>
                <span class="sl-sb-brand">SuperLinkedIn</span>
                <button class="sl-sb-close" id="slSbClose">&times;</button>
            </div>

            <div class="sl-sb-tabs">
                <button class="sl-sb-tab active" data-panel="analytics">Analytics</button>
                <button class="sl-sb-tab" data-panel="aitools">AI Tools</button>
                <button class="sl-sb-tab" data-panel="inspiration">Tips</button>
            </div>

            <div class="sl-sb-body">
                <!-- Analytics Panel -->
                <div class="sl-sb-panel active" id="slPanelAnalytics">
                    <div class="sl-section-title">Overview</div>
                    <div class="sl-stats-row">
                        <div class="sl-stat-box">
                            <div class="sl-stat-val" id="slStatFollowers">--</div>
                            <div class="sl-stat-lbl">Connections</div>
                        </div>
                        <div class="sl-stat-box">
                            <div class="sl-stat-val" id="slStatPosts">--</div>
                            <div class="sl-stat-lbl">Posts</div>
                        </div>
                        <div class="sl-stat-box">
                            <div class="sl-stat-val" id="slStatEng">--</div>
                            <div class="sl-stat-lbl">Engagement</div>
                        </div>
                    </div>

                    <div class="sl-section-title">Top Posts</div>
                    <div class="sl-top-posts" id="slTopPosts">
                        <div class="sl-empty-state">
                            <div class="sl-empty-icon">&#128202;</div>
                            <div class="sl-empty-text">Scroll your feed to collect post analytics</div>
                        </div>
                    </div>

                    <div class="sl-section-title">Engagement Breakdown</div>
                    <div class="sl-eng-section">
                        <div class="sl-eng-row">
                            <span class="sl-eng-label">&#128077; Likes</span>
                            <div class="sl-eng-bar-wrap"><div class="sl-eng-bar likes" id="slEngLikes" style="width:0%"></div></div>
                            <span class="sl-eng-val" id="slEngLikesVal">0</span>
                        </div>
                        <div class="sl-eng-row">
                            <span class="sl-eng-label">&#128172; Comments</span>
                            <div class="sl-eng-bar-wrap"><div class="sl-eng-bar comments" id="slEngComments" style="width:0%"></div></div>
                            <span class="sl-eng-val" id="slEngCommentsVal">0</span>
                        </div>
                        <div class="sl-eng-row">
                            <span class="sl-eng-label">&#128257; Reposts</span>
                            <div class="sl-eng-bar-wrap"><div class="sl-eng-bar reposts" id="slEngReposts" style="width:0%"></div></div>
                            <span class="sl-eng-val" id="slEngRepostsVal">0</span>
                        </div>
                        <div class="sl-eng-row">
                            <span class="sl-eng-label">&#128065; Views</span>
                            <div class="sl-eng-bar-wrap"><div class="sl-eng-bar views" id="slEngViews" style="width:0%"></div></div>
                            <span class="sl-eng-val" id="slEngViewsVal">0</span>
                        </div>
                    </div>

                    <div class="sl-section-title">Profile Interactions</div>
                    <div class="sl-profile-section" id="slProfileSection">
                        <div class="sl-empty-state">
                            <div class="sl-empty-icon">&#128100;</div>
                            <div class="sl-empty-text">Visit your profile to see interactions</div>
                        </div>
                    </div>

                    <div class="sl-section-title">Quick Actions</div>
                    <div class="sl-quick-actions">
                        <a class="sl-action-card" href="${API_BASE}/app" target="_blank">
                            <span class="sl-action-icon">&#128200;</span>
                            <span class="sl-action-label">Dashboard</span>
                        </a>
                        <a class="sl-action-card" href="${API_BASE}/app#queue" target="_blank">
                            <span class="sl-action-icon">&#128197;</span>
                            <span class="sl-action-label">Queue</span>
                        </a>
                        <a class="sl-action-card" href="${API_BASE}/app#analytics" target="_blank">
                            <span class="sl-action-icon">&#128202;</span>
                            <span class="sl-action-label">Analytics</span>
                        </a>
                        <a class="sl-action-card" href="${API_BASE}/app#dms" target="_blank">
                            <span class="sl-action-icon">&#128172;</span>
                            <span class="sl-action-label">DMs</span>
                        </a>
                    </div>
                </div>

                <!-- AI Tools Panel -->
                <div class="sl-sb-panel" id="slPanelAitools">
                    <div class="sl-section-title">AI Post Writer</div>
                    <textarea class="sl-ai-textarea" id="slAiText" placeholder="Describe what you want to post about, or paste text to improve..."></textarea>
                    <div class="sl-ai-actions">
                        <button class="sl-ai-btn" id="slAiGenerate">&#129302; Generate Post</button>
                        <button class="sl-ai-btn secondary" id="slAiImprove">&#10024; Improve</button>
                    </div>

                    <div class="sl-section-title">Quick Improve</div>
                    <div class="sl-quick-improve">
                        <button class="sl-quick-btn" data-action="grammar">&#9998; Grammar</button>
                        <button class="sl-quick-btn" data-action="translate">&#127760; Translate</button>
                        <button class="sl-quick-btn" data-action="hook">&#10024; Hook</button>
                        <button class="sl-quick-btn" data-action="concise">&#9986; Concise</button>
                        <button class="sl-quick-btn" data-action="engaging">&#128171; Engaging</button>
                        <button class="sl-quick-btn" data-action="humorous">&#128514; Humorous</button>
                    </div>

                    <div id="slAiLoading" class="sl-ai-loading" style="display:none;">
                        <div class="sl-spinner"></div> Generating...
                    </div>
                    <div id="slAiResult" class="sl-ai-result" style="display:none;">
                        <div class="sl-section-title">Result</div>
                        <div class="sl-ai-result-text" id="slAiResultText"></div>
                        <button class="sl-ai-copy-btn" id="slAiCopy">&#128203; Copy to Clipboard</button>
                    </div>
                </div>

                <!-- Tips Panel -->
                <div class="sl-sb-panel" id="slPanelInspiration">
                    <div class="sl-section-title">Growth Tips</div>
                    <div class="sl-tip-card" style="border-left:4px solid #0A66C2;background:#f0f7ff;">
                        <div class="sl-tip-icon">&#128337;</div>
                        <div class="sl-tip-content">
                            <div class="sl-tip-title">Post at Peak Times</div>
                            <div class="sl-tip-text">Tuesday-Thursday, 8-10 AM your local time tends to get 2x more engagement.</div>
                        </div>
                    </div>
                    <div class="sl-tip-card" style="border-left:4px solid #22c55e;background:#f0fff4;">
                        <div class="sl-tip-icon">&#129693;</div>
                        <div class="sl-tip-content">
                            <div class="sl-tip-title">Use Strong Hooks</div>
                            <div class="sl-tip-text">Start with a bold statement or question. Posts with hooks get 40% more impressions.</div>
                        </div>
                    </div>
                    <div class="sl-tip-card" style="border-left:4px solid #f59e0b;background:#fffbeb;">
                        <div class="sl-tip-icon">&#128172;</div>
                        <div class="sl-tip-content">
                            <div class="sl-tip-title">Engage Before Posting</div>
                            <div class="sl-tip-text">Comment on 5 posts before publishing yours. The algorithm rewards active users.</div>
                        </div>
                    </div>
                    <div class="sl-tip-card" style="border-left:4px solid #8b5cf6;background:#f5f3ff;">
                        <div class="sl-tip-icon">&#128247;</div>
                        <div class="sl-tip-content">
                            <div class="sl-tip-title">Use Visuals</div>
                            <div class="sl-tip-text">Posts with images get 2x more comments. Carousels and documents get 3x more reach.</div>
                        </div>
                    </div>
                    <div class="sl-tip-card" style="border-left:4px solid #ef4444;background:#fef2f2;">
                        <div class="sl-tip-icon">&#128640;</div>
                        <div class="sl-tip-content">
                            <div class="sl-tip-title">Be Consistent</div>
                            <div class="sl-tip-text">Post 3-5 times per week. Consistency signals the algorithm to boost your content.</div>
                        </div>
                    </div>

                </div>
            </div>

            <div class="sl-sb-footer">
                <a href="${API_BASE}/app" target="_blank">&#127968; Open Dashboard</a>
                <span>SuperLinkedIn v1.5</span>
            </div>
        `;
        document.body.appendChild(sb);

        sb.querySelector('#slSbClose').addEventListener('click', () => toggleSidebar());

        sb.querySelectorAll('.sl-sb-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                sb.querySelectorAll('.sl-sb-tab').forEach(t => t.classList.remove('active'));
                sb.querySelectorAll('.sl-sb-panel').forEach(p => p.classList.remove('active'));
                tab.classList.add('active');
                const panel = sb.querySelector('#slPanel' + capitalize(tab.dataset.panel));
                if (panel) panel.classList.add('active');
            });
        });

        sb.querySelector('#slAiGenerate').addEventListener('click', () => sidebarAiAction('generate'));
        sb.querySelector('#slAiImprove').addEventListener('click', () => sidebarAiAction('improve'));
        sb.querySelector('#slAiCopy').addEventListener('click', () => {
            navigator.clipboard.writeText(document.getElementById('slAiResultText').textContent);
            sb.querySelector('#slAiCopy').textContent = 'Copied!';
            setTimeout(() => sb.querySelector('#slAiCopy').textContent = 'Copy', 1500);
        });

        sb.querySelectorAll('.sl-quick-btn').forEach(btn => {
            btn.addEventListener('click', () => sidebarAiAction(btn.dataset.action));
        });

    }

    function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

    function toggleSidebar() {
        createSidebar();
        const sb = document.getElementById('sl-sidebar');
        const toggle = document.getElementById('sl-toggle-btn');
        sidebarOpen = !sidebarOpen;
        sb.classList.toggle('open', sidebarOpen);
        toggle.classList.toggle('shifted', sidebarOpen);
        toggle.classList.toggle('active', sidebarOpen);
        if (sidebarOpen) updateSidebarUI();
    }

    // ── Upgrade Panel (opened from the floating SL button) ──
    let upgradePanelOpen = false;

    function createUpgradePanel() {
        if (document.getElementById('sl-upgrade-panel')) return;

        const panel = document.createElement('div');
        panel.id = 'sl-upgrade-panel';
        panel.innerHTML = `
            <div class="sl-up-header">
                <div class="sl-up-logo">SL</div>
                <span class="sl-up-brand" id="slUpHeaderTitle">SuperLinkedIn</span>
                <button class="sl-up-icon-btn" id="slUpSettingsBtn" title="Settings" aria-label="Settings">
                    <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"></circle><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"></path></svg>
                </button>
                <button class="sl-up-icon-btn" id="slUpBackBtn" title="Back" aria-label="Back" style="display:none;">
                    <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"></polyline></svg>
                </button>
                <button class="sl-up-close" id="slUpClose" aria-label="Close">&times;</button>
            </div>

            <div class="sl-up-body" id="slUpBody">
                <div class="sl-up-view" id="slUpViewMain">
                    <div id="slUpConnBanner" class="sl-up-conn connected" style="display:none;">
                        <span class="sl-up-dot"></span>
                        <div class="sl-up-conn-text">
                            <div class="sl-up-conn-label">CONNECTED AS</div>
                            <div class="sl-up-conn-name" id="slUpUserName">--</div>
                        </div>
                        <span class="sl-up-plan" id="slUpPlanBadge">PRO</span>
                    </div>

                    <div id="slUpLoggedOut" class="sl-up-conn muted" style="display:none;">
                        <span class="sl-up-dot offline"></span>
                        <div class="sl-up-conn-text">
                            <div class="sl-up-conn-label">NOT CONNECTED</div>
                            <a href="#" id="slUpSignIn" class="sl-up-link">Sign in to your dashboard &rarr;</a>
                        </div>
                    </div>

                    <div class="sl-up-hero">
                        <span class="sl-up-pill">NEW RELEASE</span>
                        <h2 class="sl-up-title">Introducing SuperLinkedIn 2.0</h2>
                        <p class="sl-up-sub">The biggest upgrade we've ever made</p>
                    </div>

                    <button type="button" class="sl-up-webapp" id="slUpWebApp">
                        <div>
                            <div class="sl-up-webapp-title">SuperLinkedIn Web App</div>
                            <div class="sl-up-webapp-sub">Included with your subscription</div>
                        </div>
                        <span class="sl-up-webapp-arrow">&rarr;</span>
                    </button>

                    <div class="sl-up-card">
                        <div class="sl-up-card-title">WHAT'S NEW</div>
                        <div class="sl-up-features">
                            <div class="sl-up-feature">
                                <span class="sl-up-check">&#10004;</span>
                                <div>
                                    <div class="sl-up-feature-name">Engage</div>
                                    <div class="sl-up-feature-desc">Reply with intention, grow your audience, all in one focused space</div>
                                </div>
                            </div>
                            <div class="sl-up-feature">
                                <span class="sl-up-check">&#10004;</span>
                                <div>
                                    <div class="sl-up-feature-name">Web App Included</div>
                                    <div class="sl-up-feature-desc">Full SuperLinkedIn web app access with your subscription</div>
                                </div>
                            </div>
                            <div class="sl-up-feature">
                                <span class="sl-up-check">&#10004;</span>
                                <div>
                                    <div class="sl-up-feature-name">Pay Less, Get More</div>
                                    <div class="sl-up-feature-desc">Flexible plans that fit your budget</div>
                                </div>
                            </div>
                            <div class="sl-up-feature">
                                <span class="sl-up-check">&#10004;</span>
                                <div>
                                    <div class="sl-up-feature-name">Streamlined Experience</div>
                                    <div class="sl-up-feature-desc">Spend less time waiting, more time growing</div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="sl-up-card sl-up-plans-card">
                        <div class="sl-up-card-title centered">Choose Your Plan</div>

                        <div class="sl-up-plan-card">
                            <div class="sl-up-plan-price-row">
                                <span class="sl-up-plan-price">$39</span>
                                <span class="sl-up-plan-period">/month</span>
                            </div>
                            <div class="sl-up-plan-tagline">Start growing your audience</div>
                            <button type="button" class="sl-up-plan-btn outline" data-plan="pro">Upgrade to PRO</button>
                        </div>

                        <div class="sl-up-plan-card highlighted">
                            <span class="sl-up-plan-discount">-20% OFF</span>
                            <div class="sl-up-plan-name-tag">ADVANCED</div>
                            <div class="sl-up-plan-price-row">
                                <span class="sl-up-plan-price">$39</span>
                                <span class="sl-up-plan-old">$49</span>
                                <span class="sl-up-plan-period">/month</span>
                            </div>
                            <div class="sl-up-plan-tagline">Launch offer (regular price at $49/month)</div>
                            <button type="button" class="sl-up-plan-btn primary" data-plan="advanced">Upgrade to ADVANCED</button>
                        </div>

                        <div class="sl-up-plan-card">
                            <div class="sl-up-plan-name-tag">ULTRA</div>
                            <div class="sl-up-plan-price-row">
                                <span class="sl-up-plan-price">$199</span>
                                <span class="sl-up-plan-period">/month</span>
                            </div>
                            <div class="sl-up-plan-tagline">Go further with the highest limits</div>
                            <button type="button" class="sl-up-plan-btn outline" data-plan="ultra">Upgrade to ULTRA</button>
                        </div>

                        <a href="#" class="sl-up-show-features" id="slUpShowFeatures">Show features</a>

                        <div class="sl-up-fineprint">Direct payment - no trial needed</div>
                    </div>

                    <div class="sl-up-footer">
                        Questions? Contact us at <a href="mailto:info@superlinkedin.org">info@superlinkedin.org</a>
                    </div>
                </div>

                <div class="sl-up-view" id="slUpViewSettings" style="display:none;">
                    <div class="sl-up-settings-section">
                        <div class="sl-up-settings-heading">Theme mode</div>
                        <div class="sl-up-settings-sub">Choose your preferred theme color</div>
                        <div class="sl-up-theme-row" id="slUpThemeRow">
                            <button type="button" class="sl-up-theme-btn" data-theme="system">
                                <span class="sl-up-theme-swatch system"></span>
                                <span class="sl-up-theme-label">System</span>
                            </button>
                            <button type="button" class="sl-up-theme-btn" data-theme="dark">
                                <span class="sl-up-theme-swatch dark"></span>
                                <span class="sl-up-theme-label">Dark</span>
                            </button>
                            <button type="button" class="sl-up-theme-btn" data-theme="dim">
                                <span class="sl-up-theme-swatch dim"></span>
                                <span class="sl-up-theme-label">Dim</span>
                            </button>
                            <button type="button" class="sl-up-theme-btn" data-theme="light">
                                <span class="sl-up-theme-swatch light"></span>
                                <span class="sl-up-theme-label">Light</span>
                            </button>
                        </div>
                    </div>

                    <div class="sl-up-settings-section">
                        <div class="sl-up-settings-heading larger">Tabs</div>
                        <div class="sl-up-toggle-list" id="slUpToggleList">
                            <div class="sl-up-toggle-row" data-feature="home">
                                <div>
                                    <div class="sl-up-toggle-name">Home</div>
                                    <div class="sl-up-toggle-desc">User highlight, recent posts</div>
                                </div>
                                <button type="button" class="sl-up-switch on" data-feature="home" aria-label="Toggle Home"><span class="sl-up-switch-knob"></span></button>
                            </div>
                            <div class="sl-up-toggle-row" data-feature="composer">
                                <div>
                                    <div class="sl-up-toggle-name">Composer</div>
                                    <div class="sl-up-toggle-desc">Post scheduling</div>
                                </div>
                                <button type="button" class="sl-up-switch on" data-feature="composer" aria-label="Toggle Composer"><span class="sl-up-switch-knob"></span></button>
                            </div>
                            <div class="sl-up-toggle-row" data-feature="activities">
                                <div>
                                    <div class="sl-up-toggle-name">Activities</div>
                                    <div class="sl-up-toggle-desc">Detailed profile analytics</div>
                                </div>
                                <button type="button" class="sl-up-switch on" data-feature="activities" aria-label="Toggle Activities"><span class="sl-up-switch-knob"></span></button>
                            </div>
                            <div class="sl-up-toggle-row" data-feature="posts">
                                <div>
                                    <div class="sl-up-toggle-name">Posts</div>
                                    <div class="sl-up-toggle-desc">Table of user posts, comments, reposts</div>
                                </div>
                                <button type="button" class="sl-up-switch on" data-feature="posts" aria-label="Toggle Posts"><span class="sl-up-switch-knob"></span></button>
                            </div>
                            <div class="sl-up-toggle-row" data-feature="engage">
                                <div>
                                    <div class="sl-up-toggle-name">Engage</div>
                                    <div class="sl-up-toggle-desc">Never miss a mention again</div>
                                </div>
                                <button type="button" class="sl-up-switch on" data-feature="engage" aria-label="Toggle Engage"><span class="sl-up-switch-knob"></span></button>
                            </div>
                            <div class="sl-up-toggle-row" data-feature="timelines">
                                <div>
                                    <div class="sl-up-toggle-name">Timelines</div>
                                    <div class="sl-up-toggle-desc">Custom feeds</div>
                                </div>
                                <button type="button" class="sl-up-switch on" data-feature="timelines" aria-label="Toggle Timelines"><span class="sl-up-switch-knob"></span></button>
                            </div>
                            <div class="sl-up-toggle-row" data-feature="chat">
                                <div>
                                    <div class="sl-up-toggle-name">Chat</div>
                                    <div class="sl-up-toggle-desc">Chat with any profile using AI</div>
                                </div>
                                <button type="button" class="sl-up-switch" data-feature="chat" aria-label="Toggle Chat"><span class="sl-up-switch-knob"></span></button>
                            </div>
                        </div>
                    </div>

                    <div class="sl-up-settings-card">
                        <button type="button" class="sl-up-quick-link" id="slUpQuickWebApp">
                            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="2" y1="12" x2="22" y2="12"></line><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"></path></svg>
                            <span>Explore Web App</span>
                        </button>
                        <button type="button" class="sl-up-quick-link" id="slUpQuickSubscription">
                            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 7l10 5 10-5"></path><path d="M2 7v10l10 5 10-5V7"></path><path d="M12 2l10 5-10 5L2 7l10-5z"></path></svg>
                            <span>Manage subscription</span>
                        </button>
                        <button type="button" class="sl-up-quick-link" id="slUpQuickSupport">
                            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path></svg>
                            <span>Support &amp; Feedback</span>
                        </button>
                        <button type="button" class="sl-up-quick-link danger" id="slUpQuickLogout">
                            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg>
                            <span>Logout</span>
                        </button>
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(panel);

        const headerTitle = panel.querySelector('#slUpHeaderTitle');
        const settingsBtn = panel.querySelector('#slUpSettingsBtn');
        const backBtn = panel.querySelector('#slUpBackBtn');
        const viewMain = panel.querySelector('#slUpViewMain');
        const viewSettings = panel.querySelector('#slUpViewSettings');
        const body = panel.querySelector('#slUpBody');

        const showSettingsView = () => {
            viewMain.style.setProperty('display', 'none', 'important');
            viewSettings.style.setProperty('display', 'block', 'important');
            settingsBtn.style.setProperty('display', 'none', 'important');
            backBtn.style.setProperty('display', 'flex', 'important');
            headerTitle.textContent = 'Settings';
            body.scrollTop = 0;
        };
        const showMainView = () => {
            viewSettings.style.setProperty('display', 'none', 'important');
            viewMain.style.setProperty('display', 'block', 'important');
            settingsBtn.style.setProperty('display', 'flex', 'important');
            backBtn.style.setProperty('display', 'none', 'important');
            headerTitle.textContent = 'SuperLinkedIn';
            body.scrollTop = 0;
        };

        settingsBtn.addEventListener('click', showSettingsView);
        backBtn.addEventListener('click', showMainView);
        panel.querySelector('#slUpClose').addEventListener('click', () => toggleUpgradePanel());

        const dashUrl = 'https://app.superlinkedin.org/app';
        const upgradeUrl = 'https://app.superlinkedin.org/upgrade';
        const subscriptionUrl = 'https://app.superlinkedin.org/app#settings';
        const supportUrl = 'mailto:info@superlinkedin.org';

        const openTab = (url) => window.open(url, '_blank', 'noopener,noreferrer');

        panel.querySelector('#slUpWebApp').addEventListener('click', () => openTab(dashUrl));
        const signIn = panel.querySelector('#slUpSignIn');
        if (signIn) signIn.addEventListener('click', (e) => { e.preventDefault(); openTab(dashUrl); });
        panel.querySelector('#slUpShowFeatures').addEventListener('click', (e) => { e.preventDefault(); openTab(upgradeUrl); });

        panel.querySelectorAll('.sl-up-plan-btn[data-plan]').forEach(btn => {
            btn.addEventListener('click', () => {
                if (btn.classList.contains('current')) return;
                const plan = btn.dataset.plan || 'pro';
                openTab(`${upgradeUrl}?plan=${encodeURIComponent(plan)}`);
            });
        });

        // Quick links in Settings
        panel.querySelector('#slUpQuickWebApp').addEventListener('click', () => openTab(dashUrl));
        panel.querySelector('#slUpQuickSubscription').addEventListener('click', () => openTab(subscriptionUrl));
        panel.querySelector('#slUpQuickSupport').addEventListener('click', () => openTab(supportUrl));
        panel.querySelector('#slUpQuickLogout').addEventListener('click', () => {
            try {
                chrome.runtime.sendMessage({ type: 'LOGOUT' }, () => {
                    refreshUpgradePanelStatus();
                    showMainView();
                });
            } catch {
                refreshUpgradePanelStatus();
            }
        });

        // Theme picker
        const themeButtons = panel.querySelectorAll('.sl-up-theme-btn[data-theme]');
        themeButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                const theme = btn.dataset.theme || 'dim';
                applyUpgradeTheme(theme);
                try { chrome.storage.local.set({ slUpTheme: theme }); } catch {}
            });
        });

        // Feature toggles (UI-only, persisted to chrome.storage)
        const switches = panel.querySelectorAll('.sl-up-switch[data-feature]');
        switches.forEach(sw => {
            sw.addEventListener('click', () => {
                const on = sw.classList.toggle('on');
                const feature = sw.dataset.feature;
                try {
                    chrome.storage.local.get(['slUpFeatures'], (res) => {
                        const features = (res && res.slUpFeatures) || {};
                        features[feature] = on;
                        chrome.storage.local.set({ slUpFeatures: features });
                    });
                } catch {}
            });
        });

        // Restore saved theme + toggles
        try {
            chrome.storage.local.get(['slUpTheme', 'slUpFeatures'], (res) => {
                const theme = (res && res.slUpTheme) || 'dim';
                applyUpgradeTheme(theme);
                if (res && res.slUpFeatures) {
                    Object.entries(res.slUpFeatures).forEach(([feature, on]) => {
                        const sw = panel.querySelector(`.sl-up-switch[data-feature="${feature}"]`);
                        if (sw) sw.classList.toggle('on', !!on);
                    });
                }
            });
        } catch {
            applyUpgradeTheme('dim');
        }
    }

    function applyUpgradeTheme(theme) {
        const panel = document.getElementById('sl-upgrade-panel');
        if (!panel) return;
        panel.classList.remove('theme-system', 'theme-dark', 'theme-dim', 'theme-light');
        panel.classList.add(`theme-${theme}`);
        const buttons = panel.querySelectorAll('.sl-up-theme-btn[data-theme]');
        buttons.forEach(b => b.classList.toggle('selected', b.dataset.theme === theme));
    }

    async function refreshUpgradePanelStatus() {
        const connBanner = document.getElementById('slUpConnBanner');
        const loggedOut = document.getElementById('slUpLoggedOut');
        const userNameEl = document.getElementById('slUpUserName');
        const planBadgeEl = document.getElementById('slUpPlanBadge');
        if (!connBanner) return;

        try {
            const stored = await chrome.storage.local.get(['authToken', 'userName']);
            const token = stored && stored.authToken;
            if (!token) {
                connBanner.style.setProperty('display', 'none', 'important');
                if (loggedOut) loggedOut.style.setProperty('display', 'flex', 'important');
                return;
            }

            connBanner.style.setProperty('display', 'flex', 'important');
            if (loggedOut) loggedOut.style.setProperty('display', 'none', 'important');
            if (userNameEl) userNameEl.textContent = stored.userName || 'Connected';

            try {
                const res = await fetch('https://app.superlinkedin.org/api/analytics/summary', {
                    headers: { 'Authorization': `Bearer ${token}` }
                });
                if (res.ok) {
                    const data = await res.json();
                    const plan = String(data.plan || data.tier || '').toLowerCase();
                    if (planBadgeEl) {
                        if (!plan || plan === 'free' || plan === 'trial') {
                            planBadgeEl.textContent = 'FREE';
                        } else {
                            planBadgeEl.textContent = plan.toUpperCase();
                        }
                    }
                    document.querySelectorAll('.sl-up-plan-btn[data-plan]').forEach(btn => {
                        btn.classList.remove('current');
                        if (plan && btn.dataset.plan === plan) {
                            btn.classList.add('current');
                            btn.textContent = 'Current Plan';
                        }
                    });
                }
            } catch {
                // network – plan badge stays default
            }
        } catch {
            // storage failure – keep existing visibility state
        }
    }

    function toggleUpgradePanel() {
        createUpgradePanel();
        const panel = document.getElementById('sl-upgrade-panel');
        const toggle = document.getElementById('sl-toggle-btn');
        upgradePanelOpen = !upgradePanelOpen;
        panel.classList.toggle('open', upgradePanelOpen);
        if (toggle) {
            toggle.classList.toggle('shifted', upgradePanelOpen);
            toggle.classList.toggle('active', upgradePanelOpen);
        }
        if (upgradePanelOpen) refreshUpgradePanelStatus();
    }

    function updateSidebarUI() {
        const sb = document.getElementById('sl-sidebar');
        if (!sb) return;

        const el = (id) => document.getElementById(id);

        el('slStatFollowers').textContent = formatNum(sidebarData.followers || 0);
        el('slStatPosts').textContent = formatNum(sidebarData.posts.length);
        const totalEng = sidebarData.totalLikes + sidebarData.totalComments + sidebarData.totalReposts;
        const avgEng = sidebarData.posts.length > 0
            ? ((totalEng / sidebarData.posts.length) * 100 / Math.max(sidebarData.totalImpressions / sidebarData.posts.length, 1)).toFixed(1) + '%'
            : '0%';
        el('slStatEng').textContent = avgEng;

        // Top posts
        const topContainer = el('slTopPosts');
        if (sidebarData.topPosts.length > 0) {
            topContainer.innerHTML = sidebarData.topPosts.map(p => `
                <div class="sl-post-card">
                    <div class="sl-post-text">${escapeHtml(p.text)}</div>
                    <div class="sl-post-metrics">
                        <span class="sl-post-metric"><b>${formatNum(p.likes)}</b> likes</span>
                        <span class="sl-post-metric"><b>${formatNum(p.comments)}</b> comments</span>
                        <span class="sl-post-metric"><b>${formatNum(p.reposts)}</b> reposts</span>
                    </div>
                </div>
            `).join('');
        }

        // Engagement bars
        const maxEng = Math.max(
            sidebarData.totalLikes, sidebarData.totalComments,
            sidebarData.totalReposts, sidebarData.totalImpressions, 1
        );
        el('slEngLikes').style.width = (sidebarData.totalLikes / maxEng * 100) + '%';
        el('slEngLikesVal').textContent = formatNum(sidebarData.totalLikes);
        el('slEngComments').style.width = (sidebarData.totalComments / maxEng * 100) + '%';
        el('slEngCommentsVal').textContent = formatNum(sidebarData.totalComments);
        el('slEngReposts').style.width = (sidebarData.totalReposts / maxEng * 100) + '%';
        el('slEngRepostsVal').textContent = formatNum(sidebarData.totalReposts);
        el('slEngViews').style.width = (sidebarData.totalImpressions / maxEng * 100) + '%';
        el('slEngViewsVal').textContent = formatNum(sidebarData.totalImpressions);

        // Profile section
        if (window.location.href.includes('/in/')) {
            const profile = scrapeProfileInfo();
            if (profile.name) {
                const initials = profile.name.split(' ').map(w => w[0]).join('').substring(0, 2).toUpperCase();
                el('slProfileSection').innerHTML = `
                    <div class="sl-profile-card">
                        <div class="sl-profile-avatar">${initials}</div>
                        <div>
                            <div class="sl-profile-name">${escapeHtml(profile.name)}</div>
                            <div class="sl-profile-headline">${escapeHtml(profile.headline).substring(0, 60)}</div>
                        </div>
                    </div>
                    <div class="sl-interaction-grid">
                        <div class="sl-interact-item">
                            <div class="sl-interact-val">${formatNum(sidebarData.followers || 0)}</div>
                            <div class="sl-interact-lbl">Connections</div>
                        </div>
                        <div class="sl-interact-item">
                            <div class="sl-interact-val">${sidebarData.posts.length}</div>
                            <div class="sl-interact-lbl">Posts Tracked</div>
                        </div>
                        <div class="sl-interact-item">
                            <div class="sl-interact-val">${formatNum(sidebarData.totalLikes)}</div>
                            <div class="sl-interact-lbl">Total Likes</div>
                        </div>
                        <div class="sl-interact-item">
                            <div class="sl-interact-val">${formatNum(sidebarData.totalImpressions)}</div>
                            <div class="sl-interact-lbl">Impressions</div>
                        </div>
                    </div>
                `;
            }
        }
    }

    async function sidebarAiAction(type) {
        const textarea = document.getElementById('slAiText');
        const resultDiv = document.getElementById('slAiResult');
        const resultText = document.getElementById('slAiResultText');
        const loadingDiv = document.getElementById('slAiLoading');
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
                resultText.textContent = 'Please connect your account first (use the extension popup).';
                return;
            }

            const endpoint = type === 'generate' ? '/api/ai/write' : '/api/ai/improve';
            const body = type === 'generate'
                ? { prompt: text || 'Write me an engaging LinkedIn post about professional growth', tone: 'auto' }
                : { text, action: type };

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
            resultText.textContent = data.post || data.text || data.result || data.error || 'No result';
        } catch {
            loadingDiv.style.display = 'none';
            resultDiv.style.display = 'block';
            resultText.textContent = 'Error: Could not connect to server.';
        }
    }

    // ── DM Scraper ──
    async function scrapeMessaging() {
        const conversations = [];
        const seen = new Set();

        const junkPatterns = /^(messaging|linkedin|search|compose|new message|filter|starred|unread|my connections|other|inmail|sponsored|focused|archive|view |learn how|try premium|upgrade|promoted|ad\b)/i;
        const junkContains = /\bprofile\b|\bcompany:|\bconnect with\b|\bfollow\b|\bview\b.*\bprofile\b|\bpeople you may know\b|\bsuggested\b|\bjob alert/i;
        function isValidName(n) {
            if (!n || n.length < 2 || n.length > 80) return false;
            if (junkPatterns.test(n)) return false;
            if (junkContains.test(n)) return false;
            if (/^[\d\s.,!?]+$/.test(n)) return false;
            if (n.split(' ').length > 6) return false;
            return true;
        }

        // Strategy 1: Standard class-based selectors for conversation list items
        let convItems = document.querySelectorAll(
            '.msg-conversation-listitem, .msg-conversations-container__convo-item, ' +
            'li.msg-conversation-card, li[class*="msg-conversation"]'
        );

        // Strategy 2: Look for conversation list items via thread links
        if (convItems.length === 0) {
            const threadLinks = document.querySelectorAll('a[href*="/messaging/thread/"]');
            if (threadLinks.length > 0) {
                const items = new Set();
                threadLinks.forEach(a => {
                    const li = a.closest('li') || a.closest('[role="listitem"]') || a.parentElement;
                    if (li) items.add(li);
                });
                convItems = Array.from(items);
            }
        }

        // Strategy 3: Find list container with class containing "msg-conversation"
        if (convItems.length === 0) {
            const listContainer = document.querySelector('ul[class*="msg-conversations-container__conversations-list"], ul[class*="list-style-none"][class*="msg"]');
            if (listContainer) {
                convItems = listContainer.querySelectorAll(':scope > li');
            }
        }

        console.log('[SuperLinkedIn] DM scrape: found', convItems.length, 'candidate elements');

        convItems.forEach((el, i) => {
            let name = '';

            // Try specific class selectors for name
            const nameSelectors = [
                '[class*="participant-names"]',
                '[class*="conversation-listitem__participant"]',
                '[class*="conversation-card__participant"]',
                'h3[class*="truncate"]',
            ];
            for (const sel of nameSelectors) {
                const found = el.querySelector(sel);
                if (found) {
                    const t = found.textContent.trim().replace(/\n/g, ' ').replace(/\s+/g, ' ');
                    if (isValidName(t)) { name = t; break; }
                }
            }

            // Fallback: img alt text (profile photos)
            if (!name) {
                const imgs = el.querySelectorAll('img[alt]');
                for (const img of imgs) {
                    const alt = (img.getAttribute('alt') || '').trim();
                    if (isValidName(alt) && !alt.toLowerCase().includes('linkedin')) { name = alt; break; }
                }
            }

            // Skip if no valid name or duplicate
            if (!name || seen.has(name.toLowerCase())) return;

            // Final check: reject entries whose full text looks like a non-DM element
            const fullText = (el.textContent || '').trim().toLowerCase();
            if (/^view\s|learn how|try premium|people you may know|suggested|job alert|get hired|grow your network|boost your/i.test(fullText)) return;
            if (/learn how|try premium|people you may know|suggested for you|get hired faster/i.test(fullText)) return;
            if (fullText.includes("'s profile") || fullText.includes('\u2019s profile')) return;
            if (/^view company|view .{1,40} profile/i.test(fullText)) return;

            seen.add(name.toLowerCase());

            // Find message preview
            let preview = '';
            const previewSelectors = [
                '[class*="message-snippet"]',
                '[class*="conversation-card__message"]',
                'p[class*="truncate"]',
            ];
            for (const sel of previewSelectors) {
                const found = el.querySelector(sel);
                if (found) {
                    const t = found.textContent.trim().replace(/\n/g, ' ').replace(/\s+/g, ' ');
                    if (t && t.length > 0 && t !== name) { preview = t; break; }
                }
            }

            // Find timestamp
            let time = '';
            const timeEl = el.querySelector('time');
            if (timeEl) {
                time = timeEl.getAttribute('datetime') || timeEl.textContent.trim();
            }

            // Check unread status
            const elClasses = (typeof el.className === 'string' ? el.className : '') + ' ' + el.innerHTML.substring(0, 500);
            const unread = elClasses.includes('unread');

            // Find profile picture (LinkedIn CDN avatars). Prefer images whose alt matches the name
            // so we don't accidentally pick up reaction emoji or company logos in the row.
            let participantPicture = '';
            const candidateImgs = el.querySelectorAll('img[src]');
            const nameLower = name.toLowerCase();
            for (const img of candidateImgs) {
                const src = img.getAttribute('src') || '';
                const alt = (img.getAttribute('alt') || '').toLowerCase();
                if (!src.startsWith('http')) continue;
                // LinkedIn profile photo CDNs
                const isProfileCdn = /media-exp\d+\.licdn\.com|media\.licdn\.com|static\.licdn\.com\/sc\/h\/aahz/.test(src);
                if (!isProfileCdn) continue;
                // Skip ghost/default avatars
                if (/ghost-person|aahz/i.test(src)) continue;
                if (alt && alt === nameLower) { participantPicture = src; break; }
                if (!participantPicture) participantPicture = src; // first plausible match as fallback
            }

            conversations.push({
                id: 'dm-' + i + '-' + name.replace(/\s+/g, '-').toLowerCase().substring(0, 30),
                participantName: name,
                participantPicture: participantPicture,
                lastMessage: preview.substring(0, 200),
                lastMessageAt: time,
                unread: unread,
            });
        });

        // Scrape active thread messages (with client-side dedup and sent/received detection)
        let activeMessages = [];
        const seenMsgTexts = new Set();

        // Get owner name for sent/received detection
        let threadOwnerName = '';
        try {
            const stored = await chrome.storage.local.get('ownerName');
            threadOwnerName = (stored.ownerName || '').toLowerCase().replace(/[^a-z\s]/g, '').trim();
        } catch (e) {}

        const msgContainer = document.querySelector(
            '[class*="msg-s-message-list"], [class*="msg-thread"] [role="list"], ' +
            'ul[class*="msg-s-message-list"]'
        );
        if (msgContainer) {
            const msgEls = msgContainer.querySelectorAll(
                '[class*="msg-s-event-listitem"], [class*="msg-s-message-group"]'
            );
            msgEls.forEach(el => {
                let sender = '';
                const senderEl = el.querySelector('[class*="message-group__name"], [class*="event-listitem__name"]');
                if (senderEl) sender = senderEl.textContent.trim();

                // Detect if this message was sent by the logged-in user
                let isSent = false;
                if (sender) {
                    const normalSender = sender.toLowerCase().replace(/[^a-z\s]/g, '').trim();
                    if (normalSender === 'you' || (threadOwnerName && (
                        normalSender.includes(threadOwnerName) || threadOwnerName.includes(normalSender)
                    ))) {
                        isSent = true;
                    }
                }
                // LinkedIn often marks own messages with specific classes
                const elClasses = (typeof el.className === 'string' ? el.className : '').toLowerCase();
                if (elClasses.includes('msg-s-message-group--selfauthor') ||
                    elClasses.includes('self-author') ||
                    elClasses.includes('outbound') ||
                    el.querySelector('[class*="selfauthor"], [class*="self-author"]')) {
                    isSent = true;
                }

                let body = '';
                const bodyEl = el.querySelector('[class*="event-listitem__body"], [class*="message-group__body"], p[class*="msg-s-event"]');
                if (bodyEl) body = bodyEl.textContent.trim();
                if (!body) {
                    const ps = el.querySelectorAll('p');
                    for (const p of ps) {
                        const t = p.textContent.trim();
                        if (t && t.length > 0 && t !== sender) { body = t; break; }
                    }
                }

                let time = '';
                const timeEl = el.querySelector('time');
                if (timeEl) time = timeEl.getAttribute('datetime') || timeEl.textContent.trim();

                if (body && body.length > 1) {
                    const dedupKey = body.substring(0, 200).toLowerCase();
                    if (!seenMsgTexts.has(dedupKey)) {
                        seenMsgTexts.add(dedupKey);
                        activeMessages.push({
                            sender: isSent ? 'you' : (sender || 'them'),
                            text: body.substring(0, 500),
                            timestamp: time,
                        });
                    }
                }
            });
        }

        // Active conversation header name
        const headerName = document.querySelector(
            '[class*="msg-overlay-bubble-header__title"], [class*="msg-thread"] h2, ' +
            '[class*="entity-lockup__entity-title"]'
        );
        const activeConvName = headerName ? headerName.textContent.trim() : '';

        console.log('[SuperLinkedIn] DM scrape:', conversations.length, 'valid conversations,', activeMessages.length, 'messages in active thread');

        return {
            conversations,
            activeThread: activeMessages.length > 0 ? { participantName: activeConvName, messages: activeMessages } : null,
        };
    }

    // Wait until a selector exists (or the timeout elapses). Resolves with
    // the matched element or null. Used to coordinate with LinkedIn's SPA
    // navigation, which renders panels asynchronously.
    function waitForSelector(selector, timeoutMs) {
        return new Promise((resolve) => {
            const start = Date.now();
            const tick = () => {
                const el = document.querySelector(selector);
                if (el) return resolve(el);
                if (Date.now() - start >= timeoutMs) return resolve(null);
                setTimeout(tick, 150);
            };
            tick();
        });
    }

    // Normalise a participant name for comparison — strip diacritics, drop
    // suffixes like ", PhD", squash whitespace, lowercase.
    function normName(s) {
        return String(s || '')
            .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
            .replace(/,.*$/, '')
            .replace(/\s+/g, ' ')
            .trim()
            .toLowerCase();
    }

    function findConversationCard(recipientName) {
        const target = normName(recipientName);
        if (!target) return null;
        const cards = document.querySelectorAll(
            '.msg-conversation-listitem, .msg-conversation-card, li.msg-conversations-container__convo-item'
        );
        let bestPrefix = null;
        for (const card of cards) {
            const nameEl = card.querySelector(
                '.msg-conversation-listitem__participant-names, .msg-conversation-card__participant-names, .msg-conversation-listitem__participant-name'
            );
            const nm = normName(nameEl ? nameEl.textContent : '');
            if (!nm) continue;
            if (nm === target) return card;
            if (!bestPrefix && (nm.startsWith(target) || target.startsWith(nm))) bestPrefix = card;
        }
        return bestPrefix;
    }

    async function openConversation(recipientName) {
        // Try the search box first — it's the most reliable way to surface
        // the conversation, even if the user has thousands of threads.
        const search = document.querySelector('input[placeholder*="Search messages"], input.msg-search-typeahead__search-input');
        if (search) {
            search.focus();
            search.value = recipientName;
            search.dispatchEvent(new Event('input', { bubbles: true }));
            await new Promise(r => setTimeout(r, 1200));
        }
        let card = findConversationCard(recipientName);
        if (!card) {
            // Clear search and fall back to scanning the unfiltered list
            if (search) {
                search.value = '';
                search.dispatchEvent(new Event('input', { bubbles: true }));
                await new Promise(r => setTimeout(r, 600));
            }
            card = findConversationCard(recipientName);
        }
        if (!card) return false;
        // The clickable element is sometimes a child link; click whichever
        // exists to ensure LinkedIn routes to the thread.
        const link = card.querySelector('a.msg-conversation-listitem__link, a[href*="/messaging/thread/"]') || card;
        link.click();
        return true;
    }

    async function sendDmReply(recipientName, text) {
        try {
            if (!/(^|\.)linkedin\.com$/i.test(location.hostname)) {
                return { success: false, error: 'This tab is not on linkedin.com' };
            }

            // Navigate to messaging if we're somewhere else on LinkedIn. We
            // can't just set location.href because that aborts the message
            // listener; instead, resolve immediately with a "navigating"
            // error and let the next scheduler tick try again.
            if (!/\/messaging(\/|$)/.test(location.pathname)) {
                history.pushState({}, '', '/messaging/');
                window.dispatchEvent(new PopStateEvent('popstate'));
                // SPA route change isn't always picked up — fall back to a
                // hard navigation if the messaging container doesn't appear.
                const ok = await waitForSelector('.msg-conversations-container, .msg-overlay-list-bubble', 4000);
                if (!ok) {
                    location.href = 'https://www.linkedin.com/messaging/';
                    return { success: false, error: 'Opening LinkedIn Messaging — will retry shortly.' };
                }
            }

            const listEl = await waitForSelector('.msg-conversations-container, .msg-overlay-list-bubble', 8000);
            if (!listEl) {
                return { success: false, error: 'LinkedIn Messaging did not load. Reload linkedin.com/messaging and try again.' };
            }

            // Already in a thread with the right person? Just send.
            const activeHeader = document.querySelector('.msg-thread__link-to-profile, .msg-entity-lockup__entity-title');
            const onTarget = activeHeader && normName(activeHeader.textContent).startsWith(normName(recipientName).split(' ')[0]);
            if (!onTarget) {
                const opened = await openConversation(recipientName);
                if (!opened) {
                    return { success: false, error: `Could not find a conversation with "${recipientName}" in your inbox. Connect with them on LinkedIn first.` };
                }
            }

            const input = await waitForSelector('.msg-form__contenteditable, .msg-form__message-texteditor [contenteditable="true"]', 6000);
            if (!input) return { success: false, error: 'Message input did not load on LinkedIn.' };

            input.focus();
            input.innerHTML = '';
            const p = document.createElement('p');
            p.textContent = text;
            input.appendChild(p);
            input.dispatchEvent(new Event('input', { bubbles: true }));

            // Wait for LinkedIn to enable the send button after detecting input.
            let sendBtn = null;
            for (let i = 0; i < 20; i++) {
                sendBtn = document.querySelector('.msg-form__send-button:not([disabled]), button[type="submit"].msg-form__send-btn:not([disabled])');
                if (sendBtn) break;
                await new Promise(r => setTimeout(r, 150));
            }
            if (!sendBtn) {
                return { success: false, error: 'Send button stayed disabled — LinkedIn may have blocked the message field.' };
            }
            sendBtn.click();
            console.log('[SuperLinkedIn] DM sent to', recipientName);

            // Give LinkedIn ~700ms to actually post; if the input clears we
            // treat it as a successful send.
            await new Promise(r => setTimeout(r, 700));
            return { success: true };
        } catch (err) {
            return { success: false, error: (err && err.message) || 'Send failed' };
        }
    }

    // ── Message listener for popup commands ──
    chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
        if (msg.type === 'TOGGLE_SIDEBAR') {
            toggleSidebar();
            return false;
        }
        if (msg.type === 'DM_SEND_REPLY') {
            sendDmReply(msg.recipientName, msg.text).then(sendResponse).catch(err => {
                sendResponse({ success: false, error: (err && err.message) || 'Unhandled error' });
            });
            return true; // keep channel open for async response
        }
    });

    // ── Init ──
    let _scrapeInterval = null;
    let _urlObserver = null;

    function init() {
        if (!isExtensionValid()) return;
        createSidebar();
        setTimeout(runScrape, SCRAPE_DELAY);

        _urlObserver = new MutationObserver(() => {
            if (!isExtensionValid()) { _urlObserver.disconnect(); _urlObserver = null; return; }
            const currentUrl = window.location.href;
            if (currentUrl !== lastUrl) {
                lastUrl = currentUrl;
                setTimeout(runScrape, SCRAPE_DELAY);
            }
        });
        _urlObserver.observe(document.body, { childList: true, subtree: true });

        _scrapeInterval = setInterval(runScrape, 30000);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
