(function () {
    'use strict';

    const API_BASE = 'https://rwz9r5zqtw.us-east-1.awsapprunner.com';
    const SCRAPE_DELAY = 3000;
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
    function scrapeFollowers() {
        const selectors = [
            '.pv-top-card--list-bullet .t-bold',
            '.pvs-header__subtitle',
            '[data-test-id="follower-count"]',
        ];
        for (const sel of selectors) {
            const els = document.querySelectorAll(sel);
            for (const el of els) {
                if (/follower/i.test(el.textContent || '')) return parseNumber(el.textContent);
            }
        }
        const allText = document.body.innerText;
        const fm = allText.match(/([\d,.]+[KMB]?)\s+followers/i);
        if (fm) return parseNumber(fm[1]);
        const cm = allText.match(/([\d,.]+)\s+connections/i);
        if (cm) return parseNumber(cm[1]);
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

    function scrapeProfileInfo() {
        const nameEl = document.querySelector('.text-heading-xlarge, .pv-top-card--list li:first-child');
        const headlineEl = document.querySelector('.text-body-medium, .pv-top-card--experience-list-text');
        return {
            name: nameEl ? nameEl.textContent.trim() : '',
            headline: headlineEl ? headlineEl.textContent.trim() : '',
        };
    }

    function sendToBackground(data) {
        chrome.runtime.sendMessage({ type: 'ANALYTICS_DATA', payload: data });
    }

    function scrapeAnalyticsDashboard() {
        const stats = {};
        const bodyText = document.body.innerText || '';

        // Strategy 1: Scan all artdeco-card containers for stat+label pairs
        document.querySelectorAll('.artdeco-card, [class*="analytics"], [class*="dashboard"]').forEach(card => {
            const text = card.textContent || '';
            const numbers = text.match(/(\d[\d,.]*)\s*(Post impression|Impression|Follower|Profile viewer|Search appearance)/gi);
            if (numbers) {
                numbers.forEach(match => {
                    const num = parseNumber(match);
                    const lower = match.toLowerCase();
                    if (lower.includes('impression') && num > 0) stats.postImpressions = num;
                    if (lower.includes('follower') && num > 0) stats.followers = num;
                    if (lower.includes('profile viewer') && num > 0) stats.profileViews = num;
                    if (lower.includes('search appear') && num > 0) stats.searchAppearances = num;
                });
            }
        });

        // Strategy 2: Look for bold numbers next to descriptive text in the page
        document.querySelectorAll('.t-24, .t-bold, h2, h3, [class*="stat"], [class*="metric"]').forEach(el => {
            const num = parseNumber(el.textContent);
            if (num <= 0) return;
            const parent = el.parentElement;
            if (!parent) return;
            const context = parent.textContent.toLowerCase();
            if (context.includes('post impression') && !stats.postImpressions) stats.postImpressions = num;
            if (context.includes('follower') && !stats.followers) stats.followers = num;
            if (context.includes('profile viewer') && !stats.profileViews) stats.profileViews = num;
            if (context.includes('search appear') && !stats.searchAppearances) stats.searchAppearances = num;
            if (context.includes('members reached') && !stats.membersReached) stats.membersReached = num;
        });

        // Strategy 3: Regex on full page text as fallback
        const patterns = [
            { key: 'postImpressions', re: /(\d[\d,.]*[KMB]?)\s*(?:Post )?[Ii]mpressions?/i },
            { key: 'followers', re: /(\d[\d,.]*[KMB]?)\s*(?:Total )?[Ff]ollowers?/i },
            { key: 'profileViews', re: /(\d[\d,.]*[KMB]?)\s*[Pp]rofile viewers?/i },
            { key: 'searchAppearances', re: /(\d[\d,.]*[KMB]?)\s*[Ss]earch appear/i },
            { key: 'membersReached', re: /(\d[\d,.]*[KMB]?)\s*[Mm]embers? reached/i },
        ];
        patterns.forEach(({ key, re }) => {
            if (!stats[key]) {
                const m = bodyText.match(re);
                if (m) stats[key] = parseNumber(m[1]);
            }
        });

        return Object.keys(stats).length > 0 ? stats : null;
    }

    function runScrape() {
        const url = window.location.href;
        const data = { url, timestamp: new Date().toISOString() };

        if (url.includes('/in/')) {
            const followers = scrapeFollowers();
            if (followers !== null) {
                data.followers = followers;
                sidebarData.followers = followers;
            }
            const profile = scrapeProfileInfo();
            data.profile = profile;

            const dashboardStats = scrapeAnalyticsDashboard();
            if (dashboardStats) {
                data.dashboardStats = dashboardStats;
                if (dashboardStats.followers) sidebarData.followers = dashboardStats.followers;
            }
        }

        if (url.includes('/dashboard') || url.includes('/analytics') || url.includes('/creator')) {
            const dashboardStats = scrapeAnalyticsDashboard();
            if (dashboardStats) data.dashboardStats = dashboardStats;
        }

        if (url.includes('/feed') || url.includes('/in/') || url.includes('/posts/')) {
            const posts = scrapePostMetrics();
            if (posts.length > 0) {
                data.posts = posts;
                updateSidebarData(posts);
            }
        }

        if (data.followers !== undefined || (data.posts && data.posts.length > 0) || data.dashboardStats) {
            sendToBackground(data);
        }

        if (sidebarOpen) updateSidebarUI();
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
        toggle.innerHTML = '<span class="sl-toggle-text">SL</span>';
        toggle.addEventListener('click', () => toggleSidebar());
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
                <button class="sl-sb-tab" data-panel="inspiration">Inspiration</button>
            </div>

            <div class="sl-sb-body">
                <!-- Analytics Panel -->
                <div class="sl-sb-panel active" id="slPanelAnalytics">
                    <div class="sl-section-title">Overview</div>
                    <div class="sl-stats-row">
                        <div class="sl-stat-box">
                            <div class="sl-stat-val" id="slStatFollowers">--</div>
                            <div class="sl-stat-lbl">Followers</div>
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
                        <div style="text-align:center;color:#bbb;font-size:0.78rem;padding:16px;">
                            Scroll your feed to collect analytics
                        </div>
                    </div>

                    <div class="sl-section-title">Engagement Breakdown</div>
                    <div class="sl-eng-section">
                        <div class="sl-eng-row">
                            <span class="sl-eng-label">Likes</span>
                            <div class="sl-eng-bar-wrap"><div class="sl-eng-bar likes" id="slEngLikes" style="width:0%"></div></div>
                            <span class="sl-eng-val" id="slEngLikesVal">0</span>
                        </div>
                        <div class="sl-eng-row">
                            <span class="sl-eng-label">Comments</span>
                            <div class="sl-eng-bar-wrap"><div class="sl-eng-bar comments" id="slEngComments" style="width:0%"></div></div>
                            <span class="sl-eng-val" id="slEngCommentsVal">0</span>
                        </div>
                        <div class="sl-eng-row">
                            <span class="sl-eng-label">Reposts</span>
                            <div class="sl-eng-bar-wrap"><div class="sl-eng-bar reposts" id="slEngReposts" style="width:0%"></div></div>
                            <span class="sl-eng-val" id="slEngRepostsVal">0</span>
                        </div>
                        <div class="sl-eng-row">
                            <span class="sl-eng-label">Views</span>
                            <div class="sl-eng-bar-wrap"><div class="sl-eng-bar views" id="slEngViews" style="width:0%"></div></div>
                            <span class="sl-eng-val" id="slEngViewsVal">0</span>
                        </div>
                    </div>

                    <div class="sl-section-title">Profile Interactions</div>
                    <div class="sl-profile-section" id="slProfileSection">
                        <div style="text-align:center;color:#bbb;font-size:0.78rem;padding:16px;">
                            Visit a profile to see interactions
                        </div>
                    </div>
                </div>

                <!-- AI Tools Panel -->
                <div class="sl-sb-panel" id="slPanelAitools">
                    <div class="sl-section-title">AI Post Writer</div>
                    <textarea class="sl-ai-textarea" id="slAiText" placeholder="Write or paste your post here..."></textarea>
                    <div class="sl-ai-actions">
                        <button class="sl-ai-btn" id="slAiGenerate">&#129302; Generate</button>
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
                        <button class="sl-ai-copy-btn" id="slAiCopy">Copy</button>
                    </div>
                </div>

                <!-- Inspiration Panel -->
                <div class="sl-sb-panel" id="slPanelInspiration">
                    <div class="sl-section-title">Post Inspiration</div>
                    <div class="sl-inspiration-list" id="slInspirationList">
                        <div style="text-align:center;color:#bbb;font-size:0.78rem;padding:16px;">
                            Loading inspiration posts...
                        </div>
                    </div>

                    <div class="sl-section-title" style="margin-top:14px;">Tips for Growth</div>
                    <div class="sl-insp-card" style="background:#f0f7ff;border-color:#c7dcf0;">
                        <div class="sl-insp-text" style="font-size:0.75rem;color:#0A66C2;">
                            <b>Post at peak times:</b> Tuesday–Thursday, 8–10 AM your local time tends to get 2x more engagement.
                        </div>
                    </div>
                    <div class="sl-insp-card" style="background:#f0fff4;border-color:#c6f0d4;">
                        <div class="sl-insp-text" style="font-size:0.75rem;color:#16a34a;">
                            <b>Use hooks:</b> Start with a bold statement or question. Posts with strong hooks get 40% more impressions.
                        </div>
                    </div>
                    <div class="sl-insp-card" style="background:#fffbeb;border-color:#fde68a;">
                        <div class="sl-insp-text" style="font-size:0.75rem;color:#d97706;">
                            <b>Engage first:</b> Comment on 5 posts before publishing yours. The algorithm rewards active users.
                        </div>
                    </div>
                </div>
            </div>

            <div class="sl-sb-footer">
                <a href="${API_BASE}/app" target="_blank">Open Dashboard</a>
                <span>SuperLinkedIn v1.1</span>
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

        loadInspiration();
    }

    function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

    function toggleSidebar() {
        createSidebar();
        const sb = document.getElementById('sl-sidebar');
        const toggle = document.getElementById('sl-toggle-btn');
        sidebarOpen = !sidebarOpen;
        sb.classList.toggle('open', sidebarOpen);
        toggle.classList.toggle('shifted', sidebarOpen);
        if (sidebarOpen) updateSidebarUI();
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
                            <div class="sl-interact-lbl">Followers</div>
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
        } catch {
            loadingDiv.style.display = 'none';
            resultDiv.style.display = 'block';
            resultText.textContent = 'Error: Could not connect to server.';
        }
    }

    async function loadInspiration() {
        const container = document.getElementById('slInspirationList');
        if (!container) return;

        try {
            const { authToken } = await chrome.storage.local.get('authToken');
            if (!authToken) {
                container.innerHTML = '<div style="text-align:center;color:#bbb;font-size:0.78rem;padding:16px;">Connect your account to see inspiration</div>';
                return;
            }

            const res = await fetch(`${API_BASE}/api/inspiration/posts?limit=5`, {
                headers: { 'Authorization': `Bearer ${authToken}` }
            });

            if (!res.ok) {
                showFallbackInspiration(container);
                return;
            }

            const data = await res.json();
            const posts = data.posts || [];

            if (!posts.length) {
                showFallbackInspiration(container);
                return;
            }

            container.innerHTML = posts.map(p => `
                <div class="sl-insp-card">
                    <div class="sl-insp-text">${escapeHtml((p.text || '').substring(0, 150))}${(p.text || '').length > 150 ? '...' : ''}</div>
                    <div class="sl-insp-meta">${formatNum(p.likes || 0)} likes &middot; ${formatNum(p.comments || 0)} comments</div>
                </div>
            `).join('');
        } catch {
            showFallbackInspiration(container);
        }
    }

    function showFallbackInspiration(container) {
        const tips = [
            { text: "Share your biggest professional lesson from the past year. Be specific and vulnerable.", meta: "Storytelling format" },
            { text: "Post a contrarian take on a common industry belief. Back it up with data or experience.", meta: "Thought leadership" },
            { text: "Celebrate a team member's achievement publicly. Tag them and explain why it mattered.", meta: "Community building" },
            { text: "Share 3 tools or resources that changed how you work this quarter.", meta: "Value-driven content" },
            { text: "Write about a recent failure and what you learned. Authenticity drives engagement.", meta: "Authentic storytelling" },
        ];
        container.innerHTML = tips.map(t => `
            <div class="sl-insp-card">
                <div class="sl-insp-text">${t.text}</div>
                <div class="sl-insp-meta">${t.meta}</div>
            </div>
        `).join('');
    }

    // ── Message listener for popup commands ──
    chrome.runtime.onMessage.addListener((msg) => {
        if (msg.type === 'TOGGLE_SIDEBAR') {
            toggleSidebar();
        }
    });

    // ── Init ──
    function init() {
        createSidebar();
        setTimeout(runScrape, SCRAPE_DELAY);

        const observer = new MutationObserver(() => {
            const currentUrl = window.location.href;
            if (currentUrl !== lastUrl) {
                lastUrl = currentUrl;
                setTimeout(runScrape, SCRAPE_DELAY);
            }
        });
        observer.observe(document.body, { childList: true, subtree: true });

        setInterval(runScrape, 30000);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
