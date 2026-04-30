require('dotenv').config();
const express = require('express');
const session = require('express-session');
const crypto = require('crypto');
const path = require('path');
const Stripe = require('stripe');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

const app = express();
const PORT = process.env.PORT || 3000;

// ---------- DYNAMODB ----------

const DYNAMO_TABLE = process.env.DYNAMODB_TABLE || 'superlinkedin-users';

const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const ddb = DynamoDBDocumentClient.from(ddbClient);

async function dbGetUser(linkedinId) {
    try {
        const result = await ddb.send(new GetCommand({
            TableName: DYNAMO_TABLE,
            Key: { linkedinId },
        }));
        return result.Item || null;
    } catch (err) {
        console.error('DynamoDB getUser error:', err.message);
        return null;
    }
}

async function dbSaveUser(userData) {
    try {
        await ddb.send(new PutCommand({
            TableName: DYNAMO_TABLE,
            Item: { ...userData, updatedAt: new Date().toISOString() },
        }));
    } catch (err) {
        console.error('DynamoDB saveUser error:', err.message);
    }
}

async function dbUpdateFields(linkedinId, fields) {
    const keys = Object.keys(fields);
    if (keys.length === 0) return;

    const exprParts = [];
    const exprNames = {};
    const exprValues = { ':updatedAt': new Date().toISOString() };

    keys.forEach((key, i) => {
        const nameToken = `#f${i}`;
        const valToken = `:v${i}`;
        exprParts.push(`${nameToken} = ${valToken}`);
        exprNames[nameToken] = key;
        exprValues[valToken] = fields[key];
    });

    exprParts.push('#upd = :updatedAt');
    exprNames['#upd'] = 'updatedAt';

    try {
        await ddb.send(new UpdateCommand({
            TableName: DYNAMO_TABLE,
            Key: { linkedinId },
            UpdateExpression: 'SET ' + exprParts.join(', '),
            ExpressionAttributeNames: exprNames,
            ExpressionAttributeValues: exprValues,
        }));
    } catch (err) {
        console.error('DynamoDB updateFields error:', err.message);
    }
}

app.set('trust proxy', 1);

const LINKEDIN = {
    clientId: process.env.LINKEDIN_CLIENT_ID,
    clientSecret: process.env.LINKEDIN_CLIENT_SECRET,
    redirectUri: `${process.env.BASE_URL}/auth/linkedin/callback`,
    authUrl: 'https://www.linkedin.com/oauth/v2/authorization',
    tokenUrl: 'https://www.linkedin.com/oauth/v2/accessToken',
    userInfoUrl: 'https://api.linkedin.com/v2/userinfo',
    scope: 'openid profile email w_member_social',
};

app.use(session({
    secret: process.env.SESSION_SECRET || 'fallback-secret',
    resave: true,
    saveUninitialized: true,
    cookie: {
        secure: process.env.NODE_ENV === 'production',
        httpOnly: true,
        sameSite: 'lax',
        maxAge: 7 * 24 * 60 * 60 * 1000,
    },
}));

app.use(express.json());
app.use(express.static(path.join(__dirname), {
    index: 'index.html',
    extensions: ['html'],
}));

// Serve dashboard
app.get('/app', (req, res) => {
    res.sendFile(path.join(__dirname, 'app.html'));
});

// ---------- AUTH ROUTES ----------

// Step 1: Redirect user to LinkedIn authorization page
app.get('/auth/linkedin', (req, res) => {
    const state = crypto.randomBytes(32).toString('hex');
    req.session.oauthState = state;

    const params = new URLSearchParams({
        response_type: 'code',
        client_id: LINKEDIN.clientId,
        redirect_uri: LINKEDIN.redirectUri,
        scope: LINKEDIN.scope,
        state: state,
    });

    res.redirect(`${LINKEDIN.authUrl}?${params.toString()}`);
});

// Step 2: LinkedIn redirects back with an authorization code
app.get('/auth/linkedin/callback', async (req, res) => {
    const { code, state, error, error_description } = req.query;

    if (error) {
        return res.redirect(`/auth/error.html?message=${encodeURIComponent(error_description || 'Authorization denied')}`);
    }

    if (!code) {
        return res.redirect('/auth/error.html?message=' + encodeURIComponent('No authorization code received'));
    }

    if (state !== req.session.oauthState) {
        return res.redirect('/auth/error.html?message=' + encodeURIComponent('Invalid state parameter. Please try again.'));
    }

    try {
        // Step 3: Exchange authorization code for access token
        const tokenResponse = await fetch(LINKEDIN.tokenUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                grant_type: 'authorization_code',
                code: code,
                client_id: LINKEDIN.clientId,
                client_secret: LINKEDIN.clientSecret,
                redirect_uri: LINKEDIN.redirectUri,
            }),
        });

        if (!tokenResponse.ok) {
            const errData = await tokenResponse.text();
            console.error('Token exchange failed:', errData);
            return res.redirect('/auth/error.html?message=' + encodeURIComponent('Failed to exchange authorization code'));
        }

        const tokenData = await tokenResponse.json();
        const accessToken = tokenData.access_token;

        // Step 4: Fetch user profile from LinkedIn
        const profileResponse = await fetch(LINKEDIN.userInfoUrl, {
            headers: { Authorization: `Bearer ${accessToken}` },
        });

        if (!profileResponse.ok) {
            console.error('Profile fetch failed:', await profileResponse.text());
            return res.redirect('/auth/error.html?message=' + encodeURIComponent('Failed to fetch LinkedIn profile'));
        }

        const profile = await profileResponse.json();

        // Step 5: Look up existing user in DynamoDB, merge with session
        const dbUser = await dbGetUser(profile.sub) || {};
        const existingSession = req.session.user || {};

        req.session.user = {
            ...dbUser,
            ...existingSession,
            linkedinId: profile.sub,
            name: profile.name,
            email: profile.email,
            picture: profile.picture,
            accessToken: accessToken,
        };

        // Upsert basic profile to DynamoDB
        if (!dbUser.linkedinId) {
            await dbSaveUser({
                linkedinId: profile.sub,
                name: profile.name,
                email: profile.email,
                picture: profile.picture,
                paid: false,
                createdAt: new Date().toISOString(),
            });
        } else {
            await dbUpdateFields(profile.sub, {
                name: profile.name,
                email: profile.email,
                picture: profile.picture,
            });
        }

        delete req.session.oauthState;

        console.log(`User signed in: ${profile.name} (${profile.email}) | paid=${!!req.session.user.paid}`);

        if (req.session.user.paid) {
            res.redirect('/app');
        } else {
            res.redirect('/upgrade');
        }

    } catch (err) {
        console.error('OAuth callback error:', err);
        res.redirect('/auth/error.html?message=' + encodeURIComponent('An unexpected error occurred'));
    }
});

// ---------- STRIPE CHECKOUT ----------

const stripe = process.env.STRIPE_SECRET_KEY
    ? new Stripe(process.env.STRIPE_SECRET_KEY)
    : null;

const PLANS = {
    pro_monthly:      { price: process.env.STRIPE_PRICE_PRO_MONTHLY,      name: 'SuperLinkedIn Pro',      trial: 3 },
    pro_yearly:       { price: process.env.STRIPE_PRICE_PRO_YEARLY,       name: 'SuperLinkedIn Pro',      trial: 3 },
    advanced_monthly: { price: process.env.STRIPE_PRICE_ADVANCED_MONTHLY, name: 'SuperLinkedIn Advanced', trial: 3 },
    advanced_yearly:  { price: process.env.STRIPE_PRICE_ADVANCED_YEARLY,  name: 'SuperLinkedIn Advanced', trial: 3 },
    ultra_monthly:    { price: process.env.STRIPE_PRICE_ULTRA_MONTHLY,    name: 'SuperLinkedIn Ultra',    trial: 3 },
    ultra_yearly:     { price: process.env.STRIPE_PRICE_ULTRA_YEARLY,     name: 'SuperLinkedIn Ultra',    trial: 3 },
};

app.post('/api/checkout', async (req, res) => {
    if (!stripe) {
        return res.status(500).json({ error: 'Stripe is not configured' });
    }

    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { plan } = req.body;
    const planConfig = PLANS[plan];

    if (!planConfig || !planConfig.price) {
        return res.status(400).json({ error: 'Invalid plan selected' });
    }

    try {
        const session = await stripe.checkout.sessions.create({
            mode: 'subscription',
            payment_method_types: ['card'],
            line_items: [{
                price: planConfig.price,
                quantity: 1,
            }],
            subscription_data: {
                trial_period_days: planConfig.trial,
            },
            customer_email: req.session.user.email,
            metadata: {
                linkedinId: req.session.user.linkedinId,
                plan: plan,
            },
            success_url: `${process.env.BASE_URL}/checkout/success?session_id={CHECKOUT_SESSION_ID}`,
            cancel_url: `${process.env.BASE_URL}/upgrade`,
        });

        res.json({ url: session.url });
    } catch (err) {
        console.error('Stripe checkout error:', err);
        res.status(500).json({ error: 'Failed to create checkout session' });
    }
});

app.post('/api/checkout/confirm', async (req, res) => {
    if (req.session.user) {
        req.session.user.paid = true;
        await dbUpdateFields(req.session.user.linkedinId, {
            paid: true,
            paidAt: new Date().toISOString(),
        });
    }
    res.json({ ok: true });
});

// ---------- ONBOARDING ----------

app.post('/api/onboarding/writing-dna', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { likedPosts } = req.body;
    req.session.user.writingDNA = likedPosts || [];

    const allTags = (likedPosts || []).flatMap(p => p.tags || []);
    const tagCounts = {};
    allTags.forEach(tag => { tagCounts[tag] = (tagCounts[tag] || 0) + 1; });
    req.session.user.writingProfile = tagCounts;

    await dbUpdateFields(req.session.user.linkedinId, {
        writingDNA: req.session.user.writingDNA,
        writingProfile: tagCounts,
    });

    console.log(`Writing DNA saved for ${req.session.user.name}:`, tagCounts);
    res.json({ success: true, profile: tagCounts });
});

app.post('/api/onboarding/creators', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { creators } = req.body;
    req.session.user.favoriteCreators = creators || [];

    await dbUpdateFields(req.session.user.linkedinId, {
        favoriteCreators: creators || [],
    });

    console.log(`Favorite creators saved for ${req.session.user.name}:`, creators);
    res.json({ success: true });
});

app.post('/api/onboarding/resolve-linkedin', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { url } = req.body;
    if (!url) return res.status(400).json({ error: 'URL is required' });

    let profileUrl = url.trim();
    if (!profileUrl.startsWith('http')) profileUrl = 'https://' + profileUrl;

    try {
        const response = await fetch(profileUrl, {
            headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SuperLinkedIn/1.0)' },
            redirect: 'follow',
            signal: AbortSignal.timeout(8000),
        });
        const html = await response.text();

        const ogTitle = html.match(/<meta[^>]*property=["']og:title["'][^>]*content=["']([^"']+)["']/i)
            || html.match(/<meta[^>]*content=["']([^"']+)["'][^>]*property=["']og:title["']/i);
        const titleTag = html.match(/<title[^>]*>([^<]+)<\/title>/i);

        let name = '';
        if (ogTitle) {
            name = ogTitle[1].replace(/\s*[-|].*$/, '').replace(/\s*\(.*\)/, '').trim();
        } else if (titleTag) {
            name = titleTag[1].replace(/\s*[-|].*$/, '').replace(/\s*\(.*\)/, '').trim();
        }

        res.json({ name });
    } catch (err) {
        console.error('LinkedIn resolve failed:', err.message);
        res.json({ name: '' });
    }
});

app.post('/api/onboarding/products', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { products } = req.body;
    req.session.user.products = products || [];

    await dbUpdateFields(req.session.user.linkedinId, {
        products: products || [],
    });

    console.log(`Products saved for ${req.session.user.name}:`, products.map(p => p.url));
    res.json({ success: true });
});

app.post('/api/onboarding/analyze-url', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { url } = req.body;
    if (!url) return res.status(400).json({ error: 'URL is required' });

    try {
        const response = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-US,en;q=0.9',
            },
            redirect: 'follow',
            signal: AbortSignal.timeout(10000),
        });
        const html = await response.text();

        function extractMeta(html, nameOrProp) {
            const patterns = [
                new RegExp(`<meta[^>]*(?:name|property)\\s*=\\s*["']${nameOrProp}["'][^>]*content\\s*=\\s*["']([^"']+)["']`, 'i'),
                new RegExp(`<meta[^>]*content\\s*=\\s*["']([^"']+)["'][^>]*(?:name|property)\\s*=\\s*["']${nameOrProp}["']`, 'i'),
            ];
            for (const p of patterns) {
                const m = html.match(p);
                if (m) return m[1].trim();
            }
            return '';
        }

        const ogTitle = extractMeta(html, 'og:title');
        const titleMatch = html.match(/<title[^>]*>([^<]+)<\/title>/i);
        const name = ogTitle || (titleMatch ? titleMatch[1].trim() : '');

        const ogDesc = extractMeta(html, 'og:description');
        const metaDesc = extractMeta(html, 'description');
        const twitterDesc = extractMeta(html, 'twitter:description');
        const description = ogDesc || metaDesc || twitterDesc || '';

        res.json({ name, description });
    } catch (err) {
        console.error('URL analysis failed:', err.message);
        res.json({ name: '', description: '' });
    }
});

app.post('/api/onboarding/profile', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { aboutYou } = req.body;
    req.session.user.aboutYou = aboutYou || '';

    await dbUpdateFields(req.session.user.linkedinId, {
        aboutYou: aboutYou || '',
    });

    console.log(`Profile saved for ${req.session.user.name}: "${aboutYou}"`);
    res.json({ success: true });
});

app.get('/api/onboarding/writing-dna', (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    res.json({
        writingDNA: req.session.user.writingDNA || [],
        writingProfile: req.session.user.writingProfile || {},
    });
});

// ---------- AI & QUEUE ----------

app.post('/api/ai/generate-posts', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ posts: [] });
    }

    const user = req.session.user;
    const writingProfile = user.writingProfile || {};
    const topTags = Object.entries(writingProfile).sort((a, b) => b[1] - a[1]).map(e => e[0]).slice(0, 5);
    const aboutYou = user.aboutYou || '';
    const creators = (user.favoriteCreators || []).map(c => c.name).join(', ');

    const systemPrompt = `You are a LinkedIn content strategist. Generate 3 unique LinkedIn posts for a user. Each post should be engaging, authentic, and ready to publish. Return ONLY a JSON array of 3 strings, each being a complete post.`;

    const userPrompt = `Generate 3 LinkedIn posts for me.
About me: ${aboutYou || 'A professional looking to grow on LinkedIn.'}
My preferred writing styles: ${topTags.join(', ') || 'motivational, professional, storytelling'}
Creators I admire: ${creators || 'thought leaders in my industry'}
Make each post different in format (one list-based, one story, one insight/opinion). Keep posts between 100-300 words. Do NOT include hashtags.`;

    try {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${apiKey}`,
            },
            body: JSON.stringify({
                model: 'gpt-4o-mini',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt },
                ],
                temperature: 0.8,
                max_tokens: 2000,
            }),
        });

        const data = await response.json();
        const content = data.choices?.[0]?.message?.content || '';

        let posts = [];
        try {
            const jsonMatch = content.match(/\[[\s\S]*\]/);
            if (jsonMatch) posts = JSON.parse(jsonMatch[0]);
        } catch {
            posts = content.split('\n\n').filter(p => p.trim().length > 50).slice(0, 3);
        }

        res.json({ posts });
    } catch (err) {
        console.error('AI generation error:', err);
        res.json({ posts: [] });
    }
});

app.post('/api/ai/write', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ error: 'OpenAI API key not configured' });
    }

    const { tone } = req.body;
    const user = req.session.user;
    const writingProfile = user.writingProfile || {};
    const topTags = Object.entries(writingProfile).sort((a, b) => b[1] - a[1]).map(e => e[0]).slice(0, 5);
    const aboutYou = user.aboutYou || '';

    const toneDesc = {
        auto: topTags.join(', ') || 'professional and authentic',
        professional: 'professional, polished, industry-focused',
        casual: 'casual, friendly, conversational',
        motivational: 'motivational, inspiring, uplifting',
        storytelling: 'narrative storytelling, personal experience',
        educational: 'educational, informative, teaching',
        contrarian: 'contrarian, bold opinions, hot takes',
        humorous: 'humorous, witty, lighthearted',
    };

    const userPrompt = `Write a single LinkedIn post for me.
About me: ${aboutYou || 'A professional looking to grow on LinkedIn.'}
Tone: ${toneDesc[tone] || toneDesc.auto}
Requirements: 100-300 words, engaging opening line, no hashtags, ready to publish. Return ONLY the post text.`;

    try {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${apiKey}`,
            },
            body: JSON.stringify({
                model: 'gpt-4o-mini',
                messages: [
                    { role: 'system', content: 'You are a LinkedIn content writer. Write a single LinkedIn post. Return ONLY the post text, nothing else.' },
                    { role: 'user', content: userPrompt },
                ],
                temperature: 0.8,
                max_tokens: 1000,
            }),
        });

        const data = await response.json();
        const post = data.choices?.[0]?.message?.content?.trim() || '';
        res.json({ post });
    } catch (err) {
        console.error('AI write error:', err);
        res.json({ error: 'Failed to generate post' });
    }
});

app.get('/api/queue', (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    res.json({ queue: req.session.user.queue || [] });
});

app.post('/api/queue', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    if (!req.session.user.queue) req.session.user.queue = [];
    const { action, text, status, index } = req.body;

    if (action === 'add' && text) {
        req.session.user.queue.push({
            text,
            status: status || 'draft',
            date: new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }),
        });
    } else if (action === 'remove' && typeof index === 'number') {
        req.session.user.queue.splice(index, 1);
    }

    await dbUpdateFields(req.session.user.linkedinId, {
        queue: req.session.user.queue,
    });

    res.json({ queue: req.session.user.queue });
});

// ---------- API ROUTES ----------

// Get current user info
app.get('/api/me', (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    const { accessToken, ...safeUser } = req.session.user;
    res.json(safeUser);
});

// Logout
app.get('/auth/logout', (req, res) => {
    req.session.destroy(() => {
        res.redirect('/');
    });
});

// Check auth status
app.get('/api/auth/status', (req, res) => {
    res.json({ authenticated: !!req.session.user });
});

// ---------- START SERVER ----------

app.listen(PORT, () => {
    console.log(`\n  SuperLinkedIn server running at http://localhost:${PORT}\n`);

    if (LINKEDIN.clientId === 'your_client_id_here') {
        console.log('  ⚠  WARNING: LinkedIn Client ID not configured!');
        console.log('  ⚠  Edit .env and add your LinkedIn app credentials.');
        console.log('  ⚠  Get them from https://www.linkedin.com/developers/apps\n');
    }
});
