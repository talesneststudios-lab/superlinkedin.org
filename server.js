require('dotenv').config();
const express = require('express');
const session = require('express-session');
const crypto = require('crypto');
const path = require('path');
const Stripe = require('stripe');

const app = express();
const PORT = process.env.PORT || 3000;

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

        // Step 5: Store user info in session
        req.session.user = {
            linkedinId: profile.sub,
            name: profile.name,
            email: profile.email,
            picture: profile.picture,
            accessToken: accessToken,
        };

        delete req.session.oauthState;

        console.log(`User signed in: ${profile.name} (${profile.email})`);

        res.redirect('/upgrade');

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

// ---------- ONBOARDING ----------

app.post('/api/onboarding/writing-dna', (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { likedPosts } = req.body;
    req.session.user.writingDNA = likedPosts || [];

    const allTags = (likedPosts || []).flatMap(p => p.tags || []);
    const tagCounts = {};
    allTags.forEach(tag => { tagCounts[tag] = (tagCounts[tag] || 0) + 1; });
    req.session.user.writingProfile = tagCounts;

    console.log(`Writing DNA saved for ${req.session.user.name}:`, tagCounts);
    res.json({ success: true, profile: tagCounts });
});

app.post('/api/onboarding/creators', (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { creators } = req.body;
    req.session.user.favoriteCreators = creators || [];

    console.log(`Favorite creators saved for ${req.session.user.name}:`, creators);
    res.json({ success: true });
});

app.post('/api/onboarding/products', (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { products } = req.body;
    req.session.user.products = products || [];

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
            headers: { 'User-Agent': 'SuperLinkedIn Bot/1.0' },
            signal: AbortSignal.timeout(8000),
        });
        const html = await response.text();

        const titleMatch = html.match(/<title[^>]*>([^<]+)<\/title>/i);
        const descMatch = html.match(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']+)["']/i)
            || html.match(/<meta[^>]*content=["']([^"']+)["'][^>]*name=["']description["']/i)
            || html.match(/<meta[^>]*property=["']og:description["'][^>]*content=["']([^"']+)["']/i);

        res.json({
            name: titleMatch ? titleMatch[1].trim() : '',
            description: descMatch ? descMatch[1].trim() : '',
        });
    } catch (err) {
        console.error('URL analysis failed:', err.message);
        res.json({ name: '', description: '' });
    }
});

app.post('/api/onboarding/profile', (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { aboutYou } = req.body;
    req.session.user.aboutYou = aboutYou || '';

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
