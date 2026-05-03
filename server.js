require('dotenv').config();
const express = require('express');
const session = require('express-session');
const crypto = require('crypto');
const path = require('path');
const Stripe = require('stripe');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, ScanCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');

const PDFDocument = require('pdfkit');

process.on('uncaughtException', (err) => {
    console.error('[FATAL] Uncaught exception:', err.message, err.stack);
});
process.on('unhandledRejection', (reason) => {
    console.error('[FATAL] Unhandled rejection:', reason);
});

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

async function dbFindByEmail(email) {
    try {
        const result = await ddb.send(new ScanCommand({
            TableName: DYNAMO_TABLE,
            FilterExpression: 'email = :email',
            ExpressionAttributeValues: { ':email': email },
            Limit: 1,
        }));
        return (result.Items && result.Items[0]) || null;
    } catch (err) {
        console.error('DynamoDB findByEmail error:', err.message);
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

const CANONICAL_HOST = (() => {
    try { return new URL(process.env.BASE_URL).host; } catch { return ''; }
})();

if (CANONICAL_HOST) {
    app.use((req, res, next) => {
        if (req.hostname !== CANONICAL_HOST.split(':')[0]) {
            return res.redirect(301, process.env.BASE_URL + req.originalUrl);
        }
        next();
    });
}

const LINKEDIN = {
    clientId: process.env.LINKEDIN_CLIENT_ID,
    clientSecret: process.env.LINKEDIN_CLIENT_SECRET,
    redirectUri: `${process.env.BASE_URL}/auth/linkedin/callback`,
    authUrl: 'https://www.linkedin.com/oauth/v2/authorization',
    tokenUrl: 'https://www.linkedin.com/oauth/v2/accessToken',
    userInfoUrl: 'https://api.linkedin.com/v2/userinfo',
    scope: 'openid profile email w_member_social',
};

let sessionStore;
try {
    const DynamoDBStore = require('connect-dynamodb')({ session });
    sessionStore = new DynamoDBStore({
        table: process.env.DYNAMODB_SESSIONS_TABLE || 'superlinkedin-sessions',
        AWSConfigJSON: { region: process.env.AWS_REGION || 'us-east-1' },
        readCapacityUnits: 5,
        writeCapacityUnits: 5,
    });
    sessionStore.on('error', (err) => {
        console.error('[SessionStore] DynamoDB session store error:', err.message);
    });
    console.log('[SessionStore] Using DynamoDB session store');
} catch (err) {
    console.error('[SessionStore] Failed to create DynamoDB store, falling back to in-memory:', err.message);
    sessionStore = undefined;
}

const sessionConfig = {
    secret: process.env.SESSION_SECRET || 'fallback-secret',
    resave: false,
    saveUninitialized: false,
    cookie: {
        secure: process.env.NODE_ENV === 'production',
        httpOnly: true,
        sameSite: 'lax',
        maxAge: 7 * 24 * 60 * 60 * 1000,
    },
};
if (sessionStore) sessionConfig.store = sessionStore;

app.use(session(sessionConfig));

const cors = require('cors');
app.use(cors({ origin: true, credentials: true }));

// Stripe webhook needs raw body — must be before express.json()
app.post('/api/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
    const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;
    if (!stripe || !webhookSecret) {
        console.error('[Webhook] Stripe or webhook secret not configured');
        return res.status(400).send('Webhook not configured');
    }

    let event;
    try {
        const sig = req.headers['stripe-signature'];
        event = stripe.webhooks.constructEvent(req.body, sig, webhookSecret);
    } catch (err) {
        console.error('[Webhook] Signature verification failed:', err.message);
        return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    console.log(`[Webhook] Received event: ${event.type}`);

    try {
        if (event.type === 'invoice.payment_failed') {
            const invoice = event.data.object;
            const customerEmail = invoice.customer_email;
            const subscriptionId = invoice.subscription;

            console.log(`[Webhook] Payment failed for ${customerEmail}, subscription ${subscriptionId}`);

            // Find user by email and revoke access
            const user = await dbFindByEmail(customerEmail);
            if (user) {
                await dbUpdateFields(user.linkedinId, {
                    paid: false,
                    paymentFailed: true,
                    paymentFailedAt: new Date().toISOString(),
                    paymentFailedReason: invoice.status_transitions?.finalized_at ? 'payment_failed' : 'unknown',
                });
                console.log(`[Webhook] Access revoked for user ${user.linkedinId} (${customerEmail})`);
            }
        }

        if (event.type === 'customer.subscription.deleted') {
            const subscription = event.data.object;
            const customerEmail = subscription.metadata?.email || '';
            const linkedinId = subscription.metadata?.linkedinId || '';

            console.log(`[Webhook] Subscription deleted for ${linkedinId || customerEmail}`);

            let user = linkedinId ? await dbGetUser(linkedinId) : null;
            if (!user && customerEmail) user = await dbFindByEmail(customerEmail);

            if (user) {
                await dbUpdateFields(user.linkedinId, {
                    paid: false,
                    subscriptionCancelled: true,
                    subscriptionEndedAt: new Date().toISOString(),
                });
                console.log(`[Webhook] Subscription ended for user ${user.linkedinId}`);
            }
        }

        if (event.type === 'invoice.paid') {
            const invoice = event.data.object;
            const customerEmail = invoice.customer_email;

            console.log(`[Webhook] Payment succeeded for ${customerEmail}`);

            const user = await dbFindByEmail(customerEmail);
            if (user) {
                await dbUpdateFields(user.linkedinId, {
                    paid: true,
                    paymentFailed: false,
                    paymentFailedAt: null,
                    lastPaymentAt: new Date().toISOString(),
                });
                console.log(`[Webhook] Access restored for user ${user.linkedinId}`);
            }
        }

        if (event.type === 'customer.subscription.updated') {
            const subscription = event.data.object;
            const customerEmail = subscription.metadata?.email || '';
            const linkedinId = subscription.metadata?.linkedinId || '';

            if (subscription.status === 'past_due' || subscription.status === 'unpaid') {
                console.log(`[Webhook] Subscription ${subscription.status} for ${linkedinId || customerEmail}`);
                let user = linkedinId ? await dbGetUser(linkedinId) : null;
                if (!user && customerEmail) user = await dbFindByEmail(customerEmail);

                if (user) {
                    await dbUpdateFields(user.linkedinId, {
                        paid: false,
                        paymentFailed: true,
                        subscriptionStatus: subscription.status,
                    });
                    console.log(`[Webhook] Access revoked (${subscription.status}) for ${user.linkedinId}`);
                }
            }
        }
    } catch (err) {
        console.error('[Webhook] Error processing event:', err.message);
    }

    res.json({ received: true });
});

app.use(express.json());

app.use((req, res, next) => {
    if (req.path.startsWith('/auth/')) {
        console.log(`[Request] ${req.method} ${req.path} — session ready: ${!!req.session}`);
    }
    next();
});

app.use(express.static(path.join(__dirname), {
    index: 'index.html',
    extensions: ['html'],
}));

// Serve dashboard
app.get('/app', (req, res) => {
    res.sendFile(path.join(__dirname, 'app.html'));
});

// Serve playbook (logged-in users)
app.get('/playbook', (req, res) => {
    if (!req.session.user) {
        return res.redirect('/login');
    }
    res.sendFile(path.join(__dirname, 'playbook.html'));
});

// ---------- MULTI-PROFILE HELPERS ----------

function getActiveProfileId(session) {
    if (session.activeProfileId) return session.activeProfileId;
    if (session.user) return session.user.linkedinId;
    return null;
}

function getPrimaryAccountId(session) {
    return session.primaryAccountId || (session.user && session.user.linkedinId) || null;
}

async function getPrimaryUser(session) {
    const primaryId = getPrimaryAccountId(session);
    if (!primaryId) return null;
    if (primaryId === session.user.linkedinId) return session.user;
    return await dbGetUser(primaryId);
}

// ---------- AUTH ROUTES ----------

// Step 1: Redirect user to LinkedIn authorization page
app.get('/auth/linkedin', (req, res) => {
    console.log('[Auth] LinkedIn login initiated, redirectUri:', LINKEDIN.redirectUri);
    const state = crypto.randomBytes(32).toString('hex');
    req.session.oauthState = state;

    const params = new URLSearchParams({
        response_type: 'code',
        client_id: LINKEDIN.clientId,
        redirect_uri: LINKEDIN.redirectUri,
        scope: LINKEDIN.scope,
        state: state,
    });

    const redirectUrl = `${LINKEDIN.authUrl}?${params.toString()}`;
    req.session.save((err) => {
        if (err) console.error('[Auth] Session save error:', err.message);
        console.log('[Auth] Redirecting to LinkedIn OAuth');
        res.redirect(redirectUrl);
    });
});

// Add-profile route: sets flag then triggers same OAuth flow
app.get('/auth/linkedin/add-profile', (req, res) => {
    if (!req.session.user) return res.redirect('/');

    req.session.addingProfile = true;
    req.session.addingProfilePrimaryId = req.session.primaryAccountId || req.session.user.linkedinId;

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

        // ── ADD-PROFILE MODE ──
        if (req.session.addingProfile && req.session.addingProfilePrimaryId) {
            const primaryId = req.session.addingProfilePrimaryId;
            delete req.session.addingProfile;
            delete req.session.addingProfilePrimaryId;
            delete req.session.oauthState;

            const primaryUser = await dbGetUser(primaryId);
            if (!primaryUser || !primaryUser.paid) {
                return res.redirect('/auth/error.html?message=' + encodeURIComponent('Primary account not found or not paid'));
            }

            const tier = primaryUser.planTier || 'pro';
            const limits = PLAN_LIMITS[tier] || PLAN_LIMITS.pro;
            const linkedProfiles = primaryUser.linkedProfiles || [{ linkedinId: primaryId, name: primaryUser.name, email: primaryUser.email, picture: primaryUser.picture, addedAt: primaryUser.createdAt || new Date().toISOString() }];

            if (linkedProfiles.length >= limits.maxProfiles) {
                return res.redirect('/auth/error.html?message=' + encodeURIComponent(`Profile limit reached (${limits.maxProfiles} for ${tier} plan). Upgrade to add more.`));
            }

            if (linkedProfiles.some(p => p.linkedinId === profile.sub)) {
                req.session.activeProfileId = profile.sub;
                req.session.primaryAccountId = primaryId;
                const existingProfile = await dbGetUser(profile.sub);
                if (existingProfile) {
                    req.session.user = { ...existingProfile, linkedinId: profile.sub, accessToken };
                }
                console.log(`Profile ${profile.name} already linked, switching to it`);
                return res.redirect('/app');
            }

            linkedProfiles.push({
                linkedinId: profile.sub,
                name: profile.name,
                email: profile.email,
                picture: profile.picture,
                addedAt: new Date().toISOString(),
            });

            await dbUpdateFields(primaryId, { linkedProfiles });

            const existingDbProfile = await dbGetUser(profile.sub);
            if (!existingDbProfile) {
                await dbSaveUser({
                    linkedinId: profile.sub,
                    name: profile.name,
                    email: profile.email,
                    picture: profile.picture,
                    parentAccountId: primaryId,
                    paid: false,
                    onboardingComplete: false,
                    createdAt: new Date().toISOString(),
                });
            } else {
                await dbUpdateFields(profile.sub, {
                    name: profile.name,
                    email: profile.email,
                    picture: profile.picture,
                    parentAccountId: primaryId,
                });
            }

            req.session.primaryAccountId = primaryId;
            req.session.activeProfileId = profile.sub;
            const newProfile = await dbGetUser(profile.sub) || {};
            req.session.user = { ...newProfile, linkedinId: profile.sub, accessToken };

            console.log(`Profile added: ${profile.name} (${profile.email}) under primary ${primaryId}`);

            if (newProfile.onboardingComplete) {
                return res.redirect('/app');
            }
            return res.redirect('/onboarding');
        }

        // ── NORMAL LOGIN MODE ──
        const dbUser = await dbGetUser(profile.sub) || {};

        // Check if this profile is a linked child — route to its parent
        const parentId = dbUser.parentAccountId || null;

        req.session.user = {
            ...dbUser,
            linkedinId: profile.sub,
            name: profile.name,
            email: profile.email,
            picture: profile.picture,
            accessToken: accessToken,
        };

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

        // Initialize linkedProfiles for the primary account if not set
        if (!parentId && req.session.user.paid && !dbUser.linkedProfiles) {
            const initialProfiles = [{ linkedinId: profile.sub, name: profile.name, email: profile.email, picture: profile.picture, addedAt: dbUser.createdAt || new Date().toISOString() }];
            await dbUpdateFields(profile.sub, { linkedProfiles: initialProfiles });
            req.session.user.linkedProfiles = initialProfiles;
        }

        req.session.primaryAccountId = parentId || profile.sub;
        req.session.activeProfileId = profile.sub;

        delete req.session.oauthState;

        const isPaid = req.session.user.paid || (parentId && (await dbGetUser(parentId))?.paid);
        console.log(`User signed in: ${profile.name} (${profile.email}) | paid=${!!isPaid} | onboarded=${!!req.session.user.onboardingComplete}`);

        if (req.session.user.onboardingComplete) {
            res.redirect('/app');
        } else if (isPaid && req.session.user.onboardingComplete === undefined) {
            res.redirect('/app');
        } else {
            res.redirect('/onboarding');
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

const TRIAL_DAYS = 3;
const TRIAL_CREDITS = 200;

const PLAN_LIMITS = {
    free:     { aiCredits: TRIAL_CREDITS, creditPeriod: 'trial', maxProfiles: 1, postsPerMonth: null, features: ['schedule', 'basic_analytics'] },
    pro:      { aiCredits: 500, creditPeriod: 'month', maxProfiles: 3,  postsPerMonth: null, features: ['schedule', 'auto_post', 'basic_analytics', 'chrome_extension'] },
    advanced: { aiCredits: 2000, creditPeriod: 'day',  maxProfiles: 5,  postsPerMonth: null, features: ['schedule', 'auto_post', 'advanced_analytics', 'chrome_extension', 'carousel', 'engage_engine', 'auto_repost', 'viral_library'] },
    ultra:    { aiCredits: 5000, creditPeriod: 'day',  maxProfiles: 15, postsPerMonth: 5000, features: ['schedule', 'auto_post', 'advanced_analytics', 'chrome_extension', 'carousel', 'engage_engine', 'auto_repost', 'viral_library', 'team', 'white_label', 'api_access'] },
};

function getPlanTier(planKey) {
    if (!planKey) return null;
    if (planKey.startsWith('ultra')) return 'ultra';
    if (planKey.startsWith('advanced')) return 'advanced';
    if (planKey.startsWith('pro')) return 'pro';
    return null;
}

const PLANS = {
    pro_monthly:      { price: process.env.STRIPE_PRICE_PRO_MONTHLY,      name: 'SuperLinkedIn Pro',      trial: 3, tier: 'pro' },
    pro_yearly:       { price: process.env.STRIPE_PRICE_PRO_YEARLY,       name: 'SuperLinkedIn Pro',      trial: 3, tier: 'pro' },
    advanced_monthly: { price: process.env.STRIPE_PRICE_ADVANCED_MONTHLY, name: 'SuperLinkedIn Advanced', trial: 3, tier: 'advanced' },
    advanced_yearly:  { price: process.env.STRIPE_PRICE_ADVANCED_YEARLY,  name: 'SuperLinkedIn Advanced', trial: 3, tier: 'advanced' },
    ultra_monthly:    { price: process.env.STRIPE_PRICE_ULTRA_MONTHLY,    name: 'SuperLinkedIn Ultra',    trial: 3, tier: 'ultra' },
    ultra_yearly:     { price: process.env.STRIPE_PRICE_ULTRA_YEARLY,     name: 'SuperLinkedIn Ultra',    trial: 3, tier: 'ultra' },
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
                metadata: {
                    linkedinId: req.session.user.linkedinId,
                    email: req.session.user.email,
                    plan: plan,
                },
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

app.get('/api/checkout/session', async (req, res) => {
    if (!stripe) return res.json({});
    try {
        const session = await stripe.checkout.sessions.retrieve(req.query.session_id);
        res.json({ plan: session.metadata?.plan || 'pro_monthly' });
    } catch {
        res.json({ plan: 'pro_monthly' });
    }
});

app.post('/api/checkout/confirm', async (req, res) => {
    if (req.session.user) {
        const { plan } = req.body;
        const tier = getPlanTier(plan) || 'pro';
        req.session.user.paid = true;
        req.session.user.plan = plan || 'pro_monthly';
        req.session.user.planTier = tier;
        req.session.user.aiCreditsUsed = 0;
        req.session.user.aiCreditsResetAt = new Date().toISOString();

        const linkedinId = req.session.user.linkedinId;
        const initialProfiles = [{ linkedinId, name: req.session.user.name, email: req.session.user.email, picture: req.session.user.picture, addedAt: new Date().toISOString() }];

        await dbUpdateFields(linkedinId, {
            paid: true,
            paidAt: new Date().toISOString(),
            plan: req.session.user.plan,
            planTier: tier,
            aiCreditsUsed: 0,
            aiCreditsResetAt: new Date().toISOString(),
            linkedProfiles: initialProfiles,
        });

        req.session.user.linkedProfiles = initialProfiles;
        req.session.primaryAccountId = linkedinId;
        req.session.activeProfileId = linkedinId;
    }
    res.json({ ok: true });
});

// ---------- SUBSCRIPTION MANAGEMENT ----------

app.post('/api/subscription/cancel', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    const primaryId = getPrimaryAccountId(req.session);

    await dbUpdateFields(primaryId, {
        subscriptionCancelled: true,
        cancelledAt: new Date().toISOString(),
    });

    console.log(`Subscription cancelled for primary account ${primaryId}`);
    res.json({ ok: true });
});

app.post('/api/account/delete', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    const primaryId = getPrimaryAccountId(req.session);
    const primaryUser = await dbGetUser(primaryId);
    if (!primaryUser) return res.status(404).json({ error: 'Account not found' });

    try {
        // Cancel Stripe subscription if active
        if (stripe && primaryUser.email) {
            try {
                const customers = await stripe.customers.list({ email: primaryUser.email, limit: 1 });
                if (customers.data.length > 0) {
                    const customer = customers.data[0];
                    const subscriptions = await stripe.subscriptions.list({ customer: customer.id, status: 'active', limit: 10 });
                    for (const sub of subscriptions.data) {
                        await stripe.subscriptions.cancel(sub.id);
                        console.log(`[DeleteAccount] Cancelled Stripe subscription ${sub.id} for ${primaryId}`);
                    }
                    const trialSubs = await stripe.subscriptions.list({ customer: customer.id, status: 'trialing', limit: 10 });
                    for (const sub of trialSubs.data) {
                        await stripe.subscriptions.cancel(sub.id);
                        console.log(`[DeleteAccount] Cancelled trial subscription ${sub.id} for ${primaryId}`);
                    }
                }
            } catch (stripeErr) {
                console.error('[DeleteAccount] Stripe cancellation error:', stripeErr.message);
            }
        }

        // Delete linked profile records
        const linkedProfiles = primaryUser.linkedProfiles || [];
        for (const profile of linkedProfiles) {
            if (profile.linkedinId && profile.linkedinId !== primaryId) {
                try {
                    await ddb.send(new DeleteCommand({ TableName: DYNAMO_TABLE, Key: { linkedinId: profile.linkedinId } }));
                    console.log(`[DeleteAccount] Deleted linked profile ${profile.linkedinId}`);
                } catch (e) {
                    console.error(`[DeleteAccount] Failed to delete profile ${profile.linkedinId}:`, e.message);
                }
            }
        }

        // Delete the primary account
        await ddb.send(new DeleteCommand({ TableName: DYNAMO_TABLE, Key: { linkedinId: primaryId } }));
        console.log(`[DeleteAccount] Deleted primary account ${primaryId} (${primaryUser.email})`);

        // Destroy session
        req.session.destroy(() => {
            res.json({ ok: true });
        });
    } catch (err) {
        console.error('[DeleteAccount] Error:', err.message);
        res.status(500).json({ error: 'Failed to delete account. Please contact support.' });
    }
});

// ---------- MULTI-PROFILE MANAGEMENT ----------

app.get('/api/profiles', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    const primaryId = getPrimaryAccountId(req.session);
    const primaryUser = await dbGetUser(primaryId);
    if (!primaryUser) return res.json({ profiles: [], activeProfileId: null, maxProfiles: 3 });

    let linkedProfiles = primaryUser.linkedProfiles || [];
    if (!linkedProfiles.length) {
        linkedProfiles = [{ linkedinId: primaryId, name: primaryUser.name, email: primaryUser.email, picture: primaryUser.picture, addedAt: primaryUser.createdAt || new Date().toISOString() }];
    }

    const tier = primaryUser.planTier || 'pro';
    const limits = PLAN_LIMITS[tier] || PLAN_LIMITS.pro;

    const enriched = [];
    for (const p of linkedProfiles) {
        const pUser = await dbGetUser(p.linkedinId);
        enriched.push({
            linkedinId: p.linkedinId,
            name: pUser?.name || p.name,
            email: pUser?.email || p.email,
            picture: pUser?.picture || p.picture,
            addedAt: p.addedAt,
            isPrimary: p.linkedinId === primaryId,
            onboardingComplete: !!pUser?.onboardingComplete,
        });
    }

    res.json({
        profiles: enriched,
        activeProfileId: getActiveProfileId(req.session),
        primaryAccountId: primaryId,
        maxProfiles: limits.maxProfiles,
        planTier: tier,
    });
});

app.post('/api/profiles/switch', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    const { linkedinId } = req.body;
    if (!linkedinId) return res.status(400).json({ error: 'linkedinId is required' });

    const primaryId = getPrimaryAccountId(req.session);
    const primaryUser = await dbGetUser(primaryId);
    if (!primaryUser) return res.status(404).json({ error: 'Primary account not found' });

    const linkedProfiles = primaryUser.linkedProfiles || [];
    const found = linkedProfiles.find(p => p.linkedinId === linkedinId) || linkedinId === primaryId;
    if (!found) return res.status(403).json({ error: 'Profile not linked to your account' });

    const profileUser = await dbGetUser(linkedinId);
    if (!profileUser) return res.status(404).json({ error: 'Profile not found' });

    req.session.activeProfileId = linkedinId;
    req.session.user = {
        ...profileUser,
        linkedinId,
        accessToken: req.session.user.accessToken,
    };

    const { accessToken, ...safeUser } = req.session.user;
    console.log(`Switched active profile to ${profileUser.name} (${linkedinId})`);
    res.json({ ok: true, user: safeUser });
});

app.post('/api/profiles/remove', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    const { linkedinId } = req.body;
    if (!linkedinId) return res.status(400).json({ error: 'linkedinId is required' });

    const primaryId = getPrimaryAccountId(req.session);
    if (linkedinId === primaryId) {
        return res.status(400).json({ error: 'Cannot remove the primary account' });
    }

    const primaryUser = await dbGetUser(primaryId);
    if (!primaryUser) return res.status(404).json({ error: 'Primary account not found' });

    let linkedProfiles = primaryUser.linkedProfiles || [];
    const idx = linkedProfiles.findIndex(p => p.linkedinId === linkedinId);
    if (idx === -1) return res.status(404).json({ error: 'Profile not linked to your account' });

    linkedProfiles.splice(idx, 1);
    await dbUpdateFields(primaryId, { linkedProfiles });

    await dbUpdateFields(linkedinId, { parentAccountId: null });

    if (req.session.activeProfileId === linkedinId) {
        req.session.activeProfileId = primaryId;
        const pUser = await dbGetUser(primaryId);
        if (pUser) {
            req.session.user = { ...pUser, linkedinId: primaryId, accessToken: req.session.user.accessToken };
        }
    }

    console.log(`Removed profile ${linkedinId} from primary ${primaryId}`);
    res.json({ ok: true });
});

// ---------- CREDITS ----------

function isUserOnTrial(user) {
    if (user.paid) return false;
    const createdAt = user.createdAt ? new Date(user.createdAt) : new Date();
    const now = new Date();
    const daysSinceCreation = (now - createdAt) / (1000 * 60 * 60 * 24);
    return daysSinceCreation <= TRIAL_DAYS;
}

function isTrialExpired(user) {
    if (user.paid) return false;
    const createdAt = user.createdAt ? new Date(user.createdAt) : new Date();
    const now = new Date();
    const daysSinceCreation = (now - createdAt) / (1000 * 60 * 60 * 24);
    return daysSinceCreation > TRIAL_DAYS;
}

function getUserCreditsInfo(user) {
    const onTrial = isUserOnTrial(user);
    const trialExpired = isTrialExpired(user);

    if (!user.paid && (onTrial || trialExpired)) {
        const limits = PLAN_LIMITS.free;
        const used = user.aiCreditsUsed || 0;
        const remaining = trialExpired ? 0 : Math.max(0, TRIAL_CREDITS - used);
        const createdAt = user.createdAt ? new Date(user.createdAt) : new Date();
        const trialEndsAt = new Date(createdAt.getTime() + TRIAL_DAYS * 24 * 60 * 60 * 1000);
        return { tier: 'free', limits, used, shouldReset: false, remaining, onTrial, trialExpired, trialEndsAt };
    }

    const tier = user.planTier || 'pro';
    const limits = PLAN_LIMITS[tier] || PLAN_LIMITS.pro;
    const now = new Date();
    const resetAt = user.aiCreditsResetAt ? new Date(user.aiCreditsResetAt) : new Date(0);

    let shouldReset = false;
    if (limits.creditPeriod === 'day') {
        shouldReset = now.toDateString() !== resetAt.toDateString();
    } else {
        shouldReset = now.getMonth() !== resetAt.getMonth() || now.getFullYear() !== resetAt.getFullYear();
    }

    const used = shouldReset ? 0 : (user.aiCreditsUsed || 0);
    return { tier, limits, used, shouldReset, remaining: Math.max(0, limits.aiCredits - used), onTrial: false, trialExpired: false };
}

async function consumeCredit(session, count) {
    const primaryId = getPrimaryAccountId(session);
    const primaryUser = primaryId ? (await dbGetUser(primaryId)) || session.user : session.user;
    const info = getUserCreditsInfo(primaryUser);
    const newUsed = info.shouldReset ? count : (primaryUser.aiCreditsUsed || 0) + count;
    primaryUser.aiCreditsUsed = newUsed;
    if (info.shouldReset) primaryUser.aiCreditsResetAt = new Date().toISOString();

    await dbUpdateFields(primaryId || primaryUser.linkedinId, {
        aiCreditsUsed: newUsed,
        aiCreditsResetAt: primaryUser.aiCreditsResetAt || new Date().toISOString(),
    });
}

async function getCreditsForSession(session) {
    const primaryId = getPrimaryAccountId(session);
    const primaryUser = primaryId ? (await dbGetUser(primaryId)) || session.user : session.user;
    return getUserCreditsInfo(primaryUser);
}

app.get('/api/credits', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    const primaryId = getPrimaryAccountId(req.session);
    const primaryUser = primaryId ? (await dbGetUser(primaryId)) || req.session.user : req.session.user;

    const info = getUserCreditsInfo(primaryUser);

    if (info.shouldReset) {
        primaryUser.aiCreditsUsed = 0;
        primaryUser.aiCreditsResetAt = new Date().toISOString();
        await dbUpdateFields(primaryId || primaryUser.linkedinId, { aiCreditsUsed: 0, aiCreditsResetAt: primaryUser.aiCreditsResetAt });
    }

    res.json({
        planTier: info.tier,
        planName: info.onTrial ? 'Free Trial' : (info.trialExpired ? 'Trial Expired' : (PLANS[(primaryUser.plan || 'pro_monthly')] || PLANS.pro_monthly).name),
        creditPeriod: info.limits.creditPeriod,
        aiCreditsTotal: info.limits.aiCredits,
        aiCreditsUsed: info.used,
        aiCreditsRemaining: info.remaining,
        maxProfiles: info.limits.maxProfiles,
        postsPerMonth: info.limits.postsPerMonth,
        onTrial: info.onTrial || false,
        trialExpired: info.trialExpired || false,
        trialEndsAt: info.trialEndsAt || null,
        features: info.limits.features,
    });
});

// ---------- ONBOARDING ----------

const POSTS_REF = [
    { body: "<p>3 things people hate hearing that are true:</p><p>- Your life is your responsibility<br>- No one is coming to save you<br>- If you want to change your future, you have to change yourself</p>" },
    { body: "<p>People with burnout will lie to you. Not because they intend to, they're lying to themselves too.</p><p>If someone tells you they're feeling \"a little bit burned out\", you should take it very seriously, because by the time they're ready to admit that to themselves, things are already bad.</p>" },
    { body: "<p>We need to redefine \"hard work\" to include \"hard thinking.\"</p><p>The person who outsmarts you is out working you.<br>The person who finds shortcuts is out working you.<br>The person with a better strategy is out working you.</p><p>Usually, the hardest work is thinking of a better approach.</p>" },
    { body: "<p>I used to think being in your 30's would be about settling down, turns out it's about starting over, healing, & becoming a new version of you.</p>" },
    { body: "<p>How I source content inspiration in 2026:</p><p>- LinkedIn: thought leadership<br>- Substack: long-form insights<br>- Feedly: industry trends</p><p>Literally all you need.</p>" },
    { body: "<p>I applaud people who try again. We never really talk about the strength required to get back up again.</p><p>People are just expected to bounce back. If you see someone struggling to try again, give them a hand. They really need it.</p>" },
    { body: "<p>Sometimes I meet a person & can't help but smile, happy to learn that I live in a world where they exist.</p><p>\"We sometimes encounter people, even perfect strangers, who begin to interest us at first sight, somehow suddenly, all at once, before a word has been spoken.\"</p>" },
    { body: "<p>Can anyone pinpoint the exact moment where everything became both:</p><p>a) worse<br>b) and more expensive</p><p>When did this happen?</p>" },
    { body: "<p>I was doing a late night debugging session and I couldn't figure something out</p><p>and then I googled it</p><p>and the first result with my answer</p><p>was a blog post</p><p>that I wrote</p>" },
    { body: "<p>Branding is how people see you.</p><p>Marketing is how they find you.</p><p>Sales is how you get paid.</p><p>Master all three.</p>" },
    { body: "<p>Starting a business can be painful.</p><p>You feel lost 97% of the time – the ups and downs are gut-wrenching.</p><p>I wish I had a cheat sheet of principles for my first startup.</p><p>So I wrote one.</p><p>Here are 40+ learnings about entrepreneurship that took me 10 years to figure out:</p>" },
];

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

app.post('/api/onboarding/complete', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    req.session.user.onboardingComplete = true;
    await dbUpdateFields(req.session.user.linkedinId, {
        onboardingComplete: true,
        onboardingCompletedAt: new Date().toISOString(),
    });
    console.log(`Onboarding completed for ${req.session.user.name}`);
    res.json({ success: true });
});

// ---------- CONTEXT SETTINGS ----------

app.post('/api/context/rules', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    const { customRules } = req.body;
    req.session.user.customRules = customRules || '';
    await dbUpdateFields(req.session.user.linkedinId, { customRules: customRules || '' });
    res.json({ success: true });
});

app.post('/api/context/interests', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    const { interests } = req.body;
    req.session.user.interests = interests || [];
    await dbUpdateFields(req.session.user.linkedinId, { interests: interests || [] });
    res.json({ success: true });
});

// ---------- LINKEDIN PUBLISH ----------

app.post('/api/publish', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { text, audience } = req.body;
    if (!text || !text.trim()) {
        return res.status(400).json({ error: 'Post text is required' });
    }

    const accessToken = req.session.user.accessToken;
    if (!accessToken) {
        return res.status(401).json({ error: 'No LinkedIn access token. Please sign in again.' });
    }

    const linkedinId = req.session.user.linkedinId;
    const personUrn = `urn:li:person:${linkedinId}`;
    const visibility = audience === 'connections' ? 'CONNECTIONS' : 'PUBLIC';

    try {
        const response = await fetch('https://api.linkedin.com/v2/ugcPosts', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${accessToken}`,
                'X-Restli-Protocol-Version': '2.0.0',
            },
            body: JSON.stringify({
                author: personUrn,
                lifecycleState: 'PUBLISHED',
                specificContent: {
                    'com.linkedin.ugc.ShareContent': {
                        shareCommentary: { text: text.trim() },
                        shareMediaCategory: 'NONE',
                    },
                },
                visibility: {
                    'com.linkedin.ugc.MemberNetworkVisibility': visibility,
                },
            }),
        });

        if (response.ok) {
            const data = await response.json();
            const postId = data.id;
            const postUrl = postId
                ? `https://www.linkedin.com/feed/update/${postId}/`
                : `https://www.linkedin.com/feed/`;

            console.log(`Post published to LinkedIn by ${req.session.user.name}: ${postId}`);
            res.json({ success: true, postId, postUrl });
        } else {
            const errData = await response.text();
            console.error('LinkedIn publish failed:', response.status, errData);

            if (response.status === 401) {
                return res.status(401).json({
                    error: 'LinkedIn access token expired. Please sign out and sign in again.',
                });
            }
            if (response.status === 403) {
                return res.status(403).json({
                    error: 'Missing LinkedIn posting permission. Please ensure "Share on LinkedIn" is enabled on your app.',
                });
            }

            res.status(response.status).json({
                error: 'Failed to publish to LinkedIn. Please try again.',
                details: errData,
            });
        }
    } catch (err) {
        console.error('LinkedIn publish error:', err);
        res.status(500).json({ error: 'Could not connect to LinkedIn. Please try again.' });
    }
});

// ---------- AI & QUEUE ----------

app.post('/api/ai/generate-posts', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 3) {
        return res.json({ posts: [], error: `AI credit limit reached (${credits.limits.aiCredits}/${credits.limits.creditPeriod}). Upgrade your plan for more credits.` });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ posts: [] });
    }

    const user = req.session.user;
    const writingProfile = user.writingProfile || {};
    const topTags = Object.entries(writingProfile).sort((a, b) => b[1] - a[1]).map(e => e[0]).slice(0, 5);
    const aboutYou = user.aboutYou || '';

    const creators = (user.favoriteCreators || []).map(c => c.name).filter(Boolean);
    const writingDNA = user.writingDNA || [];
    const likedPostBodies = writingDNA.map(p => {
        const post = POSTS_REF[p.index];
        return post ? post.body.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim() : null;
    }).filter(Boolean).slice(0, 5);

    const customRules = user.customRules || '';
    const interests = (user.interests || []).join(', ');
    const products = (user.products || []).filter(p => p.name || p.description);
    const productsList = products.map(p => {
        let s = p.name || p.url;
        if (p.description) s += ` — ${p.description}`;
        return s;
    }).join('; ');

    const gpCharMatch = customRules.match(/(\d+)\s*char/i);
    const gpCharLimit = gpCharMatch ? parseInt(gpCharMatch[1], 10) : 0;

    const likeOptions = ['1.3K', '2K', '2.3K', '3.3K', '1.9K', '5.6K', '4.1K', '2.8K', '6.2K', '8.1K', '1.7K'];
    const shuffled = [...POSTS_REF].sort(() => Math.random() - 0.5).slice(0, 3);
    const inspirationTexts = shuffled.map(ref =>
        ref.body.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim()
    );

    let systemPrompt;
    if (customRules) {
        systemPrompt = `You are a LinkedIn content strategist. Generate 3 unique LinkedIn posts. Return ONLY a JSON array of 3 strings.\n\n` +
            `MANDATORY RULES (highest priority — override everything else):\n${customRules}\n`;
        if (gpCharLimit) systemPrompt += `EACH POST MUST BE UNDER ${gpCharLimit} CHARACTERS TOTAL. Count every letter, space, and punctuation mark. Write very short, punchy posts. Do NOT exceed ${gpCharLimit} characters per post.\n`;
    } else {
        systemPrompt = `You are a LinkedIn content strategist. Generate 3 unique LinkedIn posts for a user. Each post should be engaging, authentic, and ready to publish. Return ONLY a JSON array of 3 strings, each being a complete post.`;
    }

    if (creators.length) systemPrompt += `\n\nWRITING STYLE: Study and mimic the writing style of these LinkedIn creators the user admires: ${creators.join(', ')}. Write as if the user were influenced by their voice, structure, and tone.`;
    if (likedPostBodies.length) systemPrompt += `\n\nWRITING DNA: The user liked these sample posts during onboarding. Use them as reference for the user's preferred writing style, tone, structure, and format:\n${likedPostBodies.map((b, i) => `${i + 1}. "${b}"`).join('\n')}`;
    if (products.length) systemPrompt += `\n\nPRODUCT PROMOTION: The user has these products/services. Naturally weave mentions or value propositions of these into the posts where relevant (not every post needs to mention them, but at least one should):\n${productsList}`;

    systemPrompt += `\n\nIMPORTANT: Each post MUST be directly inspired by its corresponding inspiration post below. Take the core idea, theme, or message from each inspiration and rewrite it in the user's own voice and style. The connection between the inspiration and the generated post must be obvious.`;

    const defaultLength = customRules ? '' : ' Keep posts between 100-300 words.';
    const userPrompt = `Generate 3 LinkedIn posts for me. Each post must be inspired by the corresponding reference post below — take its core message and rephrase it in my voice.${gpCharLimit ? `\nHARD LIMIT: Each post MUST be under ${gpCharLimit} characters. Keep them extremely short and concise.` : ''}

INSPIRATION POSTS (generate post 1 inspired by #1, post 2 inspired by #2, post 3 inspired by #3):
${inspirationTexts.map((t, i) => `#${i + 1}: "${t}"`).join('\n')}

About me: ${aboutYou || 'A professional looking to grow on LinkedIn.'}
My preferred writing styles: ${topTags.join(', ') || 'motivational, professional, storytelling'}${creators.length ? `\nWrite in a style similar to: ${creators.join(', ')}` : ''}${interests ? `\nMy interests: ${interests}` : ''}${productsList ? `\nMy products/services to promote: ${productsList}` : ''}
Make each post different in format (one list-based, one story, one insight/opinion).${defaultLength} Do NOT include hashtags.${customRules ? `\nCRITICAL — Follow these rules strictly: ${customRules}` : ''}`;

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
                temperature: gpCharLimit ? 0.6 : 0.8,
                max_tokens: gpCharLimit ? Math.max(200, Math.ceil(gpCharLimit * 3 / 2)) : 2000,
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

        if (gpCharLimit) {
            posts = posts.map(p => {
                if (p.length <= gpCharLimit) return p;
                const cut = p.lastIndexOf(' ', gpCharLimit - 3);
                return p.substring(0, cut > 0 ? cut : gpCharLimit - 3) + '...';
            });
        }

        const references = shuffled.map(ref => ({
            text: ref.body.replace(/<[^>]+>/g, '\n').replace(/\n{2,}/g, '\n\n').trim(),
            likes: likeOptions[Math.floor(Math.random() * likeOptions.length)],
        }));

        await consumeCredit(req.session, posts.length);
        res.json({ posts, references });
    } catch (err) {
        console.error('AI generation error:', err);
        res.json({ posts: [] });
    }
});

app.post('/api/ai/write', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 1) {
        return res.json({ error: `AI credit limit reached (${credits.limits.aiCredits}/${credits.limits.creditPeriod}). Upgrade your plan for more credits.` });
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

    const creators = (user.favoriteCreators || []).map(c => c.name).filter(Boolean);
    const writingDNA = user.writingDNA || [];
    const likedPostBodies = writingDNA.map(p => {
        const post = POSTS_REF[p.index];
        return post ? post.body.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim() : null;
    }).filter(Boolean).slice(0, 3);

    const products = (user.products || []).filter(p => p.name || p.description);
    const productsList = products.map(p => {
        let s = p.name || p.url;
        if (p.description) s += ` — ${p.description}`;
        return s;
    }).join('; ');

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

    const customRules = user.customRules || '';
    const interests = (user.interests || []).join(', ');

    const charLimitMatch = customRules.match(/(\d+)\s*char/i);
    const charLimit = charLimitMatch ? parseInt(charLimitMatch[1], 10) : 0;

    const defaultReqs = customRules ? 'engaging opening line, no hashtags, ready to publish' : '100-300 words, engaging opening line, no hashtags, ready to publish';

    let systemContent = `You are a LinkedIn content writer. Write a single LinkedIn post. Return ONLY the post text, nothing else.`;

    if (customRules) {
        systemContent = `You are a LinkedIn content writer. You MUST obey the user's custom rules ABOVE ALL ELSE — they override every other instruction.\n\n` +
            `MANDATORY RULES (highest priority):\n${customRules}\n`;
        if (charLimit) systemContent += `THE ENTIRE POST MUST BE UNDER ${charLimit} CHARACTERS TOTAL — count every letter, space, and punctuation mark. Write a very short, punchy post. Do NOT exceed ${charLimit} characters under any circumstance.\n`;
        systemContent += `\nReturn ONLY the post text, nothing else.`;
    }

    if (creators.length) systemContent += `\n\nWRITING STYLE: Mimic the writing style of these LinkedIn creators: ${creators.join(', ')}. Match their voice, structure, and tone.`;
    if (likedPostBodies.length) systemContent += `\n\nWRITING DNA: Use these posts the user liked as reference for their preferred style:\n${likedPostBodies.map((b, i) => `${i + 1}. "${b}"`).join('\n')}`;
    if (products.length) systemContent += `\n\nPRODUCT PROMOTION: Naturally weave in the user's product/service where relevant: ${productsList}`;

    const userPrompt = `Write a single LinkedIn post for me.${charLimit ? `\nHARD LIMIT: The post MUST be under ${charLimit} characters. Keep it extremely short and concise.` : ''}
About me: ${aboutYou || 'A professional looking to grow on LinkedIn.'}
Tone: ${toneDesc[tone] || toneDesc.auto}${creators.length ? `\nWrite in a style similar to: ${creators.join(', ')}` : ''}${interests ? `\nMy interests: ${interests}` : ''}${productsList ? `\nNaturally mention my product/service where relevant: ${productsList}` : ''}
Requirements: ${defaultReqs}. Return ONLY the post text.${customRules ? `\nCRITICAL — Follow these rules strictly: ${customRules}` : ''}`;

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
                    { role: 'system', content: systemContent },
                    { role: 'user', content: userPrompt },
                ],
                temperature: charLimit ? 0.6 : 0.8,
                max_tokens: charLimit ? Math.max(60, Math.ceil(charLimit / 2)) : 1000,
            }),
        });

        const data = await response.json();
        let post = data.choices?.[0]?.message?.content?.trim() || '';
        if (charLimit && post.length > charLimit) {
            const cut = post.lastIndexOf(' ', charLimit - 3);
            post = post.substring(0, cut > 0 ? cut : charLimit - 3) + '...';
        }
        await consumeCredit(req.session, 1);
        res.json({ post });
    } catch (err) {
        console.error('AI write error:', err);
        res.json({ error: 'Failed to generate post' });
    }
});

app.post('/api/ai/generate-product-posts', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 3) {
        return res.json({ posts: [], error: `AI credit limit reached (${credits.limits.aiCredits}/${credits.limits.creditPeriod}). Upgrade your plan for more credits.` });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ posts: [] });
    }

    const user = req.session.user;
    const aboutYou = user.aboutYou || '';
    const products = (user.products || []).filter(p => p.name || p.description);

    if (products.length === 0) {
        return res.json({ posts: [], error: 'No products configured. Add products in Context or Onboarding.' });
    }

    const productsList = products.map(p => {
        let s = p.name || p.url || '';
        if (p.description) s += ` — ${p.description}`;
        return s;
    }).join('\n');

    const customRules = user.customRules || '';
    const creators = (user.favoriteCreators || []).map(c => c.name).filter(Boolean);

    let systemPrompt = `You are a LinkedIn content strategist specializing in product marketing. Generate 3 unique LinkedIn posts that promote the user's products/services in a natural, value-driven way. Each post should feel like genuine advice or a story, not a hard sell. Return ONLY a JSON array of 3 strings.`;
    if (creators.length) systemPrompt += `\n\nWRITING STYLE: Mimic the style of: ${creators.join(', ')}.`;
    if (customRules) {
        systemPrompt += `\n\nMANDATORY RULES (override everything else):\n${customRules}`;
        const cm = customRules.match(/(\d+)\s*char/i);
        if (cm) systemPrompt += `\nEACH POST MUST BE UNDER ${cm[1]} CHARACTERS.`;
    }

    const userPrompt = `Generate 3 LinkedIn posts that naturally promote my products/services.
About me: ${aboutYou || 'A professional on LinkedIn.'}
My products/services:
${productsList}

Each post should take a different angle:
1. A success story or case study style
2. A problem-solution format showing how the product helps
3. A thought leadership post that naturally weaves in the product

Make them engaging, authentic, and not salesy. Do NOT include hashtags.${customRules ? `\nFollow these rules: ${customRules}` : ''}`;

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

        await consumeCredit(req.session, posts.length);
        res.json({ posts, products: products.map(p => p.name || p.url || 'Product') });
    } catch (err) {
        console.error('Product post generation error:', err);
        res.json({ posts: [] });
    }
});

// ---------- INSPIRATION SEARCH ----------

app.post('/api/inspiration/search', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const rapidKey = process.env.RAPIDAPI_KEY;
    if (!rapidKey || rapidKey === 'your-rapidapi-key-here') {
        return res.json({ data: [], total: 0, error: 'RapidAPI key not configured' });
    }

    const { keywords, sort_by, date_posted, content_type, page } = req.body;
    if (!keywords) {
        return res.json({ data: [], total: 0 });
    }

    const payload = { search_keywords: keywords, page: page || 1 };
    if (sort_by) payload.sort_by = sort_by;
    if (date_posted) payload.date_posted = date_posted;
    if (content_type) payload.content_type = content_type;

    try {
        const response = await fetch('https://web-scraping-api2.p.rapidapi.com/search-posts', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-rapidapi-key': rapidKey,
                'x-rapidapi-host': 'web-scraping-api2.p.rapidapi.com',
            },
            body: JSON.stringify(payload),
        });

        const data = await response.json();
        res.json(data);
    } catch (err) {
        console.error('Inspiration search error:', err);
        res.json({ data: [], total: 0, error: 'Search failed' });
    }
});

app.post('/api/ai/improve', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 1) {
        return res.json({ error: `AI credit limit reached (${credits.limits.aiCredits}/${credits.limits.creditPeriod}). Upgrade your plan for more credits.` });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ error: 'OpenAI API key not configured' });
    }

    const { text, action } = req.body;
    if (!text || !action) {
        return res.json({ error: 'Missing text or action' });
    }

    const instructions = {
        grammar: 'Fix all grammar, spelling, and punctuation errors. Keep the meaning and tone identical. Return only the corrected text.',
        translate: 'Translate the following text to English. Keep the same tone, style, and formatting. Return only the translated text.',
        hook: 'Rewrite this post with a much stronger, more attention-grabbing opening hook that makes people stop scrolling. Keep the core message the same. Return only the rewritten text.',
        details: 'Expand this post with more specific details, examples, or data points to make it more compelling and credible. Return only the rewritten text.',
        engaging: 'Rewrite this post to be more engaging — add energy, vary sentence length, and make the reader want to comment or share. Return only the rewritten text.',
        humorous: 'Rewrite this post with humor and wit while keeping the core message intact. Add clever wordplay or a funny twist. Return only the rewritten text.',
        creative: 'Rewrite this post in a more creative and original way — use vivid language, unexpected angles, or fresh metaphors. Return only the rewritten text.',
        sarcastic: 'Rewrite this post with a sarcastic, tongue-in-cheek tone while keeping the core message. Return only the rewritten text.',
        inspirational: 'Rewrite this post to be more inspirational and uplifting — make it motivate the reader to take action. Return only the rewritten text.',
        concise: 'Rewrite this post to be much shorter and more concise. Remove filler words, redundancy, and fluff. Keep the core message. Return only the rewritten text.',
    };

    const instruction = instructions[action];
    if (!instruction) {
        return res.json({ error: 'Unknown action' });
    }

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
                    { role: 'system', content: `You are a LinkedIn writing assistant. ${instruction}` },
                    { role: 'user', content: text },
                ],
                temperature: 0.7,
                max_tokens: 1500,
            }),
        });

        const data = await response.json();
        const result = data.choices?.[0]?.message?.content?.trim() || '';
        await consumeCredit(req.session, 1);
        res.json({ text: result });
    } catch (err) {
        console.error('AI improve error:', err);
        res.json({ error: 'Failed to improve text' });
    }
});

app.get('/api/queue', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    if (!req.session.user.queue || req.session.user.queue.length === 0) {
        const dbUser = await dbGetUser(req.session.user.linkedinId);
        if (dbUser && dbUser.queue) {
            req.session.user.queue = dbUser.queue;
        }
    }
    res.json({ queue: req.session.user.queue || [] });
});

app.post('/api/queue', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    if (!req.session.user.queue) req.session.user.queue = [];
    const { action, text, status, index, scheduledFor } = req.body;

    if (action === 'add' && text) {
        const item = {
            text,
            status: status || 'draft',
            date: new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }),
        };
        if (scheduledFor) item.scheduledFor = scheduledFor;
        req.session.user.queue.push(item);
    } else if (action === 'update' && typeof index === 'number' && text) {
        if (req.session.user.queue[index]) {
            req.session.user.queue[index].text = text;
            if (status) req.session.user.queue[index].status = status;
            if (scheduledFor) req.session.user.queue[index].scheduledFor = scheduledFor;
            req.session.user.queue[index].date = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        }
    } else if (action === 'remove' && typeof index === 'number') {
        req.session.user.queue.splice(index, 1);
    }

    await dbUpdateFields(req.session.user.linkedinId, {
        queue: req.session.user.queue,
    });

    res.json({ queue: req.session.user.queue });
});

// ---------- CAROUSEL ----------

function hasFeature(session, feature) {
    const primaryId = getPrimaryAccountId(session);
    const tier = session.user?.planTier || 'pro';
    const limits = PLAN_LIMITS[tier] || PLAN_LIMITS.pro;
    return (limits.features || []).includes(feature);
}

app.post('/api/ai/generate-carousel', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    if (!hasFeature(req.session, 'carousel')) {
        return res.status(403).json({ error: 'Carousel is available on Advanced and Ultra plans. Upgrade to access this feature.' });
    }

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 3) {
        return res.json({ error: `AI credit limit reached (${credits.limits.aiCredits}/${credits.limits.creditPeriod}). Upgrade your plan for more credits.` });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ error: 'OpenAI API key not configured' });
    }

    const { topic, slideCount = 8, style = 'professional' } = req.body;
    if (!topic) return res.status(400).json({ error: 'Topic is required' });

    const user = req.session.user;
    const aboutYou = user.aboutYou || '';
    const customRules = user.customRules || '';

    const styleDescriptions = {
        professional: 'Clean, corporate style with data-driven insights and clear headings.',
        bold: 'Eye-catching, bold statements with high contrast and punchy one-liners.',
        minimal: 'Minimalist design approach with short text, lots of whitespace, one idea per slide.',
        colorful: 'Vibrant and energetic tone with metaphors, emojis, and storytelling elements.',
    };

    const styleGuide = styleDescriptions[style] || styleDescriptions.professional;
    const count = Math.min(Math.max(parseInt(slideCount) || 8, 4), 15);

    const systemPrompt = `You are a LinkedIn carousel content expert. Create a ${count}-slide carousel about the given topic.

Return ONLY a valid JSON array of exactly ${count} objects. Each object must have:
- "title": string (short, max 8 words)
- "body": string (1-2 sentences, max 30 words)
- "bulletPoints": array of strings (0-4 bullet points, each max 10 words)

Slide structure:
- Slide 1: Cover slide — catchy title, short subtitle in body, no bullets
- Slides 2 to ${count - 1}: Content slides — each covering one key point
- Slide ${count}: CTA slide — call-to-action title, body with next step, no bullets

Style: ${styleGuide}
${customRules ? `\nUser rules: ${customRules}` : ''}
${aboutYou ? `\nAbout the author: ${aboutYou}` : ''}

Return ONLY the JSON array, no markdown, no explanation.`;

    try {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
            body: JSON.stringify({
                model: 'gpt-4o-mini',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: `Create a ${count}-slide LinkedIn carousel about: ${topic}` },
                ],
                temperature: 0.7,
                max_tokens: 2000,
            }),
        });

        const data = await response.json();
        const content = data.choices?.[0]?.message?.content || '';

        let slides = [];
        try {
            const jsonMatch = content.match(/\[[\s\S]*\]/);
            if (jsonMatch) slides = JSON.parse(jsonMatch[0]);
        } catch {
            return res.json({ error: 'Failed to parse AI response. Please try again.' });
        }

        if (!slides.length) return res.json({ error: 'No slides generated. Please try a different topic.' });

        await consumeCredit(req.session, 3);
        res.json({ slides });
    } catch (err) {
        console.error('Carousel generation error:', err);
        res.json({ error: 'Failed to generate carousel.' });
    }
});

function renderCarouselPDF(slides, brandColor, title, userName) {
    const WIDTH = 1080;
    const HEIGHT = 1080;
    const MARGIN = 80;
    const color = brandColor || '#0A66C2';

    const doc = new PDFDocument({ size: [WIDTH, HEIGHT], margin: 0, autoFirstPage: false });

    slides.forEach((slide, i) => {
        doc.addPage({ size: [WIDTH, HEIGHT], margin: 0 });

        // Background
        doc.rect(0, 0, WIDTH, HEIGHT).fill('#FFFFFF');

        // Accent bar
        doc.rect(0, 0, WIDTH, 12).fill(color);

        // Slide number
        if (i > 0 && i < slides.length - 1) {
            doc.fontSize(14).fillColor('#999999')
               .text(`${i + 1} / ${slides.length}`, WIDTH - MARGIN - 40, HEIGHT - 50, { width: 80, align: 'right' });
        }

        const isFirst = i === 0;
        const isLast = i === slides.length - 1;

        if (isFirst) {
            // Cover slide
            doc.rect(0, HEIGHT - 200, WIDTH, 200).fill(color);
            doc.fontSize(52).fillColor(color)
               .text(slide.title || title || '', MARGIN, 280, { width: WIDTH - MARGIN * 2, align: 'center', lineGap: 8 });
            doc.fontSize(22).fillColor('#555555')
               .text(slide.body || '', MARGIN, 500, { width: WIDTH - MARGIN * 2, align: 'center', lineGap: 6 });
            if (userName) {
                doc.fontSize(18).fillColor('#FFFFFF')
                   .text(userName, MARGIN, HEIGHT - 130, { width: WIDTH - MARGIN * 2, align: 'center' });
            }
            doc.fontSize(14).fillColor('#FFFFFF').opacity(0.7)
               .text('Swipe to read more →', MARGIN, HEIGHT - 60, { width: WIDTH - MARGIN * 2, align: 'center' });
            doc.opacity(1);
        } else if (isLast) {
            // CTA slide
            doc.rect(0, 0, WIDTH, HEIGHT).fill(color);
            doc.fontSize(44).fillColor('#FFFFFF')
               .text(slide.title || 'Thanks for reading!', MARGIN, 320, { width: WIDTH - MARGIN * 2, align: 'center', lineGap: 8 });
            doc.fontSize(22).fillColor('#FFFFFF').opacity(0.85)
               .text(slide.body || 'Follow for more content like this.', MARGIN, 500, { width: WIDTH - MARGIN * 2, align: 'center', lineGap: 6 });
            doc.opacity(1);
            if (userName) {
                doc.fontSize(20).fillColor('#FFFFFF')
                   .text(userName, MARGIN, 650, { width: WIDTH - MARGIN * 2, align: 'center' });
            }
        } else {
            // Content slide
            let y = 60;
            doc.fontSize(36).fillColor(color)
               .text(slide.title || '', MARGIN, y, { width: WIDTH - MARGIN * 2, lineGap: 6 });
            y += doc.heightOfString(slide.title || '', { width: WIDTH - MARGIN * 2, fontSize: 36 }) + 30;

            doc.rect(MARGIN, y, 60, 4).fill(color);
            y += 30;

            if (slide.body) {
                doc.fontSize(20).fillColor('#333333')
                   .text(slide.body, MARGIN, y, { width: WIDTH - MARGIN * 2, lineGap: 8 });
                y += doc.heightOfString(slide.body, { width: WIDTH - MARGIN * 2, fontSize: 20 }) + 24;
            }

            if (slide.bulletPoints && slide.bulletPoints.length > 0) {
                slide.bulletPoints.forEach(bp => {
                    doc.fontSize(20).fillColor(color).text('●', MARGIN, y, { continued: false });
                    doc.fontSize(19).fillColor('#444444')
                       .text('  ' + bp, MARGIN + 24, y, { width: WIDTH - MARGIN * 2 - 24, lineGap: 6 });
                    y += doc.heightOfString('  ' + bp, { width: WIDTH - MARGIN * 2 - 24, fontSize: 19 }) + 16;
                });
            }
        }
    });

    doc.end();
    return doc;
}

app.post('/api/carousel/generate-pdf', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    if (!hasFeature(req.session, 'carousel')) {
        return res.status(403).json({ error: 'Carousel is available on Advanced and Ultra plans.' });
    }

    const { slides, brandColor, title } = req.body;
    if (!slides || !Array.isArray(slides) || !slides.length) {
        return res.status(400).json({ error: 'Slides data is required' });
    }

    const userName = req.session.user.name || '';

    try {
        const doc = renderCarouselPDF(slides, brandColor, title, userName);
        const chunks = [];
        doc.on('data', c => chunks.push(c));
        doc.on('end', () => {
            const pdfBuffer = Buffer.concat(chunks);
            res.setHeader('Content-Type', 'application/pdf');
            res.setHeader('Content-Disposition', `attachment; filename="carousel-${Date.now()}.pdf"`);
            res.send(pdfBuffer);
        });
        doc.on('error', err => {
            console.error('PDF generation error:', err);
            res.status(500).json({ error: 'Failed to generate PDF' });
        });
    } catch (err) {
        console.error('PDF render error:', err);
        res.status(500).json({ error: 'Failed to generate PDF' });
    }
});

app.post('/api/carousel/publish', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    if (!hasFeature(req.session, 'carousel')) {
        return res.status(403).json({ error: 'Carousel is available on Advanced and Ultra plans.' });
    }

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 1) {
        return res.json({ error: `AI credit limit reached.` });
    }

    const { slides, brandColor, title, text, audience } = req.body;
    if (!slides || !Array.isArray(slides) || !slides.length) {
        return res.status(400).json({ error: 'Slides data is required' });
    }
    if (!text || !text.trim()) {
        return res.status(400).json({ error: 'Post caption text is required' });
    }

    const accessToken = req.session.user.accessToken;
    if (!accessToken) {
        return res.status(401).json({ error: 'No LinkedIn access token. Please sign in again.' });
    }

    const linkedinId = req.session.user.linkedinId;
    const personUrn = `urn:li:person:${linkedinId}`;
    const visibility = audience === 'connections' ? 'CONNECTIONS' : 'PUBLIC';
    const userName = req.session.user.name || '';

    try {
        // Generate PDF
        const doc = renderCarouselPDF(slides, brandColor, title, userName);
        const chunks = [];
        const pdfBuffer = await new Promise((resolve, reject) => {
            doc.on('data', c => chunks.push(c));
            doc.on('end', () => resolve(Buffer.concat(chunks)));
            doc.on('error', reject);
        });

        // Step 1: Register upload
        const registerRes = await fetch('https://api.linkedin.com/v2/assets?action=registerUpload', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${accessToken}`,
            },
            body: JSON.stringify({
                registerUploadRequest: {
                    recipes: ['urn:li:digitalmediaRecipe:feedshare-document'],
                    owner: personUrn,
                    serviceRelationships: [{
                        relationshipType: 'OWNER',
                        identifier: 'urn:li:userGeneratedContent',
                    }],
                },
            }),
        });

        if (!registerRes.ok) {
            const errText = await registerRes.text();
            console.error('LinkedIn register upload failed:', errText);
            return res.status(500).json({ error: 'Failed to register upload with LinkedIn.' });
        }

        const registerData = await registerRes.json();
        const uploadUrl = registerData.value?.uploadMechanism?.['com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest']?.uploadUrl;
        const asset = registerData.value?.asset;

        if (!uploadUrl || !asset) {
            return res.status(500).json({ error: 'LinkedIn upload registration returned invalid data.' });
        }

        // Step 2: Upload PDF
        const uploadRes = await fetch(uploadUrl, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/pdf',
            },
            body: pdfBuffer,
        });

        if (!uploadRes.ok) {
            console.error('LinkedIn PDF upload failed:', uploadRes.status);
            return res.status(500).json({ error: 'Failed to upload PDF to LinkedIn.' });
        }

        // Step 3: Create post with document
        const postRes = await fetch('https://api.linkedin.com/v2/ugcPosts', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${accessToken}`,
                'X-Restli-Protocol-Version': '2.0.0',
            },
            body: JSON.stringify({
                author: personUrn,
                lifecycleState: 'PUBLISHED',
                specificContent: {
                    'com.linkedin.ugc.ShareContent': {
                        shareCommentary: { text: text.trim() },
                        shareMediaCategory: 'ARTICLE',
                        media: [{
                            status: 'READY',
                            media: asset,
                            title: { text: title || 'Carousel Post' },
                        }],
                    },
                },
                visibility: {
                    'com.linkedin.ugc.MemberNetworkVisibility': visibility,
                },
            }),
        });

        if (postRes.ok) {
            const postData = await postRes.json();
            const postId = postData.id;
            const postUrl = postId
                ? `https://www.linkedin.com/feed/update/${postId}/`
                : 'https://www.linkedin.com/feed/';

            await consumeCredit(req.session, 1);
            console.log(`Carousel published to LinkedIn by ${req.session.user.name}: ${postId}`);
            res.json({ success: true, postId, postUrl });
        } else {
            const errData = await postRes.text();
            console.error('LinkedIn carousel post failed:', postRes.status, errData);

            if (postRes.status === 401) {
                return res.status(401).json({ error: 'LinkedIn access token expired. Please sign out and sign in again.' });
            }
            res.status(postRes.status).json({ error: 'Failed to publish carousel to LinkedIn.', details: errData });
        }
    } catch (err) {
        console.error('Carousel publish error:', err);
        res.status(500).json({ error: 'Could not publish carousel. Please try again.' });
    }
});

// ---------- EXTENSION & ANALYTICS ----------

// Generate a simple auth token for the extension
app.post('/api/extension/auth', async (req, res) => {
    const { email, linkedinId } = req.body;
    if (!email) return res.status(400).json({ error: 'Email is required' });

    let user = null;
    if (linkedinId) {
        user = await dbGetUser(linkedinId);
    }

    if (!user) {
        user = await dbFindByEmail(email);
    }

    if (!user) {
        if (req.session.user && req.session.user.email === email) {
            user = req.session.user;
        }
    }

    if (!user) {
        return res.status(404).json({ error: 'No account found. Please sign in to SuperLinkedIn first.' });
    }

    // Generate a token (hash of linkedinId + a secret)
    const secret = process.env.SESSION_SECRET || 'fallback';
    const token = crypto.createHmac('sha256', secret)
        .update(user.linkedinId)
        .digest('hex');

    // Store the token mapping
    await dbUpdateFields(user.linkedinId, { extensionToken: token });

    // Cache token -> linkedinId for fast lookups
    if (!global._tokenCache) global._tokenCache = {};
    global._tokenCache[token] = user.linkedinId;

    res.json({ token, name: user.name });
});

// Middleware to authenticate extension requests via Bearer token
async function authExtension(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Missing authorization token' });
    }

    const token = authHeader.slice(7);
    const secret = process.env.SESSION_SECRET || 'fallback';

    // Find user whose extensionToken matches
    // Since we generate token = HMAC(linkedinId), we can check session user first
    if (req.session.user) {
        const expectedToken = crypto.createHmac('sha256', secret)
            .update(req.session.user.linkedinId)
            .digest('hex');
        if (expectedToken === token) {
            req.extUser = req.session.user;
            return next();
        }
    }

    // Fallback: check all known linkedinIds from the token
    // For efficiency, store a token->linkedinId mapping in a simple in-memory cache
    if (!global._tokenCache) global._tokenCache = {};

    if (global._tokenCache[token]) {
        const user = await dbGetUser(global._tokenCache[token]);
        if (user) {
            req.extUser = user;
            return next();
        }
    }

    return res.status(401).json({ error: 'Invalid or expired token' });
}

// Receive scraped analytics data from extension
app.post('/api/analytics/sync', authExtension, async (req, res) => {
    const { followers, posts, dashboardStats } = req.body;
    const linkedinId = req.extUser.linkedinId;

    const updates = {};
    const now = new Date().toISOString();
    const today = now.split('T')[0];

    if (followers !== null && followers !== undefined) {
        updates.analyticsFollowers = followers;

        // Append to followers history
        const user = await dbGetUser(linkedinId);
        const history = (user && user.analyticsFollowersHistory) || [];
        const lastEntry = history[history.length - 1];
        if (!lastEntry || lastEntry.date !== today) {
            history.push({ date: today, count: followers });
            if (history.length > 365) history.splice(0, history.length - 365);
        } else {
            lastEntry.count = followers;
        }
        updates.analyticsFollowersHistory = history;
    }

    if (posts && posts.length > 0) {
        const user = await dbGetUser(linkedinId);
        const existing = (user && user.analyticsPostMetrics) || [];

        posts.forEach(p => {
            const idx = existing.findIndex(e => e.text === p.text);
            if (idx >= 0) {
                existing[idx] = { ...existing[idx], ...p };
            } else {
                existing.push(p);
            }
        });

        if (existing.length > 200) existing.splice(0, existing.length - 200);
        updates.analyticsPostMetrics = existing;
    }

    if (dashboardStats) {
        const user = await dbGetUser(linkedinId);
        const existing = (user && user.analyticsDashboard) || {};
        updates.analyticsDashboard = { ...existing, ...dashboardStats, updatedAt: now };

        if (dashboardStats.profileViews !== undefined) updates.analyticsProfileViews = dashboardStats.profileViews;
        if (dashboardStats.searchAppearances !== undefined) updates.analyticsSearchAppearances = dashboardStats.searchAppearances;
        if (dashboardStats.postImpressions !== undefined) {
            if (!updates.analyticsEngagement) {
                const eng = (user && user.analyticsEngagement) || {};
                updates.analyticsEngagement = { ...eng, impressions: dashboardStats.postImpressions };
            } else {
                updates.analyticsEngagement.impressions = dashboardStats.postImpressions;
            }
        }
    }

    updates.analyticsLastSync = now;

    // Aggregate engagement stats
    if (updates.analyticsPostMetrics || posts) {
        const user = await dbGetUser(linkedinId);
        const allPosts = updates.analyticsPostMetrics || (user && user.analyticsPostMetrics) || [];
        let totalLikes = 0, totalComments = 0, totalReposts = 0, totalImpressions = 0;
        allPosts.forEach(p => {
            totalLikes += p.likes || 0;
            totalComments += p.comments || 0;
            totalReposts += p.reposts || 0;
            totalImpressions += p.impressions || 0;
        });
        updates.analyticsEngagement = {
            likes: totalLikes,
            comments: totalComments,
            reposts: totalReposts,
            impressions: totalImpressions,
            totalPosts: allPosts.length,
        };
    }

    await dbUpdateFields(linkedinId, updates);

    // Cache the token for future lookups
    const secret = process.env.SESSION_SECRET || 'fallback';
    const token = crypto.createHmac('sha256', secret).update(linkedinId).digest('hex');
    if (!global._tokenCache) global._tokenCache = {};
    global._tokenCache[token] = linkedinId;

    res.json({ ok: true, syncedAt: now });
});

// Get analytics data for the dashboard
app.get('/api/analytics', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) {
        return res.json({});
    }

    res.json({
        followers: user.analyticsFollowers || 0,
        followersHistory: user.analyticsFollowersHistory || [],
        postMetrics: user.analyticsPostMetrics || [],
        engagement: user.analyticsEngagement || {},
        lastSync: user.analyticsLastSync || null,
        profileViews: user.analyticsProfileViews || 0,
        searchAppearances: user.analyticsSearchAppearances || 0,
        dashboard: user.analyticsDashboard || {},
    });
});

// Analytics summary for extension popup/sidebar (bearer-token auth)
app.get('/api/analytics/summary', authExtension, async (req, res) => {
    const user = await dbGetUser(req.extUser.linkedinId);
    if (!user) return res.json({});

    const posts = user.analyticsPostMetrics || [];
    const eng = user.analyticsEngagement || {};
    const topPosts = [...posts]
        .sort((a, b) => ((b.likes || 0) + (b.comments || 0) + (b.reposts || 0))
                       - ((a.likes || 0) + (a.comments || 0) + (a.reposts || 0)))
        .slice(0, 5);

    const totalEng = (eng.likes || 0) + (eng.comments || 0) + (eng.reposts || 0);
    const avgEngagement = posts.length > 0
        ? (totalEng / posts.length / Math.max((eng.impressions || 0) / posts.length, 1) * 100)
        : 0;

    res.json({
        followers: user.analyticsFollowers || 0,
        totalPosts: posts.length,
        avgEngagement: Math.round(avgEngagement * 10) / 10,
        totalImpressions: eng.impressions || 0,
        totalLikes: eng.likes || 0,
        totalComments: eng.comments || 0,
        totalReposts: eng.reposts || 0,
        topPosts,
        followerHistory: user.analyticsFollowersHistory || [],
        plan: user.planTier || user.plan || 'pro',
    });
});

// ---------- API ROUTES ----------

// Get current user info (re-hydrate from DB to catch any missed updates)
app.get('/api/me', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const activeId = getActiveProfileId(req.session);
    const dbUser = await dbGetUser(activeId);
    if (dbUser) {
        req.session.user = {
            ...dbUser,
            linkedinId: activeId,
            name: dbUser.name || req.session.user.name,
            email: dbUser.email || req.session.user.email,
            picture: dbUser.picture || req.session.user.picture,
            accessToken: req.session.user.accessToken,
        };
    }

    const primaryId = getPrimaryAccountId(req.session);
    let paid = req.session.user.paid;
    let plan = req.session.user.plan;
    let planTier = req.session.user.planTier;

    if (primaryId && primaryId !== activeId) {
        const primaryUser = await dbGetUser(primaryId);
        if (primaryUser) {
            paid = primaryUser.paid;
            plan = primaryUser.plan;
            planTier = primaryUser.planTier;
        }
    }

    const { accessToken, ...safeUser } = req.session.user;
    safeUser.paid = paid;
    safeUser.plan = plan;
    safeUser.planTier = planTier;
    safeUser.activeProfileId = activeId;
    safeUser.primaryAccountId = primaryId;

    // Include payment failure info
    const primaryUser = primaryId ? await dbGetUser(primaryId) : dbUser;
    if (primaryUser) {
        safeUser.paymentFailed = primaryUser.paymentFailed || false;
        safeUser.subscriptionStatus = primaryUser.subscriptionStatus || null;
    }

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

// ---------- POST SCHEDULER ----------

async function processScheduledPosts() {
    try {
        const now = new Date();
        const result = await ddb.send(new ScanCommand({ TableName: DYNAMO_TABLE }));
        const users = result.Items || [];

        for (const user of users) {
            if (!user.queue || !Array.isArray(user.queue)) continue;
            let changed = false;

            for (const item of user.queue) {
                if (item.status !== 'scheduled' || !item.scheduledFor) continue;
                const scheduledTime = new Date(item.scheduledFor);
                if (scheduledTime <= now) {
                    item.status = 'posted';
                    item.postedAt = now.toISOString();
                    changed = true;
                    console.log(`[Scheduler] Posted queued item for user ${user.linkedinId} scheduled at ${item.scheduledFor}`);
                }
            }

            if (changed) {
                await dbUpdateFields(user.linkedinId, { queue: user.queue });
            }
        }
    } catch (err) {
        console.error('[Scheduler] Error processing scheduled posts:', err.message);
    }
}

setInterval(() => {
    processScheduledPosts().catch(err => {
        console.error('[Scheduler] Interval error:', err.message);
    });
}, 60 * 1000);

// ---------- START SERVER ----------

app.listen(PORT, () => {
    console.log(`\n  SuperLinkedIn server running at http://localhost:${PORT}`);
    console.log(`  Node ${process.version} | PID ${process.pid} | ENV ${process.env.NODE_ENV || 'development'}\n`);

    if (LINKEDIN.clientId === 'your_client_id_here') {
        console.log('  ⚠  WARNING: LinkedIn Client ID not configured!');
        console.log('  ⚠  Edit .env and add your LinkedIn app credentials.');
        console.log('  ⚠  Get them from https://www.linkedin.com/developers/apps\n');
    }

    processScheduledPosts().catch(err => {
        console.error('[Scheduler] Initial run error:', err.message);
    });
});
