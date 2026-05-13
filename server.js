require('dotenv').config();
const express = require('express');
const session = require('express-session');
const crypto = require('crypto');
const path = require('path');
const Stripe = require('stripe');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, ScanCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');

const PDFDocument = require('pdfkit');
const multer = require('multer');
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

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
            FilterExpression: 'email = :email AND (attribute_not_exists(pendingTeamInvite) OR pendingTeamInvite = :f)',
            ExpressionAttributeValues: { ':email': email, ':f': false },
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

// Two-host setup: marketing site lives on `www`, the app/dashboard lives on `app`.
// We path-route between them: anything in APP_PATH_PREFIXES is forced onto APP_HOST,
// everything else is forced onto MARKETING_HOST. Apex always redirects to one or the other.
const APEX_HOST = 'superlinkedin.org';
const APP_HOST = (process.env.APP_HOST || 'app.superlinkedin.org').toLowerCase();
const MARKETING_HOST = (process.env.MARKETING_HOST || 'www.superlinkedin.org').toLowerCase();
const APP_BASE = `https://${APP_HOST}`;
const MARKETING_BASE = `https://${MARKETING_HOST}`;

const APP_PATH_PREFIXES = ['/app', '/auth', '/api', '/checkout', '/billing', '/onboarding', '/upgrade', '/playbook', '/logout'];

function pathBelongsToApp(pathname) {
    if (!pathname) return false;
    return APP_PATH_PREFIXES.some(p => pathname === p || pathname.startsWith(p + '/'));
}

app.use((req, res, next) => {
    const host = (req.hostname || '').toLowerCase();
    const knownHost = host === APEX_HOST || host === APP_HOST || host === MARKETING_HOST;
    // Pass through anything that isn't one of our three known hosts (App Runner default URL, localhost, etc.)
    if (!knownHost) return next();

    const targetHost = pathBelongsToApp(req.path) ? APP_HOST : MARKETING_HOST;
    if (host !== targetHost) {
        return res.redirect(301, `https://${targetHost}${req.originalUrl}`);
    }
    next();
});

const LINKEDIN = {
    clientId: process.env.LINKEDIN_CLIENT_ID,
    clientSecret: process.env.LINKEDIN_CLIENT_SECRET,
    authUrl: 'https://www.linkedin.com/oauth/v2/authorization',
    tokenUrl: 'https://www.linkedin.com/oauth/v2/accessToken',
    userInfoUrl: 'https://api.linkedin.com/v2/userinfo',
    scope: 'openid profile email w_member_social',
};

/** Marketing REST APIs (documents, posts). Override with LINKEDIN_REST_VERSION if LinkedIn deprecates this month. */
const LINKEDIN_REST_VERSION = process.env.LINKEDIN_REST_VERSION || '202411';

function linkedinRestHeaders(extra = {}) {
    return {
        'X-Restli-Protocol-Version': '2.0.0',
        'LinkedIn-Version': LINKEDIN_REST_VERSION,
        ...extra,
    };
}

async function linkedInGetDocumentStatus(accessToken, documentUrn) {
    const encoded = encodeURIComponent(documentUrn);
    const r = await fetch(`https://api.linkedin.com/rest/documents/${encoded}`, {
        headers: {
            Authorization: `Bearer ${accessToken}`,
            ...linkedinRestHeaders(),
        },
    });
    if (!r.ok) return null;
    try {
        return await r.json();
    } catch {
        return null;
    }
}

/** Wait until PDF upload is processed; posting before AVAILABLE often fails. */
async function waitLinkedInDocumentReady(accessToken, documentUrn) {
    const deadline = Date.now() + 60000;
    let delayMs = 800;
    while (Date.now() < deadline) {
        const doc = await linkedInGetDocumentStatus(accessToken, documentUrn);
        const status = doc && doc.status;
        if (status === 'AVAILABLE') return { ok: true };
        if (status === 'PROCESSING_FAILED' || status === 'FAILED') {
            return { ok: false, error: 'LinkedIn could not process the PDF. Try a smaller carousel or simpler layout.' };
        }
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        delayMs = Math.min(delayMs + 250, 3000);
    }
    return { ok: false, error: 'LinkedIn took too long to process the PDF. Try publishing again in a minute.' };
}

function carouselPdfFilenameTitle(topic) {
    const baseRaw = String(topic || '')
        .replace(/[^a-zA-Z0-9\s\-+.]/g, '')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 72);
    const base = baseRaw || 'Carousel';
    return /\.pdf$/i.test(base) ? base : `${base}.pdf`;
}

function getRedirectUri(req) {
    const proto = req.headers['x-forwarded-proto'] || req.protocol || 'https';
    const host = req.headers['x-forwarded-host'] || req.headers.host || new URL(process.env.BASE_URL).host;
    return `${proto}://${host}/auth/linkedin/callback`;
}

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
        domain: process.env.COOKIE_DOMAIN || undefined,
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
app.get('/app', async (req, res) => {
    // If the visitor arrived via a team invite link, capture the code into the
    // session and route them through OAuth (or accept the invite immediately if
    // they're already logged in). This MUST happen before app.html is served,
    // otherwise the dashboard's loadUser() will see paid=false on a fresh
    // account and bounce the invitee to /upgrade.
    if (req.query.invite) {
        req.session.inviteCode = String(req.query.invite);
    }

    if (req.session.inviteCode) {
        if (!req.session.user) {
            console.log('[Invite] Anonymous visitor with invite code, redirecting to LinkedIn auth');
            return res.redirect('/auth/linkedin');
        }
        try {
            const result = await acceptPendingInvite(req);
            if (result && result.accepted) {
                console.log(`[Invite] Already-logged-in user ${req.session.user.name} accepted invite from ${result.inviterId}`);
            }
        } catch (err) {
            console.error('[Invite] acceptPendingInvite failed:', err.message);
        }
        return res.redirect('/app');
    }

    res.sendFile(path.join(__dirname, 'app.html'));
});

// Look up the primary account that issued an invite code. Falls back to a
// DynamoDB scan if the in-memory cache is cold (e.g. after a server restart).
if (!global._inviteCache) global._inviteCache = {};
async function findInviterByCode(code) {
    if (!code) return null;
    if (global._inviteCache[code]) return global._inviteCache[code];
    try {
        const result = await ddb.send(new ScanCommand({
            TableName: DYNAMO_TABLE,
            ProjectionExpression: 'linkedinId, teamMembers',
        }));
        for (const item of (result.Items || [])) {
            const members = item.teamMembers || [];
            if (members.some(m => m && m.inviteCode === code)) {
                global._inviteCache[code] = item.linkedinId;
                return item.linkedinId;
            }
        }
    } catch (err) {
        console.error('[Invite] findInviterByCode scan error:', err.message);
    }
    return null;
}

// Attach the currently-logged-in profile to the inviter's workspace as a
// team member. Idempotent: re-running it just refreshes the membership row.
async function acceptPendingInvite(req) {
    const code = req.session && req.session.inviteCode;
    if (!code || !req.session.user) return { accepted: false, reason: 'no_invite_or_user' };

    const inviterId = await findInviterByCode(code);
    if (!inviterId) {
        console.warn('[Invite] No inviter found for code', code);
        delete req.session.inviteCode;
        return { accepted: false, reason: 'unknown_code' };
    }

    const inviter = await dbGetUser(inviterId);
    if (!inviter) {
        delete req.session.inviteCode;
        return { accepted: false, reason: 'inviter_missing' };
    }

    const profileId = req.session.user.linkedinId;

    // Don't let the inviter accept their own invite.
    if (profileId === inviterId) {
        delete req.session.inviteCode;
        return { accepted: false, reason: 'self_invite' };
    }

    const members = inviter.teamMembers || [];
    const idx = members.findIndex(m => m && m.inviteCode === code);
    if (idx === -1) {
        delete req.session.inviteCode;
        return { accepted: false, reason: 'invite_revoked' };
    }

    const expectedInviteEmail = normalizeInviteEmail(members[idx].email || '');
    const resolvedEmail = normalizeInviteEmail((req.session.user && req.session.user.email) || '');
    if (expectedInviteEmail && resolvedEmail && expectedInviteEmail !== resolvedEmail) {
        console.warn('[Invite] Email mismatch for invite code — expected %s got %s', expectedInviteEmail, resolvedEmail || '(missing)');
        delete req.session.inviteCode;
        return { accepted: false, reason: 'email_mismatch' };
    }

    members[idx] = {
        ...members[idx],
        status: 'active',
        linkedinId: profileId,
        name: req.session.user.name || members[idx].name,
        picture: req.session.user.picture || members[idx].picture,
        joinedAt: members[idx].joinedAt || new Date().toISOString(),
    };
    await dbUpdateFields(inviterId, { teamMembers: members });

    // Tag the joining profile so /api/me can inherit the inviter's paid plan
    // (see the primaryId branch in /api/me).
    await dbUpdateFields(profileId, {
        parentAccountId: inviterId,
        invitedVia: code,
        invitedAt: members[idx].invitedAt || new Date().toISOString(),
        onboardingComplete: false,
        onboardingCompletedAt: null,
    });

    await deleteTeamInvitePlaceholder(inviterId, members[idx].email);

    req.session.primaryAccountId = inviterId;
    req.session.activeProfileId = profileId;
    if (req.session.user) {
        req.session.user.parentAccountId = inviterId;
    }
    delete req.session.inviteCode;

    console.log(`[Invite] ${req.session.user.name} (${profileId}) joined workspace of ${inviter.name} (${inviterId}) as team member`);
    return { accepted: true, inviterId, member: members[idx] };
}

// ---------- TEAM INVITE PENDING RECORDS ----------

/** Lowercase trimmed email — used when matching OAuth profile to owner invites. */
function normalizeInviteEmail(email) {
    return String(email || '').trim().toLowerCase();
}

/**
 * Dynamo item id for a synthetic "pending teammate" bound to `(ownerLinkedinId, email)`.
 * OAuth never returns this LinkedIn ID; invites are redeemed into the real LinkedIn profile.
 */
function teamInvitePlaceholderLinkedinId(ownerLinkedinId, normEmail) {
    const h = crypto.createHash('sha256').update(`${ownerLinkedinId}|${normEmail}`).digest('hex').slice(0, 48);
    return `team-inv-pend:${h}`;
}

async function upsertTeamInvitePlaceholder(ownerLinkedinId, inviteeEmailNorm, inviteCode, role, invitedAtIso) {
    const lid = teamInvitePlaceholderLinkedinId(ownerLinkedinId, inviteeEmailNorm);
    const local = inviteeEmailNorm.includes('@') ? inviteeEmailNorm.split('@')[0] : inviteeEmailNorm;
    await dbSaveUser({
        linkedinId: lid,
        email: inviteeEmailNorm,
        name: local ? local.charAt(0).toUpperCase() + local.slice(1) : 'Invitee',
        pendingTeamInvite: true,
        teamOwnerLinkedinId: ownerLinkedinId,
        pendingInviteCode: inviteCode,
        invitedRole: role || 'editor',
        paid: false,
        onboardingComplete: false,
        invitedAt: invitedAtIso || new Date().toISOString(),
        createdAt: new Date().toISOString(),
    });
}

async function deleteTeamInvitePlaceholder(ownerLinkedinId, inviteRawEmail) {
    const lid = teamInvitePlaceholderLinkedinId(ownerLinkedinId, normalizeInviteEmail(inviteRawEmail));
    try {
        await ddb.send(new DeleteCommand({
            TableName: DYNAMO_TABLE,
            Key: { linkedinId: lid },
        }));
    } catch (err) {
        console.error('[Team] deleteTeamInvitePlaceholder:', err.message);
    }
}

/** All pending-invite stubs for one email (rare duplicates if multiple owners invite same inbox). Newest wins first. */
async function listPendingTeamInvitesForEmail(normEmail) {
    if (!normEmail) return [];
    const matches = [];
    let start;
    try {
        do {
            const r = await ddb.send(new ScanCommand({
                TableName: DYNAMO_TABLE,
                ExclusiveStartKey: start,
                FilterExpression: 'pendingTeamInvite = :p AND email = :e',
                ExpressionAttributeValues: { ':p': true, ':e': normEmail },
            }));
            start = r.LastEvaluatedKey;
            (r.Items || []).forEach((it) => matches.push(it));
        } while (start);
    } catch (err) {
        console.error('[Team] listPendingTeamInvitesForEmail:', err.message);
    }
    return matches.sort((a, b) => String(b.invitedAt || '').localeCompare(String(a.invitedAt || '')));
}

async function redeemPendingTeamInviteForOAuth(session, profile, accessToken) {
    const normEmail = normalizeInviteEmail(profile.email);
    if (!normEmail || !profile.sub) return false;

    const pendingRows = (await listPendingTeamInvitesForEmail(normEmail))
        .filter((row) => row.teamOwnerLinkedinId && row.teamOwnerLinkedinId !== profile.sub);

    let inviteLinkedinId = profile.sub;

    for (const pending of pendingRows) {
        const ownerId = pending.teamOwnerLinkedinId;
        const owner = await dbGetUser(ownerId);
        const members = owner && Array.isArray(owner.teamMembers) ? owner.teamMembers : [];
        const code = pending.pendingInviteCode;
        const ix = members.findIndex(
            (m) => m && m.inviteCode === code && normalizeInviteEmail(m.email) === normEmail && m.status === 'invited',
        );
        if (ix === -1) {
            try {
                await ddb.send(new DeleteCommand({
                    TableName: DYNAMO_TABLE,
                    Key: { linkedinId: pending.linkedinId },
                }));
            } catch { /* orphaned stub */ }
            continue;
        }

        const inviteeRow = await dbGetUser(inviteLinkedinId);
        if (inviteeRow && inviteeRow.paid === true && !inviteeRow.parentAccountId) {
            continue;
        }

        members[ix] = {
            ...members[ix],
            status: 'active',
            linkedinId: inviteLinkedinId,
            name: profile.name || members[ix].name,
            picture: profile.picture || members[ix].picture,
            joinedAt: members[ix].joinedAt || new Date().toISOString(),
        };

        await dbUpdateFields(ownerId, { teamMembers: members });

        await dbUpdateFields(inviteLinkedinId, {
            parentAccountId: ownerId,
            invitedViaTeam: code,
            invitedAt: pending.invitedAt || new Date().toISOString(),
            onboardingComplete: false,
            onboardingCompletedAt: null,
        });

        try {
            await ddb.send(new DeleteCommand({
                TableName: DYNAMO_TABLE,
                Key: { linkedinId: pending.linkedinId },
            }));
        } catch (err) {
            console.error('[Team] delete stub after redeem:', err.message);
        }

        console.log(`[Invite] OAuth email redemption: LinkedIn profile ${inviteLinkedinId} joined workspace of ${ownerId}`);

        const merged = await dbGetUser(inviteLinkedinId) || {};
        session.user = {
            ...merged,
            linkedinId: inviteLinkedinId,
            accessToken,
            onboardingComplete: false,
        };
        session.primaryAccountId = ownerId;
        session.activeProfileId = inviteLinkedinId;

        return true;
    }

    return false;
}

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

// ---------- REFERRAL ----------

function generateReferralCode(linkedinId) {
    return crypto.createHash('md5').update(linkedinId).digest('hex').substring(0, 8).toUpperCase();
}

app.use((req, res, next) => {
    if (req.query.ref && !req.session.referralCode) {
        req.session.referralCode = req.query.ref;
        console.log(`[Referral] Captured ref code: ${req.query.ref}`);
    }
    next();
});

app.get('/api/referral', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    const linkedinId = req.session.user.linkedinId;
    const user = await dbGetUser(linkedinId);
    let code = user?.referralCode;

    if (!code) {
        code = generateReferralCode(linkedinId);
        await dbUpdateFields(linkedinId, { referralCode: code });
    }

    const referralLink = `${MARKETING_BASE}/?ref=${code}`;
    const referrals = user?.referrals || [];
    const discountsEarned = referrals.filter(r => r.paid).length;

    res.json({
        code,
        link: referralLink,
        totalReferred: referrals.length,
        discountsEarned,
        referrals: referrals.slice(-10).map(r => ({
            name: r.name || 'Anonymous',
            date: r.date,
            paid: r.paid || false,
        })),
    });
});

// ---------- AUTH ROUTES ----------

// Step 1: Redirect user to LinkedIn authorization page
app.get('/auth/linkedin', (req, res) => {
    const redirectUri = getRedirectUri(req);
    console.log('[Auth] LinkedIn login initiated, redirectUri:', redirectUri);
    const state = crypto.randomBytes(32).toString('hex');
    req.session.oauthState = state;
    req.session.oauthRedirectUri = redirectUri;

    const params = new URLSearchParams({
        response_type: 'code',
        client_id: LINKEDIN.clientId,
        redirect_uri: redirectUri,
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

    const redirectUri = getRedirectUri(req);
    const state = crypto.randomBytes(32).toString('hex');
    req.session.oauthState = state;
    req.session.oauthRedirectUri = redirectUri;

    const params = new URLSearchParams({
        response_type: 'code',
        client_id: LINKEDIN.clientId,
        redirect_uri: redirectUri,
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
        const redirectUri = req.session.oauthRedirectUri || getRedirectUri(req);

        // Step 3: Exchange authorization code for access token
        const tokenResponse = await fetch(LINKEDIN.tokenUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                grant_type: 'authorization_code',
                code: code,
                client_id: LINKEDIN.clientId,
                client_secret: LINKEDIN.clientSecret,
                redirect_uri: redirectUri,
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

            // REPLACE MODE: signing in via "+ Add Account" replaces the
            // previously-active LinkedIn entirely. Only one posting profile
            // at a time — whichever LinkedIn signed in last. The paying
            // owner (primaryId) stays the same so billing isn't disturbed.
            const previousProfiles = primaryUser.linkedProfiles || [];
            const droppedIds = previousProfiles
                .map(p => p && p.linkedinId)
                .filter(id => id && id !== profile.sub && id !== primaryId);

            // Sever the parent link on the LinkedIn accounts being removed
            // from this workspace so they can sign in independently later.
            for (const did of droppedIds) {
                try { await dbUpdateFields(did, { parentAccountId: null }); } catch {}
            }

            const linkedProfiles = [{
                linkedinId: profile.sub,
                name: profile.name,
                email: profile.email,
                picture: profile.picture,
                addedAt: new Date().toISOString(),
            }];
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
                // Re-purpose the existing record as a child profile of this
                // workspace. Always force onboarding to run again so the
                // owner can capture writing-DNA / interests / products for
                // THIS persona — even if the LinkedIn ID had previously
                // completed onboarding under a different account.
                await dbUpdateFields(profile.sub, {
                    name: profile.name,
                    email: profile.email,
                    picture: profile.picture,
                    parentAccountId: primaryId,
                    onboardingComplete: false,
                    onboardingCompletedAt: null,
                });
            }

            req.session.primaryAccountId = primaryId;
            req.session.activeProfileId = profile.sub;
            const newProfile = await dbGetUser(profile.sub) || {};
            req.session.user = { ...newProfile, linkedinId: profile.sub, accessToken, onboardingComplete: false };

            console.log(`Profile added: ${profile.name} (${profile.email}) under primary ${primaryId} -> /onboarding`);
            return res.redirect('/onboarding');
        }

        // ── NORMAL LOGIN MODE ──
        const dbUser = await dbGetUser(profile.sub) || {};

        req.session.user = {
            ...dbUser,
            linkedinId: profile.sub,
            name: profile.name,
            email: profile.email,
            picture: profile.picture,
            accessToken: accessToken,
        };

        if (!dbUser.linkedinId) {
            const newUser = {
                linkedinId: profile.sub,
                name: profile.name,
                email: profile.email,
                picture: profile.picture,
                accessToken: accessToken,
                paid: false,
                referralCode: generateReferralCode(profile.sub),
                createdAt: new Date().toISOString(),
            };
            if (req.session.referralCode) {
                newUser.referredBy = req.session.referralCode;
                console.log(`[Referral] New user ${profile.name} referred by code: ${req.session.referralCode}`);
            }
            await dbSaveUser(newUser);
        } else {
            await dbUpdateFields(profile.sub, {
                name: profile.name,
                email: profile.email,
                picture: profile.picture,
                accessToken: accessToken,
            });
        }

        let emailInviteRedeemed = false;
        if (!req.session.inviteCode) {
            try {
                emailInviteRedeemed = await redeemPendingTeamInviteForOAuth(req.session, profile, accessToken);
            } catch (err) {
                console.error('[Invite] redeemPendingTeamInviteForOAuth:', err.message);
            }
        }

        const refreshed = await dbGetUser(profile.sub) || {};
        const actingParentId = refreshed.parentAccountId || null;
        req.session.user = {
            ...refreshed,
            linkedinId: profile.sub,
            name: profile.name,
            email: profile.email,
            picture: profile.picture,
            accessToken,
        };

        // REPLACE MODE: a primary login always pins linkedProfiles to JUST
        // this LinkedIn. If a previous "+ Add Account" had attached child
        // profiles, they get severed here so only the latest signed-in
        // LinkedIn can post. Children are unlinked (parentAccountId cleared)
        // so they can sign in independently later if they choose.
        if (!actingParentId && req.session.user.paid) {
            const previousProfiles = refreshed.linkedProfiles || [];
            const droppedIds = previousProfiles
                .map(p => p && p.linkedinId)
                .filter(id => id && id !== profile.sub);
            for (const did of droppedIds) {
                try { await dbUpdateFields(did, { parentAccountId: null }); } catch {}
            }
            const initialProfiles = [{
                linkedinId: profile.sub,
                name: profile.name,
                email: profile.email,
                picture: profile.picture,
                addedAt: refreshed.createdAt || new Date().toISOString(),
            }];
            await dbUpdateFields(profile.sub, { linkedProfiles: initialProfiles });
            req.session.user.linkedProfiles = initialProfiles;
        }

        req.session.primaryAccountId = actingParentId || profile.sub;
        req.session.activeProfileId = profile.sub;

        delete req.session.oauthState;

        // Invite link (?invite=) or email-matched pending row attaches this
        // profile to the owner's workspace before we choose onboarding vs app.
        let inviteAccepted = emailInviteRedeemed;
        if (req.session.inviteCode) {
            try {
                const r = await acceptPendingInvite(req);
                inviteAccepted = inviteAccepted || !!(r && r.accepted);
                if (r && r.accepted) {
                    const u = await dbGetUser(profile.sub) || {};
                    req.session.user = {
                        ...u,
                        linkedinId: profile.sub,
                        name: profile.name,
                        email: profile.email,
                        picture: profile.picture,
                        accessToken,
                    };
                }
            } catch (err) {
                console.error('[Invite] OAuth-callback acceptPendingInvite error:', err.message);
            }
        }

        // Re-resolve paid status: acceptPendingInvite may have just set
        // parentAccountId so we need to look that up again rather than relying
        // on the stale `parentId` captured before invite acceptance.
        const effectiveParentId = req.session.primaryAccountId && req.session.primaryAccountId !== profile.sub
            ? req.session.primaryAccountId
            : null;
        const isPaid = req.session.user.paid || (effectiveParentId && (await dbGetUser(effectiveParentId))?.paid);
        console.log(`User signed in: ${profile.name} (${profile.email}) | paid=${!!isPaid} | onboarded=${!!req.session.user.onboardingComplete} | invite=${inviteAccepted}`);

        if (req.session.user.onboardingComplete && isPaid) {
            res.redirect('/app');
        } else if (inviteAccepted) {
            // New team members always go through onboarding regardless of
            // their personal paid status, since billing is handled by the
            // inviter's account.
            res.redirect('/onboarding');
        } else if (req.session.user.onboardingComplete && !isPaid) {
            res.redirect('/upgrade');
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
    ultra:    { aiCredits: 5000, creditPeriod: 'day',  maxProfiles: 15, postsPerMonth: 5000, features: ['schedule', 'auto_post', 'advanced_analytics', 'chrome_extension', 'carousel', 'engage_engine', 'auto_repost', 'viral_library', 'team', 'white_label', 'api_access', 'ai_image_gen'] },
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

        // Process referral: reward the referrer with a 40% discount
        const paidUser = await dbGetUser(linkedinId);
        if (paidUser && paidUser.referredBy && stripe) {
            try {
                const refResult = await ddb.send(new ScanCommand({
                    TableName: DYNAMO_TABLE,
                    FilterExpression: 'referralCode = :code',
                    ExpressionAttributeValues: { ':code': paidUser.referredBy },
                }));
                const referrer = refResult.Items && refResult.Items[0];
                if (referrer) {
                    const referrals = referrer.referrals || [];
                    referrals.push({ name: req.session.user.name, date: new Date().toISOString(), paid: true, linkedinId });
                    await dbUpdateFields(referrer.linkedinId, { referrals, pendingReferralDiscount: true });

                    // Apply 40% coupon to referrer's next Stripe invoice
                    const customers = await stripe.customers.list({ email: referrer.email, limit: 1 });
                    if (customers.data.length) {
                        let coupon;
                        try {
                            coupon = await stripe.coupons.retrieve('REFERRAL40');
                        } catch {
                            coupon = await stripe.coupons.create({
                                id: 'REFERRAL40',
                                percent_off: 40,
                                duration: 'once',
                                name: 'Referral Reward - 40% Off',
                            });
                        }
                        const subs = await stripe.subscriptions.list({ customer: customers.data[0].id, status: 'active', limit: 1 });
                        if (subs.data.length) {
                            await stripe.subscriptions.update(subs.data[0].id, { coupon: coupon.id });
                            console.log(`[Referral] Applied 40% discount to referrer ${referrer.name} (${referrer.email})`);
                        }
                    }
                }
            } catch (err) {
                console.error('[Referral] Error processing referral reward:', err.message);
            }
        }
    }
    res.json({ ok: true });
});

app.post('/api/billing-portal', async (req, res) => {
    if (!stripe) return res.status(500).json({ error: 'Stripe is not configured' });
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    try {
        const email = req.session.user.email;
        const customers = await stripe.customers.list({ email, limit: 1 });

        if (!customers.data.length) {
            return res.status(404).json({ error: 'No Stripe customer found. Please contact support.' });
        }

        const session = await stripe.billingPortal.sessions.create({
            customer: customers.data[0].id,
            return_url: `${process.env.BASE_URL}/app#settings`,
        });

        res.json({ url: session.url });
    } catch (err) {
        console.error('Stripe billing portal error:', err);
        res.status(500).json({ error: 'Failed to open billing portal.' });
    }
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

/** Monthly engage-action limits by plan (display + future enforcement). */
const ENGAGE_MONTHLY_QUOTA = { free: 15, pro: 40, advanced: 75, ultra: 300 };

function countPostedThisCalendarMonth(user) {
    const queue = (user && user.queue) || [];
    const now = new Date();
    return queue.filter(q => {
        if (q.status !== 'posted') return false;
        const d = q.postedAt ? new Date(q.postedAt) : null;
        if (!d || isNaN(d.getTime())) return false;
        return d.getMonth() === now.getMonth() && d.getFullYear() === now.getFullYear();
    }).length;
}

function getEngageUsedThisMonth(user) {
    const n = user && user.engageActionsThisMonth;
    if (typeof n === 'number' && !isNaN(n)) return n;
    return 0;
}

function getNextMonthlyBoundaryMs() {
    const now = new Date();
    return new Date(now.getFullYear(), now.getMonth() + 1, 1, 0, 0, 0, 0).getTime();
}

function getNextDailyBoundaryMs() {
    const next = new Date();
    next.setDate(next.getDate() + 1);
    next.setHours(0, 0, 0, 0);
    return next.getTime();
}

function getNextAiCreditResetMs(info, primaryUser) {
    const limits = info.limits;
    const cp = limits.creditPeriod;
    if (cp === 'trial' && info.trialEndsAt) {
        return new Date(info.trialEndsAt).getTime();
    }
    if (cp === 'day') {
        return getNextDailyBoundaryMs();
    }
    if (cp === 'month') {
        return getNextMonthlyBoundaryMs();
    }
    return getNextMonthlyBoundaryMs();
}

/** One-time bonus pool shown in green (missions, promos). Missions grant 200 via reduced usage — this is the display figure. */
const MISSIONS_BONUS_CREDITS = 200;

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

    const tier = info.tier;
    const engageCap = ENGAGE_MONTHLY_QUOTA[tier] ?? ENGAGE_MONTHLY_QUOTA.pro;
    const engageUsed = getEngageUsedThisMonth(primaryUser);
    const engageRem = Math.max(0, engageCap - engageUsed);

    const postsCap = info.limits.postsPerMonth;
    const postsUsed = postsCap != null ? countPostedThisCalendarMonth(primaryUser) : null;
    const postsRem = postsCap != null ? Math.max(0, postsCap - postsUsed) : null;

    const bonusOneTime = (primaryUser.missionsCompleted ? MISSIONS_BONUS_CREDITS : 0)
        + (typeof primaryUser.promoBonusCredits === 'number' ? primaryUser.promoBonusCredits : 0);

    const nextAiResetMs = getNextAiCreditResetMs(info, primaryUser);
    const nextEngagePostResetMs = getNextMonthlyBoundaryMs();

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
        bonusCreditsOneTime: bonusOneTime,
        missionsCompleted: !!primaryUser.missionsCompleted,
        engageQuota: engageCap,
        engageUsed,
        engageRemaining: engageRem,
        postsUsed,
        postsRemaining: postsRem,
        nextAiCreditResetAt: new Date(nextAiResetMs).toISOString(),
        nextQuotaResetAt: new Date(nextEngagePostResetMs).toISOString(),
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

function plainTextFromOnboardingPostHtml(html) {
    return String(html || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function likedOnboardingBodiesFromWritingDNA(writingDNA) {
    const rows = Array.isArray(writingDNA) ? writingDNA : [];
    return rows.map((p) => {
        const ref = POSTS_REF[p.index];
        return ref ? plainTextFromOnboardingPostHtml(ref.body) : '';
    }).filter(Boolean);
}

/** One-time onboarding: distill liked sample posts into a reusable style directive for downstream post AI */
async function synthesizeOnboardingAiWritingStylePrompt(apiKey, writingDNA) {
    const bodies = likedOnboardingBodiesFromWritingDNA(writingDNA);
    if (!bodies.length) return '';

    const postsBlock = bodies.map((b, i) => `---\nSample ${i + 1}:\n${b}`).join('\n');
    try {
        const aiRes = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
            body: JSON.stringify({
                model: 'gpt-4o-mini',
                temperature: 0.35,
                max_tokens: 500,
                messages: [
                    {
                        role: 'system',
                        content: [
                            'You analyze LinkedIn-style posts a new creator liked during onboarding.',
                            'Produce a compact style guide another AI must follow when drafting LinkedIn posts in this person\'s voice.',
                            '',
                            'Output:',
                            '- Short bullets and tight sentences only (stay under ~1800 characters).',
                            '- Cover tone, rhythm, paragraphing, hooks/closings, lists vs prose, vulnerability, humor, questions, framing, formatting habits (line breaks), and thematic leanings.',
                            '- Generalize patterns only — do NOT copy recognizable phrases from the samples.',
                            '- Do not say "onboarding", "samples", or "the user liked".',
                            '- Output ONLY the style guide text.',
                        ].join('\n'),
                    },
                    { role: 'user', content: `Posts they preferred (plain text):\n\n${postsBlock}` },
                ],
            }),
        });
        const data = await aiRes.json();
        let text = (data.choices?.[0]?.message?.content || '').trim();
        if (text.length > 2500) text = text.slice(0, 2497) + '...';
        return text;
    } catch (err) {
        console.error('[onboarding] synthesize aiWritingStylePrompt failed:', err.message);
        return '';
    }
}

app.post('/api/onboarding/writing-dna', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const likedPosts = Array.isArray(req.body.likedPosts) ? req.body.likedPosts : [];
    req.session.user.writingDNA = likedPosts;

    const allTags = likedPosts.flatMap(p => p.tags || []);
    const tagCounts = {};
    allTags.forEach(tag => { tagCounts[tag] = (tagCounts[tag] || 0) + 1; });
    req.session.user.writingProfile = tagCounts;

    let aiWritingStylePrompt = '';
    const apiKey = process.env.OPENAI_API_KEY;
    if (likedPosts.length && apiKey && apiKey !== 'sk-your-openai-api-key-here') {
        aiWritingStylePrompt = await synthesizeOnboardingAiWritingStylePrompt(apiKey, likedPosts);
    }

    req.session.user.aiWritingStylePrompt = aiWritingStylePrompt;

    await dbUpdateFields(req.session.user.linkedinId, {
        writingDNA: req.session.user.writingDNA,
        writingProfile: tagCounts,
        aiWritingStylePrompt,
    });

    console.log(`Writing DNA saved for ${req.session.user.name}:`, tagCounts, aiWritingStylePrompt ? `(style prompt ${aiWritingStylePrompt.length} chars)` : '(no style prompt)');
    res.json({ success: true, profile: tagCounts, aiWritingStylePrompt: !!aiWritingStylePrompt });
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

app.get('/api/onboarding/writing-dna', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    const db = await dbGetUser(req.session.user.linkedinId) || {};
    res.json({
        writingDNA: db.writingDNA || req.session.user.writingDNA || [],
        writingProfile: db.writingProfile || req.session.user.writingProfile || {},
        hasAiWritingStyle: !!(db.aiWritingStylePrompt && String(db.aiWritingStylePrompt).trim()),
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

// ---------- PUBLISH WITH IMAGE ----------

const AI_STUDIO_MAX_FETCH_IMAGE_BYTES = 10 * 1024 * 1024;

/** Used by /api/publish-ai-studio-image to avoid SSRF while allowing OpenAI-hosted results. */
function isAllowedAiStudioImageUrl(dbUser, urlStr) {
    const s = String(urlStr || '').trim();
    if (!s) return false;
    const list = Array.isArray(dbUser?.aiGeneratedImages) ? dbUser.aiGeneratedImages : [];
    if (list.some((it) => it && typeof it.url === 'string' && it.url.trim() === s)) return true;

    let u;
    try {
        u = new URL(s);
    } catch {
        return false;
    }
    if (u.protocol !== 'https:') return false;
    const h = u.hostname.toLowerCase();
    if (/(^|\.)openai\.com$/i.test(h)) return true;
    if (/(^|\.)blob\.core\.windows\.net$/i.test(h)) return true;
    if (/(^|\.)azureedge\.net$/i.test(h)) return true;
    if (/oaidalle|openaiusercontent/i.test(h)) return true;
    return false;
}

/** Register LinkedIn asset, upload bytes, publish single-image ugcPost. */
async function publishLinkedInImageFromBuffer(accessToken, linkedinId, userDisplayName, text, audience, imageBuffer, mimetype) {
    const personUrn = `urn:li:person:${linkedinId}`;
    const visibility = audience === 'connections' ? 'CONNECTIONS' : 'PUBLIC';
    const rawMime = String(mimetype || '').toLowerCase().split(';')[0].trim();
    const contentType = /^image\/(jpeg|jpg|png|gif|webp)$/.test(rawMime) ? rawMime.replace('image/jpg', 'image/jpeg') : 'image/png';

    const registerRes = await fetch('https://api.linkedin.com/v2/assets?action=registerUpload', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${accessToken}`,
        },
        body: JSON.stringify({
            registerUploadRequest: {
                recipes: ['urn:li:digitalmediaRecipe:feedshare-image'],
                owner: personUrn,
                serviceRelationships: [{ relationshipType: 'OWNER', identifier: 'urn:li:userGeneratedContent' }],
            },
        }),
    });

    if (!registerRes.ok) {
        const errText = await registerRes.text();
        console.error('LinkedIn register image upload failed:', errText);
        return { ok: false, status: 500, error: 'Failed to register image upload with LinkedIn.' };
    }

    const registerData = await registerRes.json();
    const uploadUrl = registerData.value?.uploadMechanism?.['com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest']?.uploadUrl;
    const asset = registerData.value?.asset;

    if (!uploadUrl || !asset) {
        return { ok: false, status: 500, error: 'LinkedIn upload registration returned invalid data.' };
    }

    const uploadRes = await fetch(uploadUrl, {
        method: 'PUT',
        headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': contentType,
        },
        body: imageBuffer,
    });

    if (!uploadRes.ok) {
        console.error('LinkedIn image upload failed:', uploadRes.status);
        return { ok: false, status: 500, error: 'Failed to upload image to LinkedIn.' };
    }

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
                    shareMediaCategory: 'IMAGE',
                    media: [{
                        status: 'READY',
                        media: asset,
                    }],
                },
            },
            visibility: {
                'com.linkedin.ugc.MemberNetworkVisibility': visibility,
            },
        }),
    });

    if (postRes.ok) {
        const data = await postRes.json();
        const postId = data.id;
        const postUrl = postId ? `https://www.linkedin.com/feed/update/${postId}/` : 'https://www.linkedin.com/feed/';
        console.log(`Image post published to LinkedIn by ${userDisplayName}: ${postId}`);
        return { ok: true, postId, postUrl };
    }

    const errData = await postRes.text();
    console.error('LinkedIn image post failed:', postRes.status, errData);
    if (postRes.status === 401) {
        return { ok: false, status: 401, error: 'LinkedIn access token expired. Please sign out and sign in again.', linkedin401: true };
    }
    return { ok: false, status: postRes.status >= 400 && postRes.status < 600 ? postRes.status : 500, error: 'Failed to publish image post to LinkedIn.' };
}

app.post('/api/publish-with-image', upload.single('image'), async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const { text, audience } = req.body;
    if (!text || !text.trim()) {
        return res.status(400).json({ error: 'Post text is required' });
    }
    if (!req.file) {
        return res.status(400).json({ error: 'Image file is required' });
    }

    const accessToken = req.session.user.accessToken;
    if (!accessToken) {
        return res.status(401).json({ error: 'No LinkedIn access token. Please sign in again.' });
    }

    const linkedinId = req.session.user.linkedinId;

    try {
        const out = await publishLinkedInImageFromBuffer(
            accessToken,
            linkedinId,
            req.session.user.name,
            text,
            audience,
            req.file.buffer,
            req.file.mimetype || 'image/png',
        );
        if (out.ok) {
            return res.json({ success: true, postId: out.postId, postUrl: out.postUrl });
        }
        if (out.linkedin401 || out.status === 401) {
            return res.status(401).json({ error: out.error });
        }
        return res.status(out.status || 500).json({ error: out.error });
    } catch (err) {
        console.error('LinkedIn image publish error:', err);
        res.status(500).json({ error: 'Could not connect to LinkedIn. Please try again.' });
    }
});

// ---------- AI & QUEUE ----------

async function hydrateSessionUserFromDb(sessionUser) {
    if (!sessionUser || !sessionUser.linkedinId) return sessionUser;
    const dbUser = await dbGetUser(sessionUser.linkedinId);
    if (!dbUser) return sessionUser;
    return { ...sessionUser, ...dbUser };
}

/** Prefer GPT-distilled onboarding style; fall back to raw liked post excerpts */
function onboardingStyleInjectionForAi(user, rawSnippetLimit = 5) {
    const condensed = (user.aiWritingStylePrompt || '').trim();
    if (condensed) {
        return `\n\nONBOARDING WRITING STYLE (match this author's voice and structure):\n${condensed}`;
    }
    const raw = likedOnboardingBodiesFromWritingDNA(user.writingDNA || []).slice(0, rawSnippetLimit);
    if (!raw.length) return '';
    return `\n\nWRITING DNA: The user liked these sample posts during onboarding. Use them as reference for the user's preferred writing style, tone, structure, and format:\n${raw.map((b, i) => `${i + 1}. "${b}"`).join('\n')}`;
}

/** Context → products use `title`; onboarding may use `name`. URL-only rows are still valid. */
function userProductsForPrompt(raw) {
    const list = Array.isArray(raw) ? raw : [];
    return list
        .map(p => {
            if (!p || typeof p !== 'object') return null;
            const url = typeof p.url === 'string' ? p.url.trim() : '';
            const name = String(p.name || p.title || '').trim();
            const description = typeof p.description === 'string' ? p.description.trim() : '';
            if (!url && !name && !description) return null;
            return { url, name, description };
        })
        .filter(Boolean);
}

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

    const user = await hydrateSessionUserFromDb(req.session.user);
    const writingProfile = user.writingProfile || {};
    const topTags = Object.entries(writingProfile).sort((a, b) => b[1] - a[1]).map(e => e[0]).slice(0, 5);
    const aboutYou = user.aboutYou || '';

    const creators = (user.favoriteCreators || []).map(c => c.name).filter(Boolean);
    const onboardingStyle = onboardingStyleInjectionForAi(user, 5);

    const customRules = user.customRules || '';
    const interests = (user.interests || []).join(', ');
    const products = userProductsForPrompt(user.products);
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
    if (onboardingStyle) systemPrompt += onboardingStyle;
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

async function resolveUserFromReq(req) {
    if (req.session && req.session.user) return req.session.user;
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith('Bearer ')) {
        const token = authHeader.slice(7);
        const secret = process.env.SESSION_SECRET || 'fallback';
        if (!global._tokenCache) global._tokenCache = {};
        if (global._tokenCache[token]) {
            return await dbGetUser(global._tokenCache[token]);
        }
        try {
            const result = await ddb.send(new ScanCommand({
                TableName: DYNAMO_TABLE,
                FilterExpression: 'extensionToken = :token',
                ExpressionAttributeValues: { ':token': token },
            }));
            if (result.Items && result.Items[0]) {
                global._tokenCache[token] = result.Items[0].linkedinId;
                return result.Items[0];
            }
        } catch (err) {
            console.error('[resolveUser] DB scan error:', err.message);
        }
    }
    return null;
}

app.post('/api/ai/write', async (req, res) => {
    let user = await resolveUserFromReq(req);
    if (!user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    user = await hydrateSessionUserFromDb(user);

    const fakeSession = { user };
    const credits = await getCreditsForSession(fakeSession);
    if (credits.remaining < 1) {
        return res.json({ error: `AI credit limit reached (${credits.limits.aiCredits}/${credits.limits.creditPeriod}). Upgrade your plan for more credits.` });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ error: 'OpenAI API key not configured' });
    }

    const { tone, prompt: extPrompt } = req.body;
    const writingProfile = user.writingProfile || {};
    const topTags = Object.entries(writingProfile).sort((a, b) => b[1] - a[1]).map(e => e[0]).slice(0, 5);
    const aboutYou = user.aboutYou || '';

    const creators = (user.favoriteCreators || []).map(c => c.name).filter(Boolean);
    const onboardingStyle = onboardingStyleInjectionForAi(user, 3);

    const products = userProductsForPrompt(user.products);
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
    if (onboardingStyle) systemContent += onboardingStyle;
    if (products.length) systemContent += `\n\nPRODUCT PROMOTION: Naturally weave in the user's product/service where relevant: ${productsList}`;

    const topicHint = extPrompt ? `\nTopic/prompt from user: ${extPrompt}` : '';
    const userPrompt = `Write a single LinkedIn post for me.${charLimit ? `\nHARD LIMIT: The post MUST be under ${charLimit} characters. Keep it extremely short and concise.` : ''}${topicHint}
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
        await consumeCredit(fakeSession, 1);
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

    const user = await hydrateSessionUserFromDb(req.session.user);
    const aboutYou = user.aboutYou || '';
    const products = userProductsForPrompt(user.products);

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
    const onboardingStyle = onboardingStyleInjectionForAi(user, 5);

    let systemPrompt = `You are a LinkedIn content strategist specializing in product marketing. Generate 3 unique LinkedIn posts that promote the user's products/services in a natural, value-driven way. Each post should feel like genuine advice or a story, not a hard sell. Return ONLY a JSON array of 3 strings.`;
    if (creators.length) systemPrompt += `\n\nWRITING STYLE: Mimic the style of: ${creators.join(', ')}.`;
    if (onboardingStyle) systemPrompt += onboardingStyle;
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
    const user = await resolveUserFromReq(req);
    if (!user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    const fakeSession = { user };
    const credits = await getCreditsForSession(fakeSession);
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
        await consumeCredit(fakeSession, 1);
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
        const now = new Date();
        const item = {
            text,
            status: status || 'draft',
            date: now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }),
        };
        if (scheduledFor) item.scheduledFor = scheduledFor;
        if (status === 'posted') {
            item.postedAt = now.toISOString();
            item.postedAtDay = now.getDay();
            item.postedAtHour = now.getHours();
        }
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

async function hasFeature(session, feature) {
    const primaryId = getPrimaryAccountId(session);
    let tier = session.user?.planTier || 'pro';

    if (primaryId) {
        const primary = await dbGetUser(primaryId);
        if (primary && primary.planTier) tier = primary.planTier;
    }

    const limits = PLAN_LIMITS[tier] || PLAN_LIMITS.pro;
    return (limits.features || []).includes(feature);
}

app.post('/api/ai/generate-carousel', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    if (!(await hasFeature(req.session, 'carousel'))) {
        return res.status(403).json({ error: 'Carousel is available on Advanced and Ultra plans. Upgrade to access this feature.' });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ error: 'OpenAI API key not configured' });
    }

    const { topic, slideCount = 8, style: styleRaw = 'professional' } = req.body;
    if (!topic) return res.status(400).json({ error: 'Topic is required' });

    const user = await hydrateSessionUserFromDb(req.session.user);
    const aboutYou = user.aboutYou || '';
    const customRules = user.customRules || '';

    const styleDescriptions = {
        professional: 'Clean, corporate style with data-driven insights and clear headings.',
        bold: 'Eye-catching, bold statements with high contrast and punchy one-liners.',
        minimal: 'Minimalist design approach with short text, lots of whitespace, one idea per slide.',
        colorful: 'Warm, playful, storytelling voice: vivid hooks, concrete imagery, metaphors, short punchy lines. Include 1–2 tasteful emoji on cover and/or ONE content slide where it fits LinkedIn norms (never every line). Avoid generic corporate filler; sound human and memorable.',
        image_forward: 'Visual-first: every slide should read like a storyboard beat—concrete scenes, objects, and metaphors a designer could illustrate with stock photos, icons, or diagrams. Titles and bullets name what you would *see* on screen.',
        cartoon_colors: 'Cartoon energy: playful, saturated “Saturday morning” vibe in the language—bouncy verbs, friendly micro-copy, expressive contrast, light humor where it fits. Still professional enough for LinkedIn.',
        story_arc: 'Strong narrative arc: hook on slide 2, rising insight, subtle tension, payoff before the CTA. Slide titles feel like chapter beats.',
        data_dense: 'Analytical: numbers, frameworks, “before/after”, short punchy stats-style claims (no fake data—use qualitative rigor if no figures).',
        editorial: 'Editorial / thought-leadership: contrarian hooks, crisp thesis lines, magazine-like section heads.',
        pastel_soft: 'Soft and approachable: gentle language, reassuring tone, “coach in your corner” energy—avoid harsh jargon.',
    };

    const count = Math.min(Math.max(parseInt(slideCount, 10) || 8, 4), 15);
    const creditsPerSlide = 2;
    const slideCreditCost = count * creditsPerSlide;

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < slideCreditCost) {
        return res.json({
            error: `Need at least ${slideCreditCost} AI credits for ${count} slides (2 credits per slide; ${credits.remaining} remaining). Upgrade your plan or pick fewer slides.`,
        });
    }

    const sk = typeof styleRaw === 'string' ? styleRaw.trim() : 'professional';
    const styleGuide = styleDescriptions[sk] || styleDescriptions.professional;
    const onboardCarouselStyle = onboardingStyleInjectionForAi(user, 5);

    const systemPrompt = `You are a LinkedIn carousel content expert. Create a ${count}-slide carousel about the given topic.

The slide copy must clearly reflect the Style below — do not revert to bland corporate wording when the style asks for bold, playful, editorial, or story-driven tones.

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
${aboutYou ? `\nAbout the author: ${aboutYou}` : ''}${onboardCarouselStyle || ''}

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
                max_tokens: Math.min(4096, 700 + count * 190),
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

        if (slides.length !== count) {
            return res.json({
                error: `The model returned ${slides.length} slides instead of ${count}. Try again, or pick a different slide count or topic.`,
            });
        }

        await consumeCredit(req.session, slideCreditCost);
        res.json({ slides, creditsUsed: slideCreditCost });
    } catch (err) {
        console.error('Carousel generation error:', err);
        res.json({ error: 'Failed to generate carousel.' });
    }
});

/** Credits per AI image (OpenAI image generation). Ultra-only feature. */
const AI_IMAGE_CREDIT_COST = 10;
/** DALL·E models were retired on the public API — use GPT Image models instead. Override via OPENAI_IMAGE_MODEL. */
const AI_IMAGE_MODEL = process.env.OPENAI_IMAGE_MODEL || 'gpt-image-1';
/** e.g. 1024x1024, 1024x1536 (portrait, more vertical room for text), 1536x1024 (landscape). */
const AI_IMAGE_SIZE = process.env.OPENAI_IMAGE_SIZE || '1024x1536';

const AI_IMAGE_STYLE_FRAGMENTS = {
    comic: 'comic book art, bold ink outlines, halftone or cel shading, vibrant colors',
    cartoon: 'cartoon illustration, rounded shapes, expressive simple forms, cheerful palette',
    photorealistic: 'photorealistic, highly detailed, natural lighting, professional photo look',
    minimal: 'minimal flat vector illustration, clean geometric shapes, plenty of whitespace, limited palette',
    watercolor: 'watercolor painting aesthetic, soft bleeding edges, paper texture feel',
    '3d': '3D CGI render, soft studio lighting, modern product-visual style',
    sketch: 'pencil sketch or ink drawing, monochrome or light wash, hand-drawn feel',
};

app.get('/api/ai/generated-images', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'ai_image_gen'))) {
        return res.status(403).json({ error: 'Ultra plan required.', items: [] });
    }
    const user = await dbGetUser(req.session.user.linkedinId);
    const items = Array.isArray(user?.aiGeneratedImages) ? [...user.aiGeneratedImages].reverse() : [];
    res.json({ items: items.slice(0, 30) });
});

app.post('/api/ai/generate-image', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'ai_image_gen'))) {
        return res.status(403).json({ error: 'AI Image Studio is available on Ultra only. Upgrade to unlock this feature.' });
    }

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < AI_IMAGE_CREDIT_COST) {
        return res.json({
            error: `Need at least ${AI_IMAGE_CREDIT_COST} AI credits (${credits.remaining} remaining). Image generation runs in one request on our servers — try again after your quota resets.`,
        });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') {
        return res.json({ error: 'OpenAI API key not configured.' });
    }

    const body = req.body || {};
    const topic = String(body.topic || '').trim().slice(0, 3500);
    const styleRaw = String(body.style || 'minimal').toLowerCase();
    if (!topic) return res.status(400).json({ error: 'Describe the subject or scene for your image.' });

    const styleKey = Object.prototype.hasOwnProperty.call(AI_IMAGE_STYLE_FRAGMENTS, styleRaw)
        ? styleRaw
        : 'minimal';
    const styleFragment = AI_IMAGE_STYLE_FRAGMENTS[styleKey];

    const prompt = `${topic}\n\nStyle: ${styleFragment}.\n` +
        'Professional, suitable as LinkedIn visuals. Avoid clutter, watermark text, logos, or legible copyrighted characters unless the user explicitly asked.\n' +
        'Layout: Leave a clear safety margin inside the frame — keep all visible text, faces, and key elements at least 8% away from every edge so nothing looks cropped at the borders.';

    try {
        const response = await fetch('https://api.openai.com/v1/images/generations', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
            body: JSON.stringify({
                model: AI_IMAGE_MODEL,
                prompt,
                n: 1,
                size: AI_IMAGE_SIZE,
                quality: 'medium',
                output_format: 'jpeg',
                output_compression: 85,
            }),
        });
        const data = await response.json();
        const errMsg = data.error?.message || (typeof data.error === 'string' ? data.error : null);

        const first = data.data?.[0];
        let imageUrl = first?.url ? String(first.url).trim() : '';
        const b64 = first?.b64_json;
        if (!imageUrl && b64 && typeof b64 === 'string') {
            imageUrl = `data:image/jpeg;base64,${b64}`;
        }

        if (!response.ok || !imageUrl) {
            console.error('OpenAI images error:', response.status, errMsg || data);
            return res.json({
                error: errMsg || 'Image generation failed. Try a different description or simplify the topic.',
            });
        }

        const revisedPrompt = first?.revised_prompt || null;

        await consumeCredit(req.session, AI_IMAGE_CREDIT_COST);

        const linkedinId = req.session.user.linkedinId;
        const fresh = await dbGetUser(linkedinId);
        let list = Array.isArray(fresh?.aiGeneratedImages) ? [...fresh.aiGeneratedImages] : [];
        list.push({
            url: imageUrl,
            revisedPrompt,
            topic,
            style: styleKey,
            createdAt: new Date().toISOString(),
        });
        if (list.length > 40) list = list.slice(-40);
        await dbUpdateFields(linkedinId, { aiGeneratedImages: list });

        res.json({
            url: imageUrl,
            revisedPrompt,
            creditsConsumed: AI_IMAGE_CREDIT_COST,
            style: styleKey,
            historyPreview: [...list].reverse().slice(0, 12),
        });
    } catch (err) {
        console.error('AI image generation error:', err);
        res.json({ error: 'Could not generate image. Please try again in a moment.' });
    }
});

app.post('/api/publish-ai-studio-image', async (req, res) => {
    if (!req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    if (!(await hasFeature(req.session, 'ai_image_gen'))) {
        return res.status(403).json({ error: 'AI Image Studio is available on Ultra only.' });
    }

    const { text, audience, imageUrl } = req.body || {};
    if (!text || !String(text).trim()) {
        return res.status(400).json({ error: 'Post text is required' });
    }
    const normalizedUrl = String(imageUrl || '').trim();
    if (!normalizedUrl) {
        return res.status(400).json({ error: 'Image URL is required' });
    }

    const accessToken = req.session.user.accessToken;
    if (!accessToken) {
        return res.status(401).json({ error: 'No LinkedIn access token. Please sign in again.' });
    }

    const linkedinId = req.session.user.linkedinId;
    const dbUser = await dbGetUser(linkedinId);
    if (!isAllowedAiStudioImageUrl(dbUser, normalizedUrl)) {
        return res.status(400).json({
            error: 'This image URL is not allowed. Use an image generated in AI Image Studio, or generate a new one.',
        });
    }

    try {
        const imgRes = await fetch(normalizedUrl, {
            redirect: 'follow',
            headers: { 'User-Agent': 'SuperLinkedIn/1.0' },
        });
        if (!imgRes.ok) {
            console.error('[AI Studio publish] image fetch failed:', imgRes.status);
            return res.status(502).json({
                error: 'Could not download the image — temporary links expire. Regenerate the image and post again.',
            });
        }
        const lenHdr = imgRes.headers.get('content-length');
        if (lenHdr && Number(lenHdr) > AI_STUDIO_MAX_FETCH_IMAGE_BYTES) {
            return res.status(413).json({ error: 'Image file is too large.' });
        }
        const buf = Buffer.from(await imgRes.arrayBuffer());
        if (!buf.length || buf.length > AI_STUDIO_MAX_FETCH_IMAGE_BYTES) {
            return res.status(413).json({ error: 'Image file is too large or empty.' });
        }

        let mime = (imgRes.headers.get('content-type') || '').split(';')[0].trim().toLowerCase();
        if (!/^image\/(jpeg|jpg|png|gif|webp)$/.test(mime)) {
            mime = 'image/png';
        }

        const out = await publishLinkedInImageFromBuffer(
            accessToken,
            linkedinId,
            req.session.user.name,
            String(text),
            audience,
            buf,
            mime,
        );

        if (out.ok) {
            return res.json({ success: true, postId: out.postId, postUrl: out.postUrl });
        }
        if (out.linkedin401 || out.status === 401) {
            return res.status(401).json({ error: out.error });
        }
        return res.status(out.status || 500).json({ error: out.error || 'Failed to publish.' });
    } catch (err) {
        console.error('[AI Studio publish]', err);
        res.status(500).json({ error: 'Could not publish post. Try again.' });
    }
});

/** Light page backgrounds keyed by Carousel Creator visual style (exports match preview mood). */
function carouselPdfPalettes(themeKey) {
    const t = (themeKey || 'professional').trim();
    const body = ({
        colorful: '#FFF8F0',
        bold: '#EEF2F6',
        minimal: '#FAFAFA',
        professional: '#FFFFFF',
        pastel_soft: '#FAF5FF',
        cartoon_colors: '#FFF7ED',
        image_forward: '#F0F9FF',
        story_arc: '#FDF4FF',
        data_dense: '#F8FAFC',
        editorial: '#FAFAF9',
    })[t];
    const coverTint = ({
        colorful: '#FFF4E6',
        bold: '#E8EDF5',
        minimal: '#FBFBFB',
        professional: '#FFFFFF',
        pastel_soft: '#F5EFFA',
        cartoon_colors: '#FFEDD5',
        image_forward: '#E8F4FC',
        story_arc: '#FAF5FF',
        data_dense: '#F1F5F9',
        editorial: '#F5F5F4',
    })[t];
    return { bodyBg: body || '#FFFFFF', coverBg: coverTint || '#FFFFFF' };
}

function renderCarouselPDF(slides, brandColor, title, userName, slideTheme) {
    const WIDTH = 1080;
    const HEIGHT = 1080;
    const MARGIN = 100;
    const CW = WIDTH - MARGIN * 2;
    const color = brandColor || '#0A66C2';
    const pdfPal = carouselPdfPalettes(slideTheme);

    const doc = new PDFDocument({ size: [WIDTH, HEIGHT], margin: 0, autoFirstPage: false });

    // PDFKit's heightOfString uses the currently active font size on the
    // document. The `fontSize` option in the options bag is ignored, so we
    // MUST call doc.fontSize(N) before measuring, otherwise heights come back
    // computed at 12pt (the default) and our layout offsets all collapse,
    // stacking the title / body / name on top of each other.
    function measureH(text, fontSize, opts) {
        if (!text) return 0;
        doc.fontSize(fontSize);
        return doc.heightOfString(String(text), opts || { width: CW });
    }

    function measureContentHeight(slide) {
        let h = 0;
        if (slide.title) {
            h += measureH(slide.title, 56, { width: CW, lineGap: 10 }) + 40;
            h += 8 + 40; // accent bar + spacing
        }
        if (slide.body) {
            h += measureH(slide.body, 32, { width: CW, lineGap: 12 }) + 36;
        }
        if (slide.bulletPoints && slide.bulletPoints.length > 0) {
            slide.bulletPoints.forEach(bp => {
                h += measureH(bp, 30, { width: CW - 50, lineGap: 10 }) + 24;
            });
        }
        return h;
    }

    slides.forEach((slide, i) => {
        doc.addPage({ size: [WIDTH, HEIGHT], margin: 0 });

        const isFirst = i === 0;
        const isLast = i === slides.length - 1;

        if (isFirst) {
            doc.rect(0, 0, WIDTH, HEIGHT).fill(pdfPal.coverBg);
            doc.rect(0, 0, WIDTH, 20).fill(color);
            doc.rect(0, HEIGHT - 240, WIDTH, 240).fill(color);

            const titleText = slide.title || title || '';
            const bodyText = slide.body || '';
            const titleH = measureH(titleText, 68, { width: CW, align: 'center', lineGap: 12 });
            const bodyH = bodyText ? measureH(bodyText, 34, { width: CW, align: 'center', lineGap: 10 }) : 0;
            const totalH = titleH + (bodyText ? 40 + bodyH : 0);
            const startY = Math.max(120, (HEIGHT - 240 - totalH) / 2);

            doc.fontSize(68).fillColor(color)
               .text(titleText, MARGIN, startY, { width: CW, align: 'center', lineGap: 12 });
            if (bodyText) {
                doc.fontSize(34).fillColor('#555555')
                   .text(bodyText, MARGIN, startY + titleH + 40, { width: CW, align: 'center', lineGap: 10 });
            }
            if (userName) {
                doc.fontSize(28).fillColor('#FFFFFF')
                   .text(userName, MARGIN, HEIGHT - 170, { width: CW, align: 'center' });
            }
            doc.fontSize(22).fillColor('#FFFFFF').opacity(0.8)
               .text('Swipe to read more  >', MARGIN, HEIGHT - 80, { width: CW, align: 'center' });
            doc.opacity(1);
        } else if (isLast) {
            doc.rect(0, 0, WIDTH, HEIGHT).fill(color);

            const titleText = slide.title || 'Thanks for reading!';
            const bodyText = slide.body || 'Follow for more content like this.';
            const titleH = measureH(titleText, 64, { width: CW, align: 'center', lineGap: 12 });
            const bodyH = measureH(bodyText, 34, { width: CW, align: 'center', lineGap: 10 });
            const nameH = userName ? 60 : 0;
            const titleToBodyGap = 50;
            const bodyToNameGap = 60;
            const totalH = titleH + titleToBodyGap + bodyH + (userName ? bodyToNameGap + nameH : 0);
            const startY = Math.max(80, (HEIGHT - totalH) / 2);

            doc.fontSize(64).fillColor('#FFFFFF')
               .text(titleText, MARGIN, startY, { width: CW, align: 'center', lineGap: 12 });
            doc.fontSize(34).fillColor('#FFFFFF').opacity(0.85)
               .text(bodyText, MARGIN, startY + titleH + titleToBodyGap, { width: CW, align: 'center', lineGap: 10 });
            doc.opacity(1);
            if (userName) {
                doc.fontSize(30).fillColor('#FFFFFF')
                   .text(userName, MARGIN, startY + titleH + titleToBodyGap + bodyH + bodyToNameGap, { width: CW, align: 'center' });
            }
        } else {
            doc.rect(0, 0, WIDTH, HEIGHT).fill(pdfPal.bodyBg);
            doc.rect(0, 0, WIDTH, 20).fill(color);

            doc.fontSize(20).fillColor('#AAAAAA')
               .text(`${i + 1} / ${slides.length}`, MARGIN, HEIGHT - 60, { width: CW, align: 'right' });

            const contentH = measureContentHeight(slide);
            let y = Math.max(80, (HEIGHT - contentH) / 2);

            if (slide.title) {
                const titleH = measureH(slide.title, 56, { width: CW, lineGap: 10 });
                doc.fontSize(56).fillColor(color)
                   .text(slide.title, MARGIN, y, { width: CW, lineGap: 10 });
                y += titleH + 40;

                doc.rect(MARGIN, y, 80, 6).fill(color);
                y += 8 + 40;
            }

            if (slide.body) {
                const bodyH = measureH(slide.body, 32, { width: CW, lineGap: 12 });
                doc.fontSize(32).fillColor('#333333')
                   .text(slide.body, MARGIN, y, { width: CW, lineGap: 12 });
                y += bodyH + 36;
            }

            if (slide.bulletPoints && slide.bulletPoints.length > 0) {
                slide.bulletPoints.forEach(bp => {
                    const bpH = measureH(bp, 30, { width: CW - 50, lineGap: 10 });
                    doc.save();
                    doc.circle(MARGIN + 10, y + 14, 7).fill(color);
                    doc.restore();
                    doc.fontSize(30).fillColor('#444444')
                       .text(bp, MARGIN + 50, y, { width: CW - 50, lineGap: 10 });
                    y += bpH + 24;
                });
            }
        }
    });

    doc.end();
    return doc;
}

app.post('/api/carousel/generate-pdf', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    if (!(await hasFeature(req.session, 'carousel'))) {
        return res.status(403).json({ error: 'Carousel is available on Advanced and Ultra plans.' });
    }

    const { slides, brandColor, title, slideTheme } = req.body;
    if (!slides || !Array.isArray(slides) || !slides.length) {
        return res.status(400).json({ error: 'Slides data is required' });
    }

    const userName = req.session.user.name || '';

    try {
        const doc = renderCarouselPDF(slides, brandColor, title, userName, slideTheme);
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

    if (!(await hasFeature(req.session, 'carousel'))) {
        return res.status(403).json({ error: 'Carousel is available on Advanced and Ultra plans.' });
    }

    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 1) {
        return res.json({ error: `AI credit limit reached.` });
    }

    const { slides, brandColor, title, text, audience, slideTheme } = req.body;
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
        const doc = renderCarouselPDF(slides, brandColor, title, userName, slideTheme);
        const chunks = [];
        const pdfBuffer = await new Promise((resolve, reject) => {
            doc.on('data', c => chunks.push(c));
            doc.on('end', () => resolve(Buffer.concat(chunks)));
            doc.on('error', reject);
        });

        console.log(`[Carousel] PDF generated, size: ${pdfBuffer.length} bytes. Attempting upload for ${linkedinId}`);

        let documentUrn = null;
        const pdfPostTitle = carouselPdfFilenameTitle(title);

        // --- Method 1: REST Documents API (PDF → urn:li:document:…) ---
        try {
            const initRes = await fetch('https://api.linkedin.com/rest/documents?action=initializeUpload', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${accessToken}`,
                    ...linkedinRestHeaders(),
                },
                body: JSON.stringify({
                    initializeUploadRequest: {
                        owner: personUrn,
                    },
                }),
            });

            if (initRes.ok) {
                const initData = await initRes.json();
                const uploadUrl = initData.value?.uploadUrl;
                documentUrn = initData.value?.document;

                if (uploadUrl && documentUrn) {
                    const upRes = await fetch(uploadUrl, {
                        method: 'PUT',
                        headers: {
                            'Authorization': `Bearer ${accessToken}`,
                            'Content-Type': 'application/pdf',
                        },
                        body: pdfBuffer,
                    });

                    if (!upRes.ok) {
                        console.error('[Carousel] REST Documents upload PUT failed:', upRes.status, await upRes.text());
                        documentUrn = null;
                    } else {
                        console.log('[Carousel] REST Documents upload succeeded, URN:', documentUrn);
                    }
                } else {
                    console.error('[Carousel] REST Documents init response missing uploadUrl or document:', JSON.stringify(initData));
                    documentUrn = null;
                }
            } else {
                const errBody = await initRes.text();
                console.error('[Carousel] REST Documents API failed:', initRes.status, errBody);
            }
        } catch (docErr) {
            console.error('[Carousel] REST Documents API exception:', docErr.message);
        }

        // --- Method 2: Legacy v2 Assets API (fallback) ---
        if (!documentUrn) {
            console.log('[Carousel] Falling back to legacy v2 assets API...');
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

            if (registerRes.ok) {
                const registerData = await registerRes.json();
                const uploadUrl = registerData.value?.uploadMechanism?.['com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest']?.uploadUrl;
                const asset = registerData.value?.asset;

                if (uploadUrl && asset) {
                    const uploadRes = await fetch(uploadUrl, {
                        method: 'PUT',
                        headers: {
                            'Authorization': `Bearer ${accessToken}`,
                            'Content-Type': 'application/pdf',
                        },
                        body: pdfBuffer,
                    });

                    if (uploadRes.ok) {
                        documentUrn = asset;
                        console.log('[Carousel] Legacy v2 upload succeeded, asset:', asset);
                    } else {
                        console.error('[Carousel] Legacy v2 upload PUT failed:', uploadRes.status);
                    }
                }
            } else {
                const errText = await registerRes.text();
                console.error('[Carousel] Legacy v2 register failed:', registerRes.status, errText);
            }
        }

        if (!documentUrn) {
            console.log('[Carousel] Document upload failed, falling back to text-only post for', linkedinId);

            // Fall back: publish as a text-only post and tell client to download the PDF
            const fallbackText = text.trim() + '\n\n[Carousel PDF attached separately]';
            const fbRes = await fetch('https://api.linkedin.com/v2/ugcPosts', {
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
                            shareCommentary: { text: fallbackText },
                            shareMediaCategory: 'NONE',
                        },
                    },
                    visibility: {
                        'com.linkedin.ugc.MemberNetworkVisibility': visibility,
                    },
                }),
            });

            if (fbRes.ok) {
                const fbData = await fbRes.json();
                const fbPostId = fbData.id;
                const fbPostUrl = fbPostId
                    ? `https://www.linkedin.com/feed/update/${fbPostId}/`
                    : 'https://www.linkedin.com/feed/';
                await consumeCredit(req.session, 1);
                console.log(`[Carousel] Fallback text post published: ${fbPostId}`);
                return res.json({
                    success: true,
                    postId: fbPostId,
                    postUrl: fbPostUrl,
                    fallback: true,
                    message: 'LinkedIn does not allow automatic document uploads for your app. Your caption was posted as text. Please download the PDF and add it to your post manually via LinkedIn\'s Edit Post > Add Document feature.',
                });
            } else {
                const fbErr = await fbRes.text();
                console.error('[Carousel] Fallback text post also failed:', fbRes.status, fbErr);
                return res.status(500).json({ error: 'Could not publish to LinkedIn. Please download the PDF and share it manually.' });
            }
        }

        // --- Create post ---
        let postId = null;
        let postUrl = null;

        // Try new REST Posts API first
        const isDocumentUrn = documentUrn.startsWith('urn:li:document:');
        if (isDocumentUrn) {
            const readyCheck = await waitLinkedInDocumentReady(accessToken, documentUrn);
            if (!readyCheck.ok) {
                console.warn('[Carousel] Proceeding despite document readiness:', readyCheck.error);
            }

            const postRes = await fetch('https://api.linkedin.com/rest/posts', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${accessToken}`,
                    ...linkedinRestHeaders(),
                },
                body: JSON.stringify({
                    author: personUrn,
                    commentary: text.trim(),
                    visibility: visibility === 'CONNECTIONS' ? 'CONNECTIONS' : 'PUBLIC',
                    distribution: {
                        feedDistribution: 'MAIN_FEED',
                        targetEntities: [],
                        thirdPartyDistributionChannels: [],
                    },
                    content: {
                        media: {
                            title: pdfPostTitle,
                            id: documentUrn,
                        },
                    },
                    lifecycleState: 'PUBLISHED',
                }),
            });

            if (postRes.ok || postRes.status === 201) {
                const loc = postRes.headers.get('x-restli-id') || postRes.headers.get('x-linkedin-id');
                if (loc) {
                    postId = loc;
                } else {
                    try { const d = await postRes.json(); postId = d.id; } catch {}
                }
            } else {
                const errData = await postRes.text();
                console.error('[Carousel] REST Posts API failed:', postRes.status, errData);
            }
        }

        // Fallback to legacy UGC Posts API
        if (!postId) {
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
                            shareMediaCategory: 'NATIVE_DOCUMENT',
                            media: [{
                                status: 'READY',
                                media: documentUrn,
                                title: { text: pdfPostTitle.slice(0, 200) },
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
                postId = postData.id;
            } else {
                const errData = await postRes.text();
                console.error('[Carousel] Legacy UGC post failed:', postRes.status, errData);

                if (postRes.status === 401) {
                    return res.status(401).json({ error: 'LinkedIn access token expired. Please sign out and sign in again.' });
                }
                return res.status(postRes.status).json({ error: 'Failed to publish carousel to LinkedIn.', details: errData });
            }
        }

        postUrl = postId
            ? `https://www.linkedin.com/feed/update/${postId}/`
            : 'https://www.linkedin.com/feed/';

        await consumeCredit(req.session, 1);
        console.log(`[Carousel] Published to LinkedIn by ${req.session.user.name}: ${postId}`);
        res.json({ success: true, postId, postUrl });
    } catch (err) {
        console.error('[Carousel] Publish error:', err);
        res.status(500).json({ error: 'Could not publish carousel. Please try again.' });
    }
});

// ---------- EXTENSION & ANALYTICS ----------

// Generate a simple auth token for the extension
// Cookie-based auto-login for the Chrome extension.
// Called by the extension when it has no token yet; if the browser is logged
// into the dashboard the session cookie is sent (the extension has host
// permission for app.superlinkedin.org), and we hand back a fresh token so
// the user never has to type their email/LinkedIn ID into the popup.
app.get('/api/extension/issue-token', async (req, res) => {
    if (!req.session || !req.session.user) {
        return res.status(401).json({ error: 'Not authenticated' });
    }
    const sessUser = req.session.user;
    const user = await dbGetUser(sessUser.linkedinId) || sessUser;
    const secret = process.env.SESSION_SECRET || 'fallback';
    const token = crypto.createHmac('sha256', secret)
        .update(user.linkedinId)
        .digest('hex');
    try { await dbUpdateFields(user.linkedinId, { extensionToken: token }); } catch {}
    if (!global._tokenCache) global._tokenCache = {};
    global._tokenCache[token] = user.linkedinId;
    console.log(`[Extension Auth] Cookie auto-login for ${user.name} (${user.linkedinId})`);
    res.json({
        token,
        name: user.name,
        email: user.email,
        linkedinId: user.linkedinId,
        plan: user.planTier || user.plan || 'Pro',
    });
});

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
        console.log(`[Extension Auth] No user found for email=${email}, linkedinId=${linkedinId}`);
        return res.status(404).json({ error: 'No account found. Make sure the email matches your SuperLinkedIn account.' });
    }

    const secret = process.env.SESSION_SECRET || 'fallback';
    const token = crypto.createHmac('sha256', secret)
        .update(user.linkedinId)
        .digest('hex');

    await dbUpdateFields(user.linkedinId, { extensionToken: token });

    if (!global._tokenCache) global._tokenCache = {};
    global._tokenCache[token] = user.linkedinId;

    console.log(`[Extension Auth] Success for ${user.name} (${user.linkedinId})`);
    res.json({ token, name: user.name, linkedinId: user.linkedinId, plan: user.planTier || user.plan || 'Pro' });
});

async function authExtension(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Missing authorization token' });
    }

    const token = authHeader.slice(7);
    const secret = process.env.SESSION_SECRET || 'fallback';
    if (!global._tokenCache) global._tokenCache = {};

    // 1. Check session user
    if (req.session && req.session.user) {
        const expectedToken = crypto.createHmac('sha256', secret)
            .update(req.session.user.linkedinId)
            .digest('hex');
        if (expectedToken === token) {
            req.extUser = req.session.user;
            global._tokenCache[token] = req.session.user.linkedinId;
            return next();
        }
    }

    // 2. Check in-memory cache
    if (global._tokenCache[token]) {
        const user = await dbGetUser(global._tokenCache[token]);
        if (user) {
            req.extUser = user;
            return next();
        }
    }

    // 3. Scan DynamoDB for matching extensionToken (handles server restarts)
    try {
        const result = await ddb.send(new ScanCommand({
            TableName: DYNAMO_TABLE,
            FilterExpression: 'extensionToken = :token',
            ExpressionAttributeValues: { ':token': token },
        }));
        if (result.Items && result.Items[0]) {
            const user = result.Items[0];
            global._tokenCache[token] = user.linkedinId;
            req.extUser = user;
            return next();
        }
    } catch (err) {
        console.error('[AuthExtension] DB scan error:', err.message);
    }

    return res.status(401).json({ error: 'Invalid or expired token. Please reconnect the extension.' });
}

// Receive scraped analytics data from extension
app.post('/api/analytics/sync', authExtension, async (req, res) => {
    const { followers, posts, dashboardStats, dms, feedPosts } = req.body;
    const linkedinId = req.extUser.linkedinId;

    const updates = {};
    const now = new Date().toISOString();
    const today = now.split('T')[0];

    const dbSnap = await dbGetUser(linkedinId);
    let followerAnchor = Number(dbSnap && dbSnap.analyticsFollowers) || 0;

    const rejectsFollowersCliffDrop = (prev, incoming) => {
        const n = Number(incoming);
        const p = Number(prev);
        if (!Number.isFinite(n) || n < 5) return true;
        if (!Number.isFinite(p) || p < 35) return false;
        return n <= p && n < p * 0.72 && (p - n) > 55;
    };

    if (followers !== null && followers !== undefined) {
        // Defensive floor: a value < 5 almost never reflects a real
        // connection count and is usually a stray "1 follower" widget
        // captured from the LinkedIn DOM. Skip it so a stuck low value
        // can't get persisted from a single bad scrape.
        const numFollowers = Number(followers);
        if (Number.isFinite(numFollowers) && numFollowers >= 5 && !rejectsFollowersCliffDrop(followerAnchor, numFollowers)) {
            updates.analyticsFollowers = numFollowers;
            followerAnchor = Math.max(followerAnchor, numFollowers);

            const user = dbSnap || await dbGetUser(linkedinId);
            const history = (user && user.analyticsFollowersHistory) || [];
            const lastEntry = history[history.length - 1];
            if (!lastEntry || lastEntry.date !== today) {
                history.push({ date: today, count: numFollowers });
                if (history.length > 365) history.splice(0, history.length - 365);
            } else {
                lastEntry.count = numFollowers;
            }
            updates.analyticsFollowersHistory = history;
        } else {
            console.log('[Sync] Ignoring follower count from scrape:', followers, 'anchor=', followerAnchor, 'for', linkedinId);
        }
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

    console.log('[Sync] Incoming data for', linkedinId, '- followers:', followers, 'posts:', (posts || []).length, 'dashboardStats:', JSON.stringify(dashboardStats || null));

    if (dashboardStats) {
        const user = await dbGetUser(linkedinId);
        const existing = (user && user.analyticsDashboard) || {};
        const mergedDash = { ...existing, ...dashboardStats };
        if (dashboardStats && Object.prototype.hasOwnProperty.call(dashboardStats, 'postImpressions')) {
            const a = Number(existing.postImpressions);
            const b = Number(dashboardStats.postImpressions);
            if (Number.isFinite(a) && a >= 0 && Number.isFinite(b) && b >= 0) {
                mergedDash.postImpressions = Math.max(a, b);
            }
        }
        mergedDash.updatedAt = now;
        updates.analyticsDashboard = mergedDash;

        if (dashboardStats.profileViews !== undefined) updates.analyticsProfileViews = dashboardStats.profileViews;
        if (dashboardStats.searchAppearances !== undefined) updates.analyticsSearchAppearances = dashboardStats.searchAppearances;
        if (dashboardStats && Object.prototype.hasOwnProperty.call(dashboardStats, 'postImpressions')) {
            const eng = updates.analyticsEngagement || (user && user.analyticsEngagement) || {};
            updates.analyticsEngagement = {
                ...eng,
                impressions: Number(mergedDash.postImpressions) || eng.impressions || 0,
            };
        }

        if (dashboardStats.followers !== undefined && dashboardStats.followers !== null &&
            updates.analyticsFollowersHistory === undefined) {
            const dashFollowers = Number(dashboardStats.followers);
            if (Number.isFinite(dashFollowers) && dashFollowers >= 5 &&
                !rejectsFollowersCliffDrop(followerAnchor, dashFollowers)) {
                updates.analyticsFollowers = dashFollowers;
                followerAnchor = Math.max(followerAnchor, dashFollowers);
                const history = (user && user.analyticsFollowersHistory) || [];
                const lastEntry = history[history.length - 1];
                if (!lastEntry || lastEntry.date !== today) {
                    history.push({ date: today, count: dashFollowers });
                    if (history.length > 365) history.splice(0, history.length - 365);
                } else {
                    lastEntry.count = dashFollowers;
                }
                updates.analyticsFollowersHistory = history;
            } else {
                console.log('[Sync] Ignoring dashboardStats.followers:', dashboardStats.followers, 'anchor=', followerAnchor, 'for', linkedinId);
            }
        }
    }

    if (dms && dms.conversations && dms.conversations.length > 0) {
        const user = await dbGetUser(linkedinId);
        const existing = (user && user.dmConversations) || [];
        const dmJunkRe = /^view\s|learn how|try premium|people you may know|suggested|job alert|view company/i;
        const dmProfileRe = /['\u2019]s profile/i;
        const cleanConversations = dms.conversations.filter(c => {
            const name = c.participantName || '';
            if (dmJunkRe.test(name) || dmProfileRe.test(name) || name.length < 2) return false;
            return true;
        });
        cleanConversations.forEach(incoming => {
            const idx = existing.findIndex(e => e.participantName === incoming.participantName);
            if (idx >= 0) {
                existing[idx].lastMessage = incoming.lastMessage || existing[idx].lastMessage;
                existing[idx].lastMessageAt = incoming.lastMessageAt || existing[idx].lastMessageAt;
                existing[idx].unread = incoming.unread;
                if (incoming.participantPicture) existing[idx].participantPicture = incoming.participantPicture;
                if (incoming.participantUrl) existing[idx].participantUrl = incoming.participantUrl;
            } else {
                existing.push({
                    id: incoming.id || ('dm-' + Date.now() + '-' + Math.random().toString(36).substring(2, 8)),
                    participantName: incoming.participantName,
                    participantUrl: incoming.participantUrl || '',
                    participantPicture: incoming.participantPicture || '',
                    lastMessage: incoming.lastMessage || '',
                    lastMessageAt: incoming.lastMessageAt || now,
                    unread: incoming.unread || false,
                    labels: [],
                    messages: [],
                });
            }
        });
        if (dms.activeThread && dms.activeThread.messages && dms.activeThread.messages.length > 0) {
            const threadConv = existing.find(c => c.participantName === dms.activeThread.participantName);
            if (threadConv) {
                // Dedup by normalized text content to avoid duplicates from scraping
                const existingTexts = new Set(
                    (threadConv.messages || []).map(m => (m.text || '').substring(0, 200).toLowerCase().trim())
                );
                dms.activeThread.messages.forEach(msg => {
                    const key = (msg.text || '').substring(0, 200).toLowerCase().trim();
                    if (key && !existingTexts.has(key)) {
                        existingTexts.add(key);
                        threadConv.messages.push(msg);
                    }
                });
                if (threadConv.messages.length > 500) {
                    threadConv.messages = threadConv.messages.slice(-500);
                }
            }
        }
        if (existing.length > 200) existing.splice(0, existing.length - 200);
        updates.dmConversations = existing;
        console.log('[Sync] DM data merged:', cleanConversations.length, 'conversations (filtered from', dms.conversations.length, 'raw)');
    }

    if (feedPosts && feedPosts.length > 0) {
        const user = await dbGetUser(linkedinId);
        const existing = (user && user.engageFeed) || [];
        const existingTexts = new Set(existing.map(p => (p.text || '').substring(0, 80).toLowerCase()));
        feedPosts.forEach(fp => {
            const key = (fp.text || '').substring(0, 80).toLowerCase();
            if (key && !existingTexts.has(key)) {
                existingTexts.add(key);
                existing.push(fp);
            }
        });
        if (existing.length > 100) existing.splice(0, existing.length - 100);
        updates.engageFeed = existing;
        console.log('[Sync] Feed posts for Discover:', feedPosts.length, 'new, total:', existing.length);
    }

    updates.analyticsLastSync = now;

    // Aggregate engagement stats from post-level scraping
    if (updates.analyticsPostMetrics || posts) {
        const user2 = await dbGetUser(linkedinId);
        const allPosts = updates.analyticsPostMetrics || (user2 && user2.analyticsPostMetrics) || [];
        let totalLikes = 0, totalComments = 0, totalReposts = 0, totalImpressions = 0;
        allPosts.forEach(p => {
            totalLikes += p.likes || 0;
            totalComments += p.comments || 0;
            totalReposts += p.reposts || 0;
            totalImpressions += p.impressions || 0;
        });

        const prevEng = updates.analyticsEngagement || (user2 && user2.analyticsEngagement) || {};
        let dashboardImpressions = prevEng.impressions || 0;
        if (updates.analyticsDashboard && updates.analyticsDashboard.postImpressions !== undefined) {
            dashboardImpressions = Number(updates.analyticsDashboard.postImpressions) || dashboardImpressions;
        } else if (dashboardStats && dashboardStats.postImpressions !== undefined) {
            dashboardImpressions = Number(dashboardStats.postImpressions) || dashboardImpressions;
        }
        const dashboardMembersReached = (dashboardStats && dashboardStats.membersReached !== undefined)
            ? dashboardStats.membersReached
            : null;

        const hasPosts = allPosts.length > 0;
        updates.analyticsEngagement = {
            // Sum of per-post like counts only; LinkedIn dashboard "social engagements" lives under analyticsDashboard.socialEngagements.
            likes: hasPosts ? totalLikes : (prevEng.likes || 0),
            comments: hasPosts ? (totalComments || prevEng.comments || 0) : (prevEng.comments || 0),
            reposts: hasPosts ? (totalReposts || prevEng.reposts || 0) : (prevEng.reposts || 0),
            impressions: Math.max(dashboardImpressions, totalImpressions),
            totalPosts: allPosts.length,
            membersReached: dashboardMembersReached !== null ? dashboardMembersReached : (prevEng.membersReached || 0),
        };
    }

    // Rebuild postTimeStats from posted queue items with engagement data
    {
        const freshUser = await dbGetUser(linkedinId);
        const postedItems = ((freshUser && freshUser.queue) || []).filter(q => q.status === 'posted' && q.postedAtDay !== undefined);
        const postMetrics = updates.analyticsPostMetrics || (freshUser && freshUser.analyticsPostMetrics) || [];
        if (postedItems.length > 0) {
            const statsMap = {};
            postedItems.forEach(item => {
                const key = `${item.postedAtDay}-${item.postedAtHour}`;
                if (!statsMap[key]) statsMap[key] = { dayOfWeek: item.postedAtDay, hour: item.postedAtHour, impressions: 0, engagements: 0, count: 0 };
                statsMap[key].count++;
                const metric = postMetrics.find(m => m.text && item.text && m.text.substring(0, 80) === item.text.substring(0, 80));
                if (metric) {
                    statsMap[key].impressions += (metric.impressions || 0);
                    statsMap[key].engagements += (metric.likes || 0) + (metric.comments || 0) + (metric.reposts || 0);
                }
            });
            updates.postTimeStats = Object.values(statsMap);
        }
    }

    await dbUpdateFields(linkedinId, updates);

    // Cache the token for future lookups
    const secret = process.env.SESSION_SECRET || 'fallback';
    const token = crypto.createHmac('sha256', secret).update(linkedinId).digest('hex');
    if (!global._tokenCache) global._tokenCache = {};
    global._tokenCache[token] = linkedinId;

    res.json({ ok: true, syncedAt: now });
});

// LinkedIn best-practice posting scores by day (0=Sun..6=Sat) and hour
const GENERAL_SCORES = (() => {
    const dayWeights = { 0: 25, 1: 60, 2: 85, 3: 90, 4: 80, 5: 55, 6: 20 };
    const hourWeights = {
        6: 40, 7: 75, 8: 90, 9: 85, 10: 70, 11: 65,
        12: 80, 13: 60, 14: 50, 15: 45, 16: 55, 17: 75, 18: 60,
        19: 35, 20: 20,
    };
    const scores = [];
    for (let d = 0; d < 7; d++) {
        for (let h = 6; h <= 20; h++) {
            const score = Math.round((dayWeights[d] || 20) * (hourWeights[h] || 20) / 100);
            scores.push({ day: d, hour: h, score });
        }
    }
    return scores;
})();

const DAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

function scoreLabel(s) {
    if (s >= 75) return 'Best';
    if (s >= 55) return 'Great';
    if (s >= 35) return 'Good';
    return 'Low';
}

app.get('/api/best-times', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });

    const user = await dbGetUser(req.session.user.linkedinId);
    const postTimeStats = (user && user.postTimeStats) || [];
    const queue = (user && user.queue) || [];
    const hasPersonalData = postTimeStats.length >= 5;

    const slotMap = {};
    GENERAL_SCORES.forEach(gs => {
        const key = `${gs.day}-${gs.hour}`;
        slotMap[key] = { day: gs.day, dayName: DAY_NAMES[gs.day], hour: gs.hour, generalScore: gs.score, userScore: 0, userCount: 0 };
    });

    if (hasPersonalData) {
        const maxEng = Math.max(...postTimeStats.map(p => (p.engagements || 0) / Math.max(p.count, 1)), 1);
        postTimeStats.forEach(p => {
            const key = `${p.dayOfWeek}-${p.hour}`;
            if (slotMap[key]) {
                const avgEng = (p.engagements || 0) / Math.max(p.count, 1);
                slotMap[key].userScore = Math.round((avgEng / maxEng) * 100);
                slotMap[key].userCount = p.count;
            }
        });
    }

    const scheduledTimes = new Set(
        queue.filter(q => q.status === 'scheduled' && q.scheduledFor)
            .map(q => new Date(q.scheduledFor).toISOString().slice(0, 13))
    );

    const slots = Object.values(slotMap).map(s => {
        const finalScore = hasPersonalData
            ? Math.round(0.4 * s.generalScore + 0.6 * s.userScore)
            : s.generalScore;
        return {
            day: s.dayName,
            dayNum: s.day,
            hour: s.hour,
            score: finalScore,
            label: scoreLabel(finalScore),
            source: hasPersonalData && s.userCount > 0 ? 'hybrid' : 'general',
        };
    }).sort((a, b) => b.score - a.score);

    // Compute next best concrete datetime
    const now = new Date();
    const tz = (user && user.timezone) || Intl.DateTimeFormat().resolvedOptions().timeZone;
    const top5 = slots.slice(0, 5);
    let nextBest = null;
    for (let offset = 0; offset < 14 && !nextBest; offset++) {
        const candidate = new Date(now.getTime() + offset * 86400000);
        const cDay = candidate.getDay();
        for (const slot of top5) {
            if (slot.dayNum === cDay) {
                const dt = new Date(candidate);
                dt.setHours(slot.hour, 0, 0, 0);
                if (dt > now && !scheduledTimes.has(dt.toISOString().slice(0, 13))) {
                    nextBest = dt.toISOString();
                    break;
                }
            }
        }
    }

    res.json({ slots: slots.slice(0, 15), nextBest, hasPersonalData });
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

/** Sums interaction counts scraped from visible post cards / stored metrics rows. */
function summarizePostInteractions(postMetrics) {
    const posts = Array.isArray(postMetrics) ? postMetrics : [];
    return {
        likes: posts.reduce((s, p) => s + (Number(p.likes) || 0), 0),
        comments: posts.reduce((s, p) => s + (Number(p.comments) || 0), 0),
        reposts: posts.reduce((s, p) => s + (Number(p.reposts) || 0), 0),
        impressionsSum: posts.reduce((s, p) => s + (Number(p.impressions) || 0), 0),
    };
}

/**
 * AVG engagement uses dashboard social engagements ÷ reconciled impressions when both exist.
 * Total impressions use max(dashboard rollup, sum of tracked post cards, stored engagement snapshot)
 * so a bad analytics-page scrape (tiny number) does not override the feed sidebar’s “Post impressions”.
 * Otherwise use summed post reactions ÷ summed post impressions (tracked cards only).
 */
function computeAvgEngagementAndDisplayImpressions(postMetrics, analyticsEngagement, analyticsDashboard) {
    const dash = analyticsDashboard || {};
    const eng = analyticsEngagement || {};
    const fromPosts = summarizePostInteractions(postMetrics);

    const hasDashSocial = Object.prototype.hasOwnProperty.call(dash, 'socialEngagements');
    const socialDash = hasDashSocial ? Number(dash.socialEngagements) : null;

    const hasDashPostImp = Object.prototype.hasOwnProperty.call(dash, 'postImpressions');
    const dashPostImp = hasDashPostImp ? Number(dash.postImpressions) : null;

    /** Feed identity sidebar often has the real “Post impressions” rollup; `/analytics/` page scrape sometimes overwrites dashboard with another widget’s digits (too low). */
    const dashImpNonNeg =
        hasDashPostImp && dashPostImp !== null && Number.isFinite(dashPostImp) && dashPostImp >= 0 ? dashPostImp : 0;
    const sumImp = fromPosts.impressionsSum || 0;
    const engImp = Number(eng.impressions) || 0;
    const reconciledImp = Math.max(dashImpNonNeg, sumImp, engImp);

    let avgEngagement = 0;
    const postInteractions = fromPosts.likes + fromPosts.comments + fromPosts.reposts;
    if (socialDash !== null && Number.isFinite(socialDash) && socialDash >= 0 && reconciledImp > 0) {
        avgEngagement = (socialDash / reconciledImp) * 100;
    } else if (sumImp > 0) {
        avgEngagement = (postInteractions / sumImp) * 100;
    }

    const totalImpressions = reconciledImp;

    return { avgEngagement, totalImpressions, fromPosts };
}

// Analytics summary for extension popup/sidebar (bearer-token auth)
app.get('/api/analytics/summary', authExtension, async (req, res) => {
    const user = await dbGetUser(req.extUser.linkedinId);
    if (!user) return res.json({});

    const posts = user.analyticsPostMetrics || [];
    const eng = user.analyticsEngagement || {};
    const dashboard = user.analyticsDashboard || {};

    const topPosts = [...posts]
        .sort((a, b) => ((b.likes || 0) + (b.comments || 0) + (b.reposts || 0))
                       - ((a.likes || 0) + (a.comments || 0) + (a.reposts || 0)))
        .slice(0, 5);

    const dash = dashboard;
    const hasDashSocial = dash && Object.prototype.hasOwnProperty.call(dash, 'socialEngagements');
    const socialEngagementsDashboard = hasDashSocial ? (Number(dash.socialEngagements) || 0) : null;

    const { avgEngagement, totalImpressions, fromPosts } = computeAvgEngagementAndDisplayImpressions(posts, eng, dash);
    const likesFromPosts = fromPosts.likes;
    const commentsFromPosts = fromPosts.comments;
    const repostsFromPosts = fromPosts.reposts;

    // Prefer the followers count from the profile page scrape over the dashboard.
    // Any value < 5 is almost certainly a stale bad scrape (e.g. a stray
    // "1 follower" Pages widget) – return 0 instead so the popup doesn't
    // mislead until a real /mynetwork/ scrape replaces it.
    const rawFollowers = user.analyticsFollowers || dash.followers || 0;
    const followers = (Number(rawFollowers) >= 5) ? Number(rawFollowers) : 0;

    res.json({
        followers,
        totalPosts: posts.length || (user.queueItems || []).filter(q => q.status === 'posted').length || 0,
        avgEngagement: Math.round(avgEngagement * 10) / 10,
        totalImpressions,

        /** @deprecated Prefer likesFromPosts + socialEngagementsDashboard — kept for older extension builds */
        totalLikes: likesFromPosts,

        socialEngagementsDashboard,

        likesFromPosts,
        commentsFromPosts,
        repostsFromPosts,

        totalComments: commentsFromPosts,
        totalReposts: repostsFromPosts,

        membersReached: dash.membersReached || eng.membersReached || 0,
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

app.post('/api/me/dismiss-suggestions', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    await dbUpdateFields(req.session.user.linkedinId, { suggestedPostsSeen: true });
    res.json({ ok: true });
});

app.post('/api/me/complete-missions', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const linkedinId = req.session.user.linkedinId;
    const user = await dbGetUser(linkedinId);
    if (user && user.missionsCompleted) {
        return res.json({ ok: true, creditsAwarded: 0 });
    }

    const hasProfile = !!(user && user.aboutYou);
    const hasPost = !!(user && (user.queue || []).some(q => q.status === 'posted'));
    const hasExtension = !!(user && user.analyticsLastSync);

    if (!hasProfile || !hasPost || !hasExtension) {
        return res.status(400).json({ error: 'Not all missions completed yet.' });
    }

    const creditsAwarded = 200;
    const currentUsed = user.aiCreditsUsed || 0;
    const newUsed = Math.max(0, currentUsed - creditsAwarded);

    await dbUpdateFields(linkedinId, {
        missionsCompleted: true,
        missionsCompletedAt: new Date().toISOString(),
        aiCreditsUsed: newUsed,
    });

    console.log(`[Missions] All 3 missions completed by ${user.name} (${linkedinId}), awarded ${creditsAwarded} credits`);
    res.json({ ok: true, creditsAwarded });
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

// ---------- VIRAL LIBRARY ----------

const VIRAL_POSTS = (() => {
    try { return require('./viral-posts.json'); } catch { return []; }
})();

app.get('/api/viral-library', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'viral_library'))) return res.status(403).json({ error: 'Upgrade to Advanced or Ultra to access the Viral Library.' });

    const { q, category, type, minLikes, page } = req.query;
    let results = [...VIRAL_POSTS];
    if (q) { const lq = q.toLowerCase(); results = results.filter(p => p.text.toLowerCase().includes(lq) || p.author.toLowerCase().includes(lq)); }
    if (category) results = results.filter(p => p.category === category);
    if (type) results = results.filter(p => p.type === type);
    if (minLikes) results = results.filter(p => p.likes >= parseInt(minLikes));
    results.sort((a, b) => b.likes - a.likes);
    const pg = parseInt(page) || 1;
    const perPage = 20;
    const total = results.length;
    const paged = results.slice((pg - 1) * perPage, pg * perPage);
    res.json({ posts: paged, total, page: pg, pages: Math.ceil(total / perPage) });
});

// ---------- ENGAGE ENGINE ----------

app.get('/api/engage/discover', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    const feedData = (user && user.engageFeed) || [];
    const postMetrics = (user && user.analyticsPostMetrics) || [];
    const discover = feedData.length > 0 ? feedData : postMetrics.slice(0, 20).map(p => ({
        text: p.text, likes: p.likes || 0, comments: p.comments || 0, reposts: p.reposts || 0,
        impressions: p.impressions || 0, author: 'Your network', date: p.date || null,
    }));
    res.json({ posts: discover });
});

app.get('/api/engage/mentions', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'engage_engine'))) return res.status(403).json({ error: 'Upgrade to Advanced or Ultra.' });
    const user = await dbGetUser(req.session.user.linkedinId);
    res.json({ mentions: (user && user.engageMentions) || [] });
});

app.get('/api/engage/replies', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'engage_engine'))) return res.status(403).json({ error: 'Upgrade to Advanced or Ultra.' });
    const user = await dbGetUser(req.session.user.linkedinId);
    res.json({ replies: (user && user.engageReplies) || [] });
});

app.post('/api/engage/ai-reply', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'engage_engine'))) return res.status(403).json({ error: 'Upgrade to Advanced or Ultra.' });
    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 1) return res.json({ error: 'No AI credits remaining.' });
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') return res.json({ error: 'OpenAI API key not configured.' });
    const { postText, style } = req.body;
    if (!postText) return res.status(400).json({ error: 'postText is required.' });
    try {
        const aiRes = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
            body: JSON.stringify({ model: 'gpt-4o-mini', max_tokens: 200, messages: [
                { role: 'system', content: `You write short, thoughtful LinkedIn comments. Style: ${style || 'supportive'}. Keep it under 100 words. Be authentic, not generic.` },
                { role: 'user', content: `Write a reply to this LinkedIn post:\n\n${postText}` },
            ]}),
        });
        const data = await aiRes.json();
        await consumeCredit(req.session, 1);
        res.json({ reply: data.choices?.[0]?.message?.content || '' });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// Engage Lists CRUD
app.get('/api/engage/lists', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'engage_engine'))) return res.status(403).json({ error: 'Upgrade to Advanced or Ultra.' });
    const user = await dbGetUser(req.session.user.linkedinId);
    res.json({ lists: (user && user.engageLists) || [] });
});

app.post('/api/engage/lists', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'engage_engine'))) return res.status(403).json({ error: 'Upgrade to Advanced or Ultra.' });
    const { name, members } = req.body;
    if (!name) return res.status(400).json({ error: 'List name is required.' });
    const user = await dbGetUser(req.session.user.linkedinId);
    const lists = (user && user.engageLists) || [];
    lists.push({ id: Date.now().toString(36), name, members: members || [], createdAt: new Date().toISOString() });
    await dbUpdateFields(req.session.user.linkedinId, { engageLists: lists });
    res.json({ lists });
});

app.delete('/api/engage/lists/:id', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    let lists = (user && user.engageLists) || [];
    lists = lists.filter(l => l.id !== req.params.id);
    await dbUpdateFields(req.session.user.linkedinId, { engageLists: lists });
    res.json({ lists });
});

app.put('/api/engage/lists/:id', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    const lists = (user && user.engageLists) || [];
    const list = lists.find(l => l.id === req.params.id);
    if (!list) return res.status(404).json({ error: 'List not found.' });
    if (req.body.name) list.name = req.body.name;
    if (req.body.members) list.members = req.body.members;
    await dbUpdateFields(req.session.user.linkedinId, { engageLists: lists });
    res.json({ lists });
});

// ---------- AUTO-ENGAGE ----------

app.get('/api/auto-engage/rules', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'auto_repost'))) return res.status(403).json({ error: 'Upgrade to Advanced or Ultra.' });
    const user = await dbGetUser(req.session.user.linkedinId);
    res.json({ rules: (user && user.autoEngageRules) || [] });
});

app.post('/api/auto-engage/rules', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'auto_repost'))) return res.status(403).json({ error: 'Upgrade to Advanced or Ultra.' });
    const { type, source, minEngagement, maxPerDay, style, active } = req.body;
    if (!type) return res.status(400).json({ error: 'Rule type is required.' });
    const user = await dbGetUser(req.session.user.linkedinId);
    const rules = (user && user.autoEngageRules) || [];
    rules.push({ id: Date.now().toString(36), type, source: source || '', minEngagement: minEngagement || 50, maxPerDay: maxPerDay || 5, style: style || 'supportive', active: active !== false, createdAt: new Date().toISOString() });
    await dbUpdateFields(req.session.user.linkedinId, { autoEngageRules: rules });
    res.json({ rules });
});

app.put('/api/auto-engage/rules/:id', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    const rules = (user && user.autoEngageRules) || [];
    const rule = rules.find(r => r.id === req.params.id);
    if (!rule) return res.status(404).json({ error: 'Rule not found.' });
    Object.assign(rule, req.body, { id: rule.id, createdAt: rule.createdAt });
    await dbUpdateFields(req.session.user.linkedinId, { autoEngageRules: rules });
    res.json({ rules });
});

app.delete('/api/auto-engage/rules/:id', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    let rules = (user && user.autoEngageRules) || [];
    rules = rules.filter(r => r.id !== req.params.id);
    await dbUpdateFields(req.session.user.linkedinId, { autoEngageRules: rules });
    res.json({ rules });
});

// ---------- TEAM COLLABORATION ----------

app.get('/api/team/members', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const primaryId = getPrimaryAccountId(req.session);
    const user = await dbGetUser(primaryId);
    res.json({ members: (user && user.teamMembers) || [] });
});

app.post('/api/team/invite', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const { email, role } = req.body;
    const inviteEmailNorm = normalizeInviteEmail(email);
    if (!inviteEmailNorm) return res.status(400).json({ error: 'Email is required.' });
    const primaryId = getPrimaryAccountId(req.session);
    const user = await dbGetUser(primaryId);

    const tier = (user && user.planTier) || 'pro';
    const limits = PLAN_LIMITS[tier] || PLAN_LIMITS.pro;
    const linkedProfiles = (user && user.linkedProfiles) || [];
    const members = (user && user.teamMembers) || [];
    const totalUsed = linkedProfiles.length + members.length;

    if (totalUsed >= limits.maxProfiles) {
        return res.status(403).json({ error: `You have reached the ${limits.maxProfiles}-member limit for your ${tier.toUpperCase()} plan. Upgrade to add more.` });
    }

    if (members.some(m => m && normalizeInviteEmail(m.email) === inviteEmailNorm)) {
        return res.status(400).json({ error: 'This email has already been invited.' });
    }
    const inviteCode = crypto.randomBytes(16).toString('hex');
    const invitedAt = new Date().toISOString();
    const roleNorm = role || 'editor';
    members.push({ email: inviteEmailNorm, role: roleNorm, status: 'invited', inviteCode, invitedAt });
    await dbUpdateFields(primaryId, { teamMembers: members });
    await upsertTeamInvitePlaceholder(primaryId, inviteEmailNorm, inviteCode, roleNorm, invitedAt);
    if (!global._inviteCache) global._inviteCache = {};
    global._inviteCache[inviteCode] = primaryId;
    const inviteLink = `${APP_BASE}/app?invite=${inviteCode}`;
    res.json({ members, inviteLink });
});

app.delete('/api/team/members/:email', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const primaryId = getPrimaryAccountId(req.session);
    const user = await dbGetUser(primaryId);
    const targetNorm = normalizeInviteEmail(decodeURIComponent(req.params.email || ''));
    const before = (user && user.teamMembers) || [];
    const removed = before.filter(m => m && normalizeInviteEmail(m.email) === targetNorm);
    const members = before.filter(m => !m || normalizeInviteEmail(m.email) !== targetNorm);
    await dbUpdateFields(primaryId, { teamMembers: members });
    if (removed.length) await deleteTeamInvitePlaceholder(primaryId, removed[0].email);
    // Invalidate any cached invite codes belonging to the removed members so
    // a future click on the link can't silently re-attach them.
    if (global._inviteCache) {
        removed.forEach(m => { if (m && m.inviteCode) delete global._inviteCache[m.inviteCode]; });
    }
    // If this member had already accepted, sever the parent link on their
    // profile so they stop inheriting our paid plan.
    for (const m of removed) {
        if (m && m.linkedinId) {
            try { await dbUpdateFields(m.linkedinId, { parentAccountId: null, invitedVia: null }); } catch {}
        }
    }
    res.json({ members });
});

app.put('/api/team/members/:email', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const primaryId = getPrimaryAccountId(req.session);
    const user = await dbGetUser(primaryId);
    const members = (user && user.teamMembers) || [];
    const targetNorm = normalizeInviteEmail(decodeURIComponent(req.params.email || ''));
    const member = members.find(m => m && normalizeInviteEmail(m.email) === targetNorm);
    if (!member) return res.status(404).json({ error: 'Member not found.' });
    if (req.body.role) member.role = req.body.role;
    await dbUpdateFields(primaryId, { teamMembers: members });
    res.json({ members });
});

app.get('/api/team/queue', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'team'))) return res.status(403).json({ error: 'Upgrade to Ultra.' });
    const primaryId = getPrimaryAccountId(req.session);
    const user = await dbGetUser(primaryId);
    const linkedProfiles = (user && user.linkedProfiles) || [];
    const sharedQueue = [];
    for (const p of linkedProfiles) {
        const pUser = await dbGetUser(p.linkedinId);
        if (pUser && pUser.queue) {
            pUser.queue.forEach(item => sharedQueue.push({ ...item, authorName: pUser.name || p.name, authorPicture: pUser.picture || p.picture }));
        }
    }
    sharedQueue.sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
    res.json({ queue: sharedQueue });
});

app.post('/api/team/approve/:index', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const { linkedinId, action } = req.body;
    if (!linkedinId) return res.status(400).json({ error: 'linkedinId required.' });
    const user = await dbGetUser(linkedinId);
    if (!user || !user.queue) return res.status(404).json({ error: 'Queue not found.' });
    const idx = parseInt(req.params.index);
    if (idx < 0 || idx >= user.queue.length) return res.status(400).json({ error: 'Invalid index.' });
    if (action === 'approve') user.queue[idx].status = 'scheduled';
    else if (action === 'reject') user.queue[idx].status = 'rejected';
    await dbUpdateFields(linkedinId, { queue: user.queue });
    res.json({ ok: true });
});

// ---------- WHITE-LABEL REPORTS ----------

app.get('/api/reports/branding', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'white_label'))) return res.status(403).json({ error: 'Upgrade to Ultra.' });
    const user = await dbGetUser(req.session.user.linkedinId);
    res.json({ branding: (user && user.reportBranding) || { companyName: '', logoUrl: '', brandColor: '#0A66C2', reportTitle: 'LinkedIn Performance Report' } });
});

app.put('/api/reports/branding', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'white_label'))) return res.status(403).json({ error: 'Upgrade to Ultra.' });
    const { companyName, logoUrl, brandColor, reportTitle } = req.body;
    const branding = { companyName: companyName || '', logoUrl: logoUrl || '', brandColor: brandColor || '#0A66C2', reportTitle: reportTitle || 'LinkedIn Performance Report' };
    await dbUpdateFields(req.session.user.linkedinId, { reportBranding: branding });
    res.json({ branding });
});

app.get('/api/reports/generate', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'white_label'))) return res.status(403).json({ error: 'Upgrade to Ultra.' });
    const PDFDocument = require('pdfkit');
    const user = await dbGetUser(req.session.user.linkedinId);
    const branding = (user && user.reportBranding) || {};
    const engagement = (user && user.analyticsEngagement) || {};
    const dashboard = (user && user.analyticsDashboard) || {};
    const postMetrics = (user && user.analyticsPostMetrics) || [];
    const followers = dashboard.followers || user.analyticsFollowers || 0;
    const impressions = dashboard.postImpressions || engagement.impressions || 0;
    const color = branding.brandColor || '#0A66C2';

    const doc = new PDFDocument({ size: 'A4', margin: 50 });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="report-${Date.now()}.pdf"`);
    doc.pipe(res);

    doc.fontSize(24).fillColor(color).text(branding.reportTitle || 'LinkedIn Performance Report', { align: 'center' });
    doc.moveDown(0.5);
    if (branding.companyName) doc.fontSize(14).fillColor('#555').text(branding.companyName, { align: 'center' });
    doc.fontSize(10).fillColor('#999').text(`Generated: ${new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}`, { align: 'center' });
    doc.moveDown(2);

    doc.fontSize(16).fillColor(color).text('Overview');
    doc.moveDown(0.5);
    doc.fontSize(11).fillColor('#333');
    const dash = dashboard || {};
    const dashSocial = dash && Object.prototype.hasOwnProperty.call(dash, 'socialEngagements')
        ? (Number(dash.socialEngagements) || 0)
        : null;
    const postAgg = summarizePostInteractions(postMetrics);
    doc.text(`Followers: ${followers.toLocaleString()}`);
    doc.text(`Total Impressions: ${impressions.toLocaleString()}`);
    if (dashSocial !== null) {
        doc.text(`Social engagements (LinkedIn dashboard — reactions, replies, reshares combined): ${dashSocial.toLocaleString()}`);
    }
    doc.text(`Likes (sum on tracked/scraped posts): ${postAgg.likes.toLocaleString()}`);
    doc.text(`Comments/Replies on tracked posts: ${postAgg.comments.toLocaleString()}`);
    doc.text(`Reposts on tracked posts: ${postAgg.reposts.toLocaleString()}`);
    doc.text(`Members Reached: ${(dashboard.membersReached || 0).toLocaleString()}`);
    doc.text(`Profile Views: ${(dashboard.profileViews || 0).toLocaleString()}`);
    doc.moveDown(1.5);

    if (postMetrics.length > 0) {
        doc.fontSize(16).fillColor(color).text('Top Performing Posts');
        doc.moveDown(0.5);
        const sorted = [...postMetrics].sort((a, b) => (b.impressions || 0) - (a.impressions || 0));
        sorted.slice(0, 10).forEach((p, i) => {
            doc.fontSize(10).fillColor('#333').text(`${i + 1}. ${(p.text || '').substring(0, 120)}...`);
            doc.fontSize(9).fillColor('#888').text(`   ${(p.impressions || 0).toLocaleString()} impressions | ${(p.likes || 0)} likes | ${(p.comments || 0)} comments`);
            doc.moveDown(0.3);
        });
    }

    doc.end();
});

// ---------- API ACCESS (v1) ----------

async function apiKeyAuth(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) return res.status(401).json({ error: 'Missing API key.' });
    const key = authHeader.slice(7);
    const keyHash = crypto.createHash('sha256').update(key).digest('hex');
    const result = await ddb.send(new ScanCommand({ TableName: DYNAMO_TABLE }));
    const users = result.Items || [];
    for (const u of users) {
        if (u.apiKeys && Array.isArray(u.apiKeys)) {
            const match = u.apiKeys.find(k => k.keyHash === keyHash);
            if (match) {
                if (!(await hasFeatureForUser(u, 'api_access'))) return res.status(403).json({ error: 'API access requires Ultra plan.' });
                match.lastUsedAt = new Date().toISOString();
                await dbUpdateFields(u.linkedinId, { apiKeys: u.apiKeys });
                req.apiUser = u;
                return next();
            }
        }
    }
    return res.status(401).json({ error: 'Invalid API key.' });
}

function hasFeatureForUser(user, feature) {
    const tier = user.planTier || 'pro';
    const limits = PLAN_LIMITS[tier] || PLAN_LIMITS.pro;
    return (limits.features || []).includes(feature);
}

app.post('/api/apikeys', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!(await hasFeature(req.session, 'api_access'))) return res.status(403).json({ error: 'Upgrade to Ultra for API access.' });
    const { name } = req.body;
    const rawKey = `sl_${crypto.randomBytes(32).toString('hex')}`;
    const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex');
    const user = await dbGetUser(req.session.user.linkedinId);
    const apiKeys = (user && user.apiKeys) || [];
    apiKeys.push({ id: Date.now().toString(36), name: name || 'Default', keyHash, createdAt: new Date().toISOString(), lastUsedAt: null });
    await dbUpdateFields(req.session.user.linkedinId, { apiKeys });
    res.json({ key: rawKey, apiKeys: apiKeys.map(k => ({ id: k.id, name: k.name, createdAt: k.createdAt, lastUsedAt: k.lastUsedAt })) });
});

app.delete('/api/apikeys/:id', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    let apiKeys = (user && user.apiKeys) || [];
    apiKeys = apiKeys.filter(k => k.id !== req.params.id);
    await dbUpdateFields(req.session.user.linkedinId, { apiKeys });
    res.json({ apiKeys: apiKeys.map(k => ({ id: k.id, name: k.name, createdAt: k.createdAt, lastUsedAt: k.lastUsedAt })) });
});

app.get('/api/v1/me', apiKeyAuth, (req, res) => {
    const u = req.apiUser;
    res.json({ linkedinId: u.linkedinId, name: u.name, email: u.email, plan: u.planTier, followers: u.analyticsFollowers || 0 });
});

app.get('/api/v1/queue', apiKeyAuth, (req, res) => {
    res.json({ queue: (req.apiUser.queue || []).map(q => ({ text: q.text, status: q.status, scheduledFor: q.scheduledFor, postedAt: q.postedAt })) });
});

app.post('/api/v1/posts', apiKeyAuth, async (req, res) => {
    const { text, scheduledFor } = req.body;
    if (!text) return res.status(400).json({ error: 'text is required.' });
    const user = req.apiUser;
    const queue = user.queue || [];
    const now = new Date();
    const item = { text, status: scheduledFor ? 'scheduled' : 'draft', date: now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) };
    if (scheduledFor) item.scheduledFor = scheduledFor;
    queue.push(item);
    await dbUpdateFields(user.linkedinId, { queue });
    res.json({ ok: true, item });
});

app.get('/api/v1/analytics', apiKeyAuth, (req, res) => {
    const u = req.apiUser;
    const dashboard = u.analyticsDashboard || {};
    const engagement = u.analyticsEngagement || {};
    const postMetrics = u.analyticsPostMetrics || [];
    const fromPosts = summarizePostInteractions(postMetrics);
    const hasDashSocial = Object.prototype.hasOwnProperty.call(dashboard, 'socialEngagements');
    const socialEngagementsDashboard = hasDashSocial ? (Number(dashboard.socialEngagements) || 0) : null;
    res.json({
        followers: dashboard.followers || u.analyticsFollowers || 0,
        impressions: dashboard.postImpressions || engagement.impressions || 0,

        /** Combined interaction count from LinkedIn's analytics pages when scraped; null if not synced yet. */
        socialEngagementsDashboard,

        likesFromPosts: fromPosts.likes,
        commentsFromPosts: fromPosts.comments,
        repostsFromPosts: fromPosts.reposts,

        /** @deprecated ambiguous; use socialEngagementsDashboard or sum of *FromPosts fields */
        engagements: socialEngagementsDashboard != null
            ? socialEngagementsDashboard
            : (fromPosts.likes + fromPosts.comments + fromPosts.reposts),

        membersReached: dashboard.membersReached || 0,
        profileViews: dashboard.profileViews || 0,
    });
});

// ---------- DM ENDPOINTS ----------

/** Most recent activity time for a conversation (for sorting lists newest-first). */
function dmConversationRecencyMs(c) {
    let best = 0;
    if (c && c.lastMessageAt) {
        const p = new Date(c.lastMessageAt).getTime();
        if (!isNaN(p)) best = Math.max(best, p);
    }
    const msgs = (c && c.messages) || [];
    for (let i = msgs.length - 1; i >= 0; i--) {
        const ts = msgs[i] && msgs[i].timestamp;
        if (ts) {
            const p = new Date(ts).getTime();
            if (!isNaN(p)) { best = Math.max(best, p); break; }
        }
    }
    return best;
}

app.get('/api/dms', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    const convos = (user && user.dmConversations) || [];
    const junkRe = /^view\s|learn how|try premium|people you may know|suggested|job alert|view company|get hired|grow your|boost your/i;
    const profileRe = /['\u2019]s profile/i;
    const filtered = convos.filter(c => {
        const name = c.participantName || '';
        const preview = c.lastMessage || '';
        if (junkRe.test(name) || junkRe.test(preview)) return false;
        if (profileRe.test(name)) return false;
        if (name.length < 2) return false;
        return true;
    });
    filtered.sort((a, b) => dmConversationRecencyMs(b) - dmConversationRecencyMs(a));
    res.json({ conversations: filtered });
});

app.post('/api/dms/reply', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const { conversationId, text } = req.body;
    if (!conversationId || !text) return res.status(400).json({ error: 'conversationId and text are required' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const convos = user.dmConversations || [];
    const conv = convos.find(c => c.id === conversationId);
    if (!conv) return res.status(404).json({ error: 'Conversation not found' });

    const newMsg = {
        sender: 'You',
        text: text.substring(0, 2000),
        timestamp: new Date().toISOString(),
    };
    if (!conv.messages) conv.messages = [];
    conv.messages.push(newMsg);
    conv.lastMessage = text.substring(0, 200);
    conv.lastMessageAt = newMsg.timestamp;

    await dbUpdateFields(user.linkedinId, { dmConversations: convos });
    res.json({ ok: true, message: newMsg, sendViaExtension: true });
});

app.post('/api/dms/ai-reply', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 1) return res.json({ error: 'No AI credits remaining.' });
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') return res.json({ error: 'OpenAI API key not configured.' });

    const { conversationId, style } = req.body;
    if (!conversationId) return res.status(400).json({ error: 'conversationId is required' });

    const user = await dbGetUser(req.session.user.linkedinId);
    const convos = (user && user.dmConversations) || [];
    const conv = convos.find(c => c.id === conversationId);
    if (!conv) return res.status(404).json({ error: 'Conversation not found' });

    const recentMsgs = (conv.messages || []).slice(-6).map(m => `${m.sender}: ${m.text}`).join('\n');
    const styleMap = {
        professional: 'Reply in a professional, polished tone.',
        friendly: 'Reply in a warm, friendly conversational tone.',
        brief: 'Reply concisely in 1-2 short sentences.',
        'follow-up': 'Write a polite follow-up message checking in.',
    };
    const senderName = (user.name || req.session.user.name || 'there').trim();
    const signOffRules = `You are writing as "${senderName}" (the SuperLinkedIn user sending this DM). If you end with a sign-off, use exactly "${senderName}" — their real first and last name. Never use placeholders like [Your Name], [Name], brackets, or "Your Name" in the signature. The recipient is ${conv.participantName || 'the other person'}.`;
    const systemPrompt = `You write LinkedIn DM replies. ${styleMap[style] || styleMap.professional} ${signOffRules} Keep it natural and under 150 words.`;

    const stripDmPlaceholders = (text, realName) => {
        if (!text || !realName) return text;
        let out = text;
        out = out.replace(/\[(?:Your\s*)?Name\]/gi, realName);
        out = out.replace(/\{your\s*name\}/gi, realName);
        return out;
    };

    try {
        const aiRes = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
            body: JSON.stringify({
                model: 'gpt-4o-mini', max_tokens: 250,
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: `Recent conversation with ${conv.participantName || 'this contact'}:\n\n${recentMsgs}\n\nWrite the next reply from ${senderName} (the person sending from SuperLinkedIn). End with a normal closing line signed "${senderName}" if appropriate.` },
                ],
            }),
        });
        const data = await aiRes.json();
        await consumeCredit(req.session, 1);
        const raw = data.choices?.[0]?.message?.content || '';
        res.json({ reply: stripDmPlaceholders(raw, senderName) });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/dms/auto-followup', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    res.json({ rules: (user && user.dmAutoFollowup) || [] });
});

app.post('/api/dms/auto-followup', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const { triggerDays, messageTemplate, style, active } = req.body;
    if (!triggerDays || !messageTemplate) return res.status(400).json({ error: 'triggerDays and messageTemplate are required' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const rules = user.dmAutoFollowup || [];
    const newRule = {
        id: 'dmfu-' + Date.now(),
        triggerDays: parseInt(triggerDays),
        messageTemplate: messageTemplate.substring(0, 500),
        style: style || 'professional',
        active: active !== false,
        createdAt: new Date().toISOString(),
    };
    rules.push(newRule);
    await dbUpdateFields(user.linkedinId, { dmAutoFollowup: rules });
    res.json({ ok: true, rule: newRule });
});

app.delete('/api/dms/auto-followup/:id', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const rules = (user.dmAutoFollowup || []).filter(r => r.id !== req.params.id);
    await dbUpdateFields(user.linkedinId, { dmAutoFollowup: rules });
    res.json({ ok: true });
});

app.get('/api/dms/:id', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    const convos = (user && user.dmConversations) || [];
    const conv = convos.find(c => c.id === req.params.id);
    if (!conv) return res.status(404).json({ error: 'Conversation not found' });
    res.json({ conversation: conv });
});

app.put('/api/dms/:id/labels', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const { labels } = req.body;
    if (!Array.isArray(labels)) return res.status(400).json({ error: 'labels must be an array' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const convos = user.dmConversations || [];
    const conv = convos.find(c => c.id === req.params.id);
    if (!conv) return res.status(404).json({ error: 'Conversation not found' });
    conv.labels = labels;
    await dbUpdateFields(user.linkedinId, { dmConversations: convos });
    res.json({ ok: true, labels: conv.labels });
});

// ---------- BULK DMs (variable substitution + rate-limited send queue) ----------

const BULK_DM_DAILY_LIMIT = 25;
const BULK_DM_MONTHLY_LIMIT = 500;
const BULK_DM_MIN_INTERVAL_MS = 60 * 1000;       // 1 min
const BULK_DM_MAX_INTERVAL_MS = 2 * 60 * 1000;   // 2 min
const BULK_DM_MAX_LENGTH = 1500;
const BULK_DM_PER_RECIPIENT_COOLDOWN_MS = 24 * 60 * 60 * 1000;

function renderBulkDmTemplate(template, recipient) {
    const name = (recipient.name || '').trim();
    const nameParts = name.split(/\s+/).filter(Boolean);
    const first = nameParts[0] || name;
    const last = nameParts.length > 1 ? nameParts.slice(1).join(' ') : '';
    return String(template || '')
        .replace(/\[full_name\]/gi, name || 'there')
        .replace(/\[first_name\]/gi, first || 'there')
        .replace(/\[last_name\]/gi, last || '')
        .replace(/\[name\]/gi, name || 'there')
        .replace(/\[first\]/gi, first || 'there');
}

function todayStartIso() {
    const d = new Date();
    d.setHours(0, 0, 0, 0);
    return d.toISOString();
}
function monthStartIso() {
    const d = new Date();
    d.setDate(1);
    d.setHours(0, 0, 0, 0);
    return d.toISOString();
}

function bulkDmCounts(history) {
    const dayStart = new Date(todayStartIso()).getTime();
    const monthStart = new Date(monthStartIso()).getTime();
    let today = 0, month = 0;
    (history || []).forEach(h => {
        if (h.status !== 'sent') return;
        const t = h.sentAt ? new Date(h.sentAt).getTime() : 0;
        if (t >= monthStart) month++;
        if (t >= dayStart) today++;
    });
    return { today, month };
}

function nextSendDelay() {
    return BULK_DM_MIN_INTERVAL_MS + Math.floor(Math.random() * (BULK_DM_MAX_INTERVAL_MS - BULK_DM_MIN_INTERVAL_MS));
}

/** AI-generated DM template (uses [first_name], [full_name], [last_name] placeholders) */
app.post('/api/bulk-dms/ai-generate', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const credits = await getCreditsForSession(req.session);
    if (credits.remaining < 1) return res.json({ error: 'No AI credits remaining.' });
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey || apiKey === 'sk-your-openai-api-key-here') return res.json({ error: 'OpenAI API key not configured.' });

    const { tone } = req.body || {};
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const aboutYou = user.aboutYou || '';
    const customRules = (user.customRules || '').slice(0, 800);
    const interests = (user.interests || []).join(', ');
    const writingProfile = user.writingProfile || {};
    const topTags = Object.entries(writingProfile).sort((a, b) => b[1] - a[1]).map(e => e[0]).slice(0, 5);

    const toneMap = {
        auto: topTags.length ? `voice leaning toward: ${topTags.join(', ')}` : 'professional, authentic',
        professional: 'professional, polished, concise',
        casual: 'casual, warm, conversational',
        motivational: 'motivational, uplifting',
        storytelling: 'personal, story-led',
        educational: 'helpful, educational',
        contrarian: 'bold, memorable',
        humorous: 'humorous, light',
    };
    const toneLine = toneMap[tone] || toneMap.auto;
    const senderName = (user.name || req.session.user.name || 'there').trim();
    const dmOnboardingStyle = onboardingStyleInjectionForAi(user, 3);

    const systemPrompt = `You write LinkedIn direct message TEMPLATES for bulk personalized outreach. Output ONLY the message body text — no title, no quotation marks, no markdown code fences.

The message MUST include at least one of these exact tokens for the recipient: [first_name], [full_name], or [last_name] — square brackets as shown. You may use more than one. Do not use real example names for recipients; only these placeholders for names.

Keep the message at or under ${BULK_DM_MAX_LENGTH} characters. Avoid hashtags unless essential.`;

    const userPrompt = `Write one LinkedIn DM template to send to many connections.

Tone: ${toneLine}.
You are writing AS: ${senderName}.
${aboutYou ? `About the sender: ${aboutYou}\n` : ''}${interests ? `Topics/interests: ${interests}\n` : ''}${customRules ? `User rules (follow when compatible): ${customRules}\n` : ''}${dmOnboardingStyle ? `${dmOnboardingStyle.replace(/^\n+/, '')}\n` : ''}
Make it read naturally when [first_name], [full_name], and [last_name] are replaced per recipient.`;

    try {
        const aiRes = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
            body: JSON.stringify({
                model: 'gpt-4o-mini',
                max_tokens: 550,
                temperature: 0.75,
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt },
                ],
            }),
        });
        const data = await aiRes.json();
        let message = (data.choices?.[0]?.message?.content || '').trim();
        if (!message) {
            return res.json({ error: 'AI returned empty text' });
        }
        if ((message.startsWith('"') && message.endsWith('"')) || (message.startsWith("'") && message.endsWith("'"))) {
            message = message.slice(1, -1).trim();
        }
        if (message.length > BULK_DM_MAX_LENGTH) {
            message = message.substring(0, BULK_DM_MAX_LENGTH - 3) + '...';
        }
        await consumeCredit(req.session, 1);
        res.json({ message });
    } catch (err) {
        console.error('bulk-dms ai-generate', err);
        res.json({ error: 'Failed to generate message' });
    }
});

app.get('/api/bulk-dms', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const queue = user.bulkDmQueue || [];
    const history = user.bulkDmHistory || [];
    const counts = bulkDmCounts(history.concat(queue));
    res.json({
        queue,
        history,
        counts,
        limits: { daily: BULK_DM_DAILY_LIMIT, monthly: BULK_DM_MONTHLY_LIMIT },
        paused: !!user.bulkDmPaused,
    });
});

app.post('/api/bulk-dms/queue', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const { template, recipients } = req.body;
    if (!template || typeof template !== 'string' || !template.trim()) {
        return res.status(400).json({ error: 'Message template is required' });
    }
    if (template.length > BULK_DM_MAX_LENGTH) {
        return res.status(400).json({ error: `Message must be under ${BULK_DM_MAX_LENGTH} characters` });
    }
    if (!Array.isArray(recipients) || recipients.length === 0) {
        return res.status(400).json({ error: 'At least one recipient is required' });
    }

    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const history = user.bulkDmHistory || [];
    const queue = user.bulkDmQueue || [];
    const counts = bulkDmCounts(history.concat(queue));
    const remainingDay = Math.max(0, BULK_DM_DAILY_LIMIT - counts.today);
    const remainingMonth = Math.max(0, BULK_DM_MONTHLY_LIMIT - counts.month);
    const allowed = Math.min(remainingDay, remainingMonth);
    if (allowed <= 0) {
        return res.status(429).json({ error: 'You have reached your bulk-DM limit. Try again later or upgrade your plan.' });
    }

    // Skip recipients we already messaged in the last 24h to avoid spamming
    const cooldownThreshold = Date.now() - BULK_DM_PER_RECIPIENT_COOLDOWN_MS;
    const recentlyMessaged = new Set(
        history
            .filter(h => h.status === 'sent' && h.sentAt && new Date(h.sentAt).getTime() > cooldownThreshold)
            .map(h => ((h.recipient && h.recipient.handle) || (h.recipient && h.recipient.name) || '').toLowerCase())
    );

    let scheduledFor = Date.now();
    // Stagger from now using the 1-2 min interval
    let lastQueuedTs = (queue[queue.length - 1] && new Date(queue[queue.length - 1].scheduledFor || 0).getTime()) || Date.now();
    let added = 0;
    let skipped = 0;
    const newItems = [];
    for (const r of recipients) {
        if (added >= allowed) { skipped++; continue; }
        const key = ((r.handle || r.name || '') + '').toLowerCase();
        if (!key) { skipped++; continue; }
        if (recentlyMessaged.has(key)) { skipped++; continue; }

        lastQueuedTs = Math.max(lastQueuedTs, Date.now()) + nextSendDelay();
        const item = {
            id: 'bdm-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8),
            recipient: {
                name: (r.name || '').trim(),
                handle: (r.handle || '').trim(),
                picture: r.picture || '',
            },
            template,
            renderedMessage: renderBulkDmTemplate(template, r),
            status: 'pending',
            scheduledFor: new Date(lastQueuedTs).toISOString(),
            createdAt: new Date().toISOString(),
        };
        queue.push(item);
        newItems.push(item);
        added++;
    }

    await dbUpdateFields(user.linkedinId, { bulkDmQueue: queue });
    res.json({ ok: true, added, skipped, items: newItems });
});

app.post('/api/bulk-dms/pause', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const { paused } = req.body;
    await dbUpdateFields(req.session.user.linkedinId, { bulkDmPaused: !!paused });
    res.json({ ok: true, paused: !!paused });
});

app.post('/api/bulk-dms/clear', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const queue = user.bulkDmQueue || [];
    const history = user.bulkDmHistory || [];
    queue.forEach(q => {
        if (q.status === 'pending') {
            history.push({ ...q, status: 'cancelled', cancelledAt: new Date().toISOString() });
        }
    });
    await dbUpdateFields(user.linkedinId, { bulkDmQueue: [], bulkDmHistory: history });
    res.json({ ok: true });
});

app.post('/api/bulk-dms/cancel/:id', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const queue = user.bulkDmQueue || [];
    const history = user.bulkDmHistory || [];
    const idx = queue.findIndex(q => q.id === req.params.id);
    if (idx < 0) return res.status(404).json({ error: 'Queue item not found' });
    const [item] = queue.splice(idx, 1);
    item.status = 'cancelled';
    item.cancelledAt = new Date().toISOString();
    history.push(item);
    await dbUpdateFields(user.linkedinId, { bulkDmQueue: queue, bulkDmHistory: history });
    res.json({ ok: true });
});

// Called by the dashboard tick: returns the next due item the extension should
// deliver (and immediately marks it "sending" so we don't double-send). The
// browser then calls /api/bulk-dms/result with success/failure.
app.post('/api/bulk-dms/next-due', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    if (user.bulkDmPaused) return res.json({ item: null, paused: true });

    const queue = user.bulkDmQueue || [];
    const history = user.bulkDmHistory || [];
    const counts = bulkDmCounts(history.concat(queue));
    if (counts.today >= BULK_DM_DAILY_LIMIT || counts.month >= BULK_DM_MONTHLY_LIMIT) {
        return res.json({ item: null, limitReached: true });
    }

    const now = Date.now();
    const idx = queue.findIndex(q => q.status === 'pending' && new Date(q.scheduledFor || 0).getTime() <= now);
    if (idx < 0) return res.json({ item: null });

    queue[idx].status = 'sending';
    queue[idx].pickedUpAt = new Date().toISOString();
    await dbUpdateFields(user.linkedinId, { bulkDmQueue: queue });
    res.json({ item: queue[idx] });
});

app.post('/api/bulk-dms/result', async (req, res) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not authenticated' });
    const { id, ok, error } = req.body;
    if (!id) return res.status(400).json({ error: 'id required' });
    const user = await dbGetUser(req.session.user.linkedinId);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const queue = user.bulkDmQueue || [];
    const history = user.bulkDmHistory || [];
    const idx = queue.findIndex(q => q.id === id);
    if (idx < 0) return res.status(404).json({ error: 'Queue item not found' });
    const [item] = queue.splice(idx, 1);
    if (ok) {
        item.status = 'sent';
        item.sentAt = new Date().toISOString();
    } else {
        item.status = 'failed';
        item.failedAt = new Date().toISOString();
        item.error = (error || 'Send failed').substring(0, 200);
    }
    history.push(item);
    if (history.length > 500) history.splice(0, history.length - 500);
    await dbUpdateFields(user.linkedinId, { bulkDmQueue: queue, bulkDmHistory: history });
    res.json({ ok: true, item });
});

// ---------- POST SCHEDULER ----------

async function publishToLinkedIn(accessToken, linkedinId, text, audience) {
    const personUrn = `urn:li:person:${linkedinId}`;
    const visibility = audience === 'connections' ? 'CONNECTIONS' : 'PUBLIC';

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

    if (!response.ok) {
        const errText = await response.text();
        throw new Error(`LinkedIn API ${response.status}: ${errText}`);
    }

    const data = await response.json();
    return data.id || null;
}

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
                    const token = user.accessToken;
                    if (!token) {
                        item.status = 'failed';
                        item.error = 'No access token. User needs to sign in again.';
                        item.failedAt = now.toISOString();
                        changed = true;
                        console.error(`[Scheduler] No access token for user ${user.linkedinId}, marking as failed`);
                        continue;
                    }

                    try {
                        const postId = await publishToLinkedIn(token, user.linkedinId, item.text, item.audience || 'public');
                        item.status = 'posted';
                        item.postedAt = now.toISOString();
                        item.postedAtDay = now.getDay();
                        item.postedAtHour = now.getHours();
                        item.postId = postId;
                        item.postUrl = postId ? `https://www.linkedin.com/feed/update/${postId}/` : null;
                        console.log(`[Scheduler] Successfully published post for ${user.linkedinId}: ${postId}`);
                    } catch (pubErr) {
                        item.status = 'failed';
                        item.error = pubErr.message;
                        item.failedAt = now.toISOString();
                        console.error(`[Scheduler] Failed to publish for ${user.linkedinId}: ${pubErr.message}`);
                    }
                    changed = true;
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
