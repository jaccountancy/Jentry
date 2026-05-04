import crypto from "crypto";
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
import express from "express";
import { google } from "googleapis";
import MailComposer from "mailcomposer";
import multer from "multer";
import { Pool } from "pg";
import sharp from "sharp";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const requiredEnvironmentVariables = [
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REFRESH_TOKEN",
    "GMAIL_SENDER",
    "DATABASE_URL",
    "XERO_CLIENT_ID",
    "XERO_CLIENT_SECRET"
];

const optionalEnvironmentVariables = {
    openAIAPIKey: process.env.OPENAI_API_KEY?.trim() || "",
    openAIModel: process.env.OPENAI_MODEL?.trim() || "gpt-4.1-mini"
};

const SERVER_SUPER_ADMIN_EMAILS = new Set(
    (process.env.SERVER_SUPER_ADMIN_EMAILS || "")
        .split(",")
        .map((email) => email.trim().toLowerCase())
        .filter(Boolean)
);

for (const key of requiredEnvironmentVariables) {
    if (!process.env[key] || !process.env[key].trim()) {
        throw new Error(`Missing required environment variable: ${key}`);
    }
}

const app = express();
app.get("/stripe/webhook", (_request, response) => {
    response.status(200).json({ ok: true, endpoint: "stripe-webhook" });
});
app.head("/stripe/webhook", (_request, response) => {
    response.status(200).end();
});
app.post("/stripe/webhook", express.raw({ type: "application/json", limit: "2mb" }), async (request, response) => {
    try {
        if (!STRIPE_WEBHOOK_SECRET) {
            response.status(503).json({ message: "Stripe webhook secret is not configured." });
            return;
        }

        const signature = normalizeOptionalString(request.header("stripe-signature"));
        if (!signature) {
            response.status(400).json({ message: "Missing Stripe signature." });
            return;
        }

        const event = verifyStripeWebhookEvent(request.body, signature, STRIPE_WEBHOOK_SECRET);
        await handleStripeWebhookEvent(event);
        response.json({ received: true });
    } catch (error) {
        response.status(400).json({ message: error instanceof Error ? error.message : "Invalid Stripe webhook." });
    }
});
app.use(express.json({ limit: "25mb" }));
app.use(express.urlencoded({ extended: true, limit: "25mb" }));

const upload = multer({
    storage: multer.memoryStorage(),
    limits: {
        fileSize: 25 * 1024 * 1024
    }
});

const port = Number(process.env.PORT || 3001);

const oauth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET
);

oauth2Client.setCredentials({
    refresh_token: process.env.GOOGLE_REFRESH_TOKEN
});

const gmail = google.gmail({
    version: "v1",
    auth: oauth2Client
});

const XERO_AUTHORIZE_URL = "https://login.xero.com/identity/connect/authorize";
const XERO_TOKEN_URL = "https://identity.xero.com/connect/token";
const XERO_CONNECTIONS_URL = "https://api.xero.com/connections";
const XERO_INVOICES_URL = "https://api.xero.com/api.xro/2.0/Invoices";
const XERO_INVOICE_HISTORY_URL = (invoiceId) => `${XERO_INVOICES_URL}/${encodeURIComponent(invoiceId)}/History`;
const XERO_ACCOUNTS_URL = "https://api.xero.com/api.xro/2.0/Accounts";
const XERO_BANK_TRANSACTIONS_URL = "https://api.xero.com/api.xro/2.0/BankTransactions";
const XERO_CONTACTS_URL = "https://api.xero.com/api.xro/2.0/Contacts";
const STRIPE_API_BASE_URL = "https://api.stripe.com/v1";
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY?.trim() || "";
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET?.trim() || "";
const STRIPE_PRICE_GBP_MONTHLY_ID = process.env.STRIPE_PRICE_GBP_MONTHLY_ID?.trim() || "";
const JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE = Number(process.env.JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE || 300);
const JENTRY_PAID_SUBSCRIPTION_AMOUNT_GBP = (JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE / 100).toFixed(2);
const JACCOUNTANCY_FREE_CLIENT_DOMAINS = new Set(
    (process.env.JACCOUNTANCY_FREE_CLIENT_DOMAINS || "jaccountancy.co.uk")
        .split(",")
        .map((value) => value.trim().toLowerCase())
        .filter(Boolean)
);


class XeroAPIError extends Error {
    constructor({ message, code = "XERO_UPSTREAM_ERROR", status = 502, requiresReconnect = false, upstreamStatus = null, upstreamBody = null } = {}) {
        super(message || "Xero request failed.");
        this.name = "XeroAPIError";
        this.code = code;
        this.status = status;
        this.requiresReconnect = requiresReconnect;
        this.upstreamStatus = upstreamStatus;
        this.upstreamBody = upstreamBody;
    }
}

function xeroErrorResponse(error, fallbackMessage = "Xero request failed.") {
    if (error instanceof XeroAPIError) {
        return {
            status: error.status,
            body: compactObject({
                code: error.code,
                message: error.message,
                requiresReconnect: error.requiresReconnect,
                upstreamStatus: error.upstreamStatus,
                upstreamBody: error.upstreamBody
            })
        };
    }

    return {
        status: 500,
        body: {
            code: "XERO_BACKEND_ERROR",
            message: error instanceof Error ? error.message : fallbackMessage,
            requiresReconnect: false
        }
    };
}

function sendXeroError(response, error, fallbackMessage = "Xero request failed.") {
    const normalized = xeroErrorResponse(error, fallbackMessage);
    response.status(normalized.status).json(normalized.body);
}

async function readXeroResponse(response) {
    const text = await response.text().catch(() => "");
    if (!text) return { raw: "", data: null };

    try {
        return { raw: text, data: JSON.parse(text) };
    } catch {
        return { raw: text, data: null };
    }
}

function isXeroAuthStatus(status) {
    return status === 401 || status === 403;
}

function classifyXeroError(status, body, fallbackMessage = "Xero request failed.") {
    const message =
        body?.error_description ||
        body?.error ||
        body?.Message ||
        body?.Detail ||
        body?.Elements?.[0]?.ValidationErrors?.map((entry) => entry.Message).join("; ") ||
        fallbackMessage;

    if (status === 401) {
        return new XeroAPIError({
            message: "Xero authorization expired.",
            code: "XERO_AUTH_EXPIRED",
            status: 401,
            requiresReconnect: true,
            upstreamStatus: status,
            upstreamBody: body
        });
    }

    if (status === 403) {
        return new XeroAPIError({
            message: message || "Xero tenant access was denied.",
            code: "XERO_TENANT_ACCESS_DENIED",
            status: 403,
            requiresReconnect: false,
            upstreamStatus: status,
            upstreamBody: body
        });
    }

    if (status === 409) {
        return new XeroAPIError({
            message,
            code: "XERO_DUPLICATE_OR_CONFLICT",
            status: 409,
            requiresReconnect: false,
            upstreamStatus: status,
            upstreamBody: body
        });
    }

    return new XeroAPIError({
        message,
        code: status >= 500 ? "XERO_UPSTREAM_UNAVAILABLE" : "XERO_UPSTREAM_ERROR",
        status: status >= 500 ? 503 : 502,
        requiresReconnect: isXeroAuthStatus(status),
        upstreamStatus: status,
        upstreamBody: body
    });
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});


const SUSPENDED_ACCOUNT_ERROR = {
    code: "ACCOUNT_SUSPENDED",
    message: "Suspended. Please contact Jaccountancy."
};

const PAID_SUBSCRIPTION_REQUIRED_ERROR = {
    code: "PAID_SUBSCRIPTION_REQUIRED",
    message: `We understand that you're not a Jaccountancy client. Jentry is free only for Jaccountancy clients. To continue using the software, start the Â£${JENTRY_PAID_SUBSCRIPTION_AMOUNT_GBP} per month subscription.`
};

function sendSuspendedResponse(response) {
    response.status(403).json(SUSPENDED_ACCOUNT_ERROR);
}

function sendPaidSubscriptionRequiredResponse(response) {
    response.status(402).json(PAID_SUBSCRIPTION_REQUIRED_ERROR);
}

const XERO_OAUTH_START_TOKEN_TTL_MS = Number(process.env.XERO_OAUTH_START_TOKEN_TTL_MS || 10 * 60 * 1000);

function xeroOAuthStartTokenSecret() {
    return normalizeOptionalString(
        process.env.JENTRY_AUTH_TOKEN_SECRET ||
        process.env.XERO_OAUTH_START_TOKEN_SECRET ||
        process.env.XERO_CLIENT_SECRET ||
        process.env.GOOGLE_CLIENT_SECRET
    );
}

function signXeroOAuthStartTokenPayload(encodedPayload) {
    const secret = xeroOAuthStartTokenSecret();
    if (!secret) {
        throw new Error("Missing Xero OAuth start token secret.");
    }

    return toBase64Url(crypto.createHmac("sha256", secret).update(encodedPayload).digest());
}

function createXeroOAuthStartToken({ email, displayName = "", accountId = "" } = {}) {
    const normalizedEmail = normalizeEmail(email);
    if (!normalizedEmail) {
        throw new Error("Missing authenticated email for Xero OAuth start token.");
    }

    const payload = {
        email: normalizedEmail,
        displayName: normalizeOptionalString(displayName) || normalizedEmail,
        accountId: normalizeOptionalString(accountId) || null,
        issuedAt: Date.now(),
        expiresAt: Date.now() + XERO_OAUTH_START_TOKEN_TTL_MS,
        nonce: crypto.randomUUID()
    };

    const encodedPayload = toBase64Url(JSON.stringify(payload));
    const signature = signXeroOAuthStartTokenPayload(encodedPayload);
    return `${encodedPayload}.${signature}`;
}

function verifyXeroOAuthStartToken(token) {
    const normalizedToken = normalizeOptionalString(token);
    if (!normalizedToken) return null;

    const [encodedPayload, signature] = normalizedToken.split(".");
    if (!encodedPayload || !signature) return null;

    const expectedSignature = signXeroOAuthStartTokenPayload(encodedPayload);
    const signatureBuffer = Buffer.from(signature);
    const expectedBuffer = Buffer.from(expectedSignature);

    if (signatureBuffer.length !== expectedBuffer.length || !crypto.timingSafeEqual(signatureBuffer, expectedBuffer)) {
        return null;
    }

    try {
        const paddedPayload = encodedPayload.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (encodedPayload.length % 4)) % 4);
        const payload = JSON.parse(Buffer.from(paddedPayload, "base64").toString("utf8"));

        if (!payload?.expiresAt || Date.now() > Number(payload.expiresAt)) {
            return null;
        }

        const email = normalizeEmail(payload.email);
        if (!email) return null;

        return {
            email,
            displayName: normalizeOptionalString(payload.displayName) || email,
            accountId: normalizeOptionalString(payload.accountId) || null
        };
    } catch {
        return null;
    }
}

async function getOrCreateUserFromEmail({ email, displayName = "", loginEvent = false } = {}) {
    const normalizedEmail = normalizeEmail(email);
    if (!normalizedEmail) return null;

    await ensureCoreModelTables();

    const result = await pool.query(
        `
        INSERT INTO users (email, display_name, role, is_super_admin, last_seen_at, last_login_at, updated_at)
        VALUES ($1, $2, 'user', $4, now(), COALESCE($3::timestamptz, now()), now())
        ON CONFLICT (email)
        DO UPDATE SET
            display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), users.display_name),
            is_super_admin = users.is_super_admin OR EXCLUDED.is_super_admin,
            last_seen_at = now(),
            updated_at = now()
        RETURNING *
        `,
        [
            normalizedEmail,
            normalizeOptionalString(displayName) || normalizedEmail,
            loginEvent ? new Date().toISOString() : null,
            SERVER_SUPER_ADMIN_EMAILS.has(normalizedEmail)
        ]
    );

    return mapUserRow(result.rows[0]);
}

function legacyXeroAccountEmail(accountId) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) return null;

    const digest = crypto
        .createHash("sha256")
        .update(normalizedAccountId)
        .digest("hex")
        .slice(0, 24);

    return `xero-account-${digest}@jentry.local`;
}

async function getOrCreateLegacyXeroUserForAccount({ accountId, displayName = "" } = {}) {
    const email = legacyXeroAccountEmail(accountId);
    if (!email) return null;

    return getOrCreateUserFromEmail({
        email,
        displayName: normalizeOptionalString(displayName) || "Jentry Xero Connection",
        loginEvent: false
    });
}

async function requireActiveUserOrXeroStartToken(request, response, next) {
    const tokenPayload = verifyXeroOAuthStartToken(request.query?.xeroAuthToken || request.query?.authToken || request.query?.token);

    if (tokenPayload) {
        const requestedAccountId = normalizeOptionalString(request.query?.accountId);
        if (tokenPayload.accountId && requestedAccountId && tokenPayload.accountId !== requestedAccountId) {
            response.status(403).json({ code: "XERO_AUTH_TOKEN_ACCOUNT_MISMATCH", message: "Xero login token does not match this account." });
            return;
        }

        const user = await getOrCreateUserFromEmail({
            email: tokenPayload.email,
            displayName: tokenPayload.displayName,
            loginEvent: true
        });

        if (!user) {
            response.status(401).json({ code: "AUTH_REQUIRED", message: "Authentication required." });
            return;
        }

        if (user.isSuspended) {
            sendSuspendedResponse(response);
            return;
        }

        request.authenticatedUser = user;
        next();
        return;
    }

    await requireActiveUser(request, response, next);
}

async function requireActiveUserOrLegacyXeroAccount(request, response, next) {
    try {
        const user = await getOrCreateAuthenticatedUser(request);
        if (user) {
            if (user.isSuspended) {
                sendSuspendedResponse(response);
                return;
            }

            request.authenticatedUser = user;
            next();
            return;
        }

        const accountId = normalizeOptionalString(
            request.body?.accountId ||
            request.query?.accountId
        );

        if (!accountId) {
            response.status(401).json({ code: "AUTH_REQUIRED", message: "Authentication required." });
            return;
        }

        // Compatibility mode for the current iOS client, which still calls Xero routes without a real app session.
        const legacyUser = await getOrCreateLegacyXeroUserForAccount({
            accountId,
            displayName: request.headers["x-user-display-name"] || request.body?.displayName
        });

        if (!legacyUser) {
            response.status(401).json({ code: "AUTH_REQUIRED", message: "Authentication required." });
            return;
        }

        if (legacyUser.isSuspended) {
            sendSuspendedResponse(response);
            return;
        }

        request.authenticatedUser = legacyUser;
        next();
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to authenticate user." });
    }
}

function extractAuthenticatedEmail(request) {
    // Prefer a real authenticated identity from the platform/proxy/JWT.
    // Keep x-user-email for backwards compatibility, but production should not rely on it.
    const directEmail = normalizeEmail(
        request.headers["x-authenticated-user-email"] ||
        request.headers["x-jentry-user-email"] ||
        request.headers["x-forwarded-email"] ||
        request.headers["x-ms-client-principal-name"] ||
        request.headers["cf-access-authenticated-user-email"] ||
        stripGoogleAuthenticatedUserPrefix(request.headers["x-goog-authenticated-user-email"]) ||
        request.headers["x-user-email"] ||
        request.body?.authenticatedUserEmail ||
        request.body?.connectedUserEmail ||
        request.body?.userEmail ||
        request.body?.submittedByEmail ||
        request.query?.authenticatedUserEmail ||
        request.query?.userEmail ||
        request.query?.connectedUserEmail
    );

    if (directEmail) return directEmail;

    const authorization = normalizeOptionalString(request.headers.authorization);
    const token = authorization.toLowerCase().startsWith("bearer ")
        ? authorization.slice(7).trim()
        : "";
    const claims = parseJWTPayload(token);
    return normalizeEmail(
        claims?.email ||
        claims?.preferred_username ||
        claims?.upn ||
        claims?.unique_name ||
        claims?.user_email
    );
}

function stripGoogleAuthenticatedUserPrefix(value) {
    const normalized = normalizeOptionalString(value);
    if (!normalized) return "";

    return normalized.replace(/^accounts\.google\.com:/i, "");
}

async function getOrCreateAuthenticatedUser(request) {
    const email = extractAuthenticatedEmail(request);
    if (!email) return null;

    const displayName = normalizeOptionalString(
        request.headers["x-user-display-name"] ||
        request.body?.displayName ||
        request.body?.submittedByName
    ) || email;

    return getOrCreateUserFromEmail({
        email,
        displayName,
        loginEvent: Boolean(request.headers["x-login-event"])
    });
}

async function requireAuthenticatedUser(request, response, next) {
    try {
        const user = await getOrCreateAuthenticatedUser(request);
        if (!user) {
            response.status(401).json({ code: "AUTH_REQUIRED", message: "Authentication required." });
            return;
        }
        request.authenticatedUser = user;
        next();
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to authenticate user." });
    }
}

async function requireActiveUser(request, response, next) {
    await requireAuthenticatedUser(request, response, async () => {
        if (request.authenticatedUser?.isSuspended) {
            sendSuspendedResponse(response);
            return;
        }
        next();
    });
}

async function requireAdminUser(request, response, next) {
    await requireAuthenticatedUser(request, response, async () => {
        const user = request.authenticatedUser;
        if (user?.isSuspended) {
            sendSuspendedResponse(response);
            return;
        }
        if (!user?.isSuperAdmin) {
            response.status(403).json({ code: "ADMIN_FORBIDDEN", message: "Super admin access required." });
            return;
        }
        next();
    });
}

function requireSuperAdmin(request, response, next) {
    const headerEmail = normalizeEmail(request.header("X-Jentry-Super-Admin-Email"));
    const authenticatedEmail = normalizeEmail(request.authenticatedUser?.email);

    if (!headerEmail || headerEmail !== authenticatedEmail || !request.authenticatedUser?.isSuperAdmin) {
        response.status(403).json({ message: "Forbidden" });
        return;
    }

    request.superAdminEmail = authenticatedEmail;
    next();
}

async function requireAuthorizedAccountAccess(request, response, accountId, { allowCreate = false } = {}) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) {
        response.status(400).json({ code: "ACCOUNT_ID_REQUIRED", message: "Missing accountId." });
        return false;
    }

    const user = request.authenticatedUser;
    if (!user) {
        response.status(401).json({ code: "AUTH_REQUIRED", message: "Authentication required." });
        return false;
    }

    if (user.isSuperAdmin) {
        return true;
    }

    await ensureCoreModelTables();

    const result = await pool.query(
        `
        SELECT
            a.id AS account_id,
            a.assigned_user_id,
            a.status,
            a.billing_mode,
            a.subscription_status,
            m.user_id AS member_user_id
        FROM accounts a
        LEFT JOIN memberships m
            ON m.account_id = a.id
           AND m.user_id = $2
        WHERE a.id = $1
        LIMIT 1
        `,
        [normalizedAccountId, user.id]
    );

    const account = result.rows[0] || null;

    if (!account) {
        if (!allowCreate) {
            response.status(403).json({ code: "ACCOUNT_FORBIDDEN", message: "You are not authorised to access this account." });
            return false;
        }

        await upsertAccountRecord({
            accountId: normalizedAccountId,
            assignedUserId: user.id,
            clientEmail: user.email
        });
        await pool.query(
            `INSERT INTO memberships (user_id, account_id, role) VALUES ($1, $2, 'owner') ON CONFLICT (user_id, account_id) DO NOTHING`,
            [user.id, normalizedAccountId]
        );
        await writeAdminAuditLog(request, {
            action: "workspace.registered",
            targetType: "workspace",
            targetId: normalizedAccountId,
            details: {
                category: "registration",
                actionTitle: "New account registration",
                targetName: normalizedAccountId,
                detail: `${user.email} registered a new Jentry account.`,
                afterSummary: user.email
            }
        });
        return true;
    }

    if (account.status === "suspended") {
        sendSuspendedResponse(response);
        return false;
    }

    if (isBillingBlockedRow(account)) {
        sendPaidSubscriptionRequiredResponse(response);
        return false;
    }

    if (account.assigned_user_id === user.id || account.member_user_id === user.id) {
        return true;
    }

    response.status(403).json({ code: "ACCOUNT_FORBIDDEN", message: "You are not authorised to access this account." });
    return false;
}

app.get("/health", (_request, response) => {
    console.log("Health check received.");
    response.json({ ok: true });
});


app.post("/session/heartbeat", requireActiveUser, async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.body?.accountId);
        const connectedUserEmail = normalizeEmail(request.body?.connectedUserEmail);
        await updatePresence({
            userId: request.authenticatedUser.id,
            accountId,
            connectedUserEmail
        });
        response.json({ ok: true, lastSeenAt: new Date().toISOString() });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to update heartbeat." });
    }
});

app.get("/account/access-status", requireActiveUser, async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.query?.accountId || request.body?.accountId);
        if (!await requireAuthorizedAccountAccess(request, response, accountId, { allowCreate: false })) {
            return;
        }

        await ensureCoreModelTables();
        const result = await pool.query(
            `
            SELECT
                id,
                company_name,
                status,
                billing_mode,
                subscription_status,
                subscription_amount_pence,
                subscription_started_at,
                subscription_current_period_end,
                stripe_customer_id,
                stripe_subscription_id
            FROM accounts
            WHERE id = $1
            LIMIT 1
            `,
            [accountId]
        );

        const row = result.rows[0];
        if (!row) {
            response.status(404).json({ message: "Account not found." });
            return;
        }

        response.json(mapAccountAccessStatusRow(row));
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load account access status." });
    }
});

app.patch("/account/xero-files-email", requireActiveUser, async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.body?.accountId || request.query?.accountId);
        const inboxEmail = normalizeEmail(request.body?.inboxEmail || request.body?.xeroFilesEmail || request.query?.inboxEmail);
        if (!accountId || !inboxEmail) {
            response.status(400).json({ message: "Account ID and Xero Files email are required." });
            return;
        }
        if (!await requireAuthorizedAccountAccess(request, response, accountId, { allowCreate: false })) {
            return;
        }

        await upsertJentryInboxRecord({
            accountId,
            inboxEmail,
            assignedUserEmail: request.authenticatedUser?.email || null,
            assignedUserId: request.authenticatedUser?.id || null,
            updatedBy: request.authenticatedUser?.email || null,
            isActive: true
        });

        await pool.query(
            `UPDATE accounts SET client_email = COALESCE(client_email, $2), updated_at = now() WHERE id = $1`,
            [accountId, request.authenticatedUser?.email || null]
        );

        response.json({ success: true, inboxEmail });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to save Xero Files email." });
    }
});

app.post("/billing/checkout-session", requireActiveUser, async (request, response) => {
    try {
        if (!STRIPE_SECRET_KEY) {
            response.status(503).json({ message: "Stripe secret key is not configured." });
            return;
        }
        if (!STRIPE_PRICE_GBP_MONTHLY_ID) {
            response.status(503).json({ message: "Stripe monthly price ID is not configured." });
            return;
        }

        const accountId = normalizeOptionalString(request.body?.accountId || request.query?.accountId);
        if (!await requireAuthorizedAccountAccess(request, response, accountId, { allowCreate: false })) {
            return;
        }

        await ensureCoreModelTables();
        const accountResult = await pool.query(
            `SELECT * FROM accounts WHERE id = $1 LIMIT 1`,
            [accountId]
        );
        const account = accountResult.rows[0];
        if (!account) {
            response.status(404).json({ message: "Account not found." });
            return;
        }

        const successURL = stripeCheckoutSuccessURL(request);
        const cancelURL = stripeCheckoutCancelURL(request);

        const form = new URLSearchParams();
        form.set("mode", "subscription");
        form.set("success_url", successURL);
        form.set("cancel_url", cancelURL);
        form.set("line_items[0][price]", STRIPE_PRICE_GBP_MONTHLY_ID);
        form.set("line_items[0][quantity]", "1");
        form.set("metadata[accountId]", accountId);
        form.set("metadata[userId]", request.authenticatedUser.id);
        form.set("metadata[userEmail]", request.authenticatedUser.email);
        form.set("subscription_data[metadata][accountId]", accountId);
        form.set("subscription_data[metadata][userId]", request.authenticatedUser.id);
        form.set("subscription_data[metadata][userEmail]", request.authenticatedUser.email);
        form.set("client_reference_id", accountId);
        form.set("customer_email", request.authenticatedUser.email);

        if (account.stripe_customer_id) {
            form.set("customer", account.stripe_customer_id);
        }

        const checkoutResponse = await fetch(`${STRIPE_API_BASE_URL}/checkout/sessions`, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${STRIPE_SECRET_KEY}`,
                "Content-Type": "application/x-www-form-urlencoded"
            },
            body: form
        });

        const checkoutBody = await checkoutResponse.json().catch(() => ({}));
        if (!checkoutResponse.ok) {
            response.status(502).json({ message: checkoutBody?.error?.message || "Unable to create Stripe checkout session." });
            return;
        }

        await writeAdminAuditLog(request, {
            action: "billing.checkout_started",
            targetType: "workspace",
            targetId: accountId,
            details: {
                category: "billing",
                actionTitle: "Started paid subscription checkout",
                targetName: account.company_name || accountId,
                detail: `Stripe checkout started for ${account.company_name || accountId}.`
            }
        });

        response.json({
            success: true,
            checkoutURL: checkoutBody.url,
            sessionId: checkoutBody.id
        });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to create Stripe checkout session." });
    }
});

app.use([
    "/xero/status",
    "/xero/tenants",
    "/xero/select-tenant",
    "/xero/disconnect",
    "/xero/accounts",
    "/xero/match-transactions",
    "/xero/publish-bill",
    "/xero/attach-document",
    "/xero/contacts",
    "/xero/ensure-contact"
], requireActiveUserOrLegacyXeroAccount);

app.use([
    "/jentry/uploads",
    "/jentry/inbox/register",
    "/inbound-email-addresses/register",
    "/inbound-email-submissions",
    "/analyze",
    "/jentry/analyze",
    "/problem-report",
    "/jentry/problem-report"
], requireActiveUser);

app.post("/oauth/xero/start-token", requireActiveUserOrLegacyXeroAccount, async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.body?.accountId || request.query?.accountId);
        if (!accountId) {
            response.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId, { allowCreate: true })) {
            return;
        }

        const returnUri = normalizeOptionalString(request.body?.returnUri || request.query?.returnUri) || xeroAppRedirectURI();
        const xeroAuthToken = createXeroOAuthStartToken({
            email: request.authenticatedUser.email,
            displayName: request.authenticatedUser.displayName || request.authenticatedUser.email,
            accountId
        });

        const startURL = new URL(`${request.protocol}://${request.get("host")}/oauth/xero/start`);
        startURL.searchParams.set("accountId", accountId);
        startURL.searchParams.set("returnUri", returnUri);
        startURL.searchParams.set("xeroAuthToken", xeroAuthToken);

        response.json({
            ok: true,
            xeroAuthToken,
            startURL: startURL.toString(),
            expiresInSeconds: Math.floor(XERO_OAUTH_START_TOKEN_TTL_MS / 1000)
        });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to create Xero login token." });
    }
});

app.get("/oauth/xero/start", requireActiveUserOrXeroStartToken, async (request, response) => {
    try {
        if (!xeroConfigured()) {
            response.status(500).json({ message: "Xero OAuth is not configured on the backend." });
            return;
        }

        const accountId = normalizeOptionalString(request.query.accountId);
        if (!accountId) {
            response.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId, { allowCreate: true })) {
            return;
        }

        const returnUri = normalizeOptionalString(request.query.returnUri) || xeroAppRedirectURI();
        const { verifier, challenge } = createPKCEPair();
        const state = crypto.randomUUID();

        await createXeroAuthSession({
            state,
            accountId,
            returnUri,
            verifier
        });

        const authorizeURL = new URL(XERO_AUTHORIZE_URL);
        authorizeURL.searchParams.set("response_type", "code");
        authorizeURL.searchParams.set("client_id", xeroClientID());
        authorizeURL.searchParams.set("redirect_uri", xeroRedirectURI());
        authorizeURL.searchParams.set("scope", xeroScopes());
        authorizeURL.searchParams.set("state", state);
        authorizeURL.searchParams.set("code_challenge", challenge);
        authorizeURL.searchParams.set("code_challenge_method", "S256");

        response.redirect(authorizeURL.toString());
    } catch (error) {
        const message = error instanceof Error ? error.message : "Unable to start Xero authorization.";
        response.status(500).json({ message });
    }
});

app.get("/oauth/xero/callback", async (request, response) => {
    const state = normalizeOptionalString(request.query.state);
    const code = normalizeOptionalString(request.query.code);
    let authSession = null;
    let fallbackReturnURL = xeroAppRedirectURI();
    let tokenExchangeSucceeded = false;
    let tokensStored = false;

    try {
        authSession = state ? await getXeroAuthSession(state) : null;
        fallbackReturnURL = authSession?.returnUri || xeroAppRedirectURI();

        console.log("Xero OAuth callback received.", {
            accountId: authSession?.accountId || null,
            state: state || null,
            hasCode: Boolean(code),
            codePreview: code ? `${code.slice(0, 6)}...` : null
        });

        if (!authSession) {
            console.warn("Xero OAuth callback rejected: missing or expired auth session.", {
                state: state || null,
                hasCode: Boolean(code)
            });

            response.redirect(
                buildReturnURL(fallbackReturnURL, {
                    status: "error",
                    message: "Missing or expired Xero authorization session."
                })
            );
            return;
        }

        await deleteXeroAuthSession(state);

        const authError = normalizeOptionalString(request.query.error);
        if (authError) {
            console.warn("Xero OAuth callback returned an authorization error.", {
                accountId: authSession.accountId,
                state: state || null,
                authError
            });

            response.redirect(
                buildReturnURL(authSession.returnUri, {
                    accountId: authSession.accountId,
                    status: "error",
                    message: authError
                })
            );
            return;
        }

        if (!code) {
            console.warn("Xero OAuth callback rejected: authorization code missing.", {
                accountId: authSession.accountId,
                state: state || null
            });

            response.redirect(
                buildReturnURL(authSession.returnUri, {
                    accountId: authSession.accountId,
                    status: "error",
                    message: "Authorization code missing."
                })
            );
            return;
        }

        const tokenData = await exchangeXeroToken({
            grant_type: "authorization_code",
            client_id: xeroClientID(),
            client_secret: xeroClientSecret(),
            code,
            redirect_uri: xeroRedirectURI(),
            code_verifier: authSession.verifier
        });
        tokenExchangeSucceeded = Boolean(tokenData.access_token);

        console.log("Xero OAuth token exchange completed.", {
            accountId: authSession.accountId,
            tokenExchangeSucceeded,
            hasAccessToken: Boolean(tokenData.access_token),
            hasRefreshToken: Boolean(tokenData.refresh_token),
            expiresIn: tokenData.expires_in || null
        });

        const connectedUserEmail = extractConnectedUserEmail(tokenData.id_token);

        const tenants = await fetchXeroTenants(tokenData.access_token);
        const selectedTenant = tenants[0] || null;
        const chartOfAccounts = selectedTenant
            ? await fetchChartOfAccounts(tokenData.access_token, selectedTenant.tenantId)
            : [];

        await upsertXeroConnection(authSession.accountId, () => ({
            accountId: authSession.accountId,
            accessToken: tokenData.access_token,
            refreshToken: tokenData.refresh_token,
            idToken: tokenData.id_token || null,
            connectedUserEmail,
            scope: tokenData.scope || xeroScopes(),
            expiresAt: new Date(Date.now() + Number(tokenData.expires_in || 1800) * 1000).toISOString(),
            connectedAt: new Date().toISOString(),
            lastRefreshedAt: new Date().toISOString(),
            tenants,
            selectedTenantId: selectedTenant?.tenantId || null,
            selectedTenantName: selectedTenant?.tenantName || null,
            chartOfAccounts,
            chartOfAccountsLastSyncedAt: selectedTenant ? new Date().toISOString() : null
        }));
        tokensStored = true;

        console.log("Xero OAuth callback completed and tokens stored.", {
            accountId: authSession.accountId,
            tokenExchangeSucceeded,
            tokensStored,
            tenantCount: tenants.length,
            selectedTenantId: selectedTenant?.tenantId || null,
            connectedUserEmail: connectedUserEmail || null
        });

        response.redirect(
            buildReturnURL(authSession.returnUri, {
                accountId: authSession.accountId,
                status: "connected",
                tenantCount: tenants.length
            })
        );
    } catch (error) {
        const message = error instanceof Error ? error.message : "Xero connection failed.";

        console.error("Xero OAuth callback failed.", {
            accountId: authSession?.accountId || null,
            state: state || null,
            hasCode: Boolean(code),
            codePreview: code ? `${code.slice(0, 6)}...` : null,
            tokenExchangeSucceeded,
            tokensStored,
            message,
            stack: error instanceof Error ? error.stack : undefined
        });

        response.redirect(
            buildReturnURL(fallbackReturnURL, {
                accountId: authSession?.accountId || "",
                status: "error",
                message
            })
        );
    }
});

app.get("/xero/status", async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.query.accountId);
        if (!accountId) {
            response.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
            return;
        }

        const connection = await ensureFreshXeroConnection(accountId);
        const missingScopes = missingXeroScopes(connection, ["accounting.attachments"]);
        const requiresReconnect = missingScopes.length > 0;
        const statusMessage = requiresReconnect
            ? `Reconnect Xero to grant the required scope: ${missingScopes.join(", ")}.`
            : null;

        response.json({
            isConnected: true,
            requiresReconnect,
            connectedUserEmail: connection.connectedUserEmail || null,
            selectedTenantId: connection.selectedTenantId || null,
            selectedTenantName: connection.selectedTenantName || null,
            tenants: connection.tenants || [],
            connectedAt: connection.connectedAt || null,
            expiresAt: connection.expiresAt || null,
            tokenValid: true,
            tenantSelected: Boolean(connection.selectedTenantId),
            accountsSynced: Array.isArray(connection.chartOfAccounts) && connection.chartOfAccounts.length > 0,
            contactsSynced: Boolean(connection.contactsLastSyncedAt),
            chartOfAccountsLastSyncedAt: connection.chartOfAccountsLastSyncedAt || null,
            contactsLastSyncedAt: connection.contactsLastSyncedAt || null,
            chartOfAccountsCount: Array.isArray(connection.chartOfAccounts) ? connection.chartOfAccounts.length : 0,
            message: statusMessage
        });
    } catch (error) {
        sendXeroError(response, error, "Unable to fetch Xero status.");
    }
});

app.get("/xero/tenants", async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.query.accountId);
        if (!accountId) {
            response.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
            return;
        }

        const connection = await ensureFreshXeroConnection(accountId);
        response.json({
            tenants: connection.tenants || [],
            selectedTenantId: connection.selectedTenantId || null,
            selectedTenantName: connection.selectedTenantName || null,
            connectedUserEmail: connection.connectedUserEmail || null
        });
    } catch (error) {
        sendXeroError(response, error);
    }
});

app.post("/xero/select-tenant", async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.body?.accountId);
        const tenantId = normalizeOptionalString(request.body?.tenantId);

        if (!accountId || !tenantId) {
            response.status(400).json({ message: "Missing accountId or tenantId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
            return;
        }

        const connection = await ensureFreshXeroConnection(accountId);
        const selectedTenant = (connection.tenants || []).find((tenant) => tenant.tenantId === tenantId);
        if (!selectedTenant) {
            response.status(404).json({ message: "That Xero organisation was not found for this connection." });
            return;
        }

        const chartOfAccounts = await fetchChartOfAccounts(connection.accessToken, selectedTenant.tenantId);
        const updated = await upsertXeroConnection(accountId, (previous) => ({
            ...previous,
            selectedTenantId: selectedTenant.tenantId,
            selectedTenantName: selectedTenant.tenantName,
            chartOfAccounts,
            chartOfAccountsLastSyncedAt: new Date().toISOString()
        }));

        response.json({
            isConnected: true,
            connectedUserEmail: updated.connectedUserEmail || null,
            selectedTenantId: updated.selectedTenantId,
            selectedTenantName: updated.selectedTenantName,
            chartOfAccountsCount: updated.chartOfAccounts.length
        });
    } catch (error) {
        sendXeroError(response, error);
    }
});

app.post("/xero/disconnect", async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.body?.accountId);
        if (!accountId) {
            response.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
            return;
        }

        await deleteXeroConnection(accountId);
        response.json({ disconnected: true, connectedUserEmail: null });
    } catch (error) {
        sendXeroError(response, error);
    }
});

app.get("/xero/accounts", async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.query.accountId);
        if (!accountId) {
            response.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
            return;
        }

        const connection = await ensureFreshXeroConnection(accountId);
        const tenantId = normalizeOptionalString(request.query.tenantId) || connection.selectedTenantId;
        if (!tenantId) {
            sendXeroError(response, new XeroAPIError({ message: "No Xero organisation has been selected for this account.", code: "XERO_TENANT_NOT_SELECTED", status: 403, requiresReconnect: false }));
            return;
        }

        const chartOfAccounts = await fetchChartOfAccounts(connection.accessToken, tenantId);
        await upsertXeroConnection(accountId, (previous) => ({
            ...previous,
            chartOfAccounts,
            chartOfAccountsLastSyncedAt: new Date().toISOString()
        }));

        response.json({
            accounts: chartOfAccounts,
            syncedAt: new Date().toISOString(),
            selectedTenantId: tenantId,
            connectedUserEmail: connection.connectedUserEmail || null
        });
    } catch (error) {
        sendXeroError(response, error);
    }
});

app.post("/xero/match-transactions", async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.body?.accountId);
        const document = request.body?.document;

        if (!accountId) {
            response.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!document || typeof document !== "object") {
            response.status(400).json({ message: "Missing document payload." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
            return;
        }

        const connection = await ensureFreshXeroConnection(accountId);
        if (!connection.selectedTenantId) {
            sendXeroError(response, new XeroAPIError({ message: "No Xero organisation has been selected for this account.", code: "XERO_TENANT_NOT_SELECTED", status: 403, requiresReconnect: false }));
            return;
        }

        const matches = await findMatchingXeroTransactions({
            accessToken: connection.accessToken,
            tenantId: connection.selectedTenantId,
            document
        });

        response.json({
            matches,
            selectedTenantId: connection.selectedTenantId,
            selectedTenantName: connection.selectedTenantName || null,
            connectedUserEmail: connection.connectedUserEmail || null
        });
    } catch (error) {
        sendXeroError(response, error);
    }
});

app.post("/xero/publish-bill", upload.any(), async (request, response) => {
    try {
        const metadataFile = Array.isArray(request.files)
            ? request.files.find((file) => file.fieldname === "metadata")
            : null;
        const metadataText = metadataFile?.buffer?.toString("utf8") || request.body?.metadata;
        const metadata = parseJSONField(metadataText, {});
        const accountId = normalizeOptionalString(
            request.body?.accountId ||
            metadata.accountId ||
            metadata.accountID
        );

        if (!accountId) {
            response.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
            return;
        }

        const documentFiles = (Array.isArray(request.files) ? request.files : []).filter(
            (file) => file.fieldname === "documents[]"
        );

        const result = await createXeroLedgerDocument(accountId, metadata, documentFiles);
        const connection = await getXeroConnection(accountId);

        response.json({
            submissionId: metadata.submissionId || result.remoteSubmissionID,
            remoteSubmissionID: result.remoteSubmissionID,
            xeroTransactionID: result.xeroTransactionID || result.invoiceId,
            xeroTransactionId: result.xeroTransactionID || result.invoiceId,
            xeroInvoiceID: result.xeroTransactionID || result.invoiceId,
            xeroInvoiceId: result.xeroTransactionID || result.invoiceId,
            invoiceID: result.xeroTransactionID || result.invoiceId,
            invoiceId: result.invoiceId,
            billID: result.xeroTransactionID || result.invoiceId,
            billId: result.xeroTransactionID || result.invoiceId,
            published: result.published,
            idempotent: result.idempotent || false,
            attachmentsUploaded: result.attachmentsUploaded,
            warning: result.warning || null,
            message: result.confirmationMessage,
            tenantName: connection?.selectedTenantName || null,
            connectedUserEmail: connection?.connectedUserEmail || null
        });
    } catch (error) {
        sendXeroError(response, error, "Unable to publish bill to Xero.");
    }
});

app.post(
    "/jentry/uploads",
    upload.fields([
        { name: "metadata", maxCount: 1 },
        { name: "documents[]", maxCount: 50 }
    ]),
    async (request, response) => {
        try {
            console.log("Upload request received.", {
                contentType: request.headers["content-type"] || "unknown"
            });

            const metadataFile = request.files?.metadata?.[0];
            const metadataText = metadataFile?.buffer.toString("utf8") || request.body?.metadata;
            if (!metadataText) {
                console.error("Upload rejected: metadata missing.");
                response.status(400).json({ message: "Missing metadata upload." });
                return;
            }

            const metadata = JSON.parse(metadataText);
            const documentFiles = request.files?.["documents[]"] ?? [];
            const routingMethod = normalizeOptionalString(metadata.routingMethod) || "xeroFilesEmail";

            console.log("Upload parsed.", {
                submissionId: metadata.submissionId || "missing",
                routingMethod,
                documentCount: documentFiles.length
            });

            if (routingMethod === "xeroIntegration") {
                const accountId = normalizeOptionalString(metadata.accountId || metadata.accountID);
                if (!accountId) {
                    throw new Error("Missing accountId for Xero-integrated submission.");
                }

                if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
                    return;
                }

                const result = await createXeroLedgerDocument(accountId, metadata, documentFiles);
                response.json({
                    submissionId: metadata.submissionId || result.remoteSubmissionID,
                    message: result.confirmationMessage,
                    remoteSubmissionID: result.remoteSubmissionID,
                    xeroTransactionID: result.xeroTransactionID || result.invoiceId,
                    xeroTransactionId: result.xeroTransactionID || result.invoiceId,
                    xeroInvoiceID: result.xeroTransactionID || result.invoiceId,
                    xeroInvoiceId: result.xeroTransactionID || result.invoiceId,
                    invoiceID: result.xeroTransactionID || result.invoiceId,
                    invoiceId: result.invoiceId,
                    billID: result.xeroTransactionID || result.invoiceId,
                    billId: result.xeroTransactionID || result.invoiceId,
                    published: result.published,
                    idempotent: result.idempotent || false,
                    attachmentsUploaded: result.attachmentsUploaded,
                    warning: result.warning || null
                });
                return;
            }

            const result = await handleEmailUpload(metadata, documentFiles);
            response.json(result);
        } catch (error) {
            const message = error instanceof Error ? error.message : "Unknown backend error.";
            console.error("Upload processing failed.", {
                message,
                stack: error instanceof Error ? error.stack : undefined
            });
            if (error instanceof XeroAPIError) {
                sendXeroError(response, error, message);
                return;
            }
            response.status(500).json({ message });
        }
    }
);

app.post(
    "/xero/attach-document",
    upload.fields([
        { name: "documents[]", maxCount: 50 },
        { name: "documents", maxCount: 50 }
    ]),
    async (request, response) => {
        try {
            const accountId = normalizeOptionalString(request.body?.accountId || request.body?.accountID);
            const xeroTransactionId = normalizeOptionalString(
                request.body?.xeroTransactionId ||
                request.body?.xeroTransactionID ||
                request.body?.xeroInvoiceId ||
                request.body?.xeroInvoiceID ||
                request.body?.invoiceId ||
                request.body?.invoiceID ||
                request.body?.billId ||
                request.body?.billID
            );
            const xeroInvoiceType = normalizeOptionalString(request.body?.xeroInvoiceType);
            const xeroTargetRecordType = normalizeOptionalString(request.body?.xeroTargetRecordType);
            const documentFiles = [
                ...(request.files?.["documents[]"] ?? []),
                ...(request.files?.documents ?? [])
            ];

            if (!accountId) {
                response.status(400).json({ message: "Missing accountId." });
                return;
            }

            if (!xeroTransactionId) {
                response.status(400).json({ message: "Missing xeroTransactionId." });
                return;
            }

            if (!await requireAuthorizedAccountAccess(request, response, accountId)) {
                return;
            }

            if (documentFiles.length === 0) {
                response.status(400).json({ message: "Missing documents[]." });
                return;
            }

            const result = await attachDocumentsToXeroTransaction({
                accountId,
                xeroTransactionId,
                xeroInvoiceType,
                xeroTargetRecordType,
                documentFiles
            });

            response.status(200).json(result);
        } catch (error) {
            sendXeroError(response, error, "Unable to attach documents to Xero transaction.");
        }
    }
);

for (const analyzePath of ["/analyze", "/jentry/analyze"]) {
    app.post(analyzePath, upload.single("document"), analyzeHandler);
}

for (const reportPath of ["/problem-report", "/jentry/problem-report"]) {
    app.post(
        reportPath,
        upload.fields([
            { name: "metadata", maxCount: 1 },
            { name: "attachments[]", maxCount: 50 }
        ]),
        problemReportHandler
    );
}

const inboxRegistrationHandler = async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.body?.accountId);
        const clientId = normalizeOptionalString(request.body?.clientId);
        const companyName = normalizeOptionalString(request.body?.companyName);
        const inboxEmail = normalizeEmail(request.body?.inboxEmail);

        if (!accountId || !inboxEmail) {
            response.status(400).json({ message: "Missing accountId or inboxEmail." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(request, response, accountId, { allowCreate: true })) {
            return;
        }

        await ensureJentryAccountsTable();
        await upsertAccountRecord({
            accountId,
            clientId: clientId || null,
            companyName: companyName || null,
            assignedUserId: request.authenticatedUser?.id || null
        });
        await upsertJentryInboxRecord({
            accountId,
            inboxEmail,
            assignedUserEmail: request.authenticatedUser?.email || null,
            assignedUserId: request.authenticatedUser?.id || null,
            updatedBy: request.authenticatedUser?.email || null,
            isActive: true
        });

        await pool.query(
            `
            INSERT INTO jentry_accounts (account_id, client_id, company_name, jentry_inbox_email)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (account_id)
            DO UPDATE SET
                client_id = EXCLUDED.client_id,
                company_name = EXCLUDED.company_name,
                jentry_inbox_email = EXCLUDED.jentry_inbox_email
            `,
            [accountId, clientId || null, companyName || null, inboxEmail]
        );

        response.json({ ok: true });
    } catch (error) {
        response.status(500).json({
            message: error instanceof Error ? error.message : "Unable to register inbox."
        });
    }
};

for (const inboxRegistrationPath of ["/jentry/inbox/register", "/inbound-email-addresses/register"]) {
    app.post(inboxRegistrationPath, inboxRegistrationHandler);
}

app.get("/inbound-email-submissions", async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.query.accountId);
        const inboxEmail = normalizeEmail(request.query.inboxEmail);
        const clientId = normalizeOptionalString(request.query.clientId);

        if (!accountId && !inboxEmail && !clientId) {
            response.status(400).json({ message: "Missing accountId, inboxEmail, or clientId." });
            return;
        }

        await ensureProcessedInboundSubmissionsTable();
        await ensureJentryAccountsTable();

        const conditions = [];
        const values = [];

        if (accountId) {
            values.push(accountId);
            conditions.push(`p.account_id = $${values.length}`);
        }

        if (inboxEmail) {
            values.push(inboxEmail);
            conditions.push(`LOWER(a.jentry_inbox_email) = LOWER($${values.length})`);
        }

        if (clientId) {
            values.push(clientId);
            conditions.push(`a.client_id = $${values.length}`);
        }

        const whereClause = conditions.length > 0
            ? `WHERE ${conditions.join(" AND ")}`
            : "";

        if (accountId && !await requireAuthorizedAccountAccess(request, response, accountId)) {
            return;
        }

        const result = await pool.query(
            `
            SELECT 
                p.id,
                p.account_id,
                p.title,
                p.summary,
                p.extracted_documents,
                p.status,
                p.created_at,
                a.client_id,
                a.company_name,
                a.jentry_inbox_email
            FROM processed_inbound_submissions p
            LEFT JOIN jentry_accounts a
                ON a.account_id = p.account_id
            ${whereClause}
            ORDER BY p.created_at DESC
            LIMIT 100
            `,
            values
        );

        response.json({
            submissions: result.rows.map((row) => ({
                id: row.id,
                accountId: row.account_id,
                clientId: row.client_id || null,
                companyName: row.company_name || null,
                inboxEmail: row.jentry_inbox_email || null,
                title: row.title,
                summary: row.summary,
                extractedDocuments: row.extracted_documents,
                status: row.status,
                createdAt: row.created_at
            }))
        });
    } catch (error) {
        response.status(500).json({
            message: error instanceof Error
                ? error.message
                : "Unable to fetch inbound email submissions."
        });
    }
});

app.post("/inbound/postmark", express.json({ limit: "25mb" }), async (request, response) => {
    try {
        const payload = request.body || {};

        const recipient = extractPrimaryRecipient(payload);
        const inboxEmail = normalizeEmail(recipient);
        if (!inboxEmail) {
            response.status(400).json({ message: "Missing inbound recipient." });
            return;
        }

        const accountId = await resolveAccountIdFromInboundAddress(inboxEmail);
        if (!accountId) {
            response.status(404).json({ message: "No Jentry account matched this inbox address." });
            return;
        }

        const attachments = normalizePostmarkAttachments(payload.Attachments);
        if (attachments.length === 0) {
            response.status(200).json({ ok: true, ignored: true, reason: "No supported attachments." });
            return;
        }

        const submission = await createInboundEmailSubmission({
            accountId,
            inboxEmail,
            payload
        });

        await saveInboundEmailAttachments(submission.id, attachments);

        response.status(200).json({
            ok: true,
            submissionId: submission.id,
            accountId
        });

        queueInboundEmailProcessing(submission, attachments).catch((error) => {
            console.error("Inbound email processing failed.", {
                submissionId: submission.id,
                message: error instanceof Error ? error.message : String(error)
            });
        });
    } catch (error) {
        console.error("Postmark inbound webhook failed.", {
            message: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined
        });
        response.status(500).json({ message: "Inbound processing failed." });
    }
});

app.get("/xero/contacts", async (req, res) => {
    try {
        const accountId = normalizeOptionalString(req.query.accountId);
        if (!accountId) {
            res.status(400).json({ message: "Missing accountId." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(req, res, accountId)) {
            return;
        }

        const connection = await ensureFreshXeroConnection(accountId);
        if (!connection.selectedTenantId) {
            sendXeroError(res, new XeroAPIError({ message: "No Xero tenant selected.", code: "XERO_TENANT_NOT_SELECTED", status: 403, requiresReconnect: false }));
            return;
        }

        const data = await xeroGetJSON(XERO_CONTACTS_URL, {
            accessToken: connection.accessToken,
            tenantId: connection.selectedTenantId
        });

        await upsertXeroConnection(accountId, (previous) => ({
            ...previous,
            contactsLastSyncedAt: new Date().toISOString()
        }));

        res.json({
            contacts: Array.isArray(data?.Contacts) ? data.Contacts : [],
            contactsSynced: true,
            contactsLastSyncedAt: new Date().toISOString()
        });
    } catch (error) {
        sendXeroError(res, error, "Unable to load Xero contacts.");
    }
});

app.post("/xero/ensure-contact", async (req, res) => {
    try {
        const accountId = normalizeOptionalString(req.body?.accountId);
        const contactName = normalizeOptionalString(req.body?.contactName || req.body?.supplierName);

        if (!accountId || !contactName) {
            res.status(400).json({ message: "Missing accountId and contactName or supplierName." });
            return;
        }

        if (!await requireAuthorizedAccountAccess(req, res, accountId)) {
            return;
        }

        const connection = await ensureFreshXeroConnection(accountId);
        if (!connection.selectedTenantId) {
            sendXeroError(res, new XeroAPIError({ message: "No Xero tenant selected.", code: "XERO_TENANT_NOT_SELECTED", status: 403, requiresReconnect: false }));
            return;
        }

        const contactsResult = await xeroGetJSON(
            `${XERO_CONTACTS_URL}?where=Name%20%3D%20%22${encodeURIComponent(contactName)}%22`,
            {
                accessToken: connection.accessToken,
                tenantId: connection.selectedTenantId
            }
        );

        const existing = Array.isArray(contactsResult?.Contacts) ? contactsResult.Contacts[0] : null;
        if (existing) {
            res.json({ contact: existing, created: false });
            return;
        }

        const createResponse = await fetch(XERO_CONTACTS_URL, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${connection.accessToken}`,
                "xero-tenant-id": connection.selectedTenantId,
                Accept: "application/json",
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ Contacts: [{ Name: contactName }] })
        });

        const created = await createResponse.json().catch(() => ({}));

        if (!createResponse.ok) {
            const errMsg = created?.Elements?.[0]?.ValidationErrors?.map((entry) => entry.Message).join("; ")
                || created?.Message
                || "Xero contact creation failed.";
            throw new Error(errMsg);
        }

        res.json({
            contact: Array.isArray(created?.Contacts) ? created.Contacts[0] : created,
            created: true
        });
    } catch (error) {
        sendXeroError(res, error, "Unable to ensure contact in Xero.");
    }
});



app.use("/admin", requireAuthenticatedUser, requireSuperAdmin);

app.get("/admin/overview", async (_request, response) => {
    try {
        response.json(await buildAdminOverview());
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load admin overview." });
    }
});

app.get("/admin/workspaces", async (_request, response) => {
    try {
        response.json(await listAdminWorkspaces());
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load admin workspaces." });
    }
});

app.get("/admin/audit-trail", async (_request, response) => {
    try {
        response.json(await listAdminAuditTrail());
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load admin audit trail." });
    }
});

app.get("/admin/subscriptions", async (_request, response) => {
    try {
        response.json({ subscriptions: await listAdminSubscriptions() });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load paid subscriptions." });
    }
});

app.patch("/admin/workspaces/:accountId/suspension", async (request, response) => {
    try {
        const { accountId } = request.params;
        const { isSuspended, reason, actorEmail } = request.body;
        await setWorkspaceSuspension({
            accountId,
            isSuspended,
            reason,
            actorEmail,
            request
        });
        response.json({ success: true });
    } catch (error) {
        const status = error?.statusCode || 500;
        response.status(status).json({ message: error instanceof Error ? error.message : "Unable to update workspace suspension." });
    }
});

app.patch("/admin/workspaces/:accountId/registry", async (request, response) => {
    try {
        const { accountId } = request.params;
        const { displayName, assignedUserEmail, inboxEmail, actorEmail } = request.body;
        await updateWorkspaceRegistry({
            accountId,
            displayName,
            assignedUserEmail,
            inboxEmail,
            actorEmail,
            request
        });
        response.json({ success: true });
    } catch (error) {
        const status = error?.statusCode || 500;
        response.status(status).json({ message: error instanceof Error ? error.message : "Unable to update workspace registry." });
    }
});

app.patch("/admin/workspaces/:accountId/billing", async (request, response) => {
    try {
        const { accountId } = request.params;
        const { billingMode, actorEmail } = request.body;
        await updateWorkspaceBilling({
            accountId,
            billingMode,
            actorEmail,
            request
        });
        response.json({ success: true });
    } catch (error) {
        const status = error?.statusCode || 500;
        response.status(status).json({ message: error instanceof Error ? error.message : "Unable to update workspace billing." });
    }
});

app.get("/admin/users", async (_request, response) => {
    try {
        await ensureCoreModelTables();
        const result = await pool.query(`
            SELECT
                u.id,
                u.email,
                u.display_name,
                u.role,
                u.is_super_admin,
                u.is_suspended,
                u.suspension_reason,
                u.last_seen_at,
                u.last_login_at,
                COALESCE(jsonb_agg(DISTINCT jsonb_build_object('accountId', a.id, 'companyName', a.company_name, 'clientId', a.client_id)) FILTER (WHERE a.id IS NOT NULL), '[]'::jsonb) AS assigned_accounts,
                COALESCE(jsonb_agg(DISTINCT jsonb_build_object('inboxId', ji.id, 'inboxEmail', ji.inbox_email, 'isActive', ji.is_active)) FILTER (WHERE ji.id IS NOT NULL), '[]'::jsonb) AS jentry_inboxes,
                MAX(xc.connected_user_email) AS connected_xero_email,
                MAX(a.last_submission_at) AS last_submission_at
            FROM users u
            LEFT JOIN memberships m ON m.user_id = u.id
            LEFT JOIN accounts a ON a.id = m.account_id OR a.assigned_user_id = u.id
            LEFT JOIN jentry_inboxes ji ON ji.assigned_user_id = u.id OR LOWER(ji.assigned_user_email) = LOWER(u.email)
            LEFT JOIN xero_connections xc ON xc.account_id = a.id
            GROUP BY u.id
            ORDER BY u.created_at DESC
        `);
        response.json({ users: result.rows.map(mapAdminUserRow) });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load admin users." });
    }
});

app.patch("/admin/users/:userId/suspension", async (request, response) => {
    try {
        await ensureCoreModelTables();
        const userId = normalizeOptionalString(request.params.userId);
        const isSuspended = Boolean(request.body?.isSuspended);
        const reason = normalizeOptionalString(request.body?.reason) || (isSuspended ? "Manual suspension by Jaccountancy" : null);
        const result = await pool.query(
            `UPDATE users SET is_suspended = $2, suspension_reason = $3, updated_at = now() WHERE id = $1 RETURNING *`,
            [userId, isSuspended, reason]
        );
        if (!result.rows[0]) {
            response.status(404).json({ message: "User not found." });
            return;
        }
        await writeAdminAuditLog(request, {
            action: isSuspended ? "user.suspended" : "user.reenabled",
            targetType: "user",
            targetId: userId,
            details: { reason }
        });
        response.json({ user: mapUserRow(result.rows[0]) });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to update suspension." });
    }
});

app.get("/admin/accounts", async (_request, response) => {
    try {
        await ensureCoreModelTables();
        const result = await pool.query(`
            SELECT
                a.id AS account_id,
                a.company_name,
                a.client_id,
                a.status,
                a.last_submission_at,
                u.email AS assigned_user_email,
                u.display_name AS assigned_user_display_name,
                u.last_seen_at,
                u.is_suspended,
                ji.inbox_email AS jentry_inbox_email,
                xc.connected_user_email AS connected_xero_email,
                xc.is_connected,
                xc.requires_reconnect,
                xc.tenant_id,
                xc.tenant_name,
                COUNT(p.id)::int AS submission_count
            FROM accounts a
            LEFT JOIN users u ON u.id = a.assigned_user_id
            LEFT JOIN jentry_inboxes ji ON ji.account_id = a.id AND ji.is_active = true
            LEFT JOIN xero_connections xc ON xc.account_id = a.id
            LEFT JOIN processed_inbound_submissions p ON p.account_id = a.id
            GROUP BY a.id, u.id, ji.id, xc.id
            ORDER BY a.created_at DESC
        `);
        response.json({ accounts: result.rows.map(mapAdminAccountRow) });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load admin accounts." });
    }
});

app.patch("/admin/accounts/:accountId", async (request, response) => {
    try {
        await ensureCoreModelTables();
        const accountId = normalizeOptionalString(request.params.accountId);
        const assignedUserId = normalizeOptionalString(request.body?.assignedUserId || request.body?.assigned_user_id) || null;
        const companyName = normalizeOptionalString(request.body?.companyName || request.body?.displayName || request.body?.company_name) || null;
        const clientEmail = normalizeEmail(request.body?.clientEmail || request.body?.client_email) || null;
        const status = normalizeOptionalString(request.body?.status) || null;
        const result = await pool.query(
            `
            UPDATE accounts
            SET assigned_user_id = COALESCE($2, assigned_user_id),
                company_name = COALESCE($3, company_name),
                client_email = COALESCE($4, client_email),
                status = COALESCE($5, status),
                updated_at = now()
            WHERE id = $1
            RETURNING *
            `,
            [accountId, assignedUserId, companyName, clientEmail, status]
        );
        if (!result.rows[0]) {
            response.status(404).json({ message: "Account not found." });
            return;
        }
        await writeAdminAuditLog(request, {
            action: "account.updated",
            targetType: "account",
            targetId: accountId,
            details: { assignedUserId, companyName, clientEmail, status }
        });
        response.json({ account: mapAccountRow(result.rows[0]) });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to update account." });
    }
});

app.get("/admin/inboxes", async (_request, response) => {
    try {
        await ensureCoreModelTables();
        const result = await pool.query(`
            SELECT ji.*, a.company_name, u.email AS user_email
            FROM jentry_inboxes ji
            LEFT JOIN accounts a ON a.id = ji.account_id
            LEFT JOIN users u ON u.id = ji.assigned_user_id
            ORDER BY ji.updated_at DESC
        `);
        response.json({ inboxes: result.rows.map(mapInboxRow) });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load inboxes." });
    }
});

app.patch("/admin/inboxes/:inboxId", async (request, response) => {
    try {
        await ensureCoreModelTables();
        const inboxId = normalizeOptionalString(request.params.inboxId);
        const inboxEmail = normalizeEmail(request.body?.inboxEmail || request.body?.inbox_email) || null;
        const assignedUserId = normalizeOptionalString(request.body?.assignedUserId || request.body?.assigned_user_id) || null;
        const assignedUserEmail = normalizeEmail(request.body?.assignedUserEmail || request.body?.assigned_user_email) || null;
        const isActive = typeof request.body?.isActive === "boolean" ? request.body.isActive : null;
        const result = await pool.query(
            `
            UPDATE jentry_inboxes
            SET inbox_email = COALESCE($2, inbox_email),
                assigned_user_id = COALESCE($3, assigned_user_id),
                assigned_user_email = COALESCE($4, assigned_user_email),
                is_active = COALESCE($5, is_active),
                updated_by = $6,
                updated_at = now()
            WHERE id = $1
            RETURNING *
            `,
            [inboxId, inboxEmail, assignedUserId, assignedUserEmail, isActive, request.superAdminEmail || request.authenticatedUser?.email || null]
        );
        if (!result.rows[0]) {
            response.status(404).json({ message: "Inbox not found." });
            return;
        }
        await writeAdminAuditLog(request, {
            action: "inbox.updated",
            targetType: "jentry_inbox",
            targetId: inboxId,
            details: { inboxEmail, assignedUserId, assignedUserEmail, isActive }
        });
        response.json({ inbox: mapInboxRow(result.rows[0]) });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to update inbox." });
    }
});

app.get("/admin/xero-connections", async (_request, response) => {
    try {
        await ensureCoreModelTables();
        const result = await pool.query(`
            SELECT xc.*, a.company_name, a.client_id
            FROM xero_connections xc
            LEFT JOIN accounts a ON a.id = xc.account_id
            ORDER BY xc.updated_at DESC
        `);
        response.json({ connections: result.rows.map(mapXeroConnectionRow) });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load Xero connections." });
    }
});

app.get("/admin/activity", async (_request, response) => {
    try {
        await ensureCoreModelTables();
        const result = await pool.query(`
            SELECT l.*, u.email AS actor_user_email, a.company_name
            FROM admin_audit_log l
            LEFT JOIN users u ON u.id = l.actor_user_id
            LEFT JOIN accounts a ON a.id = l.target_id AND l.target_type = 'account'
            ORDER BY l.created_at DESC
            LIMIT 250
        `);
        response.json({ activity: result.rows.map(mapAuditRow) });
    } catch (error) {
        response.status(500).json({ message: error instanceof Error ? error.message : "Unable to load activity." });
    }
});

app.use((request, response) => {
    response.status(404).send(`Cannot ${request.method} ${request.path}`);
});

app.listen(port, () => {
    console.log(`Jentry relay listening on port ${port}`);
    console.log(JSON.stringify({
        routes: [
            "GET /health",
            "POST /oauth/xero/start-token",
            "GET /oauth/xero/start",
            "GET /oauth/xero/callback",
            "GET /xero/status",
            "GET /xero/tenants",
            "POST /xero/select-tenant",
            "POST /xero/disconnect",
            "GET /xero/accounts",
            "POST /xero/match-transactions",
            "POST /xero/publish-bill",
            "POST /xero/attach-document",
            "POST /jentry/uploads",
            "POST /jentry/inbox/register",
            "GET /inbound-email-submissions",
            "POST /analyze",
            "POST /jentry/analyze",
            "POST /problem-report",
            "POST /jentry/problem-report",
            "POST /inbound/postmark",
            "GET /xero/contacts",
            "POST /xero/ensure-contact",
            "POST /session/heartbeat",
            "GET /admin/overview",
            "GET /admin/workspaces",
            "GET /admin/audit-trail",
            "PATCH /admin/workspaces/:accountId/suspension",
            "PATCH /admin/workspaces/:accountId/registry",
            "GET /admin/users",
            "PATCH /admin/users/:userId/suspension",
            "GET /admin/accounts",
            "PATCH /admin/accounts/:accountId",
            "GET /admin/inboxes",
            "PATCH /admin/inboxes/:inboxId",
            "GET /admin/xero-connections",
            "GET /admin/activity"
        ]
    }));
});

async function analyzeHandler(request, response) {
    try {
        if (!optionalEnvironmentVariables.openAIAPIKey) {
            response.status(503).json({ message: "OpenAI receipt extraction is not configured." });
            return;
        }

        const documentFile = request.file;
        if (!documentFile?.buffer?.length) {
            response.status(400).json({ message: "Missing document upload." });
            return;
        }

        const capturedAt = normalizeOptionalString(request.body?.capturedAt);
        const analysisContext = parseJSONField(request.body?.analysisContext, null);
        console.log("Receipt analysis request received.", {
            contentType: request.headers["content-type"] || "unknown",
            mimeType: documentFile.mimetype || "unknown",
            size: documentFile.size || documentFile.buffer.length,
            hasAnalysisContext: Boolean(analysisContext)
        });

        const extraction = await analyzeReceiptWithOpenAI({
            buffer: documentFile.buffer,
            mimeType: documentFile.mimetype || "image/jpeg",
            capturedAt,
            analysisContext
        });

        const enhancedExtraction = applyAccountingIntelligence(extraction, analysisContext);

        console.log("Receipt analysis completed.", {
            merchant: enhancedExtraction.merchant || "unknown",
            totalText: enhancedExtraction.totalText || "unknown",
            needsReview: enhancedExtraction.needsReview,
            codingConfidence: enhancedExtraction.codingConfidence ?? enhancedExtraction.extractionConfidence ?? null
        });

        response.json(enhancedExtraction);
    } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown analysis error.";
        console.error("Receipt analysis failed.", {
            message,
            stack: error instanceof Error ? error.stack : undefined
        });
        response.status(500).json({ message });
    }
}

async function problemReportHandler(request, response) {
    try {
        const metadataFile = request.files?.metadata?.[0];
        const metadataText = metadataFile?.buffer.toString("utf8") || request.body?.metadata;

        if (!metadataText) {
            response.status(400).json({ message: "Missing problem report metadata." });
            return;
        }

        const metadata = JSON.parse(metadataText);

        const to = normalizeEmail(metadata.to) || "jay@jaccountancy.co.uk";
        const from = normalizeEmail(process.env.GMAIL_SENDER);
        const subject = normalizeOptionalString(metadata.subject) || "Jentry problem report";
        const body = normalizeOptionalString(metadata.body) || "A problem report was sent from Jentry.";

        const attachments = request.files?.["attachments[]"] ?? [];

        const rawMessage = await buildRawMessage({
            from,
            to,
            subject,
            body,
            attachments: attachments.map((file) => ({
                filename: file.originalname,
                content: file.buffer,
                contentType: file.mimetype || "application/octet-stream"
            }))
        });

        const sendResult = await gmail.users.messages.send({
            userId: "me",
            requestBody: { raw: rawMessage }
        });

        response.json({
            reportID: sendResult.data.id,
            message: `Problem report emailed to ${to}.`
        });
    } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown problem report error.";
        response.status(500).json({ message });
    }
}


async function ensureCoreModelTables() {
    await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto`).catch(() => undefined);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
            email text UNIQUE NOT NULL,
            display_name text,
            role text NOT NULL DEFAULT 'user',
            is_super_admin boolean NOT NULL DEFAULT false,
            is_suspended boolean NOT NULL DEFAULT false,
            suspension_reason text,
            last_seen_at timestamptz,
            last_login_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    `);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS accounts (
            id text PRIMARY KEY,
            company_name text,
            client_id text,
            client_email text,
            nature_of_business text,
            is_vat_registered boolean NOT NULL DEFAULT false,
            status text NOT NULL DEFAULT 'active',
            assigned_user_id text REFERENCES users(id) ON DELETE SET NULL,
            last_submission_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    `);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS memberships (
            id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
            user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            role text NOT NULL DEFAULT 'member',
            created_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE(user_id, account_id)
        )
    `);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS xero_connections (
            account_id text PRIMARY KEY,
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    `);

    await pool.query(`ALTER TABLE xero_connections ADD COLUMN IF NOT EXISTS id text DEFAULT gen_random_uuid()::text`);
    await pool.query(`ALTER TABLE xero_connections ADD COLUMN IF NOT EXISTS connected_user_email text`);
    await pool.query(`ALTER TABLE xero_connections ADD COLUMN IF NOT EXISTS tenant_id text`);
    await pool.query(`ALTER TABLE xero_connections ADD COLUMN IF NOT EXISTS tenant_name text`);
    await pool.query(`ALTER TABLE xero_connections ADD COLUMN IF NOT EXISTS is_connected boolean NOT NULL DEFAULT false`);
    await pool.query(`ALTER TABLE xero_connections ADD COLUMN IF NOT EXISTS requires_reconnect boolean NOT NULL DEFAULT false`);
    await pool.query(`ALTER TABLE xero_connections ADD COLUMN IF NOT EXISTS last_connected_at timestamptz`);
    await pool.query(`ALTER TABLE xero_connections ADD COLUMN IF NOT EXISTS last_synced_at timestamptz`);
    await pool.query(`ALTER TABLE accounts ADD COLUMN IF NOT EXISTS billing_mode text NOT NULL DEFAULT 'free'`);
    await pool.query(`ALTER TABLE accounts ADD COLUMN IF NOT EXISTS subscription_status text NOT NULL DEFAULT 'free'`);
    await pool.query(`ALTER TABLE accounts ADD COLUMN IF NOT EXISTS stripe_customer_id text`);
    await pool.query(`ALTER TABLE accounts ADD COLUMN IF NOT EXISTS stripe_subscription_id text`);
    await pool.query(`ALTER TABLE accounts ADD COLUMN IF NOT EXISTS subscription_amount_pence integer NOT NULL DEFAULT ${JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE}`);
    await pool.query(`ALTER TABLE accounts ADD COLUMN IF NOT EXISTS subscription_started_at timestamptz`);
    await pool.query(`ALTER TABLE accounts ADD COLUMN IF NOT EXISTS subscription_current_period_end timestamptz`);
    await pool.query(`ALTER TABLE accounts ADD COLUMN IF NOT EXISTS subscription_updated_at timestamptz`);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS jentry_inboxes (
            id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
            account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            inbox_email text UNIQUE NOT NULL,
            assigned_user_email text,
            assigned_user_id text REFERENCES users(id) ON DELETE SET NULL,
            is_active boolean NOT NULL DEFAULT true,
            updated_by text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    `);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS admin_audit_log (
            id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
            actor_user_id text REFERENCES users(id) ON DELETE SET NULL,
            actor_email text,
            action text NOT NULL,
            target_type text,
            target_id text,
            details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        )
    `);
}

async function upsertAccountRecord({
    accountId,
    clientId = null,
    companyName = null,
    natureOfBusiness = null,
    isVatRegistered = null,
    status = null,
    assignedUserId = null,
    clientEmail = null,
    billingMode = null,
    subscriptionStatus = null,
    stripeCustomerId = null,
    stripeSubscriptionId = null,
    subscriptionAmountPence = null,
    subscriptionStartedAt = null,
    subscriptionCurrentPeriodEnd = null
} = {}) {
    const id = normalizeOptionalString(accountId);
    if (!id) return null;
    await ensureCoreModelTables();
    const result = await pool.query(
        `
        INSERT INTO accounts (
            id, client_id, company_name, nature_of_business, is_vat_registered, status, assigned_user_id, client_email,
            billing_mode, subscription_status, stripe_customer_id, stripe_subscription_id, subscription_amount_pence,
            subscription_started_at, subscription_current_period_end, subscription_updated_at, updated_at
        )
        VALUES (
            $1, $2, $3, $4, COALESCE($5, false), COALESCE($6, 'active'), $7, $8,
            COALESCE($9, 'free'), COALESCE($10, 'free'), $11, $12, COALESCE($13, ${JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE}),
            $14, $15, now(), now()
        )
        ON CONFLICT (id)
        DO UPDATE SET
            client_id = COALESCE(EXCLUDED.client_id, accounts.client_id),
            company_name = COALESCE(EXCLUDED.company_name, accounts.company_name),
            nature_of_business = COALESCE(EXCLUDED.nature_of_business, accounts.nature_of_business),
            is_vat_registered = COALESCE(EXCLUDED.is_vat_registered, accounts.is_vat_registered),
            status = COALESCE(EXCLUDED.status, accounts.status),
            assigned_user_id = COALESCE(EXCLUDED.assigned_user_id, accounts.assigned_user_id),
            client_email = COALESCE(EXCLUDED.client_email, accounts.client_email),
            billing_mode = COALESCE(EXCLUDED.billing_mode, accounts.billing_mode),
            subscription_status = COALESCE(EXCLUDED.subscription_status, accounts.subscription_status),
            stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, accounts.stripe_customer_id),
            stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, accounts.stripe_subscription_id),
            subscription_amount_pence = COALESCE(EXCLUDED.subscription_amount_pence, accounts.subscription_amount_pence),
            subscription_started_at = COALESCE(EXCLUDED.subscription_started_at, accounts.subscription_started_at),
            subscription_current_period_end = COALESCE(EXCLUDED.subscription_current_period_end, accounts.subscription_current_period_end),
            subscription_updated_at = now(),
            updated_at = now()
        RETURNING *
        `,
        [
            id,
            clientId,
            companyName,
            natureOfBusiness,
            isVatRegistered,
            status,
            assignedUserId,
            clientEmail,
            billingMode,
            subscriptionStatus,
            stripeCustomerId,
            stripeSubscriptionId,
            subscriptionAmountPence,
            subscriptionStartedAt,
            subscriptionCurrentPeriodEnd
        ]
    );
    return mapAccountRow(result.rows[0]);
}

async function upsertJentryInboxRecord({ accountId, inboxEmail, assignedUserEmail = null, assignedUserId = null, updatedBy = null, isActive = true } = {}) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    const normalizedInboxEmail = normalizeEmail(inboxEmail);
    if (!normalizedAccountId || !normalizedInboxEmail) return null;
    await upsertAccountRecord({ accountId: normalizedAccountId });
    const result = await pool.query(
        `
        INSERT INTO jentry_inboxes (account_id, inbox_email, assigned_user_email, assigned_user_id, is_active, updated_by, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, now())
        ON CONFLICT (inbox_email)
        DO UPDATE SET
            account_id = EXCLUDED.account_id,
            assigned_user_email = COALESCE(EXCLUDED.assigned_user_email, jentry_inboxes.assigned_user_email),
            assigned_user_id = COALESCE(EXCLUDED.assigned_user_id, jentry_inboxes.assigned_user_id),
            is_active = EXCLUDED.is_active,
            updated_by = EXCLUDED.updated_by,
            updated_at = now()
        RETURNING *
        `,
        [normalizedAccountId, normalizedInboxEmail, assignedUserEmail, assignedUserId, Boolean(isActive), updatedBy]
    );
    return mapInboxRow(result.rows[0]);
}

async function updatePresence({ userId, accountId = null, connectedUserEmail = null } = {}) {
    await ensureCoreModelTables();
    if (userId) {
        await pool.query(`UPDATE users SET last_seen_at = now(), updated_at = now() WHERE id = $1`, [userId]);
    }
    if (accountId) {
        await upsertAccountRecord({ accountId });
        if (userId) {
            await pool.query(
                `INSERT INTO memberships (user_id, account_id, role) VALUES ($1, $2, 'member') ON CONFLICT (user_id, account_id) DO NOTHING`,
                [userId, accountId]
            );
        }
        if (connectedUserEmail) {
            await pool.query(`UPDATE xero_connections SET connected_user_email = COALESCE($2, connected_user_email), last_synced_at = now() WHERE account_id = $1`, [accountId, connectedUserEmail]);
        }
    }
}

async function markSubmissionAccepted({ accountId, userId = null } = {}) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) return;
    await upsertAccountRecord({ accountId: normalizedAccountId });
    await pool.query(`UPDATE accounts SET last_submission_at = now(), updated_at = now() WHERE id = $1`, [normalizedAccountId]);
    if (userId) {
        await pool.query(`UPDATE users SET last_seen_at = now(), updated_at = now() WHERE id = $1`, [userId]);
    }
}


function ensureAllowedActor(actorEmail, request) {
    const normalizedActorEmail = normalizeEmail(actorEmail || request?.superAdminEmail);
    if (normalizedActorEmail !== request?.superAdminEmail) {
        const error = new Error("actorEmail must match the validated X-Jentry-Super-Admin-Email header.");
        error.statusCode = 403;
        throw error;
    }
    return normalizedActorEmail;
}

function toISODateTime(value) {
    if (!value) return null;
    const date = value instanceof Date ? value : new Date(value);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

async function buildAdminOverview() {
    await ensureCoreModelTables();
    await ensureProcessedInboundSubmissionsTable().catch(() => undefined);

    const [accounts, xero, activeToday, submissionsToday] = await Promise.all([
        pool.query(`
            SELECT
                COUNT(*)::int AS total,
                COUNT(*) FILTER (
                    WHERE a.status = 'suspended' OR COALESCE(u.is_suspended, false) = true
                )::int AS suspended,
                COUNT(*) FILTER (WHERE COALESCE(a.billing_mode, 'free') = 'free')::int AS free_accounts,
                COUNT(*) FILTER (WHERE COALESCE(a.billing_mode, 'free') <> 'free')::int AS paid_accounts,
                COUNT(*) FILTER (
                    WHERE COALESCE(a.billing_mode, 'free') <> 'free'
                      AND COALESCE(a.subscription_status, 'free') IN ('active', 'trialing', 'paid')
                )::int AS active_paid_accounts,
                COALESCE(SUM(
                    CASE
                        WHEN COALESCE(a.billing_mode, 'free') <> 'free'
                         AND COALESCE(a.subscription_status, 'free') IN ('active', 'trialing', 'paid')
                        THEN COALESCE(a.subscription_amount_pence, ${JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE})
                        ELSE 0
                    END
                ), 0)::int AS monthly_recurring_revenue_pence
            FROM accounts a
            LEFT JOIN users u ON u.id = a.assigned_user_id
        `),
        pool.query(`
            SELECT
                COUNT(*) FILTER (WHERE is_connected = true)::int AS connected,
                COUNT(*) FILTER (WHERE requires_reconnect = true OR is_connected = false)::int AS reconnect_needed
            FROM xero_connections
        `),
        pool.query(`
            SELECT COUNT(DISTINCT account_id)::int AS active_today
            FROM (
                SELECT a.id AS account_id
                FROM accounts a
                LEFT JOIN users u ON u.id = a.assigned_user_id
                WHERE u.last_seen_at >= date_trunc('day', now())
                   OR a.last_submission_at >= date_trunc('day', now())
                UNION
                SELECT account_id
                FROM xero_connections
                WHERE last_synced_at >= date_trunc('day', now())
            ) active_workspaces
        `),
        pool.query(`
            SELECT COUNT(*)::int AS submissions_today
            FROM processed_inbound_submissions
            WHERE created_at >= date_trunc('day', now())
        `).catch(() => ({ rows: [{ submissions_today: 0 }] }))
    ]);

    return {
        totalClientsCount: Number(accounts.rows[0]?.total || 0),
        connectedCount: Number(xero.rows[0]?.connected || 0),
        suspendedCount: Number(accounts.rows[0]?.suspended || 0),
        reconnectNeededCount: Number(xero.rows[0]?.reconnect_needed || 0),
        activeTodayCount: Number(activeToday.rows[0]?.active_today || 0),
        submissionsTodayCount: Number(submissionsToday.rows[0]?.submissions_today || 0),
        freeAccountsCount: Number(accounts.rows[0]?.free_accounts || 0),
        paidAccountsCount: Number(accounts.rows[0]?.paid_accounts || 0),
        activePaidAccountsCount: Number(accounts.rows[0]?.active_paid_accounts || 0),
        monthlyRecurringRevenuePence: Number(accounts.rows[0]?.monthly_recurring_revenue_pence || 0)
    };
}

async function listAdminWorkspaces() {
    await ensureCoreModelTables();
    await ensureProcessedInboundSubmissionsTable().catch(() => undefined);

    const result = await pool.query(`
        SELECT
            a.id AS account_id,
            a.company_name,
            a.client_id,
            a.client_email,
            a.nature_of_business,
            a.is_vat_registered,
            a.status,
            a.billing_mode,
            a.subscription_status,
            a.subscription_amount_pence,
            a.subscription_started_at,
            a.subscription_current_period_end,
            a.stripe_customer_id,
            a.stripe_subscription_id,
            a.last_submission_at,
            u.email AS assigned_user_email,
            u.display_name AS assigned_user_display_name,
            u.last_seen_at,
            u.is_suspended AS assigned_user_is_suspended,
            u.suspension_reason AS assigned_user_suspension_reason,
            ji.inbox_email AS jentry_inbox_email,
            xc.connected_user_email AS xero_connected_user_email,
            xc.tenant_id AS xero_organisation_id,
            xc.tenant_name AS xero_organisation_name,
            xc.is_connected,
            xc.requires_reconnect,
            xc.last_connected_at,
            xc.last_synced_at,
            xc.payload,
            COUNT(p.id)::int AS submission_count,
            COUNT(p.id) FILTER (WHERE p.status = 'failed')::int AS failed_submission_count,
            COUNT(p.id) FILTER (WHERE p.status IN ('published', 'ready', 'processed'))::int AS in_xero_count,
            MAX(p.created_at) AS latest_submission_at
        FROM accounts a
        LEFT JOIN users u ON u.id = a.assigned_user_id
        LEFT JOIN jentry_inboxes ji ON ji.account_id = a.id AND ji.is_active = true
        LEFT JOIN xero_connections xc ON xc.account_id = a.id
        LEFT JOIN processed_inbound_submissions p ON p.account_id = a.id
        GROUP BY a.id, u.id, ji.id, xc.account_id, xc.connected_user_email, xc.tenant_id, xc.tenant_name, xc.is_connected, xc.requires_reconnect, xc.last_connected_at, xc.last_synced_at, xc.payload
        ORDER BY a.company_name NULLS LAST, a.created_at DESC
    `);

    return result.rows.map(mapAdminWorkspaceRow);
}

async function listAdminSubscriptions() {
    await ensureCoreModelTables();
    const result = await pool.query(`
        SELECT
            a.id AS account_id,
            a.company_name,
            a.client_email,
            a.billing_mode,
            a.subscription_status,
            a.subscription_amount_pence,
            a.subscription_started_at,
            a.subscription_current_period_end,
            a.stripe_customer_id,
            a.stripe_subscription_id,
            xc.tenant_id,
            xc.tenant_name
        FROM accounts a
        LEFT JOIN xero_connections xc ON xc.account_id = a.id
        WHERE COALESCE(a.billing_mode, 'free') <> 'free'
           OR COALESCE(a.subscription_status, 'free') <> 'free'
        ORDER BY a.company_name NULLS LAST, a.created_at DESC
    `);
    return result.rows.map(mapAdminSubscriptionRow);
}

async function listAdminAuditTrail() {
    await ensureCoreModelTables();
    const result = await pool.query(`
        SELECT l.*, a.company_name
        FROM admin_audit_log l
        LEFT JOIN accounts a ON a.id = l.target_id AND l.target_type IN ('account', 'workspace')
        ORDER BY l.created_at DESC
        LIMIT 250
    `);
    return result.rows.map(mapAdminAuditTrailRow);
}

async function setWorkspaceSuspension({ accountId, isSuspended, reason = null, actorEmail, request } = {}) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) {
        const error = new Error("Missing accountId.");
        error.statusCode = 400;
        throw error;
    }

    const normalizedActorEmail = ensureAllowedActor(actorEmail, request);
    const normalizedReason = normalizeOptionalString(reason) || (isSuspended ? "Manual suspension by Jaccountancy" : null);
    const nextSuspended = Boolean(isSuspended);

    await ensureCoreModelTables();

    const before = await pool.query(
        `SELECT a.*, u.email AS assigned_user_email, u.is_suspended AS user_is_suspended FROM accounts a LEFT JOIN users u ON u.id = a.assigned_user_id WHERE a.id = $1 LIMIT 1`,
        [normalizedAccountId]
    );

    if (!before.rows[0]) {
        const error = new Error("Workspace not found.");
        error.statusCode = 404;
        throw error;
    }

    const beforeSummary = before.rows[0].status === "suspended" || before.rows[0].user_is_suspended ? "Suspended" : "Active";
    const nextStatus = nextSuspended ? "suspended" : "active";

    const updated = await pool.query(
        `UPDATE accounts SET status = $2, updated_at = now() WHERE id = $1 RETURNING *`,
        [normalizedAccountId, nextStatus]
    );

    if (before.rows[0].assigned_user_id) {
        await pool.query(
            `UPDATE users SET is_suspended = $2, suspension_reason = $3, updated_at = now() WHERE id = $1`,
            [before.rows[0].assigned_user_id, nextSuspended, nextSuspended ? normalizedReason : null]
        );
    }

    const companyName = updated.rows[0]?.company_name || before.rows[0].company_name || normalizedAccountId;
    await writeAdminAuditLog(request, {
        action: nextSuspended ? "workspace.suspended" : "workspace.reenabled",
        targetType: "workspace",
        targetId: normalizedAccountId,
        actorEmail: normalizedActorEmail,
        details: {
            category: "accessControl",
            actionTitle: nextSuspended ? "Suspended user" : "Re-enabled user",
            targetName: companyName,
            reason: normalizedReason,
            beforeSummary,
            afterSummary: nextSuspended
                ? `Suspended${normalizedReason ? ` Ã¢â‚¬Â¢ ${normalizedReason}` : ""}`
                : "Active",
            detail: nextSuspended
                ? `${companyName} was suspended and will be blocked on next access.`
                : `${companyName} was re-enabled and can access Jentry again.`
        }
    });
}

async function updateWorkspaceBilling({ accountId, billingMode, actorEmail, request } = {}) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) {
        const error = new Error("Missing accountId.");
        error.statusCode = 400;
        throw error;
    }

    const nextBillingMode = normalizeBillingMode(billingMode);
    const normalizedActorEmail = ensureAllowedActor(actorEmail, request);

    await ensureCoreModelTables();

    const before = await pool.query(`SELECT * FROM accounts WHERE id = $1 LIMIT 1`, [normalizedAccountId]);
    const beforeRow = before.rows[0];
    if (!beforeRow) {
        const error = new Error("Workspace not found.");
        error.statusCode = 404;
        throw error;
    }

    const nextSubscriptionStatus = nextBillingMode === "free"
        ? "free"
        : (isPaidSubscriptionActive(beforeRow.subscription_status) ? beforeRow.subscription_status : "inactive");

    await pool.query(
        `
        UPDATE accounts
        SET billing_mode = $2,
            subscription_status = $3,
            subscription_amount_pence = COALESCE(subscription_amount_pence, $4),
            subscription_updated_at = now(),
            updated_at = now()
        WHERE id = $1
        `,
        [normalizedAccountId, nextBillingMode, nextSubscriptionStatus, JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE]
    );

    await writeAdminAuditLog(request, {
        action: nextBillingMode === "free" ? "billing.marked_free" : "billing.marked_paid_required",
        targetType: "workspace",
        targetId: normalizedAccountId,
        actorEmail: normalizedActorEmail,
        details: {
            category: "billing",
            actionTitle: nextBillingMode === "free" ? "Made account free" : "Made account paid",
            targetName: beforeRow.company_name || normalizedAccountId,
            beforeSummary: `${beforeRow.billing_mode || "free"} â€¢ ${beforeRow.subscription_status || "free"}`,
            afterSummary: `${nextBillingMode} â€¢ ${nextSubscriptionStatus}`,
            detail: nextBillingMode === "free"
                ? `${beforeRow.company_name || normalizedAccountId} now uses the free Jaccountancy plan.`
                : `${beforeRow.company_name || normalizedAccountId} now requires the Â£${JENTRY_PAID_SUBSCRIPTION_AMOUNT_GBP}/month paid plan.`
        }
    });
}

async function updateWorkspaceRegistry({ accountId, displayName, assignedUserEmail, inboxEmail, actorEmail, request } = {}) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    const normalizedDisplayName = normalizeOptionalString(displayName);
    const normalizedAssignedUserEmail = normalizeEmail(assignedUserEmail);
    const normalizedInboxEmail = normalizeEmail(inboxEmail);

    if (!normalizedAccountId) {
        const error = new Error("Missing accountId.");
        error.statusCode = 400;
        throw error;
    }

    const normalizedActorEmail = ensureAllowedActor(actorEmail, request);
    await ensureCoreModelTables();

    const before = await pool.query(
        `SELECT a.*, u.email AS assigned_user_email, ji.inbox_email FROM accounts a LEFT JOIN users u ON u.id = a.assigned_user_id LEFT JOIN jentry_inboxes ji ON ji.account_id = a.id AND ji.is_active = true WHERE a.id = $1 LIMIT 1`,
        [normalizedAccountId]
    );

    const beforeRow = before.rows[0] || null;
    let assignedUserId = beforeRow?.assigned_user_id || null;

    if (normalizedAssignedUserEmail) {
        const user = await pool.query(
            `
            INSERT INTO users (email, display_name, role, is_super_admin, updated_at)
            VALUES ($1, $2, 'user', $3, now())
            ON CONFLICT (email)
            DO UPDATE SET display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), users.display_name),
                is_super_admin = users.is_super_admin OR EXCLUDED.is_super_admin,
                updated_at = now()
            RETURNING id
            `,
            [
                normalizedAssignedUserEmail,
                normalizedAssignedUserEmail,
                SERVER_SUPER_ADMIN_EMAILS.has(normalizedAssignedUserEmail)
            ]
        );
        assignedUserId = user.rows[0]?.id || assignedUserId;
    }

    const updated = await upsertAccountRecord({
        accountId: normalizedAccountId,
        companyName: normalizedDisplayName || beforeRow?.company_name || null,
        assignedUserId,
        clientEmail: normalizedAssignedUserEmail || beforeRow?.client_email || null
    });

    if (assignedUserId) {
        await pool.query(
            `INSERT INTO memberships (user_id, account_id, role) VALUES ($1, $2, 'member') ON CONFLICT (user_id, account_id) DO NOTHING`,
            [assignedUserId, normalizedAccountId]
        );
    }

    if (normalizedInboxEmail) {
        await upsertJentryInboxRecord({
            accountId: normalizedAccountId,
            inboxEmail: normalizedInboxEmail,
            assignedUserEmail: normalizedAssignedUserEmail || beforeRow?.assigned_user_email || null,
            assignedUserId,
            updatedBy: normalizedActorEmail,
            isActive: true
        });
    }

    const targetName = updated?.companyName || normalizedDisplayName || beforeRow?.company_name || normalizedAccountId;
    await writeAdminAuditLog(request, {
        action: "workspace.registry_updated",
        targetType: "workspace",
        targetId: normalizedAccountId,
        actorEmail: normalizedActorEmail,
        details: {
            category: "registry",
            actionTitle: "Updated workspace registry",
            targetName,
            beforeSummary: [beforeRow?.company_name, beforeRow?.assigned_user_email, beforeRow?.inbox_email].filter(Boolean).join(" Ã¢â‚¬Â¢ ") || "No registry record",
            afterSummary: [normalizedDisplayName || updated?.companyName, normalizedAssignedUserEmail, normalizedInboxEmail].filter(Boolean).join(" Ã¢â‚¬Â¢ "),
            detail: `${targetName} registry details were updated.`
        }
    });
}

function normalizeRequiredAdminString(value) {
    return normalizeOptionalString(value);
}

function mapAdminWorkspaceRow(row) {
    const payload = row.payload && typeof row.payload === "object" ? row.payload : {};
    const chartOfAccounts = Array.isArray(payload.chartOfAccounts) ? payload.chartOfAccounts : [];
    const contacts = Array.isArray(payload.contacts) ? payload.contacts : [];
    const accountID = normalizeRequiredAdminString(row.account_id);
    const companyName = normalizeRequiredAdminString(row.company_name);
    const clientID = normalizeRequiredAdminString(row.client_id);
    const jentryInboxEmail = normalizeRequiredAdminString(row.jentry_inbox_email);

    return {
        accountID,
        companyName,
        clientID,
        displayName: normalizeRequiredAdminString(row.company_name || row.assigned_user_display_name),
        assignedUserEmail: row.assigned_user_email || row.client_email || null,
        jentryInboxEmail,
        xeroConnectedUserEmail: row.xero_connected_user_email || payload.connectedUserEmail || null,
        xeroOrganisationID: row.xero_organisation_id || payload.selectedTenantId || null,
        xeroOrganisationName: row.xero_organisation_name || payload.selectedTenantName || null,
        isConnectedToXero: Boolean(row.is_connected),
        requiresReconnect: Boolean(row.requires_reconnect),
        chartOfAccountsCount: chartOfAccounts.length,
        contactsCount: contacts.length,
        chartOfAccountsLastSyncedAt: toISODateTime(payload.chartOfAccountsLastSyncedAt),
        contactsLastSyncedAt: toISODateTime(payload.contactsLastSyncedAt),
        lastConnectedAt: toISODateTime(row.last_connected_at || payload.connectedAt),
        lastSeenAt: toISODateTime(row.last_seen_at || row.last_synced_at),
        lastSubmissionAt: toISODateTime(row.latest_submission_at || row.last_submission_at),
        submissionCount: Number(row.submission_count || 0),
        failedSubmissionCount: Number(row.failed_submission_count || 0),
        inXeroCount: Number(row.in_xero_count || 0),
        isSuspended: row.status === "suspended" || Boolean(row.assigned_user_is_suspended),
        suspensionReason: row.assigned_user_suspension_reason || null,
        suspendedAt: null,
        natureOfBusiness: row.nature_of_business || null,
        isVATRegistered: Boolean(row.is_vat_registered),
        billingMode: normalizeBillingMode(row.billing_mode),
        subscriptionStatus: normalizeSubscriptionStatus(row.subscription_status),
        subscriptionAmountPence: Number(row.subscription_amount_pence || JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE),
        subscriptionStartedAt: toISODateTime(row.subscription_started_at),
        subscriptionCurrentPeriodEnd: toISODateTime(row.subscription_current_period_end),
        stripeCustomerID: row.stripe_customer_id || null,
        stripeSubscriptionID: row.stripe_subscription_id || null
    };
}

function mapAdminAuditTrailRow(row) {
    const details = row.details_json && typeof row.details_json === "object" ? row.details_json : {};
    return {
        id: row.id,
        timestamp: toISODateTime(row.created_at),
        actorEmail: row.actor_email || null,
        category: details.category || row.target_type || "admin",
        actionTitle: details.actionTitle || row.action,
        targetName: details.targetName || row.company_name || null,
        targetID: row.target_id || null,
        detail: details.detail || null,
        reason: details.reason || null,
        beforeSummary: details.beforeSummary || null,
        afterSummary: details.afterSummary || null
    };
}

async function writeAdminAuditLog(request, { action, targetType, targetId, details = {}, actorEmail = null } = {}) {
    await ensureCoreModelTables();
    await pool.query(
        `
        INSERT INTO admin_audit_log (actor_user_id, actor_email, action, target_type, target_id, details_json)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        `,
        [
            request.authenticatedUser?.id || null,
            normalizeEmail(actorEmail) || request.superAdminEmail || request.authenticatedUser?.email || null,
            action,
            targetType || null,
            targetId || null,
            JSON.stringify(details || {})
        ]
    );
}

async function handleStripeWebhookEvent(event) {
    const eventType = normalizeOptionalString(event?.type);
    const object = event?.data?.object || {};

    if (eventType === "checkout.session.completed") {
        await syncStripeSubscriptionToAccount({
            accountId: normalizeOptionalString(object?.metadata?.accountId || object?.client_reference_id),
            stripeCustomerId: normalizeOptionalString(object?.customer),
            stripeSubscriptionId: normalizeOptionalString(object?.subscription)
        });
        return;
    }

    if (eventType === "customer.subscription.updated" || eventType === "customer.subscription.created") {
        await applyStripeSubscriptionRecord(object);
        return;
    }

    if (eventType === "customer.subscription.deleted") {
        await applyStripeSubscriptionRecord(object, { forceInactive: true });
    }
}

async function syncStripeSubscriptionToAccount({ accountId, stripeCustomerId, stripeSubscriptionId } = {}) {
    if (!STRIPE_SECRET_KEY || !accountId || !stripeSubscriptionId) return;

    const subscriptionResponse = await fetch(`${STRIPE_API_BASE_URL}/subscriptions/${encodeURIComponent(stripeSubscriptionId)}`, {
        headers: { Authorization: `Bearer ${STRIPE_SECRET_KEY}` }
    });
    const subscription = await subscriptionResponse.json().catch(() => null);
    if (!subscriptionResponse.ok || !subscription) return;

    await applyStripeSubscriptionRecord(subscription, { accountIdOverride: accountId, stripeCustomerIdOverride: stripeCustomerId });
}

async function applyStripeSubscriptionRecord(subscription, { forceInactive = false, accountIdOverride = "", stripeCustomerIdOverride = "" } = {}) {
    const subscriptionId = normalizeOptionalString(subscription?.id);
    if (!subscriptionId) return;

    const metadataAccountId = normalizeOptionalString(subscription?.metadata?.accountId);
    const stripeCustomerId = normalizeOptionalString(stripeCustomerIdOverride || subscription?.customer);
    const nextStatus = forceInactive ? "inactive" : normalizeSubscriptionStatus(subscription?.status);
    const accountId = metadataAccountId || normalizeOptionalString(accountIdOverride);

    await ensureCoreModelTables();

    let resolvedAccountId = accountId;
    if (!resolvedAccountId) {
        const lookup = await pool.query(
            `SELECT id FROM accounts WHERE stripe_subscription_id = $1 OR stripe_customer_id = $2 LIMIT 1`,
            [subscriptionId, stripeCustomerId || null]
        );
        resolvedAccountId = lookup.rows[0]?.id || "";
    }

    if (!resolvedAccountId) return;

    const priceAmountPence = Number(
        subscription?.items?.data?.[0]?.price?.unit_amount
        || subscription?.plan?.amount
        || JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE
    );
    const subscriptionStartedAt = subscription?.current_period_start
        ? new Date(Number(subscription.current_period_start) * 1000).toISOString()
        : null;
    const subscriptionCurrentPeriodEnd = subscription?.current_period_end
        ? new Date(Number(subscription.current_period_end) * 1000).toISOString()
        : null;

    await pool.query(
        `
        UPDATE accounts
        SET billing_mode = 'paid_required',
            subscription_status = $2,
            stripe_customer_id = COALESCE($3, stripe_customer_id),
            stripe_subscription_id = $4,
            subscription_amount_pence = $5,
            subscription_started_at = COALESCE($6::timestamptz, subscription_started_at),
            subscription_current_period_end = $7::timestamptz,
            subscription_updated_at = now(),
            updated_at = now()
        WHERE id = $1
        `,
        [
            resolvedAccountId,
            nextStatus,
            stripeCustomerId || null,
            subscriptionId,
            priceAmountPence,
            subscriptionStartedAt,
            subscriptionCurrentPeriodEnd
        ]
    );
}

function mapUserRow(row) {
    if (!row) return null;
    return {
        id: row.id,
        email: row.email,
        displayName: row.display_name || null,
        role: row.role || "user",
        isSuperAdmin: Boolean(row.is_super_admin),
        isSuspended: Boolean(row.is_suspended),
        suspensionReason: row.suspension_reason || null,
        lastSeenAt: row.last_seen_at || null,
        lastLoginAt: row.last_login_at || null,
        createdAt: row.created_at || null,
        updatedAt: row.updated_at || null
    };
}

function mapAccountRow(row) {
    if (!row) return null;
    return {
        accountId: row.id || row.account_id,
        companyName: row.company_name || null,
        clientId: row.client_id || null,
        clientEmail: row.client_email || null,
        natureOfBusiness: row.nature_of_business || null,
        isVatRegistered: Boolean(row.is_vat_registered),
        status: row.status || null,
        billingMode: normalizeBillingMode(row.billing_mode),
        subscriptionStatus: normalizeSubscriptionStatus(row.subscription_status),
        subscriptionAmountPence: Number(row.subscription_amount_pence || JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE),
        subscriptionStartedAt: row.subscription_started_at || null,
        subscriptionCurrentPeriodEnd: row.subscription_current_period_end || null,
        stripeCustomerId: row.stripe_customer_id || null,
        stripeSubscriptionId: row.stripe_subscription_id || null,
        assignedUserId: row.assigned_user_id || null,
        lastSubmissionAt: row.last_submission_at || null,
        createdAt: row.created_at || null,
        updatedAt: row.updated_at || null
    };
}

function mapAdminUserRow(row) {
    return {
        userId: row.id,
        email: row.email,
        displayName: row.display_name || null,
        role: row.role || "user",
        isSuperAdmin: Boolean(row.is_super_admin),
        isSuspended: Boolean(row.is_suspended),
        suspensionReason: row.suspension_reason || null,
        lastOnline: row.last_seen_at || null,
        lastLogin: row.last_login_at || null,
        assignedAccounts: row.assigned_accounts || [],
        connectedXeroEmail: row.connected_xero_email || null,
        jentryInboxes: row.jentry_inboxes || [],
        lastSubmission: row.last_submission_at || null
    };
}

function mapAdminAccountRow(row) {
    return {
        accountId: row.account_id,
        companyName: row.company_name || null,
        clientId: row.client_id || null,
        assignedUserEmail: row.assigned_user_email || null,
        assignedUserDisplayName: row.assigned_user_display_name || null,
        jentryInboxEmail: row.jentry_inbox_email || null,
        xeroConnectionStatus: row.is_connected ? "connected" : "disconnected",
        connectedXeroEmail: row.connected_xero_email || null,
        isSuspended: Boolean(row.is_suspended),
        lastSeenAt: row.last_seen_at || null,
        lastSubmissionAt: row.last_submission_at || null,
        submissionCounts: Number(row.submission_count || 0),
        isConnectedToXero: Boolean(row.is_connected),
        requiresReconnect: Boolean(row.requires_reconnect),
        tenantId: row.tenant_id || null,
        tenantName: row.tenant_name || null,
        status: row.status || null,
        billingMode: normalizeBillingMode(row.billing_mode),
        subscriptionStatus: normalizeSubscriptionStatus(row.subscription_status)
    };
}

function mapAccountAccessStatusRow(row) {
    return {
        accountId: row.id,
        companyName: row.company_name || null,
        isSuspended: row.status === "suspended",
        billingMode: normalizeBillingMode(row.billing_mode),
        subscriptionStatus: normalizeSubscriptionStatus(row.subscription_status),
        subscriptionAmountPence: Number(row.subscription_amount_pence || JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE),
        subscriptionStartedAt: row.subscription_started_at || null,
        subscriptionCurrentPeriodEnd: row.subscription_current_period_end || null,
        stripeCustomerId: row.stripe_customer_id || null,
        stripeSubscriptionId: row.stripe_subscription_id || null,
        requiresPaidSubscription: isBillingBlockedRow(row)
    };
}

function mapAdminSubscriptionRow(row) {
    return {
        accountId: row.account_id,
        companyName: row.company_name || null,
        clientEmail: row.client_email || null,
        tenantId: row.tenant_id || null,
        tenantName: row.tenant_name || null,
        billingMode: normalizeBillingMode(row.billing_mode),
        subscriptionStatus: normalizeSubscriptionStatus(row.subscription_status),
        subscriptionAmountPence: Number(row.subscription_amount_pence || JENTRY_PAID_SUBSCRIPTION_AMOUNT_PENCE),
        subscriptionStartedAt: row.subscription_started_at || null,
        subscriptionCurrentPeriodEnd: row.subscription_current_period_end || null,
        stripeCustomerId: row.stripe_customer_id || null,
        stripeSubscriptionId: row.stripe_subscription_id || null
    };
}

function mapInboxRow(row) {
    return {
        inboxId: row.id,
        accountId: row.account_id,
        inboxEmail: row.inbox_email,
        companyName: row.company_name || null,
        assignedUserEmail: row.assigned_user_email || row.user_email || null,
        assignedUserId: row.assigned_user_id || null,
        isActive: Boolean(row.is_active),
        updatedBy: row.updated_by || null,
        updatedAt: row.updated_at || null
    };
}

function mapXeroConnectionRow(row) {
    return {
        id: row.id || null,
        accountId: row.account_id,
        account: {
            accountId: row.account_id,
            companyName: row.company_name || null,
            clientId: row.client_id || null
        },
        tenantId: row.tenant_id || row.payload?.selectedTenantId || null,
        tenantName: row.tenant_name || row.payload?.selectedTenantName || null,
        connectedUserEmail: row.connected_user_email || row.payload?.connectedUserEmail || null,
        isConnected: Boolean(row.is_connected),
        requiresReconnect: Boolean(row.requires_reconnect),
        lastConnectedAt: row.last_connected_at || row.payload?.connectedAt || null,
        lastSync: row.last_synced_at || row.payload?.lastRefreshedAt || null,
        updatedAt: row.updated_at || null
    };
}

function mapAuditRow(row) {
    return {
        id: row.id,
        user: row.actor_email || row.actor_user_email || null,
        actorUserId: row.actor_user_id || null,
        account: row.company_name || null,
        action: row.action,
        targetType: row.target_type || null,
        targetId: row.target_id || null,
        submissionId: row.details_json?.submissionId || null,
        device: row.details_json?.device || null,
        details: row.details_json || {},
        createdAt: row.created_at
    };
}

async function ensureJentryAccountsTable() {
    await ensureCoreModelTables();
    await pool.query(`
        CREATE TABLE IF NOT EXISTS jentry_accounts (
            account_id text PRIMARY KEY,
            client_id text,
            company_name text,
            jentry_inbox_email text UNIQUE,
            created_at timestamptz NOT NULL DEFAULT now()
        )
    `);
}

async function resolveAccountIdFromInboundAddress(email) {
    const normalizedEmail = normalizeEmail(email);
    if (!normalizedEmail) return null;

    await ensureJentryAccountsTable();

    const inboxResult = await pool.query(
        `SELECT account_id FROM jentry_inboxes WHERE LOWER(inbox_email) = LOWER($1) AND is_active = true LIMIT 1`,
        [normalizedEmail]
    );

    if (inboxResult.rows[0]?.account_id) {
        return inboxResult.rows[0].account_id;
    }

    const result = await pool.query(
        `SELECT account_id FROM jentry_accounts WHERE LOWER(jentry_inbox_email) = LOWER($1) LIMIT 1`,
        [normalizedEmail]
    );

    return result.rows[0]?.account_id || null;
}

async function ensureInboundEmailSubmissionsTable() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS inbound_email_submissions (
            id text PRIMARY KEY,
            account_id text NOT NULL,
            inbox_email text NOT NULL,
            sender_email text,
            subject text,
            raw_payload jsonb NOT NULL,
            status text NOT NULL DEFAULT 'received',
            created_at timestamptz NOT NULL DEFAULT now()
        )
    `);
}

async function ensureInboundEmailAttachmentsTable() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS inbound_email_attachments (
            id text PRIMARY KEY,
            submission_id text NOT NULL,
            filename text NOT NULL,
            content_type text,
            file_data bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )
    `);
}

async function ensureProcessedInboundSubmissionsTable() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS processed_inbound_submissions (
            id text PRIMARY KEY,
            account_id text NOT NULL,
            title text,
            summary text,
            extracted_documents jsonb NOT NULL,
            status text NOT NULL DEFAULT 'ready',
            created_at timestamptz NOT NULL DEFAULT now()
        )
    `);
}

async function createInboundEmailSubmission({ accountId, inboxEmail, payload }) {
    await ensureInboundEmailSubmissionsTable();

    const id = crypto.randomUUID();

    await pool.query(
        `
        INSERT INTO inbound_email_submissions (
            id, account_id, inbox_email, sender_email, subject, raw_payload, status
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
        `,
        [
            id,
            accountId,
            inboxEmail,
            normalizeOptionalString(payload?.FromFull?.Email || payload?.From),
            normalizeOptionalString(payload?.Subject),
            JSON.stringify(payload),
            "received"
        ]
    );

    return { id, accountId };
}

async function saveInboundEmailAttachments(submissionId, attachments = []) {
    await ensureInboundEmailAttachmentsTable();

    for (const attachment of attachments) {
        await pool.query(
            `
            INSERT INTO inbound_email_attachments (
                id, submission_id, filename, content_type, file_data
            ) VALUES ($1, $2, $3, $4, $5)
            `,
            [
                crypto.randomUUID(),
                submissionId,
                attachment.originalname,
                attachment.mimetype || null,
                attachment.buffer
            ]
        );
    }
}

async function loadInboundEmailAttachments(submissionId) {
    await ensureInboundEmailAttachmentsTable();

    const result = await pool.query(
        `
        SELECT id, filename, content_type, file_data
        FROM inbound_email_attachments
        WHERE submission_id = $1
        ORDER BY created_at ASC
        `,
        [submissionId]
    );

    return result.rows.map((row) => ({
        id: row.id,
        originalname: row.filename,
        mimetype: row.content_type || "application/octet-stream",
        buffer: row.file_data
    }));
}

async function createProcessedInboundSubmission({
    submissionId,
    accountId,
    extractedDocuments = [],
    attachments = []
}) {
    await ensureProcessedInboundSubmissionsTable();

    const primary = extractedDocuments[0] || {};

    const title =
        normalizeOptionalString(primary.suggestedTitle) ||
        normalizeOptionalString(primary.summary) ||
        normalizeOptionalString(attachments[0]?.originalname) ||
        "Email submission";

    const summary =
        normalizeOptionalString(primary.longDescription) ||
        normalizeOptionalString(primary.shortDescription) ||
        normalizeOptionalString(primary.summary) ||
        "Processed from inbound email.";

    await markSubmissionAccepted({ accountId });

    await pool.query(
        `
        INSERT INTO processed_inbound_submissions (
            id, account_id, title, summary, extracted_documents, status
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        ON CONFLICT (id)
        DO UPDATE SET
            account_id = EXCLUDED.account_id,
            title = EXCLUDED.title,
            summary = EXCLUDED.summary,
            extracted_documents = EXCLUDED.extracted_documents,
            status = EXCLUDED.status
        `,
        [
            submissionId,
            accountId,
            title,
            summary,
            JSON.stringify(extractedDocuments),
            "ready"
        ]
    );
}

async function queueInboundEmailProcessing(submission, attachments = []) {
    setImmediate(async () => {
        try {
            if (!optionalEnvironmentVariables.openAIAPIKey) {
                throw new Error("OpenAI receipt extraction is not configured.");
            }

            const storedAttachments = attachments.length > 0
                ? attachments
                : await loadInboundEmailAttachments(submission.id);

            const extractedDocuments = [];

            for (const attachment of storedAttachments) {
                const extraction = await analyzeReceiptWithOpenAI({
                    buffer: attachment.buffer,
                    mimeType: attachment.mimetype || "application/pdf",
                    capturedAt: new Date().toISOString()
                });

                extractedDocuments.push({
                    ...extraction,
                    sourceFilename: attachment.originalname || null,
                    sourceMimeType: attachment.mimetype || null
                });
            }

            await createProcessedInboundSubmission({
                submissionId: submission.id,
                accountId: submission.accountId,
                extractedDocuments,
                attachments: storedAttachments
            });

            await pool.query(
                `UPDATE inbound_email_submissions SET status = 'processed' WHERE id = $1`,
                [submission.id]
            );
        } catch (error) {
            console.error("Inbound email processing failed.", {
                submissionId: submission.id,
                message: error instanceof Error ? error.message : String(error)
            });

            await pool.query(
                `UPDATE inbound_email_submissions SET status = 'failed' WHERE id = $1`,
                [submission.id]
            ).catch(() => undefined);
        }
    });
}

function extractPrimaryRecipient(payload) {
    const to =
        normalizeOptionalString(payload?.ToFull?.[0]?.Email) ||
        normalizeOptionalString(payload?.OriginalRecipient) ||
        normalizeOptionalString(payload?.To);

    if (!to) return "";

    const match = to.match(/<([^>]+)>/);
    return normalizeEmail(match ? match[1] : to);
}

function normalizePostmarkAttachments(attachments) {
    if (!Array.isArray(attachments)) return [];

    return attachments
        .map((attachment) => {
            const filename = normalizeOptionalString(attachment?.Name);
            const contentType = normalizeOptionalString(attachment?.ContentType) || "application/octet-stream";
            const content = normalizeOptionalString(attachment?.Content);

            if (!filename || !content) return null;

            const lower = filename.toLowerCase();

            const supported =
                contentType === "application/pdf" ||
                contentType.startsWith("image/") ||
                lower.endsWith(".pdf") ||
                lower.endsWith(".jpg") ||
                lower.endsWith(".jpeg") ||
                lower.endsWith(".png") ||
                lower.endsWith(".heic");

            if (!supported) return null;

            return {
                originalname: filename,
                mimetype: contentType,
                buffer: Buffer.from(content, "base64"),
                size: Buffer.byteLength(content, "base64")
            };
        })
        .filter(Boolean);
}

function parseJWTPayload(token) {
    const normalizedToken = normalizeOptionalString(token);
    if (!normalizedToken) return null;

    const parts = normalizedToken.split(".");
    if (parts.length < 2) return null;

    try {
        const payload = parts[1]
            .replace(/-/g, "+")
            .replace(/_/g, "/");

        const paddedPayload = payload + "=".repeat((4 - (payload.length % 4)) % 4);
        return JSON.parse(Buffer.from(paddedPayload, "base64").toString("utf8"));
    } catch {
        return null;
    }
}

function extractConnectedUserEmail(idToken, fallbackEmail = "") {
    const claims = parseJWTPayload(idToken);

    return normalizeOptionalString(
        claims?.email ||
        claims?.preferred_username ||
        claims?.upn ||
        fallbackEmail
    ) || null;
}

function normalizeEmail(value) {
    if (typeof value !== "string") {
        return "";
    }

    return value.trim().toLowerCase();
}

function normalizeOptionalString(value) {
    if (typeof value !== "string") {
        return "";
    }

    const trimmed = value.trim();
    return trimmed ? trimmed : "";
}

function normalizeBillingMode(value) {
    const normalized = normalizeOptionalString(value).toLowerCase();
    if (normalized === "paid_required" || normalized === "subscribed" || normalized === "free") {
        return normalized;
    }
    return "free";
}

function normalizeSubscriptionStatus(value) {
    const normalized = normalizeOptionalString(value).toLowerCase();
    return normalized || "free";
}

function isPaidSubscriptionActive(status) {
    return new Set(["active", "trialing", "paid"]).has(normalizeSubscriptionStatus(status));
}

function isFreeJaccountancyClient(email) {
    const normalizedEmail = normalizeEmail(email);
    const domain = normalizedEmail.split("@")[1] || "";
    return Boolean(domain) && JACCOUNTANCY_FREE_CLIENT_DOMAINS.has(domain);
}

function isBillingBlockedRow(row) {
    const billingMode = normalizeBillingMode(row?.billing_mode);
    const subscriptionStatus = normalizeSubscriptionStatus(row?.subscription_status);
    return billingMode === "paid_required" && !isPaidSubscriptionActive(subscriptionStatus);
}

function stripeCheckoutSuccessURL(request) {
    return normalizeOptionalString(
        process.env.STRIPE_SUCCESS_URL ||
        request.body?.successURL ||
        request.body?.successUrl ||
        request.query?.successURL ||
        request.query?.successUrl
    ) || "jentry://subscription-success?session_id={CHECKOUT_SESSION_ID}";
}

function stripeCheckoutCancelURL(request) {
    return normalizeOptionalString(
        process.env.STRIPE_CANCEL_URL ||
        request.body?.cancelURL ||
        request.body?.cancelUrl ||
        request.query?.cancelURL ||
        request.query?.cancelUrl
    ) || "jentry://subscription-cancelled";
}

function verifyStripeWebhookEvent(rawBody, stripeSignature, webhookSecret) {
    if (!Buffer.isBuffer(rawBody)) {
        throw new Error("Stripe webhook body must be raw bytes.");
    }

    const timestamp = normalizeOptionalString(
        stripeSignature
            .split(",")
            .find((entry) => entry.startsWith("t="))
            ?.slice(2)
    );
    const signature = normalizeOptionalString(
        stripeSignature
            .split(",")
            .find((entry) => entry.startsWith("v1="))
            ?.slice(3)
    );

    if (!timestamp || !signature) {
        throw new Error("Stripe webhook signature is invalid.");
    }

    const expected = crypto
        .createHmac("sha256", webhookSecret)
        .update(`${timestamp}.${rawBody.toString("utf8")}`, "utf8")
        .digest("hex");

    const expectedBuffer = Buffer.from(expected, "hex");
    const signatureBuffer = Buffer.from(signature, "hex");

    if (expectedBuffer.length !== signatureBuffer.length || !crypto.timingSafeEqual(expectedBuffer, signatureBuffer)) {
        throw new Error("Stripe webhook signature verification failed.");
    }

    return JSON.parse(rawBody.toString("utf8"));
}

function buildXeroHistoryDetail(metadata = {}) {
    const submittedByName = normalizeOptionalString(metadata.submittedByName);
    const submittedByEmail = normalizeOptionalString(metadata.submittedByEmail);
    const submitterLabel = submittedByEmail || submittedByName || null;

    return submitterLabel
        ? `Submitted with Xero API by Jentry through ${submitterLabel}`
        : "Submitted with Xero API by Jentry";
}

function parseJSONField(value, fallback) {
    if (value == null || value === "") {
        return fallback;
    }

    if (typeof value === "object") {
        return value;
    }

    try {
        return JSON.parse(String(value));
    } catch {
        return fallback;
    }
}

function getSenderEmail() {
    return normalizeEmail(
        process.env.GMAIL_SENDER_EMAIL ||
        process.env.GMAIL_SENDER ||
        process.env.JENTRY_BACKEND_SENDER_EMAIL ||
        "theteam@jaccountancy.co.uk"
    );
}

function xeroClientID() {
    return normalizeOptionalString(process.env.XERO_CLIENT_ID);
}

function xeroClientSecret() {
    return normalizeOptionalString(process.env.XERO_CLIENT_SECRET);
}

function normalizeAbsoluteURL(value) {
    const rawValue = normalizeOptionalString(value);
    if (!rawValue) return null;

    try {
        return new URL(rawValue).toString();
    } catch {
        return null;
    }
}

function normalizeWebURL(value) {
    const absoluteURL = normalizeAbsoluteURL(value);
    if (!absoluteURL) return null;

    try {
        const parsedURL = new URL(absoluteURL);
        return parsedURL.protocol === "http:" || parsedURL.protocol === "https:"
            ? parsedURL.toString()
            : null;
    } catch {
        return null;
    }
}

function xeroRedirectURI() {
    const fallbackURL = "https://jentry-jentry.up.railway.app/oauth/xero/callback";
    const configuredURL = normalizeWebURL(process.env.XERO_REDIRECT_URI);

    if (configuredURL) return configuredURL;

    const rawConfiguredURL = normalizeOptionalString(process.env.XERO_REDIRECT_URI);
    if (rawConfiguredURL) {
        console.warn(
            "Ignoring invalid XERO_REDIRECT_URI. Xero requires an http/https callback URL.",
            rawConfiguredURL
        );
    }

    return fallbackURL;
}

function xeroAppRedirectURI() {
    return normalizeAbsoluteURL(process.env.JENTRY_XERO_APP_REDIRECT_URI)
        || "jentry://oauth/xero/callback";
}

function xeroScopes() {
    const rawScopes = normalizeOptionalString(process.env.XERO_SCOPES)
        || "openid profile email offline_access accounting.transactions accounting.attachments accounting.contacts accounting.settings";

    return rawScopes
        .split(/\s+/)
        .filter(Boolean)
        .join(" ");
}

function xeroConfigured() {
    return Boolean(xeroClientID() && xeroClientSecret() && xeroRedirectURI());
}

function hasXeroScope(connection, requiredScope) {
    const normalizedScope = normalizeOptionalString(requiredScope);
    if (!normalizedScope) return true;

    const grantedScopes = String(connection?.scope || "")
        .split(/\s+/)
        .map((scope) => normalizeOptionalString(scope))
        .filter(Boolean);

    return grantedScopes.includes(normalizedScope);
}

function missingXeroScopes(connection, requiredScopes = []) {
    return requiredScopes.filter((scope) => !hasXeroScope(connection, scope));
}

function toBase64Url(value) {
    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value);

    return buffer
        .toString("base64")
        .replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/=+$/g, "");
}

function createPKCEPair() {
    const verifier = toBase64Url(crypto.randomBytes(32));
    const challenge = toBase64Url(crypto.createHash("sha256").update(verifier).digest());
    return { verifier, challenge };
}

function buildReturnURL(baseURL, params) {
    const target = new URL(baseURL);

    Object.entries(params).forEach(([key, value]) => {
        if (value != null && value !== "") {
            target.searchParams.set(key, String(value));
        }
    });

    return target.toString();
}

async function ensureXeroConnectionsTable() {
    await ensureCoreModelTables();
}

async function getXeroConnection(accountId) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) return null;

    await ensureXeroConnectionsTable();

    const result = await pool.query(
        `SELECT payload FROM xero_connections WHERE account_id = $1`,
        [normalizedAccountId]
    );

    return result.rows[0]?.payload || null;
}

async function upsertXeroConnection(accountId, updater) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) {
        throw new Error("Missing accountId.");
    }

    const previous = await getXeroConnection(normalizedAccountId);
    const next = await updater(previous);

    await ensureXeroConnectionsTable();

    await upsertAccountRecord({ accountId: normalizedAccountId });

    await pool.query(
        `
        INSERT INTO xero_connections (
            account_id, payload, connected_user_email, tenant_id, tenant_name,
            is_connected, requires_reconnect, last_connected_at, last_synced_at, updated_at
        )
        VALUES ($1, $2::jsonb, $3, $4, $5, true, false, COALESCE($6::timestamptz, now()), now(), now())
        ON CONFLICT (account_id)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            connected_user_email = EXCLUDED.connected_user_email,
            tenant_id = EXCLUDED.tenant_id,
            tenant_name = EXCLUDED.tenant_name,
            is_connected = true,
            requires_reconnect = false,
            last_connected_at = COALESCE(xero_connections.last_connected_at, EXCLUDED.last_connected_at),
            last_synced_at = now(),
            updated_at = now()
        `,
        [
            normalizedAccountId,
            JSON.stringify(next),
            next.connectedUserEmail || null,
            next.selectedTenantId || null,
            next.selectedTenantName || null,
            next.connectedAt || null
        ]
    );

    return next;
}

async function deleteXeroConnection(accountId) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) return;

    await ensureXeroConnectionsTable();

    await pool.query(
        `UPDATE xero_connections SET is_connected = false, requires_reconnect = true, updated_at = now() WHERE account_id = $1`,
        [normalizedAccountId]
    );
}

async function exchangeXeroToken(params) {
    const body = new URLSearchParams(params);

    const response = await fetch(XERO_TOKEN_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        body
    });

    const { data, raw } = await readXeroResponse(response);

    if (!response.ok) {
        const error = classifyXeroError(response.status, data || raw, "Xero token exchange failed.");
        if (response.status === 400 || data?.error === "invalid_grant") {
            throw new XeroAPIError({
                message: "Xero authorization expired.",
                code: "XERO_AUTH_EXPIRED",
                status: 401,
                requiresReconnect: true,
                upstreamStatus: response.status,
                upstreamBody: data || raw
            });
        }
        throw error;
    }

    return data || {};
}

async function fetchXeroTenants(accessToken) {
    const response = await fetch(XERO_CONNECTIONS_URL, {
        headers: {
            Authorization: `Bearer ${accessToken}`,
            Accept: "application/json"
        }
    });

    const { data, raw } = await readXeroResponse(response);

    if (!response.ok) {
        throw classifyXeroError(response.status, data || raw, "Failed to fetch Xero organisations.");
    }

    return sanitizeTenants(data);
}

function sanitizeTenants(connections) {
    if (!Array.isArray(connections)) return [];

    return connections
        .map((connection) => ({
            id: normalizeOptionalString(connection.id || connection.connectionId),
            tenantId: normalizeOptionalString(connection.tenantId),
            tenantName: normalizeOptionalString(connection.tenantName),
            tenantType: normalizeOptionalString(connection.tenantType) || null,
            createdDateUtc: normalizeOptionalString(connection.createdDateUtc) || null,
            updatedDateUtc: normalizeOptionalString(connection.updatedDateUtc) || null
        }))
        .filter((tenant) => tenant.tenantId && tenant.tenantName);
}

async function fetchChartOfAccounts(accessToken, tenantId) {
    const response = await fetch(XERO_ACCOUNTS_URL, {
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "xero-tenant-id": tenantId,
            Accept: "application/json"
        }
    });

    const { data, raw } = await readXeroResponse(response);

    if (!response.ok) {
        throw classifyXeroError(response.status, data || raw, "Failed to fetch Xero chart of accounts.");
    }

    const accounts = Array.isArray(data?.Accounts) ? data.Accounts : [];

    return accounts
        .map((account) => ({
            code: normalizeOptionalString(account.Code),
            name: normalizeOptionalString(account.Name),
            type: normalizeOptionalString(account.Type) || null,
            classType: normalizeOptionalString(account.Class) || null,
            status: normalizeOptionalString(account.Status) || null,
            taxType: normalizeOptionalString(account.TaxType) || null
        }))
        .filter((account) => account.code && account.name && account.status !== "ARCHIVED")
        .sort((lhs, rhs) => lhs.code.localeCompare(rhs.code, undefined, { numeric: true }));
}

function tokenExpiresSoon(expiresAt) {
    if (!expiresAt) return true;

    return new Date(expiresAt).getTime() - Date.now() < 120000;
}

async function ensureFreshXeroConnection(accountId) {
    const connection = await getXeroConnection(accountId);

    if (!connection) {
        throw new XeroAPIError({
            message: "No Xero connection found for this account.",
            code: "XERO_NOT_CONNECTED",
            status: 401,
            requiresReconnect: true
        });
    }

    if (tokenExpiresSoon(connection.expiresAt)) {
        return refreshXeroConnection(accountId, connection);
    }

    try {
        await fetchXeroTenants(connection.accessToken);
        return connection;
    } catch (error) {
        if (error instanceof XeroAPIError && error.status === 403) {
            throw error;
        }
        return refreshXeroConnection(accountId, connection);
    }
}

async function refreshXeroConnection(accountId, connection) {
    if (!connection.refreshToken) {
        throw new XeroAPIError({
            message: "Xero authorization expired.",
            code: "XERO_AUTH_EXPIRED",
            status: 401,
            requiresReconnect: true
        });
    }

    const tokenData = await exchangeXeroToken({
        grant_type: "refresh_token",
        client_id: xeroClientID(),
        client_secret: xeroClientSecret(),
        refresh_token: connection.refreshToken
    });

    const tenants = await fetchXeroTenants(tokenData.access_token).catch(() => []);

    return upsertXeroConnection(accountId, async (previous) => {
        const selectedTenantId = previous?.selectedTenantId || tenants[0]?.tenantId || null;
        const selectedTenant = tenants.find((tenant) => tenant.tenantId === selectedTenantId) || tenants[0] || null;

        const chartOfAccounts = selectedTenant
            ? await fetchChartOfAccounts(tokenData.access_token, selectedTenant.tenantId)
                .catch(() => previous?.chartOfAccounts || [])
            : previous?.chartOfAccounts || [];

        return {
            accountId: String(accountId),
            accessToken: tokenData.access_token,
            refreshToken: tokenData.refresh_token,
            idToken: tokenData.id_token || null,
            connectedUserEmail: extractConnectedUserEmail(
                tokenData.id_token,
                previous?.connectedUserEmail
            ),
            scope: tokenData.scope || previous?.scope || xeroScopes(),
            expiresAt: new Date(Date.now() + Number(tokenData.expires_in || 1800) * 1000).toISOString(),
            connectedAt: previous?.connectedAt || new Date().toISOString(),
            lastRefreshedAt: new Date().toISOString(),
            tenants,
            selectedTenantId: selectedTenant?.tenantId || null,
            selectedTenantName: selectedTenant?.tenantName || null,
            chartOfAccounts,
            chartOfAccountsLastSyncedAt: chartOfAccounts.length > 0
                ? new Date().toISOString()
                : previous?.chartOfAccountsLastSyncedAt || null
        };
    });
}

function xeroDateTimeLiteral(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return "";
    }

    return `DateTime(${date.getUTCFullYear()},${date.getUTCMonth() + 1},${date.getUTCDate()})`;
}

function xeroDateRangeWhereClause(dateISO, fieldName = "Date", windowDays = 14) {
    const normalizedDate = normalizeOptionalString(dateISO);
    if (!normalizedDate) {
        return "";
    }

    const centerDate = new Date(normalizedDate);
    if (Number.isNaN(centerDate.getTime())) {
        return "";
    }

    const startDate = new Date(centerDate);
    startDate.setUTCDate(startDate.getUTCDate() - windowDays);

    const endDate = new Date(centerDate);
    endDate.setUTCDate(endDate.getUTCDate() + windowDays);

    return `${fieldName}>=${xeroDateTimeLiteral(startDate)}&&${fieldName}<=${xeroDateTimeLiteral(endDate)}`;
}

function appendXeroQueryParameters(url, parameters) {
    const target = new URL(url);
    for (const [key, value] of Object.entries(parameters)) {
        const normalizedValue = normalizeOptionalString(value);
        if (normalizedValue) {
            target.searchParams.set(key, normalizedValue);
        }
    }
    return target.toString();
}

async function xeroGetJSON(url, { accessToken, tenantId }) {
    const response = await fetch(url, {
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "xero-tenant-id": tenantId,
            Accept: "application/json"
        }
    });

    const { data, raw } = await readXeroResponse(response);

    if (!response.ok) {
        throw classifyXeroError(response.status, data || raw, "Xero request failed.");
    }

    return data || {};
}

// =========================
// XERO MATCHING
// =========================

async function findMatchingXeroTransactions({ accessToken, tenantId, document }) {
    const merchant = normalizeOptionalString(document.merchant);
    const totalAmount = coerceNumber(document.totalAmount ?? parseAmountText(document.totalText));
    const dateISO = normalizeOptionalString(document.dateISO);
    const invoiceNumber = normalizeOptionalString(document.invoiceNumber);

    const [invoiceData, bankTransactionData] = await Promise.all([
        xeroGetJSON(XERO_INVOICES_URL, { accessToken, tenantId }).catch(() => ({ Invoices: [] })),
        xeroGetJSON(XERO_BANK_TRANSACTIONS_URL, { accessToken, tenantId }).catch(() => ({ BankTransactions: [] }))
    ]);

    const invoiceMatches = Array.isArray(invoiceData?.Invoices)
        ? invoiceData.Invoices.map((invoice) => ({
            kind: "invoice",
            id: normalizeOptionalString(invoice.InvoiceID),
            reference: normalizeOptionalString(invoice.InvoiceNumber) || normalizeOptionalString(invoice.Reference),
            contactName: normalizeOptionalString(invoice.Contact?.Name),
            amount: coerceNumber(invoice.Total),
            date: normalizeOptionalString(invoice.DateString || invoice.Date),
            status: normalizeOptionalString(invoice.Status),
            raw: invoice
        }))
        : [];

    const bankMatches = Array.isArray(bankTransactionData?.BankTransactions)
        ? bankTransactionData.BankTransactions.map((transaction) => ({
            kind: "bankTransaction",
            id: normalizeOptionalString(transaction.BankTransactionID),
            reference: normalizeOptionalString(transaction.Reference),
            contactName: normalizeOptionalString(transaction.Contact?.Name),
            amount: coerceNumber(transaction.Total),
            date: normalizeOptionalString(transaction.DateString || transaction.Date),
            status: normalizeOptionalString(transaction.Status),
            raw: transaction
        }))
        : [];

    return [...invoiceMatches, ...bankMatches]
        .map((candidate) => scoreTransactionMatch(candidate, {
            merchant,
            totalAmount,
            dateISO,
            invoiceNumber
        }))
        .filter((candidate) => candidate.score >= 45)
        .sort((lhs, rhs) => rhs.score - lhs.score)
        .slice(0, 6);
}

function scoreTransactionMatch(candidate, document) {
    let score = 0;

    const normalizedMerchant = normalizeComparableText(document.merchant);
    const normalizedContact = normalizeComparableText(candidate.contactName);
    const normalizedReference = normalizeComparableText(candidate.reference);

    if (normalizedMerchant && normalizedContact.includes(normalizedMerchant)) {
        score += 35;
    } else if (normalizedMerchant && normalizedReference.includes(normalizedMerchant)) {
        score += 20;
    }

    if (document.invoiceNumber) {
        const normalizedInvoiceNumber = normalizeComparableText(document.invoiceNumber);
        if (normalizedInvoiceNumber && normalizedReference.includes(normalizedInvoiceNumber)) {
            score += 35;
        }
    }

    if (document.totalAmount != null && candidate.amount != null) {
        const delta = Math.abs(document.totalAmount - candidate.amount);
        if (delta < 0.01) score += 30;
        else if (delta <= 1) score += 18;
        else if (delta <= 5) score += 8;
    }

    const daysApart = dayDistance(document.dateISO, candidate.date);
    if (daysApart != null) {
        if (daysApart === 0) score += 25;
        else if (daysApart <= 3) score += 15;
        else if (daysApart <= 10) score += 6;
    }

    return {
        transactionID: candidate.id,
        kind: candidate.kind,
        reference: candidate.reference || null,
        contactName: candidate.contactName || null,
        amount: candidate.amount ?? null,
        date: candidate.date || null,
        status: candidate.status || null,
        score,
        summary: buildMatchSummary(candidate, score)
    };
}

function buildMatchSummary(candidate, score) {
    const fragments = [
        candidate.contactName || candidate.reference || "Existing Xero transaction",
        candidate.amount != null ? formatCurrency(candidate.amount) : null,
        candidate.date || null,
        `match score ${score}`
    ].filter(Boolean);

    return fragments.join(" Ã¢â‚¬Â¢ ");
}

function normalizeComparableText(value) {
    return normalizeOptionalString(value)
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");
}

function parseAmountText(value) {
    const normalized = normalizeOptionalString(value).replace(/[^0-9.-]/g, "");
    const parsed = Number(normalized);

    return Number.isFinite(parsed) ? parsed : null;
}

function coerceNumber(value) {
    return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function dayDistance(lhs, rhs) {
    if (!lhs || !rhs) return null;

    const lhsDate = new Date(lhs);
    const rhsDate = new Date(rhs);

    if (Number.isNaN(lhsDate.getTime()) || Number.isNaN(rhsDate.getTime())) {
        return null;
    }

    return Math.abs(Math.round((lhsDate.getTime() - rhsDate.getTime()) / 86400000));
}

function formatCurrency(value) {
    return new Intl.NumberFormat("en-GB", {
        style: "currency",
        currency: "GBP"
    }).format(value);
}

// New helpers to infer previous supplier coding from Xero
async function findPreviousSupplierCoding({ accessToken, tenantId, supplierName }) {
    const data = await xeroGetJSON(XERO_INVOICES_URL, {
        accessToken,
        tenantId
    });
    const invoices = Array.isArray(data?.Invoices) ? data.Invoices : [];
    const matchingBills = invoices.filter((invoice) => {
        return normalizeComparableText(invoice.Contact?.Name)
            .includes(normalizeComparableText(supplierName));
    });
    const accountCodes = [];
    for (const bill of matchingBills) {
        for (const line of bill.LineItems || []) {
            if (line.AccountCode) {
                accountCodes.push({
                    accountCode: line.AccountCode,
                    description: line.Description || "",
                    taxType: line.TaxType || ""
                });
            }
        }
    }
    return accountCodes;
}

function pickMostCommonAccountCode(previousCoding) {
    const counts = new Map();
    for (const item of previousCoding) {
        if (!item.accountCode) continue;
        counts.set(item.accountCode, (counts.get(item.accountCode) || 0) + 1);
    }
    return [...counts.entries()]
        .sort((a, b) => b[1] - a[1])[0]?.[0] || null;
}

function pickDocumentDate(extractedDocuments) {
    const candidates = Array.isArray(extractedDocuments) ? extractedDocuments : [];

    for (const document of candidates) {
        if (typeof document?.dateISO === "string" && document.dateISO.trim()) {
            return document.dateISO.trim().slice(0, 10);
        }
    }

    return new Date().toISOString().slice(0, 10);
}

// buildBillLineItems is now async to allow Xero lookups for historical coding
async function buildBillLineItems(metadata) {
    const extractedDocuments = Array.isArray(metadata.extractedDocuments)
        ? metadata.extractedDocuments
        : [];

    const chartOfAccounts = Array.isArray(metadata.chartOfAccounts)
        ? metadata.chartOfAccounts
        : [];

    const lineItems = [];

    for (const document of extractedDocuments) {
        // Suggest a code from historical supplier coding in Xero (if available)
     let suggestedCode = null;

if (normalizeOptionalString(document.merchant)) {
    try {
        const previousCoding = await findPreviousSupplierCoding({
            accessToken: metadata.accessToken,
            tenantId: metadata.tenantId,
            supplierName: document.merchant
        });

        suggestedCode = pickMostCommonAccountCode(previousCoding);
    } catch (error) {
        console.warn("Previous supplier coding lookup failed; continuing without suggestion.", {
            message: error instanceof Error ? error.message : String(error)
        });
    }
}

        const documentLineItems = Array.isArray(document?.lineItems) ? document.lineItems : [];
        const lineItemCode = normalizeOptionalString(document?.nominalCode || document?.accountCode);
        const fallbackTaxType = normalizeOptionalString(document?.taxType);

        if (documentLineItems.length > 0) {
            for (const item of documentLineItems) {
                const rawAmount = normalizeOptionalString(item?.amountText).replace(/[^0-9.-]/g, "");
                const parsedAmount = Number(rawAmount);
                const code = normalizeOptionalString(item?.nominalCode || lineItemCode);
                const taxType = normalizeOptionalString(
                    item?.taxType ||
                    fallbackTaxType ||
                    inferTaxTypeFromDocument(document, chartOfAccounts, code)
                );

                lineItems.push(compactObject({
                    Description: String(item?.name || "Receipt line item").slice(0, 4000),
                    Quantity: Number(normalizeOptionalString(item?.quantity).replace(/[^0-9.-]/g, "")) || 1,
                    UnitAmount: Number.isFinite(parsedAmount) && parsedAmount !== 0 ? parsedAmount : undefined,
                    AccountCode: code || suggestedCode || undefined,
                    TaxType: taxType || undefined
                }));
            }
        } else {
            const description = String(
                document?.shortDescription ||
                document?.summary ||
                document?.merchant ||
                "Receipt"
            ).trim();

            const parsedTotal = Number(document?.totalAmount);
            const code = lineItemCode;
            const taxType = normalizeOptionalString(
                document?.taxType ||
                inferTaxTypeFromDocument(document, chartOfAccounts, code)
            );

            lineItems.push(compactObject({
                Description: description.slice(0, 4000),
                Quantity: 1,
                UnitAmount: Number.isFinite(parsedTotal) && parsedTotal > 0 ? parsedTotal : undefined,
                AccountCode: code || suggestedCode || undefined,
                TaxType: taxType || undefined
            }));
        }
    }

    if (lineItems.length > 0) {
        return lineItems;
    }

    const descriptions = Array.isArray(metadata.descriptions) ? metadata.descriptions : [];
    const fallbackDescription = descriptions.join(", ").slice(0, 4000) || "Jentry document submission";

    return [
        {
            Description: fallbackDescription,
            Quantity: 1
        }
    ];
}

function createPublishDocumentHash(documentFiles = []) {
    const hash = crypto.createHash("sha256");
    for (const file of documentFiles) {
        hash.update(file.originalname || "");
        hash.update(String(file.size || file.buffer?.length || 0));
        if (file.buffer?.length) hash.update(file.buffer);
    }
    return hash.digest("hex");
}

async function ensureXeroAuthSessionsTable() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS xero_auth_sessions (
            state text PRIMARY KEY,
            account_id text NOT NULL,
            return_uri text NOT NULL,
            verifier text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )
    `);
}

async function createXeroAuthSession({ state, accountId, returnUri, verifier }) {
    await ensureXeroAuthSessionsTable();
    await pool.query(
        `
        INSERT INTO xero_auth_sessions (state, account_id, return_uri, verifier, created_at)
        VALUES ($1, $2, $3, $4, now())
        `,
        [state, accountId, returnUri, verifier]
    );
}

async function getXeroAuthSession(state) {
    await ensureXeroAuthSessionsTable();
    await pool.query(`DELETE FROM xero_auth_sessions WHERE created_at < now() - interval '30 minutes'`);

    const result = await pool.query(
        `
        SELECT state, account_id, return_uri, verifier, created_at
        FROM xero_auth_sessions
        WHERE state = $1
        LIMIT 1
        `,
        [state]
    );

    const row = result.rows[0];
    if (!row) return null;

    return {
        state: row.state,
        accountId: row.account_id,
        returnUri: row.return_uri,
        verifier: row.verifier,
        createdAt: row.created_at
    };
}

async function deleteXeroAuthSession(state) {
    if (!state) return;
    await ensureXeroAuthSessionsTable();
    await pool.query(`DELETE FROM xero_auth_sessions WHERE state = $1`, [state]);
}

async function ensureXeroPublishRecordsTable() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS xero_publish_records (
            account_id text NOT NULL,
            submission_id text NOT NULL,
            document_hash text,
            invoice_id text,
            status text NOT NULL DEFAULT 'pending',
            attachments_uploaded boolean,
            warning text,
            xero_invoice jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (account_id, submission_id)
        )
    `);
}

async function createXeroPublishRecord({ accountId, submissionId, documentHash, status }) {
    await ensureXeroPublishRecordsTable();
    await pool.query(
        `
        INSERT INTO xero_publish_records (account_id, submission_id, document_hash, status, updated_at)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (account_id, submission_id)
        DO UPDATE SET status = EXCLUDED.status,
            document_hash = COALESCE(xero_publish_records.document_hash, EXCLUDED.document_hash),
            warning = null,
            updated_at = now()
        `,
        [accountId, submissionId, documentHash || null, status || "pending"]
    );
}

async function getXeroPublishRecord({ accountId, submissionId }) {
    await ensureXeroPublishRecordsTable();
    const result = await pool.query(
        `SELECT * FROM xero_publish_records WHERE account_id = $1 AND submission_id = $2 LIMIT 1`,
        [accountId, submissionId]
    );
    return result.rows[0] || null;
}

async function updateXeroPublishRecord({
    accountId,
    submissionId,
    invoiceId = null,
    status,
    attachmentsUploaded = null,
    warning = null,
    xeroInvoice = null
}) {
    await ensureXeroPublishRecordsTable();
    await pool.query(
        `
        UPDATE xero_publish_records
        SET invoice_id = COALESCE($3, invoice_id),
            status = COALESCE($4, status),
            attachments_uploaded = COALESCE($5, attachments_uploaded),
            warning = $6,
            xero_invoice = COALESCE($7::jsonb, xero_invoice),
            updated_at = now()
        WHERE account_id = $1 AND submission_id = $2
        `,
        [
            accountId,
            submissionId,
            invoiceId,
            status || null,
            attachmentsUploaded,
            warning,
            xeroInvoice ? JSON.stringify(xeroInvoice) : null
        ]
    );
}

function inferTaxTypeFromDocument(document, chartOfAccounts, accountCode) {
    if (normalizeOptionalString(document?.vatText) || document?.vatAmount != null) {
        return "INPUT";
    }

    if (!accountCode) return "";

    const matchedAccount = chartOfAccounts.find((account) => account.code === accountCode);
    return matchedAccount?.taxType || "";
}

function compactObject(value) {
    return Object.fromEntries(
        Object.entries(value).filter(([, candidate]) => candidate !== undefined && candidate !== "")
    );
}

async function buildXeroDocumentPayload(metadata) {
    const extractedDocuments = Array.isArray(metadata.extractedDocuments)
        ? metadata.extractedDocuments
        : [];

    const primaryDocument = extractedDocuments[0] || {};
    const documentType = normalizeOptionalString(metadata.documentType) === "sales"
        ? "sales"
        : "purchase";

    const contactName = normalizeOptionalString(primaryDocument.merchant)
        || `${normalizeOptionalString(metadata.companyName) || "Jentry"} ${
            documentType === "sales" ? "customer" : "supplier"
        }`;

    const referenceBase = normalizeOptionalString(primaryDocument.invoiceNumber)
        || normalizeOptionalString(metadata.submissionId)
        || `jentry-${Date.now()}`;

    const status = normalizeOptionalString(metadata.approvalStatus) === "draft"
        ? "DRAFT"
        : "AUTHORISED";

    return {
        Type: documentType === "sales" ? "ACCREC" : "ACCPAY",
        Contact: {
            Name: contactName.slice(0, 255)
        },
        DateString: pickDocumentDate(extractedDocuments),
        DueDateString: pickDocumentDate(extractedDocuments),
        Status: status,
        LineAmountTypes: normalizeOptionalString(metadata.lineAmountTypes) || "Inclusive",
        Reference: `${normalizeOptionalString(metadata.clientId)} ${referenceBase}`.trim().slice(0, 255),
        InvoiceNumber: referenceBase.slice(0, 255),
        LineItems: await buildBillLineItems(metadata)
    };
}

async function uploadAttachmentToXero({ accessToken, tenantId, invoiceId, file }) {
    const filename = normalizeOptionalString(file?.originalname) || `jentry-document-${Date.now()}.pdf`;
    const attachmentURL = `${XERO_INVOICES_URL}/${invoiceId}/Attachments/${encodeURIComponent(filename)}`;

    const response = await fetch(attachmentURL, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "xero-tenant-id": tenantId,
            Accept: "application/json",
            "Content-Type": file.mimetype || "application/octet-stream",
            "Content-Length": String(file.buffer.length),
            IncludeOnline: "true"
        },
        body: file.buffer
    });

    const { data, raw } = await readXeroResponse(response);
    const responseBody = data || raw || null;

    console.log("Xero attachment response received.", {
        xeroTransactionID: invoiceId,
        filename,
        xeroResponseStatus: response.status,
        xeroResponseBody: responseBody
    });

    if (!response.ok) {
        throw classifyXeroError(response.status, responseBody, "Xero attachment upload failed.");
    }

    const attachment = Array.isArray(data?.Attachments) ? data.Attachments[0] : data?.Attachments || data || null;

    return compactObject({
        fileName: filename,
        filename,
        xeroAttachmentId: normalizeOptionalString(
            attachment?.AttachmentID ||
            attachment?.AttachmentId ||
            attachment?.attachmentID ||
            attachment?.attachmentId
        ) || null,
        xeroFileName: normalizeOptionalString(attachment?.FileName || attachment?.filename || attachment?.fileName) || filename,
        contentType: normalizeOptionalString(file.mimetype) || "application/octet-stream",
        size: file.size || file.buffer.length,
        xeroResponseStatus: response.status,
        xeroResponseBody: responseBody
    });
}

async function addXeroInvoiceHistory({ accessToken, tenantId, invoiceId, historyDetail }) {
    const detail = normalizeOptionalString(historyDetail);
    if (!detail) return;

    const response = await fetch(XERO_INVOICE_HISTORY_URL(invoiceId), {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "xero-tenant-id": tenantId,
            Accept: "application/json",
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            HistoryRecords: [
                {
                    Details: detail
                }
            ]
        })
    });

    if (!response.ok) {
        const { data, raw } = await readXeroResponse(response);
        throw classifyXeroError(response.status, data || raw, "Xero history update failed.");
    }
}

async function attachDocumentsToXeroTransaction({
    accountId,
    xeroTransactionId,
    xeroInvoiceType = "",
    xeroTargetRecordType = "",
    documentFiles = []
}) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    const normalizedTransactionId = normalizeOptionalString(xeroTransactionId);

    if (!normalizedAccountId) {
        throw new XeroAPIError({ message: "Missing accountId.", code: "XERO_ATTACHMENT_BAD_REQUEST", status: 400 });
    }

    if (!normalizedTransactionId) {
        throw new XeroAPIError({ message: "Missing xeroTransactionId.", code: "XERO_ATTACHMENT_BAD_REQUEST", status: 400 });
    }

    const files = Array.isArray(documentFiles) ? documentFiles.filter((file) => file?.buffer?.length) : [];
    if (files.length === 0) {
        throw new XeroAPIError({ message: "Missing documents[].", code: "XERO_ATTACHMENT_BAD_REQUEST", status: 400 });
    }

    const connection = await ensureFreshXeroConnection(normalizedAccountId);
    if (!connection.selectedTenantId) {
        throw new XeroAPIError({ message: "No Xero organisation has been selected for this account.", code: "XERO_TENANT_NOT_SELECTED", status: 403, requiresReconnect: false });
    }
    if (!hasXeroScope(connection, "accounting.attachments")) {
        throw new XeroAPIError({
            message: "Reconnect Xero to grant attachment access. The current Xero connection does not include the accounting.attachments scope.",
            code: "XERO_ATTACHMENTS_SCOPE_REQUIRED",
            status: 401,
            requiresReconnect: true,
            upstreamBody: {
                missingScope: "accounting.attachments"
            }
        });
    }

    const attachedFiles = [];
    for (const file of files) {
        const attachedFile = await uploadAttachmentToXero({
            accessToken: connection.accessToken,
            tenantId: connection.selectedTenantId,
            invoiceId: normalizedTransactionId,
            file
        });
        attachedFiles.push(attachedFile);
    }

    return {
        success: true,
        ok: true,
        attached: true,
        xeroTransactionID: normalizedTransactionId,
        xeroTransactionId: normalizedTransactionId,
        xeroInvoiceID: normalizedTransactionId,
        xeroInvoiceId: normalizedTransactionId,
        invoiceID: normalizedTransactionId,
        invoiceId: normalizedTransactionId,
        billID: normalizedTransactionId,
        billId: normalizedTransactionId,
        xeroInvoiceType: xeroInvoiceType || null,
        xeroTargetRecordType: xeroTargetRecordType || null,
        attachmentCount: attachedFiles.length,
        attachedFiles,
        attachments: attachedFiles
    };
}

async function createXeroLedgerDocument(accountId, metadata, documentFiles) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    const submissionId = normalizeOptionalString(metadata.submissionId || metadata.submissionID);
    const documentHash = createPublishDocumentHash(documentFiles);

    if (submissionId) {
        const existingPublish = await getXeroPublishRecord({
            accountId: normalizedAccountId,
            submissionId
        });

        if (existingPublish?.invoice_id) {
            return {
                remoteSubmissionID: existingPublish.invoice_id,
                xeroTransactionID: existingPublish.invoice_id,
                invoiceId: existingPublish.invoice_id,
                confirmationMessage: existingPublish.warning || "This submission has already been published to Xero.",
                published: true,
                idempotent: true,
                attachmentsUploaded: existingPublish.attachments_uploaded !== false,
                warning: existingPublish.warning || null,
                invoice: existingPublish.xero_invoice || null
            };
        }

        if (existingPublish?.document_hash && existingPublish.document_hash !== documentHash) {
            throw new XeroAPIError({
                message: "A different document payload already exists for this submissionId.",
                code: "XERO_PUBLISH_IDEMPOTENCY_CONFLICT",
                status: 409,
                requiresReconnect: false
            });
        }

        if (existingPublish?.status === "pending") {
            throw new XeroAPIError({
                message: "This submission is already being published to Xero.",
                code: "XERO_PUBLISH_IN_PROGRESS",
                status: 409,
                requiresReconnect: false
            });
        }

        await createXeroPublishRecord({
            accountId: normalizedAccountId,
            submissionId,
            documentHash,
            status: "pending"
        });
    }

    const connection = await ensureFreshXeroConnection(normalizedAccountId);

    if (!connection.selectedTenantId) {
        throw new XeroAPIError({ message: "No Xero organisation has been selected for this account.", code: "XERO_TENANT_NOT_SELECTED", status: 403, requiresReconnect: false });
    }

    const metadataWithAccounts = {
        ...metadata,
        chartOfAccounts: connection.chartOfAccounts || [],
        // Provide access/tenant for historical coding lookups
        accessToken: connection.accessToken,
        tenantId: connection.selectedTenantId
    };

    const xeroPayload = await buildXeroDocumentPayload(metadataWithAccounts);
    const documentType = normalizeOptionalString(metadata.documentType) === "sales"
        ? "sales"
        : "purchase";

    const response = await fetch(XERO_INVOICES_URL, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${connection.accessToken}`,
            "xero-tenant-id": connection.selectedTenantId,
            Accept: "application/json",
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            Invoices: [xeroPayload]
        })
    });

    const { data, raw } = await readXeroResponse(response);

    if (!response.ok) {
        const error = classifyXeroError(response.status, data || raw, `Xero ${documentType} creation failed.`);
        if (submissionId) {
            await updateXeroPublishRecord({
                accountId: normalizedAccountId,
                submissionId,
                status: "failed",
                warning: error.message
            }).catch(() => undefined);
        }
        throw error;
    }

    const invoice = data?.Invoices?.[0];

    if (!invoice?.InvoiceID) {
        throw new XeroAPIError({
            message: "Xero did not return an invoice ID.",
            code: "XERO_INVALID_RESPONSE",
            status: 502,
            requiresReconnect: false,
            upstreamStatus: response.status,
            upstreamBody: data || raw
        });
    }

    let attachmentsUploaded = true;
    let warning = null;
    const historyDetail = buildXeroHistoryDetail(metadata);
    const canUploadAttachments = hasXeroScope(connection, "accounting.attachments");

    try {
        await addXeroInvoiceHistory({
            accessToken: connection.accessToken,
            tenantId: connection.selectedTenantId,
            invoiceId: invoice.InvoiceID,
            historyDetail
        });
    } catch (error) {
        warning = error instanceof Error
            ? `Published to Xero, but history update failed: ${error.message}`
            : "Published to Xero, but history update failed.";
        console.warn("Xero history update failed after invoice creation.", {
            invoiceId: invoice.InvoiceID,
            historyDetail,
            message: warning
        });
    }

    if (!canUploadAttachments) {
        attachmentsUploaded = false;
        const attachmentWarning = "Published to Xero, but attachment upload is blocked until Xero is reconnected with the accounting.attachments scope.";
        warning = warning ? `${warning} ${attachmentWarning}` : attachmentWarning;
    } else {
        for (const file of documentFiles.slice(0, 10)) {
            try {
                await uploadAttachmentToXero({
                    accessToken: connection.accessToken,
                    tenantId: connection.selectedTenantId,
                    invoiceId: invoice.InvoiceID,
                    file
                });
            } catch (error) {
                attachmentsUploaded = false;
                const attachmentWarning = error instanceof Error
                    ? `Published to Xero, but attachment upload failed: ${error.message}`
                    : "Published to Xero, but attachment upload failed.";
                warning = warning ? `${warning} ${attachmentWarning}` : attachmentWarning;
                console.warn("Xero attachment upload failed after invoice creation.", {
                    invoiceId: invoice.InvoiceID,
                    message: warning
                });
                break;
            }
        }
    }

    const result = {
        remoteSubmissionID: invoice.InvoiceID,
        xeroTransactionID: invoice.InvoiceID,
        invoiceId: invoice.InvoiceID,
        confirmationMessage: "Submission accepted and published to Xero.",
        published: true,
        idempotent: false,
        attachmentsUploaded,
        warning,
        invoice
    };

    if (submissionId) {
        await updateXeroPublishRecord({
            accountId: normalizedAccountId,
            submissionId,
            invoiceId: invoice.InvoiceID,
            status: attachmentsUploaded ? "published" : "published_with_attachment_warning",
            attachmentsUploaded,
            warning,
            xeroInvoice: invoice
        });
    }

    await markSubmissionAccepted({ accountId: normalizedAccountId });

    return result;
}

async function handleEmailUpload(metadata, documentFiles) {
    const to = normalizeEmail(metadata.deliveryTo);
    const from = normalizeEmail(metadata.preferredFromEmail) || getSenderEmail();
    const subject = String(metadata.deliverySubject || "Jentry submission");
    const body = String(metadata.deliveryBody || "");

    if (!to) {
        throw new Error("Missing deliveryTo in metadata.");
    }

    if (documentFiles.length === 0) {
        throw new Error("No documents were uploaded.");
    }

    const rawMessage = await buildRawMessage({
        from,
        to,
        subject,
        body,
        attachments: documentFiles.map((file) => ({
            filename: file.originalname,
            content: file.buffer,
            contentType: file.mimetype || "application/octet-stream"
        }))
    });

    const sendResult = await gmail.users.messages.send({
        userId: "me",
        requestBody: {
            raw: rawMessage
        }
    });

    await markSubmissionAccepted({ accountId: metadata.accountId || metadata.accountID });

    return {
        submissionId: metadata.submissionId || sendResult.data.id,
        message: `Submission emailed to ${to} from ${from}.`
    };
}

async function analyzeReceiptWithOpenAI({ buffer, mimeType, capturedAt, analysisContext = null }) {
    const { originalImageDataURL, enhancedImageDataURL } = await buildReceiptAnalysisImages({ buffer, mimeType });
    const codingInstructions = buildXeroCodingInstructions(analysisContext);

    const body = {
        model: optionalEnvironmentVariables.openAIModel,
        input: [
            {
                role: "system",
                content: [
                    {
                        type: "input_text",
                        text: [
                            "You extract fields from receipts and invoices.",
                            "Read the document image carefully and return only the structured JSON requested.",
                            "The user may photograph a receipt while it is lying on top of other papers, screens, or notes. Ignore any background text that is not part of the main receipt or invoice.",
                            "Prefer the merchant's trading name over street names, phone numbers, card brands, or generic receipt wording.",
                            "Prefer the final amount paid or grand total over line items, VAT lines, auth references, or subtotal lines.",
                            "You may receive both an original document image and a cleaned high-contrast enhancement of the same document. Use both together, but prefer the enhanced version",
                            "If a PDF is supplied, it has been converted to an image preview of its first page before analysis.",
                            "If a field is unclear, leave it null and set needsReview to true.",
                            "Produce a short summary in plain English such as 'Food and drink receipt for Via'.",
                            "Also return dedicated title fields for vendor and final amount. These title fields must be the best normalized vendor name and final paid total for naming the document.",
                            "The suggested title should be concise and usually follow the pattern 'Ã‚Â£11.58 Ã¢â‚¬â€œ McDonald's'. Do not use store numbers, cashier names, phone numbers, dates, or addresses",
                            "Also produce a longer helpful description for the detail screen, covering what the document appears to be, the merchant, the total, the date, and any notable payment",
                            codingInstructions.systemInstruction,
                            "Apply accounting judgement, not just literal item matching. Never classify food, drink, restaurants, cafes, takeaways, pubs, bars, or refreshments as Cost of Goods Sold unless the business context clearly shows the items were bought for resale or the client is a food/drink trading business. For ordinary service businesses, low-value food and drink receipts are usually subsistence, travel, staff welfare, refreshments, or entertainment depending on context. If friends, social dining, alcohol, guests, unclear attendees, or unclear business purpose are present, set needsReview true and cap coding confidence below 0.55.",
                            "Avoid lazy use of General Expenses. Use General Expenses only as a last-resort fallback when no more specific account in the chart applies. Mixed receipts must be coded line by line. Technology hardware and accessories such as AirPods, headphones, chargers, keyboards, mice, monitors, laptops, phones, tablets and computer accessories should normally use Computer Equipment or IT Software and Consumables depending on whether it is hardware or software. Tools, repairs, maintenance equipment, garden tools, cleaning equipment and premises upkeep should normally use Repairs & Maintenance unless the item is a capital asset. If the item appears personal or business purpose is unclear, flag review rather than forcing a confident code.",
                        ].join(" ")
                    }
                ]
            },
            {
                role: "user",
                content: [
                    {
                        type: "input_text",
                        text: [
                            "Extract the merchant, final paid total, currency, date, VAT amount if visible, invoice or receipt number if visible, payment method, category, and short summary",
                            "Return titleVendor as the best vendor name for naming the document, and titleAmountText as the best final paid amount text for naming the document.",
                            capturedAt ? `If the document date is unreadable, you may use this fallback capture timestamp: ${capturedAt}.` : "",
                            "Recognized text should contain the important visible text from the document.",
                            "The second image, if present, is a cleaned enhancement of the same receipt to improve extraction quality.",
                            codingInstructions.userInstruction
                        ].filter(Boolean).join(" ")
                    },
                    {
                        type: "input_image",
                        image_url: originalImageDataURL,
                        detail: "high"
                    },
                    {
                        type: "input_image",
                        image_url: enhancedImageDataURL,
                        detail: "high"
                    }
                ]
            }
        ],
        text: {
            format: {
                type: "json_schema",
                name: "receipt_extraction",
                strict: true,
                schema: {
                    type: "object",
                    additionalProperties: false,
                    properties: {
                        recognizedText: { type: "string" },
                        merchant: { type: ["string", "null"] },
                        totalAmount: { type: ["number", "null"] },
                        currency: { type: ["string", "null"] },
                        totalText: { type: ["string", "null"] },
                        titleVendor: { type: ["string", "null"] },
                        titleAmountText: { type: ["string", "null"] },
                        vatAmount: { type: ["number", "null"] },
                        vatText: { type: ["string", "null"] },
                        dateISO: { type: ["string", "null"] },
                        dateText: { type: ["string", "null"] },
                        invoiceNumber: { type: ["string", "null"] },
                        paymentMethod: { type: ["string", "null"] },
                        category: { type: ["string", "null"] },
                        selectedNominalCode: { type: ["string", "null"] },
                        selectedNominalCodeName: { type: ["string", "null"] },
                        selectedTaxType: { type: ["string", "null"] },
                        nominalCode: { type: ["string", "null"] },
                        accountCode: { type: ["string", "null"] },
                        taxType: { type: ["string", "null"] },
                        taxTreatment: { type: ["string", "null"] },
                        codingReasoning: { type: ["string", "null"] },
                        codingConfidence: { type: ["number", "null"] },
                        suggestedTitle: { type: "string" },
                        shortDescription: { type: "string" },
                        longDescription: { type: "string" },
                        lineItems: {
                            type: "array",
                            items: {
                                type: "object",
                                additionalProperties: false,
                                properties: {
                                    name: { type: "string" },
                                    quantity: { type: ["string", "null"] },
                                    amountText: { type: ["string", "null"] },
                                    nominalCode: { type: ["string", "null"] },
                                    nominalCodeName: { type: ["string", "null"] },
                                    taxType: { type: ["string", "null"] },
                                    taxTreatment: { type: ["string", "null"] },
                                    taxRateText: { type: ["string", "null"] },
                                    codingReasoning: { type: ["string", "null"] },
                                    codingConfidence: { type: ["number", "null"] },
                                    requiresReview: { type: "boolean" }
                                },
                                required: ["name", "quantity", "amountText", "nominalCode", "nominalCodeName", "taxType", "taxTreatment", "taxRateText", "codingReasoning", "codingConfidence", "requiresReview"]
                            }
                        },
                        extractedLines: {
                            type: "array",
                            items: { type: "string" }
                        },
                        summary: { type: "string" },
                        needsReview: { type: "boolean" },
                        extractionConfidence: { type: "number" }
                    },
                    required: [
                        "recognizedText",
                        "merchant",
                        "totalAmount",
                        "currency",
                        "totalText",
                        "titleVendor",
                        "titleAmountText",
                        "vatAmount",
                        "vatText",
                        "dateISO",
                        "dateText",
                        "invoiceNumber",
                        "paymentMethod",
                        "category",
                        "selectedNominalCode",
                        "selectedNominalCodeName",
                        "selectedTaxType",
                        "nominalCode",
                        "accountCode",
                        "taxType",
                        "taxTreatment",
                        "codingReasoning",
                        "codingConfidence",
                        "suggestedTitle",
                        "shortDescription",
                        "longDescription",
                        "lineItems",
                        "extractedLines",
                        "summary",
                        "needsReview",
                        "extractionConfidence"
                    ]
                }
            }
        }
    };

    const apiResponse = await fetch("https://api.openai.com/v1/responses", {
        method: "POST",
        headers: {
            Authorization: `Bearer ${optionalEnvironmentVariables.openAIAPIKey}`,
            "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
    });

    const rawResponse = await apiResponse.text();

    if (!apiResponse.ok) {
        throw new Error(`OpenAI extraction failed: ${rawResponse}`);
    }

    const parsedResponse = JSON.parse(rawResponse);
    const outputText = typeof parsedResponse.output_text === "string"
        ? parsedResponse.output_text
        : extractOutputText(parsedResponse);

    if (!outputText) {
        throw new Error("OpenAI extraction returned no JSON payload.");
    }

    const extraction = JSON.parse(outputText);

    const normalizedExtraction = {
        recognizedText: normalizeOptionalString(extraction.recognizedText),
        merchant: normalizeOptionalString(extraction.merchant) || null,
        totalAmount: typeof extraction.totalAmount === "number" ? extraction.totalAmount : null,
        currency: normalizeOptionalString(extraction.currency) || "GBP",
        totalText: normalizeOptionalString(extraction.totalText) || null,
        titleVendor: normalizeOptionalString(extraction.titleVendor) || null,
        titleAmountText: normalizeOptionalString(extraction.titleAmountText) || null,
        vatAmount: typeof extraction.vatAmount === "number" ? extraction.vatAmount : null,
        vatText: normalizeOptionalString(extraction.vatText) || null,
        dateISO: normalizeOptionalString(extraction.dateISO) || null,
        dateText: normalizeOptionalString(extraction.dateText) || null,
        invoiceNumber: normalizeOptionalString(extraction.invoiceNumber) || null,
        paymentMethod: normalizeOptionalString(extraction.paymentMethod) || null,
        category: normalizeOptionalString(extraction.category) || null,
        selectedNominalCode: normalizeOptionalString(extraction.selectedNominalCode) || normalizeOptionalString(extraction.nominalCode) || normalizeOptionalString(extraction.accountCode) || null,
        selectedNominalCodeName: normalizeOptionalString(extraction.selectedNominalCodeName) || null,
        selectedTaxType: normalizeOptionalString(extraction.selectedTaxType) || normalizeOptionalString(extraction.taxType) || normalizeOptionalString(extraction.taxTreatment) || null,
        nominalCode: normalizeOptionalString(extraction.nominalCode) || normalizeOptionalString(extraction.selectedNominalCode) || normalizeOptionalString(extraction.accountCode) || null,
        accountCode: normalizeOptionalString(extraction.accountCode) || normalizeOptionalString(extraction.selectedNominalCode) || normalizeOptionalString(extraction.nominalCode) || null,
        taxType: normalizeOptionalString(extraction.taxType) || normalizeOptionalString(extraction.selectedTaxType) || normalizeOptionalString(extraction.taxTreatment) || null,
        taxTreatment: normalizeOptionalString(extraction.taxTreatment) || normalizeOptionalString(extraction.selectedTaxType) || normalizeOptionalString(extraction.taxType) || null,
        codingReasoning: normalizeOptionalString(extraction.codingReasoning) || null,
        codingConfidence: typeof extraction.codingConfidence === "number" ? Math.max(0, Math.min(1, extraction.codingConfidence)) : null,
        suggestedTitle: normalizeOptionalString(extraction.suggestedTitle) || "Receipt",
        shortDescription: normalizeOptionalString(extraction.shortDescription) || "Receipt extracted and ready to review.",
        longDescription: normalizeOptionalString(extraction.longDescription) || "Receipt extracted and ready for review.",
        lineItems: Array.isArray(extraction.lineItems)
            ? extraction.lineItems
                .filter((item) => item && typeof item === "object")
                .map((item) => ({
                    name: normalizeOptionalString(item.name) || "Item",
                    quantity: normalizeOptionalString(item.quantity) || null,
                    amountText: normalizeOptionalString(item.amountText) || null,
                    nominalCode: normalizeOptionalString(item.nominalCode) || normalizeOptionalString(extraction.selectedNominalCode) || null,
                    nominalCodeName: normalizeOptionalString(item.nominalCodeName) || normalizeOptionalString(extraction.selectedNominalCodeName) || null,
                    taxType: normalizeOptionalString(item.taxType) || normalizeOptionalString(extraction.selectedTaxType) || normalizeOptionalString(extraction.taxType) || null,
                    taxTreatment: normalizeOptionalString(item.taxTreatment) || normalizeOptionalString(item.taxType) || normalizeOptionalString(extraction.selectedTaxType) || null,
                    taxRateText: normalizeOptionalString(item.taxRateText) || null,
                    codingReasoning: normalizeOptionalString(item.codingReasoning) || normalizeOptionalString(extraction.codingReasoning) || null,
                    codingConfidence: typeof item.codingConfidence === "number" ? Math.max(0, Math.min(1, item.codingConfidence)) : (typeof extraction.codingConfidence === "number" ? Math.max(0, Math.min(1, extraction.codingConfidence)) : null),
                    requiresReview: Boolean(item.requiresReview)
                }))
                .filter((item) => item.name && item.name.length > 1)
            : [],
        extractedLines: Array.isArray(extraction.extractedLines)
            ? extraction.extractedLines
                .filter((line) => typeof line === "string")
                .map((line) => normalizeOptionalString(line))
                .filter(Boolean)
            : [],
        summary: normalizeOptionalString(extraction.summary) || "Receipt ready for review.",
        needsReview: Boolean(extraction.needsReview),
        extractionConfidence: clampConfidence(extraction.extractionConfidence)
    };

    return applyAccountingSanityChecks(normalizedExtraction, analysisContext);
}



const JACCOUNTANCY_ACCOUNTING_RULES = [
    {
        code: "720",
        name: "Computer Equipment",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.9,
        keywords: [
            "airpods", "headphones", "earphones", "earbuds", "magsafe", "charger", "charging case",
            "keyboard", "mouse", "monitor", "screen", "laptop", "macbook", "imac", "computer",
            "pc", "tablet", "ipad", "iphone", "phone", "printer", "scanner", "router", "dock",
            "usb", "cable", "webcam", "microphone", "hard drive", "ssd", "memory card"
        ],
        reasoning: "Technology hardware or computer accessory matched to Computer Equipment rather than General Expenses."
    },
    {
        code: "463",
        name: "IT Software and Consumables",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.88,
        keywords: [
            "software", "subscription", "saas", "licence", "license", "hosting", "domain", "cloud",
            "openai", "chatgpt", "microsoft", "office 365", "google workspace", "adobe", "xero",
            "dext", "quickbooks", "sage", "canva", "notion", "slack", "zoom", "dropbox"
        ],
        reasoning: "Software, cloud service or IT consumable matched to IT Software and Consumables."
    },
    {
        code: "473",
        name: "Repairs & Maintenance",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.88,
        keywords: [
            "repair", "repairs", "maintenance", "service", "servicing", "parts", "replacement",
            "tool", "tools", "drill", "screwdriver", "ladder", "trimmer", "hedge trimmer",
            "garden tool", "mower", "cleaning equipment", "premises upkeep", "fixing", "sealant",
            "paint", "brush", "roller", "plumbing", "electrical repair"
        ],
        reasoning: "Tool, repair or maintenance-related item matched to Repairs & Maintenance."
    },
    {
        code: "408",
        name: "Cleaning",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.86,
        keywords: ["cleaning", "cleaner", "detergent", "bleach", "mop", "bucket", "janitorial", "sanitiser", "sanitizer"],
        reasoning: "Cleaning supply or service matched to Cleaning."
    },
    {
        code: "461",
        name: "Printing & Stationery",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.86,
        keywords: ["stationery", "paper", "notebook", "pen", "pencil", "toner", "ink cartridge", "labels", "envelopes", "printing"],
        reasoning: "Stationery or printing item matched to Printing & Stationery."
    },
    {
        code: "425",
        name: "Postage, Freight & Courier",
        taxType: "Exempt Expenses",
        confidence: 0.86,
        keywords: ["postage", "royal mail", "parcel", "courier", "dhl", "dpd", "evri", "fedex", "ups", "shipping", "freight"],
        reasoning: "Postage, freight or courier cost matched to Postage, Freight & Courier."
    },
    {
        code: "400",
        name: "Advertising & Marketing",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.86,
        keywords: ["advertising", "marketing", "facebook ads", "google ads", "linkedin ads", "leaflet", "flyer", "banner", "brand", "promotion"],
        reasoning: "Advertising or marketing cost matched to Advertising & Marketing."
    },
    {
        code: "401",
        name: "Audit & Accountancy fees",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.86,
        keywords: ["accountancy", "accountant", "audit", "bookkeeping", "tax return", "payroll service", "company accounts"],
        reasoning: "Accountancy, audit or bookkeeping service matched to Audit & Accountancy fees."
    },
    {
        code: "404",
        name: "Bank Fees",
        taxType: "No VAT",
        confidence: 0.9,
        keywords: ["bank fee", "transaction fee", "card processing", "stripe fee", "sumup fee", "paypal fee", "merchant fee"],
        reasoning: "Bank or merchant processing charge matched to Bank Fees."
    },
    {
        code: "412",
        name: "Consulting",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.82,
        keywords: ["consulting", "consultant", "advisor", "adviser", "professional advice", "strategy session"],
        reasoning: "Consulting or advisory service matched to Consulting."
    },
    {
        code: "433",
        name: "Insurance",
        taxType: "Exempt Expenses",
        confidence: 0.88,
        keywords: ["insurance", "policy", "premium", "public liability", "professional indemnity", "pii", "cover"],
        reasoning: "Insurance cost matched to Insurance."
    },
    {
        code: "441",
        name: "Legal Expenses",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.88,
        keywords: ["legal", "solicitor", "lawyer", "barrister", "counsel", "court fee", "legal advice"],
        reasoning: "Legal service or court cost matched to Legal Expenses."
    },
    {
        code: "445",
        name: "Light, Power, Heating",
        taxType: "5% (VAT on Expenses)",
        confidence: 0.85,
        keywords: ["electric", "electricity", "gas", "energy", "heating", "water bill", "utility", "utilities", "power"],
        reasoning: "Utility cost matched to Light, Power, Heating."
    },
    {
        code: "449",
        name: "Motor Vehicle Expenses",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.84,
        keywords: ["fuel", "petrol", "diesel", "shell", "bp", "esso", "texaco", "tyre", "car wash", "mot", "vehicle repair", "parking fine"],
        reasoning: "Vehicle running cost matched to Motor Vehicle Expenses."
    },
    {
        code: "457",
        name: "Operating Lease Payments",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.82,
        keywords: ["lease", "leasing", "rental agreement", "operating lease"],
        reasoning: "Lease or rental arrangement matched to Operating Lease Payments."
    },
    {
        code: "465",
        name: "Rates",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.84,
        keywords: ["business rates", "council rates", "local authority rates"],
        reasoning: "Business rates matched to Rates."
    },
    {
        code: "469",
        name: "Rent",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.84,
        keywords: ["office rent", "rent", "workspace", "serviced office", "coworking", "co-working"],
        reasoning: "Premises or workspace rent matched to Rent."
    },
    {
        code: "480",
        name: "Staff Training",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.86,
        keywords: ["training", "course", "seminar", "webinar", "qualification", "exam", "cpd", "workshop"],
        reasoning: "Training, course or CPD cost matched to Staff Training."
    },
    {
        code: "485",
        name: "Subscriptions",
        taxType: "Exempt Expenses",
        confidence: 0.84,
        keywords: ["membership", "subscription", "professional body", "aat", "icaew", "acca", "magazine", "journal"],
        reasoning: "Membership or professional subscription matched to Subscriptions."
    },
    {
        code: "489",
        name: "Telephone & Internet",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.86,
        keywords: ["telephone", "mobile bill", "phone bill", "internet", "broadband", "sim", "voip", "wifi", "wi-fi"],
        reasoning: "Telephone, mobile or internet cost matched to Telephone & Internet."
    },
    {
        code: "493",
        name: "Travel - National",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.84,
        keywords: ["train", "rail", "national rail", "taxi", "uber", "bolt", "bus", "tram", "parking", "hotel", "travelodge", "premier inn", "station", "airport transfer", "domestic travel"],
        reasoning: "Domestic travel or subsistence context matched to Travel - National."
    },
    {
        code: "494",
        name: "Travel - International",
        taxType: "No VAT",
        confidence: 0.84,
        keywords: ["flight", "airline", "international travel", "foreign hotel", "eurostar", "overseas"],
        reasoning: "International travel context matched to Travel - International."
    },
    {
        code: "710",
        name: "Office Equipment",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.84,
        keywords: ["desk", "chair", "office chair", "filing cabinet", "shredder", "whiteboard", "lamp", "office furniture"],
        reasoning: "Office equipment or furniture matched to Office Equipment."
    },
    {
        code: "420",
        name: "Entertainment-100% business",
        taxType: "20% (VAT on Expenses)",
        confidence: 0.55,
        keywords: ["client lunch", "client dinner", "business entertainment", "hospitality"],
        requiresReview: true,
        reasoning: "Possible business entertainment. Review deductibility and business purpose before posting."
    },
    {
        code: "424",
        name: "Entertainment - 0%",
        taxType: "No VAT",
        confidence: 0.5,
        keywords: ["friends", "friend", "social", "party", "alcohol", "beer", "wine", "cocktail"],
        requiresReview: true,
        reasoning: "Possible non-deductible entertainment or personal/social spend. Review before posting."
    }
];

function applyAccountingIntelligence(extraction, analysisContext = null) {
    const normalizedExtraction = extraction && typeof extraction === "object" ? extraction : {};
    const chartOfAccounts = normalizeChartOfAccountsForCoding(analysisContext?.chartOfAccounts);
    const businessContext = buildBusinessContextText(analysisContext);
    const businessProfile = classifyBusinessContext(analysisContext || {});
    const documentText = buildDocumentText(normalizedExtraction);
    const documentAmount = typeof normalizedExtraction.totalAmount === "number" ? normalizedExtraction.totalAmount : null;

    const foodContext = detectFoodDrinkContext(documentText);
    const resaleContext = detectResaleContext(documentText, businessContext, analysisContext);
    const socialContext = detectSocialOrEntertainmentContext(documentText);
    const travelContext = detectTravelContext(documentText);
    const generalCode = findAccountByCodeOrName(chartOfAccounts, "429", "General Expenses") || {
        code: "429",
        name: "General Expenses",
        taxType: "20% (VAT on Expenses)"
    };

    const originalDocumentCode = normalizeOptionalString(
        normalizedExtraction.selectedNominalCode ||
        normalizedExtraction.nominalCode ||
        normalizedExtraction.accountCode
    );
    const originalDocumentName = normalizeOptionalString(
        normalizedExtraction.selectedNominalCodeName ||
        normalizedExtraction.accountName ||
        normalizedExtraction.category
    );

    const originalDocumentLooksGeneral = isGeneralExpenseCode(originalDocumentCode, originalDocumentName);
    const originalDocumentLooksCOGS = isCostOfGoodsSoldCode(originalDocumentCode, originalDocumentName);

    if (foodContext.isFoodOrDrink && originalDocumentLooksCOGS && !resaleContext.allowed) {
        normalizedExtraction.needsReview = true;
        normalizedExtraction.extractionConfidence = capNumber(normalizedExtraction.extractionConfidence, socialContext.detected ? 0.55 : 0.6);
        normalizedExtraction.codingConfidence = capNumber(normalizedExtraction.codingConfidence ?? normalizedExtraction.extractionConfidence, socialContext.detected ? 0.5 : 0.55);
        normalizedExtraction.codingReasoning = [
            "Blocked Cost of Goods Sold for a food/refreshment receipt because there is no evidence the items were bought for resale.",
            socialContext.detected
                ? "Possible social/entertainment context detected; review deductibility and business purpose."
                : "For a normal service business this is more likely subsistence, staff welfare, refreshments, travel, or entertainment."
        ].join(" ");
        normalizedExtraction.selectedNominalCode = null;
        normalizedExtraction.selectedNominalCodeName = null;
        normalizedExtraction.nominalCode = null;
        normalizedExtraction.accountCode = null;
    }

    const originalLineItems = Array.isArray(normalizedExtraction.lineItems)
        ? normalizedExtraction.lineItems
        : [];

    const enhancedLineItems = originalLineItems.map((item) => {
        const lineText = buildLineItemText(item, normalizedExtraction);
        const amount = parseLineAmount(item?.amountText);
        const existingCode = normalizeOptionalString(item?.nominalCode || item?.accountCode);
        const existingName = normalizeOptionalString(item?.nominalCodeName || item?.accountName);
        const existingLooksSpecific = existingCode && !isGeneralExpenseCode(existingCode, existingName) && !isCostOfGoodsSoldCode(existingCode, existingName);

        let decision = decideAccountForLine({
            lineText,
            amount,
            documentText,
            businessContext,
            chartOfAccounts,
            existingCode,
            existingName,
            analysisContext
        });

        if (existingLooksSpecific && !decision.forceOverride) {
            const existingAccount = findAccountByCodeOrName(chartOfAccounts, existingCode, existingName) || {
                code: existingCode,
                name: existingName,
                taxType: normalizeOptionalString(item?.taxType || item?.taxTreatment)
            };
            decision = {
                ...decision,
                code: existingAccount.code,
                name: existingAccount.name,
                taxType: existingAccount.taxType || decision.taxType,
                confidence: Math.max(decision.confidence || 0, item.codingConfidence || 0.72),
                requiresReview: Boolean(item.requiresReview || decision.requiresReview),
                reasoning: normalizeOptionalString(item.codingReasoning) || decision.reasoning || "Retained specific AI-selected coding because it was not a generic fallback."
            };
        }

        if (isCostOfGoodsSoldCode(decision.code, decision.name) && !resaleContext.allowed) {
            decision = {
                code: null,
                name: null,
                taxType: normalizeOptionalString(item?.taxType || item?.taxTreatment) || null,
                confidence: 0.5,
                requiresReview: true,
                reasoning: "Blocked Cost of Goods Sold because resale/direct production context was not clear."
            };
        }

        if (!decision.code && (isGeneralExpenseCode(existingCode, existingName) || !existingCode)) {
            decision = {
                code: generalCode.code,
                name: generalCode.name,
                taxType: normalizeOptionalString(item?.taxType || item?.taxTreatment) || generalCode.taxType || null,
                confidence: 0.45,
                requiresReview: true,
                reasoning: "No stronger chart-of-accounts match was found. General Expenses used only as a review fallback."
            };
        }

        const taxDecision = chooseTaxTreatment({
            existingTaxType: item?.taxType,
            existingTaxTreatment: item?.taxTreatment,
            documentTaxType: normalizedExtraction.taxType || normalizedExtraction.selectedTaxType,
            documentTaxTreatment: normalizedExtraction.taxTreatment,
            accountTaxType: decision.taxType,
            document: normalizedExtraction
        });

        return {
            ...item,
            nominalCode: decision.code || null,
            nominalCodeName: decision.name || null,
            accountCode: decision.code || null,
            taxType: taxDecision.taxType,
            taxTreatment: taxDecision.taxTreatment,
            codingReasoning: decision.reasoning || normalizeOptionalString(item?.codingReasoning) || null,
            codingConfidence: clampConfidenceValue(decision.confidence ?? item?.codingConfidence ?? normalizedExtraction.codingConfidence ?? 0.6),
            requiresReview: Boolean(item?.requiresReview || decision.requiresReview)
        };
    });

    if (enhancedLineItems.length > 0) {
        normalizedExtraction.lineItems = enhancedLineItems;
        normalizedExtraction.lineItemsWithAICoding = enhancedLineItems;
    }

    const lineCodes = [...new Set(enhancedLineItems.map((item) => normalizeOptionalString(item.nominalCode)).filter(Boolean))];
    const reviewRequiredByLines = enhancedLineItems.some((item) => item.requiresReview || (item.codingConfidence ?? 0) < 0.6);

    if (lineCodes.length === 1) {
        const line = enhancedLineItems.find((item) => normalizeOptionalString(item.nominalCode) === lineCodes[0]);
        normalizedExtraction.selectedNominalCode = line.nominalCode;
        normalizedExtraction.selectedNominalCodeName = line.nominalCodeName;
        normalizedExtraction.nominalCode = line.nominalCode;
        normalizedExtraction.accountCode = line.nominalCode;
        normalizedExtraction.selectedTaxType = line.taxType || normalizedExtraction.selectedTaxType || null;
        normalizedExtraction.taxType = line.taxType || normalizedExtraction.taxType || null;
        normalizedExtraction.taxTreatment = line.taxTreatment || normalizedExtraction.taxTreatment || null;
        normalizedExtraction.codingConfidence = averageLineConfidence(enhancedLineItems);
        normalizedExtraction.codingReasoning = line.codingReasoning || normalizedExtraction.codingReasoning || null;
        normalizedExtraction.category = line.nominalCodeName || normalizedExtraction.category;
    } else if (lineCodes.length > 1) {
        normalizedExtraction.selectedNominalCode = null;
        normalizedExtraction.selectedNominalCodeName = "Mixed expense categories";
        normalizedExtraction.nominalCode = null;
        normalizedExtraction.accountCode = null;
        normalizedExtraction.category = "Mixed expenses";
        normalizedExtraction.codingConfidence = averageLineConfidence(enhancedLineItems);
        normalizedExtraction.codingReasoning = "Mixed receipt coded line-by-line rather than forcing the whole receipt into General Expenses.";
    } else if (originalDocumentLooksGeneral || !originalDocumentCode) {
        normalizedExtraction.selectedNominalCode = generalCode.code;
        normalizedExtraction.selectedNominalCodeName = generalCode.name;
        normalizedExtraction.nominalCode = generalCode.code;
        normalizedExtraction.accountCode = generalCode.code;
        normalizedExtraction.codingConfidence = Math.min(normalizedExtraction.codingConfidence ?? normalizedExtraction.extractionConfidence ?? 0.6, 0.55);
        normalizedExtraction.codingReasoning = normalizedExtraction.codingReasoning || "General Expenses is a fallback only because no more specific coding decision could be made.";
        normalizedExtraction.needsReview = true;
    }

    if (foodContext.isFoodOrDrink && documentAmount != null && documentAmount < 30 && !resaleContext.allowed) {
        normalizedExtraction.needsReview = true;
        normalizedExtraction.codingConfidence = capNumber(normalizedExtraction.codingConfidence, socialContext.detected ? 0.55 : 0.7);
        normalizedExtraction.extractionConfidence = capNumber(normalizedExtraction.extractionConfidence, socialContext.detected ? 0.6 : 0.75);
        if (!normalizedExtraction.codingReasoning) {
            normalizedExtraction.codingReasoning = socialContext.detected
                ? "Low-value food/drink receipt with possible social context. Review as subsistence, staff welfare, refreshments, or entertainment."
                : "Low-value food/drink receipt. Review business purpose before posting.";
        }
    }

    if (travelContext.detected && !foodContext.isFoodOrDrink && (!normalizedExtraction.category || normalizedExtraction.category === "General Expenses")) {
        normalizedExtraction.category = "Travel";
    }

    if (reviewRequiredByLines) {
        normalizedExtraction.needsReview = true;
    }

    const documentGuard = detectDangerousAccountDecision(
        normalizedExtraction.selectedNominalCode || normalizedExtraction.nominalCode || normalizedExtraction.accountCode,
        normalizedExtraction.selectedNominalCodeName || normalizedExtraction.category,
        documentText,
        analysisContext
    );

    if (documentGuard.dangerous) {
        normalizedExtraction.needsReview = true;
        normalizedExtraction.codingConfidence = Math.min(normalizedExtraction.codingConfidence ?? 0.5, 0.45);
        normalizedExtraction.codingReasoning = `${documentGuard.reason} Document-level coding blocked and flagged for review.`;
        normalizedExtraction.blockedAccountCode = normalizedExtraction.selectedNominalCode || normalizedExtraction.nominalCode || normalizedExtraction.accountCode || null;
        normalizedExtraction.blockedAccountName = normalizedExtraction.selectedNominalCodeName || normalizedExtraction.category || null;
        normalizedExtraction.selectedNominalCode = null;
        normalizedExtraction.selectedNominalCodeName = null;
        normalizedExtraction.nominalCode = null;
        normalizedExtraction.accountCode = null;
    }

    normalizedExtraction.bookkeepingContext = compactObject({
        natureOfBusiness: normalizeOptionalString(analysisContext?.natureOfBusiness || analysisContext?.businessNature || analysisContext?.businessType || analysisContext?.industry),
        businessProfile: compactObject({
            foodTrade: businessProfile.isFoodTrade,
            constructionOrMaintenanceTrade: businessProfile.isConstructionOrMaintenanceTrade,
            professionalService: businessProfile.isProfessionalService,
            retailOrResaleTrade: businessProfile.isRetailOrResaleTrade
        })
    });

    normalizedExtraction.codingExplanation = normalizedExtraction.codingReasoning || (reviewRequiredByLines
        ? "One or more line items require review because the coding confidence was below the auto-posting threshold."
        : "Coding selected using chart-of-accounts matching, business-context rules and receipt line-item analysis.");

    // Final auto-posting compatibility guard.
    // The mobile app requires a complete Xero coding payload every time.
    // We therefore avoid returning an incomplete extraction just because the AI was cautious.
    normalizedExtraction.lineItemsWithAICoding = buildCompleteAutoCodingLines({
        extraction: normalizedExtraction,
        chartOfAccounts,
        fallbackAccount: generalCode
    });
    normalizedExtraction.lineItems = normalizedExtraction.lineItemsWithAICoding;

    const completedLineCodes = [...new Set(
        normalizedExtraction.lineItemsWithAICoding
            .map((item) => normalizeOptionalString(item.nominalCode || item.accountCode))
            .filter(Boolean)
    )];

    if (completedLineCodes.length === 1) {
        const line = normalizedExtraction.lineItemsWithAICoding[0];
        normalizedExtraction.selectedNominalCode = line.nominalCode;
        normalizedExtraction.selectedNominalCodeName = line.nominalCodeName;
        normalizedExtraction.nominalCode = line.nominalCode;
        normalizedExtraction.accountCode = line.nominalCode;
        normalizedExtraction.category = line.nominalCodeName || normalizedExtraction.category;
    } else if (completedLineCodes.length > 1) {
        normalizedExtraction.selectedNominalCode = null;
        normalizedExtraction.selectedNominalCodeName = "Mixed expense categories";
        normalizedExtraction.nominalCode = null;
        normalizedExtraction.accountCode = null;
        normalizedExtraction.category = "Mixed expenses";
    }

    const firstCompletedLine = normalizedExtraction.lineItemsWithAICoding[0] || {};
    normalizedExtraction.taxType = normalizeOptionalString(normalizedExtraction.taxType)
        || normalizeOptionalString(normalizedExtraction.selectedTaxType)
        || normalizeOptionalString(firstCompletedLine.taxType)
        || "No VAT";
    normalizedExtraction.selectedTaxType = normalizeOptionalString(normalizedExtraction.selectedTaxType)
        || normalizedExtraction.taxType;
    normalizedExtraction.taxTreatment = normalizeOptionalString(normalizedExtraction.taxTreatment)
        || normalizeOptionalString(firstCompletedLine.taxTreatment)
        || normalizedExtraction.taxType
        || "No VAT";

    normalizedExtraction.codingConfidence = clampConfidenceValue(
        normalizedExtraction.codingConfidence ?? averageLineConfidence(normalizedExtraction.lineItemsWithAICoding) ?? normalizedExtraction.extractionConfidence
    );
    normalizedExtraction.extractionConfidence = clampConfidenceValue(normalizedExtraction.extractionConfidence);

    // Product requirement: extraction should proceed automatically. Low confidence is exposed in
    // codingConfidence/codingExplanation, but does not block posting.
    normalizedExtraction.needsReview = false;
    normalizedExtraction.lineItemsWithAICoding = normalizedExtraction.lineItemsWithAICoding.map((item) => ({
        ...item,
        requiresReview: false
    }));
    normalizedExtraction.lineItems = normalizedExtraction.lineItemsWithAICoding;

    return normalizedExtraction;
}

function applyAccountingSanityChecks(extraction, analysisContext = null) {
    return applyAccountingIntelligence(extraction, analysisContext);
}

function buildCompleteAutoCodingLines({ extraction, chartOfAccounts, fallbackAccount }) {
    const existingLines = Array.isArray(extraction.lineItemsWithAICoding) && extraction.lineItemsWithAICoding.length > 0
        ? extraction.lineItemsWithAICoding
        : (Array.isArray(extraction.lineItems) ? extraction.lineItems : []);

    const fallbackCode = normalizeOptionalString(extraction.nominalCode || extraction.accountCode || extraction.selectedNominalCode)
        || normalizeOptionalString(fallbackAccount?.code)
        || "429";
    const fallbackName = normalizeOptionalString(extraction.selectedNominalCodeName || extraction.category || fallbackAccount?.name)
        || "General Expenses";
    const fallbackMatchedAccount = findAccountByCodeOrName(chartOfAccounts, fallbackCode, fallbackName) || fallbackAccount || {
        code: fallbackCode,
        name: fallbackName,
        taxType: "No VAT"
    };
    const fallbackTax = normalizeOptionalString(extraction.taxTreatment || extraction.taxType || extraction.selectedTaxType || fallbackMatchedAccount.taxType)
        || inferFallbackTaxTreatment(extraction)
        || "No VAT";

    const sourceLines = existingLines.length > 0 ? existingLines : [
        {
            name: normalizeOptionalString(extraction.shortDescription)
                || normalizeOptionalString(extraction.summary)
                || normalizeOptionalString(extraction.merchant)
                || "Receipt",
            quantity: "1",
            amountText: normalizeOptionalString(extraction.totalText)
                || (typeof extraction.totalAmount === "number" ? String(extraction.totalAmount) : null),
            nominalCode: fallbackMatchedAccount.code,
            nominalCodeName: fallbackMatchedAccount.name,
            accountCode: fallbackMatchedAccount.code,
            taxType: fallbackTax,
            taxTreatment: fallbackTax,
            taxRateText: null,
            codingReasoning: normalizeOptionalString(extraction.codingReasoning)
                || "Automatic fallback line created because the document did not contain separable line items.",
            codingConfidence: Math.min(extraction.codingConfidence || extraction.extractionConfidence || 0.62, 0.72),
            requiresReview: false
        }
    ];

    return sourceLines.map((item) => {
        const code = normalizeOptionalString(item.nominalCode || item.accountCode || fallbackMatchedAccount.code) || "429";
        const name = normalizeOptionalString(item.nominalCodeName || item.accountName || fallbackMatchedAccount.name) || "General Expenses";
        const matchedAccount = findAccountByCodeOrName(chartOfAccounts, code, name) || { code, name, taxType: fallbackTax };
        const tax = normalizeOptionalString(item.taxTreatment || item.taxType || extraction.taxTreatment || extraction.taxType || matchedAccount.taxType)
            || inferFallbackTaxTreatment(extraction)
            || "No VAT";

        return {
            ...item,
            name: normalizeOptionalString(item.name) || normalizeOptionalString(extraction.merchant) || "Receipt line item",
            quantity: normalizeOptionalString(item.quantity) || "1",
            amountText: normalizeOptionalString(item.amountText)
                || normalizeOptionalString(extraction.totalText)
                || (typeof extraction.totalAmount === "number" ? String(extraction.totalAmount) : null),
            nominalCode: matchedAccount.code || code,
            nominalCodeName: matchedAccount.name || name,
            accountCode: matchedAccount.code || code,
            taxType: tax,
            taxTreatment: tax,
            taxRateText: normalizeOptionalString(item.taxRateText) || null,
            codingReasoning: normalizeOptionalString(item.codingReasoning)
                || normalizeOptionalString(extraction.codingReasoning)
                || "Automatically coded using chart-of-accounts rules and available document context.",
            codingConfidence: clampConfidenceValue(
                typeof item.codingConfidence === "number"
                    ? item.codingConfidence
                    : (extraction.codingConfidence ?? extraction.extractionConfidence ?? 0.65)
            ),
            requiresReview: false
        };
    });
}

function inferFallbackTaxTreatment(extraction) {
    const recognizedText = normalizeComparableWords(extraction?.recognizedText || "");
    const vatAmount = typeof extraction?.vatAmount === "number" ? extraction.vatAmount : null;

    if (vatAmount === 0 || recognizedText.includes("no vat") || recognizedText.includes("total vat 0") || recognizedText.includes("total vat Ã‚Â£0")) {
        return "No VAT";
    }

    if (vatAmount != null && vatAmount > 0) {
        return "20% (VAT on Expenses)";
    }

    return "No VAT";
}

function normalizeChartOfAccountsForCoding(chartOfAccounts) {
    const supplied = Array.isArray(chartOfAccounts) ? chartOfAccounts : [];
    const normalized = supplied
        .map((account) => ({
            code: normalizeOptionalString(account.code || account.Code || account["*Code"]),
            name: normalizeOptionalString(account.name || account.Name || account["*Name"]),
            type: normalizeOptionalString(account.type || account.Type || account["*Type"]) || null,
            taxType: normalizeOptionalString(account.taxType || account.TaxType || account["*Tax Code"]) || null,
            description: normalizeOptionalString(account.description || account.Description) || null
        }))
        .filter((account) => account.code && account.name);

    if (normalized.length > 0) return normalized;

    return [
        { code: "310", name: "Cost of Goods Sold", type: "Direct Costs", taxType: "20% (VAT on Expenses)" },
        { code: "400", name: "Advertising & Marketing", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "401", name: "Audit & Accountancy fees", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "404", name: "Bank Fees", type: "Overhead", taxType: "No VAT" },
        { code: "408", name: "Cleaning", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "412", name: "Consulting", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "420", name: "Entertainment-100% business", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "424", name: "Entertainment - 0%", type: "Overhead", taxType: "No VAT" },
        { code: "425", name: "Postage, Freight & Courier", type: "Overhead", taxType: "Exempt Expenses" },
        { code: "429", name: "General Expenses", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "433", name: "Insurance", type: "Overhead", taxType: "Exempt Expenses" },
        { code: "441", name: "Legal Expenses", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "445", name: "Light, Power, Heating", type: "Overhead", taxType: "5% (VAT on Expenses)" },
        { code: "449", name: "Motor Vehicle Expenses", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "457", name: "Operating Lease Payments", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "461", name: "Printing & Stationery", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "463", name: "IT Software and Consumables", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "465", name: "Rates", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "469", name: "Rent", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "473", name: "Repairs & Maintenance", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "480", name: "Staff Training", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "485", name: "Subscriptions", type: "Overhead", taxType: "Exempt Expenses" },
        { code: "489", name: "Telephone & Internet", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "493", name: "Travel - National", type: "Overhead", taxType: "20% (VAT on Expenses)" },
        { code: "494", name: "Travel - International", type: "Overhead", taxType: "No VAT" },
        { code: "710", name: "Office Equipment", type: "Fixed Asset", taxType: "20% (VAT on Expenses)" },
        { code: "720", name: "Computer Equipment", type: "Fixed Asset", taxType: "20% (VAT on Expenses)" },
        { code: "764", name: "Plant and Machinery", type: "Fixed Asset", taxType: "20% (VAT on Expenses)" }
    ];
}

function decideAccountForLine({ lineText, amount, documentText, businessContext, chartOfAccounts, existingCode, existingName, analysisContext = null }) {
    const text = normalizeComparableWords(`${lineText} ${documentText}`);
    const lineOnlyText = normalizeComparableWords(lineText);

    const foodContext = detectFoodDrinkContext(text);
    const resaleContext = detectResaleContext(text, businessContext, analysisContext);
    const socialContext = detectSocialOrEntertainmentContext(text);

    if (foodContext.isFoodOrDrink && !resaleContext.allowed) {
        const travelContext = detectTravelContext(text);
        if (socialContext.detected) {
            const entertainment = findAccountByCodeOrName(chartOfAccounts, "424", "Entertainment - 0%");
            return {
                code: entertainment?.code || null,
                name: entertainment?.name || null,
                taxType: entertainment?.taxType || null,
                confidence: 0.5,
                requiresReview: true,
                reasoning: "Food/drink with social or unclear attendee context. Review as entertainment or personal spend before posting.",
                forceOverride: isCostOfGoodsSoldCode(existingCode, existingName)
            };
        }

        if (travelContext.detected) {
            const travel = findAccountByCodeOrName(chartOfAccounts, "493", "Travel - National");
            return {
                code: travel?.code || null,
                name: travel?.name || null,
                taxType: travel?.taxType || null,
                confidence: 0.68,
                requiresReview: true,
                reasoning: "Food/drink appears travel or subsistence related. Review business purpose before posting.",
                forceOverride: isCostOfGoodsSoldCode(existingCode, existingName)
            };
        }

        return {
            code: null,
            name: null,
            taxType: null,
            confidence: 0.55,
            requiresReview: true,
            reasoning: "Food/refreshment item should not be posted to Cost of Goods Sold unless purchased for resale. Review whether it is subsistence, staff welfare, refreshments, entertainment, or personal.",
            forceOverride: isCostOfGoodsSoldCode(existingCode, existingName)
        };
    }

    const matchedRule = findBestAccountingRule(lineOnlyText || text);
    if (matchedRule) {
        const account = findAccountByCodeOrName(chartOfAccounts, matchedRule.code, matchedRule.name) || matchedRule;
        return applyDangerousAccountGuard({
            code: account.code,
            name: account.name,
            taxType: account.taxType || matchedRule.taxType || null,
            confidence: matchedRule.confidence,
            requiresReview: Boolean(matchedRule.requiresReview),
            reasoning: matchedRule.reasoning,
            forceOverride: isGeneralExpenseCode(existingCode, existingName) || isCostOfGoodsSoldCode(existingCode, existingName)
        }, { text: `${lineText} ${documentText}`, analysisContext, existingCode, existingName });
    }

    if (isCostOfGoodsSoldCode(existingCode, existingName) && !resaleContext.allowed) {
        return {
            code: null,
            name: null,
            taxType: null,
            confidence: 0.5,
            requiresReview: true,
            reasoning: "Cost of Goods Sold requires clear resale/direct production evidence. No such evidence was detected.",
            forceOverride: true
        };
    }

    if (resaleContext.allowed && isCostOfGoodsSoldCode(existingCode, existingName)) {
        const cogs = findAccountByCodeOrName(chartOfAccounts, "310", "Cost of Goods Sold");
        return {
            code: cogs?.code || existingCode,
            name: cogs?.name || existingName || "Cost of Goods Sold",
            taxType: cogs?.taxType || null,
            confidence: 0.82,
            requiresReview: false,
            reasoning: "Cost of Goods Sold retained because resale/direct production context was detected."
        };
    }

    return {
        code: null,
        name: null,
        taxType: null,
        confidence: null,
        requiresReview: false,
        reasoning: null,
        forceOverride: false
    };
}

function findBestAccountingRule(text) {
    const normalized = normalizeComparableWords(text);
    if (!normalized) return null;

    const matches = JACCOUNTANCY_ACCOUNTING_RULES
        .map((rule) => {
            const hitCount = rule.keywords.filter((keyword) => normalized.includes(normalizeComparableWords(keyword))).length;
            const longestHit = rule.keywords
                .filter((keyword) => normalized.includes(normalizeComparableWords(keyword)))
                .sort((lhs, rhs) => rhs.length - lhs.length)[0] || "";
            return { rule, hitCount, longestHitLength: longestHit.length };
        })
        .filter((entry) => entry.hitCount > 0)
        .sort((lhs, rhs) => rhs.hitCount - lhs.hitCount || rhs.longestHitLength - lhs.longestHitLength || rhs.rule.confidence - lhs.rule.confidence);

    return matches[0]?.rule || null;
}

function findAccountByCodeOrName(chartOfAccounts, code, name) {
    const normalizedCode = normalizeOptionalString(code);
    const normalizedName = normalizeComparableText(name);
    return chartOfAccounts.find((account) => account.code === normalizedCode)
        || chartOfAccounts.find((account) => normalizeComparableText(account.name) === normalizedName)
        || chartOfAccounts.find((account) => normalizedName && normalizeComparableText(account.name).includes(normalizedName))
        || null;
}

function buildBusinessContextText(analysisContext) {
    return [
        analysisContext?.natureOfBusiness,
        analysisContext?.businessNature,
        analysisContext?.businessType,
        analysisContext?.industry,
        analysisContext?.sector,
        analysisContext?.sicDescription,
        analysisContext?.companyName,
        analysisContext?.clientName,
        analysisContext?.businessDescription,
        analysisContext?.tradeDescription,
        analysisContext?.clientNotes
    ].map(normalizeOptionalString).join(" ").toLowerCase();
}

function classifyBusinessContext(analysisContext) {
    const context = normalizeComparableWords(buildBusinessContextText(analysisContext));

    const foodTrade = ["restaurant", "cafe", "catering", "takeaway", "hospitality", "pub", "bar", "food manufacturer", "bakery", "butcher", "greengrocer", "food retail"];
    const constructionTrade = ["builder", "construction", "joiner", "electrician", "plumber", "landscaping", "gardener", "property maintenance", "contractor", "tradesman", "tradesperson"];
    const professionalService = ["accountant", "accountancy", "bookkeeper", "consultant", "solicitor", "law firm", "marketing agency", "agency", "consultancy", "professional services"];
    const retailTrade = ["retail", "shop", "ecommerce", "e-commerce", "online store", "wholesale", "reseller", "resale"];

    return {
        raw: context,
        isFoodTrade: foodTrade.some((keyword) => context.includes(normalizeComparableWords(keyword))),
        isConstructionOrMaintenanceTrade: constructionTrade.some((keyword) => context.includes(normalizeComparableWords(keyword))),
        isProfessionalService: professionalService.some((keyword) => context.includes(normalizeComparableWords(keyword))),
        isRetailOrResaleTrade: retailTrade.some((keyword) => context.includes(normalizeComparableWords(keyword)))
    };
}

function buildDocumentText(extraction) {
    return [
        extraction?.merchant,
        extraction?.category,
        extraction?.summary,
        extraction?.shortDescription,
        extraction?.longDescription,
        extraction?.recognizedText,
        ...(Array.isArray(extraction?.extractedLines) ? extraction.extractedLines : []),
        ...(Array.isArray(extraction?.lineItems) ? extraction.lineItems.map((item) => buildLineItemText(item, extraction)) : [])
    ].map(normalizeOptionalString).join(" ").toLowerCase();
}

function buildLineItemText(item, extraction = {}) {
    return [
        item?.name,
        item?.description,
        item?.summary,
        item?.amountText,
        extraction?.merchant
    ].map(normalizeOptionalString).join(" ").toLowerCase();
}

function normalizeComparableWords(value) {
    return normalizeOptionalString(value)
        .toLowerCase()
        .replace(/&/g, " and ")
        .replace(/[^a-z0-9Ã‚Â£.\s-]/g, " ")
        .replace(/\s+/g, " ")
        .trim();
}

function detectFoodDrinkContext(text) {
    const normalized = normalizeComparableWords(text);
    const keywords = [
        "restaurant", "cafe", "cafÃƒÂ©", "coffee", "tea", "takeaway", "deliveroo", "ubereats",
        "just eat", "bar", "pub", "bistro", "grill", "kitchen", "food", "drink", "meal",
        "breakfast", "lunch", "dinner", "sandwich", "burger", "pizza", "chicken", "goujon",
        "goujons", "greggs", "mcdonald", "mcdonalds", "costa", "starbucks", "pret",
        "subway", "kfc", "nando", "via"
    ];
    return {
        isFoodOrDrink: keywords.some((keyword) => normalized.includes(normalizeComparableWords(keyword)))
    };
}

function detectSocialOrEntertainmentContext(text) {
    const normalized = normalizeComparableWords(text);
    const keywords = [
        "friend", "friends", "guest", "guests", "client lunch", "client dinner", "alcohol",
        "beer", "wine", "cocktail", "social", "party", "entertainment", "hospitality"
    ];
    return { detected: keywords.some((keyword) => normalized.includes(normalizeComparableWords(keyword))) };
}

function detectTravelContext(text) {
    const normalized = normalizeComparableWords(text);
    const keywords = [
        "train", "rail", "national rail", "taxi", "uber", "bolt", "bus", "tram", "parking",
        "hotel", "travelodge", "premier inn", "station", "airport", "journey", "travel",
        "motorway services", "service station"
    ];
    return { detected: keywords.some((keyword) => normalized.includes(normalizeComparableWords(keyword))) };
}

function detectResaleContext(text, businessContext, analysisContext = null) {
    const normalized = normalizeComparableWords(`${text} ${businessContext}`);
    const businessProfile = classifyBusinessContext(analysisContext || { natureOfBusiness: businessContext });
    const resaleKeywords = [
        "resale", "stock", "inventory", "wholesale", "goods for sale", "cost of sales",
        "raw materials", "production", "manufacturing", "for onward sale", "retail stock"
    ];

    return {
        allowed: resaleKeywords.some((keyword) => normalized.includes(normalizeComparableWords(keyword)))
            || businessProfile.isFoodTrade
            || businessProfile.isRetailOrResaleTrade,
        businessProfile
    };
}

function detectDangerousAccountDecision(code, name, text = "", analysisContext = null) {
    const normalized = `${normalizeOptionalString(code)} ${normalizeOptionalString(name)}`.toLowerCase();
    const evidence = normalizeComparableWords(text);
    const businessProfile = classifyBusinessContext(analysisContext || {});

    const dangerousRules = [
        { pattern: /(^|\D)(200|260|270)(\D|$)|revenue|sales|interest income/, reason: "Income accounts should not be selected for supplier bills or expense receipts." },
        { pattern: /(^|\D)(477|478|320)(\D|$)|salaries|wages|director|employee pay/, reason: "Payroll and wages accounts require payroll evidence, not a normal receipt." },
        { pattern: /(^|\D)(500|505|510|515|520|525|530)(\D|$)|fixed asset|asset|accumulated depreciation/, reason: "Fixed asset accounts require capitalisation judgement and should not be auto-posted without review." },
        { pattern: /(^|\D)(610|620|630|800|801|820|825|830|835|840|850|860|877)(\D|$)|vat|paye|nic|corporation tax|loan|retained earnings|dividend|suspense/, reason: "Control, tax, loan, equity and suspense accounts must not be selected automatically from receipt text." }
    ];

    for (const rule of dangerousRules) {
        if (rule.pattern.test(normalized)) {
            return { dangerous: true, reason: rule.reason };
        }
    }

    if (isCostOfGoodsSoldCode(code, name)) {
        const resaleEvidence = ["resale", "stock", "inventory", "wholesale", "raw material", "production", "manufacturing", "for onward sale", "cost of sales"]
            .some((keyword) => evidence.includes(normalizeComparableWords(keyword)));
        if (!resaleEvidence && !businessProfile.isFoodTrade && !businessProfile.isRetailOrResaleTrade) {
            return {
                dangerous: true,
                reason: "Cost of Goods Sold requires clear resale, stock, direct production, food-trade or retail context. None was detected."
            };
        }
    }

    return { dangerous: false, reason: null };
}

function applyDangerousAccountGuard(decision, { text = "", analysisContext = null, existingCode = "", existingName = "" } = {}) {
    const guard = detectDangerousAccountDecision(decision?.code, decision?.name, text, analysisContext);
    if (!guard.dangerous) return decision;

    return {
        ...decision,
        code: null,
        name: null,
        taxType: decision?.taxType || null,
        confidence: Math.min(decision?.confidence ?? 0.5, 0.45),
        requiresReview: true,
        forceOverride: true,
        blockedAccountCode: decision?.code || existingCode || null,
        blockedAccountName: decision?.name || existingName || null,
        reasoning: `${guard.reason} Blocked automatic posting and flagged for accountant review.`
    };
}

function isGeneralExpenseCode(code, name) {
    const normalized = `${normalizeOptionalString(code)} ${normalizeOptionalString(name)}`.toLowerCase();
    return /(^|\D)429(\D|$)/.test(normalized) || normalized.includes("general expenses");
}

function isCostOfGoodsSoldCode(code, name) {
    const normalized = `${normalizeOptionalString(code)} ${normalizeOptionalString(name)}`.toLowerCase();
    return /(^|\D)310(\D|$)/.test(normalized)
        || normalized.includes("cost of goods")
        || normalized.includes("cost of sales")
        || normalized.includes("cogs");
}

function parseLineAmount(value) {
    const parsed = Number(normalizeOptionalString(value).replace(/[^0-9.-]/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
}

function chooseTaxTreatment({
    existingTaxType,
    existingTaxTreatment,
    documentTaxType,
    documentTaxTreatment,
    accountTaxType,
    document
}) {
    const existing = normalizeOptionalString(existingTaxType || existingTaxTreatment || documentTaxType || documentTaxTreatment);
    const recognizedText = normalizeComparableWords(document?.recognizedText || "");
    const vatAmount = typeof document?.vatAmount === "number" ? document.vatAmount : null;

    if (existing) {
        return { taxType: existing, taxTreatment: existing };
    }

    if (vatAmount === 0 || recognizedText.includes("no vat") || recognizedText.includes("vat 0") || recognizedText.includes("total vat Ã‚Â£0")) {
        return { taxType: "No VAT", taxTreatment: "No VAT" };
    }

    if (accountTaxType) {
        return { taxType: accountTaxType, taxTreatment: accountTaxType };
    }

    return { taxType: null, taxTreatment: null };
}

function averageLineConfidence(lineItems) {
    const values = lineItems
        .map((item) => typeof item.codingConfidence === "number" ? item.codingConfidence : null)
        .filter((value) => value != null);
    if (values.length === 0) return null;
    return clampConfidenceValue(values.reduce((sum, value) => sum + value, 0) / values.length);
}

function clampConfidenceValue(value) {
    if (typeof value !== "number" || Number.isNaN(value)) return 0.6;
    return Math.max(0, Math.min(1, value));
}

function capNumber(value, maximum) {
    const normalized = typeof value === "number" && !Number.isNaN(value) ? value : maximum;
    return Math.min(normalized, maximum);
}

function buildXeroCodingInstructions(analysisContext) {
    const chartOfAccounts = Array.isArray(analysisContext?.chartOfAccounts)
        ? analysisContext.chartOfAccounts
        : [];

    if (chartOfAccounts.length === 0) {
        return {
            systemInstruction: "If Xero coding context is not provided, leave nominal-code and tax fields null unless the document explicitly states them.",
            userInstruction: "If no Xero chart of accounts is provided, return null for selectedNominalCode, selectedNominalCodeName, selectedTaxType, and leave line-level coding fields null unless clearly stated on the document."
        };
    }

    const allowedAccounts = chartOfAccounts
        .slice(0, 250)
        .map((account) => compactObject({
            code: normalizeOptionalString(account.code),
            name: normalizeOptionalString(account.name),
            type: normalizeOptionalString(account.type) || null,
            classType: normalizeOptionalString(account.classType) || null,
            taxType: normalizeOptionalString(account.taxType) || null
        }))
        .filter((account) => account.code && account.name);

    const accountListJSON = JSON.stringify(allowedAccounts);
    return {
        systemInstruction: [
            "When Xero chart-of-accounts context is supplied, choose the best matching nominal code from that list using merchant, line descriptions, invoice context, VAT evidence, and normal bookkeeping intent.",
            "Use the client's nature of business as decisive context: the same item may be COGS for a restaurant/retailer but an overhead, subsistence, staff welfare, repairs, equipment, or review item for a professional service business.",
            "Do not invent account codes. Prefer specific accounts over General Expenses. Never select dangerous control, tax, payroll, loan, equity, suspense, income, or fixed asset accounts unless the document gives clear evidence and review is flagged where judgement is required.",
            "Every coding decision must include concise accounting reasoning that explains why the account is appropriate and why obvious alternatives were rejected."
        ].join(" "),
        userInstruction: [
            "Use the supplied Xero chart of accounts and the client's nature of business to choose document-level and line-level coding.",
            "Return selectedNominalCode and selectedNominalCodeName using only one of the supplied accounts.",
            "Return selectedTaxType using the chosen account default tax type when appropriate, or another clearly better tax type if the document evidence supports it.",
            "For each line item, return nominalCode, nominalCodeName, taxType, taxRateText, codingReasoning, codingConfidence, and requiresReview.",
            "If a receipt contains mixed goods, classify each line separately instead of using one generic category for the whole receipt.",
            "Use General Expenses only as a last resort after considering all specific accounts.",
            "If a line item is too ambiguous, leave the coding fields null, set requiresReview to true, and explain why in codingReasoning.",
            "Block or review dangerous accounts: COGS without resale/direct production context; payroll/wages; VAT/PAYE/tax control accounts; loans; dividends/equity; suspense; income accounts; and fixed assets where capitalisation judgement is needed.",
            `Client business context: ${normalizeOptionalString(analysisContext?.natureOfBusiness || analysisContext?.businessNature || analysisContext?.businessType || analysisContext?.industry || analysisContext?.businessDescription) || "not provided"}.`,
            `Allowed Xero accounts: ${accountListJSON}`
        ].join(" ")
    };
}

async function buildReceiptAnalysisImages({ buffer, mimeType }) {
    const normalizedMimeType = normalizeOptionalString(mimeType) || "image/jpeg";
    const isPDF = normalizedMimeType === "application/pdf";

    try {
        const source = isPDF
            ? await sharp(buffer, { density: 180, failOn: "none" })
                .rotate()
                .resize({
                    width: 2200,
                    height: 2200,
                    fit: "inside",
                    withoutEnlargement: true
                })
                .jpeg({ quality: 94, mozjpeg: true })
                .toBuffer()
            : buffer;

        const originalImageDataURL = `data:image/jpeg;base64,${
            await sharp(source, { failOn: "none" })
                .rotate()
                .resize({
                    width: 2200,
                    height: 2200,
                    fit: "inside",
                    withoutEnlargement: true
                })
                .jpeg({ quality: 94, mozjpeg: true })
                .toBuffer()
                .then((output) => output.toString("base64"))
        }`;

        const enhancedBuffer = await sharp(source, { failOn: "none" })
            .rotate()
            .resize({
                width: 2200,
                height: 2200,
                fit: "inside",
                withoutEnlargement: true
            })
            .grayscale()
            .normalize()
            .sharpen({ sigma: 1.1, flat: 1.2, jagged: 2.2 })
            .jpeg({ quality: 94, mozjpeg: true })
            .toBuffer();

        return {
            originalImageDataURL,
            enhancedImageDataURL: `data:image/jpeg;base64,${enhancedBuffer.toString("base64")}`
        };
    } catch (error) {
        console.warn("Receipt enhancement failed. Falling back to original image only.", {
            message: error instanceof Error ? error.message : String(error)
        });

        if (normalizedMimeType.startsWith("image/")) {
            const originalImageDataURL = `data:${normalizedMimeType};base64,${buffer.toString("base64")}`;
            return {
                originalImageDataURL,
                enhancedImageDataURL: originalImageDataURL
            };
        }

        throw new Error("Unable to convert document into an image for analysis.");
    }
}

function extractOutputText(parsedResponse) {
    if (!Array.isArray(parsedResponse.output)) {
        return "";
    }

    for (const item of parsedResponse.output) {
        if (!Array.isArray(item.content)) {
            continue;
        }

        for (const contentItem of item.content) {
            if (
                (contentItem.type === "output_text" || contentItem.type === "text") &&
                typeof contentItem.text === "string"
            ) {
                return contentItem.text;
            }
        }
    }

    return "";
}

function clampConfidence(value) {
    if (typeof value !== "number" || Number.isNaN(value)) {
        return 0.6;
    }

    return Math.max(0, Math.min(1, value));
}

async function buildRawMessage({ from, to, subject, body, attachments }) {
    const MailComposerClass = MailComposer.MailComposer || MailComposer;

    const composer = new MailComposerClass({
        from,
        to,
        subject,
        text: body,
        attachments
    });

    const message = await new Promise((resolve, reject) => {
        composer.compile().build((error, builtMessage) => {
            if (error) {
                reject(error);
                return;
            }

            resolve(builtMessage);
        });
    });

    return Buffer.from(message)
        .toString("base64")
        .replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/=+$/g, "");
}
