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
    "GMAIL_SENDER"
];

const optionalEnvironmentVariables = {
    openAIAPIKey: process.env.OPENAI_API_KEY?.trim() || "",
    openAIModel: process.env.OPENAI_MODEL?.trim() || "gpt-4.1-mini"
};

for (const key of requiredEnvironmentVariables) {
    if (!process.env[key] || !process.env[key].trim()) {
        throw new Error(`Missing required environment variable: ${key}`);
    }
}

const app = express();
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
const XERO_ACCOUNTS_URL = "https://api.xero.com/api.xro/2.0/Accounts";
const XERO_BANK_TRANSACTIONS_URL = "https://api.xero.com/api.xro/2.0/BankTransactions";
const XERO_CONTACTS_URL = "https://api.xero.com/api.xro/2.0/Contacts";


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

app.get("/health", (_request, response) => {
    console.log("Health check received.");
    response.json({ ok: true });
});

app.get("/oauth/xero/start", async (request, response) => {
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
    let authSession = null;
    let fallbackReturnURL = xeroAppRedirectURI();

    try {
        authSession = state ? await getXeroAuthSession(state) : null;
        fallbackReturnURL = authSession?.returnUri || xeroAppRedirectURI();
        if (!authSession) {
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
            response.redirect(
                buildReturnURL(authSession.returnUri, {
                    accountId: authSession.accountId,
                    status: "error",
                    message: authError
                })
            );
            return;
        }

        const code = normalizeOptionalString(request.query.code);
        if (!code) {
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

        response.redirect(
            buildReturnURL(authSession.returnUri, {
                accountId: authSession.accountId,
                status: "connected",
                tenantCount: tenants.length
            })
        );
    } catch (error) {
        const message = error instanceof Error ? error.message : "Xero connection failed.";
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

        const connection = await ensureFreshXeroConnection(accountId);

        response.json({
            isConnected: true,
            requiresReconnect: false,
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
            chartOfAccountsCount: Array.isArray(connection.chartOfAccounts) ? connection.chartOfAccounts.length : 0
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

        const documentFiles = (Array.isArray(request.files) ? request.files : []).filter(
            (file) => file.fieldname === "documents[]"
        );

        const result = await createXeroLedgerDocument(accountId, metadata, documentFiles);
        const connection = await getXeroConnection(accountId);

        response.json({
            submissionId: metadata.submissionId || result.remoteSubmissionID,
            remoteSubmissionID: result.remoteSubmissionID,
            invoiceId: result.invoiceId,
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

                const result = await createXeroLedgerDocument(accountId, metadata, documentFiles);
                response.json({
                    submissionId: metadata.submissionId || result.remoteSubmissionID,
                    message: result.confirmationMessage,
                    remoteSubmissionID: result.remoteSubmissionID,
                    invoiceId: result.invoiceId,
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
            const message = error instanceof Error ? error.message : "Unknown server error.";
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

app.post("/jentry/inbox/register", async (request, response) => {
    try {
        const accountId = normalizeOptionalString(request.body?.accountId);
        const clientId = normalizeOptionalString(request.body?.clientId);
        const companyName = normalizeOptionalString(request.body?.companyName);
        const inboxEmail = normalizeEmail(request.body?.inboxEmail);

        if (!accountId || !inboxEmail) {
            response.status(400).json({ message: "Missing accountId or inboxEmail." });
            return;
        }

        await ensureJentryAccountsTable();

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
});

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
        const contactName = normalizeOptionalString(req.body?.contactName);

        if (!accountId || !contactName) {
            res.status(400).json({ message: "Missing accountId or contactName." });
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

app.use((request, response) => {
    response.status(404).send(`Cannot ${request.method} ${request.path}`);
});

app.listen(port, () => {
    console.log(`Jentry relay listening on port ${port}`);
    console.log(JSON.stringify({
        routes: [
            "GET /health",
            "GET /oauth/xero/start",
            "GET /oauth/xero/callback",
            "GET /xero/status",
            "GET /xero/tenants",
            "POST /xero/select-tenant",
            "POST /xero/disconnect",
            "GET /xero/accounts",
            "POST /xero/match-transactions",
            "POST /xero/publish-bill",
            "POST /jentry/uploads",
            "POST /jentry/inbox/register",
            "GET /inbound-email-submissions",
            "POST /analyze",
            "POST /jentry/analyze",
            "POST /problem-report",
            "POST /jentry/problem-report",
            "POST /inbound/postmark",
            "GET /xero/contacts",
            "POST /xero/ensure-contact"
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

        console.log("Receipt analysis completed.", {
            merchant: extraction.merchant || "unknown",
            totalText: extraction.totalText || "unknown",
            needsReview: extraction.needsReview
        });

        response.json(extraction);
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

async function ensureJentryAccountsTable() {
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
        || "openid profile email offline_access accounting.transactions accounting.contacts accounting.settings";

    return rawScopes
        .split(/\s+/)
        .filter(Boolean)
        .join(" ");
}

function xeroConfigured() {
    return Boolean(xeroClientID() && xeroClientSecret() && xeroRedirectURI());
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
    await pool.query(`
        CREATE TABLE IF NOT EXISTS xero_connections (
            account_id text PRIMARY KEY,
            payload jsonb NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    `);
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

    await pool.query(
        `
        INSERT INTO xero_connections (account_id, payload, updated_at)
        VALUES ($1, $2::jsonb, now())
        ON CONFLICT (account_id)
        DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()
        `,
        [normalizedAccountId, JSON.stringify(next)]
    );

    return next;
}

async function deleteXeroConnection(accountId) {
    const normalizedAccountId = normalizeOptionalString(accountId);
    if (!normalizedAccountId) return;

    await ensureXeroConnectionsTable();

    await pool.query(
        `DELETE FROM xero_connections WHERE account_id = $1`,
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

    return fragments.join(" • ");
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
    const attachmentURL = `${XERO_INVOICES_URL}/${invoiceId}/Attachments/${encodeURIComponent(file.originalname)}`;

    const response = await fetch(attachmentURL, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "xero-tenant-id": tenantId,
            Accept: "application/json",
            "Content-Type": file.mimetype || "application/pdf",
            "Content-Length": String(file.buffer.length),
            IncludeOnline: "true"
        },
        body: file.buffer
    });

    if (!response.ok) {
        const { data, raw } = await readXeroResponse(response);
        throw classifyXeroError(response.status, data || raw, "Xero attachment upload failed.");
    }
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
            warning = error instanceof Error
                ? `Published to Xero, but attachment upload failed: ${error.message}`
                : "Published to Xero, but attachment upload failed.";
            console.warn("Xero attachment upload failed after invoice creation.", {
                invoiceId: invoice.InvoiceID,
                message: warning
            });
            break;
        }
    }

    const result = {
        remoteSubmissionID: invoice.InvoiceID,
        invoiceId: invoice.InvoiceID,
        confirmationMessage: `${documentType === "sales" ? "Sales invoice" : "Purchase bill"} published directly to Xero for ${connection.selectedTenantName || "the selected organisation"}.`,
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
                            "The suggested title should be concise and usually follow the pattern '£11.58 – McDonald's'. Do not use store numbers, cashier names, phone numbers, dates, or addresses", 
                            "Also produce a longer helpful description for the detail screen, covering what the document appears to be, the merchant, the total, the date, and any notable payment",
                            codingInstructions.systemInstruction
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
                        suggestedTitle: { type: "string" },
                        shortDescription: { type: "string" },
                        longDescription: { type: "string" },
                        selectedNominalCode: { type: ["string", "null"] },
                        selectedNominalCodeName: { type: ["string", "null"] },
                        selectedTaxType: { type: ["string", "null"] },
                        codingReasoning: { type: ["string", "null"] },
                        codingConfidence: { type: "number" },
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
                                    taxRateText: { type: ["string", "null"] },
                                    codingReasoning: { type: ["string", "null"] },
                                    codingConfidence: { type: "number" },
                                    requiresReview: { type: "boolean" }
                                },
                                required: ["name", "quantity", "amountText", "nominalCode", "nominalCodeName", "taxType", "taxRateText", "codingReasoning", "codingConfidence", "requiresReview"]
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
                        "suggestedTitle",
                        "shortDescription",
                        "longDescription",
                        "selectedNominalCode",
                        "selectedNominalCodeName",
                        "selectedTaxType",
                        "codingReasoning",
                        "codingConfidence",
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

    return {
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
        suggestedTitle: normalizeOptionalString(extraction.suggestedTitle) || "Receipt",
        shortDescription: normalizeOptionalString(extraction.shortDescription) || "Receipt extracted and ready to review.",
        longDescription: normalizeOptionalString(extraction.longDescription) || "Receipt extracted and ready for review.",
        nominalCode: normalizeOptionalString(extraction.selectedNominalCode) || null,
        accountCode: normalizeOptionalString(extraction.selectedNominalCode) || null,
        nominalCodeName: normalizeOptionalString(extraction.selectedNominalCodeName) || null,
        taxType: normalizeOptionalString(extraction.selectedTaxType) || null,
        selectedNominalCode: normalizeOptionalString(extraction.selectedNominalCode) || null,
        selectedNominalCodeName: normalizeOptionalString(extraction.selectedNominalCodeName) || null,
        selectedTaxType: normalizeOptionalString(extraction.selectedTaxType) || null,
        codingReasoning: normalizeOptionalString(extraction.codingReasoning) || null,
        codingConfidence: clampConfidence(extraction.codingConfidence),
        lineItems: Array.isArray(extraction.lineItems)
            ? extraction.lineItems
                .filter((item) => item && typeof item === "object")
                .map((item) => ({
                    name: normalizeOptionalString(item.name) || "Item",
                    quantity: normalizeOptionalString(item.quantity) || null,
                    amountText: normalizeOptionalString(item.amountText) || null,
                    nominalCode: normalizeOptionalString(item.nominalCode) || null,
                    nominalCodeName: normalizeOptionalString(item.nominalCodeName) || null,
                    taxType: normalizeOptionalString(item.taxType) || null,
                    taxRateText: normalizeOptionalString(item.taxRateText) || null,
                    codingReasoning: normalizeOptionalString(item.codingReasoning) || null,
                    codingConfidence: clampConfidence(item.codingConfidence),
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
        systemInstruction: "When Xero chart-of-accounts context is supplied, choose the best matching nominal code from that list using the merchant, line descriptions, invoice context, and normal bookkeeping intent. Do not invent codes. Prefer the most specific valid expense account and carry the account default tax type where appropriate.",
        userInstruction: [
            "Use the supplied Xero chart of accounts to choose document-level and line-level coding.",
            "Return selectedNominalCode and selectedNominalCodeName using only one of the supplied accounts.",
            "Return selectedTaxType using the chosen account default tax type when appropriate, or another clearly better tax type if the document evidence supports it.",
            "For each line item, return nominalCode, nominalCodeName, taxType, taxRateText, codingReasoning, codingConfidence, and requiresReview.",
            "If a line item is too ambiguous, leave the coding fields null, set requiresReview to true, and explain why in codingReasoning.",
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
