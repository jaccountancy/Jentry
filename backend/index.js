const crypto = require("crypto");
const fs = require("fs/promises");
const path = require("path");

const express = require("express");
const cors = require("cors");
const multer = require("multer");
const { google } = require("googleapis");
const OpenAI = require("openai");

const app = express();

app.use(cors());
app.use(express.json({ limit: "25mb" }));
app.use(express.urlencoded({ extended: true, limit: "25mb" }));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 25 * 1024 * 1024
  }
});

const port = Number(process.env.PORT || 3000);

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

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

const xeroAuthSessions = new Map();
const dataDirectory = path.join(__dirname, "data");
const xeroConnectionsPath = path.join(dataDirectory, "xero-connections.json");

function toBase64Url(value) {
  const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value);
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function chunkBase64(value) {
  return value.replace(/(.{76})/g, "$1\r\n");
}

function safeJSONString(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function firstJSONObject(text) {
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) {
    throw new Error("OpenAI did not return a JSON object.");
  }
  return JSON.parse(text.slice(start, end + 1));
}

function normalizeCurrency(value) {
  if (typeof value !== "string" || !value.trim()) {
    return "GBP";
  }
  return value.trim().toUpperCase();
}

function normalizeLineItems(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((item) => ({
      name: String(item?.name || "").trim(),
      quantity: item?.quantity == null ? null : String(item.quantity).trim(),
      amountText: item?.amountText == null ? null : String(item.amountText).trim()
    }))
    .filter((item) => item.name.length > 0);
}

function normalizeStringArray(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((entry) => String(entry || "").trim())
    .filter(Boolean);
}

function coerceNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function coerceBoolean(value) {
  return Boolean(value);
}

function getSenderEmail() {
  return (
    process.env.GMAIL_SENDER_EMAIL ||
    process.env.GMAIL_SENDER ||
    process.env.JENTRY_BACKEND_SENDER_EMAIL ||
    "theteam@jaccountancy.co.uk"
  );
}

function xeroClientID() {
  return String(process.env.XERO_CLIENT_ID || "").trim();
}

function xeroRedirectURI() {
  return String(
    process.env.XERO_REDIRECT_URI ||
      "https://jentry-jentry.up.railway.app/oauth/xero/callback"
  ).trim();
}

function xeroAppRedirectURI() {
  return String(
    process.env.JENTRY_XERO_APP_REDIRECT_URI || "jentry://oauth/xero/callback"
  ).trim();
}

function xeroScopes() {
  return String(
    process.env.XERO_SCOPES ||
      "openid profile email offline_access accounting.transactions accounting.contacts"
  )
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .join(" ");
}

function xeroConfigured() {
  return xeroClientID().length > 0 && xeroRedirectURI().length > 0;
}

async function ensureDataDirectory() {
  await fs.mkdir(dataDirectory, { recursive: true });
}

async function readXeroConnections() {
  await ensureDataDirectory();
  try {
    const raw = await fs.readFile(xeroConnectionsPath, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && parsed.accounts && typeof parsed.accounts === "object") {
      return parsed;
    }
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.warn("Failed to read Xero connection store.", error);
    }
  }

  return { accounts: {} };
}

async function writeXeroConnections(store) {
  await ensureDataDirectory();
  await fs.writeFile(xeroConnectionsPath, JSON.stringify(store, null, 2));
}

async function getXeroConnection(accountId) {
  const normalizedAccountId = String(accountId || "").trim();
  if (!normalizedAccountId) {
    return null;
  }

  const store = await readXeroConnections();
  return store.accounts[normalizedAccountId] || null;
}

async function upsertXeroConnection(accountId, updater) {
  const normalizedAccountId = String(accountId || "").trim();
  if (!normalizedAccountId) {
    throw new Error("Missing accountId.");
  }

  const store = await readXeroConnections();
  const previous = store.accounts[normalizedAccountId] || null;
  const next = await updater(previous);
  store.accounts[normalizedAccountId] = next;
  await writeXeroConnections(store);
  return next;
}

async function deleteXeroConnection(accountId) {
  const normalizedAccountId = String(accountId || "").trim();
  const store = await readXeroConnections();
  delete store.accounts[normalizedAccountId];
  await writeXeroConnections(store);
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

function createPKCEPair() {
  const verifier = toBase64Url(crypto.randomBytes(32));
  const challenge = toBase64Url(crypto.createHash("sha256").update(verifier).digest());
  return { verifier, challenge };
}

async function sendEmail({ to, from, subject, body, attachments = [] }) {
  const boundary = `jentry-${Date.now()}-${Math.random().toString(16).slice(2)}`;

  const headers = [
    `From: ${from}`,
    `To: ${to}`,
    `Subject: ${subject}`,
    "MIME-Version: 1.0",
    `Content-Type: multipart/mixed; boundary="${boundary}"`
  ];

  const messageParts = [
    `--${boundary}`,
    'Content-Type: text/plain; charset="UTF-8"',
    "Content-Transfer-Encoding: 7bit",
    "",
    body
  ];

  for (const attachment of attachments) {
    messageParts.push(
      `--${boundary}`,
      `Content-Type: ${attachment.mimeType}; name="${attachment.filename}"`,
      `Content-Disposition: attachment; filename="${attachment.filename}"`,
      "Content-Transfer-Encoding: base64",
      "",
      chunkBase64(Buffer.from(attachment.data).toString("base64"))
    );
  }

  messageParts.push(`--${boundary}--`, "");

  const rawMessage = [...headers, "", ...messageParts].join("\r\n");
  const encodedMessage = toBase64Url(rawMessage);

  return gmail.users.messages.send({
    userId: "me",
    requestBody: {
      raw: encodedMessage
    }
  });
}

function buildAnalyzePrompt(capturedAt) {
  return `
You extract structured data from a single receipt or invoice image.

Return exactly one JSON object and no markdown.

Required keys:
recognizedText
merchant
totalAmount
currency
totalText
titleVendor
titleAmountText
vatAmount
vatText
dateISO
dateText
invoiceNumber
paymentMethod
category
suggestedTitle
shortDescription
longDescription
lineItems
extractedLines
summary
needsReview
extractionConfidence

Rules:
- Use null for unknown nullable values.
- Use [] for unknown arrays.
- totalAmount and vatAmount must be numbers or null.
- needsReview must be boolean.
- extractionConfidence must be a number between 0 and 1.
- lineItems must be [{ "name": string, "quantity": string|null, "amountText": string|null }].
- extractedLines must be an array of strings.
- recognizedText should contain the readable text from the receipt.
- suggestedTitle should look like: "£22.43 – Kebabish"
- currency should usually be "GBP" for UK receipts.
- If unclear, still return valid JSON and set needsReview to true.
- Do not include any extra keys.

capturedAt: ${capturedAt || "unknown"}
`.trim();
}

function shapeAnalyzeResponse(parsed) {
  const merchant = parsed.merchant == null ? null : String(parsed.merchant).trim() || null;
  const totalText = parsed.totalText == null ? null : String(parsed.totalText).trim() || null;
  const totalAmount = coerceNumber(parsed.totalAmount);
  const titleVendor = parsed.titleVendor == null ? merchant : String(parsed.titleVendor).trim() || merchant;
  const titleAmountText =
    parsed.titleAmountText == null
      ? totalText
      : String(parsed.titleAmountText).trim() || totalText;

  return {
    recognizedText: String(parsed.recognizedText || ""),
    merchant,
    totalAmount,
    currency: normalizeCurrency(parsed.currency),
    totalText,
    titleVendor,
    titleAmountText,
    vatAmount: coerceNumber(parsed.vatAmount),
    vatText: parsed.vatText == null ? null : String(parsed.vatText).trim() || null,
    dateISO: parsed.dateISO == null ? null : String(parsed.dateISO).trim() || null,
    dateText: parsed.dateText == null ? null : String(parsed.dateText).trim() || null,
    invoiceNumber: parsed.invoiceNumber == null ? null : String(parsed.invoiceNumber).trim() || null,
    paymentMethod: parsed.paymentMethod == null ? null : String(parsed.paymentMethod).trim() || null,
    category: parsed.category == null ? null : String(parsed.category).trim() || null,
    suggestedTitle: String(parsed.suggestedTitle || "Receipt"),
    shortDescription: String(parsed.shortDescription || ""),
    longDescription: String(parsed.longDescription || ""),
    lineItems: normalizeLineItems(parsed.lineItems),
    extractedLines: normalizeStringArray(parsed.extractedLines),
    summary: String(parsed.summary || ""),
    needsReview: coerceBoolean(parsed.needsReview),
    extractionConfidence:
      typeof parsed.extractionConfidence === "number" &&
      Number.isFinite(parsed.extractionConfidence)
        ? Math.max(0, Math.min(1, parsed.extractionConfidence))
        : 0.5
  };
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

function sanitizeTenants(connections) {
  if (!Array.isArray(connections)) {
    return [];
  }

  return connections
    .map((connection) => ({
      id: String(connection.id || connection.connectionId || "").trim(),
      tenantId: String(connection.tenantId || "").trim(),
      tenantName: String(connection.tenantName || "").trim(),
      tenantType: String(connection.tenantType || "").trim(),
      createdDateUtc: String(connection.createdDateUtc || "").trim(),
      updatedDateUtc: String(connection.updatedDateUtc || "").trim()
    }))
    .filter((tenant) => tenant.tenantId && tenant.tenantName);
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

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error_description || data.error || "Xero token exchange failed.");
  }

  return data;
}

async function fetchXeroTenants(accessToken) {
  const response = await fetch(XERO_CONNECTIONS_URL, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json"
    }
  });

  const data = await response.json().catch(() => []);
  if (!response.ok) {
    throw new Error("Failed to fetch Xero organisations.");
  }

  return sanitizeTenants(data);
}

function tokenExpiresSoon(expiresAt) {
  if (!expiresAt) {
    return true;
  }
  return new Date(expiresAt).getTime() - Date.now() < 120000;
}

async function ensureFreshXeroConnection(accountId) {
  const connection = await getXeroConnection(accountId);
  if (!connection) {
    throw new Error("No Xero connection found for this account.");
  }

  if (!tokenExpiresSoon(connection.expiresAt)) {
    return connection;
  }

  if (!connection.refreshToken) {
    throw new Error("Xero connection cannot be refreshed.");
  }

  const tokenData = await exchangeXeroToken({
    grant_type: "refresh_token",
    client_id: xeroClientID(),
    refresh_token: connection.refreshToken
  });

  const tenants = await fetchXeroTenants(tokenData.access_token);
  return upsertXeroConnection(accountId, (previous) => {
    const selectedTenantId = previous?.selectedTenantId || tenants[0]?.tenantId || null;
    const selectedTenant = tenants.find((tenant) => tenant.tenantId === selectedTenantId) || tenants[0] || null;
    return {
      accountId: String(accountId),
      accessToken: tokenData.access_token,
      refreshToken: tokenData.refresh_token,
      idToken: tokenData.id_token || null,
      scope: tokenData.scope || previous?.scope || xeroScopes(),
      expiresAt: new Date(Date.now() + Number(tokenData.expires_in || 1800) * 1000).toISOString(),
      connectedAt: previous?.connectedAt || new Date().toISOString(),
      lastRefreshedAt: new Date().toISOString(),
      tenants,
      selectedTenantId: selectedTenant?.tenantId || null,
      selectedTenantName: selectedTenant?.tenantName || null
    };
  });
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

function buildBillLineItems(metadata) {
  const extractedDocuments = Array.isArray(metadata.extractedDocuments)
    ? metadata.extractedDocuments
    : [];

  const lineItems = [];
  for (const document of extractedDocuments) {
    const documentLineItems = Array.isArray(document?.lineItems) ? document.lineItems : [];
    if (documentLineItems.length > 0) {
      for (const item of documentLineItems) {
        const rawAmount = String(item?.amountText || "").replace(/[^0-9.-]/g, "");
        const parsedAmount = Number(rawAmount);
        lineItems.push({
          Description: String(item?.name || "Receipt line item").slice(0, 4000),
          Quantity: Number(String(item?.quantity || "").replace(/[^0-9.-]/g, "")) || 1,
          UnitAmount: Number.isFinite(parsedAmount) && parsedAmount !== 0 ? parsedAmount : undefined
        });
      }
    } else {
      const description = String(
        document?.shortDescription ||
          document?.summary ||
          document?.merchant ||
          "Receipt"
      ).trim();
      const parsedTotal = Number(document?.totalAmount);
      lineItems.push({
        Description: description.slice(0, 4000),
        Quantity: 1,
        UnitAmount: Number.isFinite(parsedTotal) && parsedTotal > 0 ? parsedTotal : undefined
      });
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

function buildBillPayload(metadata) {
  const extractedDocuments = Array.isArray(metadata.extractedDocuments)
    ? metadata.extractedDocuments
    : [];
  const primaryDocument = extractedDocuments[0] || {};
  const supplierName =
    String(primaryDocument.merchant || "").trim() ||
    `${String(metadata.companyName || "Jentry").trim()} supplier`;

  const referenceBase =
    String(primaryDocument.invoiceNumber || "").trim() ||
    String(metadata.submissionId || "").trim() ||
    `jentry-${Date.now()}`;

  const lineItems = buildBillLineItems(metadata).map((item) => {
    const cleaned = {
      Description: item.Description,
      Quantity: item.Quantity || 1
    };
    if (item.UnitAmount != null) {
      cleaned.UnitAmount = item.UnitAmount;
    }
    return cleaned;
  });

  return {
    Type: "ACCPAY",
    Contact: {
      Name: supplierName.slice(0, 255)
    },
    DateString: pickDocumentDate(extractedDocuments),
    DueDateString: pickDocumentDate(extractedDocuments),
    Status: "AUTHORISED",
    LineAmountTypes: "Inclusive",
    Reference: `${String(metadata.clientId || "").trim()} ${referenceBase}`.trim().slice(0, 255),
    InvoiceNumber: referenceBase.slice(0, 255),
    LineItems: lineItems
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
      "IncludeOnline": "true"
    },
    body: file.buffer
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Xero attachment upload failed: ${text || response.status}`);
  }
}

async function createXeroBill(accountId, metadata, documentFiles) {
  const connection = await ensureFreshXeroConnection(accountId);
  if (!connection.selectedTenantId) {
    throw new Error("No Xero organisation has been selected for this account.");
  }

  const billPayload = buildBillPayload(metadata);
  const response = await fetch(XERO_INVOICES_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${connection.accessToken}`,
      "xero-tenant-id": connection.selectedTenantId,
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      Invoices: [billPayload]
    })
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const details =
      data?.Elements?.[0]?.ValidationErrors?.map((entry) => entry.Message).join("; ") ||
      data?.Message ||
      "Xero bill creation failed.";
    throw new Error(details);
  }

  const invoice = data?.Invoices?.[0];
  if (!invoice?.InvoiceID) {
    throw new Error("Xero did not return an invoice ID.");
  }

  for (const file of documentFiles.slice(0, 10)) {
    await uploadAttachmentToXero({
      accessToken: connection.accessToken,
      tenantId: connection.selectedTenantId,
      invoiceId: invoice.InvoiceID,
      file
    });
  }

  return {
    remoteSubmissionID: invoice.InvoiceID,
    confirmationMessage: `Submission published directly to Xero for ${connection.selectedTenantName || "the selected organisation"}.`,
    invoice
  };
}

async function handleEmailUpload(metadata, documentFiles) {
  const to = String(metadata.deliveryTo || "");
  const from = String(metadata.preferredFromEmail || getSenderEmail());
  const subject = String(metadata.deliverySubject || "Jentry submission");
  const body = String(metadata.deliveryBody || "");

  if (!to) {
    throw new Error("Missing deliveryTo in metadata.");
  }

  if (documentFiles.length === 0) {
    throw new Error("No documents were uploaded.");
  }

  const attachments = documentFiles.map((file) => ({
    filename: file.originalname,
    mimeType: file.mimetype || "application/octet-stream",
    data: file.buffer
  }));

  const sendResult = await sendEmail({
    to,
    from,
    subject,
    body,
    attachments
  });

  return {
    submissionId: metadata.submissionId || sendResult.data.id,
    message: `Submission emailed to ${to} from ${from}.`
  };
}

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

    const accountId = String(request.query.accountId || "").trim();
    if (!accountId) {
      response.status(400).json({ message: "Missing accountId." });
      return;
    }

    const returnUri = String(request.query.returnUri || xeroAppRedirectURI()).trim();
    const { verifier, challenge } = createPKCEPair();
    const state = crypto.randomUUID();

    xeroAuthSessions.set(state, {
      accountId,
      returnUri,
      verifier,
      createdAt: Date.now()
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
  const state = String(request.query.state || "").trim();
  const authSession = xeroAuthSessions.get(state);
  const fallbackReturnURL = authSession?.returnUri || xeroAppRedirectURI();

  try {
    if (!authSession) {
      response.redirect(
        buildReturnURL(fallbackReturnURL, {
          status: "error",
          message: "Missing or expired Xero authorization session."
        })
      );
      return;
    }

    xeroAuthSessions.delete(state);

    const authError = String(request.query.error || "").trim();
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

    const code = String(request.query.code || "").trim();
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
      code,
      redirect_uri: xeroRedirectURI(),
      code_verifier: authSession.verifier
    });

    const tenants = await fetchXeroTenants(tokenData.access_token);
    const selectedTenant = tenants[0] || null;

    await upsertXeroConnection(authSession.accountId, () => ({
      accountId: authSession.accountId,
      accessToken: tokenData.access_token,
      refreshToken: tokenData.refresh_token,
      idToken: tokenData.id_token || null,
      scope: tokenData.scope || xeroScopes(),
      expiresAt: new Date(Date.now() + Number(tokenData.expires_in || 1800) * 1000).toISOString(),
      connectedAt: new Date().toISOString(),
      lastRefreshedAt: new Date().toISOString(),
      tenants,
      selectedTenantId: selectedTenant?.tenantId || null,
      selectedTenantName: selectedTenant?.tenantName || null
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
    const accountId = String(request.query.accountId || "").trim();
    if (!accountId) {
      response.status(400).json({ message: "Missing accountId." });
      return;
    }

    const connection = await getXeroConnection(accountId);
    if (!connection) {
      response.json({
        isConnected: false,
        requiresReconnect: false,
        selectedTenantId: null,
        selectedTenantName: null,
        tenants: []
      });
      return;
    }

    const fresh = await ensureFreshXeroConnection(accountId);
    response.json({
      isConnected: true,
      requiresReconnect: false,
      selectedTenantId: fresh.selectedTenantId || null,
      selectedTenantName: fresh.selectedTenantName || null,
      tenants: fresh.tenants || [],
      connectedAt: fresh.connectedAt || null,
      expiresAt: fresh.expiresAt || null
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to fetch Xero status.";
    response.status(500).json({
      isConnected: false,
      requiresReconnect: true,
      message
    });
  }
});

app.get("/xero/tenants", async (request, response) => {
  try {
    const accountId = String(request.query.accountId || "").trim();
    if (!accountId) {
      response.status(400).json({ message: "Missing accountId." });
      return;
    }

    const connection = await ensureFreshXeroConnection(accountId);
    response.json({
      tenants: connection.tenants || [],
      selectedTenantId: connection.selectedTenantId || null,
      selectedTenantName: connection.selectedTenantName || null
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to load Xero organisations.";
    response.status(500).json({ message });
  }
});

app.post("/xero/select-tenant", async (request, response) => {
  try {
    const accountId = String(request.body?.accountId || "").trim();
    const tenantId = String(request.body?.tenantId || "").trim();

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

    const updated = await upsertXeroConnection(accountId, (previous) => ({
      ...previous,
      selectedTenantId: selectedTenant.tenantId,
      selectedTenantName: selectedTenant.tenantName
    }));

    response.json({
      isConnected: true,
      selectedTenantId: updated.selectedTenantId,
      selectedTenantName: updated.selectedTenantName
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to select Xero organisation.";
    response.status(500).json({ message });
  }
});

app.post("/xero/disconnect", async (request, response) => {
  try {
    const accountId = String(request.body?.accountId || "").trim();
    if (!accountId) {
      response.status(400).json({ message: "Missing accountId." });
      return;
    }

    await deleteXeroConnection(accountId);
    response.json({ disconnected: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to disconnect Xero.";
    response.status(500).json({ message });
  }
});

app.post("/xero/publish-bill", upload.any(), async (request, response) => {
  try {
    const metadataFile = Array.isArray(request.files)
      ? request.files.find((file) => file.fieldname === "metadata")
      : null;
    const metadataText = metadataFile?.buffer?.toString("utf8") || request.body?.metadata;
    const metadata = parseJSONField(metadataText, {});
    const accountId = String(
      request.body?.accountId ||
        metadata.accountId ||
        metadata.accountID ||
        ""
    ).trim();

    if (!accountId) {
      response.status(400).json({ message: "Missing accountId." });
      return;
    }

    const documentFiles = (Array.isArray(request.files) ? request.files : []).filter(
      (file) => file.fieldname === "documents[]"
    );
    const result = await createXeroBill(accountId, metadata, documentFiles);
    response.json({
      submissionId: metadata.submissionId || result.remoteSubmissionID,
      remoteSubmissionID: result.remoteSubmissionID,
      message: result.confirmationMessage,
      tenantName: (await getXeroConnection(accountId))?.selectedTenantName || null
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to publish bill to Xero.";
    response.status(500).json({ message });
  }
});

app.post(
  "/jentry/uploads",
  upload.any(),
  async (request, response) => {
    try {
      console.log("Upload request received.", {
        contentType: request.headers["content-type"] || "unknown"
      });

      const files = Array.isArray(request.files) ? request.files : [];
      const metadataFile = files.find((file) => file.fieldname === "metadata");
      const metadataText = metadataFile?.buffer?.toString("utf8") || request.body?.metadata;

      if (!metadataText) {
        console.error("Upload rejected: metadata missing.");
        response.status(400).json({ message: "Missing metadata upload." });
        return;
      }

      const metadata = JSON.parse(metadataText);
      const documentFiles = files.filter((file) => file.fieldname === "documents[]");
      const routingMethod = String(metadata.routingMethod || "xeroFilesEmail").trim();

      console.log("Upload parsed.", {
        submissionId: metadata.submissionId || "missing",
        routingMethod,
        documentCount: documentFiles.length
      });

      let result;
      if (routingMethod === "xeroIntegration") {
        const accountId = String(metadata.accountId || metadata.accountID || "").trim();
        if (!accountId) {
          throw new Error("Missing accountId for Xero-integrated submission.");
        }
        result = await createXeroBill(accountId, metadata, documentFiles);
        response.json({
          submissionId: metadata.submissionId || result.remoteSubmissionID,
          message: result.confirmationMessage,
          remoteSubmissionID: result.remoteSubmissionID
        });
        return;
      }

      result = await handleEmailUpload(metadata, documentFiles);
      console.log("Email sent successfully.", {
        submissionId: result.submissionId,
        routingMethod,
        documentCount: documentFiles.length
      });

      response.json(result);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown server error.";
      console.error("Upload processing failed.", {
        message,
        stack: error instanceof Error ? error.stack : undefined
      });
      response.status(500).json({ message });
    }
  }
);

app.post(
  "/analyze",
  upload.fields([{ name: "document", maxCount: 1 }]),
  async (request, response) => {
    try {
      console.log("Analyze request received.", {
        contentType: request.headers["content-type"] || "unknown"
      });

      const documentFile = request.files?.document?.[0];
      const capturedAt = String(request.body?.capturedAt || "");

      if (!documentFile) {
        response.status(400).json({ message: "Missing document upload." });
        return;
      }

      const mimeType = documentFile.mimetype || "image/jpeg";
      const base64Image = documentFile.buffer.toString("base64");
      const dataUrl = `data:${mimeType};base64,${base64Image}`;

      const aiResponse = await openai.responses.create({
        model: "gpt-4.1-mini",
        input: [
          {
            role: "user",
            content: [
              {
                type: "input_text",
                text: buildAnalyzePrompt(capturedAt)
              },
              {
                type: "input_image",
                image_url: dataUrl
              }
            ]
          }
        ]
      });

      const outputText = aiResponse.output_text || "";
      console.log("OpenAI raw analyze output preview.", {
        preview: outputText.slice(0, 500)
      });

      const parsed = firstJSONObject(outputText);
      const result = shapeAnalyzeResponse(parsed);

      console.log("Analyze response ready.", {
        merchant: result.merchant,
        totalAmount: result.totalAmount,
        suggestedTitle: result.suggestedTitle,
        lineItemCount: result.lineItems.length
      });

      response.json(result);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown analyze error.";
      console.error("Analyze failed.", {
        message,
        stack: error instanceof Error ? error.stack : undefined
      });
      response.status(500).json({ message });
    }
  }
);

app.post(
  "/problem-report",
  upload.any(),
  async (request, response) => {
    try {
      console.log("Problem report request received.");

      const files = Array.isArray(request.files) ? request.files : [];
      const metadataFile = files.find((file) => file.fieldname === "metadata");
      const metadataText = metadataFile?.buffer?.toString("utf8") || request.body?.metadata;

      if (!metadataText) {
        response.status(400).json({ message: "Missing metadata upload." });
        return;
      }

      const metadata = JSON.parse(metadataText);
      const attachments = files
        .filter((file) => file.fieldname === "attachments[]")
        .map((file) => ({
          filename: file.originalname,
          mimeType: file.mimetype || "application/octet-stream",
          data: file.buffer
        }));

      const to = String(metadata.to || "jay@jaccountancy.co.uk");
      const from = String(getSenderEmail());
      const subject = String(metadata.subject || "Jentry problem report");
      const body = String(metadata.body || "");

      const sendResult = await sendEmail({
        to,
        from,
        subject,
        body,
        attachments
      });

      console.log("Problem report email sent.", {
        gmailMessageId: sendResult.data.id,
        to,
        attachmentCount: attachments.length
      });

      response.json({
        reportID: sendResult.data.id || `report-${Date.now()}`,
        message: `Problem report emailed to ${to} from ${from}.`
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown problem-report error.";
      console.error("Problem report failed.", {
        message,
        stack: error instanceof Error ? error.stack : undefined
      });
      response.status(500).json({ message });
    }
  }
);

app.use((request, response) => {
  response.status(404).send(`Cannot ${request.method} ${request.path}`);
});

app.listen(port, () => {
  console.log(`Jentry backend listening on port ${port}`);
  console.log(
    safeJSONString({
      routes: [
        "GET /health",
        "GET /oauth/xero/start",
        "GET /oauth/xero/callback",
        "GET /xero/status",
        "GET /xero/tenants",
        "POST /xero/select-tenant",
        "POST /xero/disconnect",
        "POST /xero/publish-bill",
        "POST /jentry/uploads",
        "POST /analyze",
        "POST /problem-report"
      ]
    })
  );
});
