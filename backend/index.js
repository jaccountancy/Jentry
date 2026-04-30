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
  limits: { fileSize: 25 * 1024 * 1024 }
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
  return buffer.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function xeroClientID() {
  return String(process.env.XERO_CLIENT_ID || "").trim();
}

// ✅ ADDED
function xeroClientSecret() {
  return String(process.env.XERO_CLIENT_SECRET || "").trim();
}

function xeroRedirectURI() {
  return String(
    process.env.XERO_REDIRECT_URI ||
      "https://jentry-jentry.up.railway.app/oauth/xero/callback"
  ).trim();
}

// ✅ UPDATED
function xeroConfigured() {
  return (
    xeroClientID().length > 0 &&
    xeroClientSecret().length > 0 &&
    xeroRedirectURI().length > 0
  );
}

function xeroScopes() {
  return "openid profile email offline_access accounting.transactions accounting.contacts";
}

function createPKCEPair() {
  const verifier = toBase64Url(crypto.randomBytes(32));
  const challenge = toBase64Url(
    crypto.createHash("sha256").update(verifier).digest()
  );
  return { verifier, challenge };
}

async function exchangeXeroToken(params) {
  const body = new URLSearchParams(params);
  const response = await fetch(XERO_TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error_description || "Xero token exchange failed.");
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

  const data = await response.json();
  if (!response.ok) throw new Error("Failed to fetch tenants");
  return data;
}

function tokenExpiresSoon(expiresAt) {
  return new Date(expiresAt).getTime() - Date.now() < 120000;
}

async function ensureFreshXeroConnection(accountId) {
  const store = JSON.parse(await fs.readFile(xeroConnectionsPath, "utf8"));
  const connection = store.accounts[accountId];

  if (!tokenExpiresSoon(connection.expiresAt)) return connection;

  // ✅ UPDATED (client_secret added)
  const tokenData = await exchangeXeroToken({
    grant_type: "refresh_token",
    client_id: xeroClientID(),
    client_secret: xeroClientSecret(),
    refresh_token: connection.refreshToken
  });

  const tenants = await fetchXeroTenants(tokenData.access_token);

  const updated = {
    ...connection,
    accessToken: tokenData.access_token,
    refreshToken: tokenData.refresh_token,
    expiresAt: new Date(Date.now() + tokenData.expires_in * 1000).toISOString(),
    tenants
  };

  store.accounts[accountId] = updated;
  await fs.writeFile(xeroConnectionsPath, JSON.stringify(store, null, 2));

  return updated;
}

app.get("/oauth/xero/start", (req, res) => {
  if (!xeroConfigured()) {
    return res.status(500).json({ message: "Xero not configured" });
  }

  const { verifier, challenge } = createPKCEPair();
  const state = crypto.randomUUID();

  xeroAuthSessions.set(state, { verifier });

  const url = new URL(XERO_AUTHORIZE_URL);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", xeroClientID());
  url.searchParams.set("redirect_uri", xeroRedirectURI());
  url.searchParams.set("scope", xeroScopes());
  url.searchParams.set("state", state);
  url.searchParams.set("code_challenge", challenge);
  url.searchParams.set("code_challenge_method", "S256");

  res.redirect(url.toString());
});

app.get("/oauth/xero/callback", async (req, res) => {
  const state = req.query.state;
  const session = xeroAuthSessions.get(state);

  try {
    const code = req.query.code;

    // ✅ UPDATED (client_secret added)
    const tokenData = await exchangeXeroToken({
      grant_type: "authorization_code",
      client_id: xeroClientID(),
      client_secret: xeroClientSecret(),
      code,
      redirect_uri: xeroRedirectURI(),
      code_verifier: session.verifier
    });

    const tenants = await fetchXeroTenants(tokenData.access_token);

    const accountId = "default";

    await fs.writeFile(
      xeroConnectionsPath,
      JSON.stringify(
        {
          accounts: {
            [accountId]: {
              accessToken: tokenData.access_token,
              refreshToken: tokenData.refresh_token,
              expiresAt: new Date(
                Date.now() + tokenData.expires_in * 1000
              ).toISOString(),
              tenants
            }
          }
        },
        null,
        2
      )
    );

    res.send("Xero connected successfully");
  } catch (err) {
    res.status(500).send(err.message);
  }
});

app.listen(port, () => {
  console.log(`Server running on ${port}`);
});
