# Jentry Relay

This service handles:

- `POST /jentry/uploads` for the existing Gmail/Xero Files email flow
- `POST /analyze` for AI extraction
- `POST /problem-report` for support and idea submissions
- Xero OAuth and direct bill publishing for Advanced Jentry Mode

## Setup

1. Install Node.js 20 or later.
2. In `backend/`, run `npm install`.
3. Copy `.env.example` to `.env`.
4. Fill in these values in `.env` on your machine only:
   - `GOOGLE_CLIENT_ID`
   - `GOOGLE_CLIENT_SECRET`
   - `GOOGLE_REFRESH_TOKEN`
   - `GMAIL_SENDER`
   - `OPENAI_API_KEY`
   - `XERO_CLIENT_ID`
   - `XERO_REDIRECT_URI`
   - `JENTRY_XERO_APP_REDIRECT_URI`
5. Start the relay with `npm start`.

The server exposes:
- `GET /health`
- `POST /jentry/uploads`
- `POST /analyze`
- `POST /problem-report`
- `GET /oauth/xero/start`
- `GET /oauth/xero/callback`
- `GET /xero/status`
- `GET /xero/tenants`
- `POST /xero/select-tenant`
- `POST /xero/disconnect`
- `POST /xero/publish-bill`

## Railway environment

Set these environment variables in Railway:

- `PORT`
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REFRESH_TOKEN`
- `GMAIL_SENDER`
- `OPENAI_API_KEY`
- `XERO_CLIENT_ID=1C221C657B814E629933DE4A28D0E808`
- `XERO_REDIRECT_URI=https://jentry-jentry.up.railway.app/oauth/xero/callback`
- `JENTRY_XERO_APP_REDIRECT_URI=jentry://oauth/xero/callback`
- `XERO_SCOPES=openid profile email offline_access accounting.transactions accounting.contacts`

The Xero app redirect URI must exactly match:

`https://jentry-jentry.up.railway.app/oauth/xero/callback`

## iOS app configuration

Point these keys in `Jentry/Info.plist` at your deployed backend:

`JENTRYBackendBaseURL=https://your-domain.example`
`https://your-domain.example/jentry/uploads`
`https://your-domain.example/analyze`
`https://your-domain.example/problem-report`

The current app config expects:

- `JENTRYBackendBaseURL`
- `JENTRYBackendUploadURL`
- `JENTRYBackendAnalysisURL`
- `JENTRYBackendProblemReportURL`
- `JENTRYXeroAppCallbackURL=jentry://oauth/xero/callback`

For local testing, use a reachable tunnel or deployed server. A phone app cannot reach your Mac's `127.0.0.1`.

## Expected upload format

The iOS app sends:
- one `metadata` part containing JSON
- one or more `documents[]` parts containing PDFs

The relay uses:
- `deliveryTo`
- `deliverySubject`
- `deliveryBody`
- `accountId`
- `routingMethod`
- `extractedDocuments`

If `routingMethod` is `xeroFilesEmail`, the relay sends the files via Gmail API.

If `routingMethod` is `xeroIntegration`, the relay attempts to create an accounts payable bill in the selected Xero organisation and uploads the PDFs as attachments.
