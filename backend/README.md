        # Jentry Relay

This service handles:

- `POST /jentry/uploads` for Gmail / Xero Files delivery
- `POST /analyze` and `POST /jentry/analyze` for AI extraction
- `POST /problem-report` and `POST /jentry/problem-report` for support submissions
- Xero OAuth, chart-of-accounts sync, transaction matching, and direct bill publishing for Advanced Jentry Mode

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
- `POST /jentry/analyze`
- `POST /problem-report`
- `POST /jentry/problem-report`
- `GET /oauth/xero/start`
- `GET /oauth/xero/callback`
- `GET /xero/status`
- `GET /xero/tenants`
- `POST /xero/select-tenant`
- `POST /xero/disconnect`
- `GET /xero/accounts`
- `POST /xero/match-transactions`
- `POST /xero/publish-bill`

## iOS app configuration

Point these keys in `Jentry/Info.plist` at your deployed backend:

- `JENTRYBackendBaseURL=https://your-domain.example`
- `JENTRYBackendUploadURL=https://your-domain.example/jentry/uploads`
- `JENTRYBackendAnalysisURL=https://your-domain.example/analyze`
- `JENTRYBackendProblemReportURL=https://your-domain.example/problem-report`
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

and sends the files via Gmail API from the configured sender mailbox when `routingMethod` is `xeroFilesEmail`.

When `routingMethod` is `xeroIntegration`, the relay uses the selected Xero tenant to:

- sync chart-of-accounts data
- find possible existing transaction matches
- create accounts payable bills
- upload the PDFs as Xero attachments
