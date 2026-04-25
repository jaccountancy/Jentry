        # Jentry Relay

This service receives Jentry uploads and sends them through Gmail as `theteam@jaccountancy.co.uk`.

## Setup

1. Install Node.js 20 or later.
2. In `backend/`, run `npm install`.
3. Copy `.env.example` to `.env`.
4. Fill in these values in `.env` on your machine only:
   - `GOOGLE_CLIENT_ID`
   - `GOOGLE_CLIENT_SECRET`
   - `GOOGLE_REFRESH_TOKEN`
   - `GMAIL_SENDER`
5. Start the relay with `npm start`.

The server exposes:
- `GET /health`
- `POST /jentry/uploads`

## iOS app configuration

Point `JENTRYBackendUploadURL` in `Jentry/Info.plist` at your deployed endpoint, for example:

`https://your-domain.example/jentry/uploads`

For local testing, use a reachable tunnel or deployed server. A phone app cannot reach your Mac's `127.0.0.1`.

## Expected upload format

The iOS app sends:
- one `metadata` part containing JSON
- one or more `documents[]` parts containing PDFs

The relay uses:
- `deliveryTo`
- `deliverySubject`
- `deliveryBody`

and sends the files via Gmail API from the configured sender mailbox.
