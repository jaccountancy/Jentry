import dotenv from "dotenv";
import express from "express";
import { google } from "googleapis";
import MailComposer from "mailcomposer";
import multer from "multer";

dotenv.config();

const requiredEnvironmentVariables = [
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REFRESH_TOKEN",
    "GMAIL_SENDER"
];

for (const key of requiredEnvironmentVariables) {
    if (!process.env[key] || !process.env[key].trim()) {
        throw new Error(`Missing required environment variable: ${key}`);
    }
}

const port = Number(process.env.PORT || 3001);
const app = express();
const upload = multer({ storage: multer.memoryStorage() });

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

app.get("/health", (_request, response) => {
    console.log("Health check received.");
    response.json({ ok: true });
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
            const to = normalizeEmail(metadata.deliveryTo);
            const from = normalizeEmail(process.env.GMAIL_SENDER);
            const subject = String(metadata.deliverySubject || "Jentry submission");
            const body = String(metadata.deliveryBody || "");

            console.log("Upload parsed.", {
                submissionId: metadata.submissionId || "missing",
                to,
                from,
                documentCount: documentFiles.length
            });

            if (!to) {
                console.error("Upload rejected: deliveryTo missing.", {
                    submissionId: metadata.submissionId || "missing"
                });
                response.status(400).json({ message: "Missing deliveryTo in metadata." });
                return;
            }

            if (documentFiles.length === 0) {
                console.error("Upload rejected: no documents attached.", {
                    submissionId: metadata.submissionId || "missing"
                });
                response.status(400).json({ message: "No documents were uploaded." });
                return;
            }

            const rawMessage = await buildRawMessage({
                from,
                to,
                subject,
                body,
                attachments: documentFiles.map((file) => ({
                    filename: file.originalname,
                    content: file.buffer,
                    contentType: file.mimetype || "application/pdf"
                }))
            });

            const sendResult = await gmail.users.messages.send({
                userId: "me",
                requestBody: {
                    raw: rawMessage
                }
            });

            console.log("Email sent successfully.", {
                submissionId: metadata.submissionId || sendResult.data.id,
                gmailMessageId: sendResult.data.id,
                to,
                documentCount: documentFiles.length
            });

            response.json({
                submissionId: metadata.submissionId || sendResult.data.id,
                message: `Submission emailed to ${to} from ${from}.`
            });
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

app.listen(port, () => {
    console.log(`Jentry relay listening on http://127.0.0.1:${port}`);
});

function normalizeEmail(value) {
    if (typeof value !== "string") {
        return "";
    }

    return value.trim();
}

async function buildRawMessage({ from, to, subject, body, attachments }) {
    const composer = new MailComposer.MailComposer({
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
