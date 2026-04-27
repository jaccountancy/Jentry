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

function toBase64Url(value) {
  return Buffer.from(value)
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

app.get("/health", (_request, response) => {
  console.log("Health check received.");
  response.json({ ok: true });
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

      const to = String(metadata.deliveryTo || "");
      const from = String(
        process.env.GMAIL_SENDER_EMAIL ||
          process.env.JENTRY_BACKEND_SENDER_EMAIL ||
          "theteam@jaccountancy.co.uk"
      );
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
      const from = String(
        process.env.GMAIL_SENDER_EMAIL ||
          process.env.JENTRY_BACKEND_SENDER_EMAIL ||
          "theteam@jaccountancy.co.uk"
      );
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
        "POST /jentry/uploads",
        "POST /analyze",
        "POST /problem-report"
      ]
    })
  );
}); 