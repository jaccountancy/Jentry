function decodeJWT(token) {
    if (!token) return null;
    const [, payload] = token.split(".");
    if (!payload) return null;
    try {
        return JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    } catch {
        return null;
    }
}

function extractConnectedUserEmailFromToken(idToken) {
    const payload = decodeJWT(idToken);
    return normalizeOptionalString(payload?.email) || null;
}