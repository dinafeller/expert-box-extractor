import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
BUCKET = os.environ.get("BUCKET", "expert-materials")

@app.get("/health")
def health():
    return "ok", 200

@app.post("/extract")
def extract():
    data = request.get_json(force=True, silent=True) or {}
    material_id = data.get("material_id")
    if not material_id:
        return jsonify({"error": "material_id is required"}), 400

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return jsonify({"error": "Missing env SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"}), 500

    # 1) Fetch material from Supabase
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/materials",
        headers=headers,
        params={"id": f"eq.{material_id}", "select": "id,expert_id,storage_path,title"},
        timeout=30,
    )
    if r.status_code != 200:
        return jsonify({"error": "materials fetch failed", "status": r.status_code, "body": r.text}), 400

    rows = r.json()
    if not rows:
        return jsonify({"error": "material not found"}), 404

    mat = rows[0]
    storage_path = mat.get("storage_path")
    if not storage_path:
        return jsonify({"error": "material.storage_path is empty"}), 400

    # 2) Create signed URL for download (private bucket)
    r2 = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/sign/{BUCKET}/{storage_path}",
        headers=headers,
        json={"expiresIn": 600},
        timeout=30,
    )
    if r2.status_code not in (200, 201):
        return jsonify({"error": "sign failed", "status": r2.status_code, "body": r2.text}), 400

    signed_path = r2.json().get("signedURL") or r2.json().get("signedUrl")
    if not signed_path:
        return jsonify({"error": "sign did not return signedURL", "body": r2.text}), 400

    file_url = f"{SUPABASE_URL}/storage/v1{signed_path}"

    # 3) Download file bytes
    pdf_resp = requests.get(file_url, timeout=60)
    if pdf_resp.status_code != 200:
        return jsonify({"error": "download failed", "status": pdf_resp.status_code}), 400

    content_type = (pdf_resp.headers.get("content-type") or "").lower()
    blob = pdf_resp.content

    # 4) Extract text: PDF only for now (we'll extend later)
    text = ""
    if "pdf" in content_type or storage_path.lower().endswith(".pdf"):
        try:
            from pypdf import PdfReader
            import io
            reader = PdfReader(io.BytesIO(blob))
            parts = []
            for page in reader.pages:
                parts.append(page.extract_text() or "")
            text = "\n".join(parts).strip()
        except Exception as e:
            return jsonify({"error": "pdf parse failed", "details": str(e)}), 500
    else:
        # fallback: treat as utf-8 text
        try:
            text = blob.decode("utf-8", errors="ignore").strip()
        except Exception:
            text = ""

preview = text[:2000]

# --- simple chunking ---
CHUNK_SIZE = 1200

chunks = []
if text:
    for i in range(0, len(text), CHUNK_SIZE):
        chunk = text[i:i + CHUNK_SIZE]
        chunks.append(chunk)
    
return jsonify({
    "ok": True,
    "material_id": mat["id"],
    "storage_path": storage_path,
    "content_type": content_type,
    "text_len": len(text),
    "preview": preview,
    "text": text,
    "chunks": chunks,
    "chunks_count": len(chunks),
    "note": "Extractor service: returns full text + simple chunks."
}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
