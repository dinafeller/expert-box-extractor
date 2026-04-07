import os
import io
from datetime import datetime

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
BUCKET = os.environ.get("BUCKET", "expert-materials")


# ---------- helpers ----------

def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }


def update_material(material_id: str, body: dict):
    return requests.patch(
        f"{SUPABASE_URL}/rest/v1/materials",
        headers={
            **supabase_headers(),
            "Content-Type": "application/json",
        },
        params={"id": f"eq.{material_id}"},
        json=body,
        timeout=30,
    )


def fetch_material(material_id: str):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/materials",
        headers=supabase_headers(),
        params={"id": f"eq.{material_id}"},
        timeout=30,
    )
    if r.status_code != 200:
        raise Exception(f"fetch failed: {r.text}")
    rows = r.json()
    if not rows:
        raise Exception("material not found")
    return rows[0]


# ---------- VIDEO ----------

def extract_youtube_id(url: str):
    import re
    patterns = [
        r"youtube\.com/watch\?v=([^&]+)",
        r"youtu\.be/([^?]+)",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def extract_video_text(url: str):
    video_id = extract_youtube_id(url)

    if not video_id:
        return ""

    # ⚠️ пока без API — просто заглушка
    # потом подключим нормальный транскрипшн
    return f"[VIDEO CONTENT PLACEHOLDER]\nVideo URL: {url}"


# ---------- DOCUMENT ----------

def extract_document_text(storage_path: str):
    r2 = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/sign/{BUCKET}/{storage_path}",
        headers=supabase_headers(),
        json={"expiresIn": 600},
        timeout=30,
    )

    signed_path = r2.json().get("signedURL") or r2.json().get("signedUrl")
    file_url = f"{SUPABASE_URL}/storage/v1{signed_path}"

    file_resp = requests.get(file_url, timeout=60)

    blob = file_resp.content
    content_type = (file_resp.headers.get("content-type") or "").lower()

    if "pdf" in content_type:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(blob))
        return "\n".join([p.extract_text() or "" for p in reader.pages])

    elif "docx" in content_type:
        import mammoth
        return mammoth.extract_raw_text(io.BytesIO(blob)).value

    else:
        return blob.decode("utf-8", errors="ignore")


# ---------- MAIN ----------

@app.post("/extract")
def extract():
    data = request.get_json(force=True) or {}
    material_id = data.get("material_id")

    if not material_id:
        return jsonify({"error": "material_id required"}), 400

    try:
        mat = fetch_material(material_id)

        source_type = (mat.get("source_type") or "").lower()
        storage_path = mat.get("storage_path")
        source_url = mat.get("source_url")

        # ---- processing ----

        if source_type == "video":
            text = extract_video_text(source_url)

        elif source_type in ("document", "file"):
            text = extract_document_text(storage_path)

        else:
            raise Exception(f"Unsupported type: {source_type}")

        # ---- save ----

        update_material(material_id, {
            "extracted_text": text,
            "extraction_status": "extracted" if text else "empty",
            "transcription_status": "done" if source_type == "video" else None,
            "extracted_at": datetime.utcnow().isoformat(),
        })

        return jsonify({
            "ok": True,
            "type": source_type,
            "text_len": len(text)
        })

    except Exception as e:
        update_material(material_id, {
            "extraction_status": "failed",
            "extraction_error": str(e),
        })

        return jsonify({"error": str(e)}), 500


@app.get("/health")
def health():
    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
