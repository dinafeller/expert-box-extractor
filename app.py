import os
import io
from datetime import datetime

import requests
from flask import Flask, request, jsonify
from src.routes.build_expert_bot import build_expert_bot_bp

app = Flask(__name__)
app.register_blueprint(build_expert_bot_bp)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
BUCKET = os.environ.get("BUCKET", "expert-materials")


@app.get("/health")
def health():
    return "ok", 200


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }


def update_material(material_id: str, body: dict):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/materials",
        headers={
            **supabase_headers(),
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
        params={"id": f"eq.{material_id}"},
        json=body,
        timeout=30,
    )
    return r


@app.post("/extract")
def extract():
    data = request.get_json(force=True, silent=True) or {}
    material_id = data.get("material_id")

    if not material_id:
        return jsonify({"error": "material_id is required"}), 400

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return jsonify({"error": "Missing env SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"}), 500

    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/materials",
            headers=supabase_headers(),
            params={"id": f"eq.{material_id}", "select": "id,expert_id,storage_path,title"},
            timeout=30,
        )
        if r.status_code != 200:
            return jsonify({
                "error": "materials fetch failed",
                "status": r.status_code,
                "body": r.text
            }), 400

        rows = r.json()
        if not rows:
            return jsonify({"error": "material not found"}), 404

        mat = rows[0]
        storage_path = mat.get("storage_path")
        if not storage_path:
            update_material(material_id, {
                "extraction_status": "failed",
                "extraction_error": "material.storage_path is empty",
            })
            return jsonify({"error": "material.storage_path is empty"}), 400

        r2 = requests.post(
            f"{SUPABASE_URL}/storage/v1/object/sign/{BUCKET}/{storage_path}",
            headers=supabase_headers(),
            json={"expiresIn": 600},
            timeout=30,
        )
        if r2.status_code not in (200, 201):
            update_material(material_id, {
                "extraction_status": "failed",
                "extraction_error": f"sign failed: {r2.text}",
            })
            return jsonify({
                "error": "sign failed",
                "status": r2.status_code,
                "body": r2.text
            }), 400

        sign_json = r2.json()
        signed_path = sign_json.get("signedURL") or sign_json.get("signedUrl")
        if not signed_path:
            update_material(material_id, {
                "extraction_status": "failed",
                "extraction_error": "sign did not return signedURL",
            })
            return jsonify({
                "error": "sign did not return signedURL",
                "body": r2.text
            }), 400

        file_url = f"{SUPABASE_URL}/storage/v1{signed_path}"

        file_resp = requests.get(file_url, timeout=60)
        if file_resp.status_code != 200:
            update_material(material_id, {
                "extraction_status": "failed",
                "extraction_error": f"download failed: {file_resp.status_code}",
            })
            return jsonify({
                "error": "download failed",
                "status": file_resp.status_code
            }), 400

        content_type = (file_resp.headers.get("content-type") or "").lower()
        blob = file_resp.content
        lower_path = storage_path.lower()

        text = ""

        if "pdf" in content_type or lower_path.endswith(".pdf"):
            try:
                from pypdf import PdfReader
                reader = PdfReader(io.BytesIO(blob))
                parts = []
                for page in reader.pages:
                    parts.append(page.extract_text() or "")
                text = "\n".join(parts).strip()
            except Exception as e:
                update_material(material_id, {
                    "extraction_status": "failed",
                    "extraction_error": f"pdf parse failed: {str(e)}",
                })
                return jsonify({"error": "pdf parse failed", "details": str(e)}), 500

        elif "wordprocessingml.document" in content_type or lower_path.endswith(".docx"):
            try:
                import mammoth
                result = mammoth.extract_raw_text(io.BytesIO(blob))
                text = (result.value or "").strip()
            except Exception as e:
                update_material(material_id, {
                    "extraction_status": "failed",
                    "extraction_error": f"docx parse failed: {str(e)}",
                })
                return jsonify({"error": "docx parse failed", "details": str(e)}), 500

        else:
            try:
                text = blob.decode("utf-8", errors="ignore").strip()
            except Exception:
                text = ""

        if text and len(text) >= 50:
            save_resp = update_material(material_id, {
                "extracted_text": text,
                "extraction_status": "extracted",
                "extraction_error": None,
                "extracted_at": datetime.utcnow().isoformat(),
            })
            if save_resp.status_code not in (200, 204):
                return jsonify({
                    "error": "failed to save extracted text",
                    "status": save_resp.status_code,
                    "body": save_resp.text
                }), 500
        else:
            save_resp = update_material(material_id, {
                "extracted_text": text or None,
                "extraction_status": "empty",
                "extraction_error": "Document text too short or empty",
                "extracted_at": datetime.utcnow().isoformat(),
            })
            if save_resp.status_code not in (200, 204):
                return jsonify({
                    "error": "failed to save empty extraction result",
                    "status": save_resp.status_code,
                    "body": save_resp.text
                }), 500

        preview = text[:2000]

        return jsonify({
            "ok": True,
            "material_id": mat["id"],
            "storage_path": storage_path,
            "content_type": content_type,
            "text_len": len(text),
            "preview": preview,
            "note": "Extractor service: extracted text saved to materials."
        }), 200

    except Exception as e:
        try:
            update_material(material_id, {
                "extraction_status": "failed",
                "extraction_error": str(e),
            })
        except Exception:
            pass

        return jsonify({"error": "unexpected extractor error", "details": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
