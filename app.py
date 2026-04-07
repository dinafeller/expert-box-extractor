import os
import io
import re
import json
import tempfile
import subprocess
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


def fetch_material(material_id: str):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/materials",
        headers=supabase_headers(),
        params={"id": f"eq.{material_id}"},
        timeout=30,
    )
    if r.status_code != 200:
        raise Exception(f"materials fetch failed: {r.status_code} - {r.text}")

    rows = r.json()
    if not rows:
        raise Exception("material not found")

    return rows[0]


def strip_html(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html or "")).strip()


def normalize_source_type(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if raw == "file":
        return "document"
    return raw


def extract_youtube_id(url: str) -> str | None:
    patterns = [
        r"youtube\.com/watch\?v=([^&]+)",
        r"youtu\.be/([^?&/]+)",
        r"youtube\.com/shorts/([^?&/]+)",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def parse_vtt_to_text(vtt_text: str) -> str:
    lines = []
    for raw_line in (vtt_text or "").splitlines():
        line = raw_line.strip()

        if not line:
            continue
        if line.upper() == "WEBVTT":
            continue
        if "-->" in line:
            continue
        if re.match(r"^\d+$", line):
            continue

        lines.append(line)

    text = " ".join(lines)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_document_text(storage_path: str):
    r2 = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/sign/{BUCKET}/{storage_path}",
        headers=supabase_headers(),
        json={"expiresIn": 600},
        timeout=30,
    )
    if r2.status_code not in (200, 201):
        raise Exception(f"sign failed: {r2.status_code} - {r2.text}")

    sign_json = r2.json()
    signed_path = sign_json.get("signedURL") or sign_json.get("signedUrl")
    if not signed_path:
        raise Exception("sign did not return signedURL")

    file_url = f"{SUPABASE_URL}/storage/v1{signed_path}"

    file_resp = requests.get(file_url, timeout=60)
    if file_resp.status_code != 200:
        raise Exception(f"download failed: {file_resp.status_code}")

    content_type = (file_resp.headers.get("content-type") or "").lower()
    blob = file_resp.content
    lower_path = storage_path.lower()

    text = ""

    if "pdf" in content_type or lower_path.endswith(".pdf"):
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(blob))
        parts = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        text = "\n".join(parts).strip()

    elif "wordprocessingml.document" in content_type or lower_path.endswith(".docx"):
        import mammoth
        result = mammoth.extract_raw_text(io.BytesIO(blob))
        text = (result.value or "").strip()

    else:
        try:
            text = blob.decode("utf-8", errors="ignore").strip()
        except Exception:
            text = ""

    return text


def extract_youtube_transcript(url: str) -> str:
    from youtube_transcript_api import YouTubeTranscriptApi

    video_id = extract_youtube_id(url)
    if not video_id:
        raise Exception("Unsupported YouTube URL format")

    ytt = YouTubeTranscriptApi()
    fetched = ytt.fetch(video_id)

    text = " ".join((snippet.text or "").strip() for snippet in fetched)
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) < 50:
        raise Exception("YouTube transcript is too short or unavailable")

    return text


def extract_vimeo_subtitles_with_ytdlp(url: str) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_template = os.path.join(tmpdir, "video.%(ext)s")

        # Сначала пробуем обычные сабы, потом авто-сабы
        commands = [
            [
                "yt-dlp",
                "--skip-download",
                "--write-subs",
                "--sub-langs",
                "all",
                "--convert-subs",
                "vtt",
                "-o",
                output_template,
                url,
            ],
            [
                "yt-dlp",
                "--skip-download",
                "--write-auto-subs",
                "--sub-langs",
                "all",
                "--convert-subs",
                "vtt",
                "-o",
                output_template,
                url,
            ],
        ]

        last_error = None

        for cmd in commands:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=180,
            )

            if result.returncode != 0:
                last_error = f"yt-dlp failed: {result.stderr or result.stdout}"
                continue

            vtt_files = []
            for root, _, files in os.walk(tmpdir):
                for f in files:
                    if f.lower().endswith(".vtt"):
                        vtt_files.append(os.path.join(root, f))

            if not vtt_files:
                last_error = "No subtitle files were produced by yt-dlp"
                continue

            # Берём самый большой VTT — обычно самый полезный
            vtt_files.sort(key=lambda p: os.path.getsize(p), reverse=True)
            best_file = vtt_files[0]

            with open(best_file, "r", encoding="utf-8", errors="ignore") as fh:
                vtt_text = fh.read()

            text = parse_vtt_to_text(vtt_text)
            if len(text) < 50:
                last_error = "Parsed Vimeo subtitles are too short"
                continue

            return text

        raise Exception(last_error or "Vimeo subtitles not available")


def extract_video_text(source_url: str, video_provider: str | None, transcription_mode: str | None) -> str:
    provider = (video_provider or "").strip().lower()
    mode = (transcription_mode or "").strip().lower()

    if not source_url:
        raise Exception("video source_url is empty")

    if mode == "auto":
        # здесь позже будет внешний STT, пока честно не делаем вид, что уже умеем
        raise Exception("AUTO_TRANSCRIPTION_NOT_IMPLEMENTED")

    if mode != "subtitles":
        raise Exception(f"Unsupported video transcription_mode: {mode or 'null'}")

    if provider == "youtube" or "youtube.com" in source_url or "youtu.be" in source_url:
        return extract_youtube_transcript(source_url)

    if provider == "vimeo" or "vimeo.com" in source_url:
        return extract_vimeo_subtitles_with_ytdlp(source_url)

    raise Exception(f"Unsupported video provider: {provider or 'unknown'}")


@app.post("/extract")
def extract():
    data = request.get_json(force=True, silent=True) or {}
    material_id = data.get("material_id")

    if not material_id:
        return jsonify({"error": "material_id is required"}), 400

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return jsonify({"error": "Missing env SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"}), 500

    try:
        mat = fetch_material(material_id)

        source_type = normalize_source_type(mat.get("source_type"))
        storage_path = mat.get("storage_path")
        source_url = mat.get("source_url")
        video_provider = mat.get("video_provider")
        transcription_mode = mat.get("transcription_mode")
        existing_text = (mat.get("extracted_text") or "").strip()

        # ---------- DOCUMENT ----------
        if source_type == "document":
            if not storage_path:
                update_material(material_id, {
                    "extraction_status": "failed",
                    "extraction_error": "material.storage_path is empty",
                })
                return jsonify({"error": "material.storage_path is empty"}), 400

            text = extract_document_text(storage_path).strip()

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

            return jsonify({
                "ok": True,
                "material_id": material_id,
                "type": "document",
                "text_len": len(text),
                "preview": text[:2000],
            }), 200

        # ---------- VIDEO ----------
        if source_type == "video":
            if not source_url:
                update_material(material_id, {
                    "extraction_status": "failed",
                    "transcription_status": "failed",
                    "transcription_error": "material.source_url is empty",
                    "extraction_error": "material.source_url is empty",
                })
                return jsonify({"error": "material.source_url is empty"}), 400

            update_material(material_id, {
                "extraction_status": "extracting",
                "transcription_status": "processing",
                "transcription_error": None,
                "extraction_error": None,
            })

            try:
                text = extract_video_text(
                    source_url=source_url,
                    video_provider=video_provider,
                    transcription_mode=transcription_mode,
                ).strip()

                if not text or len(text) < 50:
                    raise Exception("Video transcript is too short")

                save_resp = update_material(material_id, {
                    "extracted_text": text,
                    "extraction_status": "extracted",
                    "transcription_status": "done",
                    "transcript_source": "subtitles",
                    "transcription_error": None,
                    "extraction_error": None,
                    "extracted_at": datetime.utcnow().isoformat(),
                })
                if save_resp.status_code not in (200, 204):
                    return jsonify({
                        "error": "failed to save extracted video text",
                        "status": save_resp.status_code,
                        "body": save_resp.text
                    }), 500

                return jsonify({
                    "ok": True,
                    "material_id": material_id,
                    "type": "video",
                    "provider": video_provider,
                    "text_len": len(text),
                    "preview": text[:2000],
                }), 200

            except Exception as e:
                msg = str(e)

                if msg == "AUTO_TRANSCRIPTION_NOT_IMPLEMENTED":
                    update_material(material_id, {
                        "extraction_status": "pending",
                        "transcription_status": "queued",
                        "transcription_error": None,
                        "extraction_error": None,
                    })
                    return jsonify({
                        "ok": True,
                        "material_id": material_id,
                        "type": "video",
                        "message": "Auto transcription queued but not implemented yet",
                    }), 200

                # ВАЖНО:
                # если summary уже есть, материал остаётся usable
                # поэтому НЕ ставим failed на весь материал
                if existing_text:
                    update_material(material_id, {
                        "extraction_status": "extracted",
                        "transcription_status": "failed",
                        "transcription_error": msg,
                        "extraction_error": None,
                    })
                    return jsonify({
                        "ok": False,
                        "material_id": material_id,
                        "type": "video",
                        "warning": msg,
                        "note": "Transcript failed, existing summary kept",
                    }), 200

                update_material(material_id, {
                    "extraction_status": "failed",
                    "transcription_status": "failed",
                    "transcription_error": msg,
                    "extraction_error": msg,
                })
                return jsonify({
                    "error": "video extraction failed",
                    "details": msg,
                }), 500

        return jsonify({"error": f"Unsupported source_type: {source_type}"}), 400

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
