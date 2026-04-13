import os
import io
import math
import shutil
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

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_TRANSCRIPTION_MODEL = os.environ.get(
    "OPENAI_TRANSCRIPTION_MODEL",
    "gpt-4o-transcribe",
)

# Safe margin under 25 MB
MAX_TRANSCRIBE_BYTES = 24 * 1024 * 1024


@app.get("/health")
def health():
    return "ok", 200


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
            "Prefer": "return=representation",
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
        raise Exception(f"materials fetch failed: {r.status_code} - {r.text}")

    rows = r.json()
    if not rows:
        raise Exception("material not found")

    return rows[0]


def normalize_source_type(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if raw == "file":
        return "document"
    return raw


def sign_storage_path(storage_path: str):
    r = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/sign/{BUCKET}/{storage_path}",
        headers=supabase_headers(),
        json={"expiresIn": 600},
        timeout=30,
    )
    if r.status_code not in (200, 201):
        raise Exception(f"sign failed: {r.status_code} - {r.text}")

    payload = r.json()
    signed_path = payload.get("signedURL") or payload.get("signedUrl")
    if not signed_path:
        raise Exception("sign did not return signedURL")

    return f"{SUPABASE_URL}/storage/v1{signed_path}"


def download_binary(url: str, timeout: int = 180) -> tuple[bytes, str]:
    r = requests.get(
        url,
        timeout=timeout,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ExpertBoxBot/1.0)",
        },
        stream=False,
        allow_redirects=True,
    )
    if r.status_code != 200:
        raise Exception(f"download failed: {r.status_code}")

    content_type = (r.headers.get("content-type") or "").lower()
    return r.content, content_type


def extract_document_text(storage_path: str):
    file_url = sign_storage_path(storage_path)
    blob, content_type = download_binary(file_url, timeout=120)
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


def transcribe_with_openai(source_bytes: bytes, filename: str, content_type: str | None = None) -> str:
    if not OPENAI_API_KEY:
        raise Exception("Missing OPENAI_API_KEY")

    files = {
        "file": (filename, source_bytes, content_type or "application/octet-stream"),
    }
    data = {
        "model": OPENAI_TRANSCRIPTION_MODEL,
    }

    r = requests.post(
        "https://api.openai.com/v1/audio/transcriptions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        },
        files=files,
        data=data,
        timeout=600,
    )

    if r.status_code != 200:
        raise Exception(f"OpenAI transcription failed: {r.status_code} - {r.text}")

    payload = r.json()
    text = (payload.get("text") or "").strip()

    if len(text) < 10:
        raise Exception("OpenAI transcript too short or empty")

    return text


def require_ffmpeg():
    if shutil.which("ffmpeg") is None:
        raise Exception("ffmpeg is not installed in the Render environment")
    if shutil.which("ffprobe") is None:
        raise Exception("ffprobe is not installed in the Render environment")


def write_temp_file(path: str, blob: bytes):
    with open(path, "wb") as f:
        f.write(blob)


def get_media_duration_seconds(path: str) -> float:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            path,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise Exception(f"ffprobe failed: {result.stderr or result.stdout}")

    value = (result.stdout or "").strip()
    if not value:
        raise Exception("ffprobe returned empty duration")

    return float(value)


def extract_audio_to_wav(input_path: str, output_path: str):
    result = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-c:a",
            "pcm_s16le",
            output_path,
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise Exception(f"ffmpeg audio extraction failed: {result.stderr or result.stdout}")


def split_audio_if_needed(audio_path: str, tmpdir: str) -> list[str]:
    size = os.path.getsize(audio_path)
    if size <= MAX_TRANSCRIBE_BYTES:
        return [audio_path]

    duration = get_media_duration_seconds(audio_path)
    if duration <= 0:
        return [audio_path]

    num_parts = math.ceil(size / MAX_TRANSCRIBE_BYTES)
    segment_duration = max(30, math.ceil(duration / num_parts))

    segment_pattern = os.path.join(tmpdir, "segment_%03d.wav")

    result = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            audio_path,
            "-f",
            "segment",
            "-segment_time",
            str(segment_duration),
            "-c",
            "copy",
            segment_pattern,
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise Exception(f"ffmpeg segmentation failed: {result.stderr or result.stdout}")

    files = sorted(
        os.path.join(tmpdir, name)
        for name in os.listdir(tmpdir)
        if name.startswith("segment_") and name.endswith(".wav")
    )

    if not files:
        raise Exception("No audio segments were created")

    return files


def transcript_is_suspiciously_short(text: str, duration_sec: float) -> bool:
    text = (text or "").strip()

    if not text:
        return True

    if duration_sec <= 15:
        return len(text) < 15

    if duration_sec <= 30:
        return len(text) < 40

    if duration_sec <= 60:
        return len(text) < 90

    if duration_sec <= 120:
        return len(text) < 180

    if duration_sec <= 300:
        return len(text) < 400

    return len(text) < int(duration_sec * 1.5)


def transcribe_audio_file(path: str) -> str:
    with open(path, "rb") as f:
        blob = f.read()

    duration_sec = get_media_duration_seconds(path)
    text = transcribe_with_openai(blob, os.path.basename(path), "audio/wav").strip()

    if transcript_is_suspiciously_short(text, duration_sec):
        raise Exception(
            f"Transcript suspiciously short for duration {duration_sec:.1f}s: got {len(text)} chars"
        )

    return text


def extract_uploaded_video_text(storage_path: str) -> str:
    require_ffmpeg()

    signed_url = sign_storage_path(storage_path)
    blob, _ = download_binary(signed_url, timeout=240)

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input_video.mp4")
        audio_path = os.path.join(tmpdir, "audio.wav")

        write_temp_file(input_path, blob)
        extract_audio_to_wav(input_path, audio_path)

        segments = split_audio_if_needed(audio_path, tmpdir)
        parts = []

        for segment_path in segments:
            part_text = transcribe_audio_file(segment_path).strip()
            if part_text:
                parts.append(part_text)

        text = "\n".join(parts).strip()
        if len(text) < 20:
            raise Exception("Final transcript too short or empty")

        return text


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
        file_path = mat.get("file_path")
        source_url = mat.get("source_url")
        video_provider = (mat.get("video_provider") or "").strip().lower()
        transcription_mode = (mat.get("transcription_mode") or "").strip().lower()
        existing_text = (mat.get("extracted_text") or "").strip()

        # ---------- DOCUMENT ----------
        if source_type == "document":
            effective_storage_path = storage_path or file_path

            if not effective_storage_path:
                update_material(material_id, {
                    "extraction_status": "failed",
                    "extraction_error": "material.storage_path is empty",
                })
                return jsonify({"error": "material.storage_path is empty"}), 400

            update_material(material_id, {
                "extraction_status": "extracting",
                "extraction_error": None,
            })

            text = extract_document_text(effective_storage_path).strip()

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
            update_material(material_id, {
                "extraction_status": "extracting",
                "transcription_status": "processing",
                "transcription_error": None,
                "extraction_error": None,
            })

            try:
                if transcription_mode != "auto":
                    update_material(material_id, {
                        "extraction_status": "extracted",
                        "transcription_status": "not_requested",
                        "transcription_error": None,
                        "extraction_error": None,
                        "extracted_at": datetime.utcnow().isoformat(),
                    })
                    return jsonify({
                        "ok": True,
                        "material_id": material_id,
                        "type": "video",
                        "message": "No auto transcription requested",
                    }), 200

                effective_storage_path = storage_path or file_path

                if video_provider == "upload" or effective_storage_path:
                    if not effective_storage_path:
                        raise Exception("material.storage_path is empty for uploaded video")

                    text = extract_uploaded_video_text(effective_storage_path).strip()
                else:
                    if not source_url:
                        raise Exception("material.source_url is empty")

                    raise Exception("Remote video transcription is not supported in this build")

                if not text or len(text) < 20:
                    raise Exception("Video transcript is too short")

                save_resp = update_material(material_id, {
                    "extracted_text": text,
                    "extraction_status": "extracted",
                    "transcription_status": "done",
                    "transcript_source": "auto_api",
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
                    "provider": video_provider or "upload",
                    "text_len": len(text),
                    "preview": text[:2000],
                }), 200

            except Exception as e:
                msg = str(e)

                update_material(material_id, {
                    "extraction_status": "failed",
                    "transcription_status": "failed",
                    "transcription_error": msg,
                    "extraction_error": msg,
                    "extracted_text": None,
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
