import os
import requests
headers = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
}

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")


def build_expert_bot(expert_id: str):
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "Missing env SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY",
        }

    if not expert_id:
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "expert_id is required",
        }

    return {
        "ok": True,
        "expert_id": expert_id,
        "build_result": "success",
        "bot_status": "building",
        "message": "build_expert_bot service connected"
    }
