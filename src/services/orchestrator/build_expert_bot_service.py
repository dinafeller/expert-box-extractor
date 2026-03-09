import os
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

headers = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
}

def build_expert_bot(expert_id: str):
        # check expert exists
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/experts",
        headers=headers,
        params={
            "id": f"eq.{expert_id}",
            "select": "id,bot_status"
        },
        timeout=30,
    )

    if r.status_code != 200:
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "experts fetch failed",
            "status": r.status_code,
            "body": r.text
        }

    rows = r.json()

    if not rows:
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "expert not found"
        }

    expert = rows[0]
    # set bot_status = building
    r2 = requests.patch(
        f"{SUPABASE_URL}/rest/v1/experts",
        headers=headers,
        params={"id": f"eq.{expert_id}"},
        json={"bot_status": "building"},
        timeout=30,
    )
    # cleanup previous build
    r3 = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/cleanup_expert_build",
        headers=headers,
        json={"p_expert_id": expert_id},
        timeout=30,
    )

    if r3.status_code not in (200, 204):
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "cleanup_expert_build failed",
            "status": r3.status_code,
            "body": r3.text
        }

    if r2.status_code not in (200, 204):
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "failed to set bot_status building",
            "status": r2.status_code,
            "body": r2.text
        }
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
        "message": "build started"
    }
