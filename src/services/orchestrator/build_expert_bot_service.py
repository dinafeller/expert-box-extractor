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
        
    # chunk materials
    r4 = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/chunk_all_materials_for_expert",
        headers=headers,
        json={"p_expert_id": expert_id},
        timeout=60,
    )

    if r4.status_code not in (200, 204):
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "chunk_all_materials_for_expert failed",
            "status": r4.status_code,
            "body": r4.text
        }

    # embed chunks
    r5 = requests.post(
        f"{SUPABASE_URL}/functions/v1/embed_chunks",
        headers={
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json",
        },
        json={"expert_id": expert_id},
        timeout=120,
    )

    if r5.status_code not in (200, 201):
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "embed_chunks failed",
            "status": r5.status_code,
            "body": r5.text
        }

    # classify chunks
    r6 = requests.post(
        f"{SUPABASE_URL}/functions/v1/classify_chunks",
        headers={
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json",
        },
        json={"expert_id": expert_id},
        timeout=120,
    )

    if r6.status_code not in (200, 201):
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "classify_chunks failed",
            "status": r6.status_code,
            "body": r6.text
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

    # finalize build
    r7 = requests.patch(
        f"{SUPABASE_URL}/rest/v1/experts",
        headers=headers,
        params={"id": f"eq.{expert_id}"},
        json={"bot_status": "active"},
        timeout=30,
    )

    if r7.status_code not in (200, 204):
        return {
            "ok": False,
            "build_result": "failed",
            "bot_status": "failed",
            "error": "failed to set bot_status ready",
            "status": r7.status_code,
            "body": r7.text
        }

    return {
        "ok": True,
        "expert_id": expert_id,
        "build_result": "success",
        "bot_status": "active",
        "message": "bot build completed"
    }
