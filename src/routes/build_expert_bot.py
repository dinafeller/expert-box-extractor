from flask import Blueprint, request, jsonify

build_expert_bot_bp = Blueprint("build_expert_bot", __name__)


@build_expert_bot_bp.route("/build-expert-bot", methods=["POST"])
def build_expert_bot():
    data = request.get_json(force=True, silent=True) or {}
    expert_id = data.get("expert_id")

    if not expert_id:
        return jsonify({"error": "expert_id is required"}), 400

    return jsonify({
        "ok": True,
        "message": "Build orchestrator endpoint is alive",
        "expert_id": expert_id
    })
