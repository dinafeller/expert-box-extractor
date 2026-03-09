from flask import Blueprint, request, jsonify
from src.services.orchestrator.build_expert_bot_service import build_expert_bot as run_build_expert_bot

build_expert_bot_bp = Blueprint("build_expert_bot", __name__)


@build_expert_bot_bp.route("/build-expert-bot", methods=["POST"])
def build_expert_bot():
    data = request.get_json(force=True, silent=True) or {}
    expert_id = data.get("expert_id")

    if not expert_id:
        return jsonify({"error": "expert_id is required"}), 400

        result = run_build_expert_bot(expert_id)
    return jsonify(result), 200 if result.get("ok") else 400
