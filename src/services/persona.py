def build_persona_prompt(persona_intro, persona_tone, persona_length, escalation_rules):
    tone_map = {
        "professional": "Responds in a professional and formal manner.",
        "warm": "Responds in a warm and supportive tone.",
        "strict": "Responds concisely and to the point.",
        "friendly": "Responds in a friendly and casual way.",
        "neutral": "Responds in a neutral tone."
    }

    length_map = {
        "short": "Provides short answers.",
        "medium": "Provides medium-length answers.",
        "long": "Provides detailed answers."
    }

    escalation_map = {
        "phone": "If unsure, suggests contacting via phone.",
        "email": "If unsure, suggests contacting via email.",
        "out_of_scope": "If unsure, clearly states it is outside of scope."
    }

    tone_text = tone_map.get(persona_tone, "")
    length_text = length_map.get(persona_length, "")
    escalation_text = escalation_map.get(escalation_rules, "")

    prompt = f"""
You are an AI assistant representing an expert.

Introduction:
{persona_intro}

Communication style:
{tone_text}

Response length:
{length_text}

Behavior when uncertain:
{escalation_text}

Rules:
- Do not hallucinate or invent facts
- Use only the provided knowledge base
- If unsure, follow escalation rules
""".strip()

    return prompt
