import os
import requests
from datetime import datetime

# ===== Secrets =====
XAI_API_KEY = os.getenv("XAI_API_KEY")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEB_HOOK")

# ===== XAI Endpoint =====
XAI_URL = "https://api.x.ai/v1/chat/completions"

# ===== Load Prompt =====
with open("prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()

# ===== XAI Request =====
headers = {
    "Authorization": f"Bearer {XAI_API_KEY}",
    "Content-Type": "application/json"
}

data = {
    "model": "grok-2-latest",
    "messages": [
        {"role": "system", "content": "You are a professional financial intelligence AI."},
        {"role": "user", "content": prompt}
    ],
    "temperature": 0.2
}

response = requests.post(XAI_URL, headers=headers, json=data)
response.raise_for_status()

result = response.json()["choices"][0]["message"]["content"]

# ===== Discord Post =====
discord_data = {
    "content": f"📊 **Global Intelligence Report**\n\n{result[:1900]}"
}

requests.post(DISCORD_WEBHOOK, json=discord_data)

print("Report sent successfully.")
