import os
import requests
from datetime import datetime, timedelta

# ===== Secrets =====
XAI_API_KEY = os.getenv("XAI_API_KEY")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEB_HOOK")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

# ===== Settings =====
TICKERS = ["Tesla TSLA", "Palantir PLTR", "SoFi SOFI", "Celsius CELH"]
HOURS_BACK = 24

# ===== Tavily Search =====
def search_news(query):
    url = "https://api.tavily.com/search"
    headers = {"Content-Type": "application/json"}
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "days": 1,
        "include_answer": False
    }
    res = requests.post(url, headers=headers, json=payload)
    res.raise_for_status()
    return res.json().get("results", [])

# ===== Fetch Article Content =====
def fetch_article(url):
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return r.text[:5000]
    except:
        return None

# ===== Collect Verified News =====
collected_articles = []

for ticker in TICKERS:
    results = search_news(ticker)
    for item in results[:5]:
        article_text = fetch_article(item["url"])
        if article_text:
            collected_articles.append({
                "title": item["title"],
                "url": item["url"],
                "content": article_text
            })

# ===== Build AI Input =====
news_block = ""
for a in collected_articles:
    news_block += f"""
Title: {a['title']}
URL: {a['url']}
Content:
{a['content']}
---
"""

with open("prompt.txt", "r", encoding="utf-8") as f:
    base_prompt = f.read()

final_prompt = f"""
以下は実在URLから取得した過去24時間以内の記事です。
事実のみを用い、必ずURLを参照してレポートを作成してください。

{news_block}

{base_prompt}
"""

# ===== Call XAI =====
xai_url = "https://api.x.ai/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {XAI_API_KEY}",
    "Content-Type": "application/json"
}

data = {
    "model": "grok-2-latest",
    "messages": [
        {"role": "system", "content": "You are a strict financial intelligence analyst."},
        {"role": "user", "content": final_prompt}
    ],
    "temperature": 0.1
}

response = requests.post(xai_url, headers=headers, json=data)
response.raise_for_status()
report = response.json()["choices"][0]["message"]["content"]

# ===== Discord Post =====
def send_discord(text):
    chunks = [text[i:i+1900] for i in range(0, len(text), 1900)]
    for chunk in chunks:
        requests.post(DISCORD_WEBHOOK, json={"content": chunk})

send_discord(report)

print("Verified report sent successfully.")
