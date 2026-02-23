import os
import json
import requests
import re
from openai import OpenAI

# 環境変数の読み込み
XAI_API_KEY = os.getenv("XAI_API_KEY")
DISCORD_WEB_HOOK = os.getenv("DISCORD_WEB_HOOK")
DB_FILE = "processed_ids.json"

# クライアント初期化
client = OpenAI(
    api_key=XAI_API_KEY,
    base_url="https://api.x.ai/v1"
)

# 監視対象のアカウントリスト
TARGET_ACCOUNTS = [
    "@RayDalio", "@CathieDWood", "@LizAnnSonders", "@Ritholtz", "@BobEunlimited",
    "@WarrenBuffett", "@chamath", "@naval", "@morganhousel", "@BrianFeroldi",
    "@BillAckman", "@Carl_C_Icahn", "@DanielSloeb1", "@georgesoros",
    "@HindenburgRes", "@CitronResearch", "@AlderLaneEggs", "@RealJimChanos", "@MuddyWatersRe",
    "@charliebilello", "@EconGuyRosie", "@fundstrat",
    "@AswathDamodaran", "@elerianm", "@paulkrugman", "@jimcramer", "@matt_levine"
]

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content: return {}
            try:
                return json.loads(content)
            except:
                return {}
    return {}

def save_db(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=4, ensure_ascii=False)

def check_account(account, last_id):
    # モデル名は最新のままでOK
    target_model = "grok-4-1-fast-reasoning"
    
    prompt = (
        f"Find the absolute latest post from {account} on X. "
        f"If the post ID is newer than {last_id}, provide the numeric ID and a brief summary. "
        "Format: 'ID: [numeric_id] | Summary: [text]'. If nothing new, reply 'None'."
    )

    try:
        response = client.chat.completions.create(
            model=target_model,
            messages=[
                {"role": "system", "content": "You are a precise data extractor focusing on X (Twitter) posts."},
                {"role": "user", "content": prompt}
            ],
            # 【修正箇所】ツール名を 'x_search' から 'live_search' に戻しました
            tools=[
                {
                    "type": "live_search",
                    "live_search": {
                        "sources": ["x"]
                    }
                }
            ]
        )
        
        res_text = response.choices[0].message.content.strip()
        print(f"Debug [{account}]: {res_text}")

        if "None" in res_text or not res_text:
            return None

        # 正規表現
        id_match = re.search(r"ID[:\s]+(\d+)", res_text, re.IGNORECASE)
        summary_match = re.search(r"Summary[:\s]+(.*)", res_text, re.IGNORECASE | re.DOTALL)

        if id_match and summary_match:
            new_id = id_match.group(1)
            summary = summary_match.group(1).strip()
            
            try:
                if last_id == "0" or int(new_id) > int(last_id):
                    return {"id": new_id, "summary": summary}
            except ValueError:
                print(f"Error: Invalid ID format for {account}")
        
        return None

    except Exception as e:
        print(f"Error checking {account}: {e}")
        return None

def send_discord(message):
    if not DISCORD_WEB_HOOK: return
    payload = {"content": message[:1900]}
    try:
        requests.post(DISCORD_WEB_HOOK, json=payload)
    except Exception as e:
        print(f"Discord sending error: {e}")

def main():
    db = load_db()
    new_updates = []
    print(f"Checking {len(TARGET_ACCOUNTS)} accounts...")

    for account in TARGET_ACCOUNTS:
        last_id = db.get(account, "0")
        result = check_account(account, last_id)
        
        if result:
            new_id = result["id"]
            summary = result["summary"]
            
            db[account] = new_id
            new_updates.append(f"🔔 **{account}**\n{summary}\n🔗 https://x.com/i/status/{new_id}")

    if new_updates:
        msg = "⚠️ **【投資家X監視：新着レポート】**\n\n" + "\n\n---\n\n".join(new_updates)
        send_discord(msg)
        save_db(db)
        print(f"Done! {len(new_updates)} notifications sent.")
    else:
        print("No new updates found.")

if __name__ == "__main__":
    main()
