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
client = OpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")

TARGET_ACCOUNTS = [
    "@RayDalio", "@CathieDWood", "@LizAnnSonders", "@Ritholtz", "@BobEUnlimited",
    "@WarrenBuffett", "@chamath", "@naval", "@morganhousel", "@BrianFeroldi",
    "@BillAckman", "@Carl_C_Icahn", "@DanielSLoeb1", "@georgesoros",
    "@HindenburgRes", "@CitronResearch", "@AlderLaneEggs", "@RealJimChanos", "@MuddyWatersRe",
    "@charliebilello", "@EconguyRosie", "@FundstratCap",
    "@AswathDamodaran", "@elerianm", "@paulkrugman", "@jimcramer", "@matt_levine"
]

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f:
            try: return json.load(f)
            except: return {}
    return {}

def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=4)

def check_account(account, last_id):
    # AIが判断しやすいよう英語のプロンプトを推奨（精度向上のため）
    prompt = f"Find the absolute latest post from {account} on X. If the post ID is newer than {last_id}, provide the ID and a brief summary. If no new post, reply 'None'."
    
    try:
        response = client.chat.completions.create(
            model="grok-2", # モデル名を確認（grok-4は2026年現在の最新版か、環境に応じたものを使用）
            messages=[
                {"role": "system", "content": "You are a financial bot. Format: ID: [numeric_id] / Summary: [text]. If not, reply 'None'."},
                {"role": "user", "content": prompt}
            ],
            # 修正ポイント：live_searchの構造をAPI仕様に厳密に合わせる
            tools=[{
                "type": "live_search",
                "live_search": {
                    "sources": ["x"]
                }
            }]
        )
        res_text = response.choices[0].message.content.strip()
        print(f"Debug [{account}]: {res_text}")
        return res_text
    except Exception as e:
        print(f"Error checking {account}: {e}")
        return "None"

def send_discord(message):
    if not DISCORD_WEB_HOOK:
        return
    try:
        r = requests.post(DISCORD_WEB_HOOK, json={"content": message})
        r.raise_for_status()
    except Exception as e:
        print(f"Discord error: {e}")

def main():
    db = load_db()
    new_updates = []

    print(f"Checking {len(TARGET_ACCOUNTS)} accounts...")

    for account in TARGET_ACCOUNTS:
        last_id = db.get(account, "0")
        result = check_account(account, last_id)

        if result and "ID:" in result:
            try:
                # 正規表現でIDとSummaryを抽出
                id_match = re.search(r"ID:\s*(\d+)", result)
                summary_match = re.search(r"Summary:\s*(.+)", result, re.DOTALL)
                
                if id_match and summary_match:
                    new_id = id_match.group(1)
                    summary = summary_match.group(1).strip()

                    # 新着判定（数値比較）
                    if int(new_id) > int(last_id):
                        db[account] = new_id
                        new_updates.append(f"👤 **{account}**\n📝 {summary}\n🔗 https://x.com/i/status/{new_id}")
            except Exception as e:
                print(f"Parse error for {account}: {e}")

    if new_updates:
        header = "🔔 **【投資家X監視：新着レポート】**\n\n"
        body = "\n\n---\n\n".join(new_updates)
        full_msg = header + body
        
        if len(full_msg) > 1900:
            for i in range(0, len(full_msg), 1900):
                send_discord(full_msg[i:i+1900])
        else:
            send_discord(full_msg)
        
        save_db(db)
        print(f"Sent {len(new_updates)} notifications.")
    else:
        print("No new updates found.")

if __name__ == "__main__":
    main()
