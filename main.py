import os
import json
import requests
from openai import OpenAI

# 環境変数の読み込み
XAI_API_KEY = os.getenv("XAI_API_KEY")
DISCORD_WEB_HOOK = os.getenv("DISCORD_WEB_HOOK")
DB_FILE = "processed_ids.json"

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
            try:
                return json.load(f)
            except:
                return {}
    return {}

def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=4)

def check_account(account, last_id):
    # 検索をより具体的にし、直近の投稿を1つ確実に取るように指示
    prompt = f"Find the most recent post from {account} on X. If the post ID is newer than {last_id}, provide the ID and a brief summary. If no new post, reply 'None'."
    
    try:
        response = client.chat.completions.create(
            model="grok-2", 
            messages=[
                {"role": "system", "content": "You are a financial bot. If there is a new post, reply ONLY in this format: ID: [post_id] / Summary: [text]. If not, reply 'None'."},
                {"role": "user", "content": prompt}
            ],
            tools=[{
                "type": "live_search",
                "live_search": {"sources": ["x"]}
            }]
        )
        res_text = response.choices[0].message.content
        print(f"Debug [{account}]: {res_text}") # ログでAIの回答を確認できるようにする
        return res_text
    except Exception as e:
        print(f"Error checking {account}: {e}")
        return "None"

def send_discord(message):
    if not DISCORD_WEB_HOOK:
        return
    requests.post(DISCORD_WEB_HOOK, json={"content": message})

def main():
    db = load_db()
    new_updates = []

    print(f"Checking {len(TARGET_ACCOUNTS)} accounts...")

    for account in TARGET_ACCOUNTS:
        last_id = db.get(account, "0")
        result = check_account(account, last_id)

        if result and "None" not in result and "ID:" in result:
            try:
                parts = result.split("/")
                new_id = parts[0].replace("ID:", "").strip()
                summary = parts[1].replace("Summary:", "").strip()

                if str(new_id) != str(last_id):
                    db[account] = new_id
                    new_updates.append(f"👤 **{account}**\n📝 {summary}\n🔗 https://x.com/i/status/{new_id}")
            except Exception as e:
                print(f"Parse error for {account}: {e}")
                continue

    if new_updates:
        # 1通にまとめて送信
        message_body = "🔔 **【投資家X監視：新着レポート】**\n\n" + "\n\n---\n\n".join(new_updates)
        
        # 分割送信（Discordの2000文字制限対策）
        if len(message_body) > 1900:
            for i in range(0, len(message_body), 1900):
                send_discord(message_body[i:i+1900])
        else:
            send_discord(message_body)
            
        save_db(db)
    else:
        print("No new updates to notify.")

if __name__ == "__main__":
    main()
