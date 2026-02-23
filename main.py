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
            try: return json.load(f)
            except: return {}
    return {}

def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=4)

def check_account(account, last_id):
    prompt = f"Find the absolute latest post from {account} on X. If the post ID is strictly newer than {last_id}, provide the numeric ID and a brief summary. If no new post, reply 'None'."
    
    try:
        response = client.chat.completions.create(
            model="grok-2", 
            messages=[
                {"role": "system", "content": "Return format: ID: [numeric_id] / Summary: [text]. If nothing new, return 'None'."},
                {"role": "user", "content": prompt}
            ],
            # 修正：live_searchの定義をAPIの期待する構造に変更
            tools=[{
                "type": "live_search",
                "live_search": {
                    "sources": ["x"]
                }
            }]
        )
        res_text = response.choices[0].message.content
        print(f"Debug [{account}]: {res_text}")
        return res_text
    except Exception as e:
        print(f"Error checking {account}: {e}")
        return "None"

def send_discord(message):
    if not DISCORD_WEB_HOOK:
        print("Webhook URL is missing.")
        return
    try:
        r = requests.post(DISCORD_WEB_HOOK, json={"content": message})
        r.raise_for_status()
    except Exception as e:
        print(f"Discord sending error: {e}")

def main():
    db = load_db()
    new_updates = []

    print(f"Checking {len(TARGET_ACCOUNTS)} accounts...")

    for account in TARGET_ACCOUNTS:
        last_id = db.get(account, "0")
        result = check_account(account, last_id)

        if result and "ID:" in result and "Summary:" in result:
            try:
                parts = result.split("/")
                new_id = parts[0].replace("ID:", "").strip()
                summary = parts[1].replace("Summary:", "").strip()

                if new_id.isdigit() and str(new_id) != str(last_id):
                    db[account] = new_id
                    new_updates.append(f"👤 **{account}**\n📝 {summary}\n🔗 https://x.com/i/status/{new_id}")
            except Exception as e:
                print(f"Parse error for {account}: {e}")

    if new_updates:
        msg = "🔔 **【投資家X監視：新着レポート】**\n\n" + "\n\n---\n\n".join(new_updates)
        # Discord制限対応
        if len(msg) > 1900:
            for i in range(0, len(msg), 1900):
                send_discord(msg[i:i+1900])
        else:
            send_discord(msg)
        save_db(db)
        print(f"Done! {len(new_updates)} notifications sent.")
    else:
        print("No new updates found in this run.")

if __name__ == "__main__":
    main()
