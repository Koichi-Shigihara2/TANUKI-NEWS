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
    prompt = f"""@{account} のX（旧Twitter）での**絶対に最新**の投稿を1つだけ探してください。
投稿ID（数値）と、投稿内容の簡潔な要約を以下の形式で**厳密に**返してください。
IDが見つからなければ「None」とだけ返してください。

出力形式（これ以外は出力しない）:
ID: [投稿の数値ID]
Summary: [100文字以内の要約]

最新の投稿IDが {last_id} より厳密に新しい場合のみIDを返し、それ以外は「None」としてください。"""

    try:
        response = client.chat.completions.create(
            model="grok-beta",  # 最新モデルに更新（grok-2 より推奨）
            messages=[
                {"role": "system", "content": "あなたはXの最新投稿を正確に取得できるアシスタントです。必ず指定された形式で返してください。Xのリアルタイムデータを活用してください。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,  # 安定した出力
            max_tokens=300
        )
        res_text = response.choices[0].message.content.strip()
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
                parts = result.split("\n")  # 改行で分割（形式変更対応）
                id_line = parts[0].strip()
                summary_line = parts[1].strip() if len(parts) > 1 else ""
                
                new_id = id_line.replace("ID:", "").strip()
                summary = summary_line.replace("Summary:", "").strip()

                if new_id.isdigit() and int(new_id) > int(last_id):
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
