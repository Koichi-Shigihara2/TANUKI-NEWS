import os
import json
import requests
from openai import OpenAI

# 環境変数の読み込み
XAI_API_KEY = os.getenv("XAI_API_KEY")
DISCORD_WEB_HOOK = os.getenv("DISCORD_WEB_HOOK")
DB_FILE = "processed_ids.json"

# xAIクライアントの設定
client = OpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")

# 監視対象フルリスト（計30アカウント）
TARGET_ACCOUNTS = [
    # 有名機関投資家
    "@RayDalio", "@CathieDWood", "@LizAnnSonders", "@Ritholtz", "@BobEUnlimited",
    # 有名個人投資家
    "@WarrenBuffett", "@chamath", "@naval", "@morganhousel", "@BrianFeroldi",
    # ヘッジファンド運営者
    "@BillAckman", "@Carl_C_Icahn", "@DanielSLoeb1", "@georgesoros",
    # ショートセラー
    "@HindenburgRes", "@CitronResearch", "@AlderLaneEggs", "@RealJimChanos", "@MuddyWatersRe",
    # ストラテジスト
    "@charliebilello", "@EconguyRosie", "@FundstratCap",
    # アナリスト・経済学者
    "@AswathDamodaran", "@elerianm", "@paulkrugman", "@jimcramer", "@matt_levine"
]

# 重複排除のため、RayDalioなどリスト内で重複していたものは1つにまとめています

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f:
            try:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
            except:
                return {}
    return {}

def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=4)

def check_account(account, last_id):
    """
    xAI APIの検索機能を使い、前回のIDより新しい投稿があるか確認
    """
    prompt = f"{account} の最新のX投稿をチェックしてください。前回の投稿ID({last_id})より新しい投稿があれば、その「内容の要約」と「投稿ID」を抽出してください。新着がなければ 'None' とだけ答えてください。"
    
    try:
        response = client.chat.completions.create(
            model="grok-2", # 必要に応じてgrok-3等に変更してください
            messages=[
                {"role": "system", "content": "金融アナリストとして、新着投稿があれば 'ID: 投稿ID / Summary: 要約内容' の形式で回答してください。"},
                {"role": "user", "content": prompt}
            ],
            # ここを x_search から live_search に修正
            tools=[{"type": "live_search"}] 
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error checking {account}: {e}")
        return "None"

def send_discord(message):
    if not DISCORD_WEB_HOOK:
        return
    payload = {"content": message}
    requests.post(DISCORD_WEB_HOOK, json=payload)

def main():
    db = load_db()
    new_updates = []

    print(f"Checking {len(TARGET_ACCOUNTS)} accounts...")

    for account in TARGET_ACCOUNTS:
        last_id = db.get(account, "0")
        result = check_account(account, last_id)

        if result and "None" not in result and "ID:" in result:
            try:
                # AIの回答から情報を抽出
                parts = result.split("/")
                new_id = parts[0].replace("ID:", "").strip()
                summary = parts[1].replace("Summary:", "").strip()

                # IDが更新されている場合のみ通知リストへ
                if str(new_id) != str(last_id):
                    db[account] = new_id
                    new_updates.append(f"👤 **{account}**\n📝 {summary}\n🔗 https://x.com/i/status/{new_id}")
            except:
                continue

    if new_updates:
        # Discordの1投稿の文字数制限(2000文字)を考慮し、分割して送信
        header = "🔔 **【投資家X監視：新着レポート】**\n\n"
        full_message = header + "\n\n---\n\n".join(new_updates)
        
        if len(full_message) > 1900:
            # メッセージが長すぎる場合は分割（簡易版）
            send_discord(header + "多量のアクティビティがあります。個別に確認してください。")
            # 最初の数件だけ送るなどの処理
        else:
            send_discord(full_message)
            
        save_db(db)
        print(f"Sent {len(new_updates)} updates to Discord.")
    else:
        print("No new updates found.")

if __name__ == "__main__":
    main()
