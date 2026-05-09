import asyncio
import websockets
import aiohttp
import json
import os
from dotenv import load_dotenv

load_dotenv()

PUMP_FUN_WS = "wss://pumpportal.fun/api/data"
TG_TOKEN = os.getenv("TELEGRAM_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
RPC_URL = os.getenv("RPC_URL")


# ── Dev wallet残高取得 ──────────────────────────
async def get_sol_balance(pubkey: str) -> float:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [pubkey]
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(RPC_URL, json=payload) as r:
                data = await r.json()
                lamports = data.get("result", {}).get("value", 0)
                return lamports / 1e9
    except Exception as e:
        print(f"残高取得エラー: {e}")
        return 0.0


# ── DexScreenerデータ取得 ───────────────────────
async def get_dexscreener_data(mint: str) -> dict:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as r:
                data = await r.json()
                pairs = data.get("pairs", [])
                if not pairs:
                    return {}
                return pairs[0]
    except Exception as e:
        print(f"DexScreenerエラー: {e}")
        return {}


# ── ヘッダー画像チェック ────────────────────────
def has_header_image(dex: dict) -> bool:
    return bool(dex.get("info", {}).get("header", ""))


# ── フィルター ─────────────────────────────────
BLACKLIST_KEYWORDS = ["safe", "moon", "inu", "elon", "doge2", "baby"]

async def passes_filter(data: dict) -> tuple[bool, list[str]]:
    reasons = []
    mint = data.get("mint", "")

    # フェーズ1: 即時フィルター（Pump.funデータ）

    # 条件1: 初期購入 ≥ 1.00 SOL（solAmountがSOL実額、initialBuyはトークン数量）
    sol_amount = data.get("solAmount", 0)
    if sol_amount < 1.00:
        return False, []
    reasons.append(f"✅ 初期購入: {sol_amount:.4f} SOL")

    # 条件2: 名前・シンボルあり
    name = data.get("name", "")
    symbol = data.get("symbol", "")
    if not (name and symbol):
        return False, []
    reasons.append("✅ 名前・シンボルあり")

    # 条件3: 名前ブラックリスト
    if any(kw in name.lower() for kw in BLACKLIST_KEYWORDS):
        return False, []
    reasons.append("✅ ブラックリスト通過")

    # 条件4: メタデータあり
    uri = data.get("uri", "")
    if not uri.startswith("https://"):
        return False, []
    reasons.append("✅ メタデータあり")

    # 条件5: Dev残高 ≥ 3 SOL
    dev = data.get("traderPublicKey", "")
    if dev:
        dev_balance = await get_sol_balance(dev)
        if dev_balance < 3.0:
            print(f"❌ {name} Dev残高不足: {dev_balance:.2f} SOL")
            return False, []
        reasons.append(f"✅ Dev残高: {dev_balance:.2f} SOL")

    # フェーズ2: DexScreenerフィルター（300秒後）

    print(f"⏳ {name} を300秒後にDexScreenerで確認...")
    await asyncio.sleep(300)
    print(f"⏱ 300秒経過: {name}")

    # DexScreenerへの反映遅延に備えて最大3回リトライ（30秒間隔）
    dex = {}
    for attempt in range(3):
        dex = await get_dexscreener_data(mint)
        if dex:
            break
        if attempt < 2:
            print(f"⏳ {name} DexScreener未登録、30秒後に再試行 ({attempt + 1}/3)...")
            await asyncio.sleep(30)

    if not dex:
        print(f"❌ {name} DexScreenerに登録されていません")
        return False, []

    # 条件6: 1H総取引件数 ≥ 50（MAKERS数の代替）
    txns_h1 = dex.get("txns", {}).get("h1", {})
    buys = txns_h1.get("buys", 0)
    sells = txns_h1.get("sells", 0)
    total_txns = buys + sells
    if total_txns < 50:
        print(f"❌ {name} 取引件数不足: {total_txns}件")
        return False, []
    reasons.append(f"✅ 1H取引: {total_txns}件")

    # 条件7: ヘッダー画像あり
    if not has_header_image(dex):
        print(f"❌ {name} ヘッダー画像なし")
        return False, []
    reasons.append("✅ ヘッダー画像あり")

    return True, reasons


# ── Telegram送信 ──────────────────────────────
async def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=payload)
    except Exception as e:
        print(f"Telegram送信エラー: {e}")


# ── 通知メッセージ作成 ──────────────────────────
def build_message(data: dict, reasons: list[str]) -> str:
    mint = data.get("mint", "N/A")
    name = data.get("name", "Unknown")
    symbol = data.get("symbol", "???")
    dev = data.get("traderPublicKey", "N/A")
    sol_amount = data.get("solAmount", 0)

    pump_url = f"https://pump.fun/{mint}"
    dex_url = f"https://dexscreener.com/solana/{mint}"
    reasons_text = "\n".join(reasons)

    return (
        f"🚨 <b>新規トークン検出</b>\n\n"
        f"<b>{name}</b> (${symbol})\n\n"
        f"📋 Mint: <code>{mint}</code>\n"
        f"👤 Dev: <code>{dev[:8]}...{dev[-4:]}</code>\n"
        f"💰 初期購入: {sol_amount:.4f} SOL\n\n"
        f"<b>通過条件:</b>\n{reasons_text}\n\n"
        f"🔗 <a href='{pump_url}'>Pump.fun</a>　"
        f"<a href='{dex_url}'>DexScreener</a>"
    )


# ── トークン評価タスク（並列処理） ────────────────
async def evaluate_token(data: dict):
    name = data.get("name", "???")
    try:
        print(f"🚀 評価開始: {name} (${data.get('symbol', '???')}) | {data.get('solAmount', 0):.4f} SOL")

        ok, reasons = await passes_filter(data)

        if ok:
            msg = build_message(data, reasons)
            await send_telegram(msg)
            print(f"✅ 通知送信: {name}")
        else:
            print(f"❌ フィルター落ち: {name} | {reasons}")

    except Exception as e:
        print(f"💥 タスクエラー ({name}): {e}")


# ── メインループ ───────────────────────────────
async def listen():
    print("👂 Pump.fun監視開始...")
    await send_telegram("🤖 スナイパーBot起動しました（v2: DexScreener統合版）")

    while True:
        try:
            async with websockets.connect(PUMP_FUN_WS) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                print("✅ WebSocket接続完了")

                async for message in ws:
                    data = json.loads(message)
                    # 各トークンの評価を並列タスクとして起動
                    # （300秒待ちの間も新規トークンを受信し続ける）
                    asyncio.create_task(evaluate_token(data))

        except Exception as e:
            print(f"⚠️ 接続エラー: {e} → 5秒後に再接続")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(listen())
