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


# ── Dev walletトランザクション履歴件数取得 ──────────
async def get_wallet_tx_count(pubkey: str) -> int:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [pubkey, {"limit": 10}]
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(RPC_URL, json=payload) as r:
                data = await r.json()
                signatures = data.get("result", [])
                return len(signatures)
    except Exception as e:
        print(f"TX履歴取得エラー: {e}")
        return 0


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


# ── RugCheckデータ取得 ──────────────────────────
async def get_rugcheck_data(mint: str) -> dict:
    url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json()
    except Exception as e:
        print(f"RugCheckエラー: {e}")
        return {}


# ── RugCheckフィルター ──────────────────────────
async def passes_rugcheck_filter(mint: str) -> tuple[bool, list[str]]:
    reasons = []
    data = await get_rugcheck_data(mint)

    if not data:
        reasons.append(f"❌ RugCheckデータ取得失敗: {mint[:8]}")
        return False, reasons

    # 条件9: リスク判定（dangerレベルのリスクがないこと）
    risks = data.get("risks", [])
    danger_risks = [r["name"] for r in risks if r.get("level") == "danger"]
    if danger_risks:
        reasons.append(f"❌ RugCheck危険リスク: {danger_risks}")
        return False, reasons
    reasons.append("✅ RugCheck: 危険リスクなし")

    # 条件10: Top10所有率 < 30%（AMM/System Programを除いた実ホルダーで計算）
    top_holders = data.get("topHolders", [])
    if not top_holders:
        reasons.append("❌ 所有権データなし（topHolders未取得）")
        return False, reasons
    known = data.get("knownAccounts", {})
    amm_addrs = {addr for addr, info in known.items() if info.get("type") == "AMM"}
    real_holders = [
        h for h in top_holders
        if h.get("owner", "") not in amm_addrs
        and not h.get("owner", "").startswith("11111111111")
    ]
    if not real_holders:
        reasons.append("❌ 実ホルダーなし（Bonding Curveのみ保有）")
        return False, reasons
    if len(real_holders) < 5:
        reasons.append(f"❌ 実ホルダー不足: {len(real_holders)}人")
        return False, reasons
    top10_pct = sum(h.get("pct", 0) for h in real_holders[:10])
    if top10_pct >= 30:
        reasons.append(f"❌ 所有権集中: 実ホルダーTop10={top10_pct:.1f}%")
        return False, reasons
    reasons.append(f"✅ 所有権分散: 実ホルダーTop10={top10_pct:.1f}%")

    # 条件11: Mintオーソリティ無効化済み
    mint_authority = data.get("mintAuthority", None)
    if mint_authority is not None:
        reasons.append("❌ Mintオーソリティ有効")
        return False, reasons
    reasons.append("✅ Mintオーソリティ無効化済み")

    # 条件12: Freezeオーソリティ無効化済み
    freeze_authority = data.get("freezeAuthority", None)
    if freeze_authority is not None:
        reasons.append("❌ Freezeオーソリティ有効")
        return False, reasons
    reasons.append("✅ Freezeオーソリティ無効化済み")

    return True, reasons


# ── フィルター ─────────────────────────────────
BLACKLIST_KEYWORDS = ["safe", "moon", "inu", "elon", "doge2", "baby", "musk", "together", "cock", "penis", "anal", "nigga", "shit"]

async def passes_filter(data: dict) -> tuple[bool, list[str]]:
    reasons = []
    mint = data.get("mint", "")

    # フェーズ1: 即時フィルター（Pump.funデータ + RPC）

    # 条件1: 初期購入 ≥ 0.49 SOL（solAmountがSOL実額、initialBuyはトークン数量）
    sol_amount = data.get("solAmount", 0)
    if sol_amount < 0.49:
        reasons.append(f"❌ 初期購入不足: {sol_amount:.4f} SOL")
        return False, reasons
    reasons.append(f"✅ 初期購入: {sol_amount:.4f} SOL")

    # 条件2: 名前・シンボルあり、かつ名前が3文字以上
    name = data.get("name", "")
    symbol = data.get("symbol", "")
    if not (name and symbol):
        reasons.append("❌ 名前・シンボルなし")
        return False, reasons
    if len(name) <= 2:
        reasons.append(f"❌ 名前が短すぎ: '{name}'")
        return False, reasons
    reasons.append("✅ 名前・シンボルあり")

    # 条件3: 名前ブラックリスト
    matched = [kw for kw in BLACKLIST_KEYWORDS if kw in name.lower()]
    if matched:
        reasons.append(f"❌ NGワード: {matched}")
        return False, reasons
    reasons.append("✅ ブラックリスト通過")

    # 条件4: メタデータあり
    uri = data.get("uri", "")
    if not uri.startswith("https://"):
        reasons.append("❌ メタデータなし")
        return False, reasons
    reasons.append("✅ メタデータあり")

    # 条件5: Dev残高 ≥ 0.95 SOL
    dev = data.get("traderPublicKey", "")
    if dev:
        dev_balance = await get_sol_balance(dev)
        if dev_balance < 0.95:
            reasons.append(f"❌ Dev残高不足: {dev_balance:.2f} SOL")
            return False, reasons
        reasons.append(f"✅ Dev残高: {dev_balance:.2f} SOL")

    # 条件6: Fresh Wallet検出（Dev walletのTX履歴 ≥ 10件）
    if dev:
        tx_count = await get_wallet_tx_count(dev)
        if tx_count < 10:
            reasons.append(f"❌ Fresh wallet検出: TX数={tx_count}件")
            return False, reasons
        reasons.append(f"✅ Dev wallet実績あり: {tx_count}件以上")

    # フェーズ2: DexScreener＋RugCheckフィルター（600秒後）

    print(f"⏳ {name} を10分後にDexScreener・RugCheckで確認...")
    await asyncio.sleep(600)
    print(f"⏱ 10分経過: {name}")

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
        reasons.append("❌ DexScreener未登録")
        return False, reasons

    # 条件7: 現在価格 ≤ 0.000001 SOL（割安・初期段階のトークンのみ対象）
    price_native = float(dex.get("priceNative", 1))
    if price_native > 0.000001:
        reasons.append(f"❌ 価格が高すぎ: {price_native:.8f} SOL")
        return False, reasons
    reasons.append(f"✅ 現在価格: {price_native:.8f} SOL")

    # 条件8〜11: RugCheckフィルター
    ok, rug_reasons = await passes_rugcheck_filter(mint)
    reasons.extend(rug_reasons)
    if not ok:
        return False, reasons

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
        f"📋 CA: <code>{mint}</code>\n"
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
            mint = data.get("mint", "N/A")
            print(f"❌ フィルター落ち: {name} | CA: {mint} | {reasons}")

    except Exception as e:
        print(f"💥 タスクエラー ({name}): {e}")


# ── メインループ ───────────────────────────────
async def listen():
    print("👂 Pump.fun監視開始...")
    await send_telegram("🤖 スナイパーBot起動しました（v5: RugCheck統合・ヘッダー画像条件なし版）")

    while True:
        try:
            async with websockets.connect(PUMP_FUN_WS) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                print("✅ WebSocket接続完了")

                async for message in ws:
                    data = json.loads(message)
                    asyncio.create_task(evaluate_token(data))

        except Exception as e:
            print(f"⚠️ 接続エラー: {e} → 5秒後に再接続")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(listen())
