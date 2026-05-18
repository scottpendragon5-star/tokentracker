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
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")


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
                # pumpfunペアを優先して返す（BC卒業チェックに使用）
                pumpfun_pair = next((p for p in pairs if p.get("dexId") == "pumpfun"), None)
                return pumpfun_pair if pumpfun_pair else pairs[0]
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


# ── Birdeyeトークン概要取得 ─────────────────────
async def get_birdeye_overview(mint: str) -> dict:
    url = "https://public-api.birdeye.so/defi/token_overview"
    headers = {
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana",
    }
    params = {"address": mint}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=headers, params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
                return data.get("data", {}) or {}
    except Exception as e:
        print(f"Birdeyeエラー: {e}")
        return {}


# ── Devが過去に作成したPump.funトークン一覧取得 ────
async def get_dev_created_tokens(dev: str) -> list[str] | None:
    """
    Helius APIでDevの過去TX（最大100件）を取得し、
    .pump で終わるmintアドレスを返す。
    API失敗時は None を返す（チェックスキップ用）。
    """
    url = f"https://api.helius.xyz/v0/addresses/{dev}/transactions"
    params = {"api-key": HELIUS_API_KEY, "limit": 100}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                txs = await r.json()
                if not isinstance(txs, list):
                    return None
                mints = set()
                for tx in txs:
                    for transfer in tx.get("tokenTransfers", []):
                        mint_addr = transfer.get("mint", "")
                        if mint_addr.endswith("pump"):
                            mints.add(mint_addr)
                return list(mints)
    except Exception as e:
        print(f"Heliusエラー: {e}")
        return None  # 失敗 → チェックスキップ


# ── Dev過去実績スコアリング ────────────────────
async def get_dev_track_record(dev: str) -> dict | None:
    """
    戻り値:
      None → Helius取得失敗（チェックスキップ）
      dict → 取得成功（total_tokens=0 の場合も含む）
        {
          "total_tokens": int,       # 過去Pumpトークン総数
          "graduated": int,          # 卒業トークン数（MCap ≥ $69,000）
          "graduation_rate": float,  # 卒業率（直近5件ベース）
          "max_mcap": float,         # 過去最高MCap（USD）
        }
    """
    tokens = await get_dev_created_tokens(dev)
    if tokens is None:
        return None  # Helius失敗 → スキップ

    total = len(tokens)
    if total == 0:
        return {"total_tokens": 0, "graduated": 0, "graduation_rate": 0.0, "max_mcap": 0.0}

    # 直近5件のみ確認（Birdeye 30 req/min 対策）
    check_tokens = tokens[:5]
    graduated = 0
    max_mcap = 0.0

    for mint_addr in check_tokens:
        overview = await get_birdeye_overview(mint_addr)
        if overview:
            mc = float(overview.get("marketCap", 0) or 0)
            max_mcap = max(max_mcap, mc)
            if mc >= 69000:
                graduated += 1
        await asyncio.sleep(2.5)  # 30 req/min = 2.0s間隔、余裕を持って2.5s

    return {
        "total_tokens": total,
        "graduated": graduated,
        "graduation_rate": graduated / len(check_tokens),
        "max_mcap": max_mcap,
    }


# ── ヘッダー画像チェック ────────────────────────
def has_header_image(dex: dict) -> bool:
    info = dex.get("info") or {}
    return bool(info.get("header", ""))


# ── RugCheckフィルター ──────────────────────────
async def passes_rugcheck_filter(mint: str) -> tuple[bool, list[str]]:
    reasons = []
    data = await get_rugcheck_data(mint)

    if not data:
        reasons.append(f"❌ RugCheckデータ取得失敗: {mint[:8]}")
        return False, reasons

    # 危険リスク判定（dangerレベルのリスクがないこと）
    risks = data.get("risks", [])
    danger_risks = [r["name"] for r in risks if r.get("level") == "danger"]
    if danger_risks:
        reasons.append(f"❌ RugCheck危険リスク: {danger_risks}")
        return False, reasons
    reasons.append("✅ RugCheck: 危険リスクなし")

    # Top10所有率 < 30%（AMM/System Programを除いた実ホルダーで計算）
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

    # Mintオーソリティ無効化済み
    mint_authority = data.get("mintAuthority", None)
    if mint_authority is not None:
        reasons.append("❌ Mintオーソリティ有効")
        return False, reasons
    reasons.append("✅ Mintオーソリティ無効化済み")

    # Freezeオーソリティ無効化済み
    freeze_authority = data.get("freezeAuthority", None)
    if freeze_authority is not None:
        reasons.append("❌ Freezeオーソリティ有効")
        return False, reasons
    reasons.append("✅ Freezeオーソリティ無効化済み")

    return True, reasons


# ── フィルター ─────────────────────────────────
BLACKLIST_KEYWORDS = [
    "safe", "moon", "inu", "elon", "doge2", "baby", "musk",
    "together", "cock", "penis", "anal", "nigga", "shit", "troll",
]

async def passes_filter(data: dict) -> tuple[bool, list[str]]:
    reasons = []
    mint = data.get("mint", "")

    # ━━━━━━━━━━━ Phase 1：即時フィルター（Pump.funデータ + RPC） ━━━━━━━━━━━

    # 条件1: 初期購入 0.49〜9.99 SOL
    sol_amount = data.get("solAmount", 0)
    if sol_amount < 0.49:
        reasons.append(f"❌ 初期購入不足: {sol_amount:.4f} SOL")
        return False, reasons
    if sol_amount >= 10.0:
        reasons.append(f"❌ 初期購入過多: {sol_amount:.4f} SOL")
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

    # 条件5: Dev残高 ≥ 0.95 SOL（Phase 1 スクリーン）
    dev = data.get("traderPublicKey", "")
    if dev:
        dev_balance = await get_sol_balance(dev)
        if dev_balance < 0.95:
            reasons.append(f"❌ Dev残高不足（Phase 1）: {dev_balance:.2f} SOL")
            return False, reasons
        reasons.append(f"✅ Dev残高（Phase 1）: {dev_balance:.2f} SOL")

    # 条件6: Fresh Wallet検出（Dev walletのTX履歴 ≥ 10件）
    if dev:
        tx_count = await get_wallet_tx_count(dev)
        if tx_count < 10:
            reasons.append(f"❌ Fresh wallet検出: TX数={tx_count}件")
            return False, reasons
        reasons.append(f"✅ Dev wallet実績あり: {tx_count}件以上")

    # ━━━━━━━━━━━ 10分待機 ━━━━━━━━━━━

    print(f"⏳ {name} を8分後にDexScreener・RugCheckで確認...")
    await asyncio.sleep(480)
    print(f"⏱ 8分経過: {name}")

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

    # ━━━━━━━━━━━ Phase 2：DexScreener + RugCheck + 卒業可能性フィルター ━━━━━━━━━━━

    # 条件7: Dev残高 ≥ 20.0 SOL（Phase 2 強化スクリーン・卒業意欲確認）
    if dev:
        dev_balance_p2 = await get_sol_balance(dev)
        if dev_balance_p2 < 20.0:
            reasons.append(f"❌ Dev残高不足（Phase 2）: {dev_balance_p2:.2f} SOL")
            return False, reasons
        reasons.append(f"✅ Dev残高（Phase 2）: {dev_balance_p2:.2f} SOL")

    # 条件8: 現在価格 ≤ 0.000001 SOL（価格上限・まだ初期段階）
    price_native = float(dex.get("priceNative", 1))
    if price_native > 0.000001:
        reasons.append(f"❌ 価格が高すぎ: {price_native:.8f} SOL")
        return False, reasons

    # 条件9: 現在価格 ≥ 0.0000002 SOL（価格下限・600秒で一定以上の上昇を確認）
    if price_native < 0.0000002:
        reasons.append(f"❌ 価格上昇不足: {price_native:.8f} SOL")
        return False, reasons
    reasons.append(f"✅ 現在価格: {price_native:.8f} SOL")

    # 条件10: 1H買い件数 ≥ 50件（取引が死んでいるトークンを排除）
    buys_h1 = dex.get("txns", {}).get("h1", {}).get("buys", 0)
    if buys_h1 < 50:
        reasons.append(f"❌ 1H買い件数不足: {buys_h1}件")
        return False, reasons
    reasons.append(f"✅ 1H買い件数: {buys_h1}件")

    # 条件11〜14: RugCheckフィルター
    ok, rug_reasons = await passes_rugcheck_filter(mint)
    reasons.extend(rug_reasons)
    if not ok:
        return False, reasons

    # 条件15: Bonding Curve未卒業（pumpfunペアが存在すること）
    if dex.get("dexId") != "pumpfun":
        reasons.append(f"❌ Bonding Curve卒業済み (dexId={dex.get('dexId', 'unknown')})")
        return False, reasons
    reasons.append("✅ Bonding Curve上で流通中")

    # 条件16: Dev過去実績スコアリング（Helius + Birdeye）
    if dev:
        track = await get_dev_track_record(dev)
        if track is None:
            # Helius API失敗 → チェックをスキップして続行
            reasons.append("⚠️ Dev過去実績: 取得スキップ（Helius失敗）")
        elif track["total_tokens"] < 3:
            reasons.append(f"❌ Dev経験不足: 過去Pumpトークン数={track['total_tokens']}件")
            return False, reasons
        elif track["graduation_rate"] >= 0.10 or track["max_mcap"] >= 100000:
            reasons.append(
                f"✅ Dev過去実績: 卒業率={track['graduation_rate'] * 100:.0f}%"
                f" 最高MCap=${track['max_mcap']:,.0f}"
            )
        else:
            reasons.append(
                f"❌ Dev実績不足: 卒業率={track['graduation_rate'] * 100:.0f}%"
                f" 最高MCap=${track['max_mcap']:,.0f}"
            )
            return False, reasons

    # 条件17: ヘッダー画像あり（最終スクリーン）
    if not has_header_image(dex):
        reasons.append("❌ ヘッダー画像なし")
        return False, reasons
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
    await send_telegram("🤖 スナイパーBot起動しました（v8: 卒業可能性フィルター統合版）")

    while True:
        try:
            async with websockets.connect(PUMP_FUN_WS) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                print("✅ WebSocket接続完了")

                async for message in ws:
                    data = json.loads(message)
                    # 各トークンの評価を並列タスクとして起動
                    # （600秒待ちの間も新規トークンを受信し続ける）
                    asyncio.create_task(evaluate_token(data))

        except Exception as e:
            print(f"⚠️ 接続エラー: {e} → 5秒後に再接続")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(listen())
