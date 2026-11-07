# -*- coding: utf-8 -*-
"""
Kripto Vadeli İşlem Sinyal Test Sistemi
- TradingView webhook sinyalleriyle sanal (paper) işlem testi
- İki bağımsız test modu:
  A) Sinyal takipli: BUY/SELL ile açılır, indikatörün TP/SL/CLOSE sinyalleriyle kapanır
  B) Yüzdeli TP/SL: BUY/SELL ile açılır, sizin belirlediğiniz yüzdeyle TP/SL otomatik uygulanır
- Komisyon, kayma (slippage) ve gerçek funding oranları hesaba katılır
- Fiyat verisi: OKX -> Gate -> MEXC otomatik yedekleme
"""
import os
import json
import time
import csv
import io
import secrets
import threading
import traceback
from datetime import datetime, timezone

import requests
from flask import (Flask, request, jsonify, session, redirect,
                   render_template_string, Response)

try:
    import redis as redis_mod
except Exception:
    redis_mod = None

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(24))
APP_PASSWORD = os.environ.get("APP_PASSWORD", "kripto2026")
REDIS_URL = os.environ.get("REDIS_URL", "")
STATE_KEY = "kripto_sinyal_state_v1"

# ----------------------------------------------------------------------------
# Durum (state) yönetimi
# ----------------------------------------------------------------------------

DEFAULT_SETTINGS = {
    "webhook_secret": "",
    "exchange": "auto",            # auto | okx | gate | mexc
    "funding_enabled": True,
    "A": {
        "label": "Sinyal takipli (indikatör TP/SL)",
        "start_balance": 1000.0,
        "leverage": 10,
        "position_pct": 10.0,
        "taker_fee": 0.05,
        "slippage": 0.02,
        "max_open": 10,
        "reverse_on_signal": True,
        "cooldown_min": 0,
    },
    "B": {
        "label": "Yüzdeli TP/SL (otomatik)",
        "start_balance": 1000.0,
        "leverage": 10,
        "position_pct": 10.0,
        "taker_fee": 0.05,
        "slippage": 0.02,
        "max_open": 10,
        "reverse_on_signal": True,
        "cooldown_min": 0,
        "tp_pct": 1.0,
        "sl_pct": 0.5,
        "use_indicator_exits": False,
    },
}


def fresh_state():
    st = {
        "settings": json.loads(json.dumps(DEFAULT_SETTINGS)),
        "signals": [],          # son gelen webhook sinyalleri
        "modes": {
            "A": {"balance": DEFAULT_SETTINGS["A"]["start_balance"],
                  "peak": DEFAULT_SETTINGS["A"]["start_balance"],
                  "max_dd": 0.0, "positions": {}, "trades": [],
                  "last_close": {}},
            "B": {"balance": DEFAULT_SETTINGS["B"]["start_balance"],
                  "peak": DEFAULT_SETTINGS["B"]["start_balance"],
                  "max_dd": 0.0, "positions": {}, "trades": [],
                  "last_close": {}},
        },
        "prices": {},
        "last_scan_ms": 0,
        "created": now_iso(),
    }
    if not st["settings"]["webhook_secret"]:
        st["settings"]["webhook_secret"] = secrets.token_hex(8)
    return st


LOCK = threading.RLock()
REDIS = None
REDIS_OK = False


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_ms():
    return int(time.time() * 1000)


STATE = fresh_state()


def init_redis():
    global REDIS, REDIS_OK
    if not REDIS_URL or redis_mod is None:
        return
    try:
        REDIS = redis_mod.Redis.from_url(REDIS_URL, socket_timeout=5)
        REDIS.ping()
        data = REDIS.get(STATE_KEY)
        if data:
            loaded = json.loads(data)
            if isinstance(loaded, dict) and "modes" in loaded:
                # ayar varsayılanları eksikse tamamla
                for k, v in DEFAULT_SETTINGS.items():
                    if k not in loaded["settings"]:
                        loaded["settings"][k] = v
                for m in ("A", "B"):
                    for k, v in DEFAULT_SETTINGS[m].items():
                        if k not in loaded["settings"].get(m, {}):
                            loaded["settings"].setdefault(m, {})[k] = v
                STATE.clear()
                STATE.update(loaded)
        REDIS_OK = True
    except Exception as e:
        print("Redis baglantisi kurulamadi, bellek ici kayit kullanilacak:", e)


def save_state():
    if not REDIS_OK or REDIS is None:
        return
    try:
        REDIS.set(STATE_KEY, json.dumps(STATE, ensure_ascii=False))
    except Exception as e:
        print("Redis kaydi basarisiz:", e)


# ----------------------------------------------------------------------------
# Piyasa verisi katmani (OKX -> Gate -> MEXC)
# ----------------------------------------------------------------------------

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) kripto-sinyal-test"}
HTTP = requests.Session()
HTTP.headers.update(UA)


def norm_symbol(raw):
    """BTC-USDT-SWAP / BTC_USDT / btcusdt -> BTCUSDT"""
    if not raw:
        return ""
    s = str(raw).strip().upper()
    toks = [t for t in s.replace("-", "_").split("_") if t]
    if len(toks) >= 3 and toks[-1] in ("SWAP", "PERP"):
        toks = toks[:-1]
    if len(toks) >= 2 and toks[-1] in ("SWAP", "PERP"):
        toks = toks[:-1]
    s = "".join(toks)
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    return s


class Okx:
    name = "OKX"

    def inst(self, sym):
        return sym[:-4] + "-USDT-SWAP" if sym.endswith("USDT") else None

    def instruments(self):
        r = HTTP.get("https://www.okx.com/api/v5/public/instruments",
                     params={"instType": "SWAP"}, timeout=10)
        d = r.json()
        out = set()
        for it in d.get("data", []):
            if it.get("settleCcy") == "USDT":
                out.add(norm_symbol(it["instId"]))
        return out

    def tickers(self):
        r = HTTP.get("https://www.okx.com/api/v5/market/tickers",
                     params={"instType": "SWAP"}, timeout=10)
        out = {}
        for t in r.json().get("data", []):
            if t.get("instId", "").endswith("-USDT-SWAP"):
                try:
                    out[norm_symbol(t["instId"])] = float(t["last"])
                except Exception:
                    pass
        return out

    def candles(self, sym, since_ms, until_ms=None):
        inst = self.inst(sym)
        if not inst:
            return None
        until = until_ms or (now_ms() + 60000)
        out, after = [], until + 60000
        for _ in range(20):
            r = HTTP.get("https://www.okx.com/api/v5/market/candles",
                         params={"instId": inst, "bar": "1m",
                                 "limit": "300", "after": str(after)},
                         timeout=10)
            data = r.json().get("data", [])
            if not data:
                break
            for c in data:
                ts = int(c[0])
                if since_ms <= ts <= until:
                    out.append((ts, float(c[1]), float(c[2]),
                                float(c[3]), float(c[4])))
            oldest = min(int(c[0]) for c in data)
            if oldest <= since_ms or len(data) < 300:
                break
            after = oldest
        out.sort(key=lambda x: x[0])
        return out[-1500:]

    def funding(self, sym, since_ms, until_ms):
        inst = self.inst(sym)
        if not inst:
            return None
        out, before = [], ""
        for _ in range(6):
            params = {"instId": inst, "limit": "100"}
            if before:
                params["before"] = before
            r = HTTP.get("https://www.okx.com/api/v5/public/funding-rate-history",
                         params=params, timeout=10)
            data = r.json().get("data", [])
            if not data:
                break
            for f in data:
                ts = int(f["fundingTime"])
                if since_ms <= ts <= until_ms:
                    out.append((ts, float(f["fundingRate"])))
            oldest = min(int(f["fundingTime"]) for f in data)
            if oldest <= since_ms or len(data) < 100:
                break
            before = str(oldest)
        return sorted(out)


class Gate:
    name = "Gate"

    def inst(self, sym):
        return sym[:-4] + "_USDT" if sym.endswith("USDT") else None

    def instruments(self):
        r = HTTP.get("https://api.gateio.ws/api/v4/futures/usdt/contracts", timeout=10)
        return {norm_symbol(c["name"]) for c in r.json()
                if c.get("in_delisting") is not True}

    def tickers(self):
        r = HTTP.get("https://api.gateio.ws/api/v4/futures/usdt/tickers", timeout=10)
        out = {}
        for t in r.json():
            try:
                out[norm_symbol(t["contract"])] = float(t["last"])
            except Exception:
                pass
        return out

    def candles(self, sym, since_ms, until_ms=None):
        c = self.inst(sym)
        if not c:
            return None
        out = []
        to = (until_ms // 1000) if until_ms else int(time.time())
        frm = max(since_ms // 1000, to - 2000)
        while frm < to and len(out) < 1500:
            r = HTTP.get("https://api.gateio.ws/api/v4/futures/usdt/candlesticks",
                         params={"contract": c, "interval": "1m",
                                 "from": frm, "to": min(frm + 900, to)},
                         timeout=10)
            data = r.json()
            for k in data:
                if since_ms <= k["t"] * 1000 <= to * 1000:
                    out.append((k["t"] * 1000, float(k["o"]), float(k["h"]),
                                float(k["l"]), float(k["c"])))
            frm += 900
        out.sort(key=lambda x: x[0])
        return out[-1500:]

    def funding(self, sym, since_ms, until_ms):
        c = self.inst(sym)
        if not c:
            return None
        out = []
        to = int(until_ms / 1000)
        frm = max(int(since_ms / 1000) - 7200, to - 86400 * 30)
        while frm < to:
            r = HTTP.get("https://api.gateio.ws/api/v4/futures/usdt/funding_rate",
                         params={"contract": c, "limit": 100,
                                 "from": frm, "to": min(frm + 86400 * 15, to)},
                         timeout=10)
            data = r.json()
            if not data:
                break
            for f in data:
                ts = f["t"] * 1000
                if since_ms <= ts <= until_ms:
                    out.append((ts, float(f["r"])))
            last = max(f["t"] for f in data)
            if last <= frm:
                break
            frm = last + 1
        return sorted(out)


class Mexc:
    name = "MEXC"

    def inst(self, sym):
        return sym.replace("USDT", "_USDT") if sym.endswith("USDT") else None

    def instruments(self):
        r = HTTP.get("https://contract.mexc.com/api/v1/contract/detail", timeout=10)
        out = set()
        for c in r.json().get("data", []):
            if c.get("quoteCoin") == "USDT":
                out.add(norm_symbol(c["symbol"]))
        return out

    def tickers(self):
        r = HTTP.get("https://contract.mexc.com/api/v1/contract/ticker", timeout=10)
        out = {}
        for t in r.json().get("data", []):
            try:
                out[norm_symbol(t["symbol"])] = float(t["lastPrice"])
            except Exception:
                pass
        return out

    def candles(self, sym, since_ms, until_ms=None):
        c = self.inst(sym)
        if not c:
            return None
        out = []
        end = (until_ms // 1000) if until_ms else (now_ms() // 1000)
        start = max(since_ms, end * 1000 - 2000 * 60000)
        while start < end * 1000 and len(out) < 1500:
            r = HTTP.get("https://contract.mexc.com/api/v1/contract/kline/"
                         + c, params={"interval": "Min1", "start": start // 1000,
                                      "end": min(start // 1000 + 800 * 60, end)},
                         timeout=10)
            data = r.json().get("data", [])
            if not data:
                break
            for k in data:
                ts = int(k["time"])
                if since_ms <= ts <= end * 1000:
                    out.append((ts, float(k["open"]), float(k["high"]),
                                float(k["low"]), float(k["close"])))
            start = (int(data[-1]["time"]) + 60000)
        out.sort(key=lambda x: x[0])
        return out[-1500:]

    def funding(self, sym, since_ms, until_ms):
        # MEXC funding geçmisi sayfalaması karmaşık; yakın geçmiş yeterli
        c = self.inst(sym)
        if not c:
            return None
        out = []
        page = 1
        while page <= 10:
            r = HTTP.get("https://contract.mexc.com/api/v1/contract/funding_rate",
                         params={"symbol": c, "page_size": 50, "page": page},
                         timeout=10)
            data = r.json().get("data", {}).get("resultList", [])
            if not data:
                break
            for f in data:
                ts = int(f["settleTime"])
                if since_ms <= ts <= until_ms:
                    out.append((ts, float(f["fundingRate"])))
            if int(data[-1]["settleTime"]) <= since_ms or len(data) < 50:
                break
            page += 1
        return sorted(out)


SOURCES = {"okx": Okx(), "gate": Gate(), "mexc": Mexc()}
SOURCE_ORDER = ["okx", "gate", "mexc"]
_instruments_cache = {"ts": 0, "set": set()}
_live_source = {"name": "-", "ts": 0}


def source_list():
    ex = STATE["settings"].get("exchange", "auto")
    if ex in SOURCES:
        return [SOURCES[ex]]
    return [SOURCES[k] for k in SOURCE_ORDER]


def instruments():
    if now_ms() - _instruments_cache["ts"] < 3600_000 and _instruments_cache["set"]:
        return _instruments_cache["set"]
    for src in source_list():
        try:
            s = src.instruments()
            if s:
                _instruments_cache["ts"] = now_ms()
                _instruments_cache["set"] = s
                return s
        except Exception:
            continue
    return _instruments_cache["set"]


def tickers():
    for src in source_list():
        try:
            t = src.tickers()
            if t:
                _live_source.update({"name": src.name, "ts": now_ms()})
                return t
        except Exception:
            continue
    return {}


def candles(sym, since_ms, until_ms=None):
    """Ilk basarili (None donmeyen) kaynagin sonucunu dondurur.
    Bos liste gecerli bir sonuc olabilir (o aralikta mum yok)."""
    for src in source_list():
        try:
            c = src.candles(sym, since_ms, until_ms)
            if c is not None:
                _live_source.update({"name": src.name, "ts": now_ms()})
                return c
        except Exception as e:
            print("candles hata", src.name, e)
    return []


def funding_history(sym, since_ms, until_ms):
    """Ilk destekleyen kaynagin sonucu. Bos liste = aralikta olay yok."""
    for src in source_list():
        try:
            f = src.funding(sym, since_ms, until_ms)
            if f is not None:
                return f
        except Exception:
            continue
    return []


def price_of(sym):
    t = tickers()
    return t.get(sym)


# ----------------------------------------------------------------------------
# Islemler (pozisyon ac / kapat, PnL hesabi)
# ----------------------------------------------------------------------------

def mode_cfg(mode):
    return STATE["settings"][mode]


def open_position(mode, symbol, side, price):
    cfg = mode_cfg(mode)
    m = STATE["modes"][mode]
    bal = m["balance"]
    if bal <= 0:
        return None, "Bakiye yetersiz"
    margin = bal * cfg["position_pct"] / 100.0
    if margin <= 0:
        return None, "Pozisyon buyuklugu 0"
    lev = max(1, int(cfg["leverage"]))
    slip = float(cfg["slippage"]) / 100.0
    fill = price * (1 + slip) if side == "long" else price * (1 - slip)
    notional = margin * lev
    qty = notional / fill
    entry_fee = notional * cfg["taker_fee"] / 100.0
    pos = {
        "id": f"{mode}-{symbol}-{now_ms()}-{secrets.token_hex(3)}",
        "symbol": symbol, "side": side,
        "entry_time": now_iso(), "entry_time_ms": now_ms(),
        "entry_price": price, "fill_price": round(fill, 10),
        "qty": qty, "notional": notional, "margin": margin,
        "leverage": lev, "entry_fee": entry_fee,
        "tp": None, "sl": None, "last_scan_ms": now_ms(),
        "mode": mode,
    }
    if mode == "B":
        tp_pct = float(cfg["tp_pct"]) / 100.0
        sl_pct = float(cfg["sl_pct"]) / 100.0
        if side == "long":
            pos["tp"] = fill * (1 + tp_pct)
            pos["sl"] = fill * (1 - sl_pct)
        else:
            pos["tp"] = fill * (1 - tp_pct)
            pos["sl"] = fill * (1 + sl_pct)
    m["positions"][symbol] = pos
    return pos, None


def funding_cost_for(pos, exit_ms):
    """Pozisyon suresince odenen/alinan funding (USDT). Negatif = gelir."""
    if not STATE["settings"].get("funding_enabled", True):
        return 0.0, False
    events = funding_history(pos["symbol"], pos["entry_time_ms"], exit_ms)
    if not events:
        return 0.0, False
    cost, approx = 0.0, False
    for ts, rate in events:
        px = None
        try:
            cs = candles(pos["symbol"], ts - 120000, ts + 60000)
            for c in cs:
                if c[0] <= ts <= c[0] + 60000:
                    px = c[4]
                    break
        except Exception:
            px = None
        if px is None:
            px = pos["fill_price"]
            approx = True
        notional_at = pos["qty"] * px
        if pos["side"] == "long":
            cost += notional_at * rate       # pozitif oran: long odar
        else:
            cost -= notional_at * rate       # pozitif oran: short alir
    return cost, approx


def close_position(mode, symbol, price, reason):
    m = STATE["modes"][mode]
    pos = m["positions"].get(symbol)
    if not pos:
        return None
    cfg = mode_cfg(mode)
    slip = float(cfg["slippage"]) / 100.0
    if pos["side"] == "long":
        fill = price * (1 - slip)
    else:
        fill = price * (1 + slip)
    qty = pos["qty"]
    if pos["side"] == "long":
        gross = (fill - pos["fill_price"]) * qty
    else:
        gross = (pos["fill_price"] - fill) * qty
    exit_notional = qty * fill
    exit_fee = exit_notional * cfg["taker_fee"] / 100.0
    fund, fund_approx = funding_cost_for(pos, now_ms())
    net = gross - pos["entry_fee"] - exit_fee - fund
    m["balance"] += net
    m["peak"] = max(m["peak"], m["balance"])
    dd = (m["peak"] - m["balance"]) / m["peak"] * 100 if m["peak"] > 0 else 0
    m["max_dd"] = max(m["max_dd"], dd)
    trade = dict(pos)
    trade.update({
        "exit_time": now_iso(), "exit_price": price, "fill_exit": round(fill, 10),
        "exit_reason": reason, "gross": round(gross, 6),
        "fees": round(pos["entry_fee"] + exit_fee, 6),
        "funding": round(fund, 6), "funding_approx": fund_approx,
        "net": round(net, 6), "balance_after": round(m["balance"], 6),
    })
    m["trades"].append(trade)
    if len(m["trades"]) > 5000:
        m["trades"] = m["trades"][-5000:]
    m["last_close"][symbol] = now_ms()
    del m["positions"][symbol]
    return trade


def cooldown_ok(mode, symbol):
    cd = int(mode_cfg(mode).get("cooldown_min", 0))
    if cd <= 0:
        return True
    last = STATE["modes"][mode]["last_close"].get(symbol, 0)
    return now_ms() - last >= cd * 60000


def apply_signal(mode, symbol, action, price):
    """Bir mod icin sinyali uygula; dondur: (mesaj listesi)"""
    msgs = []
    m = STATE["modes"][mode]
    cfg = mode_cfg(mode)
    pos = m["positions"].get(symbol)
    opposite = {"long": "short", "short": "long"}[{"BUY": "long", "SELL": "short"}.get(action, "")] if action in ("BUY", "SELL") else None

    if action in ("BUY", "SELL"):
        side = "long" if action == "BUY" else "short"
        if pos and pos["side"] == side:
            msgs.append(f"{mode}: {symbol} {side} pozisyon zaten acik, yeniden acilmadi")
            return msgs
        if pos and pos["side"] != side:
            close_position(mode, symbol, price, "Ters sinyal")
            msgs.append(f"{mode}: {symbol} ters sinyalle kapatildi, cevriliyor")
            pos = None
        if not cfg.get("reverse_on_signal", True) and STATE["modes"][mode]["last_close"].get(symbol, 0) and \
           now_ms() - STATE["modes"][mode]["last_close"].get(symbol, 0) < 1000:
            # ters sinyalde cevrme kapaliysa sadece kapat
            msgs.append(f"{mode}: {symbol} cevrme kapali, sadece kapatildi")
            return msgs
        if len(m["positions"]) >= int(cfg["max_open"]):
            msgs.append(f"{mode}: maksimum acik pozisyon sayisina ulasildi, {symbol} acilmadi")
            return msgs
        if not cooldown_ok(mode, symbol):
            msgs.append(f"{mode}: {symbol} bekleme suresi dolmadi, acilmadi")
            return msgs
        p, err = open_position(mode, symbol, side, price)
        if err:
            msgs.append(f"{mode}: {symbol} acilamadi - {err}")
        else:
            extra = ""
            if mode == "B" and p["tp"] and p["sl"]:
                extra = f" TP={p['tp']:.6g} SL={p['sl']:.6g}"
            msgs.append(f"{mode}: {symbol} {side} acildi @ {p['fill_price']:.6g}{extra}")
    elif action in ("TP", "SL", "CLOSE"):
        if not pos:
            return msgs
        if mode == "B" and not cfg.get("use_indicator_exits", False) and action != "CLOSE":
            return msgs  # B modunda indikator TP/SL yoksayilir
        reason = {"TP": "Sinyal TP", "SL": "Sinyal SL", "CLOSE": "Sinyal CLOSE"}[action]
        close_position(mode, symbol, price, reason)
        msgs.append(f"{mode}: {symbol} {reason} ile kapatildi @ {price:.6g}")
    return msgs


def process_signal(symbol, action, price, raw=""):
    symbol = norm_symbol(symbol)
    msgs = []
    with LOCK:
        if price is None:
            price = price_of(symbol)
        if price is None or price <= 0:
            STATE["signals"].insert(0, {
                "time": now_iso(), "symbol": symbol, "action": action,
                "price": None, "ok": False, "note": "Fiyat alinamadi"})
            STATE["signals"] = STATE["signals"][:500]
            save_state()
            return {"ok": False, "error": "fiyat alinamadi"}
        insts = instruments()
        known = symbol in insts if insts else True
        for mode in ("A", "B"):
            msgs += apply_signal(mode, symbol, action, price)
        STATE["signals"].insert(0, {
            "time": now_iso(), "symbol": symbol, "action": action,
            "price": price, "ok": True,
            "note": "" if known else "Parite vadeli listesinde bulunamadi"})
        STATE["signals"] = STATE["signals"][:500]
        save_state()
    return {"ok": True, "messages": msgs, "known": known}


# ----------------------------------------------------------------------------
# Arka plan izleyici (B modu TP/SL takibi + guncel fiyatlar)
# ----------------------------------------------------------------------------

def scan_positions():
    closed = []
    with LOCK:
        syms = set()
        for mode in ("A", "B"):
            syms.update(STATE["modes"][mode]["positions"].keys())
        for sym in syms:
            for mode in ("A", "B"):
                pos = STATE["modes"][mode]["positions"].get(sym)
                if not pos:
                    continue
                try:
                    cs = candles(sym, max(pos["last_scan_ms"],
                                          pos["entry_time_ms"]))
                except Exception:
                    cs = []
                if not cs:
                    continue
                pos["last_scan_ms"] = cs[-1][0] + 60000
                if mode != "B" or not pos.get("tp") or not pos.get("sl"):
                    continue
                for ts, o, h, l, c in cs:
                    if ts < pos["entry_time_ms"]:
                        continue
                    hit_tp = (pos["side"] == "long" and h >= pos["tp"]) or \
                             (pos["side"] == "short" and l <= pos["tp"])
                    hit_sl = (pos["side"] == "long" and l <= pos["sl"]) or \
                             (pos["side"] == "short" and h >= pos["sl"])
                    if hit_tp and hit_sl:
                        # ayni mumda ikisi de degdi: kotumser senaryo -> SL
                        closed.append(close_position(mode, sym, pos["sl"], "SL (otomatik)"))
                        break
                    if hit_sl:
                        closed.append(close_position(mode, sym, pos["sl"], "SL (otomatik)"))
                        break
                    if hit_tp:
                        closed.append(close_position(mode, sym, pos["tp"], "TP (otomatik)"))
                        break
        if closed:
            save_state()


def monitor_loop():
    while True:
        try:
            scan_positions()
        except Exception:
            traceback.print_exc()
        time.sleep(25)


def refresh_prices():
    try:
        with LOCK:
            STATE["prices"] = tickers()
    except Exception:
        pass


def price_loop():
    while True:
        refresh_prices()
        time.sleep(20)


# ----------------------------------------------------------------------------
# Webhook
# ----------------------------------------------------------------------------

def parse_payload():
    data = {}
    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = request.form.to_dict()
        if not data:
            try:
                data = json.loads(request.get_data(as_text=True))
            except Exception:
                data = {}
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            data = {"action": data}
    return data if isinstance(data, dict) else {}


def classify_action(*vals):
    for v in vals:
        if not v:
            continue
        s = str(v).strip().upper()
        if s in ("BUY", "LONG", "AL", "ALIS"):
            return "BUY"
        if s in ("SELL", "SHORT", "SAT", "SATIS"):
            return "SELL"
        if s in ("TP", "TAKEPROFIT", "TAKE_PROFIT", "TP1"):
            return "TP"
        if s in ("SL", "STOPLOSS", "STOP_LOSS", "STOP"):
            return "SL"
        if s in ("CLOSE", "EXIT", "FLAT", "KAPAT"):
            return "CLOSE"
        if "buy" in s.lower() or "long" in s.lower():
            return "BUY"
        if "sell" in s.lower() or "short" in s.lower():
            return "SELL"
    return None


@app.route("/webhook", methods=["POST", "GET"])
def webhook():
    data = parse_payload()
    secret = str(data.get("secret") or data.get("passphrase") or
                 request.args.get("secret") or "").strip()
    want = str(STATE["settings"].get("webhook_secret", "")).strip()
    if want and secret != want:
        return jsonify({"ok": False, "error": "gizli anahtar hatali"}), 403
    symbol = (data.get("symbol") or data.get("ticker") or
              data.get("pair") or data.get("s") or "")
    action = classify_action(data.get("action"), data.get("side"),
                             data.get("type"), data.get("event"),
                             data.get("signal"))
    if not action:
        return jsonify({"ok": False, "error": "action anlasilmadi"}), 400
    try:
        price = float(str(data.get("price") or data.get("close") or
                          data.get("p") or "0").replace(",", ".")) or None
    except Exception:
        price = None
    res = process_signal(symbol, action, price, raw=json.dumps(data, ensure_ascii=False))
    code = 200 if res.get("ok") else 422
    return jsonify(res), code


# ----------------------------------------------------------------------------
# Panel / API
# ----------------------------------------------------------------------------

@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok", "source": _live_source["name"],
                    "redis": REDIS_OK})


def login_required(f):
    from functools import wraps

    @wraps(f)
    def wrapper(*a, **kw):
        if not session.get("auth"):
            return redirect("/login")
        return f(*a, **kw)
    return wrapper


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == APP_PASSWORD:
            session["auth"] = True
            session.permanent = True
            return redirect("/")
        time.sleep(1)
        return render_template_string(LOGIN_HTML, error="Şifre hatalı")
    return render_template_string(LOGIN_HTML, error=None)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


def trade_stats(trades):
    wins = [t for t in trades if t["net"] > 0]
    losses = [t for t in trades if t["net"] <= 0]
    gross_p = sum(t["net"] for t in wins)
    gross_l = abs(sum(t["net"] for t in losses))
    return {
        "count": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else 0,
        "net": round(sum(t["net"] for t in trades), 4),
        "fees": round(sum(t["fees"] for t in trades), 4),
        "funding": round(sum(t["funding"] for t in trades), 4),
        "avg_win": round(gross_p / len(wins), 4) if wins else 0,
        "avg_loss": round(gross_l / len(losses), 4) if losses else 0,
        "profit_factor": round(gross_p / gross_l, 2) if gross_l > 0 else None,
    }


def per_symbol_stats(mode):
    m = STATE["modes"][mode]
    out = {}
    for t in m["trades"]:
        d = out.setdefault(t["symbol"], {"symbol": t["symbol"], "count": 0,
                                         "wins": 0, "net": 0.0, "fees": 0.0,
                                         "funding": 0.0})
        d["count"] += 1
        if t["net"] > 0:
            d["wins"] += 1
        d["net"] += t["net"]
        d["fees"] += t["fees"]
        d["funding"] += t["funding"]
    rows = []
    for d in out.values():
        d["net"] = round(d["net"], 4)
        d["fees"] = round(d["fees"], 4)
        d["funding"] = round(d["funding"], 4)
        d["win_rate"] = round(d["wins"] / d["count"] * 100, 1)
        rows.append(d)
    rows.sort(key=lambda x: x["net"], reverse=True)
    return rows


def unrealized(pos):
    px = STATE["prices"].get(pos["symbol"])
    if not px:
        return None, None
    if pos["side"] == "long":
        pnl = (px - pos["fill_price"]) * pos["qty"]
    else:
        pnl = (pos["fill_price"] - px) * pos["qty"]
    pct = pnl / pos["margin"] * 100 if pos["margin"] else 0
    return round(pnl, 4), round(pct, 2)


@app.route("/api/state")
@login_required
def api_state():
    with LOCK:
        modes = {}
        for mode in ("A", "B"):
            m = STATE["modes"][mode]
            positions = []
            for sym, p in m["positions"].items():
                upnl, upct = unrealized(p)
                positions.append({
                    "symbol": sym, "side": p["side"],
                    "entry_time": p["entry_time"], "entry": p["fill_price"],
                    "now": STATE["prices"].get(sym),
                    "tp": p.get("tp"), "sl": p.get("sl"),
                    "leverage": p["leverage"], "margin": round(p["margin"], 2),
                    "unrealized": upnl, "unrealized_pct": upct,
                })
            modes[mode] = {
                "label": mode_cfg(mode)["label"],
                "balance": round(m["balance"], 4),
                "start": mode_cfg(mode)["start_balance"],
                "net": round(m["balance"] - mode_cfg(mode)["start_balance"], 4),
                "max_dd": round(m["max_dd"], 2),
                "open_count": len(positions),
                "stats": trade_stats(m["trades"]),
                "positions": positions,
                "trades": list(reversed(m["trades"][-200:])),
                "per_symbol": per_symbol_stats(mode),
            }
        out = {
            "modes": modes,
            "settings": STATE["settings"],
            "signals": STATE["signals"][:100],
            "source": _live_source["name"],
            "redis": REDIS_OK,
            "server_time": now_iso(),
        }
    return jsonify(out)


@app.route("/api/settings", methods=["POST"])
@login_required
def api_settings():
    data = request.get_json(silent=True) or {}
    with LOCK:
        s = STATE["settings"]
        for k in ("exchange",):
            if data.get(k) in ("auto", "okx", "gate", "mexc"):
                s[k] = data[k]
        if "funding_enabled" in data:
            s["funding_enabled"] = bool(data["funding_enabled"])
        if data.get("webhook_secret") is not None:
            ws = str(data["webhook_secret"]).strip()
            if ws:
                s["webhook_secret"] = ws
        for mode in ("A", "B"):
            if mode not in data:
                continue
            for k, v in data[mode].items():
                if k not in DEFAULT_SETTINGS[mode] or k in ("label",):
                    continue
                try:
                    if isinstance(DEFAULT_SETTINGS[mode][k], bool):
                        s[mode][k] = bool(v)
                    elif isinstance(DEFAULT_SETTINGS[mode][k], (int, float)):
                        s[mode][k] = float(v)
                except Exception:
                    pass
        save_state()
    return jsonify({"ok": True})


@app.route("/api/test-signal", methods=["POST"])
@login_required
def api_test_signal():
    d = request.get_json(silent=True) or {}
    sym = norm_symbol(d.get("symbol", "BTCUSDT"))
    act = classify_action(d.get("action", "BUY"))
    if not act:
        return jsonify({"ok": False, "error": "geçersiz action"}), 400
    res = process_signal(sym, act, None)
    return jsonify(res), (200 if res.get("ok") else 422)


@app.route("/api/close", methods=["POST"])
@login_required
def api_close():
    d = request.get_json(silent=True) or {}
    sym = norm_symbol(d.get("symbol", ""))
    mode = d.get("mode")
    if mode not in ("A", "B") or not sym:
        return jsonify({"ok": False, "error": "geçersiz istek"}), 400
    with LOCK:
        pos = STATE["modes"][mode]["positions"].get(sym)
        if not pos:
            return jsonify({"ok": False, "error": "pozisyon yok"}), 404
        px = STATE["prices"].get(sym) or pos["fill_price"]
        t = close_position(mode, sym, px, "Manuel kapatma")
        save_state()
    return jsonify({"ok": True, "trade": t})


@app.route("/api/reset", methods=["POST"])
@login_required
def api_reset():
    d = request.get_json(silent=True) or {}
    mode = d.get("mode")
    with LOCK:
        for m in ("A", "B") if not mode else [mode]:
            cfg = STATE["settings"][m]
            STATE["modes"][m] = {
                "balance": cfg["start_balance"], "peak": cfg["start_balance"],
                "max_dd": 0.0, "positions": {}, "trades": [], "last_close": {}}
        save_state()
    return jsonify({"ok": True})


@app.route("/export/trades.csv")
@login_required
def export_csv():
    mode = request.args.get("mode", "A")
    if mode not in ("A", "B"):
        mode = "A"
    m = STATE["modes"][mode]
    si = io.StringIO()
    w = csv.writer(si)
    w.writerow(["parite", "yon", "acilis_zamani", "giris_fiyati", "cikis_zamani",
                "cikis_fiyati", "kapanis_sebebi", "brut_kar_zarar", "komisyon",
                "funding", "net_kar_zarar", "bakiye_sonrasi", "kaldirac"])
    for t in m["trades"]:
        w.writerow([t["symbol"], t["side"], t["entry_time"], t["fill_price"],
                    t["exit_time"], t["fill_exit"], t["exit_reason"], t["gross"],
                    t["fees"], t["funding"], t["net"], t["balance_after"],
                    t["leverage"]])
    return Response(si.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":
                             f"attachment; filename=islem-{mode}.csv"})


@app.route("/")
@login_required
def index():
    return render_template_string(APP_HTML)


# ----------------------------------------------------------------------------
# HTML
# ----------------------------------------------------------------------------

LOGIN_HTML = """<!doctype html><html lang="tr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Giriş - Kripto Sinyal Test</title><style>
body{background:#0d1117;color:#e6edf3;font-family:system-ui;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
.box{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:28px;width:min(92vw,360px)}
h1{font-size:18px;margin:0 0 6px}p{color:#8b949e;font-size:13px;margin:0 0 18px}
input{width:100%;box-sizing:border-box;padding:11px;border-radius:8px;border:1px solid #30363d;background:#0d1117;color:#e6edf3;margin-bottom:12px}
button{width:100%;padding:11px;border-radius:8px;border:0;background:#238636;color:#fff;font-weight:600;font-size:15px}
.err{color:#f85149;font-size:13px;margin-bottom:10px}
</style></head><body><div class="box"><h1>Kripto Sinyal Test Sistemi</h1>
<p>Lütfen panel şifrenizle giriş yapın.</p>
{% if error %}<div class="err">{{ error }}</div>{% endif %}
<form method="post"><input type="password" name="password" placeholder="Şifre" autofocus>
<button type="submit">Giriş Yap</button></form></div></body></html>"""

APP_HTML = r"""
<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Kripto Sinyal Test Sistemi</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--line:#30363d;--tx:#e6edf3;--mut:#8b949e;--gr:#238636;--gr2:#3fb950;--rd:#f85149;--bl:#388bfd;--yl:#d29922}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--tx);font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;font-size:14px}
header{position:sticky;top:0;z-index:20;background:rgba(13,17,23,.95);border-bottom:1px solid var(--line);padding:10px 14px;display:flex;align-items:center;justify-content:space-between;backdrop-filter:blur(6px)}
header h1{font-size:15px;margin:0;font-weight:700}
.hdr-r{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--mut)}
.badge{border:1px solid var(--line);border-radius:99px;padding:2px 8px}
.badge.on{color:var(--gr2);border-color:#1f4a2e}
.badge.off{color:var(--yl);border-color:#4a3d1f}
nav{display:flex;overflow-x:auto;gap:4px;padding:8px 10px;border-bottom:1px solid var(--line);position:sticky;top:45px;z-index:19;background:var(--bg)}
nav button{flex:0 0 auto;background:transparent;color:var(--mut);border:1px solid transparent;border-radius:8px;padding:8px 12px;font-size:13px;font-weight:600;cursor:pointer}
nav button.act{color:var(--tx);background:var(--card);border-color:var(--line)}
main{padding:12px;max-width:980px;margin:0 auto}
.grid{display:grid;gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px}
.card h2{margin:0 0 4px;font-size:15px}
.card .sub{color:var(--mut);font-size:12px;margin-bottom:10px}
.row{display:flex;flex-wrap:wrap;gap:8px}
.kpi{flex:1 1 100px;min-width:100px;background:#0d1117;border:1px solid var(--line);border-radius:10px;padding:10px}
.kpi .l{font-size:11px;color:var(--mut);margin-bottom:3px}
.kpi .v{font-size:16px;font-weight:700}
.pos{color:var(--gr2)}.neg{color:var(--rd)}
.tblwrap{overflow-x:auto;border:1px solid var(--line);border-radius:10px}
table{border-collapse:collapse;width:100%;min-width:560px;font-size:12.5px}
th,td{padding:8px 9px;text-align:right;border-bottom:1px solid #21262d;white-space:nowrap}
th{color:var(--mut);font-weight:600;position:sticky;top:0;background:var(--card)}
td.l,th.l{text-align:left}
tr:last-child td{border-bottom:0}
.mono{font-variant-numeric:tabular-nums}
.btn{background:#21262d;border:1px solid var(--line);color:var(--tx);border-radius:8px;padding:8px 12px;font-size:13px;font-weight:600;cursor:pointer}
.btn.p{background:var(--gr);border-color:var(--gr)}
.btn.d{background:transparent;border-color:#6e4041;color:var(--rd)}
.btn.sm{padding:4px 9px;font-size:11.5px}
label.f{display:block;margin:10px 0 4px;font-size:12.5px;color:var(--mut)}
input.s,select.s{width:100%;padding:10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--tx);font-size:14px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.note{background:#0d1117;border:1px solid var(--line);border-radius:10px;padding:10px;font-size:12.5px;color:var(--mut);margin-top:10px;line-height:1.55}
code,pre{font-family:ui-monospace,Menlo,monospace;font-size:11.5px}
pre{background:#0d1117;border:1px solid var(--line);border-radius:10px;padding:10px;overflow-x:auto;color:#a5d6ff;white-space:pre-wrap;word-break:break-all}
.toast{position:fixed;left:50%;transform:translateX(-50%);bottom:18px;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 16px;font-size:13px;z-index:50;max-width:90vw;box-shadow:0 4px 14px rgba(0,0,0,.5)}
.hidden{display:none}
.mtitle{display:flex;align-items:center;justify-content:space-between;gap:8px}
.tag{font-size:10.5px;border:1px solid var(--line);border-radius:99px;padding:2px 8px;color:var(--mut)}
.side-b{color:var(--gr2)}.side-s{color:var(--rd)}
h3.sec{font-size:13px;color:var(--mut);margin:18px 0 8px;font-weight:600;text-transform:uppercase;letter-spacing:.4px}
</style>
</head>
<body>
<header>
  <h1>📊 Kripto Sinyal Test</h1>
  <div class="hdr-r">
    <span class="badge" id="src">veri: -</span>
    <span class="badge" id="sto">kayıt: -</span>
    <button class="btn sm" onclick="location.href='/logout'">Çıkış</button>
  </div>
</header>
<nav id="tabs">
  <button data-t="ozet" class="act">Özet</button>
  <button data-t="acik">Açık Pozisyonlar</button>
  <button data-t="gecmis">İşlem Geçmişi</button>
  <button data-t="parite">Parite Bazlı</button>
  <button data-t="ayar">Ayarlar</button>
  <button data-t="kurulum">Kurulum</button>
</nav>
<main>
  <div id="p-ozet" class="grid"></div>
  <div id="p-acik" class="grid hidden"></div>
  <div id="p-gecmis" class="grid hidden"></div>
  <div id="p-parite" class="grid hidden"></div>
  <div id="p-ayar" class="grid hidden"></div>
  <div id="p-kurulum" class="grid hidden"></div>
</main>
<script>
let S=null, TAB='ozet';
const $=id=>document.getElementById(id);
const fmt=(n,d=2)=>n==null?'-':Number(n).toLocaleString('tr-TR',{minimumFractionDigits:d,maximumFractionDigits:d});
const sgn=n=>n==null?'':(n>0?'pos':n<0?'neg':'');
const cls=n=>n==null?'':(n>=0?'pos':'neg');
const dt=s=>s?new Date(s).toLocaleString('tr-TR'):'-';

document.querySelectorAll('#tabs button').forEach(b=>b.onclick=()=>{
  TAB=b.dataset.t;
  document.querySelectorAll('#tabs button').forEach(x=>x.classList.toggle('act',x===b));
  ['ozet','acik','gecmis','parite','ayar','kurulum'].forEach(t=>$('p-'+t).classList.toggle('hidden',t!==TAB));
  render();
});

async function load(){
  try{
    const r=await fetch('/api/state');if(!r.ok)throw 0;S=await r.json();render();
  }catch(e){}
  setTimeout(load,10000);
}

function modeCard(m,d){
  const st=d.stats, roi=d.start>0?(d.net/d.start*100):0;
  const sgnn=d.net>=0?'pos':'neg';
  return `<div class="card">
    <div class="mtitle"><h2>${d.label}</h2><span class="tag">${m} modu</span></div>
    <div class="sub">Başlangıç: ${fmt(d.start)} USDT</div>
    <div class="row">
      <div class="kpi"><div class="l">Bakiye</div><div class="v">${fmt(d.balance)} USDT</div></div>
      <div class="kpi"><div class="l">Net K/Z</div><div class="v ${sgnn}">${fmt(d.net)} USDT</div></div>
      <div class="kpi"><div class="l">Getiri</div><div class="v ${sgnn}">${fmt(roi)}%</div></div>
      <div class="kpi"><div class="l">İşlem</div><div class="v">${st.count}</div></div>
      <div class="kpi"><div class="l">Kazanma</div><div class="v">${st.win_rate}%</div></div>
      <div class="kpi"><div class="l">Açık Poz.</div><div class="v">${d.open_count}</div></div>
      <div class="kpi"><div class="l">Komisyon</div><div class="v neg">-${fmt(st.fees)}</div></div>
      <div class="kpi"><div class="l">Funding</div><div class="v ${st.funding<=0?'pos':'neg'}">${st.funding>=0?'+':''}${fmt(st.funding)}</div></div>
      <div class="kpi"><div class="l">Maks. Düşüş</div><div class="v neg">-${fmt(d.max_dd)}%</div></div>
      <div class="kpi"><div class="l">Profit Factor</div><div class="v">${st.profit_factor??'-'}</div></div>
    </div>
    <div class="note">Ort. kazanç: <b>${fmt(st.avg_win)}</b> USDT · Ort. kayıp: <b>${fmt(st.avg_loss)}</b> USDT</div>
  </div>`;
}

function render(){
  if(!S)return;
  $('src').textContent='veri: '+S.source;$('src').className='badge '+(S.source!=='-'?'on':'off');
  $('sto').textContent='kayıt: '+(S.redis?'kalıcı':'geçici');$('sto').className='badge '+(S.redis?'on':'off');
  if(TAB==='ozet')$('p-ozet').innerHTML=modeCard('A',S.modes.A)+modeCard('B',S.modes.B)+sigCard();
  if(TAB==='acik')renderOpen();
  if(TAB==='gecmis')renderHist();
  if(TAB==='parite')renderSym();
  if(TAB==='ayar')renderSettings();
  if(TAB==='kurulum')renderSetup();
}

function sigCard(){
  const rows=S.signals.slice(0,15).map(s=>`<tr>
    <td class="l">${dt(s.time)}</td><td class="l">${s.symbol}</td>
    <td><b class="${s.action==='BUY'?'side-b':'side-s'}">${s.action}</b></td>
    <td class="mono">${s.price?fmt(s.price,6):'-'}</td>
    <td class="l" style="color:var(--mut)">${s.ok?(s.note||'✓'):'⚠ '+s.note}</td></tr>`).join('');
  return `<div class="card"><h2>Son Sinyaller</h2>
  <div class="sub">TradingView'dan gelen son webhook mesajları</div>
  <div class="tblwrap"><table><thead><tr><th class="l">Zaman</th><th class="l">Parite</th><th>Sinyal</th><th>Fiyat</th><th class="l">Durum</th></tr></thead><tbody>${rows||'<tr><td colspan="5" style="text-align:center;color:var(--mut)">Henüz sinyal yok</td></tr>'}</tbody></table></div></div>`;
}

function posTable(m,d){
  const rows=d.positions.map(p=>`<tr>
    <td class="l"><b>${p.symbol}</b></td>
    <td><span class="${p.side==='long'?'side-b':'side-s'}">${p.side==='long'?'LONG':'SHORT'}</span></td>
    <td class="mono">${fmt(p.entry,6)}</td>
    <td class="mono">${p.now?fmt(p.now,6):'-'}</td>
    <td class="mono">${p.tp?fmt(p.tp,6):'-'}</td>
    <td class="mono">${p.sl?fmt(p.sl,6):'-'}</td>
    <td class="mono ${cls(p.unrealized)}">${p.unrealized==null?'-':fmt(p.unrealized)}</td>
    <td class="mono ${cls(p.unrealized)}">${p.unrealized_pct==null?'-':fmt(p.unrealized_pct)}%</td>
    <td class="mono">${p.leverage}x</td>
    <td><button class="btn sm d" onclick="closePos('${m}','${p.symbol}')">Kapat</button></td></tr>`).join('');
  return `<div class="card"><h2>${d.label}</h2>
  <div class="tblwrap"><table><thead><tr><th class="l">Parite</th><th>Yön</th><th>Giriş</th><th>Anlık</th><th>TP</th><th>SL</th><th>K/Z (USDT)</th><th>K/Z %</th><th>Kald.</th><th></th></tr></thead>
  <tbody>${rows||'<tr><td colspan="10" style="text-align:center;color:var(--mut)">Açık pozisyon yok</td></tr>'}</tbody></table></div></div>`;
}
function renderOpen(){$('p-acik').innerHTML=posTable('A',S.modes.A)+posTable('B',S.modes.B);}

let HMODE='A';
function renderHist(){
  const d=S.modes[HMODE];
  const rows=d.trades.map(t=>`<tr>
    <td class="l">${dt(t.exit_time)}</td><td class="l"><b>${t.symbol}</b></td>
    <td><span class="${t.side==='long'?'side-b':'side-s'}">${t.side==='long'?'LONG':'SHORT'}</span></td>
    <td class="mono">${fmt(t.fill_price,6)}</td><td class="mono">${fmt(t.fill_exit,6)}</td>
    <td class="l">${t.exit_reason}</td>
    <td class="mono ${cls(t.gross)}">${fmt(t.gross)}</td>
    <td class="mono">${fmt(t.fees)}</td>
    <td class="mono ${t.funding<=0?'pos':'neg'}">${fmt(t.funding)}</td>
    <td class="mono ${cls(t.net)}"><b>${fmt(t.net)}</b></td></tr>`).join('');
  $('p-gecm').innerHTML=`<div class="card">
    <div class="mtitle"><h2>İşlem Geçmişi</h2>
    <div class="row">
      <select class="btn sm" id="hsel" onchange="HMODE=this.value;render()"><option value="A"${HMODE==='A'?' selected':''}>${S.modes.A.label}</option><option value="B"${HMODE==='B'?' selected':''}>${S.modes.B.label}</option></select>
      <a class="btn sm" href="/export/trades.csv?mode=${HMODE}">CSV indir</a>
    </div></div>
    <div class="tblwrap"><table><thead><tr><th class="l">Kapanış</th><th class="l">Parite</th><th>Yön</th><th>Giriş</th><th>Çıkış</th><th class="l">Sebep</th><th>Brüt</th><th>Komisyon</th><th>Funding</th><th>Net</th></tr></thead>
    <tbody>${rows||'<tr><td colspan="10" style="text-align:center;color:var(--mut)">Kapanmış işlem yok</td></tr>'}</tbody></table></div></div>`;
}

let PMODE='A';
function renderSym(){
  const d=S.modes[PMODE];
  const rows=d.per_symbol.map(p=>`<tr>
    <td class="l"><b>${p.symbol}</b></td><td>${p.count}</td><td>${p.win_rate}%</td>
    <td class="mono ${cls(p.net)}"><b>${fmt(p.net)}</b></td>
    <td class="mono">${fmt(p.fees)}</td>
    <td class="mono ${p.funding<=0?'pos':'neg'}">${fmt(p.funding)}</td></tr>`).join('');
  const tot=modeCardMini(d);
  $('p-parite').innerHTML=`<div class="card">
    <div class="mtitle"><h2>Parite Bazlı Sonuçlar</h2>
    <select class="btn sm" onchange="PMODE=this.value;render()">
      <option value="A"${PMODE==='A'?' selected':''}>${S.modes.A.label}</option>
      <option value="B"${PMODE==='B'?' selected':''}>${S.modes.B.label}</option></select></div>
    ${tot}
    <div class="tblwrap" style="margin-top:10px"><table><thead><tr><th class="l">Parite</th><th>İşlem</th><th>Kazanma</th><th>Net K/Z</th><th>Komisyon</th><th>Funding</th></tr></thead>
    <tbody>${rows||'<tr><td colspan="6" style="text-align:center;color:var(--mut)">Veri yok</td></tr>'}</tbody></table></div></div>`;
}
function modeCardMini(d){
  const st=d.stats,roi=d.start>0?(d.net/d.start*100):0;
  return `<div class="row" style="margin-top:8px">
    <div class="kpi"><div class="l">Bakiye</div><div class="v">${fmt(d.balance)}</div></div>
    <div class="kpi"><div class="l">Net K/Z</div><div class="v ${d.net>=0?'pos':'neg'}">${fmt(d.net)}</div></div>
    <div class="kpi"><div class="l">Getiri</div><div class="v ${d.net>=0?'pos':'neg'}">${fmt(roi)}%</div></div>
    <div class="kpi"><div class="l">İşlem</div><div class="v">${st.count}</div></div></div>`;
}

const SET_FIELDS={
  A:[['start_balance','Başlangıç bakiyesi (USDT)',1000],['leverage','Kaldıraç',10],['position_pct','Pozisyon büyüklüğü (% bakiye)',10],['tp_pct',null],['sl_pct',null],['taker_fee','Taker komisyonu (%)',0.05],['slippage','Kayma / slippage (%)',0.02],['max_open','Maks. açık pozisyon',10],['cooldown_min','Kapanış sonrası bekleme (dk)',0],['reverse_on_signal','Ters sinyalde pozisyonu çevir',true]],
  B:[['start_balance','Başlangıç bakiyesi (USDT)',1000],['leverage','Kaldıraç',10],['position_pct','Pozisyon büyüklüğü (% bakiye)',10],['tp_pct','TP yüzdesi (%)',1],['sl_pct','SL yüzdesi (%)',0.5],['taker_fee','Taker komisyonu (%)',0.05],['slippage','Kayma / slippage (%)',0.02],['max_open','Maks. açık pozisyon',10],['cooldown_min','Kapanış sonrası bekleme (dk)',0],['reverse_on_signal','Ters sinyalde pozisyonu çevir',true],['use_indicator_exits','İndikatör TP/SL sinyallerini de uygula',false]]
};
function renderSettings(){
  const s=S.settings;
  const f=(mode)=>{
    const d=s[mode];
    return SET_FIELDS[mode].filter(x=>x[1]).map(([k,lab])=>{
      const v=d[k];
      if(typeof v==='boolean')return `<label class="f">${lab}</label><select class="s" id="set-${mode}-${k}"><option value="1"${v?' selected':''}>Evet</option><option value="0"${!v?' selected':''}>Hayır</option></select>`;
      const dec=(k==='taker_fee'||k==='slippage'||k==='position_pct'||k==='tp_pct'||k==='sl_pct')?4:0;
      return `<label class="f">${lab}</label><input class="s" id="set-${mode}-${k}" type="number" step="any" value="${v}">`;
    }).join('');
  };
  $('p-ayar').innerHTML=`<div class="card"><h2>Genel Ayarlar</h2>
    <div class="two">
      <div><label class="f">Fiyat verisi kaynağı</label>
      <select class="s" id="set-exchange">
        <option value="auto"${s.exchange==='auto'?' selected':''}>Otomatik (önerilen)</option>
        <option value="okx"${s.exchange==='okx'?' selected':''}>OKX</option>
        <option value="gate"${s.exchange==='gate'?' selected':''}>Gate.io</option>
        <option value="mexc"${s.exchange==='mexc'?' selected':''}>MEXC</option></select></div>
      <div><label class="f">Funding hesaplama</label>
      <select class="s" id="set-funding"><option value="1"${s.funding_enabled?' selected':''}>Açık (gerçek oranlar)</option><option value="0"${!s.funding_enabled?' selected':''}>Kapalı</option></select></div>
    </div>
    <label class="f">Webhook gizli anahtarı (boş bırakırsanız değişmez)</label>
    <input class="s" id="set-secret" placeholder="${s.webhook_secret}">
    <div class="note">Fiyat verisi borsaların genel (public) API'sinden alınır; gerçek işleme bağlanmaz. Binance ve Bybit bazı sunucu bölgelerinden engellendiği için OKX / Gate / MEXC kullanılır.</div>
  </div>
  <div class="card"><h2>${s.A.label} — A Modu</h2>${f('A')}</div>
  <div class="card"><h2>${s.B.label} — B Modu</h2>${f('B')}
    <div class="note">B modunda TP/SL, açılış fiyatından (kayma dahil) otomatik hesaplanır ve fiyat izlenerek kapatılır. İndikatör TP/SL sinyalleri varsayılan olarak yok sayılır.</div>
  </div>
  <div class="card"><div class="row">
    <button class="btn p" onclick="saveSettings()">Ayarları Kaydet</button>
    <button class="btn d" onclick="resetMode('A')">A Modunu Sıfırla</button>
    <button class="btn d" onclick="resetMode('B')">B Modunu Sıfırla</button>
  </div></div>`;
}
function saveSettings(){
  const s=S.settings,payload={};
  payload.exchange=$('set-exchange').value;
  payload.funding_enabled=$('set-funding').value==='1';
  if($('set-secret').value.trim())payload.webhook_secret=$('set-secret').value.trim();
  for(const mode of ['A','B']){
    payload[mode]={};
    for(const [k] of SET_FIELDS[mode]){
      const el=$('set-'+mode+'-'+k);if(!el)continue;
      payload[mode][k]=el.value==='1'||el.value==='0'?el.value==='1':parseFloat(el.value);
    }
  }
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)})
   .then(r=>r.json()).then(()=>{toast('Ayarlar kaydedildi');load();});
}
function resetMode(m){
  if(!confirm(m+' modu sıfırlanacak: tüm işlemler ve bakiye başlangıç değerine döner. Onaylıyor musunuz?'))return;
  fetch('/api/reset',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode:m})}).then(()=>{toast('Sıfırlandı');load();});
}
async function closePos(m,sym){
  if(!confirm(sym+' pozisyonu anlık fiyattan kapatılsın mı?'))return;
  const r=await fetch('/api/close',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode:m,symbol:sym})});
  toast(r.ok?'Pozisyon kapatıldı':'Kapatılamadı');load();
}
function renderSetup(){
  const url=location.origin+'/webhook', sec=S.settings.webhook_secret;
  const t=a=>JSON.stringify({secret:sec,symbol:'{{ticker}}',price:'{{close}}',action:a},null,2);
  $('p-kurulum').innerHTML=`<div class="card"><h2>TradingView Alarm Kurulumu</h2>
  <div class="sub">İndikatörünüzün BUY / SELL / TP / SL / CLOSE sinyalleri için aşağıdaki adımları uygulayın.</div>
  <h3 class="sec">1. Webhook Adresi</h3>
  <pre>${url}</pre>
  <h3 class="sec">2. Alarm Mesajları</h3>
  <p style="font-size:13px;color:var(--mut);margin:6px 0">Her alarm için "Mesaj" bölümüne aşağıdaki metni kopyalayın ({{ticker}} ve {{close}} otomatik dolar):</p>
  <h3 class="sec">BUY (Long aç)</h3><pre>${t('BUY')}</pre>
  <h3 class="sec">SELL (Short aç)</h3><pre>${t('SELL')}</pre>
  <h3 class="sec">TP (kâr al)</h3><pre>${t('TP')}</pre>
  <h3 class="sec">SL (zarar kes)</h3><pre>${t('SL')}</pre>
  <h3 class="sec">CLOSE (pozisyon kapat)</h3><pre>${t('CLOSE')}</pre>
  <h3 class="sec">3. Alarm Ayarı</h3>
  <div class="note">TradingView'da alarm oluştururken: <b>Koşul</b> = indikatörünüzün sinyali, <b>Süreyi</b> = "Yalnızca kapanışta" (Once Per Bar Close) önerilir, <b>Bildirimler → Webhook URL</b> = yukarıdaki adres, <b>Mesaj</b> = ilgili şablon. Webhook kullanımı TradingView'ın ücretli planlarında bulunur. Aynı parite için hem BUY hem SELL alarmlarını kurun; TP/SL/CLOSE alarmları indikatörünüzde varsa A modu bunları kullanır.</div>
  <h3 class="sec">Test Sinyali Gönder</h3>
  <div class="row">
    <input class="s" id="ts-sym" style="flex:1;min-width:120px" value="BTCUSDT" placeholder="Parite">
    <select class="s" id="ts-act" style="flex:1;min-width:100px"><option>BUY</option><option>SELL</option><option>TP</option><option>SL</option><option>CLOSE</option></select>
    <button class="btn p" onclick="testSig()">Gönder</button>
  </div>
  <div id="ts-out" class="note hidden"></div>
  </div>
  <div class="card"><h2>Sistem Nasıl Çalışır?</h2>
  <div class="note" style="margin-top:0">
  • <b>A modu (Sinyal takipli):</b> BUY/SELL ile pozisyon açılır; indikatörün TP/SL/CLOSE sinyalleriyle kapanır.<br>
  • <b>B modu (Yüzdeli TP/SL):</b> BUY/SELL ile pozisyon açılır; Ayarlar'da belirlediğiniz yüzdeyle TP ve SL otomatik konur, fiyat 7/24 izlenir.<br>
  • Her işlemde <b>taker komisyonu</b> (giriş + çıkış), <b>kayma</b> ve <b>gerçek funding oranları</b> hesaba katılır.<br>
  • Sonuçlar parite bazında ayrı ayrı gösterilir; CSV olarak indirilebilir.<br>
  • Bu sistem <b>gerçek para ile işlem yapmaz</b>; tamamen sanal (paper) testtir.<br>
  • Farklı borsalardaki fiyatlar birkaç binde bir fark edebilir; bu bir simülasyondur.
  </div></div>`;
}
function testSig(){
  const sym=$('ts-sym').value||'BTCUSDT',act=$('ts-act').value;
  fetch('/api/test-signal',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:sym,action:act})})
   .then(r=>r.json()).then(d=>{
     const el=$('ts-out');el.classList.remove('hidden');
     el.innerHTML='<b>Sonuç:</b><br>'+(d.messages||[d.error||'?']).map(m=>'• '+m).join('<br>');
     load();
   });
}
function toast(msg){
  const t=document.createElement('div');t.className='toast';t.textContent=msg;document.body.appendChild(t);
  setTimeout(()=>t.remove(),2600);
}
load();
</script>
</body>
</html>

"""


# ----------------------------------------------------------------------------
# Baslat
# ----------------------------------------------------------------------------

def _bg_threads():
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=price_loop, daemon=True).start()


_init_done = False
_init_lock = threading.Lock()


def _ensure_init():
    """Ilk istekte bir kez: Redis'ten durumu yukle + arka plan izleyicileri baslat.
    (gunicorn ile calisirken __main__ blogu hic calismaz)"""
    global _init_done
    if _init_done:
        return
    with _init_lock:
        if _init_done:
            return
        init_redis()
        refresh_prices()
        _bg_threads()
        _init_done = True


@app.before_request
def _auto_init():
    _ensure_init()


def main():
    _ensure_init()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
