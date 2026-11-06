
import os, time, sqlite3, asyncio, json
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import httpx

DB = os.path.join(os.path.dirname(__file__), "data.db")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "degistir-bunu")

app = FastAPI()

def db():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def init():
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS settings(
      key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS trades(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT, side TEXT, status TEXT,
      entry REAL, qty REAL, notional REAL,
      tp REAL, sl REAL, leverage REAL,
      open_time TEXT, close_time TEXT,
      exit REAL, gross_pnl REAL, fees REAL,
      funding REAL, net_pnl REAL, reason TEXT);
    """)
    defaults = {
      "tp_pct":"1.0","sl_pct":"0.5","leverage":"5",
      "balance":"1000","position_pct":"10",
      "taker_fee":"0.0005","slippage":"0.0002",
      "funding_enabled":"1","funding_rate":"0.0001"
    }
    for k,v in defaults.items():
        c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",(k,v))
    c.commit(); c.close()

init()

def get_settings():
    c=db(); rows=c.execute("SELECT key,value FROM settings").fetchall(); c.close()
    return {r["key"]: r["value"] for r in rows}

def set_setting(k,v):
    c=db(); c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(k,str(v))); c.commit(); c.close()

def s_float(k): return float(get_settings()[k])

@app.get("/", response_class=HTMLResponse)
def home():
    st=get_settings()
    c=db()
    trades=c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 200").fetchall()
    agg=c.execute("""SELECT symbol, COUNT(*) n,
      SUM(CASE WHEN net_pnl>0 THEN 1 ELSE 0 END) wins,
      SUM(CASE WHEN net_pnl<=0 THEN 1 ELSE 0 END) losses,
      SUM(net_pnl) net, SUM(fees) fees, SUM(funding) funding
      FROM trades WHERE status='CLOSED' GROUP BY symbol ORDER BY net DESC""").fetchall()
    c.close()
    rows="".join(f"<tr><td>{t['id']}</td><td>{t['symbol']}</td><td>{t['side']}</td><td>{t['status']}</td><td>{t['entry']}</td><td>{t['exit'] or ''}</td><td>{round(t['net_pnl'],4) if t['net_pnl'] is not None else ''}</td><td>{t['reason'] or ''}</td></tr>" for t in trades)
    aggs="".join(f"<tr><td>{a['symbol']}</td><td>{a['n']}</td><td>{a['wins']}</td><td>{a['losses']}</td><td>{round(a['net'],4)}</td><td>{round(a['fees'],4)}</td><td>{round(a['funding'],4)}</td></tr>" for a in agg)
    return f"""<html><head><meta charset='utf-8'><title>Kripto Sinyal Test</title>
    <style>body{{font-family:Arial;max-width:1100px;margin:20px auto}}table{{border-collapse:collapse;width:100%}}td,th{{border:1px solid #ccc;padding:6px;font-size:13px}}input{{width:90px}}</style></head><body>
    <h2>Kripto Vadeli Sinyal Test Paneli</h2>
    <h3>Ayarlar</h3>
    <form method='post' action='/api/settings'>
    {''.join(f"<label>{k}: <input name='{k}' value='{v}'></label> " for k,v in st.items())}
    <button>Kaydet</button></form>
    <h3>Parite Bazlı Sonuçlar</h3>
    <table><tr><th>Parite</th><th>İşlem</th><th>Kazanan</th><th>Kaybeden</th><th>Net PnL</th><th>Komisyon</th><th>Funding</th></tr>{aggs}</table>
    <h3>Son İşlemler</h3>
    <table><tr><th>ID</th><th>Parite</th><th>Yön</th><th>Durum</th><th>Giriş</th><th>Çıkış</th><th>Net PnL</th><th>Sebep</th></tr>{rows}</table>
    </body></html>"""

@app.post("/api/settings")
async def save_settings(request: Request):
    form = await request.form()
    for k in ["tp_pct","sl_pct","leverage","balance","position_pct","taker_fee","slippage","funding_enabled","funding_rate"]:
        if k in form: set_setting(k, form[k])
    return JSONResponse({"ok":True})

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    if data.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(401, "Geçersiz secret")
    symbol = str(data.get("symbol","")).upper().replace("PERP","")
    action = str(data.get("action","")).upper()
    price = float(data.get("price") or 0)
    if not symbol or action not in ("BUY","SELL","CLOSE") or price<=0:
        raise HTTPException(400, "Eksik veya hatalı mesaj")
    c=db()
    open_t=c.execute("SELECT * FROM trades WHERE symbol=? AND status='OPEN'",(symbol,)).fetchone()
    if open_t:
        close_trade(c, open_t, price, "TERS SINYAL" if action in ("BUY","SELL") else "MANUEL CLOSE")
    if action in ("BUY","SELL"):
        st=get_settings()
        bal=float(st["balance"]); pos_pct=float(st["position_pct"]); lev=float(st["leverage"])
        slip=float(st["slippage"])
        notional=bal*pos_pct/100*lev
        entry = price*(1+slip) if action=="BUY" else price*(1-slip)
        tp_pct=float(st["tp_pct"])/100; sl_pct=float(st["sl_pct"])/100
        if action=="BUY":
            tp=entry*(1+tp_pct); sl=entry*(1-sl_pct)
        else:
            tp=entry*(1-tp_pct); sl=entry*(1+sl_pct)
        qty=notional/entry
        c.execute("""INSERT INTO trades(symbol,side,status,entry,qty,notional,tp,sl,leverage,open_time)
          VALUES(?,?,?,?,?,?,?,?,?,?)""",
          (symbol,"LONG" if action=="BUY" else "SHORT","OPEN",entry,qty,notional,tp,sl,lev,
           datetime.now(timezone.utc).isoformat()))
    c.commit(); c.close()
    return {"ok":True}

def close_trade(c, t, price, reason):
    st=get_settings()
    slip=float(st["slippage"]); fee=float(st["taker_fee"])
    exit_p = price*(1-slip) if t["side"]=="LONG" else price*(1+slip)
    direction = 1 if t["side"]=="LONG" else -1
    gross = (exit_p - t["entry"])*t["qty"]*direction
    fees = t["notional"]*fee*2
    funding = 0.0
    if st.get("funding_enabled")=="1":
        opened=datetime.fromisoformat(t["open_time"])
        hours=(datetime.now(timezone.utc)-opened).total_seconds()/3600
        funding = abs(t["notional"])*float(st["funding_rate"])*(hours/8)
    net = gross - fees - funding
    c.execute("""UPDATE trades SET status='CLOSED', exit=?, close_time=?, gross_pnl=?,
      fees=?, funding=?, net_pnl=?, reason=? WHERE id=?""",
      (exit_p, datetime.now(timezone.utc).isoformat(), gross, fees, funding, net, reason, t["id"]))

async def monitor():
    while True:
        try:
            c=db()
            opens=c.execute("SELECT * FROM trades WHERE status='OPEN'").fetchall()
            c.close()
            async with httpx.AsyncClient(timeout=10) as client:
                for t in opens:
                    sym=t["symbol"]
                    if not sym.endswith("USDT"): continue
                    r=await client.get(f"https://fapi.binance.com/fapi/v1/klines",
                        params={"symbol":sym,"interval":"1m","limit":100})
                    if r.status_code!=200: continue
                    for k in r.json():
                        high=float(k[2]); low=float(k[3])
                        hit_tp = high>=t["tp"] if t["side"]=="LONG" else low<=t["tp"]
                        hit_sl = low<=t["sl"] if t["side"]=="LONG" else high>=t["sl"]
                        if hit_sl:
                            c=db(); close_trade(c,t,t["sl"],"SL"); c.commit(); c.close(); break
                        if hit_tp:
                            c=db(); close_trade(c,t,t["tp"],"TP"); c.commit(); c.close(); break
        except Exception:
            pass
        await asyncio.sleep(30)

@app.on_event("startup")
async def start():
    asyncio.create_task(monitor())
