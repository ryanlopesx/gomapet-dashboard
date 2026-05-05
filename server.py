#!/usr/bin/env python3
"""
GomaPet Dashboard — servidor local + proxy Meta Ads API
Uso: python3 server.py   →  http://localhost:8765
"""
import http.server, json, urllib.request, urllib.parse, os, time, re, threading
from socketserver import ThreadingMixIn

PORT            = int(os.environ.get("PORT", 8765))
TOKEN           = os.environ.get("META_TOKEN", "")
DEFAULT_ACCOUNT = os.environ.get("META_ACCOUNT_ID", "act_577944235396417")
API_VER         = "v19.0"
BASE            = f"https://graph.facebook.com/{API_VER}"
CACHE           = {}
TTL             = 7200  # 2h
PREWARM_DONE    = threading.Event()  # set when first cache load completes


def meta_get(path, params={}):
    p   = {"access_token": TOKEN, **params}
    url = f"{BASE}/{path}?{urllib.parse.urlencode(p)}"
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.loads(r.read())


def cached(key, fn):
    now = time.time()
    if key in CACHE and now - CACHE[key][0] < TTL:
        return CACHE[key][1]
    data = fn()
    CACHE[key] = (now, data)
    return data


def clean_name(name):
    name = re.sub(r'\[\d{2}/\d{2}/\d{4}\]\s*', '', name)
    name = re.sub(r'\s*-\s*\d{2}/\d{2}/\d{2,4}', '', name)         # - 17/03/25 anywhere
    name = re.sub(r'\s*[-—–]\s*Athenis.*', '', name)
    name = re.sub(r'Avant\s*-\s*\[CAMPANHA\s*\d+\]', 'Avant', name)
    name = re.sub(r'\[CTV[\w\s,]+\]', '', name)
    name = re.sub(r'\[CHIP\s+(\d+)\]', r'#\1', name)
    name = re.sub(r'\[(\d+)\]', r'#\1', name)
    name = re.sub(r'\[([A-Z][A-Z0-9\s]+)\]', r'\1', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:30] if len(name) > 30 else name


def find_action(arr, types):
    for item in (arr or []):
        if item.get("action_type") in types:
            return float(item.get("value", 0))
    return 0.0


PURCHASE_TYPES = {"purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase",
                  "web_in_store_purchase", "onsite_web_purchase"}


def segment_camp(name):
    """Classify a campaign name into a segment."""
    nl = name.lower()
    if any(k in nl for k in ["avant", "bafisco", "upsell", "cross", "kit", "premium"]):
        return "upsell"
    if any(k in nl for k in ["semelhante", "lookalike", "lal", "similar"]):
        return "lookalike"
    if any(k in nl for k in ["retarget", "remarket", "recompra", "fideliz", "cliente"]):
        return "retargeting"
    return "prospecting"


def seg_agg(raw_list):
    """Aggregate spend/rev/conv/msgs for a list of raw campaign records."""
    spend = sum(float(c.get("spend", 0)) for c in raw_list)
    rev   = sum(find_action(c.get("action_values", []),
                            {"omni_purchase", "onsite_web_app_purchase", "purchase"})
                for c in raw_list)
    conv  = sum(find_action(c.get("actions", []), PURCHASE_TYPES) for c in raw_list)
    msgs  = sum(find_action(c.get("actions", []),
                            {"onsite_conversion.total_messaging_connection"})
                for c in raw_list)
    roas  = round(rev / spend, 2) if spend else 0
    cpa   = round(spend / conv, 2) if conv else 0
    ticket = round(rev / conv, 2) if conv else 0
    return {
        "spend": round(spend, 2), "rev": round(rev, 2),
        "conv": int(conv), "msgs": int(msgs),
        "roas": roas, "cpa": cpa, "ticket": ticket,
        "count": len(raw_list),
    }


def build_payload(account_id):
    act = account_id if account_id.startswith("act_") else f"act_{account_id}"

    # ── Account info ───────────────────────────────────────────────────────
    acct = meta_get(act, {"fields": "id,name,currency,timezone_name"})

    # ── Account-level totals (funnel) ──────────────────────────────────────
    acct_ins = meta_get(f"{act}/insights", {
        "date_preset": "last_30d",
        "level":       "account",
        "fields":      "spend,impressions,clicks,ctr,cpc,actions,action_values,purchase_roas",
    })["data"][0]

    def act_val(key):
        return find_action(acct_ins.get("actions", []), {key})

    total_spend       = float(acct_ins["spend"])
    total_impressions = int(acct_ins["impressions"])
    total_clicks      = int(acct_ins["clicks"])
    total_ctr         = float(acct_ins["ctr"])
    total_purchases   = act_val("purchase")
    total_rev         = find_action(acct_ins.get("action_values", []),
                                    {"omni_purchase", "onsite_web_app_purchase", "onsite_web_purchase"})

    # WhatsApp funnel stages from messaging actions
    link_clicks     = act_val("link_click")
    msg_connection  = act_val("onsite_conversion.total_messaging_connection")
    msg_started_7d  = act_val("onsite_conversion.messaging_conversation_started_7d")
    first_reply     = act_val("onsite_conversion.messaging_first_reply")
    depth_2         = act_val("onsite_conversion.messaging_user_depth_2_message_send")
    depth_3         = act_val("onsite_conversion.messaging_user_depth_3_message_send")
    depth_5         = act_val("onsite_conversion.messaging_user_depth_5_message_send")
    init_checkout   = act_val("initiate_checkout") or act_val("omni_initiated_checkout")
    add_payment     = act_val("add_payment_info")

    whatsapp_funnel = [
        {"l": "Clique no anúncio (Link Click)",      "n": int(link_clicks)},
        {"l": "Iniciou conversa no WhatsApp",        "n": int(msg_connection)},
        {"l": "Conversa iniciada (7d)",              "n": int(msg_started_7d)},
        {"l": "Lead respondeu (1ª resposta)",        "n": int(first_reply)},
        {"l": "Engajamento médio (2+ msgs)",         "n": int(depth_2)},
        {"l": "Engajamento alto (3+ msgs)",          "n": int(depth_3)},
        {"l": "Iniciou checkout",                    "n": int(init_checkout)},
        {"l": "Adicionou pagamento",                 "n": int(add_payment)},
        {"l": "Compra finalizada",                   "n": int(total_purchases)},
    ]

    # ── Campaign insights ──────────────────────────────────────────────────
    camps_raw = meta_get(f"{act}/insights", {
        "date_preset": "last_30d",
        "level":       "campaign",
        "fields":      "campaign_name,campaign_id,spend,impressions,clicks,ctr,cpc,actions,action_values,purchase_roas",
        "limit":       30,
    })["data"]

    campaigns = []
    for c in camps_raw:
        spend = float(c.get("spend", 0))
        conv  = find_action(c.get("actions", []), PURCHASE_TYPES)
        rev   = find_action(c.get("action_values", []),
                            {"omni_purchase", "onsite_web_app_purchase", "purchase"})
        roas_arr = c.get("purchase_roas", [])
        roas  = float(roas_arr[0]["value"]) if roas_arr else (rev / spend if spend else 0)
        cpa   = round(spend / conv) if conv else 0
        ctr   = float(c.get("ctr", 0))
        msgs  = find_action(c.get("actions", []),
                            {"onsite_conversion.total_messaging_connection"})
        campaigns.append({
            "id":   c.get("campaign_id"),
            "n":    clean_name(c.get("campaign_name", "Campanha")),
            "inv":  round(spend, 2),
            "rev":  round(rev, 2),
            "roas": round(roas, 2),
            "cpa":  round(cpa, 2),
            "ctr":  round(ctr, 2),
            "conv": int(conv),
            "msgs": int(msgs),
        })
    campaigns.sort(key=lambda x: x["rev"], reverse=True)

    # ── Campaign segmentation ──────────────────────────────────────────────
    seg_buckets = {"upsell": [], "lookalike": [], "retargeting": [], "prospecting": []}
    for c in camps_raw:
        seg_buckets[segment_camp(c.get("campaign_name", ""))].append(c)

    segments = {k: seg_agg(v) for k, v in seg_buckets.items()}

    # Tag each processed campaign with its segment
    for camp in campaigns:
        raw = next((c for c in camps_raw if c.get("campaign_id") == camp["id"]), {})
        camp["seg"] = segment_camp(raw.get("campaign_name", ""))

    # ── Daily insights ─────────────────────────────────────────────────────
    daily_raw = meta_get(f"{act}/insights", {
        "date_preset":   "last_30d",
        "level":         "account",
        "time_increment": "1",
        "fields":        "date_start,spend,actions,action_values",
        "limit":         31,
    })["data"]

    daily = []
    for d in sorted(daily_raw, key=lambda x: x.get("date_start", "")):
        sp  = float(d.get("spend", 0))
        rev = find_action(d.get("action_values", []),
                          {"omni_purchase", "onsite_web_app_purchase", "onsite_web_purchase"})
        pur = find_action(d.get("actions", []), PURCHASE_TYPES)
        msg = find_action(d.get("actions", []),
                          {"onsite_conversion.total_messaging_connection"})
        daily.append({
            "date":  d["date_start"][5:],   # MM-DD
            "spend": round(sp, 2),
            "rev":   round(rev, 2),
            "pur":   int(pur),
            "msgs":  int(msg),
        })

    roas_overall  = round(total_rev / total_spend, 2) if total_spend else 0
    cpa_overall   = round(total_spend / total_purchases, 2) if total_purchases else 0
    avg_ctr       = round(sum(c["ctr"] for c in campaigns) / len(campaigns), 2) if campaigns else 0
    avg_ticket    = round(total_rev / total_purchases, 2) if total_purchases else 0
    cpl           = round(total_spend / msg_connection, 2) if msg_connection else 0
    lead_conv_rate = round(total_purchases / msg_connection * 100, 2) if msg_connection else 0
    recontact_pool = int(msg_connection) - int(total_purchases)

    # Daily purchase variance (for cycle/pattern analysis)
    pur_days   = [d["pur"] for d in daily]
    avg_pur_day = round(sum(pur_days) / len(pur_days), 1) if pur_days else 0
    total_msgs_daily = sum(d["msgs"] for d in daily)

    return {
        "ok":      True,
        "account": {
            "id":       acct.get("id"),
            "name":     acct.get("name"),
            "currency": acct.get("currency"),
            "bm":       "Gomapet Bm Americana",
        },
        "summary": {
            "spend":           round(total_spend, 2),
            "rev":             round(total_rev, 2),
            "conv":            int(total_purchases),
            "roas":            roas_overall,
            "cpa":             round(cpa_overall, 2),
            "ctr":             round(total_ctr, 2),
            "impressions":     total_impressions,
            "clicks":          total_clicks,
            "avg_ctr":         avg_ctr,
            "avg_ticket":      avg_ticket,
            "cpl":             cpl,
            "lead_conv_rate":  lead_conv_rate,
            "recontact_pool":  recontact_pool,
            "total_msgs":      int(msg_connection),
            "avg_pur_day":     avg_pur_day,
        },
        "campaigns":       campaigns[:10],
        "segments":        segments,
        "daily":           daily,
        "whatsapp_funnel": whatsapp_funnel,
        "fetched_at":      int(time.time()),
    }


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=os.path.dirname(os.path.abspath(__file__)), **kw)

    def log_message(self, fmt, *args):
        ts = time.strftime("%H:%M:%S")
        path = args[0] if args else ""
        if "/api/" in str(path):
            print(f"  [{ts}] API {args[1] if len(args)>1 else ''} {path[:80]}")

    def do_GET(self):
        if self.path.startswith("/api/ping"):
            self._json({"ok": True, "ready": PREWARM_DONE.is_set()})
            return

        if self.path.startswith("/api/meta-ads"):
            qs     = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            act_id = (qs.get("account_id") or [DEFAULT_ACCOUNT])[0].strip() or DEFAULT_ACCOUNT
            force  = "force" in qs
            if force and f"meta_{act_id}" in CACHE:
                del CACHE[f"meta_{act_id}"]
            # Wait for pre-warm to finish (avoids duplicate Meta API calls on startup)
            if not PREWARM_DONE.is_set():
                PREWARM_DONE.wait(timeout=60)
            try:
                data = cached(f"meta_{act_id}", lambda: build_payload(act_id))
                self._json(data)
            except Exception as e:
                print(f"  [ERR] Meta API: {e}")
                self._json({"ok": False, "error": str(e)}, 502)
            return

        if self.path.startswith("/api/refresh"):
            act_id = DEFAULT_ACCOUNT
            if f"meta_{act_id}" in CACHE:
                del CACHE[f"meta_{act_id}"]
            try:
                data = build_payload(act_id)
                CACHE[f"meta_{act_id}"] = (time.time(), data)
                self._json(data)
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 502)
            return

        super().do_GET()

    def _json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def _prewarm():
    """Fetch Meta data in background so first browser hit is instant."""
    try:
        time.sleep(0.5)  # let server bind first
        print(f"  [pre-warm] Carregando dados da Meta Ads…")
        data = build_payload(DEFAULT_ACCOUNT)
        CACHE[f"meta_{DEFAULT_ACCOUNT}"] = (time.time(), data)
        print(f"  [pre-warm] OK — ROAS {data['summary']['roas']}× · {data['summary']['conv']} conversões")
    except Exception as e:
        print(f"  [pre-warm] Erro: {e}")
    finally:
        PREWARM_DONE.set()  # unblock any waiting requests even if pre-warm failed


class ThreadingHTTPServer(ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Erro: variável META_TOKEN não definida.")
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f"\n  ╔══════════════════════════════════════════╗")
    print(f"  ║  GomaPet Dashboard  →  localhost:{PORT}    ║")
    print(f"  ║  Conta: Gomapet Americana 1 (USD)        ║")
    print(f"  ║  BM: Gomapet Bm Americana                ║")
    print(f"  ╚══════════════════════════════════════════╝")
    print(f"\n  Ctrl+C para parar\n")
    threading.Thread(target=_prewarm, daemon=True).start()
    server = ThreadingHTTPServer(("", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Servidor parado.")
