#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, time, logging
from datetime import datetime, timezone
from typing import Dict, Any, List
import requests, pandas as pd
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.oauth2 import service_account

API_KEY = os.environ["ODDS_API_KEY"]
SPORT = "icehockey_nhl"
REGIONS = "us,eu,us_ex"
ODDS_FORMAT = "american"
API_HOST = "https://api.the-odds-api.com/v4"
LOCAL_TZ = ZoneInfo("America/Toronto")
TIMEOUT = 30
MAX_RETRIES = 4
EVENT_CALL_DELAY_SEC = 0.18

PROJECT_ID = "nhl25-473523"
DATASET = "betting_odds"
TABLE = "nhl_player_odds_current"               # append-only
FULL_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE}"
def bq_client() -> bigquery.Client:
    # Cloud Run provides credentials automatically via the Job's Service Account
    return bigquery.Client(project=PROJECT_ID)

ALLOWED_BOOKMAKERS = {"BetMGM","BetRivers","DraftKings","Fanatics","FanDuel","Pinnacle"}

# Seed with bulk to get event ids cheaply
SEED_MARKETS = ["h2h"]

# ---- Market sets ----
CORE_PLAYER_MARKETS = [
    "player_points","player_power_play_points","player_assists",
    "player_blocked_shots","player_shots_on_goal","player_goals",
    "player_total_saves",
    "player_goal_scorer_first","player_goal_scorer_last","player_goal_scorer_anytime",
]

ALT_PLAYER_MARKETS = [
    "player_points_alternate","player_assists_alternate","player_power_play_points_alternate",
    "player_goals_alternate","player_shots_on_goal_alternate","player_blocked_shots_alternate",
    "player_total_saves_alternate",
]

# Final market list (dedup)
PLAYER_MARKETS = list(dict.fromkeys(CORE_PLAYER_MARKETS + ALT_PLAYER_MARKETS))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("nhl-props")

# ---------- HTTP ----------
def http_get(url: str, params: Dict[str, Any]) -> requests.Response:
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r
            log.warning("HTTP %s try %d: %s", r.status_code, i+1, r.text[:300])
        except requests.RequestException as e:
            log.warning("ReqEx try %d: %s", i+1, e)
        time.sleep(1.5*(i+1))
    r = requests.get(url, params=params, timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"GET failed {r.status_code}: {r.text[:500]}")
    return r

# ---------- Fetchers ----------
def fetch_seed_events() -> List[Dict[str, Any]]:
    url = f"{API_HOST}/sports/{SPORT}/odds"
    params = {"regions":REGIONS,"markets":",".join(SEED_MARKETS),"oddsFormat":ODDS_FORMAT,"apiKey":API_KEY}
    log.info("Seeding events via bulk %s", params["markets"])
    return http_get(url, params).json()

def fetch_event_props(event_id: str, markets: List[str]) -> Dict[str, Any]:
    url = f"{API_HOST}/sports/{SPORT}/events/{event_id}/odds"
    params = {"regions":REGIONS,"markets":",".join(markets),"oddsFormat":ODDS_FORMAT,"apiKey":API_KEY}
    return http_get(url, params).json()

# ---------- Helpers ----------
OVER_WORDS = {"over","o"}
UNDER_WORDS = {"under","u"}
YES_WORDS = {"yes","y"}
NO_WORDS = {"no","n"}

def side_from_outcome_name(name: str) -> str:
    if not name:
        return "UNKNOWN"
    low = name.strip().lower()
    if low in OVER_WORDS: return "OVER"
    if low in UNDER_WORDS: return "UNDER"
    if low in YES_WORDS: return "YES"
    if low in NO_WORDS: return "NO"
    return "PLAYER"  # goalscorer or rare labels

def extract_player(oc: Dict[str, Any], market_key: str) -> str:
    """
    Priority:
      1) description (common for O/U props)
      2) participant (some books)
      3) if goalscorer markets → outcome name when it's not Over/Under/Yes/No
    """
    if oc.get("description"):
        return str(oc["description"])
    if oc.get("participant"):
        return str(oc["participant"])
    if market_key.startswith("player_goal_scorer"):
        nm = str(oc.get("name", "")).strip()
        if side_from_outcome_name(nm) == "PLAYER":
            return nm
    return None

def base_market_key(mkey: str) -> str:
    # normalize to the non-alternate family key
    return mkey[:-10] if mkey.endswith("_alternate") else mkey

# ---------- Normalize ----------
def normalize(event: Dict[str, Any]) -> pd.DataFrame:
    rows = []
    fetched_at = datetime.now(timezone.utc)

    ev_id = event["id"]
    commence = datetime.fromisoformat(event["commence_time"].replace("Z","+00:00")).astimezone(timezone.utc)
    commence_local = commence.astimezone(LOCAL_TZ)
    home, away = event["home_team"], event["away_team"]
    sport_key = event.get("sport_key","icehockey_nhl")

    for bm in event.get("bookmakers", []):
        if bm.get("title") not in ALLOWED_BOOKMAKERS:
            continue
        bm_last = bm.get("last_update")
        bm_last_dt = datetime.fromisoformat(bm_last.replace("Z","+00:00")).astimezone(timezone.utc) if bm_last else None

        for mk in bm.get("markets", []):
            mkey = mk.get("key")
            if not mkey or not mkey.startswith("player_"):
                continue

            mlast = mk.get("last_update")
            mlast_dt = datetime.fromisoformat(mlast.replace("Z","+00:00")).astimezone(timezone.utc) if mlast else None

            is_alt = mkey.endswith("_alternate")
            fam_key = base_market_key(mkey)

            for oc in mk.get("outcomes", []):
                player = extract_player(oc, mkey)
                outcome_name = oc.get("name")
                side = side_from_outcome_name(outcome_name or "")

                rows.append({
                    "fetch_ts_utc": fetched_at,
                    "event_id": ev_id,
                    "sport_key": sport_key,
                    "commence_time_utc": commence,
                    "commence_time_local": commence_local,
                    "home_team": home,
                    "away_team": away,
                    "bookmaker_key": bm.get("key"),
                    "bookmaker_title": bm.get("title"),
                    "bookmaker_last_update_utc": bm_last_dt,
                    "market_key": mkey,                           # exact key from API
                    "base_market_key": fam_key,                   # normalized family (no _alternate)
                    "is_alternate": is_alt,                       # TRUE for alternate ladders
                    "market_last_update_utc": mlast_dt,
                    "player": player,
                    "outcome_name": outcome_name,
                    "outcome_side": side,                         # OVER/UNDER/YES/NO/PLAYER
                    "price_american": oc.get("price"),
                    "point": oc.get("point") if "point" in oc else None,
                    "regions_requested": REGIONS,
                    "odds_format": ODDS_FORMAT,
                })
    return pd.DataFrame(rows)

# ---------- BigQuery ----------
def bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(SA_PATH)
    return bigquery.Client(project=PROJECT_ID, credentials=creds)

def ensure_table_simple(client: bigquery.Client):
    schema = [
        bigquery.SchemaField("fetch_ts_utc","TIMESTAMP"),
        bigquery.SchemaField("event_id","STRING"),
        bigquery.SchemaField("sport_key","STRING"),
        bigquery.SchemaField("commence_time_utc","TIMESTAMP"),
        bigquery.SchemaField("commence_time_local","TIMESTAMP"),
        bigquery.SchemaField("home_team","STRING"),
        bigquery.SchemaField("away_team","STRING"),
        bigquery.SchemaField("bookmaker_key","STRING"),
        bigquery.SchemaField("bookmaker_title","STRING"),
        bigquery.SchemaField("bookmaker_last_update_utc","TIMESTAMP"),
        bigquery.SchemaField("market_key","STRING"),
        bigquery.SchemaField("base_market_key","STRING"),
        bigquery.SchemaField("is_alternate","BOOL"),
        bigquery.SchemaField("market_last_update_utc","TIMESTAMP"),
        bigquery.SchemaField("player","STRING"),
        bigquery.SchemaField("outcome_name","STRING"),
        bigquery.SchemaField("outcome_side","STRING"),
        bigquery.SchemaField("price_american","INT64"),
        bigquery.SchemaField("point","FLOAT64"),
        bigquery.SchemaField("regions_requested","STRING"),
        bigquery.SchemaField("odds_format","STRING"),
    ]
    try:
        client.get_table(FULL_TABLE)
        logging.info("Table exists: %s", FULL_TABLE)
    except Exception:
        client.create_table(bigquery.Table(FULL_TABLE, schema=schema))
        logging.info("Created table (no partitioning): %s", FULL_TABLE)

def upload_append(client: bigquery.Client, df: pd.DataFrame):
    if df.empty:
        logging.warning("No rows to upload."); return
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_dataframe(df, FULL_TABLE, job_config=job_config)
    job.result()
    logging.info("Uploaded %d rows → %s", len(df), FULL_TABLE)

# ---------- Main ----------
def main():
    logging.info("🎯 NHL player props (core + alternates) → BQ (append-only)")
    client = bq_client(); ensure_table_simple(client)

    events = fetch_seed_events()
    logging.info("Events: %d", len(events))

    total = 0
    for ev in events:
        try:
            payload = fetch_event_props(ev["id"], PLAYER_MARKETS)
            event_full = {**ev, "bookmakers": payload.get("bookmakers", [])}
            df = normalize(event_full)
            upload_append(client, df)
            total += len(df)
        except Exception as e:
            logging.warning("props fetch failed %s: %s", ev.get("id"), e)
        time.sleep(EVENT_CALL_DELAY_SEC)

    logging.info("✅ Done. Rows uploaded: %d", total)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
