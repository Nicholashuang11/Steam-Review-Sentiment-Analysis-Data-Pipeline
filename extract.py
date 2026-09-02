import requests
import json
import time
from datetime import datetime, timezone
def fetch_reviews(appid: int, since_timestamp: int = 0, since_reviewids: set | None = None, max_reviews: int | None = None) -> list[dict]:
    since_reviewids = since_reviewids or set()
    url = f"https://store.steampowered.com/appreviews/{appid}"
    cursor = "*"
    collected = []
    while max_reviews is None or len(collected)<max_reviews:
        params = {
        "json": 1,
        "num_per_page": 50,
        "language": "english",
        "filter": "recent",
        "purchase_type": "all",
        "cursor":cursor
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"appid: {appid} request failed {e}")
            break

        data = response.json()
        reviews = data.get("reviews",[])
        if not reviews:
            break

        stop = False
        for r in reviews:
            ts = r.get("timestamp_created")
            revid=r.get("recommendationid")
            if ts<= since_timestamp:
                stop = True
                break
            if ts == since_timestamp and revid in since_reviewids:
                continue
            collected.append({
                    "reviewid":r.get("recommendationid"),
                    "appid": appid,
                    "review":r.get("review"),
                    "language":r.get("language"),
                    "timestamp_created":r.get("timestamp_created"),
                    "refunded":r.get("refunded"),
                    "extracted_at":datetime.now(timezone.utc).isoformat()
                })
            if max_reviews is not None and len(collected)>= max_reviews:
                stop = True
                break
        if stop:
            break
        new_cursor = data.get("cursor")
        if not new_cursor or new_cursor ==cursor:
            break
        cursor = new_cursor
        time.sleep(1)

    return collected


