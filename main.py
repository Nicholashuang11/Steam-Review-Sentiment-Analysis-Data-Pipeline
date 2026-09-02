import json
import os

import pandas as pd

from extract import fetch_reviews
from config import appid, incremental_max_reviews
from storage import clean_df, save_parquet


def run_incremental():

    watermarks = json.loads(os.environ.get("WATERMARKS_JSON", "{}"))

    for id in appid:
        wm = watermarks.get(str(id), {"last_timestamp": 0, "reviewids": []})
        since_ts = wm["last_timestamp"]
        since_ids = set(wm["reviewids"])

        print(f"appid={id}: fetching since ts={since_ts} ({len(since_ids)} known id(s) at that ts)...")

        reviews = fetch_reviews(id, since_timestamp=since_ts, since_reviewids=since_ids,
                                 max_reviews=incremental_max_reviews)

        if not reviews:
            print(f"  Nothing new for appid={id}.\n")
            continue

        df = pd.DataFrame(reviews)
        df = clean_df(df)

        if df.empty:
            print(f"  All new reviews were filtered out for appid={id}.\n")
            continue

        save_parquet(df)
        print(f"  +{len(df)} new reviews written to Parquet.\n")


if __name__ == "__main__":
    run_incremental()