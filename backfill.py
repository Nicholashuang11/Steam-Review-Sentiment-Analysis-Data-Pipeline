from extract import fetch_reviews
import pandas as pd
from config import appid,backfill_max_review
from storage import clean_df, save_parquet

def backfilling():
    for id in appid:
        reviews = fetch_reviews(id, since_timestamp=0, since_reviewids=set(), max_reviews=backfill_max_review)
        if not reviews:
            print(f"no reviews for {id}")
            continue
        df = pd.DataFrame(reviews)
        df = clean_df(df)
        if df.empty:
            print(f"No new available reviews")
        save_parquet(df)
if __name__== "__main__":
    backfilling()

    