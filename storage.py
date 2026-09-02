from extract import fetch_reviews
import os
import pandas as pd
from datetime import datetime,timezone
from config import  gamemetadata,output_dir,company_name

def clean_df(df: pd.DataFrame)->pd.DataFrame:
    df["game_name"]= df["appid"].map(lambda x: gamemetadata[x]["name"])
    df["publisher"]=company_name
    df= df[df['review'].str.strip().str.len()>2]
    df = df.drop_duplicates(subset="reviewid")
    df["timestamp_created"] = pd.to_datetime(df["timestamp_created"], unit="s", utc=True)
    return df
def save_parquet(df:pd.DataFrame):
    os.makedirs(output_dir,exist_ok=True)
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path= os.path.join(output_dir,f"dt={run_date}")
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        partition_cols=["appid"]
    )
    print(f"Saved {len(df)} rows to {output_path}")