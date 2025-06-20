import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# Database connection
STAGGING_DB = 'postgresql://postgres:chriscakra15@localhost:5432/stagingdb'
engine = create_engine(STAGGING_DB)

# Directory paths
DATA_DIR = Path(__file__).parent / "adventureworks"

# Load warehouse temperature sensor data
df_sensor = pd.read_csv(DATA_DIR / "files" / "warehouse_temp_sensor.csv")
print("Loading warehouse temperature sensor data...")
df_sensor.to_sql('warehouse_temp_sensor', engine, if_exists='replace', index=False)

# Load market share report data
df_market = pd.read_csv(DATA_DIR / "files" / "market_share_report.csv")
print("Loading market share report data...")
df_market.to_sql('market_share_report', engine, if_exists='replace', index=False)

# Load tweet data
try:
    print("\nLoading tweet data...")
    tweet_file = DATA_DIR / "tweets" / "adventureworks_structured_150_tweets.txt"
    
    if not tweet_file.exists():
        print(f"[ERROR] File {tweet_file} tidak ditemukan!")
        exit(1)
        
    # Read tweet data with proper text parsing
    df_tweets = pd.read_csv(tweet_file, sep='\t', encoding='utf-8')
    print(f"\nJumlah tweet yang dibaca: {len(df_tweets)}")
    
    # Rename columns to match database structure
    df_tweets.columns = ['tweet_id', 'text', 'created_at', 'location', 'sentiment', 'product_category']
    
    # Convert tweet_id to integer
    df_tweets['tweet_id'] = df_tweets['tweet_id'].astype(int)
    
    # Load to staging database
    print("\nMemuat tweet ke staging database...")
    df_tweets.to_sql('tweets', engine, if_exists='replace', index=False, method='multi')
    print(f"Berhasil memuat {len(df_tweets)} tweet ke staging database")
    
except Exception as e:
    print(f"[ERROR] Error memuat tweet: {str(e)}")
    exit(1)

print("\nAll data loaded successfully!")
