import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from datetime import datetime
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import logging

def main():
    # Update this with your actual database connection details
    connection_string = "postgresql://postgres:chriscakra15@localhost:5432/staging"
    
    try:
        # Initialize loader
        loader = DataWarehouseLoader(connection_string)
        # ... rest of the code ...
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataWarehouseLoader:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        
    def load_dimensions(self):
        """Load data into dimension tables"""
        with self.engine.connect() as conn:
            # Load dim_time
            logger.info("Loading dim_time...")
            conn.execute("""
                INSERT INTO dwh.dim_time (time_id, timestamp, year, month, day, hour)
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY timestamp) as time_id,
                    timestamp,
                    EXTRACT(YEAR FROM timestamp) as year,
                    EXTRACT(MONTH FROM timestamp) as month,
                    EXTRACT(DAY FROM timestamp) as day,
                    EXTRACT(HOUR FROM timestamp) as hour
                FROM (
                    SELECT DISTINCT timestamp FROM staging.staging_external_sentiment
                    UNION
                    SELECT DISTINCT timestamp FROM staging.staging_warehouse_temp_sensor
                ) t
                ON CONFLICT (timestamp) DO NOTHING;
            """)

            # Load dim_sensor
            logger.info("Loading dim_sensor...")
            conn.execute("""
                INSERT INTO dwh.dim_sensor (sensor_id, temperature_c)
                SELECT DISTINCT sensor_id, temperature_c
                FROM staging.staging_warehouse_temp_sensor
                ON CONFLICT (sensor_id) DO UPDATE 
                SET temperature_c = EXCLUDED.temperature_c;
            """)

            # Load dim_tweet
            logger.info("Loading dim_tweet...")
            conn.execute("""
                INSERT INTO dwh.dim_tweet (tweet_id, author_id, tweet_text)
                SELECT DISTINCT tweet_id, user_location, tweet_text
                FROM staging.staging_external_sentiment
                ON CONFLICT (tweet_id) DO UPDATE 
                SET tweet_text = EXCLUDED.tweet_text;
            """)

            # Load dim_topic
            logger.info("Loading dim_topic...")
            conn.execute("""
                INSERT INTO dwh.dim_topic (topic_id, keyword)
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY keyword) as topic_id,
                    keyword
                FROM (
                    SELECT DISTINCT UNNEST(STRING_TO_ARRAY(matched_product, ',')) as keyword
                    FROM staging.staging_external_sentiment
                    WHERE matched_product IS NOT NULL
                ) t
                ON CONFLICT (keyword) DO NOTHING;
            """)

            # Load dim_competitor
            logger.info("Loading dim_competitor...")
            conn.execute("""
                INSERT INTO dwh.dim_competitor (competitor_id, company)
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY company) as competitor_id,
                    company
                FROM staging.staging_market_share_report
                ON CONFLICT (company) DO NOTHING;
            """)

    def load_facts(self):
        """Load data into fact tables"""
        with self.engine.connect() as conn:
            # Load fact_competitor_share
            logger.info("Loading fact_competitor_share...")
            conn.execute("""
                INSERT INTO dwh.fact_competitor_share (competitor_id, market_share_percent, time_id)
                SELECT 
                    c.competitor_id,
                    m.market_share_percent,
                    t.time_id
                FROM staging.staging_market_share_report m
                JOIN dwh.dim_competitor c ON m.company = c.company
                CROSS JOIN (
                    SELECT time_id 
                    FROM dwh.dim_time 
                    WHERE timestamp = (SELECT MAX(timestamp) FROM dwh.dim_time)
                    LIMIT 1
                ) t
                ON CONFLICT (competitor_id, time_id) 
                DO UPDATE SET market_share_percent = EXCLUDED.market_share_percent;
            """)

            # Load fact_sentiment
            logger.info("Loading fact_sentiment...")
            conn.execute("""
                INSERT INTO dwh.fact_sentiment (tweet_id, topic_id, time_id, polarity)
                SELECT 
                    s.tweet_id,
                    t.topic_id,
                    ti.time_id,
                    CASE 
                        WHEN s.sentiment = 'positive' THEN 1
                        WHEN s.sentiment = 'negative' THEN -1
                        ELSE 0
                    END as polarity
                FROM staging.staging_external_sentiment s
                CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(s.matched_product, ',')) as topic(keyword)
                JOIN dwh.dim_topic t ON t.keyword = topic.keyword
                JOIN dwh.dim_time ti ON ti.timestamp = s.timestamp
                ON CONFLICT (tweet_id, topic_id, time_id) DO NOTHING;
            """)

            # Load fact_temperature
            logger.info("Loading fact_temperature...")
            conn.execute("""
                INSERT INTO dwh.fact_temperature (sensor_id, time_id, temperature_c)
                SELECT 
                    s.sensor_id,
                    t.time_id,
                    s.temperature_c
                FROM staging.staging_warehouse_temp_sensor s
                JOIN dwh.dim_time t ON t.timestamp = s.timestamp
                ON CONFLICT (sensor_id, time_id) 
                DO UPDATE SET temperature_c = EXCLUDED.temperature_c;
            """)

    def generate_temperature_plot(self, output_dir='output'):
        """Generate temperature trend plot"""
        with self.engine.connect() as conn:
            # Get temperature data
            df = pd.read_sql("""
                SELECT 
                    t.timestamp,
                    AVG(ft.temperature_c) as avg_temperature
                FROM dwh.fact_temperature ft
                JOIN dwh.dim_time t ON ft.time_id = t.time_id
                GROUP BY t.timestamp
                ORDER BY t.timestamp
            """, conn)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Create plot
        plt.figure(figsize=(12, 6))
        plt.plot(df['timestamp'], df['avg_temperature'], marker='o')
        plt.title('Average Temperature Trend Over Time')
        plt.xlabel('Time')
        plt.ylabel('Temperature (°C)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save plot
        plot_path = os.path.join(output_dir, 'temperature_trend.png')
        plt.savefig(plot_path)
        plt.close()
        logger.info(f"Temperature plot saved to {plot_path}")

    def generate_wordcloud(self, output_dir='output'):
        """Generate word cloud from tweet text"""
        with self.engine.connect() as conn:
            # Get tweet text
            df = pd.read_sql("""
                SELECT tweet_text 
                FROM dwh.dim_tweet
            """, conn)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Generate word cloud
        text = ' '.join(df['tweet_text'].dropna())
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
        
        # Save word cloud
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('Word Cloud of Tweets')
        
        wordcloud_path = os.path.join(output_dir, 'tweet_wordcloud.png')
        plt.savefig(wordcloud_path, bbox_inches='tight')
        plt.close()
        logger.info(f"Word cloud saved to {wordcloud_path}")

def main():
    # Replace with your actual connection string
    connection_string = "postgresql://username:password@localhost:5432/your_database"
    
    try:
        # Initialize loader
        loader = DataWarehouseLoader(connection_string)
        
        # Load data
        loader.load_dimensions()
        loader.load_facts()
        
        # Generate visualizations
        loader.generate_temperature_plot()
        loader.generate_wordcloud()
        
        logger.info("Data loading and visualization completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()