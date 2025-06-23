import io
import re
from collections import Counter
import numpy as np

def get_sensor_data(days: int = 7):
    """Fetch sensor data for the specified number of days."""
    query = f"""
    SELECT 
        ds.sensor_id,
        ds.temperature_c as temperature,
        ds.location,
        ft.timestamp,
        dt.date,
        dt.hour
    FROM dwh.fact_temperature ft
    JOIN dwh.dim_sensor ds ON ft.sensor_id = ds.sensor_id
    JOIN dwh.dim_time dt ON ft.time_id = dt.time_id
    WHERE dt.date >= CURRENT_DATE - INTERVAL '{days} days'
    ORDER BY ft.timestamp DESC
    """
    return pd.read_sql(query, engine)

    