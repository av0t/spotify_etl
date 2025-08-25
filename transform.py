# transformations to be applied on dataframe
# Note: date column has already been transformed into standard YYYY-MM-DD format

import numpy as np

def transform_dataset(df):
    # convert duration_ms into minutes
    minutes = df['duration_ms'] // 60000  # Get whole minutes
    seconds = (df['duration_ms'] % 60000) / 1000  # Get remaining seconds
    df['duration_ms'] = (minutes + (seconds / 100)).round(2)  # Combine as MM.SS decimal

    df.rename(columns={'duration_ms': 'duration_min', 'artist(s)_name':'artist_names'}, inplace=True)

    # Spotify API doesn't provide the number of streams of track, but it feels important 
    # Here we generate dummy values using popularity
    df['total_streams'] = (
        # Base: squared popularity for exponential effect
        (df['popularity'] ** 2) * 10000 + 
        # Add random variation (±50% of base)
        np.random.randint(-500000, 500000, len(df))).clip(lower=50000).astype(int) 

    # Spotify API has recently deprecated 'get track's audio features' from the API
    # Here we generate dummy values using popularity
    df['danceability'] = (
        # Base: use popularity directly as base
        df['popularity'] * 0.8 +  # Scale to max 80 to leave room for randomness
        # Add random variation (±20)
        np.random.uniform(-20, 20, len(df))).clip(lower=25, upper=100).round(1).astype(int)  # Keep within 0-100 range and convert to integer

    # similarly, for Tempo a.k.a BPM (Beats Per Minute)
    # Generate tempo based on popularity (50-200 BPM)
    df['tempo'] = (
        # Base: map popularity to tempo range (50-160 BPM)
        50 + (df['popularity'] / 100) * 80 +  # Base range from 50 to 130 BPM
        # Add random variation (±40 BPM)
        np.random.uniform(-40, 40, len(df))).clip(lower=50, upper=200).round(3)  # Keep within reasonable BPM range

    return df

