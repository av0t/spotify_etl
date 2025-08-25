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

    return df

