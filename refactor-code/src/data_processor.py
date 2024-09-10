# src/data_processor.py

import pandas as pd
from scipy.stats import zscore

class DataProcessor:
    
    def calculate_z_scores(self, data):
        """Calculate Z-scores for percentage change and volume (in USDT)."""
        # Ensure data is a pandas DataFrame
        df = pd.DataFrame(data)
        
        # Calculate Z-scores for the relevant columns
        df['z_pct_change'] = zscore(df['pct_change'])
        df['z_volume_usdt'] = zscore(df['volume_usdt'])
        
        return df

    def combine_z_scores(self, df):
        """Combine the Z-scores of percentage change and volume in USDT."""
        df['combined_z_score'] = df['z_pct_change'] * df['z_volume_usdt']
        return df
