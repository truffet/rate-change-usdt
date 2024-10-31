import pandas as pd

class DataProcessor:

    def filter_normal_data(self, df):
        """
        Filters the DataFrame to include only rows where all Z-scores have an absolute value less than 2.

        Args:
            df (pd.DataFrame): The input DataFrame with Z-score columns.

        Returns:
            pd.DataFrame: DataFrame containing only rows where all Z-scores are considered "normal" (absolute value < 2).
        """
        # Define the conditions for filtering "normal" data
        conditions = (
            (df['z_rate_change_open_close'].abs() < 2) &
            (df['z_rate_change_high_low'].abs() < 2) &
            (df['z_volume_pair'].abs() < 2) &
            (df['z_rate_change_open_close_all_pairs'].abs() < 2) &
            (df['z_rate_change_high_low_all_pairs'].abs() < 2) &
            (df['z_volume_all_pairs'].abs() < 2)
        )
        
        # Filter the DataFrame based on the conditions
        normal_data_df = df[conditions]

        print(f"Filtered down to {len(normal_data_df)} rows of normal data.")
        
        return normal_data_df
