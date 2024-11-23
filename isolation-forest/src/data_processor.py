import pandas as pd
import logging

class DataProcessor:
    def extract_normal_data(self, df):
        """
        Filters the DataFrame to include only rows where all Z-scores have an absolute value less than 2.
        """
        conditions = (
            (df['z_rate_change_open_close'].abs() < 2) &
            (df['z_rate_change_high_low'].abs() < 2) &
            (df['z_volume_pair'].abs() < 2) &
            (df['z_rate_change_open_close_all_pairs'].abs() < 2) &
            (df['z_rate_change_high_low_all_pairs'].abs() < 2) &
            (df['z_volume_all_pairs'].abs() < 2)
        )
        normal_data_df = df[conditions]
        logging.info(f"Filtered down to {len(normal_data_df)} rows of normal data from {len(df)} total rows.")
        return normal_data_df

    def get_new_data(self, df, last_open_time):
        """
        Filters the DataFrame to include only new rows with open_time greater than the last recorded open_time.
        """
        df_new_data = df[df['open_time'] > last_open_time]
        logging.info(f"Identified {len(df_new_data)} new rows since last open_time: {last_open_time}")

        # Extract only the normal data from the new entries
        df_normal_data = self.extract_normal_data(df_new_data)
        
        return df_normal_data
