# src/data_processor.py

import numpy as np

class DataProcessor:
    def calculate_rate_change(self, candle):
        """
        Calculates the percentage rate change between the open and close price of the given candlestick.

        Args:
            candle (list): Candlestick data where:
                candle[1] -> open price
                candle[4] -> close price

        Returns:
            float: The percentage rate change based on the open and close price,
                   or None if data is insufficient.
        """
        if not candle or len(candle) < 5:
            return None  # Invalid or incomplete candlestick data

        open_price = float(candle[1])  # Open price of the candlestick
        close_price = float(candle[4])  # Close price of the candlestick

        rate_change = ((close_price - open_price) / open_price) * 100  # Percentage rate change
        return round(rate_change, 2)  # Return the rounded rate change

    def calculate_z_scores(self, values):
        """
        Calculates z-scores for a list of values (e.g., rate changes or volumes).

        Args:
            values (list): List of values (floats).

        Returns:
            dict: Dictionary of z-scores, with each value mapped to its z-score.
        """
        if len(values) < 2:
            return {i: None for i in range(len(values))}  # Not enough data for z-scores

        value_array = np.array(values)

        # Calculate mean and standard deviation for values
        mean_value = np.mean(value_array)
        std_dev_value = np.std(value_array)

        if std_dev_value == 0:
            return {i: 0.0 for i in range(len(values))}  # If all values are identical, return 0

        z_scores = (value_array - mean_value) / std_dev_value
        return {i: round(z_scores[i], 2) for i in range(len(values))}

    def calculate_multiplied_z_scores(self, *z_scores_dicts):
        """
        Calculates the combined z-score by multiplying multiple z-scores from different sources.

        Args:
            *z_scores_dicts: Variable number of dictionaries containing z-scores to multiply.
                             Each dictionary should have z-scores for the same set of items.

        Returns:
            dict: Dictionary of multiplied z-scores.
        """
        multiplied_scores = {}

        # Iterate over the indices in the first z-score dictionary
        for i in z_scores_dicts[0].keys():
            multiplied_score = 1  # Start with 1, as we are multiplying
            for z_scores in z_scores_dicts:
                multiplied_score *= z_scores.get(i, 1)  # Multiply z-scores, default to 1 if missing
            multiplied_scores[i] = round(multiplied_score, 2)

        return multiplied_scores
