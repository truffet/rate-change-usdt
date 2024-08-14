import pandas as pd

def sort_by_percentage_change(data):
    df = pd.DataFrame(data)
    sorted_df = df.sort_values(by='pct_change', ascending=False)
    return sorted_df
