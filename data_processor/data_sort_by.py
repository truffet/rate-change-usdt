import pandas as pd

def data_sort_by(data, param):
    df = pd.DataFrame(data)
    sorted_df = df.sort_values(by=param, ascending=False)
    return sorted_df
