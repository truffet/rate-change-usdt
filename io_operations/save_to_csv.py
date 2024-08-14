import pandas as pd
import os

def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    if not os.path.exists('data'):
        os.makedirs('data')
    df.to_csv(filename, index=False)
    print(f"Data saved to '{filename}'")
