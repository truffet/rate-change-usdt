import pandas as pd

def main():
    df = pd.read_csv('data/market_data.csv')
    sorted_df = df.sort_values(by='pct_change', ascending=False)
    sorted_df.to_csv('data/sorted_market_data.csv', index=False)
    print("Sorted data saved to 'data/sorted_market_data.csv'")

if __name__ == "__main__":
    main()
