import pandas as pd

def main():
    df = pd.read_csv('data/market_data.csv')
    print("Data loaded from 'data/market_data.csv'")  # Console print for debugging

    sorted_df = df.sort_values(by='pct_change', ascending=False)
    print("Data sorted by percentage change")  # Console print for debugging

    sorted_df.to_csv('data/sorted_market_data.csv', index=False)
    print("Sorted data saved to 'data/sorted_market_data.csv'")  # Console print for debugging

if __name__ == "__main__":
    main()
