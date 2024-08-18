# Trend Analysis Simplified

This project automates the process of fetching and analyzing the latest completed 4-hour market data for USDT pairs from Binance. It calculates the percentage change between Open and Close prices, converts trading volumes to USDT, and ranks the pairs based on their performance metrics. The results are saved into CSV files for further analysis.

## Table of Contents
- [Introduction](#introduction)
- [Setup](#setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Introduction

The `Trend Analysis Simplified` tool is designed for traders and analysts who need to quickly assess the performance of USDT trading pairs on Binance. By automating the data fetching and processing, it saves time and provides a standardized approach to trend analysis.

## Setup

### Prerequisites
- Python 3.6 or higher
- pip (Python package installer)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/truffet/rate-change-usdt.git
   ```
2. Navigate to the project directory:
   ```bash
   cd rate-change-usdt
   ```
3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Ensure the configuration file (`config.json`) is set up with the desired settings.
2. Run the main script:
   ```bash
   python main.py
   ```
3. The script will fetch the latest market data, process it, and save the results in the `data/` directory as CSV files.
4. Check the `data` folder for the output files:
   - `rate_sorted_market_data.csv`: Data sorted by percentage change.
   - `z_sorted_market_data.csv`: Data sorted by combined Z-scores.

## Configuration

The project uses a `config.json` file to manage settings like the time interval for data fetching. The default configuration is:

```json
{
    "interval": "4h"
}
```

You can modify this file to change the interval or add other configurations.

## Contributing

Contributions are welcome! If you find a bug or have a feature request, please open an issue or submit a pull request. Ensure that your code follows the project's coding standards and includes tests for any new features.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.