---

# Rate-Change-USDT Project

## Overview
This project calculates and tracks rate changes for USDT trading pairs using Binance's API. It processes data, calculates metrics like Z-scores, stores results in a SQLite database, and sends Telegram notifications for significant events.

---

## Project Structure

### **Root Level**
- **`main.py`**: The entry point of the application. Orchestrates data fetching, processing, storage, and notifications.
- **`config.json`**: Stores configurations, such as Telegram credentials.
- **`.gitignore`**: Specifies files and folders to exclude from version control.
- **`LICENSE`**: Contains licensing information.
- **`README.md`**: Documentation for the project.

---

### **Directories**

#### **`db/`**
- **Purpose**: Stores the SQLite database file.
- **Contents**:
  - `trading_data.db`: The database file for storing candlestick and rate change data.

#### **`setup_scripts/`**
- **Purpose**: Scripts for database setup, cleanup, and task scheduling.
- **Contents**:
  - **`delete_db.py`**: Deletes the database file to reset data.
  - **`scheduler.py`**: Manages scheduled execution of scripts.
  - **`setup.sh`**: A shell script for initial project setup.
  - **`setup_db.py`**: Initializes the database and creates required tables.

#### **`src/`**
- **Purpose**: Contains core modules for API interaction, data processing, database handling, and notifications.
- **Contents**:
  - **`api/`**:
    - **`binance_api.py`**: Interacts with Binance's API to fetch candlestick and market data.
  - **`data_processing/`**:
    - **`processor.py`**: Processes raw trading data, calculates rate changes and Z-scores.
  - **`db_handler/`**:
    - **`database_handler.py`**: Manages SQLite database interactions.
  - **`notifications/`**:
    - **`telegram_bot.py`**: Sends notifications via Telegram.
  - **`config_loader.py`**: Loads and validates configurations from `config.json`.
  - **`utils.py`**: Reserved for shared utility functions (currently empty).

#### **`test_scripts/`**
- **Purpose**: Contains test scripts to verify the functionality of various modules.
- **Contents**:
  - **`check_data.py`**: Validates the integrity of processed data.
  - **`test_db_data.py`**: Tests database functionality, including table structure and content.
  - **`z_all_check.py`**: Verifies the correctness of Z-score calculations.

---

## Setup and Usage

### Prerequisites
- Python 3.8+

### Setup
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd rate-change-usdt
   ```
2. Run the setup script:
   ```bash
   bash setup_scripts/setup.sh
   python3 setup_scripts/setup_db.py
   ```

### Database Initialization
To create the database and tables:
```bash
python3 setup_scripts/setup_db.py
```

### Running the Application
To start processing data:
```bash
python3 main.py
```

### Testing
Run test scripts to verify functionality:
```bash
python3 test_scripts/test_db_data.py
python3 test_scripts/check_data.py
python3 test_scripts/z_all_check.py
```

---