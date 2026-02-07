import pandas as pd
import pymysql
from pathlib import Path
from typing import Dict

try:
    import yaml
except ImportError:
    yaml = None


def load_csv(path: str) -> pd.DataFrame:
    """Load CSV and normalize columns."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.date

    # Ensure numeric conversions for common columns
    for col in ['open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def read_db_config(path: str = "db_credentials.yml") -> Dict:
    """Read DB credentials from a YAML file.

    Raises ImportError if PyYAML isn't installed.
    """
    if yaml is None:
        raise ImportError("PyYAML is required to read DB config. Install with 'pip install pyyaml'.")
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"DB config not found at {path}")
    cfg = yaml.safe_load(p.read_text()) or {}

    # Basic validation
    for key in ('host', 'user', 'password', 'database'):
        if key not in cfg:
            raise KeyError(f"Missing '{key}' in DB config")

    return cfg


def upsert_btc_prices(df: pd.DataFrame, db_config: Dict):
    """Insert (delete+insert) rows into 'bronze_btc_price' table."""

    conn = pymysql.connect(
        host=db_config.get('host', 'localhost'),
        user=db_config.get('user'),
        password=db_config.get('password'),
        database=db_config.get('database'),
        port=int(db_config.get('port', 3306))
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_btc_price (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            open DECIMAL(15, 2),
            high DECIMAL(15, 2),
            low DECIMAL(15, 2),
            close DECIMAL(15, 2),
            volume BIGINT,
            dividends INT,
            stock_splits INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    for _, row in df.iterrows():
        cursor.execute("DELETE FROM bronze_btc_price WHERE date = %s", (row.get('date'),))
        cursor.execute("""
            INSERT INTO bronze_btc_price (
                date, open, high, low, close, volume, dividends, stock_splits
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.get('date'), row.get('open'), row.get('high'), row.get('low'),
            row.get('close'), int(row.get('volume') or 0),
            int(row.get('dividends') or 0), int(row.get('stock_splits') or 0)
        ))

    conn.commit()
    cursor.close()
    conn.close()


def main(csv_path: str = "raw_data/btc_usd_jan2026.csv", config_path: str = "db_credentials.yml"):
    df = load_csv(csv_path)
    cfg = read_db_config(config_path)
    upsert_btc_prices(df, cfg)
    print(f"Inserted {len(df)} rows into 'bronze_btc_price'")


if __name__ == "__main__":
    main()
