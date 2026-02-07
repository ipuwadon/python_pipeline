"""Transform data from pipeline.bronze_btc_price into pipeline.silver_btc_price.

Transforms applied:
- avg_price = (high + low) / 2
- daily_return = (close / open) - 1
- ma_3, ma_7 = rolling means of 'close' over 3 and 7 days (sorted by date)
"""

from pathlib import Path
import logging
import argparse
from typing import Optional

import pandas as pd
import pymysql

from extract_data import read_db_config

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def _connect(db_cfg: dict):
    return pymysql.connect(
        host=db_cfg.get('host', 'localhost'),
        user=db_cfg.get('user'),
        password=db_cfg.get('password'),
        database=db_cfg.get('database'),
        port=int(db_cfg.get('port', 3306))
    )


def fetch_bronze(db_cfg: dict, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """Fetch rows from 'bronze_btc_price'. Returns a DataFrame indexed by date sorted ascending."""
    conn = _connect(db_cfg)
    try:
        sql = "SELECT date, `open`, `high`, `low`, `close`, `volume`, `dividends`, `stock_splits` FROM bronze_btc_price"
        params = []
        if start and end:
            sql += " WHERE date BETWEEN %s AND %s"
            params = [start, end]
        elif start:
            sql += " WHERE date >= %s"
            params = [start]
        elif end:
            sql += " WHERE date <= %s"
            params = [end]

        sql += " ORDER BY date ASC"
        df = pd.read_sql(sql, conn, params=params, parse_dates=['date'])
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        # Ensure numeric types
        for c in ['open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        return df
    finally:
        conn.close()


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations and add derived columns."""
    if df.empty:
        return df

    df = df.copy()
    df = df.sort_values('date')

    # compute average price
    df['avg_price'] = (df['high'] + df['low']) / 2

    # daily return; guard divide-by-zero
    df['daily_return'] = None
    mask = df['open'].notna() & (df['open'] != 0)
    df.loc[mask, 'daily_return'] = (df.loc[mask, 'close'] / df.loc[mask, 'open']) - 1

    # percent change of close (returns) and rolling stats
    df['return_pct'] = df['close'].pct_change()

    # simple moving averages
    df['ma_3'] = df['close'].rolling(window=3, min_periods=1).mean()
    df['ma_7'] = df['close'].rolling(window=7, min_periods=1).mean()
    df['ma_7d'] = df['close'].rolling(window=7, min_periods=1).mean()
    df['ma_30d'] = df['close'].rolling(window=30, min_periods=1).mean()

    # volatility (std of returns)
    df['volatility_7d'] = df['return_pct'].rolling(window=7, min_periods=1).std()

    # price range and timestamp (timestamp = transform execution time, not tied to trade date)
    df['price_range'] = df['high'] - df['low']
    # Use a single current UTC timestamp for all rows to indicate transform time
    now = pd.Timestamp.utcnow().replace(microsecond=0)
    df['timestamp'] = now

    # ensure integer for volume where possible
    if 'volume' in df.columns:
        df['volume'] = df['volume'].fillna(0).astype('Int64')

    return df


def _get_existing_columns(db_cfg: dict, table: str = 'silver_btc_price') -> set:
    conn = _connect(db_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (db_cfg.get('database'), table)
            )
            return set(r[0] for r in cur.fetchall())
    finally:
        conn.close()


def ensure_silver_table(db_cfg: dict):
    """Ensure `silver_btc_price` table exists. Create it if necessary.

    Returns the set of existing columns.
    """
    conn = pymysql.connect(
        host=db_cfg.get('host', 'localhost'),
        user=db_cfg.get('user'),
        password=db_cfg.get('password'),
        database=db_cfg.get('database'),
        port=int(db_cfg.get('port', 3306))
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_btc_price (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            open DECIMAL(15, 2),
            high DECIMAL(15, 2),
            low DECIMAL(15, 2),
            close DECIMAL(15, 2),
            volume BIGINT,
            avg_price DECIMAL(15, 2),
            return_pct DECIMAL(10, 6),
            ma_3 DECIMAL(15, 2),
            ma_7 DECIMAL(15, 2),
            ma_7d DECIMAL(15, 2),
            ma_30d DECIMAL(15, 2),
            volatility_7d DECIMAL(10, 6),
            price_range DECIMAL(15, 2),
            timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    existing = _get_existing_columns(db_cfg)
    logger.info("Detected existing silver_btc_price columns: %s", sorted(existing))
    conn.close()
    return existing


def upsert_silver(df: pd.DataFrame, db_cfg: dict, dry_run: bool = False):
    """Upsert transformed rows into silver_btc_price using the existing table schema.

    This function will NOT alter the table. It will detect existing columns and only insert into those.
    """
    if df.empty:
        logger.info("No rows to upsert into silver table.")
        return 0

    existing = _get_existing_columns(db_cfg)
    if not existing:
        raise RuntimeError("silver_btc_price table does not exist. Aborting upsert.")

    # Determine which column represents date in the table
    date_col = 'date' if 'date' in existing else ('trade_date' if 'trade_date' in existing else None)
    if date_col is None:
        raise RuntimeError("silver_btc_price table does not have a 'date' or 'trade_date' column. Aborting upsert.")

    # Logical -> table column mapping (if table uses 'trade_date' use that for 'date')
    logical_cols = [
        'date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits',
        'avg_price', 'daily_return', 'return_pct', 'ma_3', 'ma_7', 'ma_7d', 'ma_30d',
        'volatility_7d', 'price_range', 'timestamp'
    ]

    # Build list of target columns in table that we will insert into, preserving order
    target_cols = []
    for lc in logical_cols:
        if lc == 'date' and date_col == 'trade_date':
            if 'trade_date' in existing:
                target_cols.append('trade_date')
        elif lc in existing:
            target_cols.append(lc)

    if not target_cols:
        raise RuntimeError('No matching target columns found in silver_btc_price to insert into.')

    placeholders = ', '.join(['%s'] * len(target_cols))
    insert_sql = f"INSERT INTO silver_btc_price ({', '.join('`' + c + '`' for c in target_cols)}) VALUES ({placeholders})"

    rows = []
    for _, r in df.iterrows():
        values = []
        for c in target_cols:
            if c in ('date', 'trade_date'):
                values.append(r.get('date'))
                continue

            if c == 'timestamp':
                ts = r.get('timestamp')
                if pd.notna(ts):
                    try:
                        ts = pd.to_datetime(ts).to_pydatetime()
                    except Exception:
                        ts = None
                values.append(ts)
                continue

            val = r.get(c)
            if pd.isna(val):
                values.append(None)
            else:
                # cast numeric-like columns to native types
                if c in ('open', 'high', 'low', 'close', 'avg_price', 'price_range'):
                    values.append(float(val))
                elif c in ('return_pct', 'daily_return', 'ma_3', 'ma_7', 'ma_7d', 'ma_30d', 'volatility_7d'):
                    values.append(float(val))
                elif c == 'volume':
                    values.append(int(val) if pd.notna(val) else None)
                elif c in ('dividends', 'stock_splits'):
                    values.append(int(val) if pd.notna(val) else None)
                else:
                    values.append(val)
        rows.append((values[0], values))  # save date for delete + full values

    if dry_run:
        logger.info("Dry run: would upsert %d rows into silver_btc_price (columns: %s)", len(rows), target_cols)
        return len(rows)

    conn = _connect(db_cfg)
    try:
        with conn.cursor() as cur:
            for date_val, vals in rows:
                # delete existing row for this date (using proper date column)
                cur.execute(f"DELETE FROM silver_btc_price WHERE `{date_col}` = %s", (date_val,))
                cur.execute(insert_sql, vals)
        conn.commit()
        logger.info("Upserted %d rows into silver_btc_price", len(rows))
        return len(rows)
    finally:
        conn.close()


def _to_decimal_or_none(x):
    if pd.isna(x):
        return None
    return float(x)


def main(config: str = 'db_credentials.yml', start: Optional[str] = None, end: Optional[str] = None, dry_run: bool = False):
    cfg = read_db_config(config)

    logger.info("Fetching bronze data (start=%s end=%s)", start, end)
    bronze = fetch_bronze(cfg, start=start, end=end)
    if bronze.empty:
        logger.info("No bronze rows returned; aborting")
        return 0

    transformed = transform(bronze)

    ensure_silver_table(cfg)
    upsert_count = upsert_silver(transformed, cfg, dry_run=dry_run)
    return upsert_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Transform bronze to silver table')
    parser.add_argument('--config', default='db_credentials.yml')
    parser.add_argument('--start', default=None, help='Start date (inclusive) YYYY-MM-DD')
    parser.add_argument('--end', default=None, help='End date (inclusive) YYYY-MM-DD')
    parser.add_argument('--dry-run', action='store_true', help='Do not write to DB; only simulate')

    args = parser.parse_args()

    try:
        res = main(config=args.config, start=args.start, end=args.end, dry_run=args.dry_run)
        logger.info("Completed; upserted: %s", res)
    except Exception as e:
        logger.exception("Transform pipeline failed: %s", e)
        raise