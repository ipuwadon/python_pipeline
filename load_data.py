"""Load data from `silver_btc_price` table and write to a Parquet file.

Provides a small CLI and a helper function for tests or imports.
"""

from pathlib import Path
import argparse
import logging
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


def fetch_silver(db_cfg: dict, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """Fetch rows from 'silver_btc_price'. Returns a DataFrame sorted by the date column if available."""
    existing = _get_existing_columns(db_cfg)
    date_col = 'date' if 'date' in existing else ('trade_date' if 'trade_date' in existing else None)

    conn = _connect(db_cfg)
    try:
        sql = "SELECT * FROM silver_btc_price"
        params = []
        if date_col and start and end:
            sql += f" WHERE `{date_col}` BETWEEN %s AND %s"
            params = [start, end]
        elif date_col and start:
            sql += f" WHERE `{date_col}` >= %s"
            params = [start]
        elif date_col and end:
            sql += f" WHERE `{date_col}` <= %s"
            params = [end]

        if date_col:
            sql += f" ORDER BY `{date_col}` ASC"
            parse_dates = [date_col]
        else:
            sql += " ORDER BY 1 ASC"
            parse_dates = None

        df = pd.read_sql(sql, conn, params=params, parse_dates=parse_dates)

        # Normalize date column name to 'date' in DataFrame for downstream consistency
        if date_col and date_col in df.columns:
            df = df.rename(columns={date_col: 'date'})
            df['date'] = pd.to_datetime(df['date']).dt.date

        return df
    finally:
        conn.close()


def save_parquet(df: pd.DataFrame, output: str, partition: bool = False, compression: str = 'snappy', engine: Optional[str] = None):
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Determine engine
    if engine is None:
        try:
            import pyarrow  # noqa: F401
            engine = 'pyarrow'
        except Exception:
            raise RuntimeError("No parquet engine available. Install 'pyarrow'.")
            #try:
            #    import fastparquet  # noqa: F401
            #    engine = 'fastparquet'
            #except Exception:
            #    raise RuntimeError("No parquet engine available. Install 'pyarrow' or 'fastparquet'.")

    if partition:
        # require a 'date' column to partition by year/month
        if 'date' not in df.columns:
            raise RuntimeError("Partitioning requested but no 'date' column found in dataframe.")

        # add partition columns and prepare to write one file per (year, month) group
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year.astype(str)
        df['month'] = df['date'].dt.month.astype(str).str.zfill(2)

        # Timestamped filename using local system time in DDMMYYYYHH24MISS format
        timestamp_str = pd.Timestamp.now().strftime('%d%m%Y%H%M%S')

        # Determine base directory to place the timestamped files
        if out.is_dir() or str(out).endswith(('/', '\\')):
            base_dir = out
        else:
            base_dir = out.parent
        base_dir.mkdir(parents=True, exist_ok=True)

        written_files = []
        # Group by year/month and write a separate file into each partition folder
        for (yr, mo), group in df.groupby(['year', 'month']):
            dir_path = base_dir / f"year={yr}" / f"month={mo}"
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"{timestamp_str}.parquet"
            group.to_parquet(file_path, engine=engine, compression=compression, index=False)
            written_files.append(str(file_path))

        out = ','.join(written_files)
    else:
        # Non-partitioned single file
        # If output is a directory (ends with /), write file inside
        if out.is_dir() or str(out).endswith(('/', '\\')):
            out = out / 'silver_btc_price.parquet'

        df.to_parquet(out, engine=engine, compression=compression, index=False)

    logger.info("Wrote parquet to %s (partitioned=%s, engine=%s, compression=%s)", out, partition, engine, compression)


def load_to_parquet(output_dir: str = 'data/gold_btc_price.parquet', config: str = 'db_credentials.yml', start: Optional[str] = None, end: Optional[str] = None, partition: bool = False, compression: str = 'snappy', engine: Optional[str] = None) -> int:
    """Main helper that fetches the silver table and writes to Parquet. Returns number of rows exported."""
    cfg = read_db_config(config)
    logger.info("Fetching silver rows (start=%s end=%s)", start, end)

    df = fetch_silver(cfg, start=start, end=end)
    if df.empty:
        logger.info("No rows found in 'silver_btc_price' for the given range.")
        return 0

    # Convert timestamp-like columns to pandas datetime when possible
    if 'timestamp' in df.columns:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception:
            logger.warning("Could not parse 'timestamp' column as datetime; leaving as-is.")

    save_parquet(df, output_dir, partition=partition, compression=compression, engine=engine)
    return len(df)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Export silver_btc_price table to Parquet')
    parser.add_argument('--config', default='db_credentials.yml')
    parser.add_argument('--start', default=None, help='Start date (inclusive) YYYY-MM-DD')
    parser.add_argument('--end', default=None, help='End date (inclusive) YYYY-MM-DD')
    parser.add_argument('--output', default='data/gold_btc_price.parquet')
    parser.add_argument('--partition', action='store_true', help='Partition output by year/month (requires date column)')
    parser.add_argument('--compression', default='snappy', help='Parquet compression codec (snappy, gzip, etc.)')
    parser.add_argument('--engine', default=None, help='Parquet engine to use (pyarrow|fastparquet). Auto-detected by default')

    args = parser.parse_args()

    try:
        cnt = load_to_parquet(output_dir=args.output, config=args.config, start=args.start, end=args.end, partition=args.partition, compression=args.compression, engine=args.engine)
        logger.info("Export complete; rows exported: %s", cnt)
    except Exception as exc:
        logger.exception("Failed to export silver table: %s", exc)
        raise