"""Main orchestration for the pipeline.

Supports three modes:
  - prepare: download raw CSV
  - extract: load CSV into DB
  - pipeline: run prepare then extract
"""

import argparse
import sys
import logging
from pathlib import Path

from prepare_data import fetch_btc_history
import extract_data
import transform_data
import load_data

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def prepare_step(output_path: str, start: str, end: str, ticker: str, force: bool = False):
    out = fetch_btc_history(output_path=output_path, start=start, end=end, ticker=ticker, force=force)
    logger.info("Prepared data and wrote CSV: %s", out)
    return out


def extract_step(csv_path: str, config_path: str):
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = extract_data.load_csv(csv_path)
    cfg = extract_data.read_db_config(config_path)
    extract_data.upsert_btc_prices(df, cfg)
    logger.info("Extracted %d rows into DB", len(df))


def transform_step(config_path: str, start: str = None, end: str = None, dry_run: bool = False):
    cfg = transform_data.read_db_config(config_path)
    # transform_data.main returns number of upserts (or 0)
    count = transform_data.main(config=config_path, start=start, end=end, dry_run=dry_run)
    logger.info("Transform step completed; upserted %s rows", count)
    return count

def load_step(config_path: str, start: str = None, end: str = None, output: str = None, partition: str = True, compression: str = 'snappy', engine: str = None):

    try:
        cnt = load_data.load_to_parquet(output_dir=output, config=config_path, start=start, end=end, partition=partition, compression=compression, engine=engine)
        logger.info("Export complete; rows exported: %s", cnt)
        return 0
    except Exception as exc:
        logger.exception("Failed to export silver table: %s", exc)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description="Orchestrate prepare and extract steps")
    sub = parser.add_subparsers(dest='command', required=False)

    p_prep = sub.add_parser('prepare', help='Download raw data CSV')
    p_prep.add_argument('--start', default='2026-01-01')
    p_prep.add_argument('--end', default='2026-01-31')
    p_prep.add_argument('--output', default='raw_data/btc_usd_jan2026.csv')
    p_prep.add_argument('--ticker', default='BTC-USD')

    p_ext = sub.add_parser('extract', help='Load CSV into DB')
    p_ext.add_argument('--csv', default='raw_data/btc_usd_jan2026.csv')
    p_ext.add_argument('--config', default='db_credentials.yml')

    p_trans = sub.add_parser('transform', help='Transform bronze table into silver table')
    p_trans.add_argument('--start', default=None, help='Start date (inclusive) YYYY-MM-DD')
    p_trans.add_argument('--end', default=None, help='End date (inclusive) YYYY-MM-DD')
    p_trans.add_argument('--config', default='db_credentials.yml')
    p_trans.add_argument('--dry-run', action='store_true', help='Simulate transform without DB writes')

    p_load = sub.add_parser('load', help='Load silver data to Parguet file')
    p_load.add_argument('--config', default='db_credentials.yml')
    p_load.add_argument('--start', default=None, help='Start date (inclusive) YYYY-MM-DD')
    p_load.add_argument('--end', default=None, help='End date (inclusive) YYYY-MM-DD')
    p_load.add_argument('--output', default='data/gold_btc_price.parquet')
    p_load.add_argument('--partition', action='store_true', help='Partition output by year/month (requires date column)')
    p_load.add_argument('--compression', default='snappy', help='Parquet compression codec (snappy, gzip, etc.)')
    p_load.add_argument('--engine', default=None, help='Parquet engine to use (pyarrow|fastparquet). Auto-detected by default')

    p_pipe = sub.add_parser('pipeline', help='Run prepare, extract, then transform')
    p_pipe.add_argument('--start', default='2026-02-01')
    p_pipe.add_argument('--end', default='2026-02-28')
    p_pipe.add_argument('--output', default='raw_data/btc_usd_feb2026.csv')
    p_pipe.add_argument('--output-pq', default='data/gold_btc_price')
    p_pipe.add_argument('--skip-prepare', action='store_true', help='Skip the prepare (download) step')
    p_pipe.add_argument('--force', action='store_true', help='Force re-download/overwrite when running prepare')
    p_pipe.add_argument('--ticker', default='BTC-USD')
    p_pipe.add_argument('--config', default='db_credentials.yml')
    p_pipe.add_argument('--dry-run', action='store_true', help='When set, the transform step is a dry-run and will not write to DB')

    args = parser.parse_args(argv)

    # Default values (used when no subcommand is provided)
    DEFAULT_START = '2026-02-01'
    DEFAULT_END = '2026-02-28'
    DEFAULT_OUTPUT = 'raw_data/btc_usd_feb2026.csv'
    DEFAULT_TICKER = 'BTC-USD'
    DEFAULT_CONFIG = 'db_credentials.yml'
    DEFAULT_OUTPUT_PQ = 'data/gold_btc_price'
    DEFAULT_PARTITION = True
    DEFAULT_COMPRESSION = 'snappy'
    DEFAULT_ENGINE = None

    try:
        if args.command == 'prepare':
            start = getattr(args, 'start', DEFAULT_START)
            end = getattr(args, 'end', DEFAULT_END)
            output = getattr(args, 'output', DEFAULT_OUTPUT)
            ticker = getattr(args, 'ticker', DEFAULT_TICKER)
            prepare_step(output, start, end, ticker)

        elif args.command == 'extract':
            csv = getattr(args, 'csv', DEFAULT_OUTPUT)
            config = getattr(args, 'config', DEFAULT_CONFIG)
            extract_step(csv, config)

        elif args.command == 'transform':
            start = getattr(args, 'start', DEFAULT_START)
            end = getattr(args, 'end', DEFAULT_END)
            config = getattr(args, 'config', DEFAULT_CONFIG)
            dryrun = getattr(args, 'dry-run', False)
            transform_step(config, start, end, dryrun)

        elif args.command == 'load':
            start = getattr(args, 'start', DEFAULT_START)
            end = getattr(args, 'end', DEFAULT_END)
            config = getattr(args, 'config', DEFAULT_CONFIG)
            output = getattr(args, 'output', DEFAULT_OUTPUT_PQ)
            partition = getattr(args, 'partition', DEFAULT_PARTITION)
            compression = getattr(args, 'compression', DEFAULT_COMPRESSION)
            engine = getattr(args, 'engine', DEFAULT_ENGINE)
            load_step(config, start, end, output, partition, compression, engine)

        elif args.command == 'pipeline' or args.command is None:
            # default to pipeline when no subcommand provided
            start = getattr(args, 'start', DEFAULT_START)
            end = getattr(args, 'end', DEFAULT_END)
            output = getattr(args, 'output', DEFAULT_OUTPUT)
            ticker = getattr(args, 'ticker', DEFAULT_TICKER)
            config = getattr(args, 'config', DEFAULT_CONFIG)
            dry_run = getattr(args, 'dry_run', False)
            output_pq = getattr(args, 'output_pq', DEFAULT_OUTPUT_PQ)
            partition = getattr(args, 'partition', DEFAULT_PARTITION)
            compression = getattr(args, 'compression', DEFAULT_COMPRESSION)
            engine = getattr(args, 'engine', DEFAULT_ENGINE)

            logger.info("Starting pipeline: prepare -> extract -> transform")
            if getattr(args, 'skip_prepare', False):
                logger.info("Skipping prepare step as requested; using existing CSV: %s", output)
                out = output
            else:
                force_flag = getattr(args, 'force', False)
                out = prepare_step(output, start, end, ticker, force=force_flag)
            logger.info("Prepare step completed: %s", out)

            try:
                extract_step(out, config)
            except ImportError as e:
                logger.error("Extract step skipped due to missing dependency: %s", e)
                logger.error("Install required packages with: pip install pyyaml pymysql")
                return 1
            except FileNotFoundError as e:
                logger.error("Extract step failed: %s", e)
                return 1
            except Exception as e:
                logger.exception("Extract step failed: %s", e)
                return 1

            # Transform step (reads from DB bronze -> writes to silver)
            try:
                transform_step(config, start=start, end=end, dry_run=dry_run)
            except ImportError as e:
                logger.error("Transform step skipped due to missing dependency: %s", e)
                logger.error("Install required packages with: pip install pymysql pandas")
                return 1
            except Exception as e:
                logger.exception("Transform step failed: %s", e)
                return 1
            
            # Load step (get data from DB Silver -> writes to Parquet file)
            try:
                load_step(config, start, end, output_pq, partition, compression, engine)
            except Exception as e:
                logger.exception("Load step failed: %s", e)
                return 1

        else:
            parser.print_help()
            return 2

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
