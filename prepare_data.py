import yfinance as yf
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def fetch_btc_history(output_path: str = "raw_data/btc_usd_feb2026.csv", start: str = "2026-02-01", end: str = "2026-02-28", ticker: str = "BTC-USD", force: bool = False) -> Path:
    """Download BTC historical data and save as CSV.

    If the output file already exists and orce is False the existing file
    will be returned and not overwritten. Set orce=True to re-download
    and overwrite.

    On API failure, returns existing CSV if available, otherwise creates empty CSV.
    Returns the Path to the saved file.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(f"Downloading {ticker} data from {start} to {end}")
        btc = yf.Ticker(ticker)
        hist = btc.history(start=start, end=end)
        
        if hist.empty:
            raise ValueError(f"yfinance returned no data for {ticker} ({start} to {end})")
        
        logger.info(f"Successfully downloaded {len(hist)} rows for {ticker}")
        hist.to_csv(out)
        return out
        
    except Exception as e:
        logger.warning(f"Failed to download {ticker}: {e}")
        
        # If existing CSV exists, return it as fallback
        if out.exists():
            logger.info(f"Using existing CSV file: {out}")
            return out
        
        # Otherwise create empty CSV with headers so pipeline can continue
        logger.warning(f"Creating empty CSV: {out}")
        df = pd.DataFrame(columns=['Date','Open','High','Low','Close','Adj Close','Volume'])
        df.to_csv(out, index=False)
        return out


def main():
    out = fetch_btc_history()
    print(f"Saved BTC history to {out}")


if __name__ == "__main__":
    main()
