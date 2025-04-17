import yfinance as yf
import pandas as pd
import tqdm
import os
import polars as pl
from pathlib import Path

# from datetime import datetime, timedelta
# folder = "/mnt/jonarne/dev/trading_data/"


class TradingData:
    """
    A class fetching ticker data from yfinance
    and the ticker data to operate on, on an internet page.
    """

    def __init__(
        self,
        dataFolder: str,
        yfinanceFilesFolder: str,
        tickersFileName: str,
        tickersColumn: str = "Symbol",
        tickersURL: str = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
    ):
        """
        Constructor for the TradingData class.

        Args:
            dataFolder (str): Path to store data
            yfinanceFilesFolder (str): Path to store yfinance data files
            tickersFileName (str): Name of the file containing tickers
            tickersColumn (str, optional): Column name for tickers. Defaults to "Symbol".
            tickersURL (str, optional): URL to fetch ticker data. Defaults to S&P 500 companies wiki page.
        """
        # self.dataFolder = dataFolder
        # self.yfinanceFilesFolder = yfinanceFilesFolder
        # self.tickersFileName = tickersFileName
        self._tickersColumn = tickersColumn
        self._tickersURL = tickersURL

        # Initialize empty variables that might be populated later
        # self.tickers = None
        # self.data = None

        # data_folder = "/mnt/jonarne/dev/trading_data"
        self._tickerFolder = dataFolder + yfinanceFilesFolder
        self._tickerListFile = dataFolder + tickersFileName
        print("TradingData Initialized")

    def download_all_tickers(self):
        equity_table = self._get_ticker_list()
        # print(equity_table)

        self._save_ticker_table(equity_table, self._tickerListFile)
        self._fetch_and_save_tickers(
            equity_table[self._tickersColumn].tolist(), self._tickerFolder
        )

    # Fetch S&P 500 tickers
    def _get_ticker_list(self):
        # URL of the Wikipedia page containing the list of S&P 500 companies
        url = self._tickersURL
        # Read the table from the URL
        tables = pd.read_html(url)
        # Clean up any invalid ticker symbols Replace all periods with hyphens in
        # the 'Symbol' column
        tables[0][self._tickersColumn] = tables[0][self._tickersColumn].str.replace(
            ".", "-"
        )
        return tables[0]

    # Download data and save to Parquet
    def _fetch_and_save_tickers(self, tickers, output_dir):
        # os.makedirs(output_dir, exist_ok=True)
        pbar = tqdm.tqdm(
            desc="Processing data", unit="item", miniters=100, total=len(tickers)
        )
        for ticker in tickers:
            try:
                # print(f"Fetching data for {ticker}...")

                data = yf.download(ticker, progress=False)
                # print(data)
                # exit()
                data = data.reset_index()
                data.columns = [
                    "date",
                    "close",
                    "high",
                    "low",
                    "open",
                    "volume",
                ]  # Set the correct column names

                if not data.empty:
                    # Save each ticker's data as a Parquet file
                    filepath = os.path.join(output_dir, f"{ticker}.parquet")
                    # filepath =  os.path.join('/mnt/jonarne/dev/trading_data', f"{ticker}.parquet")
                    data.to_parquet(filepath)
                    # print(f"Saved {ticker} to {filepath}")
                else:
                    print(f"No data found for {ticker}.")
                pbar.update(1)
                # exit()
            except Exception as e:
                print(f"Error fetching data for {ticker}: {e}")

        pbar.close()

    def _save_ticker_table(self, data, filename):
        if not data.empty:
            # Save table as Parquet file
            data.to_parquet(filename)
            print(f"Saved Ticker description list to {filename}")
        else:
            print("No data found for Ticker description list.")

    # ---------------------------------------------------------------------------------

    # Function to load and filter parquet files
    # EXTREMLY IMPORTANT DONT PASS "ticker" THAT IS AUTOMATICALY MERGED IN THE LATER
    # ERROR MESSAGE WHEN DOING SO
    def load_ticker_parquet_files(
        self,
        filter_expr: pl.Expr = pl.lit(True),
        columns: pl.Expr = pl.all(),
        parquet_directory: Path = None,
    ) -> pl.LazyFrame:
        # her is an example of "filter_expr"
        # (pl.col("date") >= pl.lit(start_date)) & (pl.col("date") <= pl.lit(end_date))
        # Here is an example on "columns" expression:
        # pl.col(["date", "close"])
        #
        # python polars, reading parquet files.
        # I have a folder with parquet files. The files are dayly data for all tickers
        # on the S&P 500. All the files have the same structure and the naming structure
        # is like this example:
        #  "TESLA.parquet" "MSFT.parquet" "AAPL.parqet" and so on#  When readng the files I onely whant tho have data from 2017-12-06 to 2018-12-05.
        #  The files could be readed into a gigantic DataFrame. The dataframe could contain
        #  the file data pluss a column with the ticker. Ticker is a part of the parquet file name
        #  please help me with code to do this super duper efficient

        if parquet_directory is None:
            parquet_directory = Path(self._tickerFolder)
        parquet_files = list(parquet_directory.glob("*.parquet"))
        lazy_frames = [
            pl.scan_parquet(str(file))
            .filter(filter_expr)
            .select(columns)  # Select only the necessary columns
            .with_columns(pl.lit(file.stem).alias("ticker"))
            for file in parquet_files
        ]
        return pl.concat(lazy_frames)

    def load_sector_parquet_file(
        self,
        parquet_file: Path = None,
    ) -> pl.LazyFrame:

        if parquet_file is None:
            parquet_file = (Path(self._tickerListFile),)
        return pl.scan_parquet(parquet_file)
