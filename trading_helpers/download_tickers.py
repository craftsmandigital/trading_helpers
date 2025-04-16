import yfinance as yf
import pandas as pd
import tqdm
import os


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
        ticker_folder = dataFolder + yfinanceFilesFolder
        ticker_list_file = dataFolder + tickersFileName

        equity_table = get_sp500_tickers(self)
        # print(equity_table)

        save_ticker_table(equity_table, ticker_list_file)
        fetch_and_save_tickers(equity_table[tickersColumn].tolist(), ticker_folder)


# Fetch S&P 500 tickers
def get_sp500_tickers(self):
    # URL of the Wikipedia page containing the list of S&P 500 companies
    url = self._tickersURL
    # Read the table from the URL
    tables = pd.read_html(url)
    # Clean up any invalid ticker symbols
    # Replace all periods with hyphens in the 'Symbol' column
    tables[0][self._tickersColumn] = tables[0][self._tickersColumn].str.replace(
        ".", "-"
    )
    return tables[0]


# Download data and save to Parquet
def fetch_and_save_tickers(tickers, output_dir):
    # os.makedirs(output_dir, exist_ok=True)
    pbar = tqdm.tqdm(
        desc="Processing data", unit="item", miniters=100, total=len(tickers)
    )
    return
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


def save_ticker_table(data, filename):
    if not data.empty:
        # Save table as Parquet file
        data.to_parquet(filename)
        print(f"Saved Ticker description list to {filename}")
    else:
        print("No data found for Ticker description list.")
