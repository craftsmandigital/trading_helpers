# Bla Bla

## Installation

```bash
pip install git+https://github.com/craftsmandigital/trading_helpers.git
```

### Using uv

```bash
 uv add git+https://github.com/craftsmandigital/trading_helpers.git
```

## Usage

``` python
from trading_helpers import TradingData
```

``` python
# Import specific items
import polars as pl
from polars.datatypes import Date
from trading_helpers import TradingData

# Initialize class with path data

test_trader = TradingData(
    "/mnt/jonarne/dev/trading_data/",
    "/SP500/",
    "SP500_list.parquet",
)
# download_all_tickers. This function is only needed to call once,
# or when you decide to download new fresh ticker and sector data
test_trader.download_all_tickers()


# populate polars dataframe of all tickers and its meta data
sectors = test_trader.load_sector_parquet_file()
print(sectors.collect())

# populate polars dataframe with open, close, high, low and volume data
ochl_data = test_trader.load_ticker_parquet_files()
print(ochl_data.collect())


# populate polars dataframe with open, close, high, low and volume data
# Filtred and only spesified collumns (ticker column is always provided. not necesary to specify among columns)
collumns = pl.col(["date", "close"])
filter = (pl.col("date") >= pl.lit("2024-04-11").cast(Date)) & (
    pl.col("date") <= pl.lit("2025-04-11").cast(Date)
)
filtered_ochl_data = test_trader.load_ticker_parquet_files(filter, collumns)
print(filtered_ochl_data.collect())
```
