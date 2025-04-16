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
# Import specific items
from trading_helpers import TradingData

# Then use them directly

test_trader = TradingData(
    "/mnt/jonarne/dev/trading_data/",
    "/SP500/",
    "SP500_list.parquet",
)
```
