from datetime import date
import pandas as pd
from datetime import datetime

BASE_URL = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/'
FORMAT = '.csv.gz'


def get_trades(dt=date.today(), symbol=''):
    if type(dt) is str:
        dt_str = dt
    else:
        dt_str = dt.strftime("%Y%m%d")

    url = BASE_URL + dt_str + FORMAT

    # Read trades to pandas dataframe
    trades = pd.read_csv(url, compression='gzip', header=0, sep=',', quotechar='"')

    # Parse timestamp
    trades.timestamp = trades.timestamp.map(lambda t: datetime.strptime(t[:-3], "%Y-%m-%dD%H:%M:%S.%f"))

    # Filter by symbol if required
    if symbol != '':
        trades = trades[trades.symbol == symbol]

    trades = trades.set_index('timestamp')

    return trades
