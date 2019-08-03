from datetime import datetime, timedelta
import pandas as pd
from abc import abstractmethod


class Bars:

    def __init__(self):
        self.columns = ['timestamp', 'open', 'high', 'low', ' close', 'volume']
        self.bars = pd.DataFrame(columns=self.columns)

    def process_trades(self, start_date=datetime.now() - timedelta(days=1), end_date=datetime.now() - timedelta(days=1),
                       chunksize=10 ** 6, file_dir="./data/", file_format=".csv.gz", compression=None, verbose=False):

        while start_date <= end_date:
            date_str = start_date.strftime("%Y%m%d")
            file_name = date_str + file_format
            chunk_num = 0

            if verbose:
                print("Reading file " + file_name + " ...")

            for chunk in pd.read_csv(file_dir + file_name, chunksize=chunksize, compression=compression):
                if verbose:
                    print('Processing chunk ' + str(chunk_num))

                chunk['timestamp'] = chunk['timestamp'].map(lambda t: datetime.strptime(t[:-3], '%Y-%m-%dD%H:%M:%S.%f'))
                self.process_chunk(chunk)

                chunk_num = chunk_num + 1

            start_date = start_date + timedelta(days=1)

        return self

    @abstractmethod
    def process_chunk(self, chunk):
        pass

    def add_bar(self, bar):
        self.bars.add(bar)

    def new_bar(self, row):
        bar = {}
        price = row['price']

        bar['timestamp'] = row['timestamp']
        bar['open'] = price
        bar['high'] = price
        bar['low'] = price
        bar['close'] = price
        bar['volume'] = row['size']

        return bar

    def get_bars(self):
        return self.bars
        # def time_bars(trades, type='close', interval='15'):
        #
        #     freq = str(interval) + 'Min'
        #     group = trades.groupby(pd.Grouper(freq=freq))
        #
        #     # Calculate open, high, low and close bars
        #     if type == 'open':
        #         return group.first()
        #     elif type == 'close':
        #         return group.tail(1)
        #     elif type == 'high':
        #         return group.max()
        #     elif type == 'low':
        #         return group.min()
        #
        #     return trades
        #
        # def tick_bars(trades, type='close', ticks_per_bar = 500):
        #
        #     # Create a row_number column and a group by key according
        #     # the number of ticks per bar required
        #     df = trades.reset_index()
        #     df['row_number'] = np.arange(len(df))
        #     df['key'] = np.floor(df['row_number']/ticks_per_bar)
        #