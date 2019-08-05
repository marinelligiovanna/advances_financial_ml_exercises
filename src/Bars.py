from datetime import datetime, timedelta
import pandas as pd
from abc import abstractmethod


class Bars:

    def __init__(self):
        self.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        self.bars = pd.DataFrame(columns=self.columns)
        self.bar = {}
        self.is_first_bar = True

    def process_trades(self, start_date=datetime.now() - timedelta(days=1), end_date=datetime.now() - timedelta(days=1),
                       symbol=None, chunksize=10 ** 6, file_dir="../data/", file_format=".csv.gz", compression="gzip",
                       verbose=False):

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
                chunk = chunk[chunk['symbol'] == symbol]

                self.process_chunk(chunk)

                chunk_num = chunk_num + 1

            start_date = start_date + timedelta(days=1)

        return self

    @abstractmethod
    def process_chunk(self, chunk):
        pass

    def add_bar(self):
        self.bars = self.bars.append(self.bar, ignore_index=True)

    def create_new_bar(self, row):
        bar = {}
        price = row['price']

        bar['timestamp'] = row['timestamp']
        bar['open'] = price
        bar['high'] = price
        bar['low'] = price
        bar['close'] = price
        bar['volume'] = row['size']

        self.bar = bar

    def set_bar_values(self, row):
        price = row['price']
        self.bar['timestamp'] = row['timestamp']
        self.bar['high'] = price if price > self.bar['high'] else self.bar['high']
        self.bar['low'] = price if price < self.bar['low'] else self.bar['low']
        self.bar['close'] = price
        self.bar['volume'] = self.bar['volume'] + row['size']

    def get_bars(self):
        return self.bars
