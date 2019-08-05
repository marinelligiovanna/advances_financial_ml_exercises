from datetime import timedelta, datetime
import requests
import pandas as pd

BASE_URL = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/'
FORMAT = '.csv.gz'
FILE_PATH = "./data/"


class BitMex:

    @staticmethod
    def download_trades(start_date=datetime.now() - timedelta(days=2), end_date=datetime.now() - timedelta(days=1),
                        verbose=False, file_dir=FILE_PATH):
        while start_date <= end_date:
            date_str = start_date.strftime("%Y%m%d")
            file_name = date_str + FORMAT
            url = BASE_URL + file_name

            if verbose:
                print("Downloading and saving data ->" + date_str)

            res = requests.get(url, stream=True)

            if res.status_code == 200:
                with open(file_dir + file_name, 'wb') as f:
                    f.write(res.raw.read())

            start_date = start_date + timedelta(days=1)

        if verbose:
            print("Finished saving BitMex trade files")

    @staticmethod
    def get_trades_df(start_date=datetime.now() - timedelta(days=2), end_date=datetime.now() - timedelta(days=1),
                      symbol=None, verbose = False, file_dir = FILE_PATH):

        trades = None

        while start_date <= end_date:
            date_str = start_date.strftime("%Y%m%d")
            file_name = date_str + FORMAT
            file_path = file_dir + file_name

            if verbose:
                print("Reading data ->" + date_str)

            df = pd.read_csv(file_path, compression='gzip', header=0, sep=',', quotechar='"')

            if trades is None:
                trades = df
            else:
                trades = trades.append(df)

            start_date = start_date + timedelta(days=1)

        trades['timestamp'] = trades['timestamp'].map(lambda t: datetime.strptime(t[:-3], '%Y-%m-%dD%H:%M:%S.%f'))

        if symbol is not None:
            trades = trades[trades['symbol'] == symbol]

        return trades
