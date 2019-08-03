from datetime import timedelta
from datetime import datetime
import requests

BASE_URL = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/'
FORMAT = '.csv.gz'
FILE_PATH = "./data/"

class BitMex:

    @staticmethod
    def get_trades(start_date=datetime.now() - timedelta(days=2), end_date=datetime.now() - timedelta(days=1),
                   verbose=False, file_path=FILE_PATH):
        while start_date <= end_date:
            date_str = start_date.strftime("%Y%m%d")
            file_name = date_str + FORMAT
            url = BASE_URL + file_name

            if verbose:
                print("Downloading and saving data ->" + date_str)

            res = requests.get(url, stream=True)

            if res.status_code == 200:
                with open(file_path + file_name, 'wb') as f:
                    f.write(res.raw.read())

            start_date = start_date + timedelta(days=1)

        if verbose:
            print("Finished saving BitMex trade files")
