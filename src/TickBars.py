from src.Bars import Bars
from datetime import datetime

class TickBars(Bars):

    def __init__(self, num_ticks=5000):
        Bars.__init__(self)
        self.num_ticks = num_ticks
        self.count_num_ticks = 0

    def process_chunk(self, chunk):

        if self.is_first_bar:
            # Initialize bar
            row = chunk.iloc[0]
            self.create_new_bar(row)
            self.is_first_bar = False

        for idx, row in chunk.iterrows():

            if self.count_num_ticks >= self.num_ticks:
                self.add_bar()
                self.create_new_bar(row)
                self.count_num_ticks = 0

            self.set_bar_values(row)

            self.count_num_ticks += 1

if __name__ == '__main__':
    t_bars = TickBars()
    start_date = datetime(year=2019, month=8, day=1)
    end_date = datetime(year=2019, month=8, day=2)
    symbol = "XBTUSD"

    bars = t_bars.process_trades(start_date=start_date, end_date=end_date, verbose=True, symbol=symbol).get_bars()
    print(bars.head(10))