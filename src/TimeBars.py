from .Bars import Bars
from datetime import timedelta


class TimeBars(Bars):

    def __init__(self, interval=15):
        Bars.__init__(self)
        self.interval = 15

    def process_chunk(self, chunk):

        # Current and end dates of the current candle
        current_date = chunk['timestamp'].iloc[0]
        end_date = current_date + timedelta(minutes=self.interval)

        # Initialize bar values
        price = chunk['price'].iloc[0]
        bar = {}

        bar['timestamp'] = current_date
        bar['open'] = price
        bar['high'] = price
        bar['low'] = price
        bar['close'] = price
        bar['volume'] = chunk['size'].iloc[0]

        for row in chunk.rows:

            current_date = row['timestamp']

            # If the current date is greater than the close of the candle
            # it is time to close the previous candle and open a new one
            if current_date > end_date:
                self.add_bar(bar)

                # Start a new bar
                bar = self.new_bar(row)

            # Accumulate values for a new bar
            price = row['price']
            bar['timestamp'] = current_date
            bar['high'] = price if price > bar['high'] else bar['high']
            bar['low'] = price if price < bar['low'] else bar['low']
            bar['close'] = price
            bar['volume'] = bar['volume'] + row['size']

        # Close and add the last bar
        self.add_bar(bar)