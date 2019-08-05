from src.Bars import Bars
from datetime import datetime, timedelta

class TimeBars(Bars):

    def __init__(self, interval=15):
        Bars.__init__(self)

        self.interval = interval
        self.end_date = None
        self.current_date = None

    def process_chunk(self, chunk):

        if self.is_first_bar:
            # Initialize bar
            row = chunk.iloc[0]
            self.create_new_bar(row)
            self.is_first_bar = False

            self.current_date = self.bar['timestamp']
            self.end_date = self.current_date + timedelta(minutes=self.interval)

        for idx, row in chunk.iterrows():

            print(" Reading row ---- " + str(row['timestamp']))

            self.current_date = row['timestamp']

            if self.current_date > self.end_date:
                self.add_bar()
                self.create_new_bar(row)
                self.end_date = self.current_date + timedelta(minutes=self.interval)

            self.set_bar_values(row)


