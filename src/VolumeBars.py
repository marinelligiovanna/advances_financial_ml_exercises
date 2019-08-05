from .Bars import Bars


class VolumeBars(Bars):

    def __init__(self, num_units=1000):
        Bars.__init__(self)
        self.num_units = num_units
        self.count_num_units = 0

    def process_chunk(self, chunk):

        if self.is_first_bar:
            # Initialize bar
            row = chunk.iloc[0]
            self.create_new_bar(row)
            self.is_first_bar = False

        for row in chunk.rows:

            if self.count_num_units == self.num_units:
                self.add_bar()
                self.create_new_bar(row)
                self.count_num_units = 0

            self.set_bar_values(row)

            self.count_num_units += self.bar['volume']
