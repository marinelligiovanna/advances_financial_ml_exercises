import pandas as pd
import numpy as np

def time_bars(trades, type='close', interval='15'):

    freq = str(interval) + 'Min'
    group = trades.groupby(pd.Grouper(freq=freq))

    if type == 'open':
        return group.first()
    elif type == 'close':
        return group.tail(1)
    elif type == 'high':
        return group.max()
    elif type == 'low':
        return group.min()

    return trades

def tick_bars(trades, type='close', ticks_per_bar = 500):

    # Create a row_number column and a group by key according
    # the number of ticks per bar required
    df = trades.reset_index()
    df['row_number'] = np.arange(len(df))
    df['key'] = np.floor(df['row_number']/ticks_per_bar)

    group = df.groupBy('key')

    if type == 'open':
        df = group.first()
    elif type == 'close':
        df = group.tail(1)
    elif type == 'high':
        df = group.max()
    elif type == 'low':
        df = group.min()

    # Set index to timestamp and drop unused columns
    df = df.set_index('timestamp')
    df.drop('row_number')
    df.drop('key')

    return df

