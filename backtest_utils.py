# use core dataframe as one dataset

# create a positions table

# if no open positions
# look for a buy signal
# then set new position, exit, stop, and new buy

# if open positions
# look for exit or stop
# then set sell for all open positions

# look for new buy
# then set new position, update the stop (2*n)

# variable of the last row checked
# loop while row < the last row in the dataframe

# call to get what to do--if none move on else save results
# function should do check and give the updates of the stop, exit, and new buy

from utils import get_nyse, get_long_data
import pandas as pd

df = get_nyse('a')

df_long = get_long_data(df)

df_positions = pd.DataFrame(columns=['buy_row', 'buy_price', 'stop', 'next_buy', 'sell_price'])

row_tested = -1

while True:
    row_tested += 1
    # if no open positions
    if df_positions.empty or df_positions['sell_price'].isnull().any():
        if df_long.loc[row_tested, 'buy_signal']:
            positions_row = df_positions.shape[0]
            df_positions.loc[positions_row, 'buy_price'] = df_long.loc[row_tested, 'max_50']
            df_positions.loc[positions_row, 'buy_row'] = row_tested
            df_positions.loc[positions_row, 'stop'] = df_long.loc[row_tested, 'stop']
            df_positions.loc[positions_row, 'next_buy'] = df_long.loc[row_tested, 'next_buy']
    if row_tested > 60:
        break

print(df_positions)