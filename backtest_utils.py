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


def add_buy(df_positions_orig, df_market_orig, row_tested_orig):
    df_pos = df_positions_orig.copy()
    df_market_data = df_market_orig.copy()
    positions_row = df_pos.shape[0]
    ### should this just be a row that then gets appended
    df_pos.loc[positions_row, 'buy_price'] = df_market_data.loc[row_tested_orig, 'max_50']
    df_pos.loc[positions_row, 'buy_row'] = row_tested
    df_pos.loc[positions_row, 'stop'] = df_market_data.loc[row_tested_orig, 'stop'].tolist()
    df_pos.loc[positions_row, 'next_buy'] = df_market_data.loc[row_tested_orig, 'next_buy'].tolist()
    return df_pos


while True:
    row_tested += 1
    # if no open positions
    mask_open_positions = df_positions['sell_price'].isnull()
    if df_positions.empty or mask_open_positions.any():
        if df_long.loc[row_tested, 'buy_signal']:
            thing = add_buy(df_positions, df_long, row_tested)
            # positions_row = df_positions.shape[0]
            # df_positions.loc[positions_row, 'buy_price'] = df_long.loc[row_tested, 'max_50']
            # df_positions.loc[positions_row, 'buy_row'] = row_tested
            # df_positions.loc[positions_row, 'stop'] = df_long.loc[row_tested, 'stop'].tolist()
            # df_positions.loc[positions_row, 'next_buy'] = df_long.loc[row_tested, 'next_buy'].tolist()
    else:
        # test the stop or exit lower than low
        min_stop_exit = min(df_positions.loc[-1, ['stop']][-1], df_long.loc[row_tested, 'exit'])
        next_buy_value = df_positions.loc[-1, ['next_buy']][-1]
        if min_stop_exit > df_long.loc[row_tested, 'low']:
            df_positions.loc[mask_open_positions, 'sell_price'] = df_long.loc[row_tested, ['low']]
        elif next_buy_value > df_long.loc[row_tested, 'high']:
            # save new stop value
            # save new next_buy in open positions
            # save new purchase
            df_positions.loc[df_positions.shape[0], '']

    if row_tested > 60:
        break

print(df_positions)
