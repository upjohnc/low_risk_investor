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

row_tested = -1

df_new_empty = pd.DataFrame(columns=['buy_row', 'buy_price', 'stop', 'next_buy', 'sell_price'])

df_positions = df_new_empty.copy()


def add_position(df_long_orig, row_tested_orig):
    df_new = df_new_empty.copy()
    df_new.loc[0, 'buy_price'] = df_long_orig.loc[row_tested_orig, 'max_50']
    df_new.loc[0, 'buy_row'] = row_tested_orig
    df_new.loc[0, 'stop'] = [df_long_orig.loc[row_tested_orig, 'stop']]
    df_new.loc[0, 'next_buy'] = [df_long_orig.loc[row_tested_orig, 'next_buy']]
    return df_new


while True:
    row_tested += 1
    if not df_positions['sell_price'].isnull().any():
        if df_long.loc[row_tested, 'buy_signal']:
            df_new_row = add_position(df_long, row_tested)
            df_positions = df_positions.append(df_new_row).reset_index(drop=True)

    else:
        # test the stop or exit lower than low
        # min_stop_exit = min(df_positions.loc[-1, ['stop']][-1], df_long.loc[row_tested, 'exit'])
        next_buy_value = df_positions.iloc[-1]['next_buy'][-1]
        # if min_stop_exit > df_long.loc[row_tested, 'low']:
        #     df_positions.loc[mask_open_positions, 'sell_price'] = df_long.loc[row_tested, ['low']]
        if df_long.loc[row_tested, 'high'] > next_buy_value:
            mask_open = df_positions['sell_price'].isnull()
            for index, value in df_positions.loc[mask_open].iterrows():
                value['stop'].extend([df_long.loc[row_tested, 'stop']])
                value['next_buy'].extend([df_long.loc[row_tested, 'next_buy']])
            df_new_row = df_new_empty.copy()
            df_new_row.loc[0, 'buy_price'] = next_buy_value
            df_new_row.loc[0, 'buy_row'] = row_tested
            df_new_row.loc[0, 'stop'] = [df_long.loc[row_tested, 'stop']]
            df_new_row.loc[0, 'next_buy'] = [df_long.loc[row_tested, 'next_buy']]
            df_positions = df_positions.append(df_new_row).reset_index(drop=True)
    if row_tested > 100:
        break

print(df_positions)
