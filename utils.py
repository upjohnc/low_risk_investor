import pandas as pd
import numpy as np


def get_clean_and_n(df_origin):
    df = df_origin.copy()
    df = clean_nyse(df)
    df = calc_n(df)
    return df


def calc_n(df_origin):
    # take in a dataframe
    # return a series of the N for that day
    df = df_origin.copy()

    df['pdc'] = df['close'].shift()
    df = df.drop(0, axis=0).reset_index(drop=True)

    # true range
    # max(high-low, h-pdc, pdc-l)
    def calc_tr(row):
        return max(row['high'] - row['low'], row['high'] - row['pdc'], row['pdc'] - row['low'])

    df['tr'] = df.apply(calc_tr, axis=1)

    # first N (average of first 20 true ranges)
    df['N'] = np.nan
    df.loc[19, 'N'] = df.loc[:19, 'tr'].mean()

    # N
    # (19 * pdn + true range) / 20
    for i in range(20, df.shape[0]):
        df.loc[i, 'N'] = (19 * df.loc[i - 1, 'N'] + df.loc[i, 'tr']) / 20

    df = df.loc[19:]
    df = df.reset_index(drop=True)

    return df


def calc_buy_trigger(df_origin):
    # take in a dataframe
    # return the value that triggers a buy
    df = df_origin.copy()
    pass


def clean_nyse(df_original):
    df = df_original.copy()
    df = df.drop(df.columns[0], axis=1)
    df['date'] = pd.to_datetime(df['date'])
    return df
