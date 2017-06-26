import pandas as pd
import numpy as np


def get_nyse(stock_name):
    df = pd.read_csv('./stock_prices/nyse_{}.csv'.format(stock_name))
    df = get_clean_and_n(df)
    return df


def get_clean_and_n(df_origin):
    if df_origin.shape[0] < 20:
        return None
    df = df_origin.copy()
    df = clean_nyse(df)
    df = calc_n(df)
    return df


def calc_n(df_origin):
    if df_origin is None or df_origin.shape[0] < 20:
        return None
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


def clean_nyse(df_original):
    if df_original is None or df_original.shape[0] < 20:
        return None
    df = df_original.copy()
    df.columns = [i.lower() for i in df.columns]
    if df['high'].dtype == 'O' or df['low'].dtype == 'O':
        if df['high'].dtype == 'O':
            df = df.loc[df['high'] != '-']
        if df['low'].dtype == 'O':
            df = df.loc[df['low'] != '-']
        df['high'] = df['high'].astype('float')
        df['low'] = df['low'].astype('float')
        df['close'] = df['close'].astype('float')
    df = df.drop(df.columns[0], axis=1)
    df = df.reset_index(drop=True)
    df['date'] = pd.to_datetime(df['date'])
    return df
