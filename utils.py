import pandas as pd


def calc_n(df_origin):
    # take in a dataframe
    # return a series of the N for that day
    df = df_origin.copy()
    # true range
    # max(high-low, h-pdc, pdc-l)

    # first N (average of first 20 true ranges

    # N
    # (19 * pdn + true range) / 20



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
