from utils_price_scraping import save_stock_prices_nyse
import pandas as pd
import datetime as dt
from threading import Thread
from queue import Queue
import time
import os

start_date = dt.datetime(2015, 1, 1)
end_date = dt.datetime(2016, 10, 10)
# symbols = ['aapl', 'mmm', 'aaa', 'btz', 'bud', 'bwpt', 'bx', 'eot','au','auo']
folder_stock_prices = './test'
folder_stock_symbols = './stock_symbols'

params_ = Queue()

df_nyse_symbols = pd.read_csv(os.path.join(folder_stock_symbols, 'NYSE.csv'))
df_nyse_symbols.columns = [x.replace(' ', '_').lower() for x in df_nyse_symbols.columns]
df_nyse_symbols['symbol_lower'] = df_nyse_symbols['symbol'].str.lower()
df_nyse_symbols['ticker'] = 'NYSE:' + df_nyse_symbols['symbol']


def create_queue():
    ticker_have = [i.strip('nyse_').replace('.csv', '') for i in os.listdir(folder_stock_prices) if '.DS' not in i]
    #     df_temp =  df_nyse_symbols.loc[~df_nyse_symbols['symbol_lower'].isin(ticker_have)]
    df_temp = df_nyse_symbols.loc[:40]
    symbols = df_temp['symbol_lower'].tolist()
    for symbol in symbols:
        params_.put((folder_stock_prices, symbol, start_date, end_date))


def main():
    while True:
        p = params_.get()
        save_stock_prices_nyse(*p)
        params_.task_done()


create_queue()
t1 = time.time()
number_of_threads = 100
for _ in range(number_of_threads):
    worker = Thread(target=main, daemon=True)
    worker.start()
params_.join()
t2 = time.time()
print(t2 - t1)
