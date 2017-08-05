import datetime as dt
from queue import Queue
import pandas as pd
from threading import Thread

from utils_price_scraping import save_stock_prices_nasdaq

folder_stock_prices = './stock_prices/nasdaq/threading'
params_ = Queue()
date_start = dt.datetime(1960, 1, 1)
date_end = dt.datetime.now()


def create_queue(letter):
    # ticker_have = [i.strip('nyse_').replace('.csv', '') for i in os.listdir(folder_stock_prices) if '.DS' not in i]
    # #     df_temp =  df_nyse_symbols.loc[~df_nyse_symbols['symbol_lower'].isin(ticker_have)]
    # df_temp = df_nyse_symbols.loc[:40]
    df_temp = pd.read_csv('./stock_symbols/nasdaq_symbols.csv')
    symbols = [i.lower() for i in df_temp['Symbol'] if i.lower().startswith(letter)]
    for symbol in symbols:
        params_.put((folder_stock_prices, symbol, date_start, date_end))


def main():
    while True:
        p = params_.get()
        save_stock_prices_nasdaq(*p)
        params_.task_done()


def run(letter):
    create_queue(letter)
    number_of_threads = 100
    for _ in range(number_of_threads):
        worker = Thread(target=main, daemon=True)
        worker.start()
    params_.join()
