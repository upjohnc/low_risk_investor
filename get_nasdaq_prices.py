import datetime as dt
from utils_price_scraping import save_stock_prices_nyse_asyncio
import pandas as pd
import asyncio


def main(letter):
    folder_stock_prices = './stock_prices/nasdaq'
    date_start = dt.datetime(1960, 1, 1)
    date_end = dt.datetime.now()
    df_symbols = pd.read_csv('./stock_symbols/nasdaq_symbols.csv')
    coroutines = [asyncio.ensure_future(save_stock_prices_nyse_asyncio(folder_stock_prices, row['Symbol'], date_start, date_end)) for _, row
                  in df_symbols.loc[df_symbols['Symbol'].str.lower().str.startswith(letter)].iterrows()]

    event_loop = asyncio.get_event_loop()

    event_loop.run_until_complete(asyncio.wait(coroutines))
    event_loop.close()
    # for count, stock in enumerate(df_stocks['Symbol']):
    #     if stock.lower().startswith(letter):
    #         print(count, stock)
    #         save_stock_prices_nyse_asyncio('./stock_prices/nasdaq', stock, dt.datetime(1960, 1, 1), dt.datetime.now())
