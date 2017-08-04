import datetime as dt
from utils_price_scraping import save_stock_prices_nasdaq
import pandas as pd


def main():
    df_stocks = pd.read_csv('./stock_symbols/nasdaq_symbols.csv')
    for stock in df_stocks.loc[:10, 'Symbol']:
        save_stock_prices_nasdaq('./stock_prices/nasdaq', stock, dt.datetime(1960, 1, 1), dt.datetime.now())


if __name__ == '__main__':
    main()
