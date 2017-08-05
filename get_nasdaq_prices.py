import datetime as dt
# from utils_price_scraping import save_stock_prices_nyse_asyncio
import pandas as pd
import asyncio
from bs4 import BeautifulSoup as bs

#
# def main(letter):
#     folder_stock_prices = './stock_prices/nasdaq'
#     date_start = dt.datetime(1960, 1, 1)
#     date_end = dt.datetime.now()
#     df_symbols = pd.read_csv('./stock_symbols/nasdaq_symbols.csv')
#     coroutines = [asyncio.ensure_future(save_stock_prices_nyse_asyncio(folder_stock_prices, row['Symbol'], date_start, date_end)) for _, row
#                   in df_symbols.loc[df_symbols['Symbol'].str.lower().str.startswith(letter)].iterrows()]
#
#     event_loop = asyncio.get_event_loop()
#
#     event_loop.run_until_complete(asyncio.wait(coroutines))
#     event_loop.close()
    # for count, stock in enumerate(df_stocks['Symbol']):
    #     if stock.lower().startswith(letter):
    #         print(count, stock)
    #         save_stock_prices_nyse_asyncio('./stock_prices/nasdaq', stock, dt.datetime(1960, 1, 1), dt.datetime.now())


async def stock_prices_nyse_asyncio(stock_name, date_start, date_end, page_number=1, df=pd.DataFrame()):
    def set_header(df_header):
        '''reset the header row'''
        df_header_ = df_header.copy()
        df_header_.columns = df_header_.loc[0].tolist()
        df_header_ = df_header_.drop(labels=0)
        return df_header_

    print(type(df))
    if not df.empty:
        df_ = df.copy()
    else:
        df_ = df

    query = 'http://thestockmarketwatch.com/stock/stock-data.aspx?symbol={stock_name}&action=showHistory&page={page_number}&perPage=150&startMonth={start_month}&startDay={start_day}&startYear={start_year}&endMonth={end_month}&endDay={end_day}&endYear={end_year}'.format(
        stock_name=stock_name, start_day=date_start.timetuple()[2], start_month=date_start.timetuple()[1] - 1,
        start_year=date_start.timetuple()[0],
        end_day=date_end.timetuple()[2], end_month=date_end.timetuple()[1] - 1, end_year=date_end.timetuple()[0],
        page_number=page_number)
    response = await aiohttp.request('GET', query)
    response_text = await response.text()
    # find page numbers
    bs_page = bs(response_text, 'lxml')
    table_data = bs_page.find_all('table', {'class': 'qm_history_historyContent'})
    if table_data:
        df_temp = pd.read_html(str(table_data[0]))[0]
        df_temp = set_header(df_temp)
        df_ = df_.append(df_temp)
        span_page_number = bs_page.find_all('span', {'class': 'qm_text'})
        if len(span_page_number) > 0:
            for span in span_page_number:
                text_ = span.text
                if 'of ' in text_:
                    total_pages = int(text_.split()[-1])
                    if page_number < total_pages:
                        page_number += 1
                        # return (await stock_prices_nyse_asyncio(stock_name, date_start, date_end, page_number, df_))
                        return stock_prices_nyse_asyncio(stock_name, date_start, date_end, page_number, df_)
    return df_


async def save_stock_prices_nasdaq_asyncio(stock_folder, stock_name, date_start, date_end):
    df = await stock_prices_nyse_asyncio(stock_name, date_start, date_end)
    if not df.empty:
        df.to_csv(os.path.join(stock_folder, 'nasdaq_{0}.csv'.format(stock_name.lower())))

# from utils import save_stock_prices_nyse_asyncio
import datetime as dt
import pandas as pd
import os
import asyncio
import aiohttp

df_symbols = pd.read_csv('./stock_symbols/nasdaq_symbols.csv')
stocks_all = df_symbols['Symbol']
stocks = [i.lower()for i in stocks_all if i.lower().startswith('m')][50:150]
print(stocks)
date_start = dt.datetime(1960, 1, 1)
date_end = dt.datetime.now()

# folder_stock_prices = './stock_prices'
# folder_stock_prices = './stock_prices/nasdaq'
folder_stock_prices = './stock_prices'
folder_stock_symbols = './stock_symbols'
# import time
# t1 = time.time()
# df_nyse_symbols = pd.read_csv(os.path.join(folder_stock_symbols, 'NYSE.csv'))
coroutines = [asyncio.ensure_future(save_stock_prices_nasdaq_asyncio(folder_stock_prices, i, date_start, date_end)) for i in stocks]
event_loop = asyncio.get_event_loop()

event_loop.run_until_complete(asyncio.wait(coroutines))
event_loop.close()
