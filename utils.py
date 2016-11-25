import pandas as pd
import requests
from bs4 import BeautifulSoup as bs


def stock_prices(stock_name, start_date, end_date):
    '''
    stock_name format: exchange:ticker
    eg: NASDAQ:AAPL
    '''
    row_number = start_number = 0
    step = rows_returned = 10
    df = pd.DataFrame()
    while True:
        query = 'https://www.google.com/finance/historical?q={stock[0]}%3A{stock[1]}&startdate={start_month}%20{start_day}%2C%20{start_year}&enddate={end_month}%20{end_day}%2C%20{end_year}&ei=NXw3WMG9KtWNmAHcobLABw&start={start_number}&num={rows_returned}'.format(
            stock=stock_name.split(':'), start_month=start_date.strftime('%b'), start_day=start_date.day, start_year=start_date.year,
            end_month=end_date.strftime('%b'), end_day=end_date.day, end_year=end_date.year, start_number=row_number, rows_returned=rows_returned)
        response = requests.get(query)
        bs_response = bs(response.text, 'lxml')
        if len(bs_response.find_all(id="prices")) == 0:
            break
        df_temp = pd.read_html(str(bs_response.find_all(id="prices")[0]))[0]
        df_temp = df_temp.drop(labels=0)
        df_temp.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df = df.append(df_temp)
        row_number += step
    df.reset_index(drop=True, inplace=True)
    return df
