import pandas as pd
import requests
from bs4 import BeautifulSoup as bs


def stock_prices_google(stock_name, start_date, end_date):
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


def stock_prices_nyse(stock_name, date_start, date_end, page_number=1, df=pd.DataFrame()):
    def set_header(df_header):
        '''reset the header row'''
        df_header_ = df_header.copy()
        df_header_.columns = df_header_.loc[0].tolist()
        df_header_ = df_header_.drop(labels=0)
        return df_header_

    if not df.empty:
        df_ = df.copy()
    else:
        df_ = df

    query = 'http://thestockmarketwatch.com/stock/stock-data.aspx?symbol={stock_name}&action=showHistory&page={page_number}&perPage=150&startMonth={start_month}&startDay={start_day}&startYear={start_year}&endMonth={end_month}&endDay={end_day}&endYear={end_year}'.format(
        stock_name=stock_name, start_day=date_start.timetuple()[2], start_month=date_start.timetuple()[1] - 1, start_year=date_start.timetuple()[0],
        end_day=date_end.timetuple()[2], end_month=date_end.timetuple()[1] - 1, end_year=date_end.timetuple()[0], page_number=page_number)
    response = requests.get(query)

    # find page numbers
    bs_page = bs(response.text, 'lxml')
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
                        return stock_prices_nyse(stock_name, date_start, date_end, page_number, df_)
    return df_
