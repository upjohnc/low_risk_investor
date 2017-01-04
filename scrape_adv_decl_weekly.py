import os
import datetime as dt
from bs4 import BeautifulSoup
import pandas as pd
import requests

today = dt.datetime.now()
shift_date = today - dt.timedelta(days=4)
week_number = shift_date.strftime('%W')
week_friday_string = '{0}{1}5'.format(today.strftime('%Y'), str(week_number))

date_friday = dt.datetime.strptime(week_friday_string, '%Y%W%w')
date_friday_string = dt.datetime.strftime(date_friday, '%Y%m%d')

path_folder = './results/' + date_friday_string
if not os.path.exists(path_folder):
    os.makedirs(path_folder)


def scrape_adv_decl(date_scrape):
    date_string = date_scrape.strftime('%Y%m%d')
    wsj_url = 'http://online.wsj.com/mdc/public/page/2_3021-tradingdiary2-{}.html'.format(date_string)
    wsj_response = requests.get(wsj_url)
    wsj_bs_html = BeautifulSoup(wsj_response.text, 'html.parser')

    return {'date': date_scrape, 'adv': int(wsj_bs_html.find_all('table')[6].find_all('td')[5].string.replace(',', '')),
            'decl': int(wsj_bs_html.find_all('table')[6].find_all('td')[7].string.replace(',', ''))}


def dict_adv_decl(date_param):
    try:
        data = scrape_adv_decl(date_param)
    except:
        # for holidays
        date_param = date_param - dt.timedelta(days=1)
        data = dict_adv_decl(date_param)

    return data

df_adv_decl = pd.read_pickle('./results/adv_decl.pkl')

# find number of weeks missing
days_missing = (date_friday - max(df_adv_decl.index)).days

dates_list = list()

# loop through
# number of times is 7 divided by the days_missing
for i in (range(int(days_missing / 7))):
    dates_list.append(date_friday - dt.timedelta(days=(i * 7)))
# sort to get oldest date first
dates_list.sort(reverse=False)

data = list()

for next_date in dates_list:
    # get new data
    data_dict = dict_adv_decl(next_date)
    # append to old df
    data.append(data_dict)

df = pd.DataFrame(data)

df[['adv', 'decl']] = df[['adv', 'decl']].astype(int)
df.set_index(keys=['date'], drop=True, inplace=True)
df.sort_index(ascending=True, inplace=True)

df_adv_decl = df_adv_decl.append(df)

# save to result folder & path_folder
df_adv_decl.to_pickle('./results/adv_decl.pkl')
df_adv_decl.to_pickle(os.path.join(path_folder, 'adv_decl_{}.pkl'.format(date_friday.strftime('%Y%m%d'))))
