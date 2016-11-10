'''
Run this script Saturday Morning
It will return the Advance and Decline values and add it to AdDe pickle
'''

import os
import urllib.request
import datetime as dt
from bs4 import BeautifulSoup
import pandas as pd
import requests

today = dt.datetime.now()

shift = today + dt.timedelta(days = -4)

weekNum = shift.strftime('%W')

dateStr = '%s%s5' % (today.strftime('%Y'), weekNum)

friDate = dt.datetime.strptime(dateStr, '%Y%W%w')

friDateStr = dt.datetime.strftime(friDate, '%Y%m%d')

pathFolder = './results/' + friDateStr

if not os.path.exists(pathFolder):
    os.makedirs(pathFolder)

# #AdDe Function
def dictAdDe(dateParam):
    def requestAdDe(dateParam):
        dateStr = dateParam.strftime('%Y%m%d')

        gh_url = 'http://online.wsj.com/mdc/public/page/2_3021-tradingdiary2-%s.html' % dateStr

        req = requests.get(gh_url)

        b = BeautifulSoup(req.text)

        return {'date' : dateParam, 'Adv' : int(b.find_all('table')[6].find_all('td')[5].string.replace(',', '')), 'Dec' : int(b.find_all('table')[6].find_all('td')[7].string.replace(',', ''))}
    
    try:
        data = requestAdDe(dateParam)

    except:
        dateParam = dateParam + dt.timedelta(days = -1)
        
        data = requestAdDe(dateParam)
        
    return data


# #Add missing Friday's to AdDe df

dfAdDe = pd.read_pickle('./results/AdDe.pkl')

#find number of weeks missing
daysMissing = friDate - max(dfAdDe.index)

#convert to integer
daysMissing = int(daysMissing.total_seconds() / (60 * 60 * 24))

listDates = []

#loop through
#number of times is 7 divided by the daysMissing
for i in (range(int(daysMissing / 7))):
    listDates.append(friDate + dt.timedelta(days = -(i * 7)))

    
#sort to get oldest date first
listDates.sort(reverse = False)

data = []

for nextDate in listDates:
    #get new data
    dataDict = dictAdDe(nextDate)

    #append to old df
    data.append(dataDict)
df = pd.DataFrame(data)

df[['Adv', 'Dec']] = df[['Adv', 'Dec']].astype(int)
df.set_index(keys = ['date'], drop = True, inplace = True)
df.sort_index(ascending = True, inplace = True)

dfAdDe = dfAdDe.append(df)
#save to result folder & pathFolder
dfAdDe.to_pickle('./results/AdDe.pkl')
dfAdDe.to_pickle(pathFolder + '/AdDe' + friDate.strftime('%Y%m%d') + '.pkl')

# dfAdDe = pd.read_pickle('./results/AdDe.pkl')
# dfAdDe = dfAdDe.ix[:-2]
# dfAdDe.to_pickle('./results/AdDe.pkl')

dfAdDe = pd.read_pickle('./results/AdDe.pkl')
print(dfAdDe.tail())