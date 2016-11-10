
# coding: utf-8

# In[ ]:




# In[1]:

from quandlKey import *
import Quandl
import datetime as dt


# #Moving Average

# In[2]:

def movingAvg(values, window):
    import numpy as np
    import itertools
    
    weights = np.repeat(1.0, window)/window

    #including valid will REQUIRE there to be enough datapoints.
    #for example, if you take out valid, it will start @ point one,
    #not having any prior points, so itll be 1+0+0 = 1 /3 = .3333
    smas = np.convolve(values, weights, 'valid')
    
    smas = pd.Series(smas)
   
    fillWindow = pd.Series(list(itertools.repeat('', window - 1)))

    smas = list(smas.append(fillWindow).reset_index(drop = True))
   
    return smas # as a list


# #AdDe Function

# In[3]:

def dictAdDe(dateParam):
    def requestAdDe(dateParam):
        dateStr = dateParam.strftime('%Y%m%d')

        gh_url = 'http://online.wsj.com/mdc/public/page/2_3021-tradingdiary2-%s.html' %dateStr

        req = requests.get(gh_url)

        b = BeautifulSoup(req.text)

        return {'Date' : dateParam, 'Adv' : int(b.find_all('table')[6].find_all('td')[5].string.replace(',', '')), 'Dec' : int(b.find_all('table')[6].find_all('td')[7].string.replace(',', ''))}
    
    try:
        data = requestAdDe(dateParam)

    except:
        dateParam = dateParam + dt.timedelta(days = -1)
        
        data = requestAdDe(dateParam)
        
    return data


# #Add Missing Friday's to AdDe DF

# In[4]:

def adDeMissingFri():

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

    if listDates:
        for nextDate in listDates:
            #get new data
            dataDict = dictAdDe(nextDate)

            #append to old df
            data.append(dataDict)
        df = pd.DataFrame(data)

        df[['Adv', 'Dec']] = df[['Adv', 'Dec']].astype(int)
        df.set_index(keys = ['Date'], drop = True, inplace = True)
        df.sort_index(ascending = True, inplace = True)

        dfAdDe = dfAdDe.append(df)
    
    #save to result folder & pathFolder
    dfAdDe.to_pickle('./results/AdDe.pkl')
    dfAdDe.to_pickle(pathFolder + '/AdDe' + friDate.strftime('%Y%m%d') + '.pkl')


# #Create AdDe Moving Average

# In[5]:

def AdDeMA(Advance, Decline, startValue):
    # create new dataframe to keep from adding to existing df
    dfParam = pd.DataFrame(Advance)
    dfParam.columns= ['Advance']
    dfParam['Decline'] = Decline
    # create difference of Advance Decline
    dfParam['diff'] = dfParam['Advance'] - dfParam['Decline']
    
    # cummulative differences
    dfParam['cumDiff'] = dfParam['diff'].cumsum() + startValue
    dfParam.ix[0, 'cumDiff'] = startValue
    dfParam.sort(ascending = False, inplace = True)
    
    # get moving average of cummulative differences
    dfParam['cdtAvg'] = movingAvg(dfParam['cumDiff'], 15)
    
    # return only dataframe with cummulative average values
    dfParam = dfParam.ix[:-14]
    
    dfParam['cdtAvg'] = dfParam['cdtAvg'].astype(float)
    return dfParam


# #Function: Build Stock or DJI DF

# In[6]:

def createDF(quandlCode, pklName):
    
    dfDef = Quandl.get(quandlCode, authtoken = quandlKey)

    dfDef.sort_index(ascending = False, inplace = True)

    #Mask for Fridays
    fridayMask = dfDef.index.dayofweek == 4
    dfDefFriday = dfDef.ix[fridayMask]
    
    #add dates for weeks without a Friday open date
    #need to do twice because Christmas and New Year can create a two week gap
    temp1 = pd.Series(dfDefFriday.index)

    newMask = (temp1 - temp1.shift(-1)) > dt.timedelta(days = 7)

    newDates = temp1[newMask] - dt.timedelta(days = 8)
    temp1 = temp1.append(newDates)

    temp1.sort(ascending = False, inplace = True)
    temp1.reset_index(drop = True, inplace = True)

    dfTemp1 = dfDef.ix[temp1]
    dfTemp1.index = pd.to_datetime((dfTemp1.index))
    
    #second addition of dates
    temp2 = pd.Series(dfTemp1.index)
    newMask = (temp2 - temp2.shift(-1)) > dt.timedelta(days = 8)

    newDates = temp2[newMask] - dt.timedelta(days = 7)

    temp2 = temp2.append(newDates)

    temp2.sort(ascending = False, inplace = True)
    temp2.reset_index(drop = True, inplace = True)

    dfDefFriday = dfDef.ix[temp2]
    dfDefFriday.index = pd.to_datetime((dfDefFriday.index))

    ma5 = movingAvg(dfDefFriday.Close, 5)
    ma15 = movingAvg(dfDefFriday.Close, 15)
    ma40 = movingAvg(dfDefFriday.Close, 40)

    dfDefFriday.loc[:, 'ma05'] = ma5
    dfDefFriday.loc[:, 'ma15'] = ma15
    dfDefFriday.loc[:, 'ma40'] = ma40

    #Remove rows that are blank due to ma40
    dfDefFriday = dfDefFriday.ix[:-39]
    
    #set ma's as float
    dfDefFriday['ma05'] = dfDefFriday['ma05'].astype(float)
    dfDefFriday['ma15'] = dfDefFriday['ma15'].astype(float)
    dfDefFriday['ma40'] = dfDefFriday['ma40'].astype(float)
    
    #save to pathFolder
    dfDefFriday.to_pickle(pathFolder + '/' + pklName + friDate.strftime('%Y%m%d') + '.pkl')


# #Function - put AdDe, DJI, and Stock Together

# In[ ]:

def stockDf(quandlStock, pklName):
    #AdDe
    if not ('AdDe' + friDate.strftime('%Y%m%d') + '.pkl') in os.listdir(pathFolder):
        adDeMissingFri()
    if not ('AdDeMovAvg' + friDate.strftime('%Y%m%d') + '.pkl') in os.listdir(pathFolder):
        tempAdDe = pd.read_pickle(pathFolder + '/AdDe' + friDate.strftime('%Y%m%d') + '.pkl')
        # write pickle with Moving Average
        AdDeMA(tempAdDe['Adv'], tempAdDe['Dec'], 20000).to_pickle(pathFolder + '/AdDeMovAvg' + friDate.strftime('%Y%m%d') + '.pkl')
    
    #DJI
    if not ('DJI' + friDate.strftime('%Y%m%d') + '.pkl') in os.listdir(pathFolder):
        createDF('YAHOO/INDEX_DJI', 'DJI')
    #Stock
    createDF(quandlStock, pklName)


# #Call creation of full DF for each stock

# In[ ]:

import os
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup
import requests

today = dt.datetime.now()

shift = today + dt.timedelta(days = -4)

weekNum = shift.strftime('%W')

dateStr = '%s%s5' %(today.year, weekNum)

friDate = dt.datetime.strftime(dt.datetime.strptime(dateStr, '%Y%W%w'), '%Y%m%d')

pathFolder = './results/' + friDate

friDate = dt.datetime.strptime(friDate, '%Y%m%d')

if not os.path.exists(pathFolder):
    os.makedirs(pathFolder)

stockList = pd.read_pickle('./equityLists/volatilityResult100.pkl')
stockList.drop_duplicates(subset = ['quandlCode'], inplace = True)
print('List Length ', stockList.shape[0])
stockList.reset_index(drop = True, inplace = True)

for row, dataFrame in stockList.iterrows():
    print(row)
    stockDf(dataFrame['quandlCode'], dataFrame['Name'].split('/')[0].split(',')[0].split('-')[0].replace('\'', '').strip())

