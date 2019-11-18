from datetime import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
from market_profile import MarketProfile
import time

class DailyPOC(object):
    def mp_poc(self, data):
        '''Calculates the POC for each Trading Session during the week'''
        na0 = np.where((data.index.dayofweek == 6) & (data.index.hour == 17) & (data.index.minute == 0) & (data.index.second == 0)) #for IB (17, 0)
        nb0 = np.where((data.index.dayofweek == 4) & (data.index.hour == 15) & (data.index.minute == 59) & (data.index.second == 0))#for IB (15, 59)
        na1 = np.where(((data.index.dayofweek == 0)|(data.index.dayofweek == 1)|(data.index.dayofweek == 2)\
            |(data.index.dayofweek == 3)) & (data.index.hour == 15) & (data.index.minute == 59) & (data.index.second == 0))#for IB (15, 31)
        nb1 = np.where(((data.index.dayofweek == 0)|(data.index.dayofweek == 1)|(data.index.dayofweek == 2)\
            |(data.index.dayofweek == 3)) & (data.index.hour == 17) & (data.index.minute == 0) & (data.index.second == 0))#for IB (15, 14)
        nmaster = np.concatenate([na0[0], nb0[0], na1[0], nb1[0]])
        nmaster = nmaster[nmaster>=na0[0][0]]
        nmaster.sort()
        mp = MarketProfile(data, tick_size=0.25)
        data['POC'] = np.nan
        for i,k in zip(nmaster[0::2], nmaster[1::2]):
            a = data.index[i]
            b = data.index[k]
            mp_slice = mp[a:b]
            data.loc[b, 'Points'] = mp_slice.poc_price
        self.pocs = data.Points.dropna().values
        data['POC'] = data.Points.fillna(method='ffill')
        data['PPOC'] = np.nan

    def lower_PPOC(self, data):
        '''If the Close price is below the POC, it calculates the last lower POC'''
        for x in range(len(self.pocs)-1,0,-1):
            n = data[(data.Points == self.pocs[x-1])]
            a = np.where(self.pocs[x::-1] < self.pocs[x])
            if len(a[0]) == 0:
                continue
            else:
                data.loc[(data.POC == self.pocs[x]) & (data.Close < data.POC) & (data.index >= n.index[0]), 'PPOC'] = self.pocs[x-a[0][0]]
            for y in self.pocs[x-a[0][0]-1::-1]:
                if y < self.pocs[x]:
                    if (data[(data.POC == self.pocs[x]) & (data.Close < data.POC) & (data.Close < data.PPOC) & (data.index >= n.index[0])].empty):
                        break
                    else:
                        data.loc[(data.POC == self.pocs[x]) & (data.Close < data.POC) & (data.Close < data.PPOC) & (data.index >= n.index[0]), 'PPOC'] = y
    def upper_PPOC(self, data):
        '''If the Close price is above the POC, it calculates the last upper POC'''
        for x in range(len(self.pocs)-1,0,-1):
            n = data[(data.Points == self.pocs[x-1])]
            a = np.where(self.pocs[x::-1] > self.pocs[x])
            if len(a[0]) == 0:
                continue
            else:
                data.loc[(data.POC == self.pocs[x]) & (data.Close > data.POC) & (data.index >= n.index[0]), 'PPOC'] = self.pocs[x-a[0][0]]
            for y in self.pocs[x-a[0][0]-1::-1]:
                if y > self.pocs[x]:
                    if (data[(data.POC == self.pocs[x]) & (data.Close > data.POC) & (data.Close > data.PPOC) & (data.index >= n.index[0])].empty):
                        break
                    else:
                        data.loc[(data.POC == self.pocs[x]) & (data.Close > data.POC) & (data.Close > data.PPOC) & (data.index >= n.index[0]), 'PPOC'] = y

class Weekly3POC(object):
    def mp_poc(self, data):
        '''Calculates the POC for the week'''
        na0 = np.where((data.index.dayofweek == 6) & (data.index.hour == 18) & (data.index.minute == 0) & (data.index.second == 0))
        nb0 = np.where((data.index.dayofweek == 2) & (data.index.hour == 8) & (data.index.minute == 30) & (data.index.second == 59))
        na1 = np.where((data.index.dayofweek == 2) & (data.index.hour == 9) & (data.index.minute == 31) & (data.index.second == 0))
        nb1 = np.where((data.index.dayofweek == 3) & (data.index.hour == 0) & (data.index.minute == 0) & (data.index.second == 59))
        na2 = np.where((data.index.dayofweek == 3) & (data.index.hour == 0) & (data.index.minute == 1) & (data.index.second == 0))
        nb2 = np.where((data.index.dayofweek == 4) & (data.index.hour == 16) & (data.index.minute == 59) & (data.index.second == 59))
        nmaster = np.concatenate([na0[0], nb0[0], na1[0], nb1[0], na2[0], nb2[0]])
        nmaster = nmaster[nmaster>=na0[0][0]]
        nmaster.sort()
        mp = MarketProfile(data, tick_size=0.25)
        data['POC3w'] = np.nan
        for i,k in zip(nmaster[0::2], nmaster[1::2]):
            a = data.index[i]
            b = data.index[k]
            mp_slice = mp[a:b]
            data.loc[b, 'Points3w'] = mp_slice.poc_price
        self.pocs3w = data.Points3w.dropna().values
        data['POC3w'] = data.Points3w.fillna(method='ffill')
        data['PPOC3w'] = np.nan

    def lower_PPOC(self, data):
        '''If the Close price is below the POC, it calculates the last lower POC'''
        for x in range(len(self.pocs3w)-1,0,-1):
            n = data[(data.Points3w == self.pocs3w[x-1])]
            a = np.where(self.pocs3w[x::-1] < self.pocs3w[x])
            if len(a[0]) == 0:
                continue
            else:
                data.loc[(data.POC3w == self.pocs3w[x]) & (data.Close < data.POC3w) & (data.index >= n.index[0]), 'PPOC3w'] = self.pocs3w[x-a[0][0]]
            for y in self.pocs3w[x-a[0][0]-1::-1]:
                if y < self.pocs3w[x]:
                    if (data[(data.POC3w == self.pocs3w[x]) & (data.Close < data.POC3w) & (data.Close < data.PPOC3w) & (data.index >= n.index[0])].empty):
                        break
                    else:
                        data.loc[(data.POC3w == self.pocs3w[x]) & (data.Close < data.POC3w) & (data.Close < data.PPOC3w) & (data.index >= n.index[0]), 'PPOC3w'] = y
    def upper_PPOC(self, data):
        '''If the Close price is above the POC, it calculates the last upper POC'''
        for x in range(len(self.pocs3w)-1,0,-1):
            n = data[(data.Points3w == self.pocs3w[x-1])]
            a = np.where(self.pocs3w[x::-1] > self.pocs3w[x])
            if len(a[0]) == 0:
                continue
            else:
                data.loc[(data.POC3w == self.pocs3w[x]) & (data.Close > data.POC3w) & (data.index >= n.index[0]), 'PPOC3w'] = self.pocs3w[x-a[0][0]]
            for y in self.pocs3w[x-a[0][0]-1::-1]:
                if y > self.pocs3w[x]:
                    if (data[(data.POC3w == self.pocs3w[x]) & (data.Close > data.POC3w) & (data.Close > data.PPOC3w) & (data.index >= n.index[0])].empty):
                        break
                    else:
                        data.loc[(data.POC3w == self.pocs3w[x]) & (data.Close > data.POC3w) & (data.Close > data.PPOC3w) & (data.index >= n.index[0]), 'PPOC3w'] = y

class WeeklyPOC(object):
    def mp_poc(self, data):
        '''Calculates the POC for the week'''
        na0 = np.where((data.index.dayofweek == 6) & (data.index.hour == 18) & (data.index.minute == 0) & (data.index.second == 0))
        nb0 = np.where((data.index.dayofweek == 4) & (data.index.hour == 16) & (data.index.minute == 59) & (data.index.minute == 59) & (data.index.second == 59))
        nmaster = np.concatenate([na0[0], nb0[0]])
        nmaster = nmaster[nmaster>=na0[0][0]]
        nmaster.sort()
        mp = MarketProfile(data, tick_size=0.25)
        data['POCw'] = np.nan
        for i,k in zip(nmaster[0::2], nmaster[1::2]):
            a = data.index[i]
            b = data.index[k]
            mp_slice = mp[a:b]
            data.loc[b, 'Pointsw'] = mp_slice.poc_price
        self.pocsw = data.Pointsw.dropna().values
        data['POCw'] = data.Pointsw.fillna(method='ffill')
        data['PPOCw'] = np.nan

    def lower_PPOC(self, data):
        '''If the Close price is below the POC, it calculates the last lower POC'''
        for x in range(len(self.pocsw)-1,0,-1):
            n = data[(data.Pointsw == self.pocsw[x-1])]
            a = np.where(self.pocsw[x::-1] < self.pocsw[x])
            if len(a[0]) == 0:
                continue
            else:
                data.loc[(data.POCw == self.pocsw[x]) & (data.Close < data.POCw) & (data.index >= n.index[0]), 'PPOCw'] = self.pocsw[x-a[0][0]]
            for y in self.pocsw[x-a[0][0]-1::-1]:
                if y < self.pocsw[x]:
                    if (data[(data.POCw == self.pocsw[x]) & (data.Close < data.POCw) & (data.Close < data.PPOCw) & (data.index >= n.index[0])].empty):
                        break
                    else:
                        data.loc[(data.POCw == self.pocsw[x]) & (data.Close < data.POCw) & (data.Close < data.PPOCw) & (data.index >= n.index[0]), 'PPOCw'] = y
    def upper_PPOC(self, data):
        '''If the Close price is above the POC, it calculates the last upper POC'''
        for x in range(len(self.pocsw)-1,0,-1):
            n = data[(data.Pointsw == self.pocsw[x-1])]
            a = np.where(self.pocsw[x::-1] > self.pocsw[x])
            if len(a[0]) == 0:
                continue
            else:
                data.loc[(data.POCw == self.pocsw[x]) & (data.Close > data.POCw) & (data.index >= n.index[0]), 'PPOCw'] = self.pocsw[x-a[0][0]]
            for y in self.pocsw[x-a[0][0]-1::-1]:
                if y > self.pocsw[x]:
                    if (data[(data.POCw == self.pocsw[x]) & (data.Close > data.POCw) & (data.Close > data.PPOCw) & (data.index >= n.index[0])].empty):
                        break
                    else:
                        data.loc[(data.POCw == self.pocsw[x]) & (data.Close > data.POCw) & (data.Close > data.PPOCw) & (data.index >= n.index[0]), 'PPOCw'] = y

if __name__ == '__main__':

    data = pd.read_csv('C:/Users/MiloZB/Dropbox/Codigos/Data/ES_1sec(2018)Ninja.csv', parse_dates=True, index_col='Date')
    print('Starting')
    day = DailyPOC()
    day.mp_poc(data)
    day.lower_PPOC(data)
    day.upper_PPOC(data)
    data.drop(columns=['Points'], inplace=True)
    week3 = Weekly3POC()
    week3.mp_poc(data)
    week3.lower_PPOC(data)
    week3.upper_PPOC(data)
    data.drop(columns=['Points3w'], inplace=True)
    week = WeeklyPOC()
    week.mp_poc(data)
    week.lower_PPOC(data)
    week.upper_PPOC(data)
    data.drop(columns=['Pointsw'], inplace=True)
    data.to_csv('C:/Users/MiloZB/Dropbox/Codigos/Data/ES_1sec(2018)NinjaWPOC.csv')
    print('Finish')