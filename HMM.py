import datetime as dt
from datetime import datetime
import numpy as np
import pandas as pd
from matplotlib import cm, pyplot as plt
from hmmlearn.hmm import GaussianHMM
import tushare as ts
import sys
import os 

class Calc_HMM(object):
    def __init__(self):
        self.pro = ts.pro_api(
            '')
        self.td = datetime.today()
        self.tdtext = self.td.strftime('%Y%m%d')


    def get_current_month_start_and_end(self, start_date):
        if start_date.count('/') != 2:
            raise ValueError('/ is error')
        year, month = str(start_date).split(
            '/')[2], str(start_date).split('/')[0]
        # end = calendar.monthrange(int(year), int(month))[1]
        start_date = year + start_date[:2] + start_date[3:5]
        return start_date


    def get_datetoordinal(self,str):
        return datetime.strptime(str,"%Y%m%d").toordinal()

    def get_data(self, stockcode, start_date):
        startDate = self.get_current_month_start_and_end(start_date)
        df = self.pro.daily(ts_code=stockcode, start_date=startDate)
        lengend_date = df['trade_date'][1:]
        lengend_date = np.flipud(lengend_date)
        df['trade_date'] = df['trade_date'].apply(self.get_datetoordinal)
        dates = df['trade_date'][1:]
        close = df['close']
        vol = df['vol'][1:]
        close = np.flipud(close)
        diff = np.diff(close)
        dates = np.flipud(dates)
        vol = np.flipud(dates)

        x = np.column_stack([diff, vol])
        min_value = x.mean(axis=0)[0] - 3 * x.std(axis=0)[0]
        max_value = x.mean(axis=0)[0] + 3 * x.std(axis=0)[0]
        ds = pd.DataFrame(x)
        for i in range(len(ds)):
            if (ds.loc[i, 0] < min_value) | (ds.loc[i, 0] > max_value):
                ds.loc[i,0] = x.mean(axis=0)[0]
            else:
                pass
        x_test = ds.iloc[:-3]
        x_pre = ds.iloc[-3:]
        model = GaussianHMM(
            n_components=2, covariance_type='full', n_iter=1000000000, tol=0.000001)
        model.fit(x_test)
        # print(stockcode, model.transmat_, model.means_,
            #   np.dot(model.transmat_, model.means_))
        expected_returns_volumns = np.dot(model.transmat_, model.means_)
        expected_returns = expected_returns_volumns[:,0]
        predicted_price = []
        current_price = close[-4]
        hidden_stats_list = []
        returns_list = []
        for i in range(len(x_pre)):
            hidden_stats = model.predict(x_pre.iloc[i].values.reshape(1,2))
            predicted_price.append(current_price + expected_returns[hidden_stats])
            current_price = predicted_price[i]
            hidden_stats_list.append(hidden_stats[0])
            returns_list.append(expected_returns[hidden_stats][0])
        
        return hidden_stats_list, returns_list


    def generate_HMM(self,stockcode):
        count_stats = {}
        count_stats["1"] = 0
        count_stats["0"] = 0
        avg_return = {}
        avg_return["1"] = 0 
        avg_return["0"] = 0
        counter = 5
        try:
            start_date = datetime.strftime((datetime.today()-dt.timedelta(days=16)).date(), '%m/%d/%Y')
            for i in range(counter):
                for j in self.get_data(stockcode, start_date)[0]:
                    if self.get_data(stockcode, start_date)[0][j] == 1:
                        count_stats["1"] += 1
                        avg_return["1"] += np.mean(self.get_data(stockcode, start_date)[1])
                    else:
                        count_stats["0"] += 1 
                        avg_return["0"] += np.mean(self.get_data(stockcode, start_date)[1])
        except:
            start_date = datetime.strftime((datetime.today()-dt.timedelta(days=32)).date(), '%m/%d/%Y')
            for i in range(counter):
                for j in self.get_data(stockcode, start_date)[0]:
                    if self.get_data(stockcode, start_date)[0][j] == 1:
                        count_stats["1"] += 1
                        avg_return["1"] += np.mean(self.get_data(stockcode, start_date)[1])
                    else:
                        count_stats["0"] += 1 
                        avg_return["0"] += np.mean(self.get_data(stockcode, start_date)[1])

        avg_return["1"] = avg_return["1"]/(3*counter)
        avg_return["0"] = avg_return["0"]/(3*counter)
        if count_stats["1"] > count_stats["0"]:
            map_key = "1"
        else:
            map_key = "0"
        return avg_return[map_key]     


