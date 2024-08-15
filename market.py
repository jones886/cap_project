import tushare as ts
import xlrd
import xlwt
from pandas.core.frame import DataFrame
import pandas as pd
import time
import numpy as np
import sys
import os
import datetime as dt
from datetime import datetime, date


class Calc_SMA(object):
    def __init__(self):
        self.pro = ts.pro_api(
            '')
        self.td = datetime.today()
        self.tdtext = self.td.strftime('%Y%m%d')
        self.time_range = datetime.strftime(
            (datetime.today()-dt.timedelta(days=360)).date(), '%Y%m%d')


    def get_transation(self, stockcode):
        df_market = self.pro.daily(ts_code=stockcode,
                            start_date=self.time_range, end_date=self.tdtext)
        df_market['trade_date'] = pd.to_datetime(df_market['trade_date'])
        dates = pd.DatetimeIndex(df_market['trade_date']).date.astype(np.datetime64)
        opens = df_market.open.values
        highs = df_market.high.values
        lows = df_market.low.values
        closes = df_market.close.values
        volumes = df_market.vol.values
        return dates,opens, highs, lows, closes, volumes


    def calc_sma(self, N, dates, close_p):
        weights = np.ones(N) / N
        sma = np.convolve(close_p, weights, 'valid')
        return sma


    def generate_sma(self, stockcode):
        dates, opens, highs, lows, closes, volumes = self.get_transation(stockcode)
        first_day, last_day = dates[0], dates[-1]
        N5 = 5
        sma_5 = self.calc_sma(N5, dates, closes)
        N13 = 13
        sma_13 = self.calc_sma(N13, dates, closes)
        N55 = 55
        sma_55 = self.calc_sma(N55, dates, closes)
        N100 = 100
        sma_100 = self.calc_sma(N100, dates, closes)
        N200 = 200
        sma_200 = self.calc_sma(N200, dates, closes)
        price_sort = np.array([sma_5[0], sma_13[0], sma_55[0], sma_100[0], sma_200[0]])
        if price_sort.argsort().tolist() == [4, 3, 2, 1, 0] and round((sma_5[0]/sma_200[0])-1, 4) < 0.35 and round((closes[0]/sma_200[0])-1, 4) > 0:
            trend = "perfect"
        elif price_sort.argsort().tolist() == [4, 3, 2, 1, 0] and round((sma_5[0]/sma_200[0])-1, 4) > 0.40 and round((closes[0]/sma_200[0])-1, 4) > 0:
            trend = "radical"
        elif (price_sort.argsort().tolist() == [3, 4, 2, 1, 0] or price_sort.argsort().tolist() == [4, 3, 2, 0, 1]) and round((sma_5[0]/sma_200[0])-1, 4) < 0.35 and round((closes[0]/sma_200[0])-1, 4) > 0:
            trend = "good"
        elif price_sort.argsort().tolist() == [0, 1, 2, 3, 4]:
            trend = "bad"
        else:
            trend = "flat"
        
        if np.max(highs[0:200]) == np.max(highs):
            if closes[0]/np.max(highs[0:200]) < 0.75:
                position = "perfect"
            else:
                position = "radical"
        else:
            if closes[0]/np.max(highs) < 0.75:
                position = "good"
            else:
                position = "bad"

        return trend, position




