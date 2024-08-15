# -*- coding: utf-8 -*-
import os
import sys
import datetime as dt
from datetime import datetime
import math
import pandas as pd
import numpy as np
import tushare as ts
import akshare as ak

class Calc_Sharp(object):
    def __init__(self):
        self.pro = ts.pro_api(
            '')
        self.td = datetime.today()
        self.tdtext = self.td.strftime('%Y%m%d')
        self.time_range = datetime.strftime(
            (datetime.today()-dt.timedelta(days=16)).date(), '%m/%d/%Y')


    def get_interest_rate(self):
        df = ak.macro_bank_china_interest_rate()
        # print(df)
        if df.iloc[-1] == 0:
            rf = df.iloc[-2]
        else:
            rf = df.iloc[-1]

        return rf


    def get_current_month_start_and_end(self):
        if self.time_range.count('/') != 2:
            raise ValueError('/ is error')
        year, month = str(self.time_range).split(
            '/')[2], str(self.time_range).split('/')[0]
        # end = calendar.monthrange(int(year), int(month))[1]
        start_date = year + self.time_range[:2] + self.time_range[3:5]
        return start_date


    def sz_beta_sharp(self, stockcode):
        startDate = self.get_current_month_start_and_end()
        rf = self.get_interest_rate()
        #提取股票每日行情
        df1 = self.pro.daily(ts_code=stockcode, start_date=startDate,
                        end_date=self.tdtext, fields = 'trade_date, pct_chg').sort_values(by='trade_date')
        #提取深市指数
        df2 = self.pro.index_daily(ts_code='399300.SZ', start_date=startDate,
                            end_date=self.tdtext, fields='trade_date, pct_chg').sort_values(by='trade_date')
        #计算贝塔系数
        s1 = df1['pct_chg']
        s2 = df2['pct_chg']
        beta = (np.cov(s1, s2))[0][1]/np.var(s2)

        #计算夏普比率
        df1['ex_pct_close'] = df1['pct_chg'] - rf/252
        sharp = (df1['ex_pct_close'].mean() * math.sqrt(252)) / \
            df1['ex_pct_close'].std()
            
        return beta, sharp
        

    def sh_beta_sharp(self, stockcode):
        startDate = self.get_current_month_start_and_end()
        rf = self.get_interest_rate()
        #提取股票每日行情
        df1 = self.pro.daily(ts_code=stockcode, start_date=startDate,
                        end_date=self.tdtext, fields='trade_date, pct_chg').sort_values(by='trade_date')
        #提取沪市指数
        df2 = self.pro.index_daily(ts_code='000001.SH', start_date=startDate,
                            end_date=self.tdtext, fields='trade_date, pct_chg').sort_values(by='trade_date')

        #计算贝塔系数
        try:
            s1 = df1['pct_chg']
            s2 = df2['pct_chg']
            beta = (np.cov(s1, s2))[0][1]/np.var(s2)
        except:
            s1 = df1['pct_chg']
            s2 = df2['pct_chg'][:-1]
            beta = (np.cov(s1, s2))[0][1]/np.var(s2)
        #计算夏普比率
        df1['ex_pct_close'] = df1['pct_chg'] - rf/252
        sharp = (df1['ex_pct_close'].mean() * math.sqrt(252)) / \
            df1['ex_pct_close'].std()

        return beta, sharp


    def generate_beta_sharp(self,stockcode):
        if stockcode[-2] == "Z":
            beta, sharp = self.sz_beta_sharp(stockcode)
        else:
            beta, sharp = self.sh_beta_sharp(stockcode)

        return beta, sharp

