import tushare as ts
import os
import sys
import datetime as dt
from datetime import datetime
import numpy as np
import calendar
import pandas as pd
import csv
import pymysql
from functools import reduce
from hmm import Calc_HMM
from sharp import Calc_Sharp
from market import Calc_SMA

class Filter_stock(object):
    def __init__(self):
        self.pro = ts.pro_api(
            '')
        self.exchange_market = ["SZSE","SSE"]
        self.dbs = pymysql.connect(host='127.0.0.1', port=3306, db='stock_market',
                                  user='', password='', charset="utf8")
        self.td = datetime.today()
        self.tdtext = self.td.strftime('%Y%m%d')
        # self.benchmark_date = (datetime.today()-dt.timedelta(days=21)).date().strftime('%Y-%m-%d')

    
    def query_stock_repo(self):
        sql = "SELECT stock_code FROM stock_repo_info"
        df_trading = pd.read_sql(sql, self.dbs)
        on_trading = df_trading["stock_code"].to_list()
        return on_trading
        

    def query_annual_fkpi(self):
        current_dcf_period = self.tdtext[:4] + "_" + str(int(self.tdtext[:4]) + 4)
        sql = "SELECT stock_code, dcf, annual_profit_dedt,  debt_to_assets, annual_grossprofit_margin FROM stock_financial_info WHERE dcf > 0 and dcf_period = '%s' and annual_profit_dedt > 10000000 and debt_to_assets < 50 and annual_grossprofit_margin > 15 and annual_operationcashflow_to_profit > 1 and rd_exp > 0" % current_dcf_period
        annual_kpi = pd.read_sql(sql, self.dbs)
        annual_kpi.rename(columns={'stock_code':'ts_code'}, inplace = True)
        return annual_kpi


    def query_quarter_kpi(self):
        if int(self.tdtext[4:6]) >= 1 and int(self.tdtext[4:6]) <= 3:
            q_gr_yoy = "q4_grossincome_growth"
            q_op_yoy = "q4_operationprofit_growth"
            q_profit_yoy = "q4_profit_growth"
        elif int(self.tdtext[4:6]) >= 4 and int(self.tdtext[4:6]) <= 8:
            q_gr_yoy = "q1_grossincome_growth"
            q_op_yoy = "q1_operationprofit_growth"
            q_profit_yoy = "q1_profit_growth"
        elif int(self.tdtext[4:6]) >= 9 and int(self.tdtext[4:6]) <= 10:
            q_gr_yoy = "q2_grossincome_growth"
            q_op_yoy = "q2_operationprofit_growth"
            q_profit_yoy = "q2_profit_growth"
        else:
            q_gr_yoy = "q3_grossincome_growth"
            q_op_yoy = "q3_operationprofit_growth"
            q_profit_yoy = "q3_profit_growth"        

        sql = "SELECT stock_code, %s, %s, %s FROM stock_financial_info WHERE %s IS NOT NULL AND ocfps > eps" % (q_gr_yoy, q_op_yoy, q_profit_yoy, q_profit_yoy)
        quarter_kpi = pd.read_sql(sql, self.dbs)
        quarter_kpi.rename(columns={'stock_code': 'ts_code'}, inplace=True)
        quarter_kpi.columns = ["ts_code", "q_gr_yoy", "q_op_yoy", "q_profit_yoy"]

        return quarter_kpi


    def query_close(self):
        sample_df = self.pro.bak_daily(ts_code="600000.SH", fields='trade_date, ts_code,close,pe,activity', limit="1")
        tradeDate = sample_df['trade_date'].values[0]
        daily_kpi = self.pro.bak_daily(
            trade_date=tradeDate, fields='ts_code, close, pe, activity, strength, vol, total_mv')
        return daily_kpi


    def filter_stock(self, daily_kpi, quarter_kpi, annual_kpi):
        select_code = {}
        # avg_activity = daily_kpi['activity'].mean()
        # avg_strength = daily_kpi['strength'].mean() 
        avg_activity = -2000
        avg_strength = -2000
        dfs = [annual_kpi, quarter_kpi, daily_kpi]
        df_kpi = reduce(lambda left,right: pd.merge(left,right,on='ts_code',how='left'), dfs)
        df_kpi = df_kpi.dropna()
        df_kpi = df_kpi.drop_duplicates()
        # print(df_kpi['total_mv'])
        # firstportfolio = df_kpi.loc[(df_kpi['close']/df_kpi['dcf'] > 0.9)
        #                            & (df_kpi['q_gr_yoy'] > 1) & (df_kpi['q_op_yoy'] > 1) & (df_kpi['q_profit_yoy'] > 1) & (df_kpi['strength'] > avg_strength) & (df_kpi['activity'] > avg_activity) & df_kpi['ocfps']>=df_kpi['q_eps'], df_kpi['q_profit_yoy'] > df_kpi['pe']]
        firstportfolio = df_kpi.loc[(df_kpi['dcf']/df_kpi['close'] > 0.9)
                                    & (df_kpi['q_gr_yoy'] > 1) & (df_kpi['q_op_yoy'] > 1) & (df_kpi['q_profit_yoy'] > 1) & (df_kpi['strength'] > avg_strength) & (df_kpi['total_mv'] > 50) & (df_kpi['activity'] > avg_activity) & (df_kpi['q_profit_yoy'] > df_kpi['pe']), ['ts_code', 'dcf', 'close', 'strength', 'activity']]
        firstportfolio = firstportfolio.sort_values(
            by="activity", ascending=False)
        firstportfolio['dcf_vs_close'] = firstportfolio['dcf'] - \
            firstportfolio['close']
        stock_pool = firstportfolio["ts_code"].tolist()
        
        for stock_code in stock_pool:
            hmm = Calc_HMM()
            firstportfolio.loc[firstportfolio["ts_code"] == stock_code, [
                "proj_val_change"]] = hmm.generate_HMM(stock_code)/firstportfolio.loc[firstportfolio["ts_code"] == stock_code, [
                    "close"]].values[0]

            risk_kpi = Calc_Sharp()
            firstportfolio.loc[firstportfolio["ts_code"] == stock_code, [
                "beta", "sharp"]] = risk_kpi.generate_beta_sharp(stock_code)

            sma = Calc_SMA()
            firstportfolio.loc[firstportfolio["ts_code"] == stock_code, [
                "trend", "position"]] = sma.generate_sma(stock_code)

        return firstportfolio

    def portfolio_score(self, firstportfolio, on_trading):
        stock_score = {}
        stock_pool = firstportfolio["ts_code"].tolist()
        # print(firstportfolio)
        hmm_pool = np.array(
            firstportfolio["proj_val_change"].tolist()).argsort().tolist()
        sharp_pool = np.array(
            firstportfolio["sharp"].tolist()).argsort().tolist()
        dcf_profit_pool = np.array(
            firstportfolio["dcf_vs_close"].tolist()).argsort().tolist()
        active_pool = dcf_profit_pool = np.array(
            firstportfolio["activity"].tolist()).argsort().tolist()
        strength_pool = dcf_profit_pool = np.array(
            firstportfolio["strength"].tolist()).argsort().tolist()

        for index, value in enumerate(stock_pool):
            stock_score.setdefault(value, {})["index"] = index
            stock_score.setdefault(value, {})["total_score"] = 0

        for key, value in stock_score.items():
            stock_score.setdefault(key, {})["hmm_score"] = hmm_pool.index(value.get("index"))
            stock_score.setdefault(key, {})["sharp_score"] = sharp_pool.index(value.get("index"))
            stock_score.setdefault(key, {})["dcf_vs_close_score"] = dcf_profit_pool.index(value.get("index"))
            stock_score.setdefault(key, {})["active_pool_score"] = (active_pool.index(value.get("index")))
            stock_score.setdefault(key, {})["strength_pool_score"] = (strength_pool.index(value.get("index")))

        for i in range(len(firstportfolio)):
            for key, value in stock_score.items():
                if key == firstportfolio.iloc[i]["ts_code"]:
                    if firstportfolio.iloc[i]["trend"] == "perfect":
                        stock_score.setdefault(key, {})["trend_score"] = 5
                    elif firstportfolio.iloc[i]["trend"] == "radical":
                        stock_score.setdefault(key, {})["trend_score"] = 4
                    elif firstportfolio.iloc[i]["trend"] == "good":
                        stock_score.setdefault(key, {})["trend_score"] = 3
                    elif firstportfolio.iloc[i]["trend"] == "flat":
                        stock_score.setdefault(key, {})["trend_score"] = 1
                    else:
                        stock_score.setdefault(key, {})["trend_score"] = 0

                    if firstportfolio.iloc[i]["position"] == "perfect":
                        stock_score.setdefault(key, {})["position_score"] = 5
                    elif firstportfolio.iloc[i]["position"] == "radical":
                        stock_score.setdefault(key, {})["position_score"] = 4
                    elif firstportfolio.iloc[i]["position"] == "good":
                        stock_score.setdefault(key, {})["position_score"] = 3
                    else:
                        stock_score.setdefault(key, {})["position_score"] = 0
                else:
                    pass

        df_score = pd.DataFrame.from_dict(stock_score, orient="index", dtype=None, columns= None)
        df_score = df_score.reset_index()
        df_score = df_score.fillna(0)
        df_score["total_score"] = df_score["hmm_score"] + \
            df_score["sharp_score"] + \
            df_score["dcf_vs_close_score"] + \
            df_score["active_pool_score"] + \
            df_score["strength_pool_score"] + \
            df_score["trend_score"] + \
            df_score["position_score"]
        df_score.rename(columns={'level_0': 'ts_code'}, inplace=True)
        df_score = df_score.sort_values(by="total_score", ascending=True)

        df_score["hmm_normalized"] = df_score["hmm_score"] / \
            df_score["hmm_score"].max()
        df_score["sharp_normalized"] = df_score["sharp_score"] / \
            df_score["sharp_score"].max()
        df_score["dcf_vs_close_normalized"] = df_score["dcf_vs_close_score"] / \
            df_score["dcf_vs_close_score"].max()
        df_score["active_pool_normalized"] = df_score["active_pool_score"] / \
            df_score["active_pool_score"].max()
        df_score["strength_pool_normalized"] = df_score["strength_pool_score"] / \
            df_score["strength_pool_score"].max()
        df_score["trend_normalized"] = df_score["trend_score"] / \
            df_score["trend_score"].max()
        df_score["position_normalized"] = df_score["position_score"] / \
            df_score["position_score"].max()
        df_score["total_score_normalized"] = df_score["total_score"] / \
            df_score["total_score"].max()

        print(df_score["total_score"])
        # print(on_trading)
        stock_portfolio = []
        stock_list = df_score["ts_code"].to_list()
        for stock_code in stock_list:
            if stock_code in on_trading:
                pass
            else:
                stock_portfolio.append(stock_code)
        return df_score, stock_portfolio


    def generate_portfolio(self):
        daily_kpi = self.query_close()
        annual_kpi = self.query_annual_fkpi()
        quarter_kpi = self.query_quarter_kpi()
        firstportfolio = self.filter_stock(daily_kpi, annual_kpi, quarter_kpi)
        on_trading = self.query_stock_repo()
        df_score, stock_portfolio = self.portfolio_score(firstportfolio,on_trading)
        return df_score, stock_portfolio
    



