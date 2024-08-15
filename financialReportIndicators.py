import tushare as ts
import os
import sys
import datetime as dt
from datetime import datetime
import numpy as np
import pandas as pd
import pymysql


def query_sql_all():
    f_kpi_all = []
    db = pymysql.connect(host='localhost', port=3306, db='stock_market',
                         user='', password='', charset="utf8")
    cur = db.cursor()
    sql = "SELECT stock_code FROM stock_financial_info"
    cur.execute(sql)
    results = cur.fetchall()
    for row in results:
        if row[0]:
            f_kpi_all.append(row[0])
        else:
            pass
    cur.close()
    # db.close()
    return f_kpi_all


def annual_report_indicators(pro, f_kpi_all, tdtext):
    report_period = str(int(tdtext[:4])-1) + "1"
    for stock_code in f_kpi_all:
        annual_kpi = pro.query('fina_indicator_vip', period=report_period, ts_code = stock_code, fields='ts_code, profit_dedt, grossprofit_margin, debt_to_assets, ocf_to_profit, rd_exp')
        if len(annual_kpi) != 0:
            annual_kpi = annual_kpi.fillna(0)
            update_sql_fkpi(annual_kpi.iloc[0]["ts_code"], "annual_profit_dedt",  annual_kpi.iloc[0]["profit_dedt"])
            update_sql_fkpi(annual_kpi.iloc[0]["ts_code"], "annual_grossprofit_margin",  annual_kpi.iloc[0]["grossprofit_margin"])
            update_sql_fkpi(annual_kpi.iloc[0]["ts_code"], "debt_to_assets",  annual_kpi.iloc[0]["debt_to_assets"])
            update_sql_fkpi(annual_kpi.iloc[0]["ts_code"], "annual_operationcashflow_to_profit",  annual_kpi.iloc[0]["ocf_to_profit"])
            update_sql_fkpi(annual_kpi.iloc[0]["ts_code"], "rd_exp",  annual_kpi.iloc[0]["rd_exp"])
            update_sql_fkpi(annual_kpi.iloc[0]["ts_code"], "update_date",  tdtext)
        else:
            pass

def quarter_report_indicators(pro, f_kpi_all, tdtext):
    q1_report_period = tdtext[:4] + "0331"
    q2_report_period = tdtext[:4] + "0630"
    q3_report_period = tdtext[:4] + "0930"
    q4_report_period = str(int(tdtext[:4])-1) + "1"
    for stock_code in f_kpi_all:
        if int(tdtext[4:6]) >= 1 and int(tdtext[4:6]) <= 5:
            q4_kpi = pro.query('fina_indicator_vip', period=q4_report_period, ts_code = stock_code, fields='ts_code, q_gr_yoy, q_op_yoy, q_profit_yoy, eps, ocfps')
            if len(q4_kpi) != 0:
                q4_kpi = q4_kpi.fillna(0)
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "q4_grossincome_growth", q4_kpi.iloc[0]["q_gr_yoy"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "q4_operationprofit_growth", q4_kpi.iloc[0]["q_op_yoy"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "q4_profit_growth", q4_kpi.iloc[0]["q_profit_yoy"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "eps", q4_kpi.iloc[0]["eps"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "ocfps", q4_kpi.iloc[0]["ocfps"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "update_date", tdtext)                
            else:
                pass
            q1_kpi = pro.query('fina_indicator_vip', period=q1_report_period, ts_code = stock_code, fields='ts_code, q_gr_yoy, q_op_yoy, q_profit_yoy, eps, ocfps')
            if len(q1_kpi) != 0:
                q1_kpi = q1_kpi.fillna(0)
                update_sql_fkpi(q1_kpi.iloc[0]["ts_code"], "q1_grossincome_growth", q1_kpi.iloc[0]["q_gr_yoy"])
                update_sql_fkpi(q1_kpi.iloc[0]["ts_code"], "q1_operationprofit_growth", q1_kpi.iloc[0]["q_op_yoy"])
                update_sql_fkpi(q1_kpi.iloc[0]["ts_code"], "q1_profit_growth", q1_kpi.iloc[0]["q_profit_yoy"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "eps", q4_kpi.iloc[0]["eps"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "ocfps", q4_kpi.iloc[0]["ocfps"])                
                update_sql_fkpi(q1_kpi.iloc[0]["ts_code"], "update_date", tdtext)
            else:
                pass
        elif int(tdtext[4:6]) >=7 and int(tdtext[4:6]) <= 9:
            q2_kpi = pro.query('fina_indicator_vip', period=q2_report_period, ts_code = stock_code, fields='ts_code, q_gr_yoy, q_op_yoy, q_profit_yoy, eps, ocfps')
            if len(q2_kpi) != 0:
                q2_kpi = q2_kpi.fillna(0)
                update_sql_fkpi(q2_kpi.iloc[0]["ts_code"], "q2_grossincome_growth", q2_kpi.iloc[0]["q_gr_yoy"])
                update_sql_fkpi(q2_kpi.iloc[0]["ts_code"], "q2_operationprofit_growth", q2_kpi.iloc[0]["q_op_yoy"])
                update_sql_fkpi(q2_kpi.iloc[0]["ts_code"], "q2_profit_growth", q2_kpi.iloc[0]["q_profit_yoy"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "eps", q4_kpi.iloc[0]["eps"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "ocfps", q4_kpi.iloc[0]["ocfps"])                
                update_sql_fkpi(q2_kpi.iloc[0]["ts_code"], "update_date", tdtext)
            else:
                pass
        elif int(tdtext[4:6]) >=10 and int(tdtext[4:6]) <= 11:
            q3_kpi = pro.query('fina_indicator_vip', period=q3_report_period, ts_code = stock_code, fields='ts_code, q_gr_yoy, q_op_yoy, q_profit_yoy, eps, ocfps')
            if len(q3_kpi) != 0:
                q3_kpi = q3_kpi.fillna(0)
                update_sql_fkpi(q3_kpi.iloc[0]["ts_code"], "q3_grossincome_growth", q3_kpi.iloc[0]["q_gr_yoy"])
                update_sql_fkpi(q3_kpi.iloc[0]["ts_code"], "q3_operationprofit_growth", q3_kpi.iloc[0]["q_op_yoy"])
                update_sql_fkpi(q3_kpi.iloc[0]["ts_code"], "q3_profit_growth", q3_kpi.iloc[0]["q_profit_yoy"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "eps", q4_kpi.iloc[0]["eps"])
                update_sql_fkpi(q4_kpi.iloc[0]["ts_code"], "ocfps", q4_kpi.iloc[0]["ocfps"])                    
                update_sql_fkpi(q3_kpi.iloc[0]["ts_code"], "update_date", tdtext)
            else:
                pass
        else:
            pass


def update_sql_fkpi(stock_code, field, field_value):
    db = pymysql.connect(host='localhost', port=3306, db='stock_market',
                         user='', password='', charset="utf8")
    cur = db.cursor()
    if field == "update_date":
        sql = "UPDATE stock_financial_info SET %s =str_to_date('%s', '%%Y%%m%%d') WHERE stock_code = '%s'" % (
            field, field_value, stock_code)
    else:
        print(stock_code, field_value)
        sql = "UPDATE stock_financial_info SET %s = %s WHERE stock_code = '%s'" % (
            field, field_value, stock_code)
    cur.execute(sql)
    db.commit()
    cur.close()
    # db.close()


def main(argc, argv, envp):
    td = datetime.today()
    tdtext = td.strftime('%Y%m%d')
    pro = ts.pro_api('')
    f_kpi_all = query_sql_all()
    # annual_report_indicators(pro, f_kpi_all, tdtext)
    quarter_report_indicators(pro, f_kpi_all, tdtext)


if __name__ == "__main__":
    sys.exit(main(len(sys.argv), sys.argv, os.environ))
