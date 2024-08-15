import tushare as ts
import os
import sys
import datetime as dt
from datetime import datetime
import numpy as np
import pandas as pd
import pymysql


def get_current_month_start_and_end(tdtext):
    if tdtext.count('/') != 2:
        raise ValueError('/ is error')
    year, month = str(tdtext).split('/')[2], str(tdtext).split('/')[0]
    # end = calendar.monthrange(int(year), int(month))[1]
    start_date = '%s0101' % (year)
    return start_date

def weightedmean(df,cond,output):
    indicator = (df.loc[df[cond] == df[cond].max(), [output]].values[0][0] * 3.5 + df.loc[df[cond] == df[cond].max()[:2] + str(int(df[cond].max()[2:4])-1) + "1231", [output]].values[0][0]*1 + df.loc[df[cond] == df[cond].max()[:2] +
                                                                                                                                             str(int(df[cond].max()[2:4])-2) + "1231", [output]].values[0][0]*0.5)/5
    return indicator

def crosssheet(baseyear, sheet1, sheet2, sheet1f, sheet2f):
    indicator = sheet1.loc[sheet1['end_date'] == baseyear, [
        sheet1f]].values[0][0] / sheet2.loc[sheet2['end_date'] == baseyear, [sheet2f]].values[0][0]
    return indicator


def crosssheet2(baseyear, sheet1, sheet2, sheet1f, sheet2f1,sheet2f2):
    indicator = sheet1.loc[sheet1['end_date'] == baseyear, [
        sheet1f]].values[0][0] / (sheet2.loc[sheet2['end_date'] == baseyear, [sheet2f1]].values[0][0] + sheet2.loc[sheet2['end_date'] == baseyear, [sheet2f2]].values[0][0])
    return indicator

def profit_report(pro, stockcode,startDate, endDate):
    report_period = []
    profit = pro.income(ts_code=stockcode, start_date=startDate, end_date=endDate)
    profit = profit.loc[(profit['end_date'].str.slice(4,8)=="1231")]
    profit = profit.loc[(profit['ebit'].isna() == False)]
    profit = profit.reset_index()
    # profit.columns = ['index', 'end_date', 'revenue',
    #                   'oper_cost', 'income_tax', 'n_income', 'ebit']
    profit = profit.drop(columns = ['index','ts_code','ann_date','f_ann_date','report_type','comp_type','distable_profit'])
    profit['sell_exp'].fillna(0, inplace=True)
    profit['fv_value_chg_gain'].fillna(0, inplace=True)
    profit['forex_gain'].fillna(0,inplace=True)
    profit['oth_b_income'].fillna(0,inplace=True)
    profit['other_bus_cost'].fillna(0,inplace=True)
    profit['n_oth_b_income'].fillna(0,inplace=True)
    profit['oth_compr_income'].fillna(0,inplace=True)
    profit['non_oper_income'].fillna(0,inplace=True)
    profit['non_oper_exp'].fillna(0,inplace=True)
    profit['invest_income'].fillna(0,inplace=True)
    profit['minority_gain'].fillna(0,inplace=True)
    profit['assets_impair_loss'].fillna(0,inplace=True)
    profit['income_tax'].fillna(0,inplace=True)
    profit['total_profit'].fillna(0,inplace=True)
    # print(profit[{'end_date','revenue'}])
    profit['tax_rate'] = profit['income_tax']/profit['total_profit']
    profit['cost_rate'] = profit['oper_cost'].astype('float64')/profit['revenue'].astype('float64')
    profit['biz_tax_rate'] = profit['biz_tax_surchg'].astype(
        'float64')/profit['revenue']
    profit['sell_exp_rate'] = profit['sell_exp'].astype(
        'float64')/profit['revenue']
    profit['admin_exp_rate'] = profit['admin_exp'].astype(
        'float64')/profit['revenue']
    profit['minority_gain_rate'] = profit['minority_gain'] / profit['total_profit']
    profit['noplat'] = profit['ebit'] * (1-profit['tax_rate'])
    profit['growth'] =0.00
    profit.sort_values(by=["end_date"], inplace=True, ascending=[True])
    for key, value in profit.iterrows():
        if value['end_date'] == profit['end_date'].min():
            a = value['revenue']
        else:
            profit.loc[profit['end_date']==value['end_date'],['growth']] = value['revenue']/a -1
            a = value['revenue']
        if value['end_date'] == profit['end_date'].max():
            m = value['minority_gain_rate']
        else:
            pass
    for i in range(1,6):
        end_date = profit['end_date'].max()[:2] + \
            str(int(profit['end_date'].max()[2:4])+1) + "1231"       
        growth = weightedmean(profit, 'end_date', 'growth')
        cost_rate = weightedmean(profit, 'end_date', 'cost_rate')
        biz_tax_rate = weightedmean(profit, 'end_date', 'biz_tax_rate')
        sell_exp_rate = weightedmean(profit, 'end_date', 'sell_exp_rate')        
        admin_exp_rate = weightedmean(profit, 'end_date', 'admin_exp_rate')
        assets_impair_loss = weightedmean(profit, 'end_date', 'assets_impair_loss')                                                                                                                                                                
        fv_value_chg_gain = weightedmean(profit, 'end_date', 'fv_value_chg_gain')
        invest_income = weightedmean(profit, 'end_date', 'invest_income')
        minority_gain_rate = m
        oth_b_income = weightedmean(profit, 'end_date', 'oth_b_income')
        other_bus_cost = weightedmean(profit, 'end_date', 'other_bus_cost')
        oth_compr_income = weightedmean(profit, 'end_date', 'oth_compr_income')
        non_oper_income = weightedmean(profit, 'end_date', 'non_oper_income')
        non_oper_exp = weightedmean(profit, 'end_date', 'non_oper_exp')
        tax_rate = weightedmean(profit, 'end_date', 'tax_rate')
         
        revenue = profit.loc[profit['end_date'] == profit['end_date'].max(), [
            'revenue']].values[0][0] * (1+growth)
        oper_cost = revenue * cost_rate
        biz_tax_surchg = revenue * biz_tax_rate
        sell_exp = revenue * sell_exp_rate
        admin_exp = revenue * admin_exp_rate
        forex_gain = weightedmean(profit, 'end_date', 'forex_gain')
        print('growth', end_date, growth)
        report_period.append(end_date)

        profit = profit.append({'end_date': end_date, 'income_tax': 0, 'n_income': 0, 'ebit': 0, 'tax_rate': tax_rate, 'noplat': 0,
                                'growth': growth, 'oper_cost': oper_cost, 'revenue': revenue, 'cost_rate': cost_rate, 
                                'biz_tax_rate': biz_tax_rate, 'biz_tax_surchg': biz_tax_surchg, 'sell_exp': sell_exp, 
                                'sell_exp_rate': sell_exp_rate, 'admin_exp_rate': admin_exp_rate, 'admin_exp': admin_exp, 
                                'assets_impair_loss': assets_impair_loss, 'fv_value_chg_gain': fv_value_chg_gain, 
                                'invest_income': invest_income, 'forex_gain': forex_gain, 'oth_b_income': oth_b_income,
                                'other_bus_cost': other_bus_cost, 'oth_compr_income': oth_compr_income, 'non_oper_income': non_oper_income,
                                'non_oper_exp': non_oper_exp, 'minority_gain_rate': minority_gain_rate}, ignore_index=True)
    
    profit['gross_marin'] = 1-profit['cost_rate']
    # print(profit[{'cost_rate'}])
    return profit, report_period


def assumption(profit, div, bs, cf, tdtext, interest_rate):
    ap = pd.DataFrame()
    # ap['add_lt_borr'] =0
    # previous 5 year report indicators
    for i in range(1,6):
        baseyear = str(int(tdtext[:4])-i)+"1231"
        min_cf_rate = bs.loc[bs['end_date'] == baseyear, ['money_cap']].values[0][0] / \
            profit.loc[profit['end_date'] == baseyear,
                       ['revenue']].values[0][0]
        min_cf = bs.loc[bs['end_date'] == baseyear, ['money_cap']].values[0][0]
        if int(baseyear[:4]) >=2015:
            add_lt_borr = (bs.loc[bs['end_date'] == baseyear, ['lt_borr']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['lt_borr']].values[0][0])
            add_bond_payable = bs.loc[bs['end_date'] == baseyear, ['bond_payable']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['bond_payable']].values[0][0]
            add_st_borr = bs.loc[bs['end_date'] == baseyear, ['st_borr']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['st_borr']].values[0][0] 
            surplus_rese_rate = crosssheet(
                baseyear, bs, profit, 'add_surplus_rese', 'compr_inc_attr_p')          
            notes_receiv_rate = crosssheet(
                baseyear, bs, profit, 'notes_receiv', 'revenue')
            accounts_receiv_rate = crosssheet(
                baseyear, bs, profit, 'accounts_receiv', 'revenue')
            oth_receiv_rate = crosssheet(
                baseyear, bs, profit, 'oth_receiv', 'revenue')
            prepayment_rate = crosssheet(
                baseyear, bs, profit, 'prepayment', 'oper_cost')
            inventories_rate = crosssheet(
                baseyear, bs, profit, 'inventories', 'oper_cost')
            amor_exp_rate = crosssheet2(
                baseyear, bs, profit, 'amor_exp', 'sell_exp', 'admin_exp')
            notes_payable_rate = crosssheet(
                baseyear, bs, profit, 'notes_payable', 'oper_cost')
            acct_payable_rate = crosssheet(
                baseyear, bs, profit, 'acct_payable', 'oper_cost')
            adv_receipts_rate = crosssheet(
                baseyear, bs, profit, 'adv_receipts', 'revenue')
            acc_exp_rate = crosssheet2(
                baseyear, bs, profit, 'acc_exp', 'sell_exp', 'admin_exp')
            add_fixture = cf.loc[cf['end_date'] == baseyear, ['c_pay_acq_const_fiolta']].values[0][0]
            add_cip = bs.loc[bs['end_date'] == baseyear, [
                'cip']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['cip']].values[0][0]
            add_materials = bs.loc[bs['end_date'] == baseyear, [
                'const_materials']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['const_materials']].values[0][0]
            ciptofixture_rate = 0.5
            fixture_depreciation = 1/15
            amort_intang_assets = cf.loc[cf['end_date'] == baseyear,['amort_intang_assets']].values[0][0]
            add_intang_assets = bs.loc[bs['end_date'] == baseyear, [
                'intan_assets']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['intan_assets']].values[0][0]
            lt_amort_deferred_exp = cf.loc[cf['end_date'] == baseyear, [
                'lt_amort_deferred_exp']].values[0][0]
            add_lt_amor_exp = bs.loc[bs['end_date'] == baseyear, [
                'lt_amor_exp']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['lt_amor_exp']].values[0][0]
            lt_loan_rate = 0.0583
            st_loan_rate = 0.0547
            income_interest_rate = 0.0093
            conv_bond_rate = 0.002    
            add_total_share = bs.loc[bs['end_date'] == baseyear, [
                'total_share']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['total_share']].values[0][0]
            add_cap_rese = bs.loc[bs['end_date'] == baseyear, [
                'cap_rese']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['cap_rese']].values[0][0]
            # add_oper_receiv = bs.loc[bs['end_date'] == baseyear, [
            #     'total_oper_receiv']].values[0][0] - bs.loc[bs['end_date'] == str(int(baseyear[:4])-1)+"1231", ['total_oper_receiv']].values[0][0]
            notes_receiv = bs.loc[bs['end_date'] == baseyear, ['notes_receiv']].values[0][0]
            accounts_receiv = bs.loc[bs['end_date'] ==
                                     baseyear, ['accounts_receiv']].values[0][0]
            oth_receiv = bs.loc[bs['end_date'] ==
                                baseyear, ['oth_receiv']].values[0][0]
            prepayment = bs.loc[bs['end_date'] ==
                                baseyear, ['prepayment']].values[0][0]
            inventories = bs.loc[bs['end_date'] ==
                                 baseyear, ['inventories']].values[0][0]
            amor_exp = bs.loc[bs['end_date'] ==
                              baseyear, ['amor_exp']].values[0][0]
            oth_cur_assets = bs.loc[bs['end_date'] ==
                                    baseyear, ['oth_cur_assets']].values[0][0]
            lt_rec = bs.loc[bs['end_date'] ==
                            baseyear, ['lt_rec']].values[0][0]
            lt_amor_exp = bs.loc[bs['end_date'] ==
                                 baseyear, ['lt_amor_exp']].values[0][0]
            notes_payable = bs.loc[bs['end_date'] ==
                                   baseyear, ['notes_payable']].values[0][0]
            acct_payable = bs.loc[bs['end_date'] ==
                                  baseyear, ['acct_payable']].values[0][0]
            adv_receipts = bs.loc[bs['end_date'] ==
                                  baseyear, ['adv_receipts']].values[0][0]
            payroll_payable = bs.loc[bs['end_date'] ==
                                     baseyear, ['payroll_payable']].values[0][0]
            lt_payroll_payable = bs.loc[bs['end_date'] ==
                                        baseyear, ['lt_payroll_payable']].values[0][0]
            taxes_payable = bs.loc[bs['end_date'] ==
                                   baseyear, ['taxes_payable']].values[0][0]
            oth_payable = bs.loc[bs['end_date'] ==
                                 baseyear, ['oth_payable']].values[0][0]
            acc_exp = bs.loc[bs['end_date'] ==
                             baseyear, ['acc_exp']].values[0][0]
            deferred_inc = bs.loc[bs['end_date'] ==
                                  baseyear, ['deferred_inc']].values[0][0]
            oth_cur_liab = bs.loc[bs['end_date'] ==
                                  baseyear, ['oth_cur_liab']].values[0][0]
            lt_payable = bs.loc[bs['end_date'] ==
                                baseyear, ['lt_payable']].values[0][0]
            specific_payables = bs.loc[bs['end_date'] ==
                                       baseyear, ['specific_payables']].values[0][0]
            estimated_liab = bs.loc[bs['end_date'] ==
                                    baseyear, ['estimated_liab']].values[0][0]
            end_intan_assets = bs.loc[bs['end_date'] ==
                                      baseyear, ['intan_assets']].values[0][0]
            goodwill = bs.loc[bs['end_date'] ==
                              baseyear, ['goodwill']].values[0][0]
            produc_bio_assets = bs.loc[bs['end_date'] ==
                                       baseyear, ['produc_bio_assets']].values[0][0]
            oil_and_gas_assets = bs.loc[bs['end_date'] ==
                                        baseyear, ['oil_and_gas_assets']].values[0][0]
            fixed_assets_disp = bs.loc[bs['end_date'] ==
                                       baseyear, ['fixed_assets_disp']].values[0][0]
            fa_avail_for_sale = bs.loc[bs['end_date'] ==
                                       baseyear, ['fa_avail_for_sale']].values[0][0]
            htm_invest = bs.loc[bs['end_date'] ==
                                baseyear, ['htm_invest']].values[0][0]
            invest_real_estate = bs.loc[bs['end_date'] ==
                                        baseyear, ['invest_real_estate']].values[0][0]
            lt_eqt_invest = bs.loc[bs['end_date'] ==
                                   baseyear, ['lt_eqt_invest']].values[0][0]
            oth_nca = bs.loc[bs['end_date'] ==
                             baseyear, ['oth_nca']].values[0][0]
            oth_ncl = bs.loc[bs['end_date'] ==
                             baseyear, ['oth_ncl']].values[0][0]
            defer_tax_assets = bs.loc[bs['end_date'] ==
                                      baseyear, ['defer_tax_assets']].values[0][0]
            defer_tax_liab = bs.loc[bs['end_date'] ==
                                    baseyear, ['defer_tax_liab']].values[0][0]
            forex_differ = bs.loc[bs['end_date'] ==
                                  baseyear, ['forex_differ']].values[0][0]
            trad_asset = bs.loc[bs['end_date'] ==
                                baseyear, ['trad_asset']].values[0][0]
            div_receiv = bs.loc[bs['end_date'] ==
                                baseyear, ['div_receiv']].values[0][0]
            int_receiv = bs.loc[bs['end_date'] ==
                                baseyear, ['int_receiv']].values[0][0]
            nca_within_1y = bs.loc[bs['end_date'] ==
                                   baseyear, ['nca_within_1y']].values[0][0]
            trading_fl = bs.loc[bs['end_date'] ==
                                baseyear, ['trading_fl']].values[0][0]
            st_bonds_payable = bs.loc[bs['end_date'] ==
                                      baseyear, ['st_bonds_payable']].values[0][0]
            div_payable = bs.loc[bs['end_date'] ==
                                 baseyear, ['div_payable']].values[0][0]
            int_payable = bs.loc[bs['end_date'] ==
                                 baseyear, ['int_payable']].values[0][0]
            non_cur_liab_due_1y = bs.loc[bs['end_date'] ==
                                         baseyear, ['non_cur_liab_due_1y']].values[0][0]
            bond_payable = bs.loc[bs['end_date'] ==
                                  baseyear, ['bond_payable']].values[0][0]
            invest_loss_unconf = bs.loc[bs['end_date'] ==
                                        baseyear, ['invest_loss_unconf']].values[0][0]
            eps =  profit.loc[profit['end_date'] == baseyear, ['compr_inc_attr_p']].values[0][0] / bs.loc[bs['end_date'] == baseyear, ['total_share']].values[0][0]                          
            try :
                cash_div_tax_rate = div.loc[div['end_date'] == baseyear, ['cash_div_tax']].values[0][0] / eps
            except:
                cash_div_tax_rate = 0
            prov_depr_assets = cf.loc[cf['end_date'] ==
                                      baseyear, ['prov_depr_assets']].values[0][0]
            total_liab = bs.loc[bs['end_date'] ==
                                baseyear, ['total_liab']].values[0][0]
            total_hldr_eqy_inc_min_int = bs.loc[bs['end_date'] ==
                                                baseyear, ['total_hldr_eqy_inc_min_int']].values[0][0]
            money_cap = bs.loc[bs['end_date'] ==
                               baseyear, ['money_cap']].values[0][0]
            st_borr = bs.loc[bs['end_date'] ==
                             baseyear, ['st_borr']].values[0][0]
            trading_fl = bs.loc[bs['end_date'] ==
                                baseyear, ['trading_fl']].values[0][0]
            lt_borr = bs.loc[bs['end_date'] ==
                             baseyear, ['lt_borr']].values[0][0]
            minority_int = bs.loc[bs['end_date'] ==
                                  baseyear, ['minority_int']].values[0][0]
            total_share = bs.loc[bs['end_date'] ==
                                 baseyear, ['total_share']].values[0][0]
                                
        else:
            pass
        ap = ap.append({'end_date': baseyear, 'min_cf_rate': min_cf_rate, 'min_cf': min_cf, 'add_lt_borr': add_lt_borr, 'add_bond_payable': add_bond_payable, 'add_st_borr': add_st_borr, 'surplus_rese_rate': surplus_rese_rate, "notes_receiv_rate": notes_receiv_rate, 'accounts_receiv_rate': accounts_receiv_rate, 'oth_receiv_rate': oth_receiv_rate, 'prepayment_rate': prepayment_rate, 'inventories_rate': inventories_rate, 'amor_exp_rate': amor_exp_rate, 'notes_payable_rate': notes_payable_rate, 'acct_payable_rate': acct_payable_rate, 'adv_receipts_rate': adv_receipts_rate, 'acc_exp_rate': acc_exp_rate, 'add_fixture': add_fixture, 'add_cip': add_cip, 'ciptofixture_rate': ciptofixture_rate, 'add_materials': add_materials, 'fixture_depreciation': fixture_depreciation, 'amort_intang_assets': amort_intang_assets, 'add_intang_assets': add_intang_assets, 'lt_amort_deferred_exp': lt_amort_deferred_exp, 'add_lt_amor_exp': add_lt_amor_exp, 'lt_loan_rate': lt_loan_rate, 'st_loan_rate': st_loan_rate, 'income_interest_rate': income_interest_rate, 'conv_bond_rate': conv_bond_rate, 'add_total_share': add_total_share, 'add_cap_rese': add_cap_rese, 'notes_receiv': notes_receiv, 'accounts_receiv': accounts_receiv, 'oth_receiv': oth_receiv, 'prepayment': prepayment, 'inventories': inventories, 'amor_exp': amor_exp, 'oth_cur_assets': oth_cur_assets, 'lt_rec': lt_rec, 'lt_amor_exp': lt_amor_exp, 'notes_payable': notes_payable, 'acct_payable': acct_payable, 'adv_receipts': adv_receipts, 'payroll_payable': payroll_payable, 'lt_payroll_payable': lt_payroll_payable, 'taxes_payable': taxes_payable, 'oth_payable': oth_payable, 'acc_exp': acc_exp, 'deferred_inc': deferred_inc, 'oth_cur_liab': oth_cur_liab, 'lt_payable': lt_payable, 'specific_payables': specific_payables, 'estimated_liab': estimated_liab, 'end_intan_assets': end_intan_assets, 'goodwill': goodwill, 'produc_bio_assets': produc_bio_assets, 'oil_and_gas_assets': oil_and_gas_assets, 'fixed_assets_disp': fixed_assets_disp, 'fa_avail_for_sale': fa_avail_for_sale, 'htm_invest': htm_invest, 'invest_real_estate': invest_real_estate, 'lt_eqt_invest': lt_eqt_invest, 'oth_nca': oth_nca, 'oth_ncl': oth_ncl, 'defer_tax_assets': defer_tax_assets, 'defer_tax_liab': defer_tax_liab, 'forex_differ': forex_differ, 'trad_asset': trad_asset, 'div_receiv': div_receiv, 'int_receiv': int_receiv, 'nca_within_1y': nca_within_1y, 'trading_fl': trading_fl, 'st_bonds_payable': st_bonds_payable, 'div_payable': div_payable, 'int_payable': int_payable, 'non_cur_liab_due_1y': non_cur_liab_due_1y, 'bond_payable': bond_payable, 'invest_loss_unconf': invest_loss_unconf, 'cash_div_tax_rate': cash_div_tax_rate, 'prov_depr_assets': prov_depr_assets, 'total_liab': total_liab, 'total_hldr_eqy_inc_min_int': total_hldr_eqy_inc_min_int, 'money_cap': money_cap, 'st_borr': st_borr, 'lt_borr': lt_borr, 'minority_int': minority_int, 'total_share': total_share},
                       ignore_index=True)
    # projected 5 year report indicators  
    ciptofixture_rate = 0.5
    fixture_depreciation = 1/15
    lt_loan_rate = 0.0583
    st_loan_rate = 0.0547
    income_interest_rate = 0.0093
    conv_bond_rate = 0.002
    end_total_share = bs.loc[bs['end_date'] == ap['end_date'].max(), ['total_share']].values[0][0]
    end_cap_rese = bs.loc[bs['end_date'] == ap['end_date'].max(), ['cap_rese']].values[0][0]
    end_fix_assets = bs.loc[bs['end_date'] == ap['end_date'].max(), ['fix_assets']].values[0][0]
    end_cip = bs.loc[bs['end_date'] == ap['end_date'].max(), ['cip']].values[0][0]
    accu_depreciation_fixture = []
    accu_depreciation_fixture.append(end_fix_assets)
    end_const_materials = bs.loc[bs['end_date'] == ap['end_date'].max(), ['const_materials']].values[0][0]
    end_intan_assets = bs.loc[bs['end_date'] == ap['end_date'].max(), ['intan_assets']].values[0][0]
    end_lt_amor_exp = bs.loc[bs['end_date'] == ap['end_date'].max(), ['lt_amor_exp']].values[0][0]
    end_money_cap = bs.loc[bs['end_date'] == ap['end_date'].max(), ['money_cap']].values[0][0]
    end_st_borr = bs.loc[bs['end_date'] == ap['end_date'].max(), ['st_borr']].values[0][0]
    end_lt_borr = bs.loc[bs['end_date'] == ap['end_date'].max(), ['lt_borr']].values[0][0]
    end_bond_payable = bs.loc[bs['end_date'] == ap['end_date'].max(), ['bond_payable']].values[0][0]

    for i in range(1,6):
        end_date = ap['end_date'].max()[:2] + \
            str(int(ap['end_date'].max()[2:4])+1) + "1231"
        # net_profit = profit.loc[profit['end_date']==end_date,['']]

        # end_cf = bs.loc[bs['end_date'] == end_date[:2]+str(int(end_date[2:4])-1),['money_cap']].values[0][0]
        min_cf_rate = weightedmean(ap, 'end_date', 'min_cf_rate')
        min_cf = profit.loc[profit['end_date']==end_date,['revenue']].values[0][0] * min_cf_rate
        add_lt_borr = (bs.loc[bs['end_date'] == end_date, ['lt_borr']].values[0][0] - bs.loc[bs['end_date'] == str(int(end_date[:4])-1)+"1231", ['lt_borr']].values[0][0])
        add_bond_payable = bs.loc[bs['end_date'] == end_date, ['bond_payable']].values[0][0] - bs.loc[bs['end_date'] == str(int(end_date[:4])-1)+"1231", ['bond_payable']].values[0][0]
        add_st_borr = bs.loc[bs['end_date'] == end_date, ['st_borr']].values[0][0] - bs.loc[bs['end_date'] == str(int(end_date[:4])-1)+"1231", ['st_borr']].values[0][0] 
        surplus_rese_rate = weightedmean(ap, 'end_date', 'surplus_rese_rate')
        notes_receiv_rate = weightedmean(ap, 'end_date', 'notes_receiv_rate')
        accounts_receiv_rate = weightedmean(
            ap, 'end_date', 'accounts_receiv_rate')
        oth_receiv_rate = weightedmean(ap, 'end_date', 'oth_receiv_rate')
        prepayment_rate = weightedmean(ap, 'end_date', 'prepayment_rate')
        inventories_rate = weightedmean(ap, 'end_date', 'inventories_rate')
        amor_exp_rate = weightedmean(ap, 'end_date', 'amor_exp_rate')
        notes_payable_rate = weightedmean(ap, 'end_date', 'notes_payable_rate')
        acct_payable_rate = weightedmean(ap, 'end_date', 'acct_payable_rate')
        adv_receipts_rate = weightedmean(ap, 'end_date', 'adv_receipts_rate')
        acc_exp_rate = weightedmean(ap, 'end_date', 'acc_exp_rate')
        add_fixture = weightedmean(ap, 'end_date', 'add_fixture')
        add_cip = weightedmean(ap, 'end_date', 'add_cip')
        add_materials = weightedmean(ap, 'end_date', 'add_materials')
        ciptofixture_rate = 0.5
        fixture_depreciation = 1/15
        amort_intang_assets = weightedmean(
            ap, 'end_date', 'amort_intang_assets')
        add_intang_assets = weightedmean(ap, 'end_date', 'add_intang_assets')
        lt_amort_deferred_exp = weightedmean(
            ap, 'end_date', 'lt_amort_deferred_exp')
        add_lt_amor_exp = weightedmean(ap, 'end_date', 'add_lt_amor_exp')
        lt_loan_rate = 0.0583
        st_loan_rate = 0.0547
        income_interest_rate = 0.0093
        conv_bond_rate = 0.02  
        add_total_share = weightedmean(ap, 'end_date', 'add_total_share')
        open_total_share = end_total_share + add_total_share
        add_cap_rese = weightedmean(ap, 'end_date', 'add_cap_rese')
        open_cap_rese = end_cap_rese + add_cap_rese
        # add_oper_receiv = bs.loc[bs['end_date'] == end_date, [
        #     'total_oper_receiv']].values[0][0] - bs.loc[bs['end_date'] == str(int(end_date[:4])-1)+"1231", ['total_oper_receiv']].values[0][0]
        notes_receiv = profit.loc[profit['end_date']
                                  == end_date, ['revenue']].values[0][0] * notes_receiv_rate
        accounts_receiv = profit.loc[profit['end_date']
                                     == end_date, ['revenue']].values[0][0] * accounts_receiv_rate
        oth_receiv = profit.loc[profit['end_date']
                                == end_date, ['revenue']].values[0][0] * oth_receiv_rate
        prepayment = profit.loc[profit['end_date']
                                == end_date, ['oper_cost']].values[0][0] * prepayment_rate
        inventories = profit.loc[profit['end_date']
                                 == end_date, ['oper_cost']].values[0][0] * inventories_rate
        amor_exp = (profit.loc[profit['end_date'] == end_date, ['sell_exp']].values[0][0] +
                    profit.loc[profit['end_date'] == end_date, ['oper_cost']].values[0][0]) * amor_exp_rate
        oth_cur_assets = bs.loc[bs['end_date'] ==
                                end_date, ['oth_cur_assets']].values[0][0]
        lt_rec = bs.loc[bs['end_date'] ==
                        end_date, ['lt_rec']].values[0][0]
        lt_amor_exp = bs.loc[bs['end_date'] ==
                             end_date, ['lt_amor_exp']].values[0][0]
        notes_payable = profit.loc[profit['end_date']
                                   == end_date, ['oper_cost']].values[0][0] * notes_payable_rate
        acct_payable = profit.loc[profit['end_date']
                                  == end_date, ['oper_cost']].values[0][0] * acct_payable_rate
        adv_receipts = profit.loc[profit['end_date']
                                  == end_date, ['revenue']].values[0][0] * adv_receipts_rate
        payroll_payable = bs.loc[bs['end_date'] ==
                                 end_date, ['payroll_payable']].values[0][0]
        lt_payroll_payable = bs.loc[bs['end_date'] ==
                                    end_date, ['lt_payroll_payable']].values[0][0]
        taxes_payable = bs.loc[bs['end_date'] ==
                               end_date, ['taxes_payable']].values[0][0]
        oth_payable = bs.loc[bs['end_date'] ==
                             end_date, ['oth_payable']].values[0][0]
        acc_exp = (profit.loc[profit['end_date'] == end_date, ['sell_exp']].values[0][0] +
                   profit.loc[profit['end_date'] == end_date, ['oper_cost']].values[0][0]) * acc_exp_rate
        deferred_inc = bs.loc[bs['end_date'] ==
                              end_date, ['deferred_inc']].values[0][0]
        oth_cur_liab = bs.loc[bs['end_date'] ==
                              end_date, ['oth_cur_liab']].values[0][0]
        lt_payable = bs.loc[bs['end_date'] ==
                            end_date, ['lt_payable']].values[0][0]
        specific_payables = bs.loc[bs['end_date'] ==
                                   end_date, ['specific_payables']].values[0][0]
        estimated_liab = bs.loc[bs['end_date'] ==
                                end_date, ['estimated_liab']].values[0][0]
        open_cip = end_cip + add_cip
        cip_to_fixture = open_cip * ciptofixture_rate
        end_cip = open_cip - cip_to_fixture
        add_accu_depreciation = (add_fixture + cip_to_fixture) * fixture_depreciation /2
        end_accu_depreciation = sum(accu_depreciation_fixture) * fixture_depreciation + add_accu_depreciation
        # print(accu_depreciation_fixture,
        #       fixture_depreciation, add_accu_depreciation)
        accu_depreciation_fixture.append(add_fixture + cip_to_fixture)
        end_fix_assets = end_fix_assets + add_fixture + cip_to_fixture - end_accu_depreciation
        end_const_materials = end_const_materials + add_materials
        intan_assets_depreciation = end_intan_assets /10
        end_intan_assets = end_intan_assets + add_intang_assets - intan_assets_depreciation
        lt_amor_exp_charge = end_lt_amor_exp / 5
        end_lt_amor_exp = end_lt_amor_exp - lt_amor_exp_charge
        total_depreciation_amorization = end_accu_depreciation + intan_assets_depreciation + lt_amor_exp_charge
        # print(end_accu_depreciation, intan_assets_depreciation, lt_amor_exp_charge)
        goodwill = bs.loc[bs['end_date'] ==
                          end_date, ['goodwill']].values[0][0]
        produc_bio_assets = bs.loc[bs['end_date'] ==
                                   end_date, ['produc_bio_assets']].values[0][0]
        oil_and_gas_assets = bs.loc[bs['end_date'] ==
                                    end_date, ['oil_and_gas_assets']].values[0][0]
        fixed_assets_disp = bs.loc[bs['end_date'] ==
                                   end_date, ['fixed_assets_disp']].values[0][0]
        fa_avail_for_sale = bs.loc[bs['end_date'] ==
                                   end_date, ['fa_avail_for_sale']].values[0][0]
        htm_invest = bs.loc[bs['end_date'] ==
                            end_date, ['htm_invest']].values[0][0]
        invest_real_estate = bs.loc[bs['end_date'] ==
                                    end_date, ['invest_real_estate']].values[0][0]
        lt_eqt_invest = bs.loc[bs['end_date'] ==
                               end_date, ['lt_eqt_invest']].values[0][0]
        st_borr = bs.loc[bs['end_date'] ==
                         end_date, ['st_borr']].values[0][0]
        media_interest_income = (end_money_cap + min_cf)/2 * income_interest_rate
        end_money_cap = bs.loc[bs['end_date'] == end_date, ['money_cap']].values[0][0]
        end_st_borr = end_st_borr + st_borr
        end_st_borr_interest = end_st_borr/2 * st_loan_rate
        media_lt_borr = end_lt_borr + add_lt_borr
        end_lt_borr_interest = (end_lt_borr + add_lt_borr/2) * lt_loan_rate
        end_lt_borr = media_lt_borr
        media_bond_payable = end_bond_payable + add_bond_payable
        end_bond_payable_interest = (end_bond_payable + add_bond_payable/2) * conv_bond_rate
        end_bond_payable = media_bond_payable
        total_borr_interest = end_st_borr_interest + end_lt_borr_interest +end_bond_payable_interest
        revenue = profit.loc[profit['end_date'] == end_date, ['revenue']].values[0][0]
        oper_cost = profit.loc[profit['end_date'] == end_date, ['oper_cost']].values[0][0]
        biz_tax_surchg = profit.loc[profit['end_date'] == end_date, ['biz_tax_surchg']].values[0][0]
        sell_exp = profit.loc[profit['end_date'] == end_date, ['sell_exp']].values[0][0]
        admin_exp = profit.loc[profit['end_date'] == end_date, ['admin_exp']].values[0][0]
        forex_gain = profit.loc[profit['end_date'] == end_date, ['forex_gain']].values[0][0]
        assets_impair_loss = profit.loc[profit['end_date'] == end_date, ['assets_impair_loss']].values[0][0]
        fv_value_chg_gain = profit.loc[profit['end_date'] == end_date, ['fv_value_chg_gain']].values[0][0]
        invest_income = profit.loc[profit['end_date'] == end_date, ['invest_income']].values[0][0]
        oth_b_income = profit.loc[profit['end_date'] == end_date, ['oth_b_income']].values[0][0]
        other_bus_cost = profit.loc[profit['end_date'] == end_date, ['other_bus_cost']].values[0][0]
        oth_compr_income = profit.loc[profit['end_date'] == end_date, ['oth_compr_income']].values[0][0]
        non_oper_income = profit.loc[profit['end_date'] == end_date, ['non_oper_income']].values[0][0]
        non_oper_exp = profit.loc[profit['end_date'] == end_date, ['non_oper_exp']].values[0][0]
        minority_gain_rate = profit.loc[profit['end_date'] == end_date, ['minority_gain_rate']].values[0][0]
        tax_rate = profit.loc[profit['end_date'] == end_date, ['tax_rate']].values[0][0]
        oth_nca = bs.loc[bs['end_date'] == end_date, ['oth_nca']].values[0][0]
        oth_ncl = bs.loc[bs['end_date'] == end_date, ['oth_ncl']].values[0][0]
        defer_tax_assets = bs.loc[bs['end_date'] ==
                                  end_date, ['defer_tax_assets']].values[0][0]
        defer_tax_liab = bs.loc[bs['end_date'] ==
                                end_date, ['defer_tax_liab']].values[0][0]
        forex_differ = bs.loc[bs['end_date'] ==
                              end_date, ['forex_differ']].values[0][0]
        trad_asset = bs.loc[bs['end_date'] ==
                            end_date, ['trad_asset']].values[0][0]
        div_receiv = bs.loc[bs['end_date'] ==
                            end_date, ['div_receiv']].values[0][0]
        int_receiv = bs.loc[bs['end_date'] ==
                            end_date, ['int_receiv']].values[0][0]
        nca_within_1y = bs.loc[bs['end_date'] ==
                               end_date, ['nca_within_1y']].values[0][0]
        trading_fl = bs.loc[bs['end_date'] ==
                            end_date, ['trading_fl']].values[0][0]
        st_bonds_payable = bs.loc[bs['end_date'] ==
                                  end_date, ['st_bonds_payable']].values[0][0]
        div_payable = bs.loc[bs['end_date'] ==
                             end_date, ['div_payable']].values[0][0]
        int_payable = bs.loc[bs['end_date'] ==
                             end_date, ['int_payable']].values[0][0]
        non_cur_liab_due_1y = bs.loc[bs['end_date'] ==
                                     end_date, ['non_cur_liab_due_1y']].values[0][0]
        bond_payable = bs.loc[bs['end_date'] ==
                              end_date, ['bond_payable']].values[0][0]
        invest_loss_unconf = bs.loc[bs['end_date'] ==
                                    end_date, ['invest_loss_unconf']].values[0][0]
        cash_div_tax_rate = weightedmean(ap, 'end_date', 'cash_div_tax_rate')
        prov_depr_assets = cf.loc[cf['end_date'] ==
                                  end_date, ['prov_depr_assets']].values[0][0]
        total_liab = bs.loc[bs['end_date'] ==
                            end_date, ['total_liab']].values[0][0]
         
        gross_margin = revenue - oper_cost - biz_tax_surchg
        operate_profit = gross_margin - sell_exp - admin_exp + \
            media_interest_income + forex_gain - total_borr_interest - \
            assets_impair_loss + fv_value_chg_gain + invest_income
        total_profit = operate_profit + non_oper_income - non_oper_exp
        income_tax = total_profit * tax_rate
        n_income = total_profit - income_tax
        minority_gain = n_income * minority_gain_rate
        n_income_attr_p = n_income - minority_gain
        compr_inc_attr_p = n_income_attr_p + oth_compr_income
        # print(tax_rate, media_interest_income, forex_gain, total_borr_interest,
        #       assets_impair_loss, fv_value_chg_gain, invest_income)

        ap = ap.append({'end_date': end_date, 'min_cf_rate': min_cf_rate, 'min_cf': min_cf, 'add_lt_borr': add_lt_borr, 'add_st_borr': add_st_borr, 'surplus_rese_rate': surplus_rese_rate, 'notes_receiv_rate': notes_receiv_rate, 'accounts_receiv_rate': accounts_receiv_rate, 'oth_receiv_rate': oth_receiv_rate, 'prepayment_rate': prepayment_rate, 'inventories_rate': inventories_rate, 'amor_exp_rate': amor_exp_rate, 'notes_payable_rate': notes_payable_rate, 'acct_payable_rate': acct_payable_rate, 'adv_receipts_rate': adv_receipts_rate, 'acc_exp_rate': acc_exp_rate, 'add_fixture': add_fixture, 'add_cip': add_cip, 'ciptofixture_rate': ciptofixture_rate, 'add_materials': add_materials, 'fixture_depreciation': fixture_depreciation, 'amort_intang_assets': amort_intang_assets, 'add_intang_assets': add_intang_assets, 'lt_amort_deferred_exp': lt_amort_deferred_exp, 'add_lt_amor_exp': add_lt_amor_exp, 'lt_loan_rate': lt_loan_rate, 'st_loan_rate': st_loan_rate, 'income_interest_rate': income_interest_rate, 'conv_bond_rate': conv_bond_rate, 'add_total_share': add_total_share, 'open_total_share': open_total_share, 'add_cap_rese': add_cap_rese, 'open_cap_rese': open_cap_rese, 'notes_receiv': notes_receiv, 'accounts_receiv': accounts_receiv, 'oth_receiv': oth_receiv, 'prepayment': prepayment, 'inventories': inventories, 'amor_exp': amor_exp, 'oth_cur_assets': oth_cur_assets, 'lt_rec': lt_rec, 'lt_amor_exp': lt_amor_exp, 'notes_payable': notes_payable, 'acct_payable': acct_payable, 'adv_receipts': adv_receipts, 'payroll_payable': payroll_payable, 'lt_payroll_payable': lt_payroll_payable, 'taxes_payable': taxes_payable, 'oth_payable': oth_payable, 'acc_exp': acc_exp, 'deferred_inc': deferred_inc, 'oth_cur_liab': oth_cur_liab, 'lt_payable': lt_payable, 'specific_payables': specific_payables, 'estimated_liab': estimated_liab, 'open_cip': open_cip, 'cip_to_fixture': cip_to_fixture, 'end_cip': end_cip, 'add_accu_depreciation': add_accu_depreciation, 'end_accu_depreciation': end_accu_depreciation, 'end_fix_assets': end_fix_assets, 'end_const_materials': end_const_materials, 'end_intan_assets': end_intan_assets, 'intan_assets_depreciation': intan_assets_depreciation, 'end_lt_amor_exp': end_lt_amor_exp, 'lt_amor_exp_charge': lt_amor_exp_charge, 'total_depreciation_amorization': total_depreciation_amorization, 'goodwill': goodwill, 'produc_bio_assets': produc_bio_assets, 'oil_and_gas_assets': oil_and_gas_assets, 'fixed_assets_disp': fixed_assets_disp, 'fa_avail_for_sale': fa_avail_for_sale, 'htm_invest': htm_invest, 'invest_real_estate': invest_real_estate, 'lt_eqt_invest': lt_eqt_invest, 'add_bond_payable': add_bond_payable, 'end_st_borr': end_st_borr, 'end_st_borr_interest': end_st_borr_interest, 'end_lt_borr': end_lt_borr, 'end_lt_borr_interest': end_lt_borr_interest, 'end_bond_payable': end_bond_payable, 'end_bond_payable_interest': end_bond_payable_interest, 'total_borr_interest': total_borr_interest, 'revenue': revenue, 'oper_cost': oper_cost, 'biz_tax_surchg': biz_tax_surchg, 'sell_exp': sell_exp, 'admin_exp': admin_exp, 'forex_gain': forex_gain, 'assets_impair_loss': assets_impair_loss, 'fv_value_chg_gain': fv_value_chg_gain, 'invest_income': invest_income, 'oth_b_income': oth_b_income, 'other_bus_cost': other_bus_cost, 'oth_compr_income': oth_compr_income, 'minority_gain_rate': minority_gain_rate, 'tax_rate': tax_rate, 'gross_margin': gross_margin, 'operate_profit': operate_profit, 'total_profit': total_profit, 'income_tax': income_tax, 'n_income': n_income, 'minority_gain': minority_gain, 'n_income_attr_p': n_income_attr_p, 'compr_inc_attr_p': compr_inc_attr_p, 'oth_nca': oth_nca, 'oth_ncl': oth_ncl, 'defer_tax_assets': defer_tax_assets, 'defer_tax_liab': defer_tax_liab, 'forex_differ': forex_differ, "trad_asset": trad_asset, 'div_receiv': div_receiv, 'int_receiv': int_receiv, 'nca_within_1y': nca_within_1y, 'trading_fl': trading_fl, 'st_bonds_payable': st_bonds_payable, 'div_payable': div_payable, 'int_payable': int_payable, 'non_cur_liab_due_1y': non_cur_liab_due_1y, 'media_interest_income': media_interest_income, 'bond_payable': bond_payable, 'invest_loss_unconf': invest_loss_unconf, 'end_total_share': end_total_share, 'cash_div_tax_rate': cash_div_tax_rate, 'st_borr': st_borr, 'non_oper_income': non_oper_income, 'non_oper_exp': non_oper_exp, 'prov_depr_assets': prov_depr_assets, 'total_liab': total_liab},
                       ignore_index=True)
        end_total_share = open_total_share
        end_cap_rese = open_cap_rese

    ap['total_oper_receiv'] = ap['notes_receiv'] + ap['accounts_receiv'] + ap['oth_receiv'] + ap['prepayment'] + ap['inventories'] + ap['amor_exp'] + ap['oth_cur_assets'] + ap['lt_rec'] + ap['lt_amor_exp']
    ap['total_oper_payable'] = ap['notes_payable'] + ap['acct_payable'] + ap['adv_receipts'] + ap['payroll_payable'] + ap['lt_payroll_payable'] + ap['taxes_payable'] + ap['oth_payable'] + ap['acc_exp'] + ap['deferred_inc'] + ap['oth_cur_liab'] + ap['lt_payable'] + ap['specific_payables'] + ap['estimated_liab']
    ap['total_oper_gap_surplus'] = ap['total_oper_receiv'] - ap['total_oper_payable']
    ap['total_intan_oth_assets'] = ap['end_intan_assets'] + ap['goodwill']
    ap['total_lt_investment'] = ap['fa_avail_for_sale'] + ap['htm_invest'] + ap['invest_real_estate'] + ap['lt_eqt_invest']
    ap.sort_values(by=["end_date"], inplace=True, ascending=[True])
    ap['add_oper_receiv'] = 0.00
    ap['add_oper_payable'] = 0.00
    ap['add_oper_gap_surplus'] = 0.00
    ap['add_total_intan_oth_assets'] = 0.00
    ap['add_produc_bio_assets'] = 0.00
    ap['add_oil_and_gas_assets'] = 0.00
    ap['add_fixed_assets_disp'] = 0.00
    ap['add_lt_investment'] = 0.00
    ap['add_oth_nca'] = 0.00
    ap['add_oth_ncl'] = 0.00
    ap['add_defer_tax_assets'] = 0.00
    ap['add_defer_tax_liab'] = 0.00
    ap['add_forex_differ'] = 0.00
    ap['add_trad_asset'] = 0.00
    ap['add_div_receiv'] = 0.00
    ap['add_int_receiv'] = 0.00
    ap['add_nca_within_1y'] = 0.00
    ap['add_trading_fl'] = 0.00
    ap['add_st_bonds_payable'] = 0.00
    ap['add_div_payable'] = 0.00
    ap['add_int_payable'] = 0.00
    ap['add_non_cur_liab_due_1y'] = 0.00
    for i in range(9):
        baseyear = str(int(ap['end_date'].max()[:4])-i) + "1231"
        preyear = str(int(baseyear[:4])-1)+"1231"
        add_oper_receiv = ap.loc[ap['end_date'] == baseyear, [
            'total_oper_receiv']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['total_oper_receiv']].values[0][0]
        add_oper_payable = ap.loc[ap['end_date'] == baseyear, [
            'total_oper_payable']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['total_oper_payable']].values[0][0]
        add_oper_gap_surplus = ap.loc[ap['end_date'] == baseyear, [
            'total_oper_gap_surplus']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['total_oper_gap_surplus']].values[0][0]
        add_total_intan_oth_assets = ap.loc[ap['end_date'] == baseyear, [
            'total_intan_oth_assets']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['total_intan_oth_assets']].values[0][0]
        add_produc_bio_assets = ap.loc[ap['end_date'] == baseyear, [
            'produc_bio_assets']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['produc_bio_assets']].values[0][0]
        add_oil_and_gas_assets = ap.loc[ap['end_date'] == baseyear, [
            'oil_and_gas_assets']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['oil_and_gas_assets']].values[0][0]
        add_fixed_assets_disp = ap.loc[ap['end_date'] == baseyear, [
            'fixed_assets_disp']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['fixed_assets_disp']].values[0][0]
        add_lt_investment = ap.loc[ap['end_date'] == baseyear, [
            'total_lt_investment']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['total_lt_investment']].values[0][0]
        add_oth_nca = ap.loc[ap['end_date'] == baseyear, [
            'oth_nca']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['oth_nca']].values[0][0]
        add_oth_ncl = ap.loc[ap['end_date'] == baseyear, [
            'oth_ncl']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['oth_ncl']].values[0][0]
        add_defer_tax_assets = ap.loc[ap['end_date'] == baseyear, [
            'defer_tax_assets']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['defer_tax_assets']].values[0][0]
        add_defer_tax_liab = ap.loc[ap['end_date'] == baseyear, [
            'defer_tax_liab']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['defer_tax_liab']].values[0][0]
        add_forex_differ = ap.loc[ap['end_date'] == baseyear, [
            'forex_differ']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['forex_differ']].values[0][0]
        add_trad_asset = ap.loc[ap['end_date'] == baseyear, [
            'trad_asset']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['trad_asset']].values[0][0]
        add_div_receiv = ap.loc[ap['end_date'] == baseyear, [
            'div_receiv']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['div_receiv']].values[0][0]
        add_int_receiv = ap.loc[ap['end_date'] == baseyear, [
            'int_receiv']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['int_receiv']].values[0][0]
        add_nca_within_1y = ap.loc[ap['end_date'] == baseyear, [
            'nca_within_1y']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['nca_within_1y']].values[0][0]
        add_trading_fl = ap.loc[ap['end_date'] == baseyear, [
            'trading_fl']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['trading_fl']].values[0][0]
        add_st_bonds_payable = ap.loc[ap['end_date'] == baseyear, [
            'st_bonds_payable']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['st_bonds_payable']].values[0][0]
        add_div_payable = ap.loc[ap['end_date'] == baseyear, [
            'div_payable']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['div_payable']].values[0][0]
        add_int_payable = ap.loc[ap['end_date'] == baseyear, [
            'int_payable']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['int_payable']].values[0][0]
        add_non_cur_liab_due_1y = ap.loc[ap['end_date'] == baseyear, [
            'non_cur_liab_due_1y']].values[0][0] - ap.loc[ap['end_date'] == preyear, ['non_cur_liab_due_1y']].values[0][0]

        # print(baseyear,add_oper_receiv)
        ap.loc[ap['end_date']==baseyear,['add_oper_receiv']] = add_oper_receiv
        ap.loc[ap['end_date'] == baseyear, ['add_oper_payable']] = add_oper_payable
        ap.loc[ap['end_date'] == baseyear, ['add_oper_gap_surplus']] = add_oper_gap_surplus
        ap.loc[ap['end_date'] == baseyear, ['add_total_intan_oth_assets']] = add_total_intan_oth_assets
        ap.loc[ap['end_date'] == baseyear, ['add_produc_bio_assets']] = add_produc_bio_assets
        ap.loc[ap['end_date'] == baseyear, ['add_oil_and_gas_assets']] = add_oil_and_gas_assets
        ap.loc[ap['end_date'] == baseyear, ['add_fixed_assets_disp']] = add_fixed_assets_disp
        ap.loc[ap['end_date'] == baseyear, ['add_lt_investment']] = add_lt_investment
        ap.loc[ap['end_date'] == baseyear, ['add_oth_nca']] = add_oth_nca
        ap.loc[ap['end_date'] == baseyear, ['add_oth_ncl']] = add_oth_ncl
        ap.loc[ap['end_date'] == baseyear, ['add_defer_tax_assets']] = add_defer_tax_assets
        ap.loc[ap['end_date'] == baseyear, ['add_defer_tax_liab']] = add_defer_tax_liab
        ap.loc[ap['end_date'] == baseyear, ['add_forex_differ']] = add_forex_differ
        ap.loc[ap['end_date'] == baseyear, ['add_trad_asset']] = add_trad_asset
        ap.loc[ap['end_date'] == baseyear, ['add_div_receiv']] = add_div_receiv
        ap.loc[ap['end_date'] == baseyear, ['add_int_receiv']] = add_int_receiv
        ap.loc[ap['end_date'] == baseyear, ['add_nca_within_1y']] = add_nca_within_1y
        ap.loc[ap['end_date'] == baseyear, ['add_trading_fl']] = add_trading_fl
        ap.loc[ap['end_date'] == baseyear, ['add_st_bonds_payable']] = add_st_bonds_payable
        ap.loc[ap['end_date'] == baseyear, ['add_div_payable']] = add_div_payable
        ap.loc[ap['end_date'] == baseyear, ['add_int_payable']] = add_int_payable
        ap.loc[ap['end_date'] == baseyear, ['add_non_cur_liab_due_1y']] = add_non_cur_liab_due_1y


        
    ap['add_capital_investment'] = ap['add_fixture'] + ap['add_materials'] + ap['add_cip'] + ap['add_fixed_assets_disp'] + ap['add_produc_bio_assets'] + ap['add_oil_and_gas_assets']

    # print(ap[{'end_date', 'notes_payable', 'acct_payable',
    #           'adv_receipts', 'payroll_payable', 'lt_payroll_payable', 'taxes_payable', 'oth_payable', 'acc_exp', 'deferred_inc', 'oth_cur_liab', 'lt_payable', 'specific_payables', 'estimated_liab'}])
    # print(ap[{'end_date', 'compr_inc_attr_p'}])
    return ap

def balancesheet(pro, stockcode, startDate, endDate):
    bs = pro.balancesheet(ts_code=stockcode, start_date=startDate, end_date=endDate)
    bs = bs.loc[(bs['end_date'].str.slice(4, 8) == "1231")]
    bs = bs.loc[(bs['fix_assets'].isna() == False)]
    bs = bs.reset_index()
    # print(bs[{'end_date','fix_assets'}])
    # profit.columns = ['index', 'end_date', 'revenue',
    #                   'oper_cost', 'income_tax', 'n_income', 'ebit']
    bs = bs.drop(columns=['index', 'ts_code', 'ann_date',
                          'f_ann_date', 'report_type', 'comp_type', 'acc_receivable', 'st_fin_payable', 'payables', 'hfs_assets', 'hfs_sales', 'lending_funds', 'oth_eqt_tools_p_shr', 'oth_eqt_tools'])
    bs['bond_payable'].fillna(0, inplace=True)
    bs['trad_asset'].fillna(0,inplace=True)
    bs['div_receiv'].fillna(0, inplace=True)
    bs['oth_receiv'].fillna(0, inplace=True)
    bs['amor_exp'].fillna(0,inplace=True)
    bs['nca_within_1y'].fillna(0,inplace=True)
    bs['oth_cur_assets'].fillna(0,inplace=True)
    bs['fa_avail_for_sale'].fillna(0,inplace=True)
    bs['htm_invest'].fillna(0,inplace=True)
    bs['invest_real_estate'].fillna(0,inplace=True)
    bs['lt_eqt_invest'].fillna(0,inplace=True)
    bs['fixed_assets_disp'].fillna(0,inplace=True)
    bs['produc_bio_assets'].fillna(0,inplace=True)
    bs['oil_and_gas_assets'].fillna(0,inplace=True)
    bs['r_and_d'].fillna(0,inplace=True)
    bs['goodwill'].fillna(0,inplace=True)
    bs['lt_rec'].fillna(0,inplace=True)
    bs['lt_amor_exp'].fillna(0,inplace=True)
    bs['defer_tax_assets'].fillna(0,inplace=True)
    bs['oth_nca'].fillna(0,inplace=True)
    bs['trading_fl'].fillna(0,inplace=True)
    bs['notes_payable'].fillna(0,inplace=True)
    bs['acct_payable'].fillna(0,inplace=True)
    bs['st_bonds_payable'].fillna(0,inplace=True)
    bs['adv_receipts'].fillna(0,inplace=True)
    bs['payroll_payable'].fillna(0,inplace=True)
    bs['div_payable'].fillna(0,inplace=True)
    bs['taxes_payable'].fillna(0,inplace=True)
    bs['int_payable'].fillna(0,inplace=True)
    bs['oth_payable'].fillna(0,inplace=True)
    bs['acc_exp'].fillna(0,inplace=True)
    bs['deferred_inc'].fillna(0,inplace=True)
    bs['non_cur_liab_due_1y'].fillna(0,inplace=True)
    bs['oth_cur_liab'].fillna(0,inplace=True)
    bs['lt_payable'].fillna(0,inplace=True)
    bs['specific_payables'].fillna(0,inplace=True)
    bs['estimated_liab'].fillna(0,inplace=True)
    bs['defer_tax_liab'].fillna(0,inplace=True)
    bs['defer_inc_non_cur_liab'].fillna(0,inplace=True)
    bs['oth_nca'].fillna(0,inplace=True)
    bs['oth_ncl'].fillna(0,inplace=True)
    bs['forex_differ'].fillna(0,inplace=True)
    bs['cip'].fillna(0,inplace=True)
    bs['lt_amor_exp'].fillna(0,inplace=True)
    bs['const_materials'].fillna(0,inplace=True)
    bs['oth_receiv'].fillna(0,inplace=True)
    bs['prepayment'].fillna(0,inplace=True)
    bs['inventories'].fillna(0,inplace=True)
    bs['notes_receiv'].fillna(0,inplace=True)
    bs['accounts_receiv'].fillna(0,inplace=True)
    bs['lt_payroll_payable'].fillna(0,inplace=True)
    bs['produc_bio_assets'].fillna(0,inplace=True)
    bs['oil_and_gas_assets'].fillna(0,inplace=True)
    bs['int_receiv'].fillna(0,inplace=True)
    bs['invest_loss_unconf'].fillna(0,inplace=True)
    bs['lt_borr'].fillna(0,inplace=True)
    bs['st_borr'].fillna(0,inplace=True)
    bs['money_cap'].fillna(0,inplace=True)
    bs['total_liab'].fillna(0,inplace=True)
    bs['minority_int'].fillna(0,inplace=True)
    bs['fix_assets'].fillna(0,inplace=True)
    bs.sort_values(by=["end_date"], inplace=True, ascending=[True])
    bs['add_surplus_rese'] = 0
    for key, value in bs.iterrows():
        if value['end_date'] == bs['end_date'].min():
            a = value['surplus_rese']
        else:
            bs.loc[bs['end_date'] == value['end_date'], [
                'add_surplus_rese']] = value['surplus_rese'] - a
            a = value['surplus_rese']
    for i in range(1,6):
        end_date = bs['end_date'].max()[:2] + str(int(bs['end_date'].max()[2:4])+1) + "1231"   
        trad_asset = weightedmean(bs, 'end_date', 'trad_asset')
        st_borr = weightedmean(bs, 'end_date', 'st_borr')
        lt_borr = weightedmean(bs, 'end_date', 'lt_borr')
        div_receiv = weightedmean(bs, 'end_date', 'div_receiv')
        int_receiv = weightedmean(bs, 'end_date', 'int_receiv')
        nca_within_1y = weightedmean(bs, 'end_date', 'nca_within_1y')
        oth_cur_assets = weightedmean(bs, 'end_date', 'oth_cur_assets')
        fa_avail_for_sale = weightedmean(bs, 'end_date','fa_avail_for_sale')
        htm_invest = weightedmean(bs, 'end_date', 'htm_invest')
        invest_real_estate = weightedmean(bs, 'end_date', 'invest_real_estate')
        lt_eqt_invest = weightedmean(bs,'end_date','lt_eqt_invest')
        fixed_assets_disp = weightedmean(bs, 'end_date', 'fixed_assets_disp')
        produc_bio_assets = weightedmean(bs, 'end_date', 'produc_bio_assets')
        oil_and_gas_assets = weightedmean(bs, 'end_date', 'oil_and_gas_assets')
        r_and_d = weightedmean(bs, 'end_date', 'r_and_d')
        goodwill = weightedmean(bs, 'end_date', 'goodwill')
        lt_rec = weightedmean(bs, 'end_date', 'lt_rec')
        lt_amor_exp = weightedmean(bs, 'end_date', 'lt_amor_exp')
        defer_tax_assets = weightedmean(bs,'end_date','defer_tax_assets')
        oth_nca = weightedmean(bs, 'end_date', 'oth_nca')
        trading_fl = weightedmean(bs, 'end_date', 'trading_fl')
        notes_payable = weightedmean(bs, 'end_date', 'notes_payable')
        acct_payable = weightedmean(bs, 'end_date', 'acct_payable')
        st_bonds_payable = weightedmean(bs, 'end_date','st_bonds_payable')
        adv_receipts = weightedmean(bs, 'end_date', 'adv_receipts')
        payroll_payable = weightedmean(bs, 'end_date', 'payroll_payable')
        div_payable = weightedmean(bs, 'end_date', 'div_payable')
        taxes_payable = weightedmean(bs, 'end_date', 'taxes_payable')
        int_payable = weightedmean(bs, 'end_date', 'int_payable')
        oth_payable = weightedmean(bs, 'end_date', 'oth_payable')
        acc_exp = weightedmean(bs, 'end_date', 'acc_exp')
        deferred_inc = weightedmean(bs, 'end_date', 'deferred_inc')
        non_cur_liab_due_1y = weightedmean(
            bs, 'end_date', 'non_cur_liab_due_1y')
        oth_cur_liab = weightedmean(bs, 'end_date', 'oth_cur_liab')
        lt_payable = weightedmean(bs, 'end_date', 'lt_payable')
        specific_payables = weightedmean(bs, 'end_date', 'specific_payables')
        estimated_liab = weightedmean(bs, 'end_date', 'estimated_liab')
        defer_tax_liab = weightedmean(bs, 'end_date', 'defer_tax_liab')
        defer_inc_non_cur_liab = weightedmean(
            bs, 'end_date', 'defer_inc_non_cur_liab')
        oth_ncl = weightedmean(bs, 'end_date', 'oth_ncl')
        forex_differ = weightedmean(bs, 'end_date', 'forex_differ')
        oth_receiv = weightedmean(bs, 'end_date', 'oth_receiv')
        amor_exp = weightedmean(bs, 'end_date','amor_exp')
        prepayment = weightedmean(bs,'end_date','prepayment')
        inventories = weightedmean(bs, 'end_date', 'inventories')
        lt_payroll_payable = weightedmean(bs, 'end_date', 'lt_payroll_payable')
        produc_bio_assets = weightedmean(bs, 'end_date', 'produc_bio_assets')
        oil_and_gas_assets = weightedmean(bs, 'end_date', 'oil_and_gas_assets')
        bond_payable = weightedmean(bs, 'end_date', 'bond_payable')
        invest_loss_unconf = weightedmean(bs, 'end_date', 'invest_loss_unconf')
        money_cap = weightedmean(bs, 'end_date', 'money_cap')
        total_liab = weightedmean(bs, 'end_date', 'total_liab')
        fix_assets = weightedmean(bs, 'end_date', 'fix_assets')
        bs = bs.append({'end_date': end_date, 'st_borr': st_borr, 'trad_asset': trad_asset, 'div_receiv': div_receiv, 
                        'int_receiv': int_receiv, 'nca_within_1y': nca_within_1y, 'oth_cur_assets': oth_cur_assets, 'fa_avail_for_sale': fa_avail_for_sale, 
                        'htm_invest': htm_invest, 'invest_real_estate': invest_real_estate, 'lt_eqt_invest': lt_eqt_invest, 
                        'fixed_assets_disp': fixed_assets_disp, 'produc_bio_assets': produc_bio_assets, 'oil_and_gas_assets': oil_and_gas_assets, 
                        'r_and_d': r_and_d, 'goodwill': goodwill, 'lt_rec': lt_rec, 'lt_amor_exp': lt_amor_exp, 'defer_tax_assets': defer_tax_assets, 
                        'oth_nca': oth_nca, 'trading_fl': trading_fl, 'notes_payable': notes_payable, 'acct_payable': acct_payable, 
                        'st_bonds_payable': st_bonds_payable, 'adv_receipts': adv_receipts, 'payroll_payable': payroll_payable, 'div_payable': div_payable, 
                        'taxes_payable': taxes_payable, 'int_payable': int_payable, 'oth_payable': oth_payable, 'acc_exp': acc_exp, 
                        'deferred_inc': deferred_inc, 'non_cur_liab_due_1y': non_cur_liab_due_1y, 'oth_cur_liab': oth_cur_liab, 
                        'lt_payable': lt_payable, 'specific_payables': specific_payables, 'estimated_liab': estimated_liab, 
                        'defer_tax_liab': defer_tax_liab, 'defer_inc_non_cur_liab': defer_inc_non_cur_liab, 'oth_ncl': oth_ncl, 
                        'forex_differ': forex_differ, 'oth_receiv': oth_receiv, 'amor_exp': amor_exp, 'prepayment': prepayment,
                        'inventories': inventories, 'lt_payroll_payable': lt_payroll_payable, 'produc_bio_assets': produc_bio_assets,
                        'oil_and_gas_assets': oil_and_gas_assets, 'bond_payable': bond_payable, 'invest_loss_unconf': invest_loss_unconf,
                        'lt_borr': lt_borr, 'money_cap': money_cap, 'total_liab': total_liab, 'fix_assets': fix_assets}, ignore_index=True)
    # print(bs['invest_loss_unconf'])

    return bs


def cashflow(pro, stockcode, startDate, endDate):
    cf = pro. cashflow(ts_code=stockcode, start_date=startDate, end_date=endDate)
    cf = cf.loc[(cf['end_date'].str.slice(4, 8) == "1231")]
    cf['amort_intang_assets'].fillna(0,inplace=True)
    cf['lt_amort_deferred_exp'].fillna(0,inplace=True)
    cf['prov_depr_assets'].fillna(0,inplace=True)
    cf = cf.loc[(cf['end_bal_cash'].isna() == False)]
    for i in range(1, 6):
        end_date = str(int(cf['end_date'].max()[:4])+1) + "1231"
        prov_depr_assets = weightedmean(cf, 'end_date', 'prov_depr_assets')
        cf = cf.append(
            {'end_date': end_date, 'prov_depr_assets': prov_depr_assets}, ignore_index=True)
    # print(cf[{'end_date', 'c_cash_equ_end_period'}])

    return cf

def projected_cf(ap,cf,tdtext):
    proj_cf = pd.DataFrame()
    curryear = tdtext[:4] + "1231"
    c_cash_equ_beg_period = cf.loc[cf['end_date'] == cf['end_date'].max(), ['c_cash_equ_end_period']].values[0][0]

    for i in range(5):
        curryear = str(int(tdtext[:4])+i)+"1231"
        net_profit = ap.loc[ap['end_date'] == curryear, ['compr_inc_attr_p']].values[0][0]
        minority_gain = ap.loc[ap['end_date'] == curryear, ['minority_gain']].values[0][0]
        total_depreciation_amorization = ap.loc[ap['end_date'] == curryear, ['total_depreciation_amorization']].values[0][0]
        fv_value_chg_gain = ap.loc[ap['end_date'] == curryear, ['fv_value_chg_gain']].values[0][0]
        media_interest_income = -ap.loc[ap['end_date'] == curryear, ['media_interest_income']].values[0][0]   
        total_borr_interest = ap.loc[ap['end_date'] == curryear, ['total_borr_interest']].values[0][0]
        invest_income = -ap.loc[ap['end_date'] == curryear, ['invest_income']].values[0][0]
        add_oper_receiv = -ap.loc[ap['end_date'] == curryear, ['add_oper_receiv']].values[0][0]
        add_oper_payable = ap.loc[ap['end_date'] == curryear, ['add_oper_payable']].values[0][0]
        add_oth_nca = -ap.loc[ap['end_date'] == curryear, ['add_oth_nca']].values[0][0]
        add_oth_ncl = ap.loc[ap['end_date'] == curryear, ['add_oth_ncl']].values[0][0]
        add_defer_tax_assets = -ap.loc[ap['end_date'] == curryear, ['add_defer_tax_assets']].values[0][0]
        add_defer_tax_liab = ap.loc[ap['end_date'] == curryear, ['add_defer_tax_liab']].values[0][0]
        add_forex_differ = ap.loc[ap['end_date'] == curryear, ['add_forex_differ']].values[0][0]
        invest_loss_unconf = ap.loc[ap['end_date'] == curryear, ['invest_loss_unconf']].values[0][0]
        n_cashflow_act = net_profit + minority_gain + \
            (total_depreciation_amorization + media_interest_income) + fv_value_chg_gain + \
            total_borr_interest + invest_income + \
            add_oper_receiv + add_oper_payable + \
            add_oth_nca + add_oth_ncl + add_defer_tax_assets + \
            add_defer_tax_liab + add_forex_differ + invest_loss_unconf
        # print(net_profit, minority_gain, total_depreciation_amorization, media_interest_income,
        #       fv_value_chg_gain, total_borr_interest, invest_income, add_oper_receiv, add_oper_payable, add_oth_nca, add_oth_ncl, add_defer_tax_assets, add_defer_tax_liab, add_forex_differ, invest_loss_unconf)


        add_trad_asset = -ap.loc[ap['end_date'] == curryear, ['add_trad_asset']].values[0][0]
        add_div_receiv = -ap.loc[ap['end_date'] == curryear, ['add_div_receiv']].values[0][0]
        add_int_receiv = -ap.loc[ap['end_date'] == curryear, ['add_int_receiv']].values[0][0]
        add_lt_investment = -ap.loc[ap['end_date'] == curryear, ['add_lt_investment']].values[0][0]
        add_nca_within_1y = -ap.loc[ap['end_date'] == curryear, ['add_nca_within_1y']].values[0][0]
        add_capital_investment = -ap.loc[ap['end_date'] == curryear, ['add_capital_investment']].values[0][0]
        add_total_intan_oth_assets = -ap.loc[ap['end_date'] == curryear, ['add_total_intan_oth_assets']].values[0][0]
        invest_income = ap.loc[ap['end_date'] == curryear, ['invest_income']].values[0][0]
        n_cashflow_inv_act = add_trad_asset + \
            add_div_receiv + add_int_receiv + add_nca_within_1y + \
            add_capital_investment + add_total_intan_oth_assets + invest_income

        
        add_trading_fl = ap.loc[ap['end_date'] == curryear, ['add_trading_fl']].values[0][0]
        add_st_bonds_payable = ap.loc[ap['end_date'] == curryear, ['add_st_bonds_payable']].values[0][0]
        add_div_payable = ap.loc[ap['end_date'] == curryear, ['add_div_payable']].values[0][0]
        add_int_payable = ap.loc[ap['end_date'] == curryear, ['add_int_payable']].values[0][0]
        add_non_cur_liab_due_1y = ap.loc[ap['end_date'] == curryear, ['add_non_cur_liab_due_1y']].values[0][0]
        add_lt_borr = ap.loc[ap['end_date'] == curryear, ['add_lt_borr']].values[0][0]
        bond_payable = ap.loc[ap['end_date'] == curryear, ['bond_payable']].values[0][0]
        media_interest_income = ap.loc[ap['end_date'] == curryear, ['media_interest_income']].values[0][0]
        total_borr_interest = -ap.loc[ap['end_date'] == curryear, ['total_borr_interest']].values[0][0]
        end_total_share = ap.loc[ap['end_date'] == curryear, ['end_total_share']].values[0][0]
        cash_div_tax_rate = ap.loc[ap['end_date'] == curryear, ['cash_div_tax_rate']].values[0][0]
        eps = ap.loc[ap['end_date'] == curryear, ['compr_inc_attr_p']].values[0][0] / end_total_share
        cash_div_tax = eps * cash_div_tax_rate
        dividend = -cash_div_tax * end_total_share
        add_cap_rese = ap.loc[ap['end_date'] == curryear, ['add_cap_rese']].values[0][0]
        n_cash_flows_fnc_act_before_st_borr = add_trading_fl + \
            add_st_bonds_payable + add_div_payable + \
            add_int_payable + add_non_cur_liab_due_1y + \
            add_lt_borr + bond_payable + \
            (total_borr_interest - media_interest_income) + dividend + add_cap_rese
        add_st_borr = ap.loc[ap['end_date'] == curryear, ['add_st_borr']].values[0][0]
        end_st_borr = ap.loc[ap['end_date'] == curryear, ['end_st_borr']].values[0][0]
        n_cash_flows_fnc_act = n_cash_flows_fnc_act_before_st_borr + end_st_borr/2
        im_n_incr_cash_equ = n_cashflow_act + n_cashflow_inv_act + n_cash_flows_fnc_act
        c_cash_equ_end_period = c_cash_equ_beg_period + im_n_incr_cash_equ
        c_cash_equ_beg_period = c_cash_equ_end_period

        proj_cf = proj_cf.append({'end_date': curryear, 'n_cashflow_act': n_cashflow_act, 'n_cashflow_inv_act': n_cashflow_inv_act, 'n_cash_flows_fnc_act': n_cash_flows_fnc_act,
                              'c_cash_equ_beg_period': c_cash_equ_beg_period, 'c_cash_equ_end_period': c_cash_equ_end_period, 'n_cash_flows_fnc_act_before_st_borr': n_cash_flows_fnc_act_before_st_borr}, ignore_index=True)
    # print(ap['add_oper_payable'])
    # print(n_cashflow_act)
    return proj_cf

def projected_debt(bs, ap, proj_cf,tdtext):
    debt = pd.DataFrame()
    st_loan_rate = 0.0547
    income_interest_rate = 0.0093
    curryear = tdtext[:4] + "1231"
    # beg_money_cap = bs.loc[bs['end_date']==str(int(curryear[:4])-1)+"1231",['money_cap']].values[0][0]
    for i in range(5):
        curryear = str(int(tdtext[:4])+i)+"1231"
        preyear = str(int(curryear[:4])-1)+"1231"
        beg_money_cap = bs.loc[bs['end_date']==preyear,['money_cap']].values[0][0]
        n_cashflow_act = proj_cf.loc[proj_cf['end_date']==curryear, ['n_cashflow_act']].values[0][0]
        n_cashflow_inv_act = proj_cf.loc[proj_cf['end_date']==curryear, ['n_cashflow_inv_act']].values[0][0]
        n_cash_flows_fnc_act_before_st_borr = proj_cf.loc[proj_cf['end_date']==curryear, ['n_cash_flows_fnc_act_before_st_borr']].values[0][0]
        min_st_borr = bs.loc[bs['end_date'] == curryear,['st_borr']].values[0][0]
        open_st_borr= bs.loc[bs['end_date'] == preyear,['st_borr']].values[0][0]
        min_cf = ap.loc[ap['end_date'] == curryear, ['min_cf']].values[0][0]
        cash_gap_surplus = -(beg_money_cap + n_cashflow_act + n_cashflow_inv_act + \
            n_cash_flows_fnc_act_before_st_borr + min_st_borr - min_cf)
        # print(beg_money_cap, n_cashflow_act, n_cashflow_inv_act,
        #       n_cash_flows_fnc_act_before_st_borr, min_st_borr)
        end_st_borr = min_st_borr + min(open_st_borr,cash_gap_surplus)
        str_borr_interest = (open_st_borr + end_st_borr)/2 * st_loan_rate
        interest_income = ((min_cf + cash_gap_surplus) +
                        beg_money_cap) / 2 * income_interest_rate
        # print(min_cf, cash_gap_surplus, beg_money_cap)
        
        debt = debt.append({'end_date': curryear, 'str_borr_interest': str_borr_interest,
                            'interest_income': interest_income, 'beg_money_cap': beg_money_cap}, ignore_index=True)

    # print(debt[{'end_date', 'str_borr_interest', 'interest_income'}])
    return debt

    # print(min_cf, interest_income)

def projected_DCF(ap,debt,pro,stockcode,tdtext):
    proj_profit = pd.DataFrame()
    curryear = tdtext[:4] + "1231"
    total_hldr_eqy_inc_min_int = ap.loc[ap['end_date']== str(int(curryear[:4])-1)+"1231",['total_hldr_eqy_inc_min_int']].values[0][0]
    
    beg_money_cap = ap.loc[ap['end_date']==str(int(curryear[:4])-1)+"1231", ['money_cap']].values[0][0]
    trad_asset = ap.loc[ap['end_date'] == str(int(curryear[:4])-1)+"1231", ['trad_asset']].values[0][0]
    fa_avail_for_sale = ap.loc[ap['end_date'] == str(int(curryear[:4])-1)+"1231", ['fa_avail_for_sale']].values[0][0]
    htm_invest = ap.loc[ap['end_date'] == str(int(curryear[:4])-1)+"1231", ['htm_invest']].values[0][0]
    invest_real_estate = ap.loc[ap['end_date'] == str(int(curryear[:4])-1)+"1231", ['invest_real_estate']].values[0][0]
    lt_eqt_invest = ap.loc[ap['end_date'] == str(int(curryear[:4])-1)+"1231", ['lt_eqt_invest']].values[0][0]
    oth_nca = ap.loc[ap['end_date'] == str(int(curryear[:4])-1)+"1231", ['oth_nca']].values[0][0]

    st_borr = ap.loc[ap['end_date']==str(int(curryear[:4])-1)+"1231", ['st_borr']].values[0][0]
    trading_fl = ap.loc[ap['end_date']==str(int(curryear[:4])-1)+"1231", ['trading_fl']].values[0][0]
    st_bonds_payable = ap.loc[ap['end_date']==str(int(curryear[:4])-1)+"1231", ['st_bonds_payable']].values[0][0]
    lt_borr = ap.loc[ap['end_date']==str(int(curryear[:4])-1)+"1231", ['lt_borr']].values[0][0]
    non_cur_liab_due_1y = ap.loc[ap['end_date']==str(int(curryear[:4])-1)+"1231", ['non_cur_liab_due_1y']].values[0][0]
    bond_payable = ap.loc[ap['end_date']==str(int(curryear[:4])-1)+"1231", ['bond_payable']].values[0][0]

    non_core_assets = beg_money_cap + trad_asset + fa_avail_for_sale + htm_invest + invest_real_estate + lt_eqt_invest + oth_nca
    debt_value = st_borr + trading_fl + st_bonds_payable + lt_borr + non_cur_liab_due_1y + bond_payable

    minority_int = ap.loc[ap['end_date']==str(int(curryear[:4])-1)+"1231", ['minority_int']].values[0][0]
    total_share = ap.loc[ap['end_date'] == str(
        int(curryear[:4])-1)+"1231", ['total_share']].values[0][0]
    # print(non_core_assets, debt_value)
    dcf_list = []
    compr_inc_attr_p_list = []
    for i in range(5):
        curryear = str(int(tdtext[:4])+i)+"1231"
        preyear = str(int(curryear[:4])-1)+"1231"
        str_borr_interest = debt.loc[debt['end_date']==curryear, ['str_borr_interest']].values[0][0]
        interest_income = debt.loc[debt['end_date']==curryear, ['interest_income']].values[0][0]
        lt_borr_interest = ap.loc[ap['end_date']== curryear, ['end_lt_borr_interest']].values[0][0]
        bond_payable_interest = ap.loc[ap['end_date']== curryear, ['end_bond_payable_interest']].values[0][0]
        revenue = ap.loc[ap['end_date'] == curryear, ['revenue']].values[0][0]
        oper_cost = ap.loc[ap['end_date'] == curryear, ['oper_cost']].values[0][0]
        biz_tax_surchg = ap.loc[ap['end_date'] == curryear, ['biz_tax_surchg']].values[0][0]
        sell_exp = ap.loc[ap['end_date'] == curryear, ['sell_exp']].values[0][0]
        admin_exp = ap.loc[ap['end_date'] == curryear, ['admin_exp']].values[0][0]
        forex_gain = ap.loc[ap['end_date'] == curryear, ['forex_gain']].values[0][0]
        assets_impair_loss = ap.loc[ap['end_date'] == curryear, ['assets_impair_loss']].values[0][0]
        fv_value_chg_gain = ap.loc[ap['end_date'] == curryear, ['fv_value_chg_gain']].values[0][0]
        invest_income = ap.loc[ap['end_date'] == curryear, ['invest_income']].values[0][0]
        non_oper_income = ap.loc[ap['end_date'] == curryear, ['non_oper_income']].values[0][0]
        non_oper_exp = ap.loc[ap['end_date'] == curryear, ['non_oper_exp']].values[0][0]
        tax_rate = ap.loc[ap['end_date'] == curryear, ['tax_rate']].values[0][0]
        minority_gain_rate = ap.loc[ap['end_date'] == curryear, ['minority_gain_rate']].values[0][0]
        oth_compr_income = ap.loc[ap['end_date'] == curryear, ['oth_compr_income']].values[0][0]
        total_depreciation_amorization = ap.loc[ap['end_date'] == curryear, ['total_depreciation_amorization']].values[0][0]
        add_oper_gap_surplus = ap.loc[ap['end_date'] == curryear, ['add_oper_gap_surplus']].values[0][0]
        add_capital_investment = ap.loc[ap['end_date'] == curryear, ['add_capital_investment']].values[0][0]
        prov_depr_assets = ap.loc[ap['end_date'] == curryear, ['prov_depr_assets']].values[0][0]
        total_liab = ap.loc[ap['end_date'] == curryear, ['total_liab']].values[0][0]


        gross_margin = revenue - oper_cost - biz_tax_surchg
        operate_profit = gross_margin - sell_exp - admin_exp + interest_income + forex_gain - (str_borr_interest + lt_borr_interest + bond_payable_interest) - assets_impair_loss + fv_value_chg_gain + invest_income
        # print(sell_exp, admin_exp, interest_income,
        #       forex_gain, str_borr_interest, lt_borr_interest, bond_payable_interest, assets_impair_loss, fv_value_chg_gain, invest_income)
        total_profit = operate_profit + non_oper_income - non_oper_exp
        income_tax = total_profit * tax_rate
        n_income = total_profit - income_tax
        minority_gain = n_income * minority_gain_rate
        n_income_attr_p = n_income - minority_gain
        compr_inc_attr_p = n_income_attr_p + oth_compr_income
        compr_inc_attr_p_list.append(compr_inc_attr_p)
        total_hldr_eqy_inc_min_int = total_hldr_eqy_inc_min_int + compr_inc_attr_p + minority_gain
        debt_to_assets = total_liab/(total_liab+total_hldr_eqy_inc_min_int)
        # print(debt_to_assets)

        EBIT = operate_profit + \
            (str_borr_interest + lt_borr_interest +
             bond_payable_interest) - interest_income
        EBITDA = EBIT + total_depreciation_amorization
        NOPLAT = EBIT * (1-tax_rate)
        gross_oper_cf = NOPLAT + total_depreciation_amorization
        cashflow_act = gross_oper_cf - add_oper_gap_surplus
        FCFF = cashflow_act - add_capital_investment + prov_depr_assets

        beta, rf, rm, ke, debt_premium = key_params(pro, stockcode, tdtext)
        kd = (debt_premium + rf) * (1-tax_rate)
        WACC = ke * (1-debt_to_assets) + kd * debt_to_assets
        discount_factor = 1/(1+WACC) ** (i+1)

        discount_cash = FCFF * discount_factor
        dcf_list.append(discount_cash)

        proj_profit = proj_profit.append(
            {'end_date': curryear, 'EBIT': EBIT, 'NOPLAT': NOPLAT, 'gross_oper_cf': gross_oper_cf, 'cashflow_act': cashflow_act, 'FCFF': FCFF, 'total_liab': total_liab, 'total_hldr_eqy_inc_min_int': total_hldr_eqy_inc_min_int, 'debt_to_assets': debt_to_assets}, ignore_index=True)

        # print(gross_oper_cf, add_oper_gap_surplus,
        #       add_capital_investment, prov_depr_assets)
    prepetual_discount_factor = 1/(1+WACC) ** 5
    prepetual_growth = 0.00
    prepetual_FCFF = dcf_list[-1]
    prepetual_discount_cash = prepetual_FCFF * \
        (1+prepetual_growth) * prepetual_discount_factor
    corp_value = sum(dcf_list) + prepetual_discount_cash + \
        non_core_assets - debt_value - minority_int
    DCF = corp_value/total_share

    EPS = compr_inc_attr_p_list[0] / total_share
    pe = DCF/EPS

    # print(DCF,pe,total_share)
    return DCF

def dividend(pro,startDate, endDate, stockcode):
    div = pro.dividend(ts_code=stockcode, fields='end_date,stk_div,stk_bo_rate,stk_co_rate,cash_div_tax,imp_ann_date')
    div = div.loc[(div['end_date'].str.slice(4, 8) == "1231")]
    div['cash_div_tax'].fillna(0,inplace=True)
    div = div.loc[(div['imp_ann_date'].isna() == False)]
    # print(div[{'end_date','cash_div_tax'}])
    return div

def key_params(pro,stockcode,tdtext):
    st_loan_rate = 0.0547
    yearstart = str(int(tdtext[:4])-1) + tdtext[4:]
    # print(yearstart)
    df1 = pro.daily(ts_code=stockcode, start_date=yearstart,
                    end_date=tdtext).sort_values(by='trade_date')
    if stockcode.split('.')[1] == "SZ":
        print("SZ", stockcode)
        df2 = pro.index_daily(ts_code='399001.SZ', start_date=yearstart,
                              end_date=tdtext).sort_values(by='trade_date')
    else:
        print("SH", stockcode)
        df2 = pro.index_daily(ts_code='000001.SH', start_date=yearstart,
                            end_date=tdtext).sort_values(by='trade_date')
    a = len(df1['pct_chg'])
    s1 = df1['pct_chg']
    s2 = df2['pct_chg'].iloc[:a]
    beta = (np.cov(s1, s2))[0][1]/np.var(s2)
    rf = 0.0271
    rm = 0.08
    ke = rf + beta * (rm-rf)
    debt_premium = st_loan_rate - rf

    return beta, rf, rm, ke, debt_premium

def query_stock(pro, exchange_market):
    stock_basic = pro.query('stock_basic', exchange=exchange_market, list_status='L', fields='ts_code,name, industry')
    return stock_basic

def query_sql_dcf(td):
    dcf_pool = []
    db = pymysql.connect(host='localhost', port=8080, db='stock_market', user='root', password='', charset="utf8")
    cur = db.cursor()
    sql = "SELECT stock_code FROM stock_financial_info WHERE dcf > -1000 and dcf IS NOT NULL and dcf_period = '%s'" % (str(td.year) + "_" + str(td.year + 4))
    cur.execute(sql)
    results = cur.fetchall()
    for row in results:
        if row[0]:
            dcf_pool.append(row[0])
        else:
            pass
    cur.close()
    db.close()
    return dcf_pool


def query_sql_all():
    dcf_all = []
    db = pymysql.connect(host='localhost', port=8080, db='stock_market',
                         user='root', password='', charset="utf8")
    cur = db.cursor()
    sql = "SELECT stock_code FROM stock_financial_info"
    cur.execute(sql)
    results = cur.fetchall()
    for row in results:
        if row[0]:
            dcf_all.append(row[0])
        else:
            pass
    cur.close()
    db.close()
    return dcf_all

def insert_sql_dcf(stock_code, dcf, dcf_period, tdtext):
    db = pymysql.connect(host='localhost', port=8080, db='stock_market',
                         user='root', password='', charset="utf8")
    cur = db.cursor()
    sql = "INSERT INTO stock_financial_info(stock_code, dcf, dcf_period,update_date) values('%s',%s,'%s',str_to_date('%s', '%%Y%%m%%d'))" % (stock_code, dcf, dcf_period, tdtext)
    cur.execute(sql)
    db.commit()
    cur.close()
    db.close()

def update_sql_dcf(stock_code, dcf, dcf_period, tdtext):
    db = pymysql.connect(host='localhost', port=8080, db='stock_market',
                         user='root', password='', charset="utf8")
    cur = db.cursor()
    sql = "UPDATE stock_financial_info SET dcf = %s, dcf_period='%s',update_date=str_to_date('%s', '%%Y%%m%%d') WHERE stock_code = '%s'" % (dcf, dcf_period, tdtext,stock_code)
    cur.execute(sql)
    db.commit()
    cur.close()
    db.close()

def main(argc, argv, envp):
    stock_markets = ["SSE", "SZSE"]
    stock_list = []
    load_rate = 0.08
    interest_rate = 0.04
    td = datetime.today()
    dcf_all = query_sql_all()
    dcf_pool = query_sql_dcf(td)
    time_range = datetime.strftime(
        (datetime.today()-dt.timedelta(days=1825)).date(), '%m/%d/%Y')
    start_date= get_current_month_start_and_end(time_range)
    tdtext = td.strftime('%Y%m%d')
    pro = ts.pro_api(
        '')
    for stock_market in stock_markets:
        stock_list.extend(query_stock(pro, stock_market)['ts_code'].tolist())
    for stockcode in stock_list:
        if stockcode in dcf_pool:
            pass
        else:
            try:
                profit, report_period = profit_report(
                    pro, stockcode, start_date, tdtext)
                div = dividend(pro, start_date, tdtext, stockcode)
                bs = balancesheet(pro, stockcode, start_date, tdtext)
                cf = cashflow(pro, stockcode, start_date, tdtext)
                ap = assumption(profit, div, bs, cf, tdtext, interest_rate)
                proj_cf = projected_cf(ap, cf, tdtext)
                debt = projected_debt(bs, ap, proj_cf, tdtext)
                DCF = projected_DCF(ap, debt, pro, stockcode, tdtext)
                if report_period:
                    dcf_period = report_period[0][:4] + "_" + report_period[-1][:4]
                else:
                    pass
                print(dcf_period)
            except:
                DCF = -1000
                dcf_period = ""
            if stockcode in dcf_all:
                try:
                    update_sql_dcf(stockcode, DCF, dcf_period, tdtext)
                except:
                    pass
            else:
                try:
                    insert_sql_dcf(stockcode, DCF, dcf_period, tdtext)
                except:
                    pass

if __name__ == "__main__":
    sys.exit(main(len(sys.argv), sys.argv, os.environ))
