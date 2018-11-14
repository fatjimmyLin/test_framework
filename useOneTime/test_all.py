# -*- coding: utf-8 -*-
from os.path import isfile, join
import pandas as pd
from os import listdir
import numpy as np
import matplotlib
matplotlib.use('TkAgg')
import os
import time

def main():

    getPath()

def getPath():
    path_name = '/Users/fatjimmy/Desktop/Quant/path_name.csv'
    path = pd.DataFrame(pd.read_csv(path_name, error_bad_lines=False, encoding='gbk', ))[
        ['resource_name', 'factor_name']]
    ic_all = []
    ir_all = []
    resource_all = []
    factor_all = []
    for index in range(len(path)):
        ic,ir = getFactor(str(path['resource_name'][index]),str(path['factor_name'][index]))
        print('Index =', index, str(path['resource_name'][index]), str(path['factor_name'][index]))
        print('IC:=', ic, 'IC_IR:=', ir)
        ic_all.append(ic)
        ir_all.append(ir)
        resource_all.append(str(path['resource_name'][index]))
        factor_all.append(str(path['factor_name'][index]))
    factor_icir = pd.DataFrame({'resource_name':resource_all, 'factor_name':factor_all, 'ic':ic_all, 'ir':ir_all})
    factor_icir = factor_icir.reindex(
        columns=['resource_name', 'factor_name', 'ic', 'ir'])
    factor_icir.to_csv('/Users/fatjimmy/Desktop/icir_fnpi.csv')

#数据预处理并计算每个因子五年的IC,IR值
def getFactor(resource_name,factor_name):
    path_factor = '/Users/fatjimmy/Desktop/Quant/factor_data/'+resource_name+'/'
    path_price = '/Users/fatjimmy/Desktop/Quant/price_data/stock/'

    onlyfiles = [f for f in listdir(path_price) if
                 isfile(join(path_price, f))]
    del onlyfiles[0]
    ic_all = []

    for index in range(len(onlyfiles) - 1):
        if os.path.exists(path_factor + onlyfiles[index]):
            # 写因子
            factor = generate_factor(path_factor, onlyfiles[index], factor_name)

            date = onlyfiles[index][:-4]
            # del factor['Unnamed: 0']
            # 读今天和下一天的收盘价
            price = pd.DataFrame(pd.read_csv(path_price + onlyfiles[index], error_bad_lines=False, encoding='gbk'))[
                ['code', 'industry', 'close']]
            price = price.rename(columns={'code': 'stock_code'})

            price_next = \
                pd.DataFrame(pd.read_csv(path_price + onlyfiles[index + 1], error_bad_lines=False, encoding='gbk'))[
                    ['code', 'close', 'amt']]
            price_next = price_next.rename(columns={'code': 'stock_code'})
            price_next = price_next.rename(columns={'close': 'close_next'})

            # 合并接下来两天的价格
            factor = pd.merge(factor, price, how='left', on=['stock_code'])
            factor = pd.merge(factor, price_next, how='left', on=['stock_code'])
            factor = factor.reindex(
                columns=['stock_code', 'industry', 'factor_value', 'close', 'close_next', 'return_ratio', 'amt'])
            factor['return_ratio'] = (factor['close_next'] - factor['close']) / factor['close']

            # 留下收益率绝对值小于10.03%的股票
            factor = factor[abs(factor.return_ratio) < 0.1003]
            # 去掉成交额不足3000万的股票
            factor = factor[factor.amt >= 30000]
            # 因子去极值:百分位法
            max = factor['factor_value'].quantile(0.975)
            min = factor['factor_value'].quantile(0.025)
            factor = factor[(factor.factor_value > min) & (factor.factor_value < max)]
            ##############################################################################################
            # 把没有当日和第二天价格的因子删掉
            factor = factor.dropna(axis=0, how='any')
            factor = deleteIPO60(factor, date)
            factor = normalization(factor)
            # 要更新一下索引，不然中性化会出bug
            factor.index = range(len(factor))
            # 行业中性化
            factor = industry_neutralization(factor)

            # 计算因子IC,IR值
            factor_testICIR = factor[['factor_value','return_ratio']]
            if factor_testICIR.empty:
                continue
            else:
                ic_result = factor_testICIR.corr()['factor_value'][1]
                # 去掉nan
                if ic_result == ic_result:
                    ic_all.append(ic_result)
    ic_all = np.array(ic_all)
    ic_mean = ic_all.mean()
    ic_std = ic_all.std()
    ir = ic_mean / ic_std
    return ic_mean,ir

#因子写在这里
def generate_factor(path,file,factor_name):
    factor = pd.DataFrame(
        pd.read_csv(path + file, error_bad_lines=False, encoding='gbk',
                    usecols=['stock_code', factor_name]))
    factor = factor.rename(columns={factor_name: 'factor_value'})
    return factor

#因子中性化
def normalization(factor):
    factor_mean = factor['factor_value'].mean()
    sigma = factor['factor_value'].std()
    factor = factor.reindex(
        columns=['stock_code', 'industry', 'factor_value', 'close', 'close_next', 'return_ratio', 'neutralized_factor'])
    factor['neutralized_factor'] = (factor['factor_value']-factor_mean)/sigma
    del factor['factor_value']
    factor = factor.rename(columns={'neutralized_factor':'factor_value'})
    return factor

#删除上市60天以内的公司
def deleteIPO60(factor,date):

    ipo_info = pd.DataFrame(pd.read_csv('~/Desktop/Quant/ipo_60.csv', error_bad_lines=False, encoding='utf-8'))
    # 将factor和ipo信息表合并
    factor = pd.merge(factor, ipo_info, how='left', on=['stock_code'])
    factor = factor.reindex(
        columns=['stock_code', 'industry', 'factor_value', 'close', 'close_next', 'return_ratio', 'date_60'])
    ##############################################################################################

    # date_60有数据，说明有IPO记录，但是已经上市超过60天，标记为1
    factor_not_nan1 = factor[(factor.date_60 == factor.date_60) & (factor.date_60 < float(date))][
        ['stock_code', 'date_60']]
    df_not_nan1 = pd.DataFrame(columns=['stock_code', 'day'])
    df_not_nan1['stock_code'] = factor_not_nan1['stock_code']
    df_not_nan1 = df_not_nan1.fillna(1)

    # date_60有数据，说明有IPO记录，上市尚未超过60天，标记为0
    factor_not_nan0 = factor[(factor.date_60 == factor.date_60) & (factor.date_60 >= float(date))][
        ['stock_code', 'date_60']]
    df_not_nan0 = pd.DataFrame(columns=['stock_code', 'day'])
    df_not_nan0['stock_code'] = factor_not_nan0['stock_code']
    df_not_nan0 = df_not_nan0.fillna(0)

    # date_60为NaN，说明为无IPO记录，标记为1
    factor_nan = factor[factor.date_60 != factor.date_60][['stock_code', 'date_60']]
    df_nan = pd.DataFrame(columns=['stock_code', 'day'])
    df_nan['stock_code'] = factor_nan['stock_code']
    df_nan = df_nan.fillna(1)

    # 将标记好的dataframe与factor取交集
    factorNotNaN1 = pd.merge(factor, df_not_nan1, how='inner', on=['stock_code'])
    factorNotNaN0 = pd.merge(factor, df_not_nan0, how='inner', on=['stock_code'])
    factorNaN = pd.merge(factor, df_nan, how='inner', on=['stock_code'])
    # 合并使得factor加入了day标记
    factor = pd.concat([factorNotNaN1, factorNotNaN0, factorNaN])
    factor = factor.sort_values(by='factor_value', axis=0, ascending=False)
    factor.index = range(len(factor))
    ##############################################################################################
    # 删除60天内的数据，即'day'列数据为0
    factor = factor[factor.day != 0]
    del factor['day']
    del factor['date_60']
    return factor

#行业中性化
def industry_neutralization(factor):
    industries = []
    #统计所有的industry类别
    for i in range(len(factor)):
        factor.at[i, 'industry'] = factor.at[i, 'industry'].encode('utf-8')
        if factor.at[i, 'industry'] not in industries:
            industries.append(factor.at[i, 'industry'])
    #字典的key是industry类别，value是[该行业公司出现的次数，该行业公司因子值的总和，行业因子值的平均数]
    dict_industry = {}
    for i in range(len(industries)):
        str1 = industries[i]
        dict_industry[str1] = [0, 0, 0]
    #计算该行业公司出现的次数，该行业公司因子值的总和
    for i in range(len(factor)):
        if dict_industry.has_key(factor.at[i, 'industry']):
            dict_industry[factor.at[i, 'industry']][0] += 1
            dict_industry[factor.at[i, 'industry']][1] += factor.at[i, 'factor_value']
    #计算行业因子值的平均数
    for key in dict_industry.keys():
        dict_industry[key][2] = dict_industry[key][1] / dict_industry[key][0]

    key = []
    value = []
    for k, v in dict_industry.iteritems():
        key.append(k)
        value.append(v[2])
    #dataframe为行业名，行业因子平均数
    industry_type = pd.DataFrame({'industry': key, 'industry_mean': value})
    factor = factor.reindex(
        columns=['stock_code', 'industry', 'close', 'close_next', 'return_ratio', 'factor_value', 'factor_new'])
    #将行业因子平均数并入到factor中
    factor = pd.merge(factor, industry_type, how='left', on=['industry'])
    #计算行业中性化后的因子值
    factor['factor_new'] = factor['factor_value'] - factor['industry_mean']
    del factor['factor_value']
    del factor['industry_mean']
    #del factor['industry']
    factor = factor.rename(columns={'factor_new': 'factor_value'})
    factor = factor.sort_values(by='factor_value', axis=0, ascending=False)
    factor.index = range(len(factor))
    return factor


if __name__ == '__main__':
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')