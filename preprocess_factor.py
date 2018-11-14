# -*- coding: utf-8 -*-
# 批量对因子预处理
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import time
import numpy as np
from sklearn import linear_model
import warnings
import matplotlib.pyplot as plt


def main():

    path_price = '/Users/fatjimmy/Desktop/Quant/price_data/stock_delete60_tcap/'
    path_stock_pool = '/Users/fatjimmy/Desktop/Quant/stock_pool/800/'

    name_list = os.listdir(path_price)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_price, f))]
    #del onlyfiles[0]
    #print(onlyfiles)

    #股票池文件处理, e.g:['20120105', '20120703', '20130107',]代表不同阶段的300，500，800股票名单
    name_list_a = os.listdir(path_stock_pool)
    name_list_a.sort()
    stock_pool_list = [f for f in name_list_a if
                       isfile(join(path_stock_pool, f))]
    del stock_pool_list[0]
    print(stock_pool_list)
    stock_pool_xs = []
    for index in range(len(stock_pool_list)):
        stock_pool_xs.append(stock_pool_list[index][:-4])
    print(stock_pool_xs)

    data_column = pd.DataFrame(pd.read_csv('/Users/fatjimmy/Desktop/data_within_sample/20160104.csv',
                                           error_bad_lines=False, encoding='gbk', ))
    del data_column['stock_code']
    del data_column['industry']
    column_list = data_column.columns.values.tolist()

    # column_list = ['pre_close', 'open', 'close', 'high', 'low', 'vwap', 'chg', 'pct_chg', 'turn', 'free_turn',
    #                'volume', 'amt', 'dealnum', 'swing', 're_ipo_chg', 'rel_ipo_pct_chg']
    # column_list = ['pre_close', 'open', 'close']

# change
    path = '/Users/fatjimmy/Desktop/Quant/generated_factor/technical_analysis/after_preprocess/lg(a%b)+lg(b%a)/'
    if not os.path.exists(path):
        os.makedirs(path)

    count = 0
    starttime = time.clock()
    for i in range(len(column_list)):
        for j in range(i+1,len(column_list)):
            count += 1
            print(count)
            print(column_list[i], column_list[j])
            # 改下面两个路径即可
# change
            path_factor = '/Users/fatjimmy/Desktop/Quant/generated_factor/technical_analysis/original/' \
                          'lg(a%b)+lg(b%a)/' + column_list[i] + ' & ' + column_list[j] + '/'
# may change
            path_output = path + column_list[i] + ' & ' +column_list[j] + '/'
            if not os.path.exists(path_output):
                os.makedirs(path_output)

            for index in range(len(onlyfiles)-1):
            #for index in range(0,5):
                if os.path.exists(path_factor + onlyfiles[index]):
                    #print(onlyfiles[index])
                    factor = pd.DataFrame(pd.read_csv(path_factor + onlyfiles[index], error_bad_lines=False, encoding='gbk',
                                                      usecols=['stock_code', 'factor_value']))
                    factor = factor.dropna(axis=0, how='any')
                    # del factor['Unnamed: 0']
                    # 去掉factor_value为inf,-inf和nan的行
                    factor = factor[factor.replace([np.inf, -np.inf], np.nan).notnull().all(axis=1)]

                    # 读价格，成交量，行业数据。读取后一天价格数据包括['pre_close','close']
                    price = pd.DataFrame(pd.read_csv(path_price + onlyfiles[index+1], error_bad_lines=False, encoding='gbk'))[
                        ['stock_code', 'industry', 'pre_close', 'close']]
                    # 换手率，成交量和市值不能取下一天的数据,要取前一天数据
                    # 尤其是市值！会引入未来数据！
                    turn = pd.DataFrame(pd.read_csv(path_price + onlyfiles[index-1], error_bad_lines=False, encoding='gbk'))[
                        ['stock_code', 'turn', 'amt', 'ev']]

                    # 与因子值合并
                    factor = pd.merge(factor, price, how='left', on=['stock_code'])
                    factor = pd.merge(factor, turn, how='left', on=['stock_code'])

                    # 改名，改为当日视角的价格
                    factor = factor.rename(columns={'close': 'close_next'})
                    factor = factor.rename(columns={'pre_close': 'close'})


                    factor['return_ratio'] = (factor['close_next']-factor['close'])/factor['close']

                    factor = factor.sort_values(by='stock_code', axis=0, ascending=True)
                    factor.index = range(len(factor))

                    # 留下收益率绝对值小于10.03%的股票
                    factor = factor[abs(factor.return_ratio) < 0.1003]

                    # 去掉没有成交额的股票
                    factor = factor[factor.amt > 0]

                    # 因子去极值:百分位法
                    max = factor['factor_value'].quantile(0.975)
                    min = factor['factor_value'].quantile(0.025)
                    factor = factor[(factor.factor_value > min) & (factor.factor_value < max)]

                    # 去掉换手率大于n%的股票
                    #factor = factor[factor.turn < 1]
                    ##############################################################################################
                    # 因子标准化
                    factor = normalization(factor)
                    # 要更新一下索引，不然中性化会出bug
                    factor.index = range(len(factor))
                    # 行业中性化
                    factor = industry_neutralization(factor)
                    # 市值中性化
                    factor = market_value_neutralization(factor)
                    # 中性化后再对因子标准化
                    factor = normalization(factor)


                    # #绘制每一天的因子值分布图像
                    # factor['factor_value'].plot.hist(grid=True, bins=50, rwidth=0.9, color='#607c8e')
                    # plt.title('The skew of factor is '+ str("%.3f" % (factor['factor_value'].skew(0))))
                    # plt.xlabel('Counts')
                    # plt.ylabel('Commute Time')
                    # plt.grid(axis='y', alpha=0.75)
                    # plt.savefig("/Users/fatjimmy/Desktop/figure/" + onlyfiles[index][:-4] + ".png")
                    # plt.clf()


                    date = onlyfiles[index][:-4]
                    #标记是否为300，500，800股票池中的股票，1为是；0为否
                    index_list = ['300', '500', '800']
                    for index_name in index_list:
                        # 找对应时间段中的股票池
                        flag = False
                        for m in range(len(stock_pool_xs) - 1):
                            # 找该日期对应的index date
                            if (date >= stock_pool_xs[m] and date < stock_pool_xs[m + 1]):
                                flag = True
                                #print('index date =', stock_pool_xs[m])
                                stock_pool = pd.DataFrame(pd.read_csv('/Users/fatjimmy/Desktop/Quant/stock_pool/'+index_name+'/'
                                                                      + stock_pool_xs[m] + '.csv',
                                                                      error_bad_lines=False, encoding='gbk',usecols=['stockInfo1']))
                                stock_pool = stock_pool.rename(columns={'stockInfo1': 'stock_code'})
                                stock_pool['label_'+index_name] = 1
                                break
                        # 对尾部日期的处理
                        if (flag == False):
                            #print('index date =', stock_pool_xs[m + 1])
                            stock_pool = pd.DataFrame(pd.read_csv('/Users/fatjimmy/Desktop/Quant/stock_pool/'+index_name+'/'
                                                                  + stock_pool_xs[m + 1] + '.csv',
                                                                  error_bad_lines=False, encoding='gbk', usecols=['stockInfo1']))
                            stock_pool = stock_pool.rename(columns={'stockInfo1': 'stock_code'})
                            stock_pool['label_'+index_name] = 1
                        factor = pd.merge(factor, stock_pool, how='left', on=['stock_code'])
                        factor = factor.fillna(0)


                    factor = factor.reindex(
                        columns=['stock_code', 'industry', 'factor_value', 'close', 'close_next',
                                 'amt', 'return_ratio', 'turn', 'label_300', 'label_500', 'label_800'])
                    factor = factor.sort_values(by='factor_value', axis=0, ascending=False)
                    factor.index = range(len(factor))
                    factor.to_csv(path_output + onlyfiles[index], encoding='gbk')
            endtime = time.clock()
            print('FactorGet' + str(count) + ': ' + str(endtime - starttime) + ' sec')

#因子标准化
def normalization(factor):
    factor_mean = factor['factor_value'].mean()
    sigma = factor['factor_value'].std()
    #有新列的时候这样写会更快
    factor = factor.reindex(
        columns=['stock_code', 'industry', 'factor_value', 'close', 'close_next',
                 'amt', 'return_ratio', 'ev', 'turn', 'neutralized_factor'])
    factor['neutralized_factor'] = (factor['factor_value']-factor_mean)/sigma
    del factor['factor_value']
    factor = factor.rename(columns={'neutralized_factor':'factor_value'})
    return factor

#行业中性化
def industry_neutralization(factor):
    industries = []
    #统计所有的industry类别
    for i in range(len(factor)):
        if factor.at[i, 'industry'] not in industries:
            industries.append(factor.at[i, 'industry'])
    #字典的key是industry类别，value是[该行业公司出现的次数，该行业公司因子值的总和，该行业因子值的平均数]
    dict_industry = {}
    for i in range(len(industries)):
        str1 = industries[i]
        dict_industry[str1] = [0, 0, 0]
    #计算该行业公司出现的次数，该行业公司因子值的总和
    for i in range(len(factor)):
        if dict_industry.__contains__(factor.at[i, 'industry']):
            dict_industry[factor.at[i, 'industry']][0] += 1
            dict_industry[factor.at[i, 'industry']][1] += factor.at[i, 'factor_value']
    #计算行业因子值的平均数
    for key in dict_industry.keys():
        dict_industry[key][2] = dict_industry[key][1] / dict_industry[key][0]
    key = []
    value = []
    for k, v in dict_industry.items():
        key.append(k)
        value.append(v[2])
    #dataframe为行业名，行业因子平均数
    industry_type = pd.DataFrame({'industry': key, 'industry_mean': value})
    factor = factor.reindex(
        columns=['stock_code', 'industry', 'close', 'close_next', 'return_ratio',
                 'factor_value', 'amt', 'factor_new', 'ev', 'turn'])
    #将行业因子平均数并入到factor中
    factor = pd.merge(factor, industry_type, how='left', on=['industry'])
    #计算行业中性化后的因子值
    factor['factor_new'] = factor['factor_value'] - factor['industry_mean']
    del factor['factor_value']
    del factor['industry_mean']
    factor = factor.rename(columns={'factor_new': 'factor_value'})
    factor = factor.sort_values(by='factor_value', axis=0, ascending=False)
    return factor

#市值中性化
def market_value_neutralization(factor):
    regr = linear_model.LinearRegression()
    x = np.array(np.log(factor['ev']), dtype=pd.Series)
    y = np.array(factor['factor_value'], dtype=pd.Series)
    regr.fit(x.reshape(-1, 1),y.reshape(-1, 1))
    a, b = regr.coef_, regr.intercept_
    factor['factor_value'] = factor['factor_value']-float(a)*np.log(factor['ev']) - float(b)
    return factor

if __name__ == '__main__':
    starttime = time.clock()

    warnings.filterwarnings(action="ignore", module="sklearn", message="^internal gelsd")

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')