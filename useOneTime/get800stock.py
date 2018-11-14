# -*- coding: utf-8 -*-
# 留下中证800的股票
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():

    path_data = '/Users/fatjimmy/Desktop/data_new/'
    path_output = '/Users/fatjimmy/Desktop/data_technical_analysis_800/'
    if not os.path.exists(path_output):
        os.makedirs(path_output)

    path_stock_pool = '/Users/fatjimmy/Desktop/Quant/stock_pool/800/'
    stock_pool_list = [f for f in listdir(path_stock_pool) if
                       isfile(join(path_stock_pool, f))]
    del stock_pool_list[0]

    stock_pool_xs = []
    for index in range(len(stock_pool_list)):
        stock_pool_xs.append(stock_pool_list[index][:-4])
    print(stock_pool_xs)

    onlyfiles = [f for f in listdir(path_data) if
                isfile(join(path_data, f))]
    del onlyfiles[0]

    # 生成原始因子数据
    for index in range(0,len(onlyfiles)):
    #for index in range(0, 1):
       print(onlyfiles[index])
       data = pd.DataFrame(pd.read_csv(path_data + onlyfiles[index], error_bad_lines=False, encoding='gbk'))
       data.drop(['name', 'trade_status','susp_reason','lastradedays_s','last_trade_day',
                  'maxupordown','adjfactor','share_totala','total_shares','free_float_shares',
                  'float_a_shares'], axis=1, inplace=True)
       data = data[data.volume > 0]
       data.index = range(len(data))
       data = data.rename(columns={'code': 'stock_code'})

       date = onlyfiles[index][:-4]
       flag = False
       for i in range(len(stock_pool_xs) - 1):
           if (date >= stock_pool_xs[i] and date < stock_pool_xs[i + 1]):
               flag = True
               print('index date =', stock_pool_xs[i])
               stock_pool = pd.DataFrame(pd.read_csv('/Users/fatjimmy/Desktop/Quant/stock_pool/800/'
                                                     + stock_pool_xs[i] + '.csv',
                                                     error_bad_lines=False, encoding='gbk', usecols=['stockInfo1']))
               stock_pool = stock_pool.rename(columns={'stockInfo1': 'stock_code'})
               stock_pool['label_800'] = 1
               break
       # 对尾部日期的处理
       if (flag == False):
           print('index date =', stock_pool_xs[i + 1])
           stock_pool = pd.DataFrame(pd.read_csv('/Users/fatjimmy/Desktop/Quant/stock_pool/800/'
                                                 + stock_pool_xs[i] + '.csv',
                                                 error_bad_lines=False, encoding='gbk', usecols=['stockInfo1']))
           stock_pool = stock_pool.rename(columns={'stockInfo1': 'stock_code'})
           stock_pool['label_800'] = 1

       data = pd.merge(data, stock_pool, how='left', on=['stock_code'])
       #data = data.fillna(0)
       data = data[data.label_800 == 1]
       del data['label_800']

       data.to_csv(path_output + onlyfiles[index], encoding='gbk', index=False)



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')