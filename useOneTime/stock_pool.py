# -*- coding: utf-8 -*-
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math

def main():

    change_stock_pool()


def change_stock_pool():
    # 输入需要选择的股票池，沪深300：300;中证500：500;中证800：800
    stockpool_index = '800'
    factor_name = 'mcap'
    path_factor = '/Users/fatjimmy/Desktop/Quant/generated_factor/' + factor_name + '/factor_' + factor_name + '_bb/'
    path_factor_output = '/Users/fatjimmy/Desktop/Quant/generated_factor/'\
                         + factor_name + '/factor_' + factor_name + '_bb_sp_' + stockpool_index + '/'
    path_stock_pool = '/Users/fatjimmy/Desktop/Quant/stock_pool/' + stockpool_index + '/'
    onlyfiles = [f for f in listdir(path_factor) if
                 isfile(join(path_factor, f))]
    #del onlyfiles[0]
    print(onlyfiles)

    stock_pool_list = [f for f in listdir(path_stock_pool) if
                       isfile(join(path_stock_pool, f))]
    del stock_pool_list[0]
    stock_pool_xs = []
    for index in range(len(stock_pool_list)):
        stock_pool_xs.append(stock_pool_list[index][:-4])
    print(stock_pool_xs)

    for index in range(0, len(onlyfiles)):
    #for index in range(0, 1):
        date = onlyfiles[index][:-4]
        print(date)
        # 找对应时间段中的股票池
        flag = False
        for i in range(len(stock_pool_xs) - 1):
            if (date >= stock_pool_xs[i] and date < stock_pool_xs[i + 1]):
                flag = True
                print('index date =', stock_pool_xs[i])
                stock_pool = pd.DataFrame(pd.read_csv(path_stock_pool + stock_pool_xs[i] + '.csv',
                                                      error_bad_lines=False, encoding='gbk', usecols=['stockInfo1']))
                stock_pool = stock_pool.rename(columns={'stockInfo1': 'stock_code'})
                stock_pool['label'] = 1
                break
        # 对尾部值的处理
        if (flag == False):
            print('index date =', stock_pool_xs[i + 1])
            stock_pool = pd.DataFrame(pd.read_csv(path_stock_pool + stock_pool_xs[i + 1] + '.csv',
                                                  error_bad_lines=False, encoding='gbk', usecols=['stockInfo1']))
            stock_pool = stock_pool.rename(columns={'stockInfo1': 'stock_code'})
            stock_pool['label'] = 1

        factor = pd.DataFrame(pd.read_csv(path_factor + onlyfiles[index], error_bad_lines=False, encoding='gbk',
                                          usecols=['stock_code', 'industry', 'factor_value', 'close', 'close_next',
                                                   'return_ratio']))
        factor = pd.merge(factor, stock_pool, how='left', on=['stock_code'])
        # 去掉label为NAN的股票，非NAN为股票池中的股票
        factor = factor[factor.label == factor.label]
        factor = factor.sort_values(by='factor_value', axis=0, ascending=False)
        factor.index = range(len(factor))
        factor = factor[['stock_code', 'industry', 'factor_value', 'close', 'close_next', 'return_ratio']]
        factor.to_csv(path_factor_output + onlyfiles[index], encoding='gbk')

if __name__ == '__main__':
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')