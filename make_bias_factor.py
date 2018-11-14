# -*- coding: utf-8 -*-
# 制作乖离率因子
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    path_factor_original = '/Users/fatjimmy/Desktop/Quant/price_data/stock_delete60_tcap/'
    path_factor_output = '/Users/fatjimmy/Desktop/Quant/generated_factor/price/remake_test/'

    if not os.path.exists(path_factor_output):
        os.makedirs(path_factor_output)

    name_list = os.listdir(path_factor_original)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_factor_original, f))]
    del onlyfiles[0]

    day = 60
    for index in range(977,len(onlyfiles)-1):
    #for index in range(day, day + 1):
        print(onlyfiles[index])
        factor = pd.DataFrame(
            pd.read_csv(path_factor_original + onlyfiles[index], error_bad_lines=False, encoding='gbk',
                        usecols=['stock_code', 'close']))
        j = 0
        first_factor = pd.DataFrame(
            pd.read_csv(path_factor_original + onlyfiles[index - day], error_bad_lines=False, encoding='gbk',
                        usecols=['stock_code', 'close']))
        for i in range(index-day+1,index):
            #print(onlyfiles[i])
            pre_factor = pd.DataFrame(
                pd.read_csv(path_factor_original + onlyfiles[i], error_bad_lines=False, encoding='gbk',
                            usecols=['stock_code', 'close']))
            first_factor = pd.merge(first_factor, pre_factor, how='left', on=['stock_code'])
            first_factor = first_factor.rename(columns={'close_x': str(j)})
            first_factor = first_factor.rename(columns={'close_y': str(j + 1)})
            first_factor = first_factor.rename(columns={'close': str(j + 1)})
            j = j + 1
        first_factor['means'] = first_factor.mean(1)

        factor_5days = pd.DataFrame(
            pd.read_csv(path_factor_original + onlyfiles[index - 5], error_bad_lines=False, encoding='gbk',
                        usecols=['stock_code', 'close']))
        factor_5days = factor_5days.rename(columns={'close': 'close_5days'})

        factor = pd.merge(factor, first_factor[['stock_code','means']], how='left', on=['stock_code'])
        factor = pd.merge(factor, factor_5days, how='left', on=['stock_code'])

        # 五日反转的计算
        factor['close_5days'] = (factor['close'] - factor['close_5days']) / (factor['close_5days'])
        factor['close'] = factor['close']/factor['means'] - 1
        factor = factor.rename(columns={'close': 'factor_value'})
        factor['factor_value'] = factor['factor_value'] + factor['close_5days']
        #factor['factor_value'] = factor['close_5days']
        factor = factor[['stock_code', 'factor_value']]
        factor = factor.dropna(axis=0, how='any')
        # 日期往后推一天，避免使用未来数据
        factor.to_csv(path_factor_output + onlyfiles[index+1], encoding='gbk', index=False)

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')