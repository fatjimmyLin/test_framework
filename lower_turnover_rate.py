# -*- coding: utf-8 -*-
# 降低换手率
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math

def main():
    path_factor_remake = '/Users/fatjimmy/Desktop//Quant/generated_factor/' \
                         'technical_analysis/piece/lg(a)+lg(b)_process/'
    path_factor_remake_lowerTR = '/Users/fatjimmy/Desktop//Quant/generated_factor/' \
                                 'technical_analysis/piece/lg(a)+lg(b)_process_lower/'

    name_list = os.listdir(path_factor_remake)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_factor_remake, f))]
    print(onlyfiles)
    del onlyfiles[0]

    if not os.path.exists(path_factor_remake_lowerTR):
        os.makedirs(path_factor_remake_lowerTR)

    for index in range(1, len(onlyfiles)):
    #for index in range(1, 2):
        if os.path.exists(path_factor_remake + onlyfiles[index]):
            print(onlyfiles[index])

            factor = pd.DataFrame(pd.read_csv(path_factor_remake + onlyfiles[index], error_bad_lines=False, encoding='gbk',
                            usecols=['stock_code', 'industry', 'factor_value', 'close',
                                     'close_next', 'return_ratio', 'amt',
                                     'turn', 'label_300', 'label_500', 'label_800']))
            factor = factor.dropna(axis=0, how='any')

            factor_pre = pd.DataFrame(pd.read_csv(path_factor_remake + onlyfiles[index - 1], error_bad_lines=False, encoding='gbk',
                            usecols=['stock_code', 'factor_value']))
            factor_pre = factor_pre.rename(columns={'factor_value': 'factor_value_pre'})
            factor_pre = factor_pre.dropna(axis=0, how='any')

            factor = pd.merge(factor, factor_pre, how='left', on=['stock_code'])


            factor['factor_value_new'] = 0.2 * factor['factor_value_pre'] + 0.8 * factor['factor_value']
            #前一天为空的用当天因子值填充
            factor['factor_value_new'] = factor['factor_value_new'].fillna(factor['factor_value'])
            factor['factor_value'] = factor['factor_value_new']

            factor = factor.sort_values(by='factor_value', axis=0, ascending=False)
            factor.index = range(len(factor))
            factor[['stock_code', 'industry', 'factor_value', 'close', 'close_next',
                    'return_ratio','amt', 'turn', 'label_300', 'label_500', 'label_800']].to_csv(
                path_factor_remake_lowerTR + onlyfiles[index], encoding='gbk')

if __name__ == '__main__':
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')