# -*- coding: utf-8 -*-
# 合并一致预期和量价数据
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():

    path_price_volume = '/Users/fatjimmy/Desktop/basic_data/5year_price_volume_data/'
    path_con_expect = '/Users/fatjimmy/Desktop/data_combine_plus2018/'

    path = '/Users/fatjimmy/Desktop/data_combine_all_plus2018/'
    if not os.path.exists(path):
        os.makedirs(path)

    name_list = os.listdir(path_con_expect)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                    isfile(join(path_con_expect, f))]
    #print(onlyfiles)
    #del onlyfiles[0]

    for index in range(len(onlyfiles)):
    # for index in range(0, 1):
        if os.path.exists(path_price_volume + onlyfiles[index]):
            factor_pv = pd.DataFrame(
                pd.read_csv(path_price_volume + onlyfiles[index], error_bad_lines=False, encoding='gbk'))[[
                'code', 'industry', 'pre_close', 'open', 'close', 'high', 'low', 'vwap', 'pct_chg',
                'turn', 'volume', 'amt', 'dealnum', 'swing', 'rel_ipo_pct_chg']]
            factor_pv = factor_pv.rename(columns={'code': 'stock_code'})
            factor_con = pd.DataFrame(
                pd.read_csv(path_con_expect + onlyfiles[index], error_bad_lines=False, encoding='gbk'))
            factor_pv = pd.merge(factor_pv, factor_con, how='left', on=['stock_code'])
            # print(factor_pv)
            factor_pv.to_csv(path + onlyfiles[index],encoding='gbk', index=False)




if __name__ == '__main__':
    starttime = time.clock()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')