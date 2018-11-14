# -*- coding: utf-8 -*-
# 计算lg(con_na)*lg(con_pb)及其衍生因子
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    path_input = '/Users/fatjimmy/Desktop/data_within_sample/'
    path_factor = '/Users/fatjimmy/Desktop/Quant/generated_factor/technical_analysis/piece/lg(a)+lg(b)/'
    if not os.path.exists(path_factor):
        os.makedirs(path_factor)

    name_list = os.listdir(path_input)
    name_list.sort()
    onlyfiles_ta = [f for f in name_list if
                isfile(join(path_input, f))]
    # print(onlyfiles_ta)
    del onlyfiles_ta[0]

    for i in range(len(onlyfiles_ta)-1):
    # for i in range(0,1):
        print(onlyfiles_ta[i])
        factor = pd.DataFrame(
            pd.read_csv(path_input + onlyfiles_ta[i], error_bad_lines=False, encoding='gbk',
                        usecols=['stock_code', 'pre_close', 'open']))
        factor = factor.reindex(
            columns=['stock_code', 'pre_close', 'open','factor_value'])
        factor = factor[factor['pre_close'] != 0]
        factor = factor[factor['open'] != 0]
        factor['factor_value'] = np.log10(abs(factor['pre_close'])) + np.log10(abs(factor['open']))
        # factor['factor_value'] = np.log10(factor['con_pb'])
        # print(factor)
        factor[['stock_code', 'factor_value']].to_csv(path_factor + onlyfiles_ta[i+1],
                                                      encoding='gbk',index=False)
        #print(factor)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')