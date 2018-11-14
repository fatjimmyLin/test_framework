# -*- coding: utf-8 -*-
# 交付给占总用的程序
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    path_input = '/Users/fatjimmy/Desktop/Quant/generated_factor/' \
                  'technical_analysis/piece/lg(a)+lg(b)_process_plus2018/'
    path_output = '/Users/fatjimmy/Desktop/lg(con_na)+lg(con_pb)/'

    if not os.path.exists(path_output):
        os.makedirs(path_output)

    name_list = os.listdir(path_input)
    name_list.sort()
    onlyfiles_ta = [f for f in name_list if
                isfile(join(path_input, f))]
    #del onlyfiles_ta[0]

    for i in range(len(onlyfiles_ta)):
    # for i in range(0,1):
        print(onlyfiles_ta[i])
        factor = pd.DataFrame(
            pd.read_csv(path_input + onlyfiles_ta[i], error_bad_lines=False, encoding='gbk',
                        usecols=['stock_code', 'factor_value']))
        factor.to_csv(path_output + onlyfiles_ta[i],encoding='gbk',index=False)



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')