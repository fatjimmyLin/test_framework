# -*- coding: utf-8 -*-
# 预处理占总的因子数据
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import time

def main():
    path_factor = '/Users/fatjimmy/Desktop/Quant/con_expect_data/20180819/'
    path_output = '/Users/fatjimmy/Desktop/zhan/'
    if not os.path.exists(path_output):
        os.makedirs(path_output)

    name_list = os.listdir(path_factor)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_factor, f))]
    del onlyfiles[0]
    # print(onlyfiles)

    for index in range(len(onlyfiles)):
    #for index in range(0, 1):
        print(onlyfiles[index])
        columns = ['stock_code', 'factor_value']
        factor = pd.DataFrame(
            pd.read_csv(path_factor + onlyfiles[index], error_bad_lines=False, encoding='gbk', header=None, names=columns))
        factor = factor.reindex(
            columns=['stock_code', 'factor_value'])
        #print(factor)
        factor.to_csv(path_output + onlyfiles[index] + '.csv', index=False)

if __name__ == '__main__':
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')