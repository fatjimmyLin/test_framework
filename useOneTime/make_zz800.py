# -*- coding: utf-8 -*-
# 制作中证800的股票池
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math

def main():
    #输入需要选择的股票池，沪深300：300；中证500：500
    path_300 = '/Users/fatjimmy/Desktop/300/'
    path_500 = '/Users/fatjimmy/Desktop/500/'
    path_800 = '/Users/fatjimmy/Desktop/800/'
    onlyfiles = [f for f in listdir(path_300) if
                 isfile(join(path_300, f))]
    del onlyfiles[0]
    print(onlyfiles)

    for index in range(0, len(onlyfiles)):
    #for index in range(0, 1):
        print(onlyfiles[index])

        factor_300 = pd.DataFrame(pd.read_csv(path_300 + onlyfiles[index], error_bad_lines=False, encoding='gbk'))
        factor_500 = pd.DataFrame(pd.read_csv(path_500 + onlyfiles[index], error_bad_lines=False, encoding='gbk'))
        frames = [factor_300,factor_500]
        factor_800 = pd.concat(frames)
        factor_800 = factor_800.sort_values(by='stockInfo1', axis=0, ascending=True)
        factor_800.index = range(len(factor_800))
        factor_800.to_csv(path_800 + onlyfiles[index],encoding='gbk')
if __name__ == '__main__':
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')