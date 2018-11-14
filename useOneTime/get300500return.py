# -*- coding: utf-8 -*-
# 比较沪深300和中证500的指数相关关系
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    path_index_300 = '/Users/fatjimmy/Desktop/Quant/300.csv'
    path_index_500 = '/Users/fatjimmy/Desktop/Quant/500.csv'

    path_factor_output = '/Users/fatjimmy/Desktop/Quant/generated_factor/price/remake/'

    index_300 = pd.DataFrame(
        pd.read_csv(path_index_300, error_bad_lines=False, encoding='gbk',))
    index_500 = pd.DataFrame(
        pd.read_csv(path_index_500, error_bad_lines=False, encoding='gbk', ))
    index_300 = pd.merge(index_300, index_500, how='left', on=['dt'])
    index_300 = index_300.reindex(columns=['dt','000300.SH','000905.SH','return_300','return_500'])
    for i in range(22,len(index_300)):
        index_300.at[i,'return_300'] = (index_300.at[i,'000300.SH']-index_300.at[i-22,'000300.SH']) / \
                                       index_300.at[i-22,'000300.SH']
        index_300.at[i, 'return_500'] = (index_300.at[i, '000905.SH'] - index_300.at[i - 22, '000905.SH']) / \
                                        index_300.at[i - 22, '000905.SH']
    index_300 = index_300.dropna(axis=0, how='any')
    index_300['delta'] = index_300['return_300'] - index_300['return_500']
    index_300.index = range(len(index_300))
    print(index_300)
    print('\n')

    years = ['2012','2013','2014','2015','2016','2017']
    dict_years = {}
    for i in range(len(years)):
        str1 = years[i]
        dict_years[str1] = [0, 0, 0]

    for i in range(len(index_300)):
        #print(index_300.at[i, 'dt'][0:4])
        if(dict_years.__contains__(index_300.at[i, 'dt'][0:4])):
            dict_years[index_300.at[i, 'dt'][0:4]][0] += 1
            if (index_300.at[i, 'delta'] > 0.03):
                dict_years[index_300.at[i, 'dt'][0:4]][1] += 1
            if (index_300.at[i, 'delta'] < -0.03):
                dict_years[index_300.at[i, 'dt'][0:4]][2] += 1
    print(dict_years)
    print('\n')

    index_300.to_csv('~/Desktop/1.csv')



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')