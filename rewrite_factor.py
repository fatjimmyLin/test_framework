# -*- coding: utf-8 -*-
# 重新改写一些因子
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math

def main():
    factor_name = 'mcap'
    path_factor = '/Users/fatjimmy/Desktop/Quant/factor_data/t_factor_value_all/'
    path_factor_original = '/Users/fatjimmy/Desktop/Quant/generated_factor/'+factor_name+'/factor_'+factor_name+'_original/'
    path_factor_remake = '/Users/fatjimmy/Desktop/Quant/generated_factor/'+factor_name+'/factor_'+factor_name+'_remake/'
    #onlyfiles = [f for f in listdir(path_factor) if
    #             isfile(join(path_factor, f))]
    #del onlyfiles[0]

    #生成原始因子数据
    #for index in range(len(onlyfiles)):
    #    print(onlyfiles[index])
    #    factor_or = pd.DataFrame(pd.read_csv(path_factor + onlyfiles[index], error_bad_lines=False, encoding='gbk',
    #                                  usecols=['stock_code', factor_name]))
    #    factor_or = factor_or.rename(columns={factor_name: 'factor_value'})
    #    factor_or = factor_or[['stock_code', 'factor_value']]
    #    factor_or.to_csv(path_factor_original + onlyfiles[index], encoding='gbk')


    onlyfiles = [f for f in listdir(path_factor_original) if
                 isfile(join(path_factor_original, f))]
    del onlyfiles[0]


    # 因子改写--布林带
    for index in range(10, len(onlyfiles)):
    #for index in range(10, 11):
        if os.path.exists(path_factor_original + onlyfiles[index - 1]):
            print(onlyfiles[index])


            factor = pd.DataFrame(
                pd.read_csv(path_factor_original + onlyfiles[index-1], error_bad_lines=False, encoding='gbk',
                            usecols=['stock_code', 'factor_value']))
            factor = factor[['stock_code', 'factor_value']]

            # 取前10天的因子值,列名为[''1','2'...'10']
            j = 0
            for i in range(index - 10, index-1):
                pre_factor = pd.DataFrame(
                    pd.read_csv(path_factor_original + onlyfiles[i], error_bad_lines=False, encoding='gbk',
                                usecols=['stock_code', 'factor_value']))
                factor = pd.merge(factor, pre_factor, how='left', on=['stock_code'])
                factor = factor.rename(columns={'factor_value_x': str(j)})
                factor = factor.rename(columns={'factor_value_y': str(j + 1)})
                factor = factor.rename(columns={'factor_value': str(j + 1)})
                j = j + 1
            factor = factor.rename(columns={'0': '10'})
            factor = factor.dropna(axis=0, how='any')

            # df用于计算max,min,mean等衍生数据
            df = pd.DataFrame(columns=['stock_code'])
            df['stock_code'] = factor['stock_code']

            # 计算day天的移动平均数
            day = 10
            alpha = 2 / (1 + day)
            factor['moving_average'] = 0
            xs = 0
            for i in range(1, day+1):
                # 指数移动平均
                # factor['moving_average'] += factor[str(i)] * (1 - alpha) ** (day - i)
                # xs += (1 - alpha) ** (i - 1)
                # 加权移动平均
                factor['moving_average'] += i * factor[str(i)]
                xs += i
            factor['moving_average'] = factor['moving_average'] / xs
            df['moving_average'] = factor['moving_average']
            factor['average_gap'] = (factor['10'] - factor['moving_average'])
            df['factor_value_ma'] = factor['average_gap']

            # 删除两列数据以免影响衍生数据的计算
            del factor['moving_average']
            del factor['average_gap']

            #衍生数据
            df['max'] = factor.max(1)
            df['min'] = factor.min(1)
            df['std'] = factor.std(1)
            df['mad'] = factor.mad(1)
            df['mean'] = factor.mean(1)
            df['skew'] = factor.skew(1)
            df['kurt'] = factor.kurt(1)
            #df['factor_value_ma'] = df['factor_value_ma'] / df['mad']
            df['factor_value_bb'] = (df['max'] - df['min']) / (df['mean'])
            df['factor_value'] = df['factor_value_ma'] * df['factor_value_bb']

            factor_name = 'factor_value_bb'
            df = df.sort_values(by=factor_name, axis=0, ascending=False)
            df.index = range(len(df))
            df = df[['stock_code',factor_name]]
            df = df.rename(columns={factor_name: 'factor_value'})

            df.to_csv(path_factor_remake + onlyfiles[index], encoding='gbk', index=False)



"""

    #因子改写--抛物线SAR
    for index in range(1,len(onlyfiles)):
    #for index in range(1, 2):
        if os.path.exists(path_factor_original + onlyfiles[index]):
            print(onlyfiles[index])
            factor = pd.DataFrame(pd.read_csv(path_factor_original + onlyfiles[index], error_bad_lines=False, encoding='gbk',
                                          usecols=['stock_code', 'factor_value']))
            factor = factor[['stock_code', 'factor_value']]
            factor = factor.dropna(axis=0, how='any')
            factor_pre = pd.DataFrame(pd.read_csv(path_factor_original + onlyfiles[index-1], error_bad_lines=False, encoding='gbk',
                                          usecols=['stock_code', 'factor_value']))
            factor_pre = factor_pre[['stock_code', 'factor_value']]
            factor_pre = factor_pre.dropna(axis=0, how='any')
            factor = pd.merge(factor, factor_pre, how='left', on=['stock_code'])
            factor = pd.merge(factor, mad, how='left', on=['stock_code'])
            factor = factor.rename(columns={'factor_value_x': 'factor_value'})
            factor = factor.rename(columns={'factor_value_y': 'factor_value_pre'})
            #print(factor)
            factor['factor_value'] = factor['factor_value_pre'] - 0.5*(factor['factor_value']-factor['factor_value_pre'])
            factor = factor.sort_values(by='factor_value', axis=0, ascending=False)
            factor.index = range(len(factor))
            #print(factor)
            factor[['stock_code', 'factor_value']].to_csv(path_factor_remake + onlyfiles[index])
            
     
"""


if __name__ == '__main__':
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')