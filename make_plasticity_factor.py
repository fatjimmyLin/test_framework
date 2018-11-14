# -*- coding: utf-8 -*-
# 制作塑性因子
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():

    path =  '/Users/fatjimmy/Desktop/plasticity_factor/'

    path_raw_data = path + 'data_new_all/'
    path_data = path + 'data_plasticity/'
    path_data_process = path + 'data_plasticity_process/'
    path_factor = path + 'factor_plasticity/'

    if not os.path.exists(path_data):
        os.makedirs(path_data)
    if not os.path.exists(path_data_process):
        os.makedirs(path_data_process)
    if not os.path.exists(path_factor):
        os.makedirs(path_factor)



    # onlyfiles = [f for f in listdir(path_raw_data) if
    #             isfile(join(path_raw_data, f))]
    # del onlyfiles[0]
    # print(onlyfiles)
    # # 获取需要的原始数据
    # for index in range(0,len(onlyfiles)):
    #    print(onlyfiles[index])
    #    data = pd.DataFrame(pd.read_csv(path_raw_data + onlyfiles[index], error_bad_lines=False, encoding='gbk',
    #                                    usecols=['code', 'open', 'close', 'high', 'low',
    #                                             'volume', 'amt', 'free_float_shares', 'adjfactor']))
    #    data = data.reindex(
    #        columns=['code', 'open', 'close', 'high', 'low','volume', 'amt', 'free_float_shares', 'mean_price', 'adjfactor'])
    #    data['mean_price'] = (data['open'] + data['close'] + data['high'] + data['low'])/4
    #    data = data[data.volume > 0]
    #    data.index = range(len(data))
    #    data = data.rename(columns={'code': 'stock_code'})
    #    data.to_csv(path_data + onlyfiles[index], encoding='gbk', index=False)


    # #由原始数据计算需要的衍生数据，十天的Bk
    # onlyfiles_data = [f for f in listdir(path_data) if
    #              isfile(join(path_data, f))]
    # del onlyfiles_data[0]
    # #print(onlyfiles_data)
    # for index in range(9, len(onlyfiles_data)):
    # #for index in range(9, 10):
    #     print(onlyfiles_data[index])
    #     factor = pd.DataFrame(
    #         pd.read_csv(path_data + onlyfiles_data[index], error_bad_lines=False, encoding='gbk',))
    #
    #     # 取前10天的因子值,列名为[''1','2'...'10']
    #     j = 0
    #     for i in range(index - 9, index):
    #         #print(onlyfiles_data[i])
    #         pre_factor = pd.DataFrame(
    #             pd.read_csv(path_data + onlyfiles_data[i], error_bad_lines=False, encoding='gbk',
    #                         usecols=['stock_code', 'mean_price']))
    #         factor = pd.merge(factor, pre_factor, how='left', on=['stock_code'])
    #         factor = factor.rename(columns={'mean_price_x': str(j)})
    #         factor = factor.rename(columns={'mean_price_y': str(j + 1)})
    #         factor = factor.rename(columns={'mean_price': str(j + 1)})
    #         j = j + 1
    #     factor = factor.rename(columns={'0': '10'})
    #     factor = factor.reindex(
    #            columns=['stock_code', 'open', 'close', 'high', 'low', 'volume', 'amt', 'free_float_shares',
    #                     '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'Bk', 'adjfactor'])
    #     factor['Bk'] = 0
    #     for k in range(1,11):
    #         factor['Bk'] += factor[str(k)]
    #     factor['Bk'] = factor['Bk']/10
    #     factor = factor.dropna(axis=0, how='any')
    #     factor.index = range(len(factor))
    #     factor = factor[['stock_code', 'volume', 'amt', 'free_float_shares', 'Bk', 'adjfactor']]
    #     #print(factor)
    #     factor.to_csv(path_data_process + onlyfiles_data[index], encoding='gbk', index=False)

    name_list = os.listdir(path_data_process)
    name_list.sort()
    onlyfiles_data_pro = [f for f in name_list if
                      isfile(join(path_data_process, f))]
    del onlyfiles_data_pro[0]
    print(onlyfiles_data_pro)

    # 计算VRBP,SPPI等数据
    for index in range(1, len(onlyfiles_data_pro)-1):
    # for index in range(1, 2):
        print(onlyfiles_data_pro[index])
        factor = pd.DataFrame(
            pd.read_csv(path_data_process + onlyfiles_data_pro[index], error_bad_lines=False, encoding='gbk',))
        factor_pre = pd.DataFrame(
            pd.read_csv(path_data_process + onlyfiles_data_pro[index-1], error_bad_lines=False, encoding='gbk',
                        usecols=['stock_code', 'Bk']))
        factor = pd.merge(factor, factor_pre, how='left', on=['stock_code'])
        factor = factor.rename(columns={'Bk_x': 'Bk'})
        factor = factor.rename(columns={'Bk_y': 'Bk-1'})
        factor = factor.reindex(columns=['stock_code', 'volume', 'amt', 'free_float_shares',
                                         'Bk', 'Bk-1', 'VRBP', 'SPPI', 'SPPI_xs', 'adjfactor', 'factor_value'])
        factor['amt'] = factor['amt'] * 1000
        factor['volume'] = factor['volume'] * 100
        # factor['Bk'] = factor['Bk'] / factor['adjfactor']
        # factor['Bk-1'] = factor['Bk-1'] / factor['adjfactor']
        factor['VRBP'] = (factor['Bk'] - factor['Bk-1']) / factor['Bk-1']
        factor['SPPI'] = (factor['amt'] - factor['Bk'] * factor['volume'])/(factor['free_float_shares'] * factor['Bk-1'])
        factor['SPPI_xs'] = np.sign(factor['SPPI']) * np.sqrt(abs(factor['SPPI']))
        #factor['factor_value'] = factor['VRBP'] / factor['SPPI_xs']
        factor['factor_value'] = factor['VRBP'] * (factor['amt'] - factor['Bk'] * factor['volume'])
        factor[['stock_code','factor_value']].to_csv(path_factor + onlyfiles_data_pro[index+1], encoding='gbk', index=False)
        #factor.to_csv('/Users/fatjimmy/Desktop/1.csv')


if __name__ == '__main__':
    starttime = time.clock()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')