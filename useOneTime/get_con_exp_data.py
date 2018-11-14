# -*- coding: utf-8 -*-
# 合并处理一致预期数据，选择有用的数据
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():

    path_raw_data = '/Users/fatjimmy/Desktop/basic_data/5year_con_exp_data/'
    path_data = '/Users/fatjimmy/Desktop/basic_data/5year_con_exp_data(22columns)/'
    if not os.path.exists(path_data):
        os.makedirs(path_data)

    list = ['factor_emotion/','factor_growth/','factor_profit/','factor_value/','stock_score_all/']
    # list = ['factor_growth/']

    name_list = os.listdir(path_raw_data + 'factor_scale/')
    name_list.sort()
    onlyfiles = [f for f in name_list if
                        isfile(join(path_raw_data + 'factor_scale/', f))]
    del onlyfiles[0]
    print(onlyfiles)
    # 获取需要的原始数据
    for index in range(0, len(onlyfiles)):
    # for index in range(0, 1):
       print(onlyfiles[index])
       data = pd.DataFrame(pd.read_csv(path_raw_data + 'factor_scale/'+ onlyfiles[index],
                                       error_bad_lines=False, encoding='gbk'))
       del data['Unnamed: 0']
       del data['Unnamed: 0.1']
       data = data.reindex(
           columns=['stock_code','con_na','con_na_rolling','mcap','na','ta','tcap'])
       for i in range(len(list)):
           #print(list[i])
           data_new = pd.DataFrame(pd.read_csv(path_raw_data + list[i] + onlyfiles[index],
                                       error_bad_lines=False, encoding='gbk'))
           del data_new['Unnamed: 0']
           del data_new['Unnamed: 0.1']
           data = pd.merge(data, data_new, how='left', on=['stock_code'])


       data = data.reindex(
           columns=['stock_code', 'con_na', 'con_target_price', 'market_confidence_75d', 'con_na_yoy',
                    'con_np_yoy', 'con_npcgrate_26w', 'con_or_yoy', 'con_roe_yoy1', 'con_roe_yoy2',
                    'con_eps', 'con_or', 'con_roe', 'con_pb', 'con_pb_order', 'con_pe', 'con_pe_order',
                    'con_peg', 'con_peg_order', 'con_ps', 'con_ps_order', 'score'])

       # print(data)
       data.to_csv(path_data + onlyfiles[index], encoding='gbk', index=False)
       # data.to_csv('/Users/fatjimmy/Desktop/1.csv', encoding='gbk', index=False)



if __name__ == '__main__':
    starttime = time.clock()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')