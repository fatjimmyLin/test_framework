# -*- coding: utf-8 -*-
# 制作GEXP(研究员的工作经验，以工作的月数计算)
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    # path_original = '/Users/fatjimmy/Desktop/con_expect_data/CMB_REPORT_RESEARCH/'
    path_researcher = '/Users/fatjimmy/Desktop/NINDS.csv'
    path_researcher_info = '/Users/fatjimmy/Desktop/con_expect_data/RESEARCHER_INFO.csv'

    # 获取研究员ID，构建字典
    res = pd.DataFrame(
        pd.read_csv(path_researcher, error_bad_lines=False, encoding='GBK',
                    usecols=['USER_ID','USER_NAME']))
    resid_list = []
    for i in range(len(res)):
        if (res.at[i, 'USER_ID'] != 0):
            resid_list.append(res.at[i, 'USER_ID'])

    # dict_res = {'研究员ID': [[研究员入职年月的list],[研究员最早入职的年月]]}
    dict_res = {}
    for i in range(len(resid_list)):
        key = resid_list[i] #key
        dict_res[key] = [[],[]] #value

    res_info = pd.DataFrame(
        pd.read_csv(path_researcher_info, error_bad_lines=False, encoding='UTF-8',
                    usecols=['USER_ID', 'Y1','M1']))
    res_info.USER_ID = res_info.USER_ID.astype(int)
    res_info.Y1 = res_info.Y1.astype(int)
    res_info.M1 = res_info.M1.astype(int)
    # 将每个研究员的所有入职记录写入字典中
    for j in range(len(res_info)):
        if(dict_res.__contains__(res_info.at[j, 'USER_ID'])):
            dict_res[res_info.at[j, 'USER_ID']][0].append([res_info.at[j, 'Y1'],res_info.at[j, 'M1']])

    for key in dict_res.keys():
         #dict_res[key][0]是研究员入职年月的list
        max_year = dict_res[key][0][0] #取第一个日期的年份为max_year,e.g[[2009, 1], [2011, 10], [2013, 4], [2017, 5]]中取2009
        # 只入职了一次，那一次的时间就是最早入职年份
        if(len(dict_res[key][0])==1):
            dict_res[key][1] = max_year
        else:
            # 计算最早入职年份
            for k in range(1,len(dict_res[key][0])):
                year = max_year[0]
                month = max_year[1]
                if(dict_res[key][0][k][0] < year):
                    max_year = dict_res[key][0][k]
                elif(dict_res[key][0][k][0] == year):
                    if((dict_res[key][0][k][1] < month)):
                        max_year = dict_res[key][0][k]
            dict_res[key][1] = max_year

    df_dict_res = pd.DataFrame.from_dict(dict_res, orient='index')

    del df_dict_res[0]
    df_dict_res['USER_ID'] = df_dict_res.index
    df_dict_res = df_dict_res.rename(columns={1: 'ENTRY_YEAR'})
    df_dict_res = df_dict_res.reindex(
        columns=['USER_ID', 'ENTRY_YEAR','WORK_MONTH'])
    df_dict_res.index = range(len(df_dict_res))
    # 计算入职工作的月数
    for i in range(len(df_dict_res)):
        df_dict_res.at[i,'WORK_MONTH'] = (2018 - df_dict_res.at[i,'ENTRY_YEAR'][0]) * 12 +\
                                         (10 - df_dict_res.at[i,'ENTRY_YEAR'][1])
    del df_dict_res['ENTRY_YEAR']
    res = pd.merge(res, df_dict_res,how='left', on=['USER_ID'])
    res = res.fillna(3) #没有USER_ID的统一按照入职三个月计算
    res.WORK_MONTH = res.WORK_MONTH.astype(int)
    res.to_csv('/Users/fatjimmy/Desktop/GEXP.csv', encoding='gbk', index=False)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')