# -*- coding: utf-8 -*-
# 制作ACC(分析师对上一年度做出的所有盈利预测相对准确性的平均)
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    path_researcher = '/Users/fatjimmy/Desktop/NINDS.csv'
    path_researcher_pj = '/Users/fatjimmy/Desktop/con_expect_data/AUTHOR_PJ/2017.csv'

    res = pd.DataFrame(
        pd.read_csv(path_researcher, error_bad_lines=False, encoding='GBK',
                    usecols=['USER_ID','USER_NAME']))
    # 删掉没有USER_ID的研究员
    res = res[res.USER_ID != 0]
    res.index = range(len(res))

    res_pj = pd.DataFrame(
        pd.read_csv(path_researcher_pj, error_bad_lines=False, encoding='UTF-8',
                    usecols=['AUTHOR_ID', 'A3', 'A6', 'A12']))
    res_pj.AUTHOR_ID = res_pj.AUTHOR_ID.astype(int)
    res_pj = res_pj.reindex(
        columns=['AUTHOR_ID', 'A3', 'A6', 'A12','A_MEANS','flag'])
    res_pj['flag'] = 1
    for i in range(len(res_pj)):
        # A3为NAN意味着A6,A12都是NAN，删除
        if(res_pj.at[i,'A3'] != res_pj.at[i,'A3']):
            res_pj.at[i, 'flag'] = 0

    res_pj = res_pj[res_pj.flag == 1]
    res_pj = res_pj[res_pj.AUTHOR_ID != 0]
    res_pj = res_pj.rename(columns={'AUTHOR_ID': 'USER_ID'})
    res_pj.index = range(len(res_pj))

    res = pd.merge(res, res_pj,how='left', on=['USER_ID'])
    res = res[res.flag == 1.0]
    res.index = range(len(res))
    # 计算A_MEANS,需要将A3,A6,A12取绝对值处理，否则-1，1的平均结果就是0,0代表没有误差
    res_name_list = []
    for i in range(len(res)):
        if(res.at[i,'USER_ID'] not in res_name_list):
            res_name_list.append(res.at[i,'USER_ID'])
        if(res.at[i,'A6'] != res.at[i,'A6']):
            res.at[i,'A_MEANS'] = abs(res.at[i,'A3'])
        else:
            if(res.at[i,'A12'] != res.at[i,'A12']):
                res.at[i, 'A_MEANS'] = (abs(res.at[i,'A3']) + abs(res.at[i,'A6']))/2
            else:
                res.at[i, 'A_MEANS'] = (abs(res.at[i,'A3']) + abs(res.at[i,'A6']) + abs(res.at[i,'A12'])) / 3
    res = res[['USER_ID', 'USER_NAME', 'A_MEANS']]

    # dict_res = {'研究员ID': A_MEANS的个数，A_MEANS的总数}
    dict_res = {}
    for i in range(len(res_name_list)):
        key = res_name_list[i] #key
        dict_res[key] = [0,0] #value

    # 统计研究员过去一年盈利预测的均值
    for i in range(len(res)):
        if (dict_res.__contains__(res.at[i, 'USER_ID'])):
            dict_res[res.at[i, 'USER_ID']][0] += 1
            dict_res[res.at[i, 'USER_ID']][1] += res.at[i, 'A_MEANS']

    df_dict_res = pd.DataFrame.from_dict(dict_res, orient='index')
    df_dict_res['USER_ID'] = df_dict_res.index
    df_dict_res = df_dict_res.rename(columns={1: 'ACC'})
    df_dict_res = df_dict_res.rename(columns={0: 'ACC_NUM'})
    df_dict_res = df_dict_res.reindex(
        columns=['USER_ID', 'ACC', 'ACC_NUM'])
    df_dict_res['ACC'] = df_dict_res['ACC']/df_dict_res['ACC_NUM']
    df_dict_res.index = range(len(df_dict_res))
    df_dict_res[['USER_ID','ACC']].to_csv('/Users/fatjimmy/Desktop/ACC.csv', encoding='gbk', index=False)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')