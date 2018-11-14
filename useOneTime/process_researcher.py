# -*- coding: utf-8 -*-
# 给研究员加ID号并处理特殊情况
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    path_researcher_list = '/Users/fatjimmy/Desktop/con_expect_data/RESEARCHER_INFO.csv'
    path_researcher = '/Users/fatjimmy/Desktop/con_expect_data/researchers.csv'

    # 将researchers和RESEARCH_INFO中的信息拼接一下
    researcher = pd.DataFrame(
        pd.read_csv(path_researcher, error_bad_lines=False, encoding='gbk'))

    researcher_list = researcher['USER_NAME'].tolist()
    organization_list = researcher['ORG_ID'].tolist()

    # dict_res = {'研究员姓名': [过去一年报告覆盖的行业数量,过去一年报告覆盖的行业代码]}
    dict_res = {}

    for i in range(len(researcher_list)):
        str1 = researcher_list[i]+str(organization_list[i]) #key 使用姓名加公司ID更严谨，防止重名
        dict_res[str1] = [0,[]] #value

    # 给研究员加ID号
    researcher_info = pd.DataFrame(
        pd.read_csv(path_researcher_list, error_bad_lines=False, encoding='UTF-8',
                    usecols=['USER_ID','USER_NAME','ORG_ID','Y1','Y2']))
    # 用ORG_ID和USER_NAME来确定USER_ID
    researcher = pd.merge(researcher, researcher_info, how='left', on=['USER_NAME','ORG_ID'])
    researcher = researcher.fillna(0)
    researcher.USER_ID = researcher.USER_ID.astype(int)
    researcher.Y1 = researcher.Y1.astype(int)
    researcher.Y2 = researcher.Y2.astype(int)
    researcher = researcher.reindex(
        columns=['USER_ID','USER_NAME','ORG_ID','num_industry','Y1','Y2','flag'])
    # flag用于标记是否需要删除，0的时候删掉
    researcher['flag'] = 1

    # 去掉同一个人在同一家机构工作的情况，从A跳到B再跳回A，表中会记两次
    res_id = []
    for i in range(len(researcher)):
         if [researcher.at[i, 'USER_ID'],researcher.at[i, 'ORG_ID']] not in res_id:
             res_id.append([researcher.at[i, 'USER_ID'],researcher.at[i, 'ORG_ID']])
         elif researcher.at[i, 'USER_ID'] != 0:
         #else:
             #print(i,[researcher.at[i, 'USER_ID'],researcher.at[i, 'ORG_ID'],researcher.at[i, 'USER_NAME']])
             researcher.at[i, 'flag'] = 0
    researcher = researcher[researcher.flag == 1]
    researcher.index = range(len(researcher))

    # 删掉同一家机构有两个同名的情况，表中会记两次，选Y2=0，即还没离职的那个
    res_name = []
    for i in range(len(researcher)):
         if [researcher.at[i, 'USER_NAME'],researcher.at[i, 'ORG_ID']] not in res_name:
             res_name.append([researcher.at[i, 'USER_NAME'],researcher.at[i, 'ORG_ID']])
         #elif researcher.at[i, 'USER_ID'] != 0:
         else:
             # 恰好两个同名的人会排在一起，i和i-1的关系
             if(researcher.at[i, 'Y2'] != 0):
                researcher.at[i, 'flag'] = 0
             elif(researcher.at[i-1, 'Y2'] != 0):
                researcher.at[i-1, 'flag'] = 0
             # 都没离职就随便选一个了
             else:researcher.at[i, 'flag'] = 0
    researcher = researcher[researcher.flag == 1]
    researcher.index = range(len(researcher))

    researcher = researcher[researcher.USER_ID != 0]
    researcher.index = range(len(researcher))
    researcher[['USER_ID','USER_NAME','ORG_ID']].to_csv(
        '/Users/fatjimmy/Desktop/con_expect_data/researchers_for_HORIZON_withID.csv', encoding='gbk', index=False)



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')