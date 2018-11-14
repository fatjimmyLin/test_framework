# -*- coding: utf-8 -*-
# 制作NINDS(分析师过去一年报告中覆盖的行业个数)
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    # path_original = '/Users/fatjimmy/Desktop/con_expect_data/CMB_REPORT_RESEARCH/'
    path_original = '/Users/fatjimmy/Desktop/CMB_REPORT_RESEARCH_process/'
    path_researcher_list = '/Users/fatjimmy/Desktop/con_expect_data/RESEARCHER_INFO.csv'
    path_researcher = '/Users/fatjimmy/Desktop/researchers.csv'

    name_list = os.listdir(path_original)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_original, f))]
    del onlyfiles[0]
    print(onlyfiles)

    #将researchers和RESEARCH_INFO中的信息拼接一下
    researcher = pd.DataFrame(
        pd.read_csv(path_researcher, error_bad_lines=False, encoding='gbk'))

    researcher_list = researcher['USER_NAME'].tolist()
    organization_list = researcher['ORG_ID'].tolist()

    # dict_res = {'研究员姓名': [过去一年报告覆盖的行业数量,过去一年报告覆盖的行业代码]}
    dict_res = {}

    for i in range(len(researcher_list)):
        str1 = researcher_list[i]+str(organization_list[i]) #key 使用姓名加公司ID更严谨，防止重名
        dict_res[str1] = [0,[]] #value

    # onlyfiles中为过去一年的研报基础数据
    for index in range(0,len(onlyfiles)):
    #for index in range(0, 1):
        print(onlyfiles[index])
        data = pd.DataFrame(
            pd.read_csv(path_original + onlyfiles[index], error_bad_lines=False, encoding='GBK'))
        #有些股票代码错了
        data = data.dropna(axis=0, how='any')
        data.industry_citiccode = data.industry_citiccode.astype(int)
        data.index = range(len(data))
        for k in range(len(data)):
            author_list = data.at[k, 'AUTHOR'].split(',')
            # 将未出现过的行业代码加入到字典中
            for j in range(len(author_list)):
                if (dict_res.__contains__(author_list[j]+str(data.at[k, 'ORGAN_ID']))): #使用姓名加公司ID作为key
                    if(data.at[k, 'industry_citiccode'] not in dict_res[author_list[j]+str(data.at[k, 'ORGAN_ID'])][1]):
                        dict_res[author_list[j]+str(data.at[k, 'ORGAN_ID'])][1].append(data.at[k, 'industry_citiccode'])
    #
    #统计研究员研究的行业出现次数
    for i in range(len(researcher_list)):
        dict_res[researcher_list[i]+str(organization_list[i])][0] = len(dict_res[researcher_list[i]+str(organization_list[i])][1])
    #将字典转为dataframe，结构为（研究员姓名,过去一年看过的行业代号）
    df_dict = pd.DataFrame.from_dict(dict_res, orient='index')
    del df_dict[0]
    df_dict['NAME+ORG_ID'] = df_dict.index
    df_dict = df_dict.rename(columns={1: 'num_industry'})
    df_dict = df_dict.reindex(
        columns=['NAME+ORG_ID','num_industry'])
    df_dict.index = range(len(df_dict))

    researcher = pd.concat([researcher, df_dict], axis=1)
    del researcher['NAME+ORG_ID']

    #给研究员加ID号
    researcher_info = pd.DataFrame(
        pd.read_csv(path_researcher_list, error_bad_lines=False, encoding='UTF-8',
                    usecols=['USER_ID','USER_NAME','ORG_ID','Y1','Y2']))
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

    #合并ID相同，但是ORG_ID不同的研究员，他跳槽了,但是研究的行业数需要合并
    combine_ID = []
    for i in range(len(researcher)):
        if ([researcher.at[i, 'USER_ID']] not in combine_ID):
            combine_ID.append(researcher.at[i, 'USER_ID'])
        elif(researcher.at[i, 'USER_ID'] != 0):
            # 确定出现两个相同ID人的位置
            same_id = researcher[(researcher.USER_ID == researcher.at[i, 'USER_ID'])].index.tolist()
            # 合并ID相同的人的行业list
            for j in range(len(researcher.at[same_id[1], 'num_industry'])):
                if(researcher.at[same_id[1], 'num_industry'][j] not in researcher.at[same_id[0], 'num_industry']):
                    researcher.at[same_id[0], 'num_industry'].append(researcher.at[same_id[1], 'num_industry'][j])
            researcher.at[i, 'flag'] = 0
    researcher = researcher[researcher.flag == 1]
    researcher.index = range(len(researcher))
    # 将具体行业代码转为行业数
    for i in range(len(researcher)):
        researcher.at[i, 'num_industry'] = len(researcher.at[i, 'num_industry'])
    print(researcher)
    print(len(researcher))
    researcher[['USER_ID','USER_NAME','ORG_ID','num_industry']].to_csv(
        '/Users/fatjimmy/Desktop/NINDS.csv', encoding='gbk', index=False)



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')