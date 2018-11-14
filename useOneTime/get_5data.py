# -*- coding: utf-8 -*-
# 制作GEXP,ACC,NAUT,NINDS,HORIZON
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math
from copy import deepcopy


def process_CMB_REPORT_RESEARCH(input,output):
    path_original = input + 'CMB_REPORT_RESEARCH/'
    path_industry = input + 'citic_industry.csv'
    path_researcher_list = input + 'RESEARCHER_INFO.csv'
    path_org_list = input + 'GG_ORG_LIST.csv'

    path_output = output + 'CMB_REPORT_RESEARCH_process/'
    path_researcher_output = output + 'researchers_withID.csv'
    print('processing CMB_REPORT_RESEARCH...')

    if not os.path.exists(path_output):
        os.makedirs(path_output)
    name_list = os.listdir(path_original)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_original, f))]
    del onlyfiles[0]
    #print(onlyfiles)

    # 获取机构ID，构建字典
    org = pd.DataFrame(
        pd.read_csv(path_org_list, error_bad_lines=False, encoding='utf-8',
                    usecols=['ID']))
    org.ID = org.ID.astype(int)
    org_list = org['ID'].tolist()

    # dict_org = {'机构ID': [机构发研报研究员姓名]}
    dict_org = {}
    for i in range(len(org_list)):
        str1 = org_list[i] #key
        dict_org[str1] = [] #value

    # 获取中信一级行业信息
    industry = pd.DataFrame(
        pd.read_csv(path_industry, error_bad_lines=False, encoding='utf-8'))
    industry = industry.rename(columns={'Ticker': 'stock_code'})

    researchers = []
    organization = []

    #onlyfiles中为过去的研报基础数据
    for index in range(0,len(onlyfiles)):
        data = pd.DataFrame(
            pd.read_csv(path_original + onlyfiles[index], error_bad_lines=False, encoding='utf-8',
                        usecols=['ORIGIN_ID', 'CODE', 'TYPE_ID', 'ORGAN_ID', 'AUTHOR']))
        # 选取报告类型
        # 21:预测表数据 22:一般个股报告 23:深度报告 24:调研报告 25:点评报告 26:新股研究 27:简评文章 28:港股研究 98:会议纪要
        data = data[(data['TYPE_ID'] == 21) | (data['TYPE_ID'] == 22) | (data['TYPE_ID'] == 23) | (data['TYPE_ID'] == 24)]
        data = data[data['AUTHOR'] != '无']
        # 把'CODE'改成 000001.SZ的形式
        data= data.rename(columns={'CODE': 'stock_code'})
        data.index = range(len(data))
        # change column type from int to string
        data.stock_code = data.stock_code.astype(str)
        for k in range(len(data)):
            data.at[k, 'stock_code'] = str(data.at[k,'stock_code'])
            # 股票代码补零
            while (len(data.at[k,'stock_code']) < 6):
                data.at[k, 'stock_code'] = '0' + data.at[k, 'stock_code']
            if(data.at[k, 'stock_code'][0] == '6'):
                data.at[k, 'stock_code'] = data.at[k, 'stock_code'] + '.SH'
            else:
                data.at[k, 'stock_code'] = data.at[k, 'stock_code'] + '.SZ'

            if (dict_org.__contains__(data.at[k, 'ORGAN_ID'])):
                author_list = data.at[k, 'AUTHOR'].split(',')
                # 将未出现过的研究员姓名加入到字典中
                for j in range(len(author_list)):
                    if(author_list[j] not in dict_org[data.at[k, 'ORGAN_ID']]):
                        dict_org[data.at[k, 'ORGAN_ID']].append(author_list[j])
                        # researcher & organization中添加[研究员姓名，工作机构代码]
                        researchers.append(author_list[j])
                        organization.append(data.at[k, 'ORGAN_ID'])
        data = pd.merge(data, industry, how='left', on=['stock_code'])
        # 有些股票代码错了
        data = data.dropna(axis=0, how='any')
        data.industry_citiccode = data.industry_citiccode.astype(int)
        data = data.reindex(columns=['ORIGIN_ID', 'stock_code', 'industry_citiccode', 'TYPE_ID', 'ORGAN_ID', 'AUTHOR'])
        # print(data)
        data.to_csv(path_output + onlyfiles[index], encoding='gbk', index=False)

    # 处理研究员信息，删去重复冗余数据，生成<研究员ID,研究员姓名，研究员所在机构>表
    researcher = pd.DataFrame(
        {'USER_NAME':researchers,
         'ORG_ID': organization,
         })
    # 将researchers和RESEARCH_INFO中的信息拼接一下
    # 给研究员加ID号
    researcher_info = pd.DataFrame(
        pd.read_csv(path_researcher_list, error_bad_lines=False, encoding='UTF-8',
                    usecols=['USER_ID', 'USER_NAME', 'ORG_ID', 'Y1', 'Y2']))
    # 用ORG_ID和USER_NAME来确定USER_ID
    researcher = pd.merge(researcher, researcher_info, how='left', on=['USER_NAME', 'ORG_ID'])
    researcher = researcher.fillna(0)
    researcher.USER_ID = researcher.USER_ID.astype(int)
    researcher.Y1 = researcher.Y1.astype(int)
    researcher.Y2 = researcher.Y2.astype(int)
    researcher = researcher.reindex(
        columns=['USER_ID', 'USER_NAME', 'ORG_ID', 'num_industry', 'Y1', 'Y2', 'flag'])
    # flag用于标记是否需要删除，0的时候删掉
    researcher['flag'] = 1

    # 去掉同一个人在同一家机构工作的情况，从A跳到B再跳回A，表中会记两次
    res_id = []
    for i in range(len(researcher)):
        if [researcher.at[i, 'USER_ID'], researcher.at[i, 'ORG_ID']] not in res_id:
            res_id.append([researcher.at[i, 'USER_ID'], researcher.at[i, 'ORG_ID']])
        elif researcher.at[i, 'USER_ID'] != 0:
            # else:
            # print(i,[researcher.at[i, 'USER_ID'],researcher.at[i, 'ORG_ID'],researcher.at[i, 'USER_NAME']])
            researcher.at[i, 'flag'] = 0
    researcher = researcher[researcher.flag == 1]
    researcher.index = range(len(researcher))

    # 删掉同一家机构有两个同名的情况，表中会记两次，选Y2=0，即还没离职的那个
    res_name = []
    for i in range(len(researcher)):
        if [researcher.at[i, 'USER_NAME'], researcher.at[i, 'ORG_ID']] not in res_name:
            res_name.append([researcher.at[i, 'USER_NAME'], researcher.at[i, 'ORG_ID']])
        # elif researcher.at[i, 'USER_ID'] != 0:
        else:
            # 恰好两个同名的人会排在一起，i和i-1的关系
            if (researcher.at[i, 'Y2'] != 0):
                researcher.at[i, 'flag'] = 0
            elif (researcher.at[i - 1, 'Y2'] != 0):
                researcher.at[i - 1, 'flag'] = 0
            # 都没离职就随便选一个了
            else:
                researcher.at[i, 'flag'] = 0
    researcher = researcher[researcher.flag == 1]
    researcher.index = range(len(researcher))

    researcher = researcher[researcher.USER_ID != 0]
    researcher.index = range(len(researcher))
    researcher[['USER_ID', 'USER_NAME', 'ORG_ID']].to_csv(path_researcher_output, encoding='gbk', index=False)
    print('finished!')
    print('\n')


def get_NAUT(input,output):
    path_original = output + 'CMB_REPORT_RESEARCH_process/'
    path_org_list = input + 'GG_ORG_LIST.csv'

    # path_NAUT_output = output + 'NAUT.csv'
    # path_researcher_1year = output + 'researchers_1year.csv'
    path_NAUT_output = '/Users/fatjimmy/Desktop/NAUT.csv'
    path_researcher_1year = '/Users/fatjimmy/Desktop/researchers_1year.csv'

    name_list = os.listdir(path_original)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_original, f))]
    # del onlyfiles[0]
    # print(onlyfiles)
    print('processing NAUT...')

    # 获取机构ID，构建字典
    org = pd.DataFrame(
        pd.read_csv(path_org_list, error_bad_lines=False, encoding='utf-8',
                    usecols=['ID']))
    org.ID = org.ID.astype(int)
    org_list = org['ID'].tolist()

    # dict_org = {'机构ID': [过去一年发研报分析师的数量,机构过去一年发研报研究员姓名]}
    dict_org = {}
    for i in range(len(org_list)):
        str1 = org_list[i] #key
        dict_org[str1] = [0,[]] #value

    researchers = []
    organization = []

    #onlyfiles中为过去一年的研报基础数据
    for index in range(len(onlyfiles)-245,len(onlyfiles)):
    # for index in range(len(onlyfiles)-1,len(onlyfiles)):
        #print(onlyfiles[index])
        data = pd.DataFrame(
            pd.read_csv(path_original + onlyfiles[index], error_bad_lines=False, encoding='GBK',
                        usecols=['ORIGIN_ID', 'stock_code', 'industry_citiccode', 'TYPE_ID', 'ORGAN_ID', 'AUTHOR']))
        for k in range(len(data)):
            # 处理AUTHOR列
            if (dict_org.__contains__(data.at[k, 'ORGAN_ID'])):
                author_list = data.at[k, 'AUTHOR'].split(',')
                # 将未出现过的研究员姓名加入到字典中
                for j in range(len(author_list)):
                    if(author_list[j] not in dict_org[data.at[k, 'ORGAN_ID']][1]):
                        dict_org[data.at[k, 'ORGAN_ID']][1].append(author_list[j])
                        researchers.append(author_list[j])
                        organization.append(data.at[k, 'ORGAN_ID'])
    #统计研究员出现次数
    for i in range(len(org_list)):
        dict_org[org_list[i]][0] = len(dict_org[org_list[i]][1])
    #将字典转为dataframe，结构为（组织ID,过去一年发布过研报的研究员数量）
    df_dict = pd.DataFrame.from_dict(dict_org, orient='index')
    del df_dict[1]
    df_dict['ORG_ID'] = df_dict.index
    df_dict = df_dict.rename(columns={0: 'num_researcher'})
    df_dict = df_dict.reindex(
        columns=['ORG_ID','num_researcher'])
    df_dict.to_csv(path_NAUT_output, encoding='gbk', index=False)
    df_researchers = pd.DataFrame(
        {'USER_NAME': researchers,
         'ORG_ID': organization,
         })
    df_researchers.to_csv(path_researcher_1year, encoding='gbk', index=False)
    print('finished!')
    print('\n')


def get_NINDS(input,output):
    path_original = output + 'CMB_REPORT_RESEARCH_process/'
    path_researcher_list = input + 'RESEARCHER_INFO.csv'
    path_researcher = output + 'researchers_1year.csv'

    path_NINDS_output = output + 'NINDS.csv'

    name_list = os.listdir(path_original)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_original, f))]
    del onlyfiles[0]
    print('processing NINDS...')

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
    for index in range(len(onlyfiles)-245,len(onlyfiles)):
    #for index in range(0, 1):
        #print(onlyfiles[index])
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
    researcher[['USER_ID','USER_NAME','ORG_ID','num_industry']].to_csv(
        path_NINDS_output, encoding='gbk', index=False)
    print('finished!')
    print('\n')


def get_GEXP(input,output):
    path_researcher = output + 'NINDS.csv'
    path_researcher_info = input + 'RESEARCHER_INFO.csv'

    path_GEXP_output = output + 'GEXP.csv'
    print('processing GEXP...')

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
    res.to_csv(path_GEXP_output, encoding='gbk', index=False)
    print('finished!')
    print('\n')


def get_ACC(input,output):
    path_researcher = output + 'NINDS.csv'
    path_researcher_pj = input + 'AUTHOR_PJ/2017.csv'

    path_ACC_output = output + 'ACC.csv'

    print('processing ACC...')

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
    df_dict_res[['USER_ID','ACC']].to_csv(path_ACC_output, encoding='gbk', index=False)
    print('finished!')
    print('\n')


def get_HORIZON(output):
    path_original = output + 'CMB_REPORT_RESEARCH_process/'
    path_researcher = output + 'researchers_withID.csv'

    path_HORIZON_output = output + 'HORIZON.csv'

    print('processing HORIZON...')

    res = pd.DataFrame(
        pd.read_csv(path_researcher, error_bad_lines=False, encoding='GBK'))

    name_list = os.listdir(path_original)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_original, f))]
    del onlyfiles[0]
    df = pd.DataFrame()

    # 把author列的人（n个）拆分为n行，
    for index in range(0, len(onlyfiles)):
    #for index in range(len(onlyfiles)-2,len(onlyfiles)):
        #print(onlyfiles[index])
        data = pd.DataFrame(
            pd.read_csv(path_original + onlyfiles[index], error_bad_lines=False, encoding='GBK'))
        #print(data)
        information = []
        for k in range(len(data)):
            # 把每一列写进一个列表中
            information_list = []
            author_list = data.at[k, 'AUTHOR'].split(',')
            #print(author_list)
            information_list.append(data.at[k, 'ORIGIN_ID'])
            information_list.append(data.at[k, 'stock_code'])
            information_list.append(data.at[k, 'industry_citiccode'])
            information_list.append(data.at[k, 'TYPE_ID'])
            information_list.append(data.at[k, 'ORGAN_ID'])
            #print(information_list)
            for j in range(len(author_list)):
                information_list.append(author_list[j])
                # 此处要用深拷贝
                information_newlist = deepcopy(information_list)
                #print(information_newlist)
                information.append(information_newlist)
                del information_list[5]
        #print(information)
        ID = []; CODE = []; CITICCODE = []; TYPE = []; ORGAN_ID =[]; AUTHOR = []
        for i in range(len(information)):
            ID.append(information[i][0])
            CODE.append(information[i][1])
            CITICCODE.append(information[i][2])
            TYPE.append(information[i][3])
            ORGAN_ID.append(information[i][4])
            AUTHOR.append(information[i][5])
        data_new = pd.DataFrame(
            {'ORIGIN_ID': ID,
             'stock_code': CODE,
             'industry_citiccode': CITICCODE,
             'TYPE_ID': TYPE,
             'ORG_ID': ORGAN_ID,
             'USER_NAME': AUTHOR,
             })
        data_new = data_new.reindex(
            columns=['ORIGIN_ID','stock_code','industry_citiccode','TYPE_ID',
                     'ORG_ID','USER_NAME','DATE','HORIZON'])
        date = onlyfiles[index][:-4]
        data_new['DATE'] = date
        # 计算HORIZON,以2000.01.01为base day
        t1 = pd.to_datetime(date)
        t2 = pd.to_datetime('2000-01-01')
        day = (t1 - t2).days
        data_new['HORIZON'] = day

        #取USER_ID
        data_new = pd.merge(data_new, res,
                                how='left', on=['USER_NAME', 'ORG_ID'])
        #print(data_new)
        df = df.append(data_new)
    df = df.dropna(axis=0, how='any')
    df.USER_ID = df.USER_ID.astype(int)
    df.index = range(len(df))
    df.to_csv(path_HORIZON_output, encoding='gbk', index=False)
    print('finished!')
    print('\n')


def main():
    # 改一改输入输出路径，input_path为初始数据的文件夹
    path_input = '/Users/fatjimmy/Desktop/5data_input/'
    path_output = '/Users/fatjimmy/Desktop/5data_output/'
    if not os.path.exists(path_output):
        os.makedirs(path_output)

    #process_CMB_REPORT_RESEARCH(path_input,path_output) # 处理原始数据并城市研究员信息表researchers_withID.csv
    get_NAUT(path_input,path_output)  # 所在单位过去一年发布过报告的分析师数量
    # get_NINDS(path_input,path_output) # 分析师过去一年报告中覆盖的行业个数
    # get_GEXP(path_input,path_output)  # 分析师经验
    # get_ACC(path_input,path_output)  # 分析师对上一年度做出的所有盈利预测相对准确性的平均
    # get_HORIZON(path_output)  # 制作HORIZON(报告的时间维度) base day是2000.01.01


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')