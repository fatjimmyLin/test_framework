# -*- coding: utf-8 -*-
# 处理CMB_REPORT_RESEARCH表
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    path_original = '/Users/fatjimmy/Desktop/con_expect_data/CMB_REPORT_RESEARCH/'
    #path_original = '/Users/fatjimmy/Desktop/CMB_REPORT_RESEARCH/'
    path_output = '/Users/fatjimmy/Desktop/con_expect_data/CMB_REPORT_RESEARCH_process/'
    if not os.path.exists(path_output):
        os.makedirs(path_output)
    path_org_list = '/Users/fatjimmy/Desktop/con_expect_data/GG_ORG_LIST.csv'
    path_industry = '/Users/fatjimmy/Desktop/citic_industry.csv'
    name_list = os.listdir(path_original)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_original, f))]
    del onlyfiles[0]
    print(onlyfiles)

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

    # 获取中信一级行业信息
    industry = pd.DataFrame(
        pd.read_csv(path_industry, error_bad_lines=False, encoding='utf-8'))
    industry = industry.rename(columns={'Ticker': 'stock_code'})
    #print(industry)

    researchers = []
    organization = []

    #onlyfiles中为过去一年的研报基础数据
    for index in range(0,len(onlyfiles)):
    #for index in range(0, 1):
        print(onlyfiles[index])
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

            # 处理AUTHOR列
            if (dict_org.__contains__(data.at[k, 'ORGAN_ID'])):
                author_list = data.at[k, 'AUTHOR'].split(',')
                # 将未出现过的研究员姓名加入到字典中
                for j in range(len(author_list)):
                    if(author_list[j] not in dict_org[data.at[k, 'ORGAN_ID']][1]):
                        dict_org[data.at[k, 'ORGAN_ID']][1].append(author_list[j])
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

    df_researchers = pd.DataFrame(
        {'USER_NAME':researchers,
         'ORG_ID': organization,
         })
    print(df_researchers)
    df_researchers.to_csv('/Users/fatjimmy/Desktop/con_expect_data/researchers_for_HORIZON.csv', encoding='gbk', index=False)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')