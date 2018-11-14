# -*- coding: utf-8 -*-
# 制作HORIZON(报告的时间维度) base day是2000.01.01
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
from copy import deepcopy
import time
import math


def main():
    path_original = '/Users/fatjimmy/Desktop/con_expect_data/CMB_REPORT_RESEARCH_process/'
    path_researcher = '/Users/fatjimmy/Desktop/con_expect_data/researchers_for_HORIZON_withID.csv'

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
        print(onlyfiles[index])
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
    df.to_csv('/Users/fatjimmy/Desktop/1.csv', encoding='gbk', index=False)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')