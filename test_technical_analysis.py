# -*- coding: utf-8 -*-
# 从原始数据中根据算子生成一些新的因子
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import numpy as np
import time
import math


def main():
    path_data = '/Users/fatjimmy/Desktop/data_2years/'
    path_output = '/Users/fatjimmy/Desktop/data_within_sample/'
    # onlyfiles = [f for f in listdir(path_data) if
    #             isfile(join(path_data, f))]
    # del onlyfiles[0]
    # print(onlyfiles)
    #
    # # 生成原始因子数据
    # for index in range(0,len(onlyfiles)):
    #    print(onlyfiles[index])
    #    data = pd.DataFrame(pd.read_csv(path_data + onlyfiles[index], error_bad_lines=False, encoding='gbk'))
    #    data.drop(['name', 'trade_status','susp_reason','lastradedays_s','last_trade_day',
    #               'maxupordown','adjfactor','share_totala'], axis=1, inplace=True)
    #    data = data[data.volume > 0]
    #    data.index = range(len(data))
    #    data = data.rename(columns={'code': 'stock_code'})
    #    data.to_csv(path_output + onlyfiles[index], encoding='gbk', index=False)


    name_list = os.listdir(path_output)
    name_list.sort()
    #print(name_list)
    onlyfiles_ta = [f for f in name_list if
                isfile(join(path_output, f))]
    del onlyfiles_ta[0]
    print(onlyfiles_ta)

# change
    path = '/Users/fatjimmy/Desktop/Quant/generated_factor/technical_analysis/original/lg(a%b)+lg(b%a)/'
    if not os.path.exists(path):
        os.makedirs(path)

    data_column = pd.DataFrame(pd.read_csv(path_output + onlyfiles_ta[0], error_bad_lines=False, encoding='gbk',))
    del data_column['stock_code']
    del data_column['industry']
    column_list =data_column.columns.values.tolist()



    # column_list = ['pre_close', 'open', 'close', 'high', 'low', 'vwap', 'chg', 'pct_chg', 'turn', 'free_turn',
    #                'volume', 'amt', 'dealnum', 'swing', 're_ipo_chg', 'rel_ipo_pct_chg']
    #column_list = ['pre_close', 'open', 'close']

    count = 0
    starttime = time.clock()
    for i in range(len(column_list)):
        for j in range(i+1,len(column_list)):
            count += 1
            print(count)
            print(column_list[i],column_list[j])
# may change
            path_factor = path + column_list[i] + ' & ' + column_list[j] + '/'
            if not os.path.exists(path_factor):
                os.makedirs(path_factor)
            for index in range(len(onlyfiles_ta)-1):
            # for index in range(0, 5):
                factor = pd.DataFrame(
                    pd.read_csv(path_output + onlyfiles_ta[index], error_bad_lines=False, encoding='gbk',
                                usecols=['stock_code', column_list[i], column_list[j]]))
                factor = factor.reindex(
                    columns=['stock_code', column_list[i], column_list[j], 'factor_value'])
# change
                factor = factor[factor[column_list[i]] != 0]
# change
                factor = factor[factor[column_list[j]] != 0]
# change
                factor['factor_value'] = np.log10(abs(factor[column_list[i]])) / np.log10(abs(factor[column_list[j]])) + \
                                         np.log10(abs(factor[column_list[j]])) / np.log10(abs(factor[column_list[i]]))
                factor = factor.dropna(axis=0, how='any')

                factor[['stock_code','factor_value']].to_csv(path_factor + onlyfiles_ta[index + 1], encoding='gbk', index=False)
            endtime = time.clock()
            print('FactorGet'+str(count)+': ' + str(endtime - starttime) + ' sec')



    # for column_name in column_list:
    #     print(column_name)
    #     path_factor = path + column_name + '/'
    #     if not os.path.exists(path_factor):
    #         os.makedirs(path_factor)
    #
    #     for index in range(10, len(onlyfiles_ta)):
    #     #for index in range(10, 11):
    #         print(onlyfiles_ta[index])
    #         factor = pd.DataFrame(
    #                 pd.read_csv(path_output + onlyfiles_ta[index - 1], error_bad_lines=False, encoding='gbk',
    #                             usecols=['stock_code', column_name]))
    #         # 取前10天的因子值,列名为[''1','2'...'10']
    #         j = 0
    #         for i in range(index - 10, index - 1):
    #             pre_factor = pd.DataFrame(
    #                 pd.read_csv(path_output + onlyfiles_ta[i], error_bad_lines=False, encoding='gbk',
    #                                 usecols=['stock_code', column_name]))
    #             factor = pd.merge(factor, pre_factor, how='left', on=['stock_code'])
    #             factor = factor.rename(columns={column_name + '_x': str(j)})
    #             factor = factor.rename(columns={column_name + '_y': str(j + 1)})
    #             factor = factor.rename(columns={column_name: str(j + 1)})
    #             j = j + 1
    #         factor = factor.rename(columns={'0': '10'})
    #         # factor.to_csv('/Users/fatjimmy/Desktop/1.csv', encoding='gbk', index=False)
    #         factor = factor.dropna(axis=0, how='any')
    #
    #         # df用于计算max,min,mean等衍生数据
    #         df = pd.DataFrame(columns=['stock_code'])
    #         df['stock_code'] = factor['stock_code']
    #
    #         # 计算day天的移动平均数
    #         day = 10
    #         alpha = 2 / (1 + day)
    #         factor['moving_average'] = 0
    #         xs = 0
    #         for i in range(1, day + 1):
    #             # 指数移动平均
    #             # factor['moving_average'] += factor[str(i)] * (1 - alpha) ** (day - i)
    #             # xs += (1 - alpha) ** (i - 1)
    #             # 加权移动平均
    #             factor['moving_average'] += i * factor[str(i)]
    #             xs += i
    #         factor['moving_average'] = factor['moving_average'] / xs
    #         df['moving_average'] = factor['moving_average']
    #         factor['average_gap'] = (factor['10'] - factor['moving_average'])
    #         df['factor_value_ma'] = factor['average_gap']
    #
    #         # 删除两列数据以免影响衍生数据的计算
    #         del factor['moving_average']
    #         del factor['average_gap']
    #
    #         # 衍生数据
    #         df['max'] = factor.max(1)
    #         df['min'] = factor.min(1)
    #         # df['std'] = factor.std(1)
    #         df['mad'] = factor.mad(1)
    #         df['mean'] = factor.mean(1)
    #         # df['skew'] = factor.skew(1)
    #         # df['kurt'] = factor.kurt(1)
    #         df['factor_value_ma'] = df['factor_value_ma'] / df['mad']
    #         df['factor_value_bb'] = (df['max'] - df['min']) / (df['mean'])
    #         df['factor_value'] = df['factor_value_ma'] * df['factor_value_bb']
    #         # df['factor_value'] = df['factor_value_bb']
    #
    #         factor_name = 'factor_value'
    #         df = df.sort_values(by=factor_name, axis=0, ascending=False)
    #         df.index = range(len(df))
    #         df = df[['stock_code', factor_name]]
    #         df = df.rename(columns={factor_name: 'factor_value'})
    #         #print(df)
    #
    #         df.to_csv(path_factor + onlyfiles_ta[index], encoding='gbk', index=False)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')