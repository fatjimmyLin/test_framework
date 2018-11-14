# -*- coding: utf-8 -*-
# 删除掉上市60天内的股票
from os.path import isfile, join
import pandas as pd
from os import listdir
import os
import time

def main():

    deleteIPO60()

def deleteIPO60():
    path_stock = '/Users/fatjimmy/Desktop/Quant/price_data/stock_tcap/'

    name_list = os.listdir(path_stock)
    name_list.sort()
    onlyfiles = [f for f in name_list if
                 isfile(join(path_stock, f))]
    del onlyfiles[0]
    print(onlyfiles)
    ipo_info = pd.DataFrame(pd.read_csv('~/Desktop/Quant/ipo_60.csv', error_bad_lines=False, encoding='utf-8'))

    for index in range(len(onlyfiles)):
        date = onlyfiles[index][:-4]
        print(date)
        factor = pd.DataFrame(pd.read_csv(path_stock + onlyfiles[index],error_bad_lines=False, encoding='gbk',
                    usecols=['code', 'industry', 'pre_close', 'close', 'amt', 'ev', 'turn']))
        factor = factor.rename(columns={'code': 'stock_code'})
        # 将factor和ipo信息表合并
        factor = pd.merge(factor, ipo_info, how='left', on=['stock_code'])
        factor = factor.reindex(
            columns=['stock_code', 'industry', 'pre_close', 'close', 'date_60', 'amt', 'ev', 'turn'])
        ##############################################################################################
        # for i in range(len(factor)):
        # 非NAN标记为0，非NAN说明'date_60'有数据，是最近刚IPO的，需要标记
        #    if factor.at[i, 'date_60'] == factor.at[i, 'date_60']:
        #        if str(factor.at[i, 'date_60'])[:-2] < date:
        #            factor.at[i, 'day'] = 1
        #        else:
        #            factor.at[i, 'day'] = 0
        #        continue
        # NAN标记为1
        #    elif factor.at[i, 'date_60'] != factor.at[i, 'date_60']:
        #        factor.at[i, 'day'] = 1

        #有ipo记录，但ipo时间超过60天，标记为1
        factor_not_nan1 = factor[(factor.date_60 == factor.date_60) & (factor.date_60 < float(date))][
            ['stock_code', 'date_60']]
        df_not_nan1 = pd.DataFrame(columns=['stock_code', 'day'])
        df_not_nan1['stock_code'] = factor_not_nan1['stock_code']
        df_not_nan1 = df_not_nan1.fillna(1)

        #有ipo记录，ipo时间尚未超过60天，标记为0
        factor_not_nan0 = factor[(factor.date_60 == factor.date_60) & (factor.date_60 >= float(date))][
            ['stock_code', 'date_60']]
        df_not_nan0 = pd.DataFrame(columns=['stock_code', 'day'])
        df_not_nan0['stock_code'] = factor_not_nan0['stock_code']
        df_not_nan0 = df_not_nan0.fillna(0)

        #无ipo记录，标记为1
        factor_nan = factor[factor.date_60 != factor.date_60][['stock_code', 'date_60']]
        df_nan = pd.DataFrame(columns=['stock_code', 'day'])
        df_nan['stock_code'] = factor_nan['stock_code']
        df_nan = df_nan.fillna(1)

        factorNotNaN1 = pd.merge(factor, df_not_nan1, how='inner', on=['stock_code'])
        factorNotNaN0 = pd.merge(factor, df_not_nan0, how='inner', on=['stock_code'])
        factorNaN = pd.merge(factor, df_nan, how='inner', on=['stock_code'])
        factor = pd.concat([factorNotNaN1, factorNotNaN0, factorNaN])
        factor = factor.sort_values(by='stock_code', axis=0, ascending=True)
        factor.index = range(len(factor))

        ##############################################################################################
        # 删除IPO60天内的数据，即'day'列为0的数据

        factor = factor[factor.day != 0]
        del factor['day']
        del factor['date_60']
        factor.to_csv('/Users/fatjimmy/Desktop/Quant/price_data/stock_delete60_tcap/' + onlyfiles[index], encoding='gbk')


if __name__ == '__main__':
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')