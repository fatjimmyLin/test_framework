# -*- coding: utf-8 -*-
# 获得股票60天上市信息表ipo_60.csv
from os import listdir
from os.path import isfile, join
import pandas as pd
import time

def IPOday():
    path_ipo = '/Users/fatjimmy/Desktop/Quant/ipo.csv'
    path_price = '/Users/fatjimmy/Desktop/Quant/price_data/stock/'
    onlyfiles = [f for f in listdir(path_price) if
                 isfile(join(path_price, f))]
    del onlyfiles[0]
    date = []
    for index in range(len(onlyfiles)):
        date.append(onlyfiles[index][:-4])
    ipoInfo = pd.DataFrame(pd.read_csv(path_ipo, error_bad_lines=False, encoding='utf-8'))
    ipoInfo = ipoInfo.sort_values(by='date')
    # index重新排序
    ipoInfo.index = range(len(ipoInfo))
    ipoInfo = ipoInfo.reindex(columns=['stock_code', 'date', 'date_60'])
    starttime2 = time.clock()
    for index in range(len(ipoInfo)):
        for i in range(len(date)):
            if str(ipoInfo.at[index,'date']) == date[i]:
                if i+60 < len(date):
                    ipoInfo.at[index,'date_60'] = date[i+60]
                    continue
                else:
                    ipoInfo.at[index,'date_60'] = date[-1]
                    continue
    starttime3 = time.clock()
    print('3: ' + str(starttime3 - starttime2) + ' sec')
    ipoInfo.to_csv('~/Desktop/Quant/ipo_60.csv')

def main():
    IPOday()

if __name__ == '__main__':
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')