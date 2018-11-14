# -*- coding: utf-8 -*-
# 测试单个因子的结果
from os.path import isfile, join
import pandas as pd
from os import listdir
import numpy as np
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import os
import time


def CalculateICIR_byYear(path_factor, market_index, isFlu, name):

    txt_path = '/Users/fatjimmy/Desktop/' + name + '.txt'
    curve_path = '/Users/fatjimmy/Desktop/' + name + '.png'

    name_list = os.listdir(path_factor)
    name_list.sort()
    onlyfiles_factor = [f for f in name_list if
                        isfile(join(path_factor, f))]
    del onlyfiles_factor[0]
    #print(onlyfiles_factor)

    years = ['2012', '2013', '2014', '2015', '2016', '2017', '2018']
    ic_years = []
    turnover = []

    capital = 1
    total = 0
    x = []
    y = []
    income_all = []
    j = 1


    #先对总体的IC，IR值做一个计算
    for i in range(1, len(onlyfiles_factor)):
        factor = pd.DataFrame(pd.read_csv(path_factor + onlyfiles_factor[i], error_bad_lines=False, encoding='gbk', ))[
            ['factor_value', 'return_ratio', 'amt', 'label_300', 'label_500', 'label_800']]
        # 股票池选择
        if (market_index > 0):
            # 1000代表不属于800里的股票
            if (market_index == 1000):
                factor = factor[factor['label_800'] == 0.0]
            else:
                factor = factor[factor['label_' + str(market_index)] == 1.0]
        # 流动性控制
        if (isFlu == True):
            factor = factor[factor.amt > 30000]
        # 计算每天的相关性,计算出来是一个2*2的矩阵，取'factor_value'列的第二行作为当天的IC值
        if factor.empty:
            os.remove(path_factor + onlyfiles_factor[i])
            continue
        else:
            ic_result = factor.corr()['factor_value'][1]
            # 去掉nan
            if ic_result == ic_result:
                ic_years.append(ic_result)
    # 计算总的IC,IR
    ic_years = np.array(ic_years)
    ic_mean_years = ic_years.mean()
    ic_std_years = ic_years.std()
    ir_years = ic_mean_years / ic_std_years

    with open(txt_path, 'wt') as f:
        print('IC_years:=', ic_mean_years, file=f)
        print('IC_IR_years:=', ir_years, file=f)
        print('\n', file=f)


    win_6years =0
    all_6years =0

    for index in range(len(years)):
        ic_all = []
        win = 0
        all = 0
        for i in range(1,len(onlyfiles_factor)):
            if onlyfiles_factor[i][:4] == years[index]:
                factor = pd.DataFrame(pd.read_csv(path_factor + onlyfiles_factor[i], error_bad_lines=False, encoding='gbk', ))[
                    ['stock_code', 'factor_value', 'return_ratio', 'close', 'close_next',
                     'amt', 'label_300', 'label_500', 'label_800']]
                # 股票池选择
                if(market_index > 0):
                    # 1000代表不属于800里的股票
                    if(market_index == 1000):
                        factor = factor[factor['label_800'] == 0.0]
                    else:
                        factor = factor[factor['label_'+str(market_index)] == 1.0]
                # 流动性控制
                if(isFlu == True):
                    factor = factor[factor.amt > 30000]
                # 计算每天的相关性,计算出来是一个2*2的矩阵，取'factor_value'列的第二行作为当天的IC值
                if factor.empty:
                    os.remove(path_factor + onlyfiles_factor[i])
                    continue
                else:
                    ic_result = factor.corr()['factor_value'][1]
                    # 去掉nan
                    if ic_result == ic_result:
                        ic_all.append(ic_result)

                # 计算换手率
                factor_lastday = pd.DataFrame(
                    pd.read_csv(path_factor + onlyfiles_factor[i - 1], error_bad_lines=False, encoding='gbk', ))[
                    ['stock_code', 'factor_value']]
                factor = pd.merge(factor, factor_lastday, how='left', on=['stock_code'])
                factor = factor.rename(columns={'factor_value_x': 'factor_value'})
                factor = factor.rename(columns={'factor_value_y': 'factor_value_lastday'})
                factor = factor.reindex(
                    columns=['stock_code', 'factor_value', 'factor_value_lastday', 'up', 'down', 'close', 'close_next'])
                factor['up'] = abs(factor['factor_value'] - factor['factor_value_lastday'])
                factor['down'] = abs(factor['factor_value'])
                # 去掉'up'列为nan的行
                turnover.append(factor[factor.up == factor.up]['up'].sum() / factor[factor.up == factor.up]['down'].sum())

                #绘制多空曲线
                sum_positive = 0
                sum_negative = 0
                sum_positive += factor['factor_value'][factor.factor_value > 0].sum()
                sum_negative += factor['factor_value'][factor.factor_value <= 0].sum()
                if sum_positive == 0 or sum_negative == 0:
                    print(onlyfiles_factor[index])
                    print('Data wrong')

                    with open(txt_path, 'at') as f:
                        print(onlyfiles_factor[index], file=f)
                        print('Data wrong', file=f)
                    continue

                factor = factor.reindex(
                    columns=['stock_code', 'factor_value', 'close', 'close_next', 'money', 'stock_num', 'money_next'])

                factor_positive = factor[factor.factor_value > 0][['stock_code', 'factor_value']]
                factor_negative = factor[factor.factor_value <= 0][['stock_code', 'factor_value']]

                df_positive = pd.DataFrame(columns=['stock_code', 'percentage'])
                df_positive['percentage'] = factor_positive['factor_value'] / sum_positive
                df_positive['stock_code'] = factor_positive['stock_code']
                df_negative = pd.DataFrame(columns=['stock_code', 'percentage'])
                df_negative['percentage'] = factor_negative['factor_value'] / sum_negative
                df_negative['stock_code'] = factor_negative['stock_code']

                factor_use_positive = pd.merge(factor, df_positive, how='inner', on=['stock_code'])
                factor_use_negative = pd.merge(factor, df_negative, how='inner', on=['stock_code'])
                factor = pd.concat([factor_use_positive, factor_use_negative])

                factor['money'] = factor['percentage'] * capital
                factor['stock_num'] = factor['money'] / factor['close']
                factor['money_next'] = factor['close_next'] * factor['stock_num']

                income_positive = 0
                income_negative = 0
                income_positive += factor['money_next'][factor.factor_value > 0].sum()
                income_negative -= factor['money_next'][factor.factor_value <= 0].sum()
                income = income_positive + income_negative
                income_all.append(income / 2)

                if(income > 0):
                    win += 1
                all += 1

                total = total + income
                j = j + 1
                y.append(total)
                x.append(j)

                # print('ic ' + onlyfiles_factor[i][:8] + ' ' + str(ic_result))
                # if(ic_result > 0.2):
                #     print('!!!!!!')
                # print('income ' + onlyfiles_factor[i][:8] + ' ' + str(income / 2))
                # print(' ')

        #计算胜率
        if(all != 0):
            win_rate = win / all
            #print('Win_rate_' + years[index] + ':=', win_rate)
            with open(txt_path, 'at') as f:
                print('Win_rate_' + years[index] + ':=', win_rate, file=f)

        #分别计算每年的IC,IR
        ic_all = np.array(ic_all)

        # 数据缺失处理
        if len(ic_all) == 0:
            # print ('data in ' + years[index]+ ' is null!')
            # print('\n')
            continue
        ic_mean = ic_all.mean()
        ic_std = ic_all.std()
        ir = ic_mean / ic_std
        # print ('IC ' + years[index] + ':=', ic_mean)
        # print ('IC_IR '+ years[index] + ':=', ir)

        with open(txt_path, 'at') as f:
            print('IC ' + years[index] + ':=', ic_mean, file=f)
            print('IC_IR ' + years[index] + ':=', ir, file=f)
            print('\n', file=f)

        win_6years += win
        all_6years += all

        # print('\n')


    # 计算胜率
    win_rate_6years = win_6years / all_6years
    # 计算换手率
    turnover = np.array(turnover)
    income_all = np.array(income_all)
    maxDrawdown = get_max_drawdown(y)

    # print('win rate = ', win_rate_6years)
    # print('turnover rate:=', '%.2f%%' % (turnover.mean() * 100))
    # # 绘制多空曲线图，计算基本指标
    # print("total money = " + str(total))
    # print("return rate = " + str('%.2f%%' % (income_all.mean() * 250 * 100)))
    # print("max drawdown = " + str('%.2f%%' % (maxDrawdown * 100)))

    with open(txt_path, 'at') as f:
        print('win rate = ', win_rate_6years, file=f)
        print('turnover rate:=', '%.2f%%' % (turnover.mean() * 100), file=f)
        # 绘制多空曲线图，计算基本指标
        print("total money = " + str(total), file=f)
        print("return rate = " + str('%.2f%%' % (income_all.mean() * 250 * 100)), file=f)
        print("max drawdown = " + str('%.2f%%' % (maxDrawdown * 100)), file=f)

    plt.cla()
    plt.plot(x, y, linewidth=1)  # linewidth决定绘制线条的粗细

    plt.title('Wealth Curve', fontsize=24)  # 标题
    plt.xlabel('Day', fontsize=14)
    plt.ylabel('Cumulative Profit', fontsize=14)

    plt.tick_params(axis='both', labelsize=14)  # 刻度加粗
    plt.savefig(curve_path)
    # plt.show()  # 输出图像


#计算最大回撤
def get_max_drawdown(array):
    drawdowns = []
    max_so_far = array[0]
    for i in range(len(array)):
        if array[i] > max_so_far:
            drawdown = 0
            drawdowns.append(drawdown)
            max_so_far = array[i]
        else:
            drawdown = max_so_far - array[i]
            drawdowns.append(drawdown)
    return max(drawdowns)


def main():

        # path_factor = '/Users/fatjimmy/Desktop//Quant/generated_factor/' \
        #               'technical_analysis/piece/lg(a)+lg(b)_process/'
        path_factor = '/Users/fatjimmy/Desktop/plasticity_factor/factor_plasticity_process/'

        # 选择股票池范围：0代表全部股票，300代表沪深300，500代表中证500，800代表中证800，1000代表不属于中证800的股票
        market_index = 800

        # 是否考虑流动性(成交量大于3000万)，True：考虑；False:不考虑
        isFlu = False

        # 分年份计算IC、IR，计算换手率，绘制收益累积曲线
        CalculateICIR_byYear(path_factor, market_index, isFlu, '1')



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    starttime = time.clock()
    main()
    endtime = time.clock()
    print('FactorGet: ' + str(endtime - starttime) + ' sec')