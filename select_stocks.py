from constant import citics_2019
from global_fun import *

from rqdatac import *

import pandas as pd
import numpy as np
from datetime import *


#
def get_days_needed_for_profit(close_stocks, selected_date, obis):
    """
    求这些股票第一次上涨到建仓价时距离建仓的时间
    :param close_stocks: 收盘价大表
    :param selected_date: 建仓日末
    :param obis: 个股代码list
    :return: df，index为selected_date, columns为obis，值为天数
    """
    all_price_minus_open_price = close_stocks.loc[selected_date:, obis] - close_stocks.loc[selected_date, obis]
    # 记录回本时间的空dataframe
    days_needed_for_profit = pd.DataFrame(index=[selected_date], columns=obis)
    for i in all_price_minus_open_price.columns:
        i_price = all_price_minus_open_price.loc[:, i]
        if len(i_price[i_price > 0]) > 0:
            profit_starts_day = i_price[i_price > 0].index[0]
            days_needed = profit_starts_day - selected_date
            days_needed_for_profit.loc[selected_date, i] = days_needed.days
        else:
            days_needed_for_profit.loc[selected_date, i] = np.nan
    return days_needed_for_profit


# 技术指标
# 成交量 > n*过去m个交易日成交量均值
def get_erupting_volume(volume_stocks, selected_date, n, m):
    v_mean_ready = volume_stocks.loc[get_previous_trading_date(selected_date, m): selected_date, :]
    v = volume_stocks.loc[selected_date, :]
    v_minus_mean = v - n * v_mean_ready.mean()
    return set(v_minus_mean[v_minus_mean > 0].index)


# 收盘价较上一日收盘价上涨幅度 > 0.05
def get_percent_higher(close_stocks, selected_date, obis, percent):
    temp = close_stocks.loc[selected_date, obis] / close_stocks.loc[get_previous_trading_date(selected_date, 1), obis]
    return set(temp[temp > percent].index)


# 过去20日价格振幅 < 5%
# 过去时间段最高价
def get_highest_price_in_dates(highest_price_stocks, obis, selected_date, time_span):
    """
    获取指定时间段最高价
    :param highest_price_stocks: 最高价df，index为日期，columns是obis，值是价格
    :param obis: obi的列表
    :param selected_date: 时间段的最后一天
    :param time_span: 时间段的长度天数
    :return: 一个series，index是obis，值是这段时间最高价
    """
    temp = highest_price_stocks.loc[get_previous_trading_date(selected_date, time_span): selected_date, obis]
    return temp.max()


# 过去时间段最低价
def get_lowest_price_in_dates(lowest_price_stocks, obis, selected_date, time_span):
    temp = lowest_price_stocks.loc[get_previous_trading_date(selected_date, time_span): selected_date, obis]
    return temp.min()


# 上涨出(非涨停(可选））缺口
def get_too_high_gap_stocks(too_high_ready, selected_date, *up_to_limit_up_ready):
    """
    :param too_high_ready: 用于选出跌出缺口的股票
    :param up_to_limit_up_ready: 用于选出跌停的股票，从跌出缺口的股票集合中剔除
    :param selected_date: 缺口产生日 或 预计建仓日
    :return: 跌出缺口的股票
    """
    # 求低开缺口集合
    print('too_high缺口')
    # 某日有上涨产生的缺口的股票
    set_too_high_gap = get_set_on_date_g(df=too_high_ready, selected_date=selected_date, p=0)
    # 某日跌停的股票
    if up_to_limit_up_ready != ():
        temp = up_to_limit_up_ready[0].loc[selected_date, :]
        temp = temp[temp == 0]
        set_up_to_limit_up = set(temp.index)
    else:
        set_up_to_limit_up = set()
    # 有下跳产生缺口且未跌停的股票
    set_too_high_gap_done = set_too_high_gap - set_up_to_limit_up
    return set_too_high_gap_done


# 下跌出(非跌停(可选））缺口
def get_too_low_gap_stocks(too_low_ready, selected_date, *down_to_limit_down_ready):
    """
    :param too_low_ready: 用于选出跌出缺口的股票
    :param down_to_limit_down_ready: 用于选出跌停的股票，从跌出缺口的股票集合中剔除
    :param selected_date: 缺口产生日 或 预计建仓日
    :return: 跌出缺口的股票
    """
    # 求低开缺口集合
    print('too_low缺口')
    # 某日有下跳产生的缺口的股票
    set_too_low_gap = get_set_on_date_l(df=too_low_ready, selected_date=selected_date, p=0)
    # 某日跌停的股票
    if down_to_limit_down_ready != ():
        temp = down_to_limit_down_ready[0].loc[selected_date, :]
        temp = temp[temp == 0]
        set_down_to_limit_down = set(temp.index)
    else:
        set_down_to_limit_down = set()
    # 有下跳产生缺口且未跌停的股票
    set_too_low_gap_done = set_too_low_gap - set_down_to_limit_down
    return set_too_low_gap_done


# 炜锋基本面指标
# 目标一，step1.1)当期ROE(TTM)位于行业前30%，即大于70%的个股
def get_roe_ttm_based_on_indus_1_1(df, df_name, selected_date, stocks_of_all_indus, p_1_1=70):
    roe_ttm_based_on_indus_1_1 = dict()
    print('building roe_ttm_based_on_indus_1_1', datetime.now())
    # all_stocks_amount = 0
    for indus in stocks_of_all_indus.keys():
        stocks_of_indus = stocks_of_all_indus[indus]
        # all_stocks_amount = all_stocks_amount + len(stocks_of_indus)
        # print(indus, 'amount:', len(stocks_of_indus))
        stocks_not_exist_in_selected_date = [i for i in stocks_of_indus if is_suspended(i, start_date=selected_date, end_date=selected_date, market='cn') is None]
        if stocks_not_exist_in_selected_date:
            print(f'stocks not exist in {selected_date}:', indus, stocks_not_exist_in_selected_date)
            stocks_of_indus = list(set(stocks_of_indus) - set(stocks_not_exist_in_selected_date))
        else:
            pass
        list_1_1 = select_stocks_greater_than_percentile(stock_list=stocks_of_indus, day=selected_date, target_df=df, df_name=df_name + indus, percentile=p_1_1)
        roe_ttm_based_on_indus_1_1[indus] = set(list_1_1)
        # print(indus, 'set_1_1', len(list_1_1))
    # print('all_stocks_amount', all_stocks_amount)
    print('building ends', datetime.now())
    return roe_ttm_based_on_indus_1_1


# 对于日时序，在某日，指标大于某值
def get_set_on_date_g(df, selected_date, p):
    temp = df.loc[selected_date, :]
    list_index = temp[temp > p].index.to_list()
    print(len(list_index))
    return set(list_index)


def get_set_on_date_l(df, selected_date, p):
    temp = df.loc[selected_date, :]
    list_index = temp[temp < p].index.to_list()
    print(len(list_index))
    return set(list_index)


# 对于季度时序，在某季度，指标位于某区间
def get_set_on_quarter_b(df, selected_quarter, p):
    temp = df.loc[selected_quarter, :]
    list_index = temp[(temp >= p[0]) & (temp <= p[1])].index.to_list()
    print(len(list_index))
    return set(list_index)


# 对于季度时序，在某季度，指标大于某值
def get_set_on_quarter_g(df, selected_quarter, p):
    temp = df.loc[selected_quarter, :]
    list_index = temp[temp > p].index.to_list()
    print(len(list_index))
    return set(list_index)


# 5.5）step 5.5 市值行业分位>30%，即大于70%的个股
def get_market_val_in_circulation_based_on_indus_5_5(df, df_name, selected_date, stocks_of_all_indus, p_5_5=70):
    market_val_in_circulation_based_on_indus_5_5 = dict()
    print('building market_val_in_circulation_based_on_indus_5_5', datetime.now())
    for indus in stocks_of_all_indus.keys():
        stocks_of_indus = stocks_of_all_indus[indus]
        # print(indus, 'amount:', len(stocks_of_indus))
        stocks_not_exist_in_selected_date = [i for i in stocks_of_indus if
                                           is_suspended(i, start_date=selected_date, end_date=selected_date,
                                                        market='cn') is None]
        if stocks_not_exist_in_selected_date:
            print(f'stocks not exist in selected_date:', indus, stocks_not_exist_in_selected_date)
            stocks_of_indus = list(set(stocks_of_indus) - set(stocks_not_exist_in_selected_date))
        else:
            pass
        list_5_5 = select_stocks_greater_than_percentile(stock_list=stocks_of_indus, day=selected_date, target_df=df, df_name=df_name + indus, percentile=p_5_5)
        set_5_5 = set(list_5_5)
        market_val_in_circulation_based_on_indus_5_5[indus] = set_5_5
        # print(indus, 'set_5_5', len(set_5_5))
    print('building ends', datetime.now())
    return market_val_in_circulation_based_on_indus_5_5


# 子目标1内取交集
def get_union_indus_set_1(set_1_2, roe_ttm_based_on_indus_1_1, industries=citics_2019):
    dict_of_selected_sets_based_on_indus_1 = dict()
    union_indus_set_1 = set()
    for indus in industries:
        # 各细分行业内部运用条件5求交集，输出满足条件的各细分行业
        dict_of_selected_sets_based_on_indus_1[indus] = set.intersection(set_1_2, roe_ttm_based_on_indus_1_1[indus])
        print('set_1 ', indus, len(dict_of_selected_sets_based_on_indus_1[indus]))
        # 各细分行业内部运用条件1求交集，输出满足条件的各细分行业的个股
        union_indus_ready = set_1_2
        union_indus_ready = set.intersection(union_indus_ready, roe_ttm_based_on_indus_1_1[indus])
        union_indus_set_1 = union_indus_set_1.union(union_indus_ready)
        print('union_indus_set_1', len(union_indus_set_1))
    return union_indus_set_1, dict_of_selected_sets_based_on_indus_1


# 子目标5行业内取交集
def get_union_indus_set_5(set_5_1, set_5_2, set_5_3, set_5_4, market_val_in_circulation_based_on_indus_5_5, industries=citics_2019):
    dict_of_selected_sets_based_on_indus_5 = dict()
    union_indus_set_5 = set()
    for indus in industries:
        # 各细分行业内部运用条件5求交集，输出满足条件的各细分行业
        dict_of_selected_sets_based_on_indus_5[indus] = set.intersection(set_5_1, set_5_2, set_5_3, set_5_4, market_val_in_circulation_based_on_indus_5_5[indus])
        print('set_5', indus, len(dict_of_selected_sets_based_on_indus_5[indus]))
        # 各细分行业内部运用条件5求交集，输出满足条件的各细分行业的个股
        union_indus_ready = set.intersection(set_5_1, set_5_2, set_5_3, set_5_4)
        union_indus_ready = set.intersection(union_indus_ready, market_val_in_circulation_based_on_indus_5_5[indus])
        union_indus_set_5 = union_indus_set_5.union(union_indus_ready)
        print('union_indus_set_5', len(union_indus_set_5))
    return union_indus_set_5, dict_of_selected_sets_based_on_indus_5


# 按各行业用5个目标中的细分条件取交集
def get_dict_of_selected_sets_based_on_indus(roe_ttm_based_on_indus_1_1, set_1_2, set_2_1_1, set_2_1_2, set_2_2, set_3_1_1, set_3_1_2, set_3_2, set_4, set_5_1, set_5_2, set_5_3, set_5_4, market_val_in_circulation_based_on_indus_5_5):
    dict_of_selected_sets_based_on_indus = dict()
    for indus in citics_2019:
        dict_of_selected_sets_based_on_indus[indus] = set.intersection(roe_ttm_based_on_indus_1_1[indus], set_1_2, set_2_1_1, set_2_1_2, set_2_2, set_3_1_1, set_3_1_2, set_3_2, set_4, set_5_1, set_5_2, set_5_3, set_5_4, market_val_in_circulation_based_on_indus_5_5[indus])
        print(indus, 'set_intersection_of_all', len(dict_of_selected_sets_based_on_indus[indus]))
    return dict_of_selected_sets_based_on_indus


# 将各行业取并集,再用5个目标取交集
def get_union_indus_set(selected_set_2, selected_set_3, selected_set_4, union_indus_set_1, union_indus_set_5):
    union_indus_set = set.intersection(selected_set_2, selected_set_3, selected_set_4, union_indus_set_1, union_indus_set_5)
    print('union_indus_set', len(union_indus_set))
    return union_indus_set