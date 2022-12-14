import pandas as pd
import numpy as np
from datetime import *
from rqdatac import *


quarter_to_time_span = {'q1': ([1, 1], [3, 31]), 'q2': ([4, 1], [6, 30]), 'q3': ([7, 1], [9, 30]), 'q4': ([10, 1], [12, 31])}


def datetime_index_to_date_index(df):
    a = df.index
    b = [i.date() for i in a]
    df.index = b


def get_ring_growth(df, lag_p=1):
    """
    输入，返回时间序列的环比变化dataframe
    :param df: df不同的列装载不同的时间序列，每一行是交易日，在时序数据随时间推移而不变的时候，行维持上一次变化后的值不变
    :param lag_p: 进行对比的两期之间间隔的期数
    :return: 行的值每一次变化后得到环比值，且在该环比时序数据随时间推移而不变的时候，行维持上一次变化后的值不变
    """
    temp = pd.DataFrame(index=df.index, columns=df.columns)
    completed_temp = pd.DataFrame()

    for c in df.columns:
        df_drop_dup = df[[c]].drop_duplicates(keep='last')
        df_change_rate = df_drop_dup/df_drop_dup.shift(lag_p) - 1
        temp.loc[:, c] = df_change_rate.loc[:, c]
    completed_temp = temp.fillna(method='ffill')
    return completed_temp


def select_stocks_greater_than_percentile(stock_list, day, target_df, df_name, percentile):
    """
    从个股列表stock_list中依据各个股的指标target的相对百分位percentile选出作为原个股列表的子集的新个股列表
    percentile是从小到大排序后的百分位，0%是最小值数的百分位，100%是最小值数的百分位
    :param stock_list: 装载着待选个股
    :param day: 指定日期
    :param target_df: rq_get_data.py中取出的该指标下时间-个股构成的dataframe
    :param df_name: target_df的名称
    :param percentile: 在指标上的排序百分位
    :return: 新的个股列表
    """
    set_in_question = set(stock_list) - set(target_df.columns)
    if set_in_question:
        print(f'stocks NOT in {df_name}', set_in_question)
        stock_list = list(set(stock_list) - set_in_question)
    df = target_df.loc[day, stock_list]
    # 计算分位数, 因为可能存在nan，所以用np.nanpercentile而不用np.percentile
    p = np.nanpercentile(df, percentile)
    # 取大于等于分位数的值的索引
    new_stock_list = df[df.ge(p)].index.to_list()
    return new_stock_list



def convert_quarter_to_full(quarter_day_value_dict, days):
    """
    用可得的最新一期季度数据填充至考察日scores_end_date
    :param quarter_day_value_dict: key为obi，值为dataframe
    :param days: 输出时间区间
    :return: 日度df
    """
    print('convert_quarter_to_full starts', datetime.now())
    stocks_list = list(quarter_day_value_dict.keys())
    df = pd.DataFrame(index=days, columns=stocks_list)
    for s in stocks_list:
        quarter_day_s_drop_dup_quarter = quarter_day_value_dict[s]
        for q, row in quarter_day_s_drop_dup_quarter.iterrows():
            # 年份须由字符转化为数字
            y_begin = int(q[0: 4])
            m_begin = quarter_to_time_span[q[4: 6]][0][0]
            d_begin = quarter_to_time_span[q[4: 6]][0][1]
            y_end = int(q[0: 4])
            m_end = quarter_to_time_span[q[4: 6]][1][0]
            d_end = quarter_to_time_span[q[4: 6]][1][1]
            begin_time_point = date(y_begin, m_begin, d_begin)
            end_time_point = date(y_end, m_end, d_end)
            quarter_span = get_trading_dates(begin_time_point, end_time_point)
            df.loc[quarter_span, s] = row[0]
            # 用可得的最新一期数据填充至考察日scores_end_date
    df = df.fillna(method='ffill')
    print('convert_quarter_to_full ends', datetime.now())
    return df


def get_lots_from_shares(obi, shares):
    """
    考虑科创板和创业板的情况下，对股票买入数四舍五入
    :param obi: 个股代码
    :param shares: 买入份数的小数形式
    :return: 买入的手数
    """
    if obi[0:3] in ['688', '300']:
        temp = shares // 100
        # 条件shares % 200 >= 100等价于temp是奇数
        if shares % 200 >= 100:
            temp = temp + 1
        else:
            pass
    else:
        temp = shares // 100
        if shares % 100 >= 50:
            temp = temp + 1
    return temp
