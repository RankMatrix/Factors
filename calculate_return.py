from global_fun import *
from rqdatac import *

import pandas as pd
import matplotlib.pyplot as plt
# plt.ion()

def check_na_in_change_rate(obis, s_date, e_date):
    """
    :param obis: 考察的个股
    :return: obis_in_everyday是字典，key是日期，value是该日收益率为空的obi的集合
    """
    na_obis_in_everyday_dict = dict()
    daily_change_rate = get_price_change_rate(obis, start_date=s_date, end_date=e_date, expect_df=True)
    datetime_index_to_date_index(daily_change_rate)
    for d, row in daily_change_rate.iterrows():
        na_obis_in_everyday_dict[d] = set(row[row.isna()].index)
    return na_obis_in_everyday_dict


def adjust_weights_based_on_na(weights_before_adj_df, na_obis_in_everyday_dict):
    """
    :param weights_before_adj_df: 含na的权重df，index是不同交易日，列是不同标的
    :param na_obis_in_everyday_dict: 每日收益率为na的个股字典，key是交易日，value是个股名单
    :return: 调整后的权重df，收益率不为na的个股均分收益率为na的个股原来的权重
    """
    assert set(weights_before_adj_df.index) == na_obis_in_everyday_dict.keys(), 'weights_ts_df and obis_in_everyday_dict do Not have the same dates'
    # df作为外层weights_df_after_adj
    df = weights_before_adj_df.copy()
    obis_count = weights_before_adj_df.shape[1]
    for d, row in weights_before_adj_df.iterrows():
        if na_obis_in_everyday_dict[d]:
            df_columns = set(df.columns)
            sum_weight_of_na_change_rate = row[list(na_obis_in_everyday_dict[d])].sum()
            df.loc[d, df_columns - na_obis_in_everyday_dict[d]] = df.loc[d, df_columns - na_obis_in_everyday_dict[d]] + sum_weight_of_na_change_rate / (obis_count - len(na_obis_in_everyday_dict[d]))
            df.loc[d, na_obis_in_everyday_dict[d]] = 0
    return df


def calculate_return(open_date, obis, weights_df_before_adj, close_date):
    """
    :param open_date: 以收盘价建仓，该收盘价所在日
    :param obis: 标的list
    :param weights_df_before_adj: 调整后的权重df,index和columns应当和daily_change_rate完全相同
    :param close_date: 以收盘价平仓，该平仓价所在日
    :return: 收益率
    """
    daily_change_rate = get_price_change_rate(obis, start_date=open_date, end_date=close_date, expect_df=True)
    datetime_index_to_date_index(daily_change_rate)
    # 可能由于obis里个股少且每个个股的日收益率起始时间晚于权重时序df设想的起始时间，daily_change_rate的起始时间比权重df起始时间更晚
    if set(weights_df_before_adj.index) - set(daily_change_rate.index):
        weights_df_before_adj = weights_df_before_adj.loc[daily_change_rate.index, :]
    assert set(weights_df_before_adj.columns) == set(daily_change_rate.columns), 'weights obis Not equal to daily_change_rate obis'
    return_ts = 1 + daily_change_rate
    na_obis_dict = check_na_in_change_rate(obis=obis, s_date=open_date, e_date=close_date)
    weights_df_after_adj = adjust_weights_based_on_na(weights_before_adj_df=weights_df_before_adj, na_obis_in_everyday_dict=na_obis_dict)
    # 行列均相同时，列自动对齐
    return_ts = return_ts * weights_df_after_adj
    return_ts['sum_return'] = return_ts.apply(lambda row: row.sum(), axis=1)
    return return_ts


def calculate_cumprod_return(stocks_list, w_ori_df, s_date, e_date, label_string):
    if not stocks_list:
        pass
    else:
        sum_return_df = calculate_return(open_date=s_date, obis=stocks_list, weights_df_before_adj=w_ori_df, close_date=e_date)
        net_value_ts = sum_return_df['sum_return'].cumprod()
        # df作为外层的net_value_ts_df
        df = pd.DataFrame(net_value_ts)
        plt.plot(df.index, df - 1, label=label_string)
    return df


# def calculate_cumprod_return_without_rebalancing(stocks_list, w_ori_df, s_date, e_date, label_string):
#     if not stocks_list:
#         pass
#     else:


