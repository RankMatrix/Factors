import numpy as np

from constant import data_targets_20221104, de_listed_dates_obis_df

from select_stocks import *
from calculate_return import *

# do this before importing pylab or pyplot
import matplotlib.pyplot as plt
import pandas as pd
import pickle
import itertools
import os
from rqdatac import *


# 求集合参数设定####################################
print('selecting_starts', datetime.now())
# 始终用老数据取集合
i_000905 = close_stocks = l_of_stocks = h_of_stocks = too_low_ready = too_high_ready = down_to_limit_down_ready = up_to_limit_up_ready = industry_all_days_all_stocks = volume_all_stocks = pe_ratio_all_stocks = a_share_market_val_in_circulation_all_stocks = du_roe_ttm_all_stocks = inc_revenue_ttm_all_stocks = gross_profit_margin_ttm_all_stocks = debt_to_asset_ratio_ttm_all_stocks = dividend_yield_ttm_all_stocks = intangible_asset_ratio_ttm_all_stocks = operating_cash_flow_per_share_ttm_all_stocks = pd.DataFrame()
path = 'C:/Users/zhili/Desktop/history_data/'
for data_name in data_targets_20221104:
    file_name = data_name + '.pkl'
    if os.path.isfile(path + file_name):

        input_path_name = output_path_name = path + data_name + '.pkl'
        # 如果存在历史数据则读取历史数据，再把新数据连在历史数据尾部，再输出包含历史数据在内的完整新数据
        # 读入
        file = open(input_path_name, 'rb')
        exec(data_name + ' = pickle.load(file)')
        print(data_name + ': exists')
        file.close()
    else:
        print(data_name + ': NOT exists')

# 不随历史更新的数据的环同比
# 目标1：du_roe_ttm_ring_growth
print('du_roe_ttm_ring_growth starts')
du_roe_ttm_ring_growth = get_ring_growth(du_roe_ttm_all_stocks)
# 目标2.1：inc_revenue_ttm_ring_growth；
print('inc_revenue_ttm_ring_growth starts')
inc_revenue_ttm_ring_growth = get_ring_growth(inc_revenue_ttm_all_stocks)
# 目标2.2：gross_profit_margin_ttm_ring_growth；
print('gross_profit_margin_ttm_ring_growth starts')
gross_profit_margin_ttm_ring_growth = get_ring_growth(gross_profit_margin_ttm_all_stocks)
# 参数设定
# p_1_1_scope = [50, 60, 70]
# p_1_2_scope = [-0.02, 0, 0.02]
# p_2_1_1_scope = [-0.05, 0, 0.05]
# p_2_1_2_scope = [-0.15, -0.1, 0]
# p_2_2_scope = [-0.05, 0, 0.05]
# p_3_1_1_scope = [(0, 2), (0, 3), (0, 4), (1, 4), (2, 4)]
# p_3_1_2_scope = [-0.15, -0.1, -0.05]
# p_3_2_scope = [-0.2, -0.1, 0]
# p_4_scope = [0.1, 0.2, 0.3]
# p_5_1_scope = [0.7, 0.8, 0.9]
# p_5_2_scope = [0.1, 0.2, 0.3]
# p_5_3_scope = [0]
# p_5_4_scope = [5, 15, 30, 80]
# p_5_5_scope = [50, 60, 70]

p_1_1_scope = [70]
p_1_2_scope = [0]
p_2_1_1_scope = [0]
p_2_1_2_scope = [-0.1]
p_2_2_scope = [0]
p_3_1_1_scope = [(0, 4)]
p_3_1_2_scope = [-0.1]
p_3_2_scope = [-0.1]
p_4_scope = [0.2]
p_5_1_scope = [0.8]
p_5_2_scope = [0.2]
p_5_3_scope = [0]
p_5_4_scope = [80]
p_5_5_scope = [70]
p_scope = list(itertools.product(p_1_1_scope, p_1_2_scope, p_2_1_1_scope, p_2_1_2_scope, p_2_2_scope, p_3_1_1_scope, p_3_1_2_scope, p_3_2_scope, p_4_scope, p_5_1_scope, p_5_2_scope, p_5_3_scope, p_5_4_scope, p_5_5_scope))
print('p_scope combination amounts: ', len(p_scope))

# 筛选股票的得分门槛 和 投入总金额
threshold_grade = 12
total_position_amount = 2000000
# 假设selected_endDate日末建仓，获得的第一个收益率是selected_endDate之后的第一个交易日的收益率
# 求集合时，指标的选取时间设定
selected_quarter = '2022q3'
# 回测起始日的遍历区间的截止日期最晚 为 数据收盘价数据的最后一天的前1天
# 遍历的建仓日(末)/得分考察日的区间
# 每个季报后回测起始日参考，年报、一季报5月1日，半年报9月1日，三季报11月1日
backtest_walk_through_dates = get_trading_dates(date(2022, 11, 1), date(2022, 11, 1))
# 用于制造随时间推移会变化的历史数据及其环比、同比、两年同期等数据，要确保基础数据时间跨度足以计算环比、同比、两年同期等数据
# 为建仓日提供季度数据
executing_endQuarter = selected_quarter
# executing_endQuarter = '2015q1'
# 和executing_endQuarter的时间跨度至少为两年，为建仓日提供季度数据的两年同比
executing_beginQuarter = '2020q3'
# executing_q_to_d_begin_date必须要 <= executing_beginQuarter季节的第一天，作为后续quarter转date的初始表起始时间
# executing_q_to_d_begin_date = date(2019, 7, 1)
y_begin = int(executing_beginQuarter[0: 4])
m_begin = quarter_to_time_span[executing_beginQuarter[4: 6]][0][0]
d_begin = quarter_to_time_span[executing_beginQuarter[4: 6]][0][1]
executing_q_to_d_begin_date = date(y_begin, m_begin, d_begin)
# ############################################################################################################



# 记录不同系列的集合的回本时长的时间序列
days_needed_for_profit_tong = pd.DataFrame()
days_needed_for_profit_tao_final = pd.DataFrame()
days_needed_for_profit_tao_intersect_with_grades = pd.DataFrame()

for selected_date in backtest_walk_through_dates:
    # 选出考察日未退市的股票，防止直接引用constant时的market_obis中出现引用的保存的数据的最后一天没有的股票
    all_stocks = all_instruments(type='CS', market='cn', date=selected_date)
    # 因为股票的财务指标（行情指标则无此问题）数据出现的日期可能早于股票上市的日期，所以需要取出在selected_date已经上市的股票来和根据指标筛出的股票做交集，得到的建仓股票池才是可行集
    market_obis = all_stocks.loc[~all_stocks['industry_code'].isin(['Unknown'])].order_book_id.to_list()
    # 剔除退市股票（不用列表推导式结合is_suspended）慢 和 不属于股票的代码, 主要用于维护从dividend表（不包含这些异常obi）里取数，其他指标表也会包含这些异常obi
    set_delisted_obis = set(de_listed_dates_obis_df.loc[:get_next_trading_date(selected_date, 1), 'order_book_id'])
    set_unknown_obis = {'000022.XSHE', '601313.XSHG', '000043.XSHE'}
    set_market_obis = set(market_obis) - set_delisted_obis - set_unknown_obis
    market_obis = list(set_market_obis)

    # 历史数据会随时间更新的表，依据selected_date取数 和 计算环比同比数
    # 3.1
    net_profit_deduct_non_recurring_pnl_all_stocks = get_pit_financials_ex(
        fields=['net_profit_deduct_non_recurring_pnl'],
        start_quarter=executing_beginQuarter,
        end_quarter=executing_endQuarter,
        order_book_ids=market_obis, date=selected_date).unstack(
        0).net_profit_deduct_non_recurring_pnl
    # 环比
    print('net_profit_deduct_non_recurring_pnl_ring_growth starts')
    net_profit_deduct_non_recurring_pnl_ring_growth = get_ring_growth(net_profit_deduct_non_recurring_pnl_all_stocks)
    # 同比
    print('net_profit_deduct_non_recurring_pnl_yoy starts')
    net_profit_deduct_non_recurring_pnl_yoy = get_ring_growth(net_profit_deduct_non_recurring_pnl_all_stocks, lag_p=4)
    # 3.2
    net_profit_all_stocks = get_pit_financials_ex(fields=['net_profit'], start_quarter=executing_beginQuarter,
                                                  end_quarter=executing_endQuarter,
                                                  order_book_ids=market_obis, date=selected_date).unstack(0).net_profit
    print('net_profit_2y_growth starts')
    net_profit_2y_growth = get_ring_growth(net_profit_all_stocks, lag_p=8)
    # 4 后续
    print('net_profit_yoy starts')
    net_profit_yoy = get_ring_growth(net_profit_all_stocks, lag_p=4)
    net_profit_yoy_dict_after_drop_dup = dict()
    for i in market_obis:
        net_profit_yoy_dict_after_drop_dup[i] = pd.DataFrame(net_profit_yoy[i])
    print('net_profit_yoy_days starts')
    net_profit_yoy_days = convert_quarter_to_full(quarter_day_value_dict=net_profit_yoy_dict_after_drop_dup,
                                                  days=get_trading_dates(executing_q_to_d_begin_date, selected_date))
    # 有可能selected_date选出的market_obi中的某obi于selected_date之后上市，于是在下述三个指标的表
    temp = net_profit_yoy_days.loc[selected_date, market_obis] - du_roe_ttm_all_stocks.loc[selected_date, market_obis] * (1 - dividend_yield_ttm_all_stocks.loc[selected_date, market_obis])
    profit_trend = pd.DataFrame(temp).T

    # 跑收益率的建仓平仓日
    backtest_open_date = get_next_trading_date(selected_date, 1, market='cn')
    backtest_close_date = date.today()
    # backtest_close_date = get_next_trading_date(backtest_open_date, 60, market='cn')
    open_close_dates = get_trading_dates(backtest_open_date, backtest_close_date)

    # 各行业股票 单独做出来，目的是只使用一次get_industry
    stocks_of_all_indus = dict()
    for indus in citics_2019:
        stocks_of_all_indus[indus] = get_industry(indus, source='citics_2019', date=selected_date, market='cn')

    # 集合#######################################
    # 求低开缺口集合
    set_too_low_gap_done = get_too_low_gap_stocks(too_low_ready, selected_date, down_to_limit_down_ready)
    # market_obis = market_obis + ['301313.XSHE']


    # 韬哥集合
    # 1. 成交量 > 2 * 过去 20 个交易日成交量均值
    set_tao_1 = get_erupting_volume(volume_stocks=volume_all_stocks, selected_date=selected_date, n=2, m=20)
    # 2.1 跳空高开
    set_tao_2_1 = get_too_high_gap_stocks(too_high_ready=too_high_ready, selected_date=selected_date,)
    # 或 2.2 收盘价较上一日收盘价上涨幅度 > 5%
    set_tao_2_2 = get_percent_higher(close_stocks=close_stocks, selected_date=selected_date, obis=market_obis, percent=0.05)
    set_tao_2 = set_tao_2_1.union(set_tao_2_2)
    # 3. 过去20日价格振幅 < 5%
    time_span = 20
    highest_over_lowest = get_highest_price_in_dates(highest_price_stocks=h_of_stocks, obis=market_obis, selected_date=selected_date, time_span=time_span) / get_lowest_price_in_dates(lowest_price_stocks=l_of_stocks, obis=market_obis, selected_date=selected_date, time_span=time_span)
    set_tao_3 = set(highest_over_lowest[highest_over_lowest < 0.05].index)
    set_tao_final = set.intersection(set_tao_1, set_tao_2, set_tao_3)
    # 炜锋集合
    # 个股打分表
    grades_of_stocks = pd.DataFrame(index=market_obis, columns=['grade'])
    grades_of_stocks['grade'] = 0
    stock_amount_picked_from_grades = 150
    # 求基础集合
    for i in p_scope:
        # 目标一，step1.1)当期ROE(TTM)位于行业前30%，即大于70%的个股
        print('step1.1)当期ROE(TTM)位于行业前30%，即大于70%的个股')
        roe_ttm_based_on_indus_1_1 = get_roe_ttm_based_on_indus_1_1(df=du_roe_ttm_all_stocks, df_name='du_roe_ttm_all_stocks', selected_date=selected_date, stocks_of_all_indus=stocks_of_all_indus, p_1_1=70)
        union_indus_set_1_1 = {obi for v in roe_ttm_based_on_indus_1_1.values() for obi in v}
        union_indus_set_1_1 = set.intersection(union_indus_set_1_1, set_market_obis)
        grades_of_stocks.loc[union_indus_set_1_1, 'grade'] = grades_of_stocks.loc[union_indus_set_1_1, 'grade'] + 1
        # step1.2)且最近两期ROE环比变化率 > 0 % （环比变化率 = 当期 / 上期 - 1）；
        print('step1.2)且最近两期ROE环比变化率 > 0 % （环比变化率 = 当期 / 上期 - 1）')
        set_1_2 = get_set_on_date_g(df=du_roe_ttm_ring_growth, selected_date=selected_date, p=i[1])
        set_1_2 = set.intersection(set_1_2, set_market_obis)
        grades_of_stocks.loc[set_1_2, 'grade'] = grades_of_stocks.loc[set_1_2, 'grade'] + 1
        # 目标2.1，step2.1.1)营收及趋势：当期营收增速>0%
        print('step2.1.1)营收及趋势：当期营收增速>0%')
        set_2_1_1 = get_set_on_date_g(df=inc_revenue_ttm_all_stocks, selected_date=selected_date, p=i[2])
        set_2_1_1 = set.intersection(set_2_1_1, set_market_obis)
        grades_of_stocks.loc[set_2_1_1, 'grade'] = grades_of_stocks.loc[set_2_1_1, 'grade'] + 1
        # step2.1.2)且环比变化率>-10%
        print('且环比变化率>-10%')
        set_2_1_2 = get_set_on_date_g(df=inc_revenue_ttm_ring_growth, selected_date=selected_date, p=i[3])
        set_2_1_2 = set.intersection(set_2_1_2, set_market_obis)
        grades_of_stocks.loc[set_2_1_2, 'grade'] = grades_of_stocks.loc[set_2_1_2, 'grade'] + 1
        # 目标2.2), step2.2当期毛利率环比变化率>0%；
        print('step2.2当期毛利率环比变化率>0%')
        set_2_2 = get_set_on_date_g(df=gross_profit_margin_ttm_ring_growth, selected_date=selected_date, p=i[4])
        set_2_2 = set.intersection(set_2_2, set_market_obis)
        grades_of_stocks.loc[set_2_2, 'grade'] = grades_of_stocks.loc[set_2_2, 'grade'] + 1
        # 目标3.1)， step3.1.1净利润及趋势：当期扣非净利润增速位于0%-400%，
        print('step3.1.1净利润及趋势：当期扣非净利润增速位于0%-400%')
        set_3_1_1 = get_set_on_quarter_b(df=net_profit_deduct_non_recurring_pnl_yoy, selected_quarter=selected_quarter, p=i[5])
        set_3_1_1 = set.intersection(set_3_1_1, set_market_obis)
        grades_of_stocks.loc[set_3_1_1, 'grade'] = grades_of_stocks.loc[set_3_1_1, 'grade'] + 1
        # step3.1.2且环比变化率>-10%；
        print('step3.1.2且环比变化率>-10%；')
        set_3_1_2 = get_set_on_quarter_g(df=net_profit_deduct_non_recurring_pnl_ring_growth, selected_quarter=selected_quarter, p=i[6])
        set_3_1_2 = set.intersection(set_3_1_2, set_market_obis)
        grades_of_stocks.loc[set_3_1_2, 'grade'] = grades_of_stocks.loc[set_3_1_2, 'grade'] + 1
        # step3.2净利润2年复合增速的变化率>-10%
        print('step3.2净利润2年复合增速的变化率>-10%')
        set_3_2 = get_set_on_quarter_g(df=net_profit_2y_growth, selected_quarter=selected_quarter, p=i[7])
        set_3_2 = set.intersection(set_3_2, set_market_obis)
        grades_of_stocks.loc[set_3_2, 'grade'] = grades_of_stocks.loc[set_3_2, 'grade'] + 1
        # 4)step4盈利持续性预判：G-ROE*(1-D)>20个百分点；
        print('step4盈利持续性预判：G-ROE*(1-D)>20个百分点')
        set_4 = get_set_on_date_g(df=profit_trend, selected_date=selected_date, p=i[8])
        set_4 = set.intersection(set_4, set_market_obis)
        grades_of_stocks.loc[set_4, 'grade'] = grades_of_stocks.loc[set_4, 'grade'] + 1
        # 5)资产质量和估值：step5.1负债率<80%。资产负债率,各股大致每两个月更新一次
        print('资产质量和估值：step5.1负债率<80%。资产负债率,各股大致每两个月更新一次')
        set_5_1 = get_set_on_date_l(df=debt_to_asset_ratio_ttm_all_stocks, selected_date=selected_date, p=i[9])
        set_5_1 = set.intersection(set_5_1, set_market_obis)
        grades_of_stocks.loc[set_5_1, 'grade'] = grades_of_stocks.loc[set_5_1, 'grade'] + 1
        # 5.2）step5.2商誉总资产比<20%.
        print('step5.2商誉总资产比<20%.')
        set_5_2 = get_set_on_date_l(df=intangible_asset_ratio_ttm_all_stocks, selected_date=selected_date, p=i[10])
        set_5_2 = set.intersection(set_5_2, set_market_obis)
        grades_of_stocks.loc[set_5_2, 'grade'] = grades_of_stocks.loc[set_5_2, 'grade'] + 1
        # 5.3) step5.3 TTM经营现金流>0；用每股经营现金流替代实现,各股大致每两个月更新一次
        print('step5.3 TTM经营现金流>0；用每股经营现金流替代实现,各股大致每两个月更新一次')
        set_5_3 = get_set_on_date_g(df=operating_cash_flow_per_share_ttm_all_stocks, selected_date=selected_date, p=i[11])
        set_5_3 = set.intersection(set_5_3, set_market_obis)
        grades_of_stocks.loc[set_5_3, 'grade'] = grades_of_stocks.loc[set_5_3, 'grade'] + 1
        # 5.4) step5.4 PE<80。PE用前面的pe_ratio_all_stocks,动态pe
        print('step5.4 PE<80。PE用前面的pe_ratio_all_stocks,动态pe')
        set_5_4 = get_set_on_date_l(df=pe_ratio_all_stocks, selected_date=selected_date, p=i[12])
        set_5_4 = set.intersection(set_5_4, set_market_obis)
        grades_of_stocks.loc[set_5_4, 'grade'] = grades_of_stocks.loc[set_5_4, 'grade'] + 1
        # 5.5）step 5.5 市值行业分位>30%，即大于70%的个股
        print('step 5.5 市值行业分位>30%，即大于70%的个股')
        market_val_in_circulation_based_on_indus_5_5 = get_market_val_in_circulation_based_on_indus_5_5(df=a_share_market_val_in_circulation_all_stocks, df_name='a_share_market_val_in_circulation_all_stocks', selected_date=selected_date, stocks_of_all_indus=stocks_of_all_indus, p_5_5=70)
        union_indus_set_5_5 = {obi for v in market_val_in_circulation_based_on_indus_5_5.values() for obi in v}
        union_indus_set_5_5 = set.intersection(union_indus_set_5_5, set_market_obis)
        grades_of_stocks.loc[union_indus_set_5_5, 'grade'] = grades_of_stocks.loc[union_indus_set_5_5, 'grade'] + 1

    # 建仓股
        # 取打分表中分值前n的obi
        # 重新定义stock_amount_picked_from_grades
        stock_amount_picked_from_grades = len(grades_of_stocks[grades_of_stocks['grade'] >= threshold_grade])
        df_selected_based_on_grades = grades_of_stocks.sort_values(by='grade', axis=0, ascending=False, inplace=False, na_position='last').iloc[0:stock_amount_picked_from_grades, ]
        set_based_on_grades = set(df_selected_based_on_grades.index)
        industries_of_set_based_on_grades = industry_all_days_all_stocks.loc[selected_date, df_selected_based_on_grades.index]
        df_selected_based_on_grades['indus'] = industries_of_set_based_on_grades

        df_selected_based_on_grades['position_amount_ratio'] = df_selected_based_on_grades['grade'] / df_selected_based_on_grades.grade.sum()
        df_selected_based_on_grades['planed_position_amount'] = df_selected_based_on_grades['position_amount_ratio'] * total_position_amount
        df_selected_based_on_grades['close'] = close_stocks.loc[selected_date, df_selected_based_on_grades.index]
        df_selected_based_on_grades['planed_position_shares'] = df_selected_based_on_grades['planed_position_amount'] / df_selected_based_on_grades['close']
        get_lots_from_shares_vec = np.vectorize(get_lots_from_shares)
        df_selected_based_on_grades['executed_position_board_lots'] = get_lots_from_shares_vec(df_selected_based_on_grades.index, df_selected_based_on_grades['planed_position_shares'])
        df_selected_based_on_grades['executed_position_amount'] = df_selected_based_on_grades['executed_position_board_lots'] * 100 * df_selected_based_on_grades['close']
        symbol_list = [instruments(i).symbol for i in df_selected_based_on_grades.index]
        df_selected_based_on_grades['symbol'] = symbol_list
        df_selected_based_on_grades.to_csv(f'C:/Users/zhili/Desktop/history_data/{selected_date}total_amount{total_position_amount}threshold_grade{threshold_grade}df_selected_based_on_grades.csv', sep=',', index=True, header=True, encoding='utf_8_sig')
        print('industries, their grades sum and their counts', df_selected_based_on_grades.groupby(by=['indus'])['grade'].agg([np.sum, np.count_nonzero]))
        # # 按大类目标取交集
        # union_indus_set_1, dict_of_selected_sets_based_on_indus_1 = get_union_indus_set_1(set_1_2, roe_ttm_based_on_indus_1_1, industries=citics_2019)
        # selected_set_2 = set.intersection(set_2_1_1, set_2_1_2, set_2_2)
        # print('set.intersection(set_2_1_1, set_2_1_2, set_2_2)', len(selected_set_2))
        # selected_set_3 = set.intersection(set_3_1_1, set_3_1_2, set_3_2)
        # print('set.intersection(set_3_1_1, set_3_1_2, set_3_2)', len(selected_set_3))
        # selected_set_4 = set.intersection(set_4)
        # print('set.intersection(set_4)', len(selected_set_4))
        # union_indus_set_5, dict_of_selected_sets_based_on_indus_5 = get_union_indus_set_5(set_5_1, set_5_2, set_5_3, set_5_4, market_val_in_circulation_based_on_indus_5_5, industries=citics_2019)
        # # 按各行业用5个目标取交集
        # dict_of_selected_sets_based_on_indus = get_dict_of_selected_sets_based_on_indus(roe_ttm_based_on_indus_1_1, set_1_2,
        #                                                                                 set_2_1_1, set_2_1_2, set_2_2,
        #                                                                                 set_3_1_1, set_3_1_2, set_3_2,
        #                                                                                 set_4, set_5_1, set_5_2, set_5_3,
        #                                                                                 set_5_4,
        #                                                                                 market_val_in_circulation_based_on_indus_5_5)
        #
        # # 将各行业取并集,再用5个目标取交集
        # union_indus_set = set.intersection(selected_set_2, selected_set_3, selected_set_4, union_indus_set_1,
        #                                       union_indus_set_5)

        # # 将各行业取并集,再用5个目标取交集
        # set_without_1 = set.intersection(selected_set_2, selected_set_3, selected_set_4, union_indus_set_5)
        # set_without_2 = set.intersection(selected_set_3, selected_set_4, union_indus_set_1, union_indus_set_5)
        # set_without_3 = set.intersection(selected_set_2, selected_set_4, union_indus_set_1, union_indus_set_5)
        # set_without_4 = set.intersection(selected_set_2, selected_set_3, union_indus_set_1, union_indus_set_5)
        # set_without_5 = set.intersection(selected_set_2, selected_set_3, selected_set_4, union_indus_set_1)
        # union_without12345 = set()
        # union_without12345 = union_without12345.union(set_without_1, set_without_2, set_without_3, set_without_4, set_without_5)

        # 回测####################################
        if len(open_close_dates) > 2:
            # 回测集合1.
            # 建仓集合
            indus_stocks_list = list(set_based_on_grades)
            # 每日权重
            ori_weight_df = pd.DataFrame(index=open_close_dates, columns=indus_stocks_list)


            # # 权重分配方式1. 资金初始均匀权重，资金每日末再平衡为初始均匀权重
            # for d, row in ori_weight_df.iterrows():
            #     ori_weight_df.loc[d] = 1 / len(indus_stocks_list)


            # 权重分配方式2. 资金初始按打分赋权重，资金每日末再平衡为初始得分权重
            grade_sum = df_selected_based_on_grades.grade.sum()
            w_df = df_selected_based_on_grades.loc[indus_stocks_list, 'grade'] / grade_sum
            for d, row in ori_weight_df.iterrows():
                ori_weight_df.loc[d] = w_df


            # 收益率图
            net_value_ts_df = calculate_cumprod_return(stocks_list=indus_stocks_list, w_ori_df=ori_weight_df, s_date=backtest_open_date, e_date=backtest_close_date, label_string='set_based_on_grades' + ' stocks_amount:' + f'{stock_amount_picked_from_grades}')


            # 回测集合2.
            if set_tao_final:
                days_needed = get_days_needed_for_profit(close_stocks=close_stocks,
                                                                                              selected_date=selected_date,
                                                                                              obis=list(
                                                                                                  set_tao_final))
                days_needed_for_profit_tao_intersect_with_grades = days_needed_for_profit_tao_intersect_with_grades.append(days_needed)

                # 建仓集合
                set_tao_final = set.intersection(set_tao_final, set)
                indus_stocks_list = list(set_tao_final)
                # 每日权重
                ori_weight_df = pd.DataFrame(index=open_close_dates, columns=indus_stocks_list)

                # # 权重分配方式1. 资金初始均匀权重，资金每日末再平衡为初始均匀权重
                # for d, row in ori_weight_df.iterrows():
                #     ori_weight_df.loc[d] = 1 / len(indus_stocks_list)

                # 权重分配方式2. 资金初始按打分赋权重，资金每日末再平衡为初始得分权重
                grade_sum = df_selected_based_on_grades.loc[indus_stocks_list, 'grade'].sum()
                w_df = df_selected_based_on_grades.loc[indus_stocks_list, 'grade'] / grade_sum
                for d, row in ori_weight_df.iterrows():
                    ori_weight_df.loc[d] = w_df

                net_value_ts_df_tao = calculate_cumprod_return(stocks_list=indus_stocks_list, w_ori_df=ori_weight_df,
                                                           s_date=backtest_open_date, e_date=backtest_close_date, label_string='set_tao_final')
            else:
                print(selected_date, ' no set_tao_final')


            # 回测集合3.
            set_tao_intersects_with_grades = set.intersection(set_tao_final, set_based_on_grades)
            if set_tao_intersects_with_grades:
                days_needed = get_days_needed_for_profit(close_stocks=close_stocks, selected_date=selected_date,
                                                                    obis=list(set_tao_intersects_with_grades))
                days_needed_for_profit_tao_final = days_needed_for_profit_tao_final.append(
                    days_needed)

                # 建仓集合
                indus_stocks_list = list(set_tao_intersects_with_grades)
                # 每日权重
                ori_weight_df = pd.DataFrame(index=open_close_dates, columns=indus_stocks_list)

                # # 权重分配方式1. 资金初始均匀权重，资金每日末再平衡为初始均匀权重
                # for d, row in ori_weight_df.iterrows():
                #     ori_weight_df.loc[d] = 1 / len(indus_stocks_list)

                # 权重分配方式2. 资金初始按打分赋权重，资金每日末再平衡为初始得分权重
                grade_sum = df_selected_based_on_grades.loc[indus_stocks_list, 'grade'].sum()
                w_df = df_selected_based_on_grades.loc[indus_stocks_list, 'grade'] / grade_sum
                for d, row in ori_weight_df.iterrows():
                    ori_weight_df.loc[d] = w_df


                net_value_ts_df_tao_intersects_with_grades = calculate_cumprod_return(stocks_list=indus_stocks_list, w_ori_df=ori_weight_df,
                                                           s_date=backtest_open_date, e_date=backtest_close_date, label_string='set_tao_intersects_with_grades')
            else:
                print(selected_date, ' no set_tao_intersects_with_grades')


            # 回测集合4.
            set_too_low_gap_done = set.intersection(set_too_low_gap_done, set_based_on_grades)
            if set_too_low_gap_done:
                days_needed = get_days_needed_for_profit(close_stocks=close_stocks, selected_date=selected_date,
                                                                    obis=list(set_too_low_gap_done))
                days_needed_for_profit_tong = days_needed_for_profit_tong.append(days_needed)

                # 建仓集合
                indus_stocks_list = list(set_too_low_gap_done)
                # 每日权重
                ori_weight_df = pd.DataFrame(index=open_close_dates, columns=indus_stocks_list)

                # # 权重分配方式1. 资金初始均匀权重，资金每日末再平衡为初始均匀权重
                # for d, row in ori_weight_df.iterrows():
                #     ori_weight_df.loc[d] = 1 / len(indus_stocks_list)

                # 权重分配方式2. 资金初始按打分赋权重，资金每日末再平衡为初始得分权重
                grade_sum = df_selected_based_on_grades.loc[indus_stocks_list, 'grade'].sum()
                w_df = df_selected_based_on_grades.loc[indus_stocks_list, 'grade'] / grade_sum
                for d, row in ori_weight_df.iterrows():
                    ori_weight_df.loc[d] = w_df

                net_value_ts_df_tong = calculate_cumprod_return(stocks_list=list(set_too_low_gap_done), w_ori_df=ori_weight_df,
                                                               s_date=backtest_open_date, e_date=backtest_close_date, label_string='set_tong')
            else:
                print(selected_date, ' no set_tong')

            # 基准指数
            i_000905_return_ts = 1 + i_000905
            i_000905_net_value_ts_ready = i_000905_return_ts.loc[backtest_open_date: backtest_close_date, '000905.XSHG']
            i_000905_net_value_ts = i_000905_net_value_ts_ready.cumprod()
            plt.plot(i_000905_net_value_ts - 1, label='i_000905')

            # 相对收益率
            # 集合一
            if indus_stocks_list:
                grades_relative = net_value_ts_df.sum_return - i_000905_net_value_ts
                plt.plot(grades_relative, label='grades_relative')
            # 集合二
            if set_tao_final:
                tao_final_relative = net_value_ts_df_tao.sum_return - i_000905_net_value_ts
                plt.plot(tao_final_relative, label='tao_final_relative')
            # 集合三
            if set_tao_intersects_with_grades:
                tao_intersects_relative = net_value_ts_df_tao_intersects_with_grades.sum_return - i_000905_net_value_ts
                plt.plot(tao_intersects_relative, label='tao_intersects_relative')
            # 集合四
            if set_too_low_gap_done:
                tong_relative = net_value_ts_df_tong.sum_return - i_000905_net_value_ts
                plt.plot(tong_relative, label='tong_relative')



            # 全部线画在一幅图上之后，再统一加legend，才能避免各条线被分别画在不同图
            num1 = 1
            num2 = 0
            num3 = 3
            num4 = 0
            plt.legend(bbox_to_anchor=(num1, num2), loc=num3, borderaxespad=num4)
            plt.title(f'net_value from {selected_date} to {backtest_close_date}')
            plt.savefig(f'{path}pics/from_{selected_date}_to_{backtest_close_date}.jpg')
            # 清除上次图像，且保留作图窗口，等待下次重新画
            plt.clf()

            # 输出参数txt文件
            out_txt = open(f'{path}pics/para_from_{selected_date}_to_{backtest_close_date}.txt', 'w')
            print(f'selected_quarter: {selected_quarter}', end='\n', file=out_txt)
            print(f'selected_date: {selected_date}', end='\n', file=out_txt)
            print(f'executing_endQuarter: {executing_endQuarter}', end='\n', file=out_txt)
            print(f'executing_beginQuarter: {executing_beginQuarter}', end='\n', file=out_txt)
            print(f'executing_q_to_d_begin_date: {executing_q_to_d_begin_date}', end='\n', file=out_txt)
            out_txt.close()


# 看个股的基本面情况
def give_grades(obis_in_pipe_list):
    f = open(f'{path}give_grades_{selected_date}.txt', 'w')
    set_obi_in_question = set(obis_in_pipe_list) - set(grades_of_stocks.index)
    if set_obi_in_question:
        print('obi in QUESTION!', set_obi_in_question)
        obis = set(obis_in_pipe_list) - set_obi_in_question
    else:
        obis = obis_in_pipe_list
    gd_temp = grades_of_stocks.loc[obis, 'grade']
    print(gd_temp)
    print(gd_temp, end='\n', file=f)
    # temp['symbol'] = temp.apply(lambda r: instruments(r.index).symbol)
    for obi in obis:
        print(obi, instruments(obi).symbol, grades_of_stocks.loc[obi, 'grade'], end='\n', file=f)
        print(obi in union_indus_set_1_1, 'step1.1)当期ROE(TTM)位于行业前30%，即大于70%的个股:', end='\n', file=f)
        print(obi in set_1_2, 'step1.2)且最近两期ROE环比变化率 > 0 % （环比变化率 = 当期 / 上期 - 1）', end='\n', file=f)
        print(obi in set_2_1_1, 'step2.1.1)营收及趋势：当期营收增速>0%', end='\n', file=f)
        print(obi in set_2_1_2, '且环比变化率>-10%', end='\n', file=f)
        print(obi in set_2_2, 'step2.2当期毛利率环比变化率>0%', end='\n', file=f)
        print(obi in set_3_1_1, 'step3.1.1净利润及趋势：当期扣非净利润增速位于0%-400%', end='\n', file=f)
        print(obi in set_3_1_2, 'step3.1.2且环比变化率>-10%；', end='\n', file=f)
        print(obi in set_3_2, 'step3.2净利润2年复合增速的变化率>-10%', end='\n', file=f)
        print(obi in set_4, 'step4盈利持续性预判：G-ROE*(1-D)>20个百分点', end='\n', file=f)
        print(obi in set_5_1, 'step5.1负债率<80%。资产负债率,各股大致每两个月更新一次', end='\n', file=f)
        print(obi in set_5_2, 'step5.2商誉总资产比<20%.', end='\n', file=f)
        print(obi in set_5_3, 'step5.3 TTM经营现金流>0；用每股经营现金流替代实现,各股大致每两个月更新一次', end='\n', file=f)
        print(obi in set_5_4, 'step5.4 PE<80。PE用前面的pe_ratio_all_stocks,动态pe', end='\n', file=f)
        print(obi in union_indus_set_5_5, 'step 5.5 市值行业分位>30%，即大于70%的个股', end='\n', file=f)
    f.close()
# give_grades(obis_in_pipe_list=['600196.XSHG', '688298.XSHG', '601026.XSHG', '603011.XSHG', '300809.XSHE', '601882.XSHG', '300161.XSHE', '002248.XSHE'])


# 看个股所属行业
selected_obis_and_indus = [(i.order_book_id, i.citics_industry_name) for i in instruments(set_based_on_grades) if i.citics_industry_name == '食品饮料']

#
chicang_list = ['605199.XSHG',
'300661.XSHE',
'300841.XSHE',
'002737.XSHE',
'002727.XSHE',
'002603.XSHE',
'603883.XSHG',
'002852.XSHE',
'002050.XSHE',
'605266.XSHG',
'603939.XSHG',
'300253.XSHE',
'600221.XSHG',
'000887.XSHE',
'300073.XSHE',
'301071.XSHE',
'603713.XSHG']