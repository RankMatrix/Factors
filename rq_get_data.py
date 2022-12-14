# import pandas as pd
from constant import beginDate, endDate, market_obis, not_fully_listed, base_data_dates, time_to_date_targets1, time_to_date_targets2
from global_fun import *
from rqdatac import *
import rqdatac as rq
rq.init()




# 显示所有列
pd.set_option('display.max_columns', None)


i_000905 = get_price_change_rate('000905.XSHG', start_date=beginDate, end_date=endDate, expect_df=True)
# 无风险收益率选取的是r001,落地格式为index为日期，列名为close，值为百分化后的小数
risk_free_rate = get_price('131810.XSHE', start_date=beginDate, end_date=endDate, frequency='1d',
                           fields='close', expect_df=True).reset_index().loc[:, ['date', 'close']].set_index('date') / 100
# risk_free_rate = pd.DataFrame()
# 收盘价
close_stocks = get_price(market_obis, start_date=beginDate, end_date=date.today(), frequency='1d', fields='close', adjust_type='pre', skip_suspended=False, market='cn', expect_df=True, time_slice=None).unstack(level=0).close

# 取出各个指标
# 缺口
h_of_stocks = get_price(market_obis, start_date=beginDate, end_date=endDate, frequency='1d', fields='high', adjust_type='pre', skip_suspended=False, market='cn', expect_df=True, time_slice=None).unstack(level=0).high
l_of_stocks = get_price(market_obis, start_date=beginDate, end_date=endDate, frequency='1d', fields='low', adjust_type='pre', skip_suspended=False, market='cn', expect_df=True, time_slice=None).unstack(level=0).low
limit_up_stocks = get_price(market_obis, start_date=beginDate, end_date=endDate, frequency='1d', fields='limit_up', adjust_type='pre', skip_suspended=False, market='cn', expect_df=True, time_slice=None).unstack(level=0).limit_up
limit_down_stocks = get_price(market_obis, start_date=beginDate, end_date=endDate, frequency='1d', fields='limit_down', adjust_type='pre', skip_suspended=False, market='cn', expect_df=True, time_slice=None).unstack(level=0).limit_down
# 下跌形成缺口
too_low_ready = h_of_stocks - l_of_stocks.shift(1)
# 上涨形成缺口
too_high_ready = l_of_stocks - h_of_stocks.shift(1)
# 下跌至跌停
down_to_limit_down_ready = close_stocks.loc[:, market_obis] - limit_down_stocks.loc[:, market_obis]
# 上涨至涨停
up_to_limit_up_ready = close_stocks.loc[:, market_obis] - limit_up_stocks.loc[:, market_obis]
# 默认为当前最新行业
# 设定industry_all_days_all_stocks的索引为日期，后续不用datetime转date
industry_all_days_all_stocks = pd.DataFrame(columns=market_obis)
print('industry_all_days_all_stocks_starts', datetime.now())
for d in base_data_dates:
    industry_d = get_instrument_industry(market_obis, source='citics_2019', level=1, date=d,
                                         market='cn').first_industry_name.to_frame().T
    industry_d['date'] = d
    industry_d.reset_index(drop=True, inplace=True)
    industry_d = industry_d.set_index('date')
    industry_all_days_all_stocks = industry_all_days_all_stocks.append(industry_d)
print('industry_all_days_all_stocks_ends', datetime.now())
# 个股隔日收益率get_price_change_rate函数是基于后复权价格
pct_chg_all_stocks = get_price_change_rate(market_obis, start_date=beginDate, end_date=endDate, expect_df=True)
volume_all_stocks = get_price(market_obis, start_date=beginDate, end_date=endDate, fields='volume', adjust_type='pre',
                              skip_suspended=False, market='cn', expect_df=True).unstack(0).volume
a_share_market_val_in_circulation_all_stocks = get_factor(market_obis, 'a_share_market_val_in_circulation',
                                                          start_date=beginDate, end_date=endDate, expect_df=True).unstack(
    0).a_share_market_val_in_circulation
pe_ratio_all_stocks = get_factor(market_obis, 'pe_ratio', start_date=beginDate, end_date=endDate, expect_df=True).unstack(0).pe_ratio
pb_ratio_lf_all_stocks = get_factor(market_obis, 'pb_ratio_lf', start_date=beginDate, end_date=endDate, expect_df=True).unstack(
    0).pb_ratio_lf
# 有的个股没披露该数据，比如平安银行
non_current_liabilities_all_stocks = get_factor(market_obis, 'non_current_liabilities', start_date=beginDate,
                                                end_date=endDate, expect_df=True).unstack(0).non_current_liabilities
net_profit_growth_ratio_ttm_all_stocks = get_factor(market_obis, 'net_profit_growth_ratio_ttm', start_date=beginDate,
                                                    end_date=endDate, expect_df=True).unstack(0).net_profit_growth_ratio_ttm
operating_revenue_growth_ratio_ttm_all_stocks = get_factor(market_obis, 'operating_revenue_growth_ratio_ttm',
                                                           start_date=beginDate, end_date=endDate, expect_df=True).unstack(
    0).operating_revenue_growth_ratio_ttm
debt_to_asset_ratio_ttm_all_stocks = get_factor(market_obis, 'debt_to_asset_ratio_ttm', start_date=beginDate,
                                                end_date=endDate, expect_df=True).unstack(0).debt_to_asset_ratio_ttm

# 取数20221104
# 目标1)	ROE及趋势：当期ROE(TTM)位于行业前30%，且最近两期ROE环比变化率>0% （环比变化率=当期/上期-1）；行业用前面算的industry_all_stocks
# roe_ttm大致每个月调整一次，各股调整日期互不相同，相邻两期之比求得环比变化率
du_roe_ttm_all_stocks = get_factor(market_obis, 'du_return_on_equity_ttm', start_date=beginDate,
                                   end_date=endDate, expect_df=True).unstack(0).du_return_on_equity_ttm


# 目标2.1)营收及趋势：当期营收增速>0%，且环比变化率>-10%
# 营收年增速ttm，大致每个月调整一次，各股调整日期互不相同
inc_revenue_ttm_all_stocks = get_factor(market_obis, 'inc_revenue_ttm', start_date=beginDate, end_date=endDate, expect_df=True).unstack(
    0).inc_revenue_ttm

# 目标2.2)当期毛利率环比变化率>0%；
# 毛利率=(营业收入ttm - 营业成本ttm）/ 营业收入ttm，再对其做环比计算
gross_profit_margin_ttm_all_stocks = get_factor(market_obis, 'gross_profit_margin_ttm', start_date=beginDate,
                                                end_date=endDate, expect_df=True).unstack(0).gross_profit_margin_ttm

# 3.1)净利润及趋势：当期扣非净利润增速位于0%-400%，且环比变化率>-10%；
# 扣除非经常性损益后的净利润,再计算同比、环比
# index是'2019q3'字符串格式，不是datetime格式，后续不用转换为date格式
# net_profit_deduct_non_recurring_pnl_all_stocks = get_pit_financials_ex(fields=['net_profit_deduct_non_recurring_pnl'],
#                                                                        start_quarter=beginQuarter,
#                                                                        end_quarter=endQuarter,
#                                                                        order_book_ids=market_obis).unstack(
#     0).net_profit_deduct_non_recurring_pnl
# # 环比
# print('net_profit_deduct_non_recurring_pnl_ring_growth starts')
# net_profit_deduct_non_recurring_pnl_ring_growth = get_ring_growth(net_profit_deduct_non_recurring_pnl_all_stocks)
# # 同比
# print('net_profit_deduct_non_recurring_pnl_yoy starts')
# net_profit_deduct_non_recurring_pnl_yoy = get_ring_growth(net_profit_deduct_non_recurring_pnl_all_stocks, lag_p=4)
# 3.2)净利润2年复合增速的变化率>-10%。
# 净利润net_profit在current_performance中不提供，只能从get_pit_financials_ex中取。净利润每年末的同比ne_t_minority_ty_yoy可以从current_performance中取得，和自己计算的net_profit_deduct_non_recurring_pnl_yoy相符，current_performance不提供季度同比，且有的个股如'000002.XSHE'的扣非后净利润未在current_performance中提供，弃用current_performance
# 根据净利润来计算2年复合增速
# index是'2019q3'字符串格式，不是datetime格式，后续不用转换为date格式
# net_profit_all_stocks = get_pit_financials_ex(fields=['net_profit'], start_quarter=beginQuarter, end_quarter=endQuarter,
#                                               order_book_ids=market_obis).unstack(0).net_profit
# print('net_profit_2y_growth starts')
# net_profit_2y_growth = get_ring_growth(net_profit_all_stocks, lag_p=8)
# 经营净利润率=净利润ttm / 营业总收入ttm
# net_profit_to_revenue_ttm_all_stocks = get_factor(market_obis,'net_profit_to_revenue_ttm',start_date=beginDate,end_date=endDate).unstack(0).net_profit_to_revenue_ttm

# 4)盈利持续性预判：G-ROE*(1-D)>20个百分点；
# 如何判断ROE的趋势？ROE的分子是盈利、分母是净资产，也就是意味着，如果分子盈利的增速比分母净资产的增长速度要快，那就意味着短期来看，一个行业或者公司的ROE可以继续提升。分子盈利的增速就是净利润累计同比增速（用g代替）、分母净资产的增速可用前一年的ROE*（1-d）（d为分红比例）。
# 分子G=利用3.2）算出的当期净利润的同比数据。分母=同期数据的ROE*（1-d),其中d=dividend_yield_ttm即连续四季度报表公布股利之和 / 公司当前股票总市值
# 分红率
dividend_yield_ttm_all_stocks = get_factor(market_obis, 'dividend_yield_ttm', start_date=beginDate,
                                           end_date=endDate, expect_df=True).unstack(0).dividend_yield_ttm
# net_profit先计算同比，再将quarter时序扩展成日时序
# print('net_profit_yoy starts')
# net_profit_yoy = get_ring_growth(net_profit_all_stocks, lag_p=4)
# net_profit_yoy_dict_after_drop_dup = dict()
# for i in market_obis:
#     net_profit_yoy_dict_after_drop_dup[i] = pd.DataFrame(net_profit_yoy[i])
# print('net_profit_yoy_days starts')
# net_profit_yoy_days = convert_quarter_to_full(quarter_day_value_dict=net_profit_yoy_dict_after_drop_dup,
#                                               days=scores_dates)
# profit_trend = net_profit_yoy_days - du_roe_ttm_all_stocks * (1 - dividend_yield_ttm_all_stocks)
# 5)资产质量和估值：
# 5.1）负债率<80%。资产负债率,各股大致每两个月更新一次,用前面算的debt_to_asset_ratio_ttm_all_stocks

# 5.2）商誉总资产比<20%.未专门找出商誉。无形资产比率ttm=(无形资产ttm+开发支出ttm+商誉ttm)/资产总计ttm.资产负债率,各股大致每两个月更新一次
intangible_asset_ratio_ttm_all_stocks = get_factor(market_obis, 'intangible_asset_ratio_ttm', start_date=beginDate,
                                                   end_date=endDate, expect_df=True).unstack(0).intangible_asset_ratio_ttm
# 5.3)TTM经营现金流>0；用每股经营现金流替代实现,各股大致每两个月更新一次
operating_cash_flow_per_share_ttm_all_stocks = get_factor(market_obis, 'operating_cash_flow_per_share_ttm',
                                                          start_date=beginDate, end_date=endDate, expect_df=True).unstack(
    0).operating_cash_flow_per_share_ttm
# 5.4)PE<80。PE用前面的pe_ratio_all_stocks(注意不再是pe_ttm)
# 5.5）市值行业分位>30%。市值用前面的a_share_market_val_in_circulation_all_stocks，行业用前面算的industry_all_stocks


time_to_date_targets = time_to_date_targets1 + time_to_date_targets2

print(datetime.now(), "transform datetime to date")
for i in time_to_date_targets:
    print(i, 'completing datetime to date')
    datetime_index_to_date_index(eval(i))
print(datetime.now(), "transforming ends")


# def update_raw_data_of_obis(order_book_ids):
#     """
#     生成字典obi_with_raw_data_dict，该字典的key存放不同的股票代码，该字典的value存放该股票的因子时序df，该df列名是指标名，行名是日期
#     :param order_book_ids:一个list，里面装不同obi字符串
#     :return:
#     """
#     obi_with_raw_data_dict = dict()
#     # 遍历每一支股票
#     print(datetime.now(), "start_to_traverse_all_stocks")
#     for obi in order_book_ids:
#         one_stock_df = pd.DataFrame(index=base_data_dates, data=None)
#         # 行业
#         if obi in industry_all_days_all_stocks.columns:
#             one_stock_df['industry'] = industry_all_days_all_stocks[obi]
#         else:
#             print(obi, "not in industry_all_days_all_stocks")
#             pass
#         # 总市值
#         if obi in a_share_market_val_in_circulation_all_stocks.columns:
#             one_stock_df['a_share_market_val_in_circulation'] = a_share_market_val_in_circulation_all_stocks[obi]
#         else:
#             print(obi, "not in market_cap_2_all_stocks")
#             pass
#         # 市盈率
#         if obi in pe_ratio_all_stocks.columns:
#             one_stock_df['pe_ratio'] = pe_ratio_all_stocks[obi]
#         else:
#             print(obi, "not in pe_ratio_all_stocks")
#             pass
#         # 市净率lf
#         if obi in pb_ratio_lf_all_stocks.columns:
#             one_stock_df['pb_ratio_lf'] = pb_ratio_lf_all_stocks[obi]
#         else:
#             print(obi, "not in pb_ratio_lf_all_stocks")
#             pass
#         # 非流动负债合计
#         if obi in non_current_liabilities_all_stocks.columns:
#             one_stock_df['non_current_liabilities'] = non_current_liabilities_all_stocks[obi]
#         else:
#             print(obi, "not in non_current_liabilities_all_stocks")
#             pass
#         # 净利润同比增长率ttm
#         if obi in net_profit_growth_ratio_ttm_all_stocks.columns:
#             one_stock_df['net_profit_growth_ratio_ttm'] = net_profit_growth_ratio_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in net_profit_growth_ratio_ttm_all_stocks")
#             pass
#         # 营业收入同比增长率ttm
#         if obi in operating_revenue_growth_ratio_ttm_all_stocks.columns:
#             one_stock_df['operating_revenue_growth_ratio_ttm'] = operating_revenue_growth_ratio_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in operating_revenue_growth_ratio_ttm_all_stocks")
#             pass
#         # 资产负债率ttm
#         if obi in debt_to_asset_ratio_ttm_all_stocks.columns:
#             one_stock_df['debt_to_asset_ratio_ttm'] = debt_to_asset_ratio_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in debt_to_asset_ratio_ttm_all_stocks")
#             pass
#         # 隔日收益率
#         if obi in pct_chg_all_stocks.columns:
#             one_stock_df['pct_chg'] = pct_chg_all_stocks[obi]
#         else:
#             # 上市第一天不存在隔日收益率或存在隔日收益率但截至运行程序时rq还未将其计算出来
#             print(obi, "not in pct_chg_all_stocks")
#             pass
#         # 不含大宗交易的成交量
#         if obi in volume_all_stocks.columns:
#             one_stock_df['pct_chg'] = volume_all_stocks[obi]
#         else:
#             print(obi, "not in volume_all_stocks")
#             pass
#         # 总股本 和 经营活动现金流量净额(元)
#         stats_from_current_performance = ['total_shares', 'net_operate_cashflow']
#         obi_total_shares = current_performance(obi, info_date=endDate, interval='50y',
#                                                fields=stats_from_current_performance)
#         if obi_total_shares is not None:
#             obi_total_shares = obi_total_shares.set_index('end_date')
#             one_stock_df = pd.merge(one_stock_df, obi_total_shares[stats_from_current_performance], left_index=True,
#                                     right_index=True, how='outer')
#             one_stock_df.fillna(method='pad', axis=0, inplace=True)
#             one_stock_df = one_stock_df.loc[base_data_dates]
#         else:
#             one_stock_df['total_shares'] = np.nan
#         # 取数20221104
#         # 杜邦roe_ttm
#         if obi in du_roe_ttm_all_stocks.columns:
#             one_stock_df['du_roe_ttm'] = du_roe_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in du_roe_ttm_all_stocks")
#             pass
#         # 营收年增速ttm
#         if obi in inc_revenue_ttm_all_stocks.columns:
#             one_stock_df['inc_revenue_ttm'] = inc_revenue_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in inc_revenue_ttm_all_stocks")
#             pass
#         # 毛利率ttm
#         if obi in gross_profit_margin_ttm_all_stocks.columns:
#             one_stock_df['gross_profit_margin_ttm'] = gross_profit_margin_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in gross_profit_margin_ttm_all_stocks")
#             pass
#         # # 扣非净利润
#         # if obi in net_profit_deduct_non_recurring_pnl_all_stocks.columns:
#         #     one_stock_df['net_profit_deduct_non_recurring_pnl'] = net_profit_deduct_non_recurring_pnl_all_stocks[obi]
#         # else:
#         #     print(obi, "not in net_profit_deduct_non_recurring_pnl_all_stocks")
#         #     pass
#         # 净利润
#         # if obi in net_profit_all_stocks.columns:
#         #     one_stock_df['net_profit'] = net_profit_all_stocks[obi]
#         # else:
#         #     print(obi, "not in net_profit_all_stocks")
#         #     pass
#         # 分红率
#         if obi in dividend_yield_ttm_all_stocks.columns:
#             one_stock_df['dividend_yield_ttm'] = dividend_yield_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in dividend_yield_ttm_all_stocks")
#             pass
#         # 资产负债率
#         if obi in debt_to_asset_ratio_ttm_all_stocks.columns:
#             one_stock_df['debt_to_asset_ratio_ttm'] = debt_to_asset_ratio_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in debt_to_asset_ratio_ttm_all_stocks")
#             pass
#         # 无形资产比率ttm
#         if obi in intangible_asset_ratio_ttm_all_stocks.columns:
#             one_stock_df['intangible_asset_ratio_ttm'] = intangible_asset_ratio_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in intangible_asset_ratio_ttm_all_stocks")
#             pass
#         # 每股经营现金流
#         if obi in operating_cash_flow_per_share_ttm_all_stocks.columns:
#             one_stock_df['operating_cash_flow_per_share_ttm'] = operating_cash_flow_per_share_ttm_all_stocks[obi]
#         else:
#             print(obi, "not in operating_cash_flow_per_share_ttm_all_stocks")
#             pass
#         obi_with_raw_data_dict[obi] = one_stock_df
#     print(datetime.now(), "traversing_ends")
#     # dfs.to_pickle(path + r'\test')
#     return obi_with_raw_data_dict


# outcom_dict = update_raw_data_of_obis(market_obis)
# print(outcom_dict['000001.XSHE'])
