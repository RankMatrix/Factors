import numpy as np
from datetime import *
import pandas as pd
from functools import reduce
from rqdatac import *
from global_fun import datetime_index_to_date_index

# # data_source = 'wind'
data_source = 'rq'
if data_source == 'rq':
    # 如果数据源用rq
    import rqdatac as rq
    rq.init()
    from rqdatac import *
elif data_source == 'wind':
    # 如果数据源用wind
    from WindPy import *
    w.start()
else:
    pass
# 数据常数
path = 'C:/Users/zhili/Desktop/history_data/'
path_of_scores = 'C:/Users/zhili/Desktop/scores/'
# 生成因子得分所需要的wind或rq数据的取数时间范围
beginDate = date(2013, 1, 1)
endDate = date(2022, 12, 12)
base_data_dates = get_trading_dates(beginDate, endDate)
# 生成的因子得分本身的时间范围
# scores_beginDate = date(2018, 12, 25)
# scores_endDate = endDate
# scores_dates = get_trading_dates(scores_beginDate, scores_endDate)
# 生成因子得分所需要的wind或rq数据的取数的季度范围，适用于季度数据（index为字符串）,季度需要在beginDate、endDate日范围内,如beginDate是20130101，beginQuarter2013q1，那么数据能跑通，但2013q1数据是在20130401至20130430之间陆续出炉
# beginQuarter = '2013q1'
# endQuarter = '2022q3'
# 回测所需参数设定#####################################


# 中信2019一级行业分类
citics_2019 = get_industry_mapping(source='citics_2019', date=None, market='cn').first_industry_name.drop_duplicates().to_list()
# 用于20221104外储的数据名称
data_targets_20221104 = ['i_000905', 'risk_free_rate', 'close_stocks', 'h_of_stocks', 'l_of_stocks', 'too_low_ready', 'too_high_ready', 'down_to_limit_down_ready', 'up_to_limit_up_ready', 'industry_all_days_all_stocks', 'volume_all_stocks', 'a_share_market_val_in_circulation_all_stocks', 'pe_ratio_all_stocks', 'du_roe_ttm_all_stocks', 'inc_revenue_ttm_all_stocks', 'gross_profit_margin_ttm_all_stocks',  'debt_to_asset_ratio_ttm_all_stocks', 'dividend_yield_ttm_all_stocks', 'intangible_asset_ratio_ttm_all_stocks', 'operating_cash_flow_per_share_ttm_all_stocks']
# 以前的因子中需要datetime转date的
time_to_date_targets1 = [
    'i_000905',
    'risk_free_rate',
    'close_stocks',
    'pct_chg_all_stocks',
    'volume_all_stocks',
    'a_share_market_val_in_circulation_all_stocks',
    'pe_ratio_all_stocks',
    'pb_ratio_lf_all_stocks',
    'non_current_liabilities_all_stocks',
    'net_profit_growth_ratio_ttm_all_stocks',
    'operating_revenue_growth_ratio_ttm_all_stocks']
# 取数20221104因子中需要datetime转date的基础数据
time_to_date_targets2 = [
    'h_of_stocks',
    'l_of_stocks',
    'too_low_ready',
    'too_high_ready',
    'down_to_limit_down_ready',
    'up_to_limit_up_ready',
    'du_roe_ttm_all_stocks',
    'inc_revenue_ttm_all_stocks',
    'gross_profit_margin_ttm_all_stocks',
    'dividend_yield_ttm_all_stocks',
    'debt_to_asset_ratio_ttm_all_stocks',
    'intangible_asset_ratio_ttm_all_stocks',
    'operating_cash_flow_per_share_ttm_all_stocks']
# 不随时间更新而更新的基础数据的环同比数据
time_to_date_targets3 = ['du_roe_ttm_ring_growth', 'inc_revenue_ttm_ring_growth', 'gross_profit_margin_ttm_ring_growth']


# 不要加date=，选出含退市的所有股票
# all_stocks = all_instruments(type='CS', market='cn', date=endDate)
all_stocks = all_instruments(type='CS', market='cn')
# 取出全部股票，剔除未知代码
market_obis = all_stocks.loc[~all_stocks['industry_code'].isin(['Unknown'])].order_book_id.to_list()
# 取出所有已退市股票
de_listed_temp = {i.de_listed_date: i.order_book_id for i in instruments(market_obis) if i.de_listed_date != '0000-00-00'}
ind = list(de_listed_temp.keys())
obis = list(de_listed_temp.values())
de_listed_dates_obis_df = pd.DataFrame(columns=['index', 'order_book_id'])
de_listed_dates_obis_df['index'] = ind
de_listed_dates_obis_df['order_book_id'] = obis
de_listed_dates_obis_df.set_index('index', inplace=True)
de_listed_dates_obis_df.index = pd.to_datetime(de_listed_dates_obis_df.index)
de_listed_dates_obis_df = de_listed_dates_obis_df.sort_index(ascending=True)
datetime_index_to_date_index(de_listed_dates_obis_df)
# 取出截至真实今天还未完成正式上市或终止上市的股票
not_fully_listed = {i.order_book_id for i in instruments(market_obis) if i.listed_date == '2999-12-31'}
market_obis = list(set(market_obis) - set(de_listed_temp.values()) - not_fully_listed)
just_fully_listed = {i.order_book_id for i in instruments(market_obis) if pd.to_datetime(i.listed_date).date() == date.today()}
# 最新打分取数的股票
market_obis = list(set(market_obis) - just_fully_listed)
# # 回测打分取数的股票
# market_obis = list(set(market_obis) - not_fully_listed_temp)

# 函数
# def var_name(var, all_var=locals()):
#     """
#     输出变量的变量名本身
#     :param var: 数据或函数变量（非变量名）
#     :param all_var: 字典{变量名(字符串）: 变量}, locals()是一个字典，key是所有变量（包括数据的名称和函数的名称）的名称，value是变量的值
#     :return: var的变量名(字符串）
#     """
#     return [var_name_str for var_name_str in all_var.keys() if all_var[var_name_str] is var][0]


def get_indus_stocks_dict(day, industries):
    """
    获取在指定日期，字典{行业：行业的股票列表}
    :param day: 日期
    :param industries: 类别列表
    :return: {行业：行业的股票列表}
    """
    A = dict()
    for i in industries:
        A[i] = get_industry(i, source='citics_2019', date=day, market='cn')
    return A


# indus_stocks_dict = get_indus_stocks_dict(scores_endDate, citics_2019)





def get_half_life_array(half_life_period, length_of_array):
    """
    返回一个权重array
    1.设第1个值V(1)(当前的值）的权重w(1)=α
    2.指数衰减模式为第1个值（不含）后的第h个值V(1+h)的权重w(1+h)=α(1-α)**h，即相邻衰减率为1-a
    3.半衰期为20的含义是w(1+20)/w(1)=0.5,即(1-α)**20 = 0.5。
    4.根据上式，解出α=1 - 0.5**(1/20) 或写成指数形式 α=1 - e^^(ln(0.5)/h)
    5.按照设定，α也恰好是第1个值的权重
    添加249个数，和0.5一起构成250个半衰指数权重
    """
    half_life_list = [1 - 0.5**(1/half_life_period)]
    for i in range(1, length_of_array, 1):
        add_num = half_life_list[-1] * 0.5**(1/half_life_period)
        half_life_list.append(add_num)
    half_life_list.reverse()
    half_life_array = np.array(half_life_list)
    # 返回长度为250的半衰array
    sum_it = sum(half_life_array)
    half_life_array_normalization = half_life_array/sum_it
    return half_life_array_normalization


if data_source == 'wind':

    def get_trading_dates(start_date, end_date):
        temp = w.tdays(start_date, end_date, "").Data[0]
        return [datetime.date(dt) for dt in temp]


    all_trading_dates = get_trading_dates(date(2010, 1, 1), date.today())

    def get_previous_trading_date(d, p_n=1):
        d_p_n = all_trading_dates[all_trading_dates.index(d) - p_n]
        return d_p_n


    def get_next_trading_date(d, n_n=1):
        d_n_n = all_trading_dates[all_trading_dates.index(d) + n_n]
        return d_n_n

# all_trading_dates = get_trading_dates(date(2000, 1, 1), date.today())


def prod(x, y):
    """用于结合reduce实现累乘"""
    return x * y


def get_longer_return_from_daily_return(daily_return_series, cumulative_period):
    """
    1.返回的是一个series，这个series中的每个数代表对daily_return_series分段求得的累积收益率。
    若只需要返回一个累积收益率，则输入的daily_return_series的长度应当等于cumulative_period，即意味着全部隔日收益率合起来为一段
    2.输入的daily_return_series的前面的index是更早的时间，后面的index是更晚的时间
    3.累计收益率是从最晚的时刻往更早的方向取时间段来计算累计收益率，而非相反方向
    4.仅仅做了复利收益率，并未取对数。如果需要复利的对数收益率，需要对该返回结果取一次对数
    """
    # 按日期降序排列待累乘的数列
    reversed_drs = daily_return_series.sort_index(ascending=False)
    reversed_drs_index_list = reversed_drs.index.to_list()
    len_reversed_drs = len(reversed_drs)
    cum_start_location = [i for i in range(0, len_reversed_drs, 1) if i % cumulative_period == 0]
    cum_start_index = [ind for ind in reversed_drs.index if reversed_drs_index_list.index(ind) in cum_start_location]
    zip_loc_index = zip(cum_start_location, cum_start_index)
    cum_rt_series = pd.Series(data=None, index=cum_start_index, dtype='float64')
    for (loc, ind) in zip_loc_index:
        if loc != cum_start_location[-1]:
            # 取出收益率
            to_cum = reversed_drs[ind:get_previous_trading_date(ind, cumulative_period-1)]
            # 得到1+收益率
            to_cum = to_cum + 1
            cum_rt_series[ind] = reduce(prod, to_cum)
        # 对于最后一小段
        else:
            # 如果正好剩余一整小段的待累乘收益率
            if len(reversed_drs[ind:]) == cumulative_period:
                to_cum = reversed_drs[ind:]
                to_cum = to_cum + 1
                cum_rt_series[ind] = reduce(prod, to_cum)
            # 如果剩余的待累乘收益率个数小于一个小段的长度，那么这个小段不累乘
            else:
                cum_rt_series[ind] = np.nan
    cum_rt_series = cum_rt_series.sort_index(ascending=True)
    return cum_rt_series


def get_stats_from_stocks(obi_dict, trading_dates, obis_list, stats_name):
    """
    :param obi_dict: 全体股票
    :param trading_dates: 取出的股票的时间范围
    :param obis_list: 待取出的股票
    :param stats_name: 取出的指标
    :return: 一个dataframe
    """
    obi_stats_df = pd.DataFrame(data=None, index=trading_dates, columns=obis_list)
    obi_stats_df.index.name = 'date'
    for obi in obis_list:
        if stats_name in obi_dict[obi].columns:
            obi_stats_df.loc[trading_dates, obi] = obi_dict[obi].loc[trading_dates, stats_name]
        else:
            obi_stats_df.loc[trading_dates, obi] = np.nan
    return obi_stats_df


def append_df(ori_df, increment_df):
    new_df = ori_df.append(increment_df)
    # 做排序的目的是新跑了原时间段中的中间一截后拼接到原数据末尾后打乱了时间顺序
    new_df = new_df.loc[~new_df.index.duplicated(keep='last')].sort_index(axis=0)
    return new_df


def append_df_in_dict(ori_dict, increment_dict):
    new_dict = dict()
    all_cols = list(ori_dict.keys) + list(increment_dict.keys)
    for col in all_cols:
        new_dict[col] = append_df(ori_dict[col], increment_dict[col])
    return new_dict


def get_suffix(obis_list):
    """
    1.用于为处理炜锋数据库取出的obi做的准备
    2.为没有交易所后缀的order_book_id的列表中的每个元素增加交易所后缀
    :return: 添加了交易所后缀的order_book_id的列表
    """
    with_to_without = {k: k[:-3] for k in obis_list}
    return with_to_without


def deal_weifeng_code_list(code_list_df):
    """
    1.处理炜锋发来的单个指标的数据
    :param code_list_df: 炜锋发来的wind_code和不带后缀的order_book_id的映射关系，dataframe格式
    :return: wind_code到不带后缀的order_book_id的字典
    """
    code_list_df.columns = ['wind_code', 'order_book_id']
    code_list_dict = code_list_df.set_index("wind_code")["order_book_id"].to_dict()
    return code_list_dict


def deal_weifeng_statistics(single_statistics_df, code_list_dict, obis_without_to_with, stat_name):
    """
    1.处理每一个指标dataframe
    :param single_statistics_df: 炜锋发来的单个指标的dataframe
    :param code_list_dict: 炜锋翻来的wind_code向股票代码映射的dataframe
    :param obis_without_to_with: 我制作的未带交易所后缀名的股票代码向带交易所后缀名的股票代码的映射的dict
    :param stat_name: single_statistics_df被改造后值的列名
    :return:处理后的单个指标的dataframe
    """
    # 用于处理的dataframe的列如下：wind代码、交易日日期、数值
    print(stat_name, datetime.now())
    if len(single_statistics_df.columns) == 3:
        single_statistics_df = single_statistics_df.iloc[:, [0, 1, 2]]
    # 用于处理的dataframe的列如下：wind代码、报告期、公布日期、数值
    elif len(single_statistics_df.columns) == 4:
        single_statistics_df = single_statistics_df.iloc[:, [0, 2, 3]]
    else:
        print('Unidentified Statistics!!')
        return None
    single_statistics_df.columns = ['wind_code', 'date', stat_name]
    # Acode_list.columns = ['wind_code', 'category']
    # single_statistics_df = pd.merge(Acode_list, single_statistics_df, left_on='wind_code', right_on='wind_code', how='inner')

    single_statistics_df['order_book_id'] = ''
    wind_code_unqualified = set()
    stc_unqualified = set()
    for ind, r in single_statistics_df.iterrows():
        if r['wind_code'] in code_list_dict.keys():
            stc_code = code_list_dict[r['wind_code']]
            if stc_code in obis_without_to_with.keys():
                single_statistics_df.at[ind, 'order_book_id'] = obis_without_to_with[stc_code]
            else:
                # 多数是B股和已退市的股票
                stc_unqualified = stc_unqualified | {stc_code}
        else:
            wind_code_unqualified = wind_code_unqualified | {r['wind_code']}
    print(stat_name, 'dealt')
    return single_statistics_df[['order_book_id', 'date', stat_name]], stc_unqualified, stc_unqualified


def get_wind_data_from_dealt_weifeng_statistics(file_names_list):
    obi_set = set()
    obi_list = list()
    for i in file_names_list:
        obi_set = obi_set | set(i['order_book_id'])
        obi_list = list(obi_set).sort(key=None, reverse=False)
    wind_data = dict()
    for i in obi_list:
        wind_data[i] = pd.DataFrame()
        for j in file_names_list:
            temp = eval(j).loc[eval(j)['order_book_id'] == i][['date', j]].set_index('date', inplace=True)
            wind_data[i] = pd.merge(wind_data[i], temp, left_index=True, right_index=True, how='outer')
    return wind_data


def get_rt_from_value(value_df, rt_periods_counts=4):
    """
    :param value_df: 用于计算增长率的数值dataframe,index为日期date，唯一列为指标的数值
    :param rt_periods_counts: 增长率所覆盖的value_df的期数。比如value_df是每3个月展现一个数值，而这里计算1年的增长率，则该参数值为4
    :return: 指定期数间的增长率
    """
    temp = value_df.shift(periods=rt_periods_counts)
    rt = value_df/temp
    rt.columns.name = ['rt' + value_df.columns[0]]
    return rt


def get_rt_from_value_in_wind_data(wind_data, stat_name):
    stc_list = list(wind_data.keys())
    for i in stc_list:
        temp = get_rt_from_value(wind_data[i][[stat_name]])
        wind_data[i] = pd.merge(wind_data[i], temp, left_index=True, right_index=True, how='outer')











def drop_dup_before_date_to_dict_for_quarter_day_value(origin_df, end_day):
    """
    :param origin_df: 它的index形如（obi，’2019q2‘）
    :param end_day: 观察日
    :return:
    """
    print('drop_dup_before_date_to_dict_for_quarter_day_value starts', datetime.now())
    # 该字典key为obi，值为dataframe
    drop_dup_dict = dict()
    stocks_list = [i[0] for i in origin_df.index]
    for s in stocks_list:
        quarter_day_s_ready_1 = origin_df.loc[s]
        quarter_day_s = quarter_day_s_ready_1.loc[quarter_day_s_ready_1['info_date'] < datetime.combine(end_day, time())]
        quarter_day_s_drop_dup_quarter = quarter_day_s.reset_index().drop_duplicates(subset='quarter', keep='last').set_index('quarter')
        quarter_day_s_drop_dup_quarter = quarter_day_s_drop_dup_quarter.drop(['info_date'], axis=1)
        drop_dup_dict[s] = quarter_day_s_drop_dup_quarter
    print('drop_dup_before_date_to_dict_for_quarter_day_value ends', datetime.now())
    return drop_dup_dict





"""
在build_factor中，相同意义的rq字段和wind字段对照，如更换数据源，则需更换代码中的字段，格式为“字段中文意思，wind字段，rq字段”
市值2，mkt_cap_ard，market_cap_2

"""


"""
备注一些常用语句
1. 读写文件
1.1 pickle文件
import pickle
写出
file = open('C:/Users/zhili/Desktop/raw_data/wind_data.pkl', 'wb')
pickle.dump(wind_data, file, 2)
file.close()
读入
file = open('C:/Users/zhili/Desktop/raw_data/wind_data.pkl', 'rb')
wind_data_ori = pickle.load(file)
file.close()

1.2 输出csv文件
g.to_csv('C:/Users/zhili/Desktop/stocks_change_rate_cumprod.csv', sep=',', index=True, header=True, encoding='utf_8')

1.3 读入xlsx文件
convertible_bonds = pd.read_excel(r'C:/Users/zhili/Desktop/convertible_bonds.xlsx') 


打印变量值
a = "hello"
b = a
print(f'{b}')


# 取数20221104各因子合并成一个双重索引df，制作成炜锋需要的格式
all_factors = pd.DataFrame()
for data_name in data_targets_20221104:
    print(data_name, 'to append to all_factors')
    eval(data_name).loc[:, 'factor'] = data_name
    temp = eval(data_name).set_index('factor', append=True)
    exec(data_name + ' = temp')
    all_factors = all_factors.append(eval(data_name))
all_factors = all_factors.swaplevel(axis=0)
all_factors = all_factors.stack()
all_factors = all_factors.unstack('factor')
all_factors.to_csv('C:/Users/zhili/Desktop/all_factors_weifeng_format.csv', sep=',', index=True, header=True, encoding='utf_8')
"""