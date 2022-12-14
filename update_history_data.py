import pickle
import os
from constant import data_targets_20221104, path, time_to_date_targets3
from rq_get_data import *


print('to update', datetime.now())
for data_name in data_targets_20221104:
    file_name = data_name + '.pkl'
    # 如果存在历史数据则读取历史数据，再把新数据连在历史数据尾部，再输出包含历史数据在内的完整新数据
    input_path_name = output_path_name = path + data_name + '.pkl'
    if os.path.isfile(path + file_name):
        print(data_name + ': exists')
        # 读入
        file = open(input_path_name, 'rb')
        exec(data_name + '_old = pickle.load(file)')
        file.close()

        # 更新基础数据
        exec(data_name + ' = ' + data_name + '_old.append(' + data_name + ')')
        temp = eval(data_name)
        temp = temp.reset_index()
        temp.drop_duplicates(subset=['index'], keep='last', inplace=True)
        temp.set_index('index', drop=True, inplace=True)
        temp = temp.sort_index(ascending=True)
        exec(data_name + ' = temp')

        # 输出
        file = open(output_path_name, 'wb')
        exec('pickle.dump(' + data_name + ', file, 2)')
        file.close()


    # 如果不存在历史数据则输出新造的数据
    else:
        print(data_name + '_old: NOT exists')
        # 输出
        file = open(output_path_name, 'wb')
        exec('pickle.dump(' + data_name + ', file, 2)')
        file.close()
        print(data_name + ' already output')
print('update ends', datetime.now())