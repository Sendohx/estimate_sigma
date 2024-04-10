# -*- coding:UTF-8 -*-

import itertools
import pandas as pd

from config import config
import fetch_data
from estimate_sigma import EstimateSigma


# 标的参数
cf = config()
cf.run()

root = cf.root # 原始数据存放路径
start_date = cf.start_date # 回测起始日期
end_date = cf.end_date # 回测结束日期

# 模型超参数
hyperparam_grid={
            'test_window': cf.test_window,
            'lambda': cf.lamb,
            'arch_p': cf.p,
            'garch_q': cf.q,
            'return_type': cf.return_type
            }

if cf.object_type == 'index':
    result_df = pd.DataFrame(columns=['index', 'window_size', 'lambda', 'arch_p', 'garch_q', 'return_type',
                                      'history_estimate_mse', 'ewma_mse', 'arch_mse', 'garch_mse'])
    for index in cf.index_list:
        index_data = fetch_data.index_data(index, start_date, end_date)

        for window, lamb, p, q, return_type in itertools.product(*hyperparam_grid.values()):
            try:
                print('开始运行\n')
                es = EstimateSigma(index, index_data, window, lamb, p, q, return_type)
                es.run()
                # print(es.df)
                es.df.to_parquet(root + f'/idx_{index}_window{window}_lambda{lamb}_p{p}_q{q}_{return_type}_df.parquet')
                result_df = result_df.append(
                    pd.DataFrame(data=[[index, window, lamb, p, q, return_type] + list(es.result_dict.values())],
                                 columns=['obj', 'sigma_window', 'lambda', 'arch_p', 'garch_q', 'return_type',
                                          'history_estimate_mse', 'ewma_mse', 'arch_mse', 'garch_mse']), ignore_index=True)
                print(f'参数组合: {index}_window={window}_lambda={lamb}_p={p}_q={q}_return={return_type} 完成\n')
                result_df.to_parquet(root + f'/sigma_estimate_summary_{start_date}_{end_date}.parquet')
            except Exception as e:
                print(f'Error:{e}')

elif cf.object_type == 'stock':
    result_df = pd.DataFrame(columns=['stock', 'window_size', 'lambda', 'arch_p', 'garch_q', 'return_type',
                                      'history_estimate_mse', 'ewma_mse', 'arch_mse', 'garch_mse'])
    for stock in cf.stock_list:
        index_data = fetch_data.index_data(stock, start_date, end_date)

        for window, lamb, p, q, return_type in itertools.product(*hyperparam_grid.values()):
            try:
                print('开始运行\n')
                es = EstimateSigma(stock, index_data, window, lamb, p, q, return_type)
                es.run()
                # print(es.df)
                es.df.to_parquet(root + f'/stk_{stock}_window{window}_lambda{lamb}_p{p}_q{q}_{return_type}_df.parquet')
                result_df = result_df.append(
                    pd.DataFrame(data=[[stock, window, lamb, p, q, return_type] + list(es.result_dict.values())],
                                 columns=['obj', 'sigma_window', 'lambda', 'arch_p', 'garch_q', 'return_type',
                                          'history_estimate_mse', 'ewma_mse', 'arch_mse', 'garch_mse']), ignore_index=True)
                print(f'参数组合: {stock}_window={window}_lambda={lamb}_p={p}_q={q}_return={return_type} 完成\n')
                result_df.to_parquet(root + f'/sigma_estimate_summary_{start_date}_{end_date}.parquet')
            except Exception as e:
                print(f'Error:{e}')

print('代码运行结束\n')
