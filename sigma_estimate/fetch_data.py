# -*- coding:UTF-8 -*-

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from connect_database import ConnectDatabase


def index_data(idx_code, start_date, end_date):
    data_start_date = datetime.strptime(start_date, '%Y%m%d').date() - timedelta(days=300)
    data_start_date = data_start_date.strftime('%Y%m%d')  # 回测起始日期向前推300天，作为数据起始日期，避免回测期内出现空值
    index_data_dict = {}
    # 指数日行情
    table = 'AINDEXEODPRICES'
    columns = 'S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_AMOUNT'
    condition1 = f"S_INFO_WINDCODE = '{idx_code}'"
    condition2 = f"TRADE_DT BETWEEN '{data_start_date}'AND '{end_date}'"
    sql1 = f''' SELECT %s FROM %s WHERE %s AND %s ''' % (columns, table, condition1, condition2)

    cd1 = ConnectDatabase(sql1)
    data = cd1.get_data()
    data = data.rename(columns={'S_INFO_WINDCODE': 'symbol',
                                'TRADE_DT': 'date',
                                'S_DQ_PRECLOSE': 'pre_close',
                                'S_DQ_OPEN': 'open',
                                'S_DQ_HIGH': 'high',
                                'S_DQ_LOW': 'low',
                                'S_DQ_CLOSE': 'close',
                                'S_DQ_AMOUNT': 'amount'})
    data[data.columns[2:]] = (data[data.columns[2:]].apply(pd.to_numeric))
    data = data.sort_values(['symbol', 'date']).copy()
    data['return'] = data['close'] / data['pre_close'] - 1
    data['log_return'] = np.log(data['close']/data['pre_close'])
    data['abs_return'] = abs(data['return'])
    # data.to_parquet(root + f'/ind_{index}_{start_date}_{end_date}.parquet')
    return data


def index_stock_data(idx_code, start_date, end_date):
    data_start_date = datetime.strptime(start_date, '%Y%m%d').date() - timedelta(days=300)
    data_start_date = data_start_date.strftime('%Y%m%d')  # 回测起始日期向前推300天，作为数据起始日期，避免回测期内出现空值
    index_stock_data_dict = dict()
    ## 成分股日行情
    sql2 = f'''
                    SELECT A.S_INFO_WINDCODE, A.TRADE_DT, A.S_DQ_ADJPRECLOSE, A.S_DQ_ADJOPEN, A.S_DQ_ADJHIGH, 
                           A.S_DQ_ADJLOW, A.S_DQ_ADJCLOSE, A.S_DQ_VOLUME
                    FROM ASHAREEODPRICES A
                    WHERE (A.TRADE_DT BETWEEN '{data_start_date}'AND '{end_date}') AND A.S_INFO_WINDCODE IN (
                        SELECT B.S_CON_WINDCODE
                        FROM AINDEXMEMBERS B
                        WHERE B.S_INFO_WINDCODE = '{idx_code}'
                            AND (
                                (B.S_CON_INDATE <= A.TRADE_DT AND B.S_CON_OUTDATE > A.TRADE_DT)
                            OR (B.S_CON_INDATE <= A.TRADE_DT AND B.S_CON_OUTDATE IS NULL)
                            )
                        )
                    '''
    cd2 = ConnectDatabase(sql2)
    ind_stk_data = cd2.get_data()
    ind_stk_data = ind_stk_data.rename(columns={'S_INFO_WINDCODE': 'symbol',
                                                'TRADE_DT': 'date',
                                                'S_DQ_ADJPRECLOSE': 'pre_close',
                                                'S_DQ_ADJOPEN': 'open',
                                                'S_DQ_ADJHIGH': 'high',
                                                'S_DQ_ADJLOW': 'low',
                                                'S_DQ_ADJCLOSE': 'close',
                                                'S_DQ_VOLUME': 'volume'})
    ind_stk_data[ind_stk_data.columns[2:]] = ind_stk_data[ind_stk_data.columns[2:]].apply(pd.to_numeric,
                                                                                          errors='coerce')
    ind_stk_data.sort_values(['symbol', 'date'], inplace=True)
    ind_stk_data['return'] = ind_stk_data['close'] / ind_stk_data['pre_close'] - 1
    ind_stk_data['log_return'] = np.log(ind_stk_data['close'] / ind_stk_data['pre_close'])
    ind_stk_data['abs_return'] = abs(ind_stk_data['return'])
    # ind_stk_data.to_parquet(root + f'/ind_stk_{index}_{start_date}_{end_date}.parquet')
    return ind_stk_data


def ind_stk_drv_data(idx_code, start_date, end_date):
    data_start_date = datetime.strptime(start_date, '%Y%m%d').date() - timedelta(days=300)
    data_start_date = data_start_date.strftime('%Y%m%d')  # 回测起始日期向前推300天，作为数据起始日期，避免回测期内出现空值
    ind_stk_drv_data_dict = {}
    ## 成分股日衍生数据
    sql3 = f'''
                SELECT A.S_INFO_WINDCODE, A.TRADE_DT, A.S_DQ_MV, A.FREE_SHARES_TODAY
                FROM ASHAREEODDERIVATIVEINDICATOR A
                WHERE (A.TRADE_DT BETWEEN '{data_start_date}'AND '{end_date}')AND A.S_INFO_WINDCODE IN (
                    SELECT B.S_CON_WINDCODE
                    FROM AINDEXMEMBERS B
                    WHERE B.S_INFO_WINDCODE = '{idx_code}'
                        AND (
                            (B.S_CON_INDATE <= A.TRADE_DT AND B.S_CON_OUTDATE > A.TRADE_DT)
                        OR (B.S_CON_INDATE <= A.TRADE_DT AND B.S_CON_OUTDATE IS NULL)
                        )
                    )
                '''
    cd3 = ConnectDatabase(sql3)
    ind_stk_drv_data = cd3.get_data()
    ind_stk_drv_data = ind_stk_drv_data.rename(columns={'S_INFO_WINDCODE': 'symbol',
                                                        'TRADE_DT': 'date',
                                                        'S_DQ_MV': 'capital',
                                                        'FREE_SHARES_TODAY': 'free_share'})
    ind_stk_drv_data[ind_stk_drv_data.columns[2:]] = ind_stk_drv_data[ind_stk_drv_data.columns[2:]].apply(pd.to_numeric, errors='coerce')
    ind_stk_drv_data.sort_values(['symbol', 'date'], inplace=True)
    # ind_stk_drv_data.to_parquet(root + f'/stk_drv_{index}_{start_date}_{end_date}.parquet')
    return ind_stk_drv_data


def stock_data(stk_code, start_date, end_date):
    data_start_date = datetime.strptime(start_date, '%Y%m%d').date() - timedelta(days=300)
    data_start_date = data_start_date.strftime('%Y%m%d')  # 回测起始日期向前推300天，作为数据起始日期，避免回测期内出现空值
    stock_data_dict = {}
    ## 筛选个股
    sql4 = f'''
            SELECT A.S_INFO_WINDCODE, A.TRADE_DT, A.S_DQ_ADJPRECLOSE, A.S_DQ_ADJOPEN, A.S_DQ_ADJHIGH, 
                   A.S_DQ_ADJLOW, A.S_DQ_ADJCLOSE, A.S_DQ_VOLUME
            FROM ASHAREEODPRICES A
            WHERE (A.TRADE_DT BETWEEN '{data_start_date}'AND '{end_date}') AND A.S_INFO_WINDCODE = '{stk_code}'
            '''
    cd4 = ConnectDatabase(sql4)
    stk_data = cd4.get_data()
    stk_data = stk_data.rename(columns={'S_INFO_WINDCODE': 'symbol',
                                        'TRADE_DT': 'date',
                                        'S_DQ_ADJPRECLOSE': 'pre_close',
                                        'S_DQ_ADJOPEN': 'open',
                                        'S_DQ_ADJHIGH': 'high',
                                        'S_DQ_ADJLOW': 'low',
                                        'S_DQ_ADJCLOSE': 'close',
                                        'S_DQ_VOLUME': 'volume'})
    stk_data[stk_data.columns[2:]] = stk_data[stk_data.columns[2:]].apply(pd.to_numeric,errors='coerce')
    stk_data.sort_values(['symbol', 'date'], inplace=True)
    stk_data['return'] = stk_data['close'] / stk_data['pre_close'] - 1
    stk_data['log_return'] = np.log(stk_data['close'] / stk_data['pre_close'])
    stk_data['abs_return'] = abs(stk_data['return'])
    # stk_data.to_parquet(root + f'/stk_{stock}_{start_date}_{end_date}.parquet')
    return stock_data_dict
