import os
import sys

from loguru import logger

sys.path.append(os.pardir)
from datetime import datetime
from multiprocessing import Pool

import pandas as pd

from utils import common, db, api


def process_stock_batch(args):
    batch, stock_dict, start_date, end_date = args
    stock_basic_len = len(batch)
    for_index = 0
    data_list = []
    for stock in batch:
        for_index += 1
        market = stock_dict[stock]['market']
        name = stock_dict[stock]['name']
        logger.info(f"{for_index}/{stock_basic_len} {market} {name} {start_date} {end_date} 读取5分k 开始")
        code = f"{stock.split('.')[1]}.{stock.split('.')[0]}"
        rs = api.bs.query_history_k_data_plus(code,
                                          'date,time,code,open,high,low,close,volume,amount,adjustflag',
                                          start_date=start_date, end_date=end_date,
                                          frequency="5", adjustflag="3")
        if rs.error_code != '0':
            logger.error('读取异常' + rs.error_msg)
            exit()
        logger.info(f"{for_index}/{stock_basic_len} {market} {name} 读取5分k 成功")
        data = rs.data
        code = int(stock.split('.')[0])
        for rs_row in data:
            # while rs.next():
            #     # 获取一条记录，将记录合并在一起
            #     rs_row = rs.get_row_data()
            date = int(str(rs_row[0]).replace("-", ""))
            time = int(rs_row[1][0:12])
            data_id = f"{code}_{date}_{time}"
            rs_row[0] = date
            rs_row[1] = time
            rs_row[2] = code
            rs_row.append(data_id)
            data_list.append(rs_row)
        logger.info(f"{for_index}/{stock_basic_len} {market} {name} 解析数据 成功")
    return data_list


def get_data(start_date, end_date, stock):
    start_date = datetime.strptime(str(start_date), '%Y%m%d').strftime("%Y-%m-%d")
    end_date = datetime.strptime(str(end_date), '%Y%m%d').strftime("%Y-%m-%d")
    code = f"{stock.split('.')[1]}.{stock.split('.')[0]}".lower()
    rs = api.bs.query_history_k_data_plus(code,
                                      'open,close,date,time,code,high,low,volume,amount,adjustflag',
                                      start_date=start_date, end_date=end_date,
                                      frequency="5", adjustflag="3")
    if rs.error_code != '0':
        logger.error('读取异常' + rs.error_msg)
        exit()
    data = rs.data
    return data


def main(start_date, end_date=None):
    columns = ['date', 'time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjustflag', 'data_id']
    start_date = datetime.strptime(str(start_date), '%Y-%m-%d').strftime('%Y-%m-%d')
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    else:
        end_date = datetime.strptime(str(end_date), '%Y-%m-%d')
    # 获取股票池
    stock_basic = common.filter_stock_basic()
    if stock_basic.empty:
        logger.error("过滤后的股票池为空，程序退出")
        exit()

    data_list = []
    # 将 stock_basic 转换为字典以提高查询效率
    stock_dict = stock_basic.set_index('ts_code')[['market', 'name']].to_dict('index')
    # 分批，每批 200 个代码（3000 ÷ 200 = 15 批）
    batch_size = 300
    batches = [stock_basic['ts_code'].to_list()[i:i + batch_size] for i in
               range(0, len(stock_basic['ts_code'].to_list()), batch_size)]
    with Pool(processes=6) as pool:
        tasks = [(batch, stock_dict, start_date, end_date) for batch in batches]
        results = pool.imap_unordered(process_stock_batch, tasks)

        # 最小化 tqdm 开销
        for batch_results in results:
            data_list.extend(batch_results)
    # pass
    # for_index = 0
    # stock_basic_len = len(stock_basic)
    # for stock in tqdm(stock_basic['ts_code'], desc="获取股票日线数据"):
    #     for_index += 1
    #     market = stock_dict[stock]['market']
    #     name = stock_dict[stock]['name']
    #     logger.info(f"{for_index}/{stock_basic_len} {market} {name} 读取5分k 开始")
    #     code = f"{stock.split('.')[1]}.{stock.split('.')[0]}"
    #     rs = api.bs.query_history_k_data_plus(code,
    #                                       'date,time,code,open,high,low,close,volume,amount,adjustflag',
    #                                       start_date=start_date, end_date=end_date,
    #                                       frequency="5", adjustflag="3")
    #     if rs.error_code != '0':
    #         logger.error('读取异常' + rs.error_msg)
    #         exit()
    #     logger.info(f"{for_index}/{stock_basic_len} {market} {name} 读取5分k 成功")
    #     data = rs.data
    #     code = int(stock.split('.')[0])
    #     for rs_row in data:
    #     # while rs.next():
    #     #     # 获取一条记录，将记录合并在一起
    #     #     rs_row = rs.get_row_data()
    #         date = int(str(rs_row[0]).replace("-",""))
    #         time = int(rs_row[1][0:12])
    #         data_id = f"{code}_{date}_{time}"
    #         rs_row[0] = date
    #         rs_row[1] = time
    #         rs_row[2] = code
    #         rs_row.append(data_id)
    #         data_list.append(rs_row)
    #     logger.info(f"{for_index}/{stock_basic_len} {market} {name} 解析数据 成功")
    #     # break
    stock_5_min_k = pd.DataFrame(data_list, columns=columns)
    inserted_count = db.smart_insert_to_mysql(stock_5_min_k, 't_stock_5_min_k', db.engine, ['data_id'])
    logger.info(f"数据已写入 MySQL 表 {'t_stock_5_min_k'} 更新条数:{inserted_count}")
    # result.to_sql('t_stock_5_min_k', con=db.engine, if_exists='replace', index=False)
    pass


if __name__ == "__main__":
    # 设置日期
    # start_date = '2019-01-01' # api的数据起始时间
    start_date = '2025-01-01'
    main(start_date)
    # main(start_date)
