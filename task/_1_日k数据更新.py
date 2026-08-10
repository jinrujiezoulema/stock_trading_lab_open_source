import time

import pandas as pd
from loguru import logger
from tqdm import tqdm

from utils import db, common


def fetch_stock_daily(stock_basic, start_date, end_date):
    """
    获取股票池的日线数据并保存到 CSV
    参数:
        stock_basic: 过滤后的股票池 DataFrame
        start_date: 开始日期（格式：YYYYMMDD）
        end_date: 结束日期（格式：YYYYMMDD）
        output_csv: 输出 CSV 文件路径
    返回:
        DataFrame: 合并后的日线数据
    """
    stock_data = []
    for_index = 0
    pro_index = 0
    stock_basic_len = len(stock_basic)
    # 将 stock_basic 转换为字典以提高查询效率
    stock_dict = stock_basic.set_index('ts_code')[['market', 'name']].to_dict('index')
    for stock in tqdm(stock_basic['ts_code'], desc="获取股票日线数据"):
        for_index += 1
        market = stock_dict[stock]['market']
        name = stock_dict[stock]['name']
        logger.info(f"获取中 {for_index}/{stock_basic_len} {market} {stock} {name}")
        while True:
            try:
                pro_index += 1
                pro = common.pro_list[pro_index % len(common.pro_list)]
                symbol = int(stock.split(".")[0])
                # df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date,
                #                                         end_date=end_date, adjust="hfq")
                df = pro.daily(ts_code=stock, start_date=start_date, end_date=end_date)
                if not df.empty:
                    df['stock_name'] = name
                    df['ts_code'] = symbol
                    df['data_id'] = df['ts_code'].astype(str) + '_' + df['trade_date']
                    stock_data.append(df)
                time.sleep(0.2)
                break
            except Exception as e:
                logger.error(f"获取 {stock} 数据失败: {e}")
                # time.sleep(0.5)
    stock_data = pd.concat(stock_data, ignore_index=True) if stock_data else pd.DataFrame()
    logger.info(f"日线数据获取完成，行数：{len(stock_data)}")

    # if not stock_data.empty:
    #     csv_path = '../data/stock_daily.csv'
    #     stock_data.to_csv(csv_path, index=False, encoding='utf-8-sig')
    #     logger.info(f"日线数据已保存到 {csv_path}")
    return stock_data


def save_to_mysql(stock_basic, stock_daily, basic_table='stock_basic', daily_table='stock_daily'):
    """
    将数据写入 MySQL 数据库
    参数:
        stock_basic: A 股基本信息 DataFrame
        stock_daily: 日线数据 DataFrame
        stock_basic: 过滤后的股票池 DataFrame
        basic_table: A 股基本信息表名
        daily_table: 日线数据表名
        pool_table: 股票池表名
    """
    if not stock_basic.empty:
        inserted_count = db.smart_insert_to_mysql(stock_basic, basic_table, db.engine, ['ts_code'])
        logger.info(f"数据已写入 MySQL 表 {basic_table} 更新条数:{inserted_count}")
    if not stock_daily.empty:
        inserted_count = db.smart_insert_to_mysql(stock_daily, daily_table, db.engine, ['data_id'])
        logger.info(f"数据已写入 MySQL 表 {daily_table} 更新条数:{inserted_count}")


def main(start_date, end_date):
    """
    主函数，依次执行加载、获取和存储
    """
    # 获取股票池
    stock_basic = common.filter_stock_basic()
    if stock_basic.empty:
        logger.error("过滤后的股票池为空，程序退出")
        return

    # 3. 获取日线数据并保存 CSV
    stock_daily = fetch_stock_daily(stock_basic, start_date, end_date)

    # 4. 写入 MySQL
    save_to_mysql(stock_basic, stock_daily)
    pass


if __name__ == "__main__":
    # 设置日期
    # start_date = '20100101'
    start_date = '20251009'
    main(start_date)
    # from strategy import 玉柱
    # from 游资溢价分析 import 溢价分析
    #
    # 玉柱.main()
    # 溢价分析.main()
