import sys
import os

sys.path.append(os.pardir)
curPath = os.path.abspath(os.path.dirname(__file__))

import time
from datetime import datetime, timedelta

from loguru import logger

from task import _1_日k数据更新
from task import _3_kdj
from task import _4_上证指数日k
from task import _5_韭研公社异动
from task import _6_同花顺行业和概念
from task import _7_市值信息每日更新
from task import _8_指数情绪周期每日更新
from task import _9_热门板块情绪每日更新
from task import _10_开盘啦dde读取
from utils import common, db
from 游资溢价分析.采集 import 龙虎榜数据采集
from 游资溢价分析 import 溢价分析
from strategy import 龙虎榜_动态资金分仓_可重复买入_最大5个仓位_每个仓位总资产五分之一_winner


@common.timer_statistics
def tasks(start_date=None):
    if db.redis_con_localhost.exists(f"每日更新.py:{start_date}"):
        logger.info(f"今日更新已执行完成，请勿重复执行。")
        return
    if db.redis_con_localhost.exists(f"run_check:每日更新.py"):
        logger.info(f"当前存在 每日更新.py 正在执行，请勿重复执行。")
        return
    db.redis_con_localhost.set(f"run_check:每日更新.py", datetime.now().strftime('%Y%m%d %H:%M:%S'))
    end_date = datetime.now().strftime('%Y%m%d')

    # logger.info(f"同花顺板块/概念成分股采集入库 开始")
    # _6_同花顺行业和概念.每日更新同花顺板块成分股()
    # logger.info(f"同花顺板块/概念成分股采集入库 完成")

    # _5_韭研公社异动.韭研公社异动采集(start_date)

    _4_上证指数日k.update(start_date=start_date, end_date=end_date)

    _1_日k数据更新.main(start_date, end_date)

    _9_热门板块情绪每日更新.更新(start_date=start_date, end_date=end_date)

    _8_指数情绪周期每日更新.更新(start_date=start_date, end_date=end_date)

    _7_市值信息每日更新.更新(start_date=start_date, end_date=end_date)

    logger.info(f"龙虎榜数据采集 开始")
    龙虎榜数据采集.main(date=start_date)
    logger.info(f"龙虎榜数据采集 完成")

    # 放在所有业务更新的最后，按 ts_code + trade_date 补充 stock_daily.dde。
    _10_开盘啦dde读取.更新(start_date=start_date, end_date=end_date)


    # _3_kdj.save_daily_kdj()
    # time.sleep(60)
    # 溢价分析.main(start_date=int((datetime.strptime(str(start_date), "%Y%m%d") - timedelta(days=90)).strftime('%Y%m%d')),
    #               latest_date=int(start_date))

    db.redis_con_localhost.set(f"每日更新.py:{start_date}", datetime.now().strftime('%Y%m%d %H:%M:%S'))
    db.redis_con_localhost.delete(f"run_check:每日更新.py")


if __name__ == '__main__':
    # 当前交易日17:30，完成日K线数据入库；
    # start_date = '20100101'
    start_date = datetime.now().strftime('%Y%m%d')
    # start_date = '20260427'
    tasks(start_date)
    # _5_韭研公社异动.日内前排()
    # 龙虎榜_动态资金分仓_可重复买入_最大5个仓位_每个仓位总资产五分之一_winner.main(start_date=int(start_date),
    #                                                                               end_date=int(start_date))

    # range_date = (datetime.strptime(str(start_date), "%Y%m%d") - timedelta(days=30)).strftime('%Y%m%d')  # 缓冲 30 天

    # 溢价分析.main(
    #     start_date=int((datetime.strptime(str(start_date), "%Y%m%d") - timedelta(days=90)).strftime('%Y%m%d')),
    #     latest_date=int(start_date))

