import argparse
import os
import sys
import time
from datetime import datetime

import pandas as pd
from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils import common, db

每日指标字段 = "ts_code,trade_date,total_mv,circ_mv,free_share"
市值更新完成标记前缀 = "股票日线:市值信息:已更新"

市值字段定义 = [
    (
        "total_mv",
        "ALTER TABLE `stock_daily` "
        "ADD COLUMN `total_mv` double NULL DEFAULT NULL COMMENT '总市值，单位万元，Tushare daily_basic.total_mv' "
        "AFTER `amount`",
    ),
    (
        "circ_mv",
        "ALTER TABLE `stock_daily` "
        "ADD COLUMN `circ_mv` double NULL DEFAULT NULL COMMENT '流通市值，单位万元，Tushare daily_basic.circ_mv' "
        "AFTER `total_mv`",
    ),
    (
        "free_share",
        "ALTER TABLE `stock_daily` "
        "ADD COLUMN `free_share` double NULL DEFAULT NULL COMMENT '自由流通股本，单位万股，Tushare daily_basic.free_share' "
        "AFTER `circ_mv`",
    ),
    (
        "free_mv",
        "ALTER TABLE `stock_daily` "
        "ADD COLUMN `free_mv` double NULL DEFAULT NULL COMMENT '自由流通市值，单位万元，stock_daily.close * free_share' "
        "AFTER `free_share`",
    ),
]


def 规范日期(value):
    if value is None:
        return None
    return datetime.strptime(str(value), "%Y%m%d").strftime("%Y%m%d")


def 转浮点或空(value):
    if pd.isna(value):
        return None
    return float(value)


def 确保市值字段存在():
    with db.engine.begin() as conn:
        result = conn.execute(text("SHOW COLUMNS FROM `stock_daily`"))
        existing_columns = {row[0] for row in result}
        for column_name, alter_sql in 市值字段定义:
            if column_name in existing_columns:
                continue
            logger.info(f"stock_daily 缺少字段 {column_name}，开始自动补列")
            conn.execute(text(alter_sql))
            logger.info(f"stock_daily 字段 {column_name} 已补充")


def 市值更新完成标记key(trade_date):
    return f"{市值更新完成标记前缀}:{int(trade_date)}"


def 是否已更新市值(trade_date):
    return db.redis_con_localhost.exists(市值更新完成标记key(trade_date)) > 0


def 标记市值已更新(trade_date, updated_count):
    db.redis_con_localhost.set(
        市值更新完成标记key(trade_date),
        f"更新时间={datetime.now().strftime('%Y%m%d %H:%M:%S')}|更新条数={updated_count}",
    )


def 查询待更新交易日(start_date=None, end_date=None, only_missing=True):
    where = ["日期 IS NOT NULL"]
    if start_date:
        where.append(f"日期 >= {int(start_date)}")
    if end_date:
        where.append(f"日期 <= {int(end_date)}")

    query = f"""
        SELECT DISTINCT 日期 as trade_date
        FROM akshare_sh000001
        WHERE {' AND '.join(where)}
        ORDER BY 日期 DESC
    """
    result = pd.read_sql(query, db.engine)
    if result.empty:
        return []

    trade_dates = [str(int(item)) for item in result["trade_date"].tolist()]
    if not only_missing:
        return trade_dates

    return [trade_date for trade_date in trade_dates if not 是否已更新市值(trade_date)]


def 查询日线股票代码集合(trade_date):
    query = f"""
        SELECT DISTINCT ts_code
        FROM stock_daily
        WHERE trade_date = {int(trade_date)}
    """
    result = pd.read_sql(query, db.engine)
    if result.empty:
        return set()
    return {int(item) for item in result["ts_code"].tolist()}


def 获取每日指标(trade_date):
    trade_date = 规范日期(trade_date)
    if not common.pro_list:
        logger.error("common.pro_list 为空，无法获取 Tushare daily_basic")
        return pd.DataFrame()

    pro_index = 0
    while True:
        try:
            pro_index += 1
            pro = common.pro_list[pro_index % len(common.pro_list)]
            logger.info(f"获取 daily_basic {trade_date}，token序号 {pro_index % len(common.pro_list)}")
            df = pro.daily_basic(trade_date=trade_date, fields=每日指标字段)
            if df is None or df.empty:
                logger.warning(f"{trade_date} daily_basic 返回为空")
                return pd.DataFrame()

            df = df.copy()
            df["ts_code"] = df["ts_code"].astype(str).str.split(".", n=1).str[0].astype(int)
            df["trade_date"] = df["trade_date"].astype(int)
            for column in ["total_mv", "circ_mv", "free_share"]:
                df[column] = pd.to_numeric(df[column], errors="coerce")
            return df[["ts_code", "trade_date", "total_mv", "circ_mv", "free_share"]]
        except Exception as e:
            logger.error(f"{trade_date} daily_basic 获取失败: {e}")
            time.sleep(10)


def 更新日线市值字段(df):
    if df.empty:
        return 0

    update_sql = """
        UPDATE stock_daily
        SET total_mv = %s,
            circ_mv = %s,
            free_share = %s,
            free_mv = CASE
                WHEN close IS NULL OR %s IS NULL THEN NULL
                ELSE close * %s
            END
        WHERE ts_code = %s
          AND trade_date = %s
    """

    params = []
    for row in df.itertuples(index=False):
        free_share = 转浮点或空(row.free_share)
        params.append(
            (
                转浮点或空(row.total_mv),
                转浮点或空(row.circ_mv),
                free_share,
                free_share,
                free_share,
                int(row.ts_code),
                int(row.trade_date),
            )
        )

    cnx = db.mysql_localhost_pool.get_connection()
    cursor = cnx.cursor()
    try:
        cursor.executemany(update_sql, params)
        cnx.commit()
        return cursor.rowcount
    finally:
        cursor.close()
        cnx.close()


def 更新单个交易日市值(trade_date):
    stock_daily_codes = 查询日线股票代码集合(trade_date)
    if not stock_daily_codes:
        logger.warning(f"{trade_date} stock_daily 中没有日线记录，跳过")
        return False, 0

    market_cap = 获取每日指标(trade_date)
    if market_cap.empty:
        return False, 0

    market_cap = market_cap[market_cap["ts_code"].isin(stock_daily_codes)]
    if market_cap.empty:
        logger.warning(f"{trade_date} daily_basic 与 stock_daily 没有匹配股票，跳过")
        return False, 0

    updated_count = 更新日线市值字段(market_cap)
    logger.info(f"{trade_date} 市值字段更新完成，匹配 {len(market_cap)} 条，影响 {updated_count} 条")
    return True, updated_count


@common.timer_statistics
def 更新(start_date=None, end_date=None, only_missing=True):
    start_date = 规范日期(start_date or datetime.now().strftime("%Y%m%d"))
    end_date = 规范日期(end_date or start_date)

    # 确保市值字段存在()

    trade_dates = 查询待更新交易日(start_date=start_date, end_date=end_date, only_missing=only_missing)
    if not trade_dates:
        logger.info(f"{start_date} - {end_date} 没有需要更新市值字段的交易日")
        return 0

    logger.info(
        f"开始更新 stock_daily 市值字段，交易日数量 {len(trade_dates)}，按日期倒序执行，范围 {trade_dates[0]} - {trade_dates[-1]}")
    total_updated = 0
    for trade_date in trade_dates:
        is_done, updated_count = 更新单个交易日市值(trade_date)
        if is_done:
            标记市值已更新(trade_date, updated_count)
            total_updated += updated_count
        else:
            logger.warning(f"{trade_date} 市值字段未完成，不写 Redis 标记")
        time.sleep(0.2)

    logger.info(f"stock_daily 市值字段更新完成，总影响 {total_updated} 条")
    return total_updated


def 主函数(start_date=None, end_date=None, force=False):
    return 更新(start_date=start_date, end_date=end_date, only_missing=not force)


def 解析命令行参数():
    parser = argparse.ArgumentParser(description="每日更新 stock_daily 市值字段")
    parser.add_argument("--start-date", dest="start_date", help="开始日期，格式 YYYYMMDD，默认今天")
    parser.add_argument("--end-date", dest="end_date", help="结束日期，格式 YYYYMMDD，默认等于 start-date")
    parser.add_argument("--force", action="store_true", help="强制覆盖已存在的市值字段")
    return parser.parse_args()


normalize_date = 规范日期
to_float_or_none = 转浮点或空
ensure_market_cap_columns = 确保市值字段存在
market_cap_done_key = 市值更新完成标记key
is_market_cap_done = 是否已更新市值
mark_market_cap_done = 标记市值已更新
query_trade_dates = 查询待更新交易日
query_stock_daily_codes = 查询日线股票代码集合
fetch_daily_basic = 获取每日指标
update_stock_daily_market_cap = 更新日线市值字段
update_one_trade_date = 更新单个交易日市值
update = 更新
main = 主函数
parse_args = 解析命令行参数

if __name__ == "__main__":
    # args = 解析命令行参数()
    # 主函数(start_date=args.start_date, end_date=args.end_date, force=args.force)
    主函数(start_date=20260101, end_date=20260609, force=False)
