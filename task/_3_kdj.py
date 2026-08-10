from multiprocessing import Pool

import pandas as pd
from loguru import logger
from tqdm import tqdm

from utils import db, common


# KDJ计算函数
def calculate_ths_kdj(df, n=9, m1=3, m2=3):
    """
    计算同花顺KDJ指标（N=9, M1=3, M2=3）
    :param df: 包含high, low, close的DataFrame
    :param n: RSV周期，默认9
    :param m1: K值平滑周期，默认3
    :param m2: D值平滑周期，默认3
    :return: 包含trade_date, k, d, j的DataFrame
    """
    # 计算RSV
    df['low_n'] = df['low'].rolling(window=n).min()
    df['high_n'] = df['high'].rolling(window=n).max()
    df['rsv'] = ((df['close'] - df['low_n']) / (df['high_n'] - df['low_n']) * 100).fillna(0)

    # 初始化K和D
    df['k'] = 50.0  # 同花顺初始值通常为50
    df['d'] = 50.0

    # 计算平滑权重
    k_weight = 1 / m1  # K 值的 RSV 权重
    k_prev_weight = 1 - k_weight  # K 值的上一周期权重
    d_weight = 1 / m2  # D 值的 K 权重
    d_prev_weight = 1 - d_weight  # D 值的上一周期权重

    # 计算 K 和 D
    for i in range(1, len(df)):
        df.iloc[i, df.columns.get_loc('k')] = k_prev_weight * df.iloc[i - 1]['k'] + k_weight * df.iloc[i]['rsv']
        df.iloc[i, df.columns.get_loc('d')] = d_prev_weight * df.iloc[i - 1]['d'] + d_weight * df.iloc[i]['k']

    # 计算 J 值
    df['j'] = 3 * df['k'] - 2 * df['d']

    # 删除中间变量，保留所需列
    return df[['trade_date', 'k', 'd', 'j']].copy()


@common.timer_statistics
def save_code_kdj(ts_code):
    query = f"""
        SELECT trade_date, high, low, close
        FROM stock_daily
        WHERE ts_code = {ts_code}
        AND trade_date >= (
            SELECT COALESCE(
                (SELECT trade_date
                FROM stock_daily
                WHERE ts_code = {ts_code}
                    AND trade_date <= (
                    SELECT COALESCE(MAX(trade_date), 0)
                    FROM stock_kdj
                    WHERE ts_code = {ts_code}
                    )
                ORDER BY trade_date DESC
                LIMIT 1 OFFSET 60
                ),
                (SELECT MIN(trade_date) FROM stock_daily WHERE ts_code = {ts_code})
                )
            )
        ORDER BY trade_date ASC
    """
    logger.info(f"开始计算 {ts_code} KDJ")
    logger.info(f"读取数据库 {ts_code} 开始")
    df = pd.read_sql(query, db.engine)
    logger.info(f"读取数据库 {ts_code} 完成, 行数: {len(df)}")
    # 确保trade_date是字符串格式（如果数据库中是int）
    df['trade_date'] = df['trade_date'].astype(str)
    # 计算KDJ
    kdj_df = calculate_ths_kdj(df)
    # 添加ts_code和data_id列
    kdj_df['ts_code'] = ts_code
    kdj_df['data_id'] = kdj_df.apply(lambda x: f"{ts_code}_{x['trade_date']}", axis=1)
    logger.info(f"计算 {ts_code} KDJ 完成, 行数: {len(kdj_df)}")
    if len(kdj_df) == 0:
        return 0
    inserted_count = 0
    # 单日写入
    inserted_count = db.mysql_localhost(sql=f"""
        INSERT INTO stock_kdj (data_id, ts_code, trade_date, k, d, j)
        VALUES ('{kdj_df[-1:].iloc[0]['data_id']}', {kdj_df[-1:].iloc[0]['ts_code']}, {kdj_df[-1:].iloc[0]['trade_date']}, {kdj_df[-1:].iloc[0]['k']}, {kdj_df[-1:].iloc[0]['d']}, {kdj_df[-1:].iloc[0]['j']})
        ON DUPLICATE KEY UPDATE data_id = data_id;
    """, commit=True)
    # # 近日插入
    # for index, row in kdj_df.iterrows():
    #     inserted_count += db.mysql_localhost(sql=f"""
    #         INSERT INTO stock_kdj (data_id, ts_code, trade_date, k, d, j)
    #         VALUES ('{row['data_id']}', {row['ts_code']}, {row['trade_date']}, {row['k']}, {row['d']}, {row['j']})
    #         ON DUPLICATE KEY UPDATE data_id = data_id;
    #     """, commit=True)
    # 增量合并
    # inserted_count = db.smart_insert_to_mysql(kdj_df[-1:], "stock_kdj", db.engine, ['data_id'])
    # 全量写入
    # inserted_count = db.smart_insert_to_mysql(kdj_df, "stock_kdj", db.engine, ['data_id'], query_exists=False)
    # logger.info(f"数据已写入 MySQL 表 stock_kdj 更新条数:{inserted_count}")
    return inserted_count


@common.timer_statistics
def save_daily_kdj():
    inserted_count = 0
    filtered_codes = common.load_stock_pool_symbol()
    for ts_code in tqdm(filtered_codes):
        inserted_count += save_code_kdj(ts_code)
    # with Pool(processes=3) as pool:
    #     results = pool.imap_unordered(save_code_kdj, [(ts_code,) for ts_code in filtered_codes])
    # # 处理结果
    # for ts_code, success, message in results:
    #     if success:
    #         logger.info(message)
    #     else:
    #         logger.error(message)
    logger.info(f"数据已写入 MySQL 表 stock_kdj 更新条数:{inserted_count}")


if __name__ == '__main__':
    save_daily_kdj()
