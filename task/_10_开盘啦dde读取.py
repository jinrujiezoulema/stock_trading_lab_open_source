# -*- coding: utf-8 -*-
"""读取开盘啦个股 DDE，并补充到 ``stock_daily.dde``。

提供两个独立的数据读取入口：

1. ``读取历史日K_DDE``：读取个股历史日 K 对应的每日 DDE 净额。
2. ``读取分时DDE``：读取当日实时或历史某日的分时 DDE 累计净额。

``更新`` 会查询指定日期范围内 ``stock_daily`` 中尚未写入 DDE 的股票，
每只股票只请求一次日 K DDE，然后按 ``ts_code + trade_date`` 批量更新。
"""

import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests
from loguru import logger

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


实时接口地址 = "https://apphwhq.longhuvip.com/w1/api/index.php"
历史接口地址 = "https://apphis.longhuvip.com/w1/api/index.php"
接口版本 = "w44"
应用版本 = "5.23.0.4"
设备ID = str(uuid.uuid4())

请求头 = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; PHU110 Build/W528JS)",
    "Connection": "Keep-Alive",
    "Accept-Encoding": "gzip",
}

_线程数据 = threading.local()
_日K返回列 = ["stock_code", "trade_date", "dde"]
_分时返回列 = ["stock_code", "trade_date", "time", "dde_cumulative", "dde_1m"]


def 规范股票代码(value):
    """将整数、Tushare 代码或带市场前缀的代码统一成六位数字。"""
    code = str(value).strip().upper()
    if "." in code:
        code = code.split(".", 1)[0]
    if code.endswith(".0"):
        code = code[:-2]
    for prefix in ("SH", "SZ", "BJ"):
        if code.startswith(prefix):
            code = code[len(prefix):]
            break
    if not code.isdigit() or len(code) > 6:
        raise ValueError(f"无效股票代码: {value}")
    return code.zfill(6)


def 规范日期(value):
    """将 YYYY-MM-DD、YYYYMMDD 或整数日期统一成 YYYYMMDD。"""
    if value is None or str(value).strip() == "":
        return None
    date_text = str(value).strip().replace("-", "")
    return datetime.strptime(date_text, "%Y%m%d").strftime("%Y%m%d")


def _空日K结果():
    return pd.DataFrame(columns=_日K返回列)


def _空分时结果():
    return pd.DataFrame(columns=_分时返回列)


def _获取会话():
    session = getattr(_线程数据, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(请求头)
        _线程数据.session = session
    return session


def _请求开盘啦(url, params, timeout=20, retries=3):
    """请求开盘啦接口；网络、JSON 和业务错误均按次数重试。"""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = _获取会话().post(url, data=params, timeout=timeout)
            response.raise_for_status()
            result = response.json()
            if not isinstance(result, dict):
                raise RuntimeError(f"接口返回类型异常: {type(result).__name__}")
            if str(result.get("errcode")) != "0":
                raise RuntimeError(f"接口返回异常: {result}")
            return result
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(0.5 * attempt)
    raise RuntimeError(
        f"开盘啦接口请求失败，动作={params.get('a')}，股票={params.get('StockID')}: {last_error}"
    ) from last_error


def _公共参数(stock_code, phone_os_new):
    return {
        "apiv": 接口版本,
        "PhoneOSNew": str(phone_os_new),
        "DeviceID": 设备ID,
        "VerSion": 应用版本,
        "Token": "0",
        "UserID": "0",
        "StockID": 规范股票代码(stock_code),
    }


def _日K单页(stock_code, index, count, timeout=20, retries=3):
    params = {
        **_公共参数(stock_code, phone_os_new=2),
        "a": "GetDaDanKLine2New",
        "c": "StockLineData",
        "Type": "d",
        "Index": str(index),
        "st": str(count),
    }
    result = _请求开盘啦(历史接口地址, params, timeout=timeout, retries=retries)
    dates = result.get("Date", []) or []
    dde_values = result.get("DDJE", []) or []
    return list(zip(dates, dde_values))


def 读取历史日K_DDE(
    stock_code,
    count=100,
    start_date=None,
    end_date=None,
    timeout=20,
    retries=3,
):
    """读取个股历史日 K DDE 净额，单位元。

    Args:
        stock_code: 股票代码，如 ``000938``、``000938.SZ`` 或整数 ``938``。
        count: 未指定日期范围时返回最近 N 个交易日，默认 100。
        start_date/end_date: 可选日期范围，格式 YYYYMMDD 或 YYYY-MM-DD。
        timeout: 单次请求超时秒数。
        retries: 每页失败重试次数。

    Returns:
        DataFrame，列为 ``stock_code, trade_date, dde``，日期倒序。
    """
    stock_code = 规范股票代码(stock_code)
    start_date = 规范日期(start_date)
    end_date = 规范日期(end_date)

    if start_date and not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    elif end_date and not start_date:
        start_date = end_date
    if start_date and start_date > end_date:
        raise ValueError(f"start_date 不能晚于 end_date: {start_date} > {end_date}")

    if start_date:
        # 接口从最新交易日向前返回。日历天数大于交易日数，加 10 天缓冲后
        # 通常一次即可覆盖目标日期；超过 600 条时再用 Index 分页。
        calendar_days = max(
            0,
            (datetime.now().date() - datetime.strptime(start_date, "%Y%m%d").date()).days,
        )
        page_size = min(600, max(10, calendar_days + 10))
        target_count = None
    else:
        target_count = int(count)
        if target_count <= 0:
            raise ValueError("count 必须大于 0")
        page_size = min(600, max(10, target_count))

    records = []
    index = 0
    while True:
        page = _日K单页(
            stock_code,
            index=index,
            count=page_size,
            timeout=timeout,
            retries=retries,
        )
        if not page:
            break

        records.extend(page)
        page_dates = []
        for raw_date, _ in page:
            try:
                page_dates.append(规范日期(raw_date))
            except (TypeError, ValueError):
                continue

        if not page_dates:
            break
        if start_date:
            if min(page_dates) <= start_date or len(page) < page_size:
                break
        elif len(records) >= target_count or len(page) < page_size:
            break

        index += len(page)

    if not records:
        return _空日K结果()

    rows = []
    for raw_date, dde in records:
        try:
            trade_date = 规范日期(raw_date)
        except (TypeError, ValueError):
            continue
        rows.append({"stock_code": stock_code, "trade_date": trade_date, "dde": dde})

    if not rows:
        return _空日K结果()

    df = pd.DataFrame(rows).drop_duplicates(subset=["trade_date"], keep="first")
    df["dde"] = pd.to_numeric(df["dde"], errors="coerce")
    df = df.dropna(subset=["dde"])
    if start_date:
        df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]
    else:
        df = df.head(target_count)
    return df[_日K返回列].sort_values("trade_date", ascending=False).reset_index(drop=True)


def 读取分时DDE(stock_code, trade_date=None, timeout=20, retries=3):
    """读取当日实时或历史某日的分时 DDE。

    ``dde_cumulative`` 是从开盘累计到该分钟的 DDE 净额；``dde_1m`` 是
    相邻累计值之差，即该分钟新增净额。两个字段单位均为元。
    """
    stock_code = 规范股票代码(stock_code)
    trade_date = 规范日期(trade_date)

    if trade_date:
        url = 历史接口地址
        params = {
            **_公共参数(stock_code, phone_os_new=1),
            "a": "GetStockDaDanTrend",
            "c": "StockL2History",
            "Day": trade_date,
        }
    else:
        url = 实时接口地址
        params = {
            **_公共参数(stock_code, phone_os_new=2),
            "a": "GetStockDaDanTrendIncremental",
            "c": "StockL2Data",
        }

    result = _请求开盘啦(url, params, timeout=timeout, retries=retries)
    dde_rows = result.get("dadanjinge", []) or []
    dde_rows = [row[:2] for row in dde_rows if isinstance(row, (list, tuple)) and len(row) >= 2]
    if not dde_rows:
        return _空分时结果()

    response_date = trade_date
    if not response_date:
        try:
            response_date = 规范日期(result.get("day"))
        except (TypeError, ValueError):
            response_date = datetime.now().strftime("%Y%m%d")

    df = pd.DataFrame(dde_rows, columns=["time", "dde_cumulative"])
    df["dde_cumulative"] = pd.to_numeric(df["dde_cumulative"], errors="coerce")
    df = df.dropna(subset=["dde_cumulative"]).reset_index(drop=True)
    if df.empty:
        return _空分时结果()

    df["dde_1m"] = df["dde_cumulative"].diff()
    df.loc[df.index[0], "dde_1m"] = df.loc[df.index[0], "dde_cumulative"]
    df.insert(0, "trade_date", response_date)
    df.insert(0, "stock_code", stock_code)
    return df[_分时返回列]


def 查询待更新股票代码(start_date, end_date, only_missing=True):
    """查询目标日期范围内需要读取 DDE 的股票代码。"""
    from utils import db

    where = [
        f"trade_date >= {int(start_date)}",
        f"trade_date <= {int(end_date)}",
    ]
    if only_missing:
        where.append("dde IS NULL")
    query = f"""
        SELECT DISTINCT ts_code
        FROM stock_daily
        WHERE {' AND '.join(where)}
        ORDER BY ts_code
    """
    result = pd.read_sql(query, db.engine)
    if result.empty:
        return []
    return [int(item) for item in result["ts_code"].dropna().tolist()]


def 更新日线DDE字段(df, only_missing=True, batch_size=5000):
    """通过临时表按股票和交易日批量更新 ``stock_daily.dde``。

    使用 ``executemany(UPDATE ...)`` 回填多年数据时会逐行执行，速度很慢。
    这里先批量写入连接级临时表，再用一次 ``UPDATE JOIN`` 更新正式表。
    """
    from utils import db

    if df.empty:
        return 0

    df = df.drop_duplicates(subset=["stock_code", "trade_date"], keep="first")

    cnx = db.mysql_localhost_pool.get_connection()
    cursor = cnx.cursor()
    try:
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS `tmp_stock_daily_dde`")
        cursor.execute(
            """
            CREATE TEMPORARY TABLE `tmp_stock_daily_dde` (
                `ts_code` int NOT NULL,
                `trade_date` int NOT NULL,
                `dde` double NULL,
                PRIMARY KEY (`ts_code`, `trade_date`)
            ) ENGINE=InnoDB
            """
        )

        insert_sql = """
            INSERT INTO `tmp_stock_daily_dde` (`ts_code`, `trade_date`, `dde`)
            VALUES (%s, %s, %s)
        """
        params = []
        staged_count = 0
        for row in df.itertuples(index=False):
            if pd.isna(row.dde):
                continue
            params.append((int(row.stock_code), int(row.trade_date), float(row.dde)))
            if len(params) >= batch_size:
                cursor.executemany(insert_sql, params)
                staged_count += len(params)
                params.clear()
        if params:
            cursor.executemany(insert_sql, params)
            staged_count += len(params)
        if staged_count == 0:
            return 0

        update_sql = """
            UPDATE stock_daily AS target
            INNER JOIN `tmp_stock_daily_dde` AS source
                    ON source.ts_code = target.ts_code
                   AND source.trade_date = target.trade_date
            SET target.dde = source.dde
        """
        if only_missing:
            update_sql += " WHERE target.dde IS NULL"
        cursor.execute(update_sql)
        updated_count = max(0, cursor.rowcount)
        cnx.commit()
        return updated_count
    except Exception:
        cnx.rollback()
        raise
    finally:
        try:
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS `tmp_stock_daily_dde`")
        except Exception:
            pass
        cursor.close()
        cnx.close()


def 更新(
    start_date=None,
    end_date=None,
    only_missing=True,
    max_workers=8,
    timeout=20,
    retries=3,
    stock_batch_size=None,
):
    """读取每只股票的日 K DDE，并分批更新 ``stock_daily.dde``。

    长日期范围会产生上千万条数据，因此不会再把所有股票结果一次性合并。
    每个股票批次读取完成后立即写库并释放内存，可安全中断后继续执行。
    """
    started_at = time.time()
    start_date = 规范日期(start_date or datetime.now().strftime("%Y%m%d"))
    end_date = 规范日期(end_date or start_date)
    if start_date > end_date:
        raise ValueError(f"start_date 不能晚于 end_date: {start_date} > {end_date}")
    max_workers = int(max_workers)
    if max_workers <= 0:
        raise ValueError("max_workers 必须大于 0")

    if stock_batch_size is None:
        calendar_days = (
            datetime.strptime(end_date, "%Y%m%d").date()
            - datetime.strptime(start_date, "%Y%m%d").date()
        ).days + 1
        estimated_rows_per_stock = max(1, calendar_days * 5 // 7)
        minimum_batch_size = min(200, max_workers * 2)
        stock_batch_size = max(
            minimum_batch_size,
            min(200, max(1, 100000 // estimated_rows_per_stock)),
        )
    stock_batch_size = int(stock_batch_size)
    if stock_batch_size <= 0:
        raise ValueError("stock_batch_size 必须大于 0")

    stock_codes = 查询待更新股票代码(start_date, end_date, only_missing=only_missing)
    if not stock_codes:
        logger.info(f"{start_date} - {end_date} 没有需要更新 DDE 的 stock_daily 记录")
        return 0

    logger.info(
        f"开始更新 stock_daily.dde，日期 {start_date} - {end_date}，"
        f"股票 {len(stock_codes)} 只，并发数 {max_workers}，每批 {stock_batch_size} 只"
    )

    empty_codes = []
    failed = []
    successful_code_count = 0
    total_interface_rows = 0
    total_updated = 0
    total_batches = (len(stock_codes) + stock_batch_size - 1) // stock_batch_size

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch_number, batch_start in enumerate(
            range(0, len(stock_codes), stock_batch_size),
            start=1,
        ):
            batch_codes = stock_codes[batch_start:batch_start + stock_batch_size]
            batch_frames = []
            batch_failed = []
            futures = {
                executor.submit(
                    读取历史日K_DDE,
                    stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    timeout=timeout,
                    retries=retries,
                ): stock_code
                for stock_code in batch_codes
            }
            for future in as_completed(futures):
                stock_code = futures[future]
                try:
                    stock_df = future.result()
                    if stock_df.empty:
                        empty_codes.append(stock_code)
                    else:
                        batch_frames.append(stock_df)
                        successful_code_count += 1
                except Exception as exc:
                    batch_failed.append((stock_code, str(exc)))

            # 并发阶段失败的股票使用主线程和更多重试次数再请求一次，
            # 避免单个瞬时网络错误导致整个多年回填任务失败。
            if batch_failed:
                logger.warning(
                    f"第 {batch_number}/{total_batches} 批有 {len(batch_failed)} 只股票失败，开始二次重试"
                )
            final_batch_failed = []
            for stock_code, _ in batch_failed:
                try:
                    stock_df = 读取历史日K_DDE(
                        stock_code,
                        start_date=start_date,
                        end_date=end_date,
                        timeout=timeout,
                        retries=max(5, retries + 2),
                    )
                    if stock_df.empty:
                        empty_codes.append(stock_code)
                    else:
                        batch_frames.append(stock_df)
                        successful_code_count += 1
                except Exception as exc:
                    final_batch_failed.append((stock_code, str(exc)))
            failed.extend(final_batch_failed)

            batch_interface_rows = 0
            batch_updated = 0
            if batch_frames:
                batch_dde = (
                    pd.concat(batch_frames, ignore_index=True)
                    .drop_duplicates(subset=["stock_code", "trade_date"], keep="first")
                )
                batch_interface_rows = len(batch_dde)
                logger.info(
                    f"第 {batch_number}/{total_batches} 批读取完成，"
                    f"接口 {batch_interface_rows} 条，开始写入 stock_daily.dde"
                )
                batch_updated = 更新日线DDE字段(batch_dde, only_missing=only_missing)
                total_interface_rows += batch_interface_rows
                total_updated += batch_updated
                del batch_dde
            del batch_frames, futures

            processed_count = min(batch_start + len(batch_codes), len(stock_codes))
            logger.info(
                f"DDE 分批写库进度 {processed_count}/{len(stock_codes)} "
                f"({batch_number}/{total_batches})，本批接口 {batch_interface_rows} 条，"
                f"本批更新 {batch_updated} 条，累计更新 {total_updated} 条，"
                f"空数据 {len(empty_codes)}，最终失败 {len(failed)}"
            )

    if successful_code_count == 0:
        failure_sample = failed[:5]
        raise RuntimeError(
            f"开盘啦未返回任何可更新的日 K DDE；空数据 {len(empty_codes)}，"
            f"失败 {len(failed)}，失败示例 {failure_sample}"
        )

    if empty_codes:
        logger.warning(
            f"{len(empty_codes)} 只股票在目标日期没有 DDE 数据，示例: {empty_codes[:20]}"
        )
    if failed:
        logger.error(f"{len(failed)} 只股票 DDE 请求最终失败，示例: {failed[:10]}")
        raise RuntimeError(
            f"DDE 已分批更新 {total_updated} 条，但仍有 {len(failed)} 只股票请求失败；"
            "未写入每日更新完成标记，重新执行时会继续补充缺失值"
        )

    logger.info(
        f"stock_daily.dde 更新完成，接口数据 {total_interface_rows} 条，"
        f"数据库影响 {total_updated} 条，耗时 {time.time() - started_at:.2f} 秒"
    )
    return total_updated


def 主函数(
    start_date=None,
    end_date=None,
    force=False,
    max_workers=8,
    timeout=20,
    retries=3,
    stock_batch_size=None,
):
    """按文件底部配置执行日 K DDE 更新。"""
    return 更新(
        start_date=start_date,
        end_date=end_date,
        only_missing=not force,
        max_workers=max_workers,
        timeout=timeout,
        retries=retries,
        stock_batch_size=stock_batch_size,
    )


normalize_stock_code = 规范股票代码
normalize_date = 规范日期
fetch_daily_dde = 读取历史日K_DDE
fetch_intraday_dde = 读取分时DDE
query_pending_stock_codes = 查询待更新股票代码
update_stock_daily_dde = 更新日线DDE字段
update = 更新
main = 主函数


if __name__ == "__main__":
    # ==================== 用户运行参数：按需修改 ====================
    开始日期 = "20100104"  # DDE 回填开始日期，格式 YYYYMMDD。
    结束日期 = "20260715"  # DDE 回填结束日期，格式 YYYYMMDD。
    是否强制覆盖已有DDE = False  # False 只补 dde 为空的记录；True 会覆盖已有值。
    接口并发数 = 8  # 同时请求的股票数量；过高可能触发接口限流。
    单次请求超时秒数 = 20  # 开盘啦单次 HTTP 请求的最长等待时间。
    接口失败重试次数 = 3  # 每页接口请求失败后的重试次数。
    每批股票数量 = None  # None 表示根据日期跨度自动计算；也可以手动填写正整数。

    主函数(
        start_date=开始日期,
        end_date=结束日期,
        force=是否强制覆盖已有DDE,
        max_workers=接口并发数,
        timeout=单次请求超时秒数,
        retries=接口失败重试次数,
        stock_batch_size=每批股票数量,
    )
