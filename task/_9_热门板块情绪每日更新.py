import json
import os
import sys
from datetime import datetime

from loguru import logger


项目根目录 = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if 项目根目录 not in sys.path:
    sys.path.append(项目根目录)

from utils import db, 热门板块情绪算法


每日分析表名 = "t_热门板块情绪_每日分析"
分析窗口交易日数 = 30
排除板块 = 热门板块情绪算法.热门板块排除集合

分析字段 = [
    "日期", "板块", "样本来源日期", "前日榜单数据完整", "当日榜单数据完整",
    "前日板块数量", "前日股票池数量", "前日明细覆盖率", "当日板块数量", "当日股票明细数量",
    "有效样本数", "行情覆盖率", "平均涨跌幅", "中位数涨跌幅", "平均振幅", "涨幅标准差",
    "晋级家数", "晋级率", "新晋级家数", "新晋级率", "红盘家数", "红盘率", "大涨家数", "大涨率", "大跌家数", "大跌率",
    "炸板家数", "炸板率", "同板块留存家数", "同板块留存率",
    "热度阶段", "承接情绪", "综合状态", "情绪分", "判定摘要", "判定依据JSON",
]


def 更新(start_date=None, end_date=None):
    global 排除板块
    当前配置 = 热门板块情绪算法.刷新运行配置()
    排除板块 = 热门板块情绪算法.热门板块排除集合
    截止日期 = 规范日期(end_date) or 规范日期(start_date) or int(datetime.now().strftime("%Y%m%d"))
    logger.info(
        f"热门板块情绪每日更新 开始，截止日期 {截止日期}，"
        f"入选阈值 {当前配置['热门板块入选数量阈值']}只，高潮阈值 {当前配置['高潮数量阈值']}只"
    )
    确保表存在()
    清理排除板块历史数据()

    上下文日期 = 查询最近交易日期(截止日期, 分析窗口交易日数 + 1)
    if len(上下文日期) < 2:
        logger.warning(f"热门板块情绪每日更新 交易日上下文不足，截止日期 {截止日期}")
        return {"截止日期": 截止日期, "分析日期数量": 0, "热门板块数量": 0, "写入数量": 0}

    分析日期 = 上下文日期[-分析窗口交易日数:]
    异动明细 = 查询异动明细(上下文日期)
    榜单有效日期 = {热门板块情绪算法.取整数(row.get("date")) for row in 异动明细}
    热门板块 = 查询热门板块(异动明细, set(分析日期))
    清理分析窗口非入选板块(分析日期, 热门板块)
    if not 热门板块:
        logger.warning(
            f"热门板块情绪每日更新 近{len(分析日期)}个交易日没有达到"
            f"{热门板块情绪算法.热门板块入选数量阈值}只入选阈值的板块"
        )
        return {
            "截止日期": 分析日期[-1],
            "分析日期数量": len(分析日期),
            "热门板块数量": 0,
            "写入数量": 0,
        }

    异动映射, 板块数量映射 = 构建异动映射(异动明细, 热门板块)
    股票代码集合 = {
        code
        for stocks in 异动映射.values()
        for code in stocks.keys()
    }
    行情映射 = 查询股票行情(分析日期, 股票代码集合)
    上一交易日映射 = {
        上下文日期[index]: 上下文日期[index - 1]
        for index in range(1, len(上下文日期))
    }

    写入数量 = 0
    状态统计 = {}
    for 日期 in 分析日期:
        样本来源日期 = 上一交易日映射.get(日期)
        for 板块 in 热门板块:
            前日股票 = list(异动映射.get((样本来源日期, 板块), {}).values())
            当日股票 = list(异动映射.get((日期, 板块), {}).values())
            前日榜单数据完整 = 样本来源日期 in 榜单有效日期
            当日榜单数据完整 = 日期 in 榜单有效日期
            前日板块数量 = 板块数量映射.get((样本来源日期, 板块), 0) if 前日榜单数据完整 else None
            当日板块数量 = 板块数量映射.get((日期, 板块), 0) if 当日榜单数据完整 else None

            result = 热门板块情绪算法.生成每日分析(
                日期=日期,
                板块=板块,
                样本来源日期=样本来源日期,
                前日股票=前日股票,
                当日股票=当日股票,
                当日行情=行情映射.get(日期, {}),
                前日板块数量=前日板块数量,
                当日板块数量=当日板块数量,
                前日榜单数据完整=前日榜单数据完整,
                当日榜单数据完整=当日榜单数据完整,
            )
            写入分析结果(result)
            写入数量 += 1
            状态 = result.get("综合状态")
            状态统计[状态] = 状态统计.get(状态, 0) + 1

    logger.info(
        f"热门板块情绪每日更新 完成，截止 {分析日期[-1]}，"
        f"板块 {len(热门板块)} 个，分析 {写入数量} 条，状态统计 {状态统计}"
    )
    return {
        "截止日期": 分析日期[-1],
        "分析日期数量": len(分析日期),
        "热门板块数量": len(热门板块),
        "热门板块": 热门板块,
        "写入数量": 写入数量,
        "状态统计": 状态统计,
    }


def 确保表存在():
    db.mysql_localhost(
        f"""
        CREATE TABLE IF NOT EXISTS `{每日分析表名}` (
          `日期` int NOT NULL COMMENT '当前交易日期 yyyyMMdd',
          `板块` varchar(32) NOT NULL,
          `样本来源日期` int DEFAULT NULL COMMENT '严格取上一交易日',
          `前日榜单数据完整` tinyint NOT NULL DEFAULT 0,
          `当日榜单数据完整` tinyint NOT NULL DEFAULT 0,
          `前日板块数量` int DEFAULT NULL COMMENT '韭研公社返回的板块个股数量',
          `前日股票池数量` int DEFAULT NULL COMMENT '前日实际落库去重股票数',
          `前日明细覆盖率` double DEFAULT NULL,
          `当日板块数量` int DEFAULT NULL,
          `当日股票明细数量` int DEFAULT NULL,
          `有效样本数` int DEFAULT NULL,
          `行情覆盖率` double DEFAULT NULL,
          `平均涨跌幅` double DEFAULT NULL,
          `中位数涨跌幅` double DEFAULT NULL,
          `平均振幅` double DEFAULT NULL,
          `涨幅标准差` double DEFAULT NULL,
          `晋级家数` int DEFAULT NULL,
          `晋级率` double DEFAULT NULL,
          `新晋级家数` int DEFAULT NULL COMMENT '当日股票池中不属于上一日股票池的涨停家数',
          `新晋级率` double DEFAULT NULL COMMENT '新增涨停家数占上一日股票池比例',
          `红盘家数` int DEFAULT NULL,
          `红盘率` double DEFAULT NULL,
          `大涨家数` int DEFAULT NULL,
          `大涨率` double DEFAULT NULL,
          `大跌家数` int DEFAULT NULL,
          `大跌率` double DEFAULT NULL,
          `炸板家数` int DEFAULT NULL,
          `炸板率` double DEFAULT NULL,
          `同板块留存家数` int DEFAULT NULL,
          `同板块留存率` double DEFAULT NULL,
          `热度阶段` varchar(16) DEFAULT NULL,
          `承接情绪` varchar(16) DEFAULT NULL,
          `综合状态` varchar(16) DEFAULT NULL,
          `情绪分` double DEFAULT NULL,
          `判定摘要` varchar(512) DEFAULT NULL,
          `判定依据JSON` json DEFAULT NULL,
          `创建时间` datetime DEFAULT CURRENT_TIMESTAMP,
          `更新时间` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`日期`, `板块`),
          INDEX `idx_热门板块情绪_板块日期` (`板块`, `日期`),
          INDEX `idx_热门板块情绪_状态日期` (`综合状态`, `日期`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='热门板块隔日股票池情绪分析'
        """,
        commit=True,
    )
    确保分析字段完整()


def 确保分析字段完整():
    现有字段 = {
        str(row.get("Field"))
        for row in (db.mysql_localhost(f"SHOW COLUMNS FROM `{每日分析表名}`", fetch=True) or [])
    }
    新增字段 = [
        ("新晋级家数", "int DEFAULT NULL COMMENT '当日股票池中不属于上一日股票池的涨停家数' AFTER `晋级率`"),
        ("新晋级率", "double DEFAULT NULL COMMENT '新增涨停家数占上一日股票池比例' AFTER `新晋级家数`"),
    ]
    for 字段名, 字段定义 in 新增字段:
        if 字段名 in 现有字段:
            continue
        db.mysql_localhost(
            f"ALTER TABLE `{每日分析表名}` ADD COLUMN `{字段名}` {字段定义}",
            commit=True,
        )
        现有字段.add(字段名)


def 清理排除板块历史数据():
    if not 排除板块:
        return
    placeholders = ",".join(["%s"] * len(排除板块))
    db.mysql_localhost(
        f"DELETE FROM `{每日分析表名}` WHERE `板块` IN ({placeholders})",
        params=tuple(sorted(排除板块)),
        commit=True,
    )


def 清理分析窗口非入选板块(日期列表, 热门板块):
    if not 日期列表:
        return
    date_placeholders = ",".join(["%s"] * len(日期列表))
    params = [int(date) for date in 日期列表]
    sql = f"DELETE FROM `{每日分析表名}` WHERE `日期` IN ({date_placeholders})"
    if 热门板块:
        board_placeholders = ",".join(["%s"] * len(热门板块))
        sql += f" AND `板块` NOT IN ({board_placeholders})"
        params.extend(热门板块)
    db.mysql_localhost(sql, params=tuple(params), commit=True)


def 查询最近交易日期(截止日期, limit):
    rows = db.mysql_localhost(
        f"""
        SELECT `日期`
        FROM `akshare_sh000001`
        WHERE `日期` <= %s
        ORDER BY `日期` DESC
        LIMIT {int(limit)}
        """,
        params=(int(截止日期),),
        fetch=True,
    )
    return list(reversed([热门板块情绪算法.取整数(row.get("日期")) for row in rows or []]))


def 查询异动明细(日期列表):
    if not 日期列表:
        return []
    placeholders = ",".join(["%s"] * len(日期列表))
    return db.mysql_localhost(
        f"""
        SELECT `date`, `板块`, `板块个股数量`, `股票代码`, `股票名称`, `几天几板`
        FROM `t_韭研公社异动解析`
        WHERE `date` IN ({placeholders})
        ORDER BY `date` ASC, `板块` ASC, `股票代码` ASC
        """,
        params=tuple(int(date) for date in 日期列表),
        fetch=True,
    ) or []


def 查询热门板块(异动明细, 分析日期集合):
    峰值映射 = {}
    for row in 异动明细:
        日期 = 热门板块情绪算法.取整数(row.get("date"))
        板块 = str(row.get("板块") or "").strip()
        if 日期 not in 分析日期集合 or not 板块 or 板块 in 排除板块:
            continue
        峰值映射[板块] = max(
            峰值映射.get(板块, 0),
            热门板块情绪算法.取整数(row.get("板块个股数量")),
        )
    return [
        board
        for board, _ in sorted(峰值映射.items(), key=lambda item: (-item[1], item[0]))
        if 峰值映射[board] >= 热门板块情绪算法.热门板块入选数量阈值
    ]


def 构建异动映射(异动明细, 热门板块):
    热门板块集合 = set(热门板块)
    异动映射 = {}
    板块数量映射 = {}
    for row in 异动明细:
        板块 = str(row.get("板块") or "").strip()
        if 板块 not in 热门板块集合:
            continue
        日期 = 热门板块情绪算法.取整数(row.get("date"))
        code = 热门板块情绪算法.取整数(row.get("股票代码"))
        key = (日期, 板块)
        if 热门板块情绪算法.是沪深主板非ST股票(code, row.get("股票名称")):
            异动映射.setdefault(key, {})[code] = {
                "股票代码": code,
                "股票名称": row.get("股票名称"),
                "几天几板": row.get("几天几板"),
            }
        板块数量映射[key] = max(
            板块数量映射.get(key, 0),
            热门板块情绪算法.取整数(row.get("板块个股数量")),
        )
    return 异动映射, 板块数量映射


def 查询股票行情(日期列表, 股票代码集合):
    if not 日期列表 or not 股票代码集合:
        return {}
    日期列表 = sorted(set(日期列表))
    股票代码列表 = sorted(set(股票代码集合))
    date_placeholders = ",".join(["%s"] * len(日期列表))
    code_placeholders = ",".join(["%s"] * len(股票代码列表))
    rows = db.mysql_localhost(
        f"""
        SELECT `trade_date`, `ts_code`, `stock_name`, `high`, `low`, `pre_close`, `pct_chg`
        FROM `stock_daily`
        WHERE `trade_date` IN ({date_placeholders})
          AND `ts_code` IN ({code_placeholders})
        """,
        params=tuple(日期列表 + 股票代码列表),
        fetch=True,
    ) or []

    result = {}
    for row in rows:
        日期 = 热门板块情绪算法.取整数(row.get("trade_date"))
        code = 热门板块情绪算法.取整数(row.get("ts_code"))
        result.setdefault(日期, {})[code] = row
    return result


def 写入分析结果(result):
    columns_sql = ", ".join(f"`{column}`" for column in 分析字段)
    placeholders = ", ".join(["%s"] * len(分析字段))
    update_columns = [column for column in 分析字段 if column not in {"日期", "板块"}]
    updates_sql = ", ".join(f"`{column}` = VALUES(`{column}`)" for column in update_columns)
    params = []
    for column in 分析字段:
        if column == "判定依据JSON":
            params.append(json.dumps(result.get("判定依据") or {}, ensure_ascii=False, separators=(",", ":")))
        else:
            params.append(result.get(column))

    db.mysql_localhost(
        f"""
        INSERT INTO `{每日分析表名}` ({columns_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
          {updates_sql},
          `更新时间` = CURRENT_TIMESTAMP
        """,
        params=tuple(params),
        commit=True,
    )


def 规范日期(value):
    if value in (None, ""):
        return None
    text = str(value).replace("-", "").replace("/", "").strip()
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"日期格式错误: {value}")
    return int(text)


def main():
    print(更新(start_date=启动开始日期, end_date=启动截止日期))


if __name__ == "__main__":
    # 手动重算时可填写 YYYYMMDD；设置为 None 时默认使用当天作为截止日期。
    启动开始日期 = None
    启动截止日期 = None
    main()
