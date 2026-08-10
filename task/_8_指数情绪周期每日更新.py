import argparse
import json
import os
import sys
from datetime import datetime

from loguru import logger

项目根目录 = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if 项目根目录 not in sys.path:
    sys.path.append(项目根目录)

from utils import db
from 实时监控 import 情绪周期


市场宽度表名 = "t_指数情绪周期_市场宽度"
每日分析表名 = "t_指数情绪周期_每日分析"
上下文交易日数量 = 180
普通主板股票SQL = """
(
    (
        (ts_code BETWEEN 1 AND 3999)
        OR (ts_code BETWEEN 600000 AND 609999)
    )
    AND (stock_name IS NULL OR stock_name NOT LIKE '%ST%')
)
"""
涨跌停幅度SQL = """
0.985
"""


def 更新(start_date=None, end_date=None):
    start_date, end_date = 规范日期范围(start_date, end_date)
    logger.info(f"指数情绪周期每日更新 开始，范围 {start_date} - {end_date}")

    确保表存在()
    上下文开始日期 = 查询上下文开始日期(start_date)
    写入市场宽度统计(上下文开始日期, end_date)

    日期列表 = 查询可分析日期列表(start_date, end_date)
    if not 日期列表:
        logger.warning(f"指数情绪周期每日更新 无可分析日期，范围 {start_date} - {end_date}")
        return {"市场宽度开始日期": 上下文开始日期, "分析日期数量": 0, "成功数量": 0}

    成功数量 = 0
    for 日期 in 日期列表:
        try:
            result = 计算并写入指数周期(日期)
            if result and result.get("状态") == "success":
                成功数量 += 1
        except Exception as e:
            logger.exception(f"{日期} 指数情绪周期计算失败: {e}")

    logger.info(f"指数情绪周期每日更新 完成，分析 {len(日期列表)} 天，成功 {成功数量} 天")
    return {"市场宽度开始日期": 上下文开始日期, "分析日期数量": len(日期列表), "成功数量": 成功数量}


def 确保表存在():
    db.mysql_localhost(
        f"""
        CREATE TABLE IF NOT EXISTS `{市场宽度表名}` (
          `日期` int NOT NULL COMMENT '交易日期 yyyyMMdd',
          `股票总数` int DEFAULT NULL,
          `上涨家数` int DEFAULT NULL,
          `下跌家数` int DEFAULT NULL,
          `涨超5家数` int DEFAULT NULL,
          `跌超5家数` int DEFAULT NULL,
          `涨停家数` int DEFAULT NULL,
          `跌停家数` int DEFAULT NULL,
          `成交额` double DEFAULT NULL COMMENT '全市场成交额，沿用 stock_daily.amount 单位',
          `平均涨跌幅` double DEFAULT NULL,
          `创建时间` datetime DEFAULT CURRENT_TIMESTAMP,
          `更新时间` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`日期`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数情绪周期-每日市场宽度'
        """,
        commit=True,
    )
    db.mysql_localhost(
        f"""
        CREATE TABLE IF NOT EXISTS `{每日分析表名}` (
          `日期` int NOT NULL COMMENT '交易日期 yyyyMMdd',
          `指数名称` varchar(32) DEFAULT '上证指数',
          `周期状态` varchar(32) DEFAULT NULL,
          `周期分数` double DEFAULT NULL,
          `摘要` varchar(512) DEFAULT NULL,
          `开盘` double DEFAULT NULL,
          `收盘` double DEFAULT NULL,
          `最高` double DEFAULT NULL,
          `最低` double DEFAULT NULL,
          `涨跌幅` double DEFAULT NULL,
          `指数成交额` double DEFAULT NULL,
          `指数成交额比例` double DEFAULT NULL,
          `市场成交额比例` double DEFAULT NULL,
          `MA5` double DEFAULT NULL,
          `MA10` double DEFAULT NULL,
          `MA20` double DEFAULT NULL,
          `MA60` double DEFAULT NULL,
          `MA5斜率` double DEFAULT NULL,
          `MA10斜率` double DEFAULT NULL,
          `MA20斜率` double DEFAULT NULL,
          `趋势得分` double DEFAULT NULL,
          `市场宽度得分` double DEFAULT NULL,
          `涨跌停结构得分` double DEFAULT NULL,
          `量能得分` double DEFAULT NULL,
          `风险偏好得分` double DEFAULT NULL,
          `市场宽度JSON` json DEFAULT NULL,
          `信号JSON` json DEFAULT NULL,
          `最近走势JSON` json DEFAULT NULL,
          `波动图JSON` json DEFAULT NULL,
          `完整结果JSON` json DEFAULT NULL,
          `创建时间` datetime DEFAULT CURRENT_TIMESTAMP,
          `更新时间` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`日期`),
          INDEX `idx_周期状态` (`周期状态`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数情绪周期-每日分析结果'
        """,
        commit=True,
    )


def 规范日期范围(start_date=None, end_date=None):
    end_date = 规范日期(end_date) or datetime.now().strftime("%Y%m%d")
    start_date = 规范日期(start_date) or end_date
    if int(start_date) > int(end_date):
        start_date, end_date = end_date, start_date
    return int(start_date), int(end_date)


def 规范日期(value):
    if value in (None, ""):
        return ""
    text = str(value).replace("-", "").replace("/", "").strip()
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"日期格式错误: {value}")
    return text


def 查询上下文开始日期(start_date):
    rows = db.mysql_localhost(
        f"""
        SELECT MIN(`日期`) AS `日期`
        FROM (
            SELECT `日期`
            FROM `akshare_sh000001`
            WHERE `日期` <= %s
            ORDER BY `日期` DESC
            LIMIT {上下文交易日数量}
        ) t
        """,
        params=(int(start_date),),
        fetch=True,
    )
    value = rows[0].get("日期") if rows else None
    return int(value or start_date)


def 写入市场宽度统计(start_date, end_date):
    rows = db.mysql_localhost(
        f"""
        SELECT
            trade_date AS `日期`,
            COUNT(*) AS `股票总数`,
            SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS `上涨家数`,
            SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS `下跌家数`,
            SUM(CASE WHEN pct_chg >= 5 THEN 1 ELSE 0 END) AS `涨超5家数`,
            SUM(CASE WHEN pct_chg <= -5 THEN 1 ELSE 0 END) AS `跌超5家数`,
            SUM(CASE WHEN ({普通主板股票SQL}) AND pre_close > 0 AND close >= ROUND(pre_close * (1 + ({涨跌停幅度SQL})), 2) THEN 1 ELSE 0 END) AS `涨停家数`,
            SUM(CASE WHEN ({普通主板股票SQL}) AND pre_close > 0 AND close <= ROUND(pre_close * (1 - ({涨跌停幅度SQL})), 2) THEN 1 ELSE 0 END) AS `跌停家数`,
            SUM(amount) AS `成交额`,
            AVG(pct_chg) AS `平均涨跌幅`
        FROM stock_daily
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date
        ORDER BY trade_date ASC
        """,
        params=(int(start_date), int(end_date)),
        fetch=True,
    )
    if not rows:
        logger.warning(f"指数情绪周期 市场宽度无数据，范围 {start_date} - {end_date}")
        return 0

    for row in rows:
        db.mysql_localhost(
            f"""
            INSERT INTO `{市场宽度表名}` (
                `日期`, `股票总数`, `上涨家数`, `下跌家数`, `涨超5家数`, `跌超5家数`,
                `涨停家数`, `跌停家数`, `成交额`, `平均涨跌幅`
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `股票总数` = VALUES(`股票总数`),
                `上涨家数` = VALUES(`上涨家数`),
                `下跌家数` = VALUES(`下跌家数`),
                `涨超5家数` = VALUES(`涨超5家数`),
                `跌超5家数` = VALUES(`跌超5家数`),
                `涨停家数` = VALUES(`涨停家数`),
                `跌停家数` = VALUES(`跌停家数`),
                `成交额` = VALUES(`成交额`),
                `平均涨跌幅` = VALUES(`平均涨跌幅`),
                `更新时间` = CURRENT_TIMESTAMP
            """,
            params=(
                取整数(row.get("日期")),
                取整数(row.get("股票总数")),
                取整数(row.get("上涨家数")),
                取整数(row.get("下跌家数")),
                取整数(row.get("涨超5家数")),
                取整数(row.get("跌超5家数")),
                取整数(row.get("涨停家数")),
                取整数(row.get("跌停家数")),
                取浮点数(row.get("成交额")),
                取浮点数(row.get("平均涨跌幅")),
            ),
            commit=True,
        )

    logger.info(f"指数情绪周期 市场宽度写入完成，范围 {start_date} - {end_date}，数量 {len(rows)}")
    return len(rows)


def 查询可分析日期列表(start_date, end_date):
    rows = db.mysql_localhost(
        f"""
        SELECT a.`日期`
        FROM `akshare_sh000001` a
        INNER JOIN `{市场宽度表名}` m ON m.`日期` = a.`日期`
        WHERE a.`日期` BETWEEN %s AND %s
        ORDER BY a.`日期` ASC
        """,
        params=(int(start_date), int(end_date)),
        fetch=True,
    )
    return [取整数(row.get("日期")) for row in rows or [] if row.get("日期")]


def 计算并写入指数周期(日期):
    index_rows = 读取上证指数日线截至(日期)
    market_rows = 读取市场宽度截至(日期)
    result = 情绪周期.计算指数周期结果(index_rows, market_rows)
    if result.get("状态") != "success":
        logger.warning(f"{日期} 指数情绪周期计算无结果: {result.get('错误信息')}")
        return result

    写入指数周期分析(result)
    logger.info(f"{日期} 指数情绪周期写入完成：{result.get('周期状态')} {result.get('周期分数')}")
    return result


def 读取上证指数日线截至(日期, limit=160):
    rows = db.mysql_localhost(
        f"""
        SELECT `日期`, `开盘`, `收盘`, `最高`, `最低`, `成交额`, `涨跌幅`
        FROM `akshare_sh000001`
        WHERE `日期` <= %s
        ORDER BY `日期` DESC
        LIMIT {int(limit)}
        """,
        params=(int(日期),),
        fetch=True,
    )
    return list(reversed(rows or []))


def 读取市场宽度截至(日期, limit=80):
    rows = db.mysql_localhost(
        f"""
        SELECT
            `日期` AS trade_date,
            `股票总数` AS total_count,
            `上涨家数` AS up_count,
            `下跌家数` AS down_count,
            `涨超5家数` AS up_gt5_count,
            `跌超5家数` AS down_lt5_count,
            `涨停家数` AS limit_up_count,
            `跌停家数` AS limit_down_count,
            `成交额` AS amount,
            `平均涨跌幅` AS avg_pct_chg
        FROM `{市场宽度表名}`
        WHERE `日期` <= %s
        ORDER BY `日期` DESC
        LIMIT {int(limit)}
        """,
        params=(int(日期),),
        fetch=True,
    )
    return list(reversed(rows or []))


def 写入指数周期分析(result):
    指数 = result.get("指数") or {}
    均线 = result.get("均线") or {}
    均线斜率 = result.get("均线斜率") or {}
    市场宽度 = result.get("市场宽度") or {}
    分项得分 = result.get("分项得分") or {}

    db.mysql_localhost(
        f"""
        INSERT INTO `{每日分析表名}` (
            `日期`, `指数名称`, `周期状态`, `周期分数`, `摘要`,
            `开盘`, `收盘`, `最高`, `最低`, `涨跌幅`, `指数成交额`, `指数成交额比例`, `市场成交额比例`,
            `MA5`, `MA10`, `MA20`, `MA60`, `MA5斜率`, `MA10斜率`, `MA20斜率`,
            `趋势得分`, `市场宽度得分`, `涨跌停结构得分`, `量能得分`, `风险偏好得分`,
            `市场宽度JSON`, `信号JSON`, `最近走势JSON`, `波动图JSON`, `完整结果JSON`
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            `指数名称` = VALUES(`指数名称`),
            `周期状态` = VALUES(`周期状态`),
            `周期分数` = VALUES(`周期分数`),
            `摘要` = VALUES(`摘要`),
            `开盘` = VALUES(`开盘`),
            `收盘` = VALUES(`收盘`),
            `最高` = VALUES(`最高`),
            `最低` = VALUES(`最低`),
            `涨跌幅` = VALUES(`涨跌幅`),
            `指数成交额` = VALUES(`指数成交额`),
            `指数成交额比例` = VALUES(`指数成交额比例`),
            `市场成交额比例` = VALUES(`市场成交额比例`),
            `MA5` = VALUES(`MA5`),
            `MA10` = VALUES(`MA10`),
            `MA20` = VALUES(`MA20`),
            `MA60` = VALUES(`MA60`),
            `MA5斜率` = VALUES(`MA5斜率`),
            `MA10斜率` = VALUES(`MA10斜率`),
            `MA20斜率` = VALUES(`MA20斜率`),
            `趋势得分` = VALUES(`趋势得分`),
            `市场宽度得分` = VALUES(`市场宽度得分`),
            `涨跌停结构得分` = VALUES(`涨跌停结构得分`),
            `量能得分` = VALUES(`量能得分`),
            `风险偏好得分` = VALUES(`风险偏好得分`),
            `市场宽度JSON` = VALUES(`市场宽度JSON`),
            `信号JSON` = VALUES(`信号JSON`),
            `最近走势JSON` = VALUES(`最近走势JSON`),
            `波动图JSON` = VALUES(`波动图JSON`),
            `完整结果JSON` = VALUES(`完整结果JSON`),
            `更新时间` = CURRENT_TIMESTAMP
        """,
        params=(
            取整数(result.get("交易日期")),
            result.get("指数名称", "上证指数"),
            result.get("周期状态", ""),
            取浮点数(result.get("周期分数")),
            result.get("摘要", ""),
            取浮点数(指数.get("开盘")),
            取浮点数(指数.get("收盘")),
            取浮点数(指数.get("最高")),
            取浮点数(指数.get("最低")),
            取浮点数(指数.get("涨跌幅")),
            取浮点数(指数.get("成交额")),
            取浮点数(指数.get("指数成交额比例")),
            取浮点数(市场宽度.get("成交额比例")),
            取浮点数(均线.get("MA5")),
            取浮点数(均线.get("MA10")),
            取浮点数(均线.get("MA20")),
            取浮点数(均线.get("MA60")),
            取浮点数(均线斜率.get("MA5")),
            取浮点数(均线斜率.get("MA10")),
            取浮点数(均线斜率.get("MA20")),
            取浮点数(分项得分.get("趋势")),
            取浮点数(分项得分.get("市场宽度")),
            取浮点数(分项得分.get("涨跌停结构")),
            取浮点数(分项得分.get("量能")),
            取浮点数(分项得分.get("风险偏好")),
            转JSON(市场宽度),
            转JSON(result.get("信号") or []),
            转JSON(result.get("最近走势") or []),
            转JSON(result.get("波动图") or []),
            转JSON(result),
        ),
        commit=True,
    )


def 转JSON(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def 取整数(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def 取浮点数(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


if __name__ == "__main__":
    更新(start_date=20260601, end_date=20260708)
