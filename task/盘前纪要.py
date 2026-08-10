from DrissionPage import WebPage, ChromiumOptions
import time
from loguru import logger
import pandas as pd
from datetime import datetime, timedelta
from functools import lru_cache
from io import StringIO
from pathlib import Path
from bs4 import BeautifulSoup

from utils import common, db, ini_util, model_util
from utils import driver_chrome

driver_web = None
def init_driver():
    global driver_web
    driver_web = driver_chrome.initDriver()


def find_stocks_in_text_by_list(text: str, stock_list: list) -> list:
    """
    根据你的股票名称列表，在文本中按出现顺序提取提到的股票
    :param text: 新闻文本
    :param stock_list: 你的股票全称列表，例如 ['诺德股份', '宝鼎科技', '京东方A', ...]
    :return: 按在文本中首次出现顺序排序的股票列表
    """
    mentioned = []

    for stock in stock_list:
        if stock in text and stock not in mentioned:
            mentioned.append(stock)

    # 按在文本中第一次出现的位置排序
    mentioned.sort(key=lambda s: text.find(s))

    return mentioned
@common.timer_statistics
def 韭研公社盘前纪要采集(start_date):
    if db.redis_con_localhost.exists(f"盘前纪要.py:{start_date}"):
        logger.info(f"今日更新已执行完成，请勿重复执行。")
        return
    # if db.redis_con_localhost.exists(f"run_check:盘前纪要.py"):
    #     logger.info(f"当前存在 盘前纪要.py 正在执行，请勿重复执行。")
    #     return
    db.redis_con_localhost.set(f"run_check:盘前纪要.py", datetime.now().strftime('%Y%m%d %H:%M:%S'))
    if driver_web is None:
        init_driver()
    logger.info(f"开始采集 {start_date} 盘前纪要")
    driver_web.get("https://www.jiuyangongshe.com/u/4df747be1bf143a998171ef03559b517")

    stock_pool = pd.read_sql(f"SELECT symbol,ts_code,name FROM stock_basic", db.engine)
    # logger.info(f"加载股票池，数量：{len(stock_pool)}")
    filtered_names = stock_pool['name'].tolist()
    # 构建名称 -> 代码 映射（symbol 通常是6位纯代码）
    name_to_code = dict(zip(stock_pool['name'], stock_pool['symbol']))
    soup = BeautifulSoup(driver_web.html, features='lxml')
    line_row = soup.find(name='div', attrs={'class': 'articleTypeTopR'})
    logger.info(f"最新一期为：{line_row.text}")
    logger.info(f"开始打开")
    while True:
        try:
            driver_web.ele('.articleTypeTopR').click()
            new_tab = driver_web.latest_tab
            break
        except Exception as e:
            # logger.error(f"打开新页面失败：{e}")
            time.sleep(1)
    logger.info(f"打开新页面成功")
    while True:
        try:
            content_text = BeautifulSoup(new_tab.html, features='lxml').find(name='div', attrs={'class': 'text-box text-justify fsDetail'}).text
            content_text_目标 = content_text.split("二、")[1].split("No.2 公告精选")[0]
            break
        except Exception as e:
            # logger.error(f"打开新页面失败：{e}")
            time.sleep(1)
    logger.warning(f"{content_text}")
    result = find_stocks_in_text_by_list(content_text_目标, filtered_names)
    if not result:
        logger.warning(f"{start_date} 未提取到股票，跳过生成ini文件")
        return
    ini_items = []
    unmatched_names = []

    logger.info("按出现顺序提取到的股票：")
    for name in result:
        code = name_to_code.get(name)
        if code:
            stock_code = str(code).zfill(6)
            ini_items.append((stock_code, name))
        else:
            unmatched_names.append(name)
            ini_items.append(("000000", name))  # 未匹配到的给占位
        logger.info(f"{ini_items[-1][0]} {ini_items[-1][1]}")
    if unmatched_names:
        logger.warning(f"以下股票未在 stock_basic 中匹配到代码: {unmatched_names}")
    # 创建输出目录（参考你 _生成板块股票ini 的风格）
    output_dir = Path(__file__).resolve().parents[1] / 'output' / '韭研公社盘前纪要' / str(start_date)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 生成 ini 文件名
    merged_stock_count = len(ini_items)
    merged_ini_path = output_dir / f"{merged_stock_count}_盘前纪要提及股票.ini"

    # 调用你已有的写入工具
    ini_util.写入列表ini(ini_items, output_dir, merged_ini_path.name)

    logger.info(
        f"韭研公社盘前纪要 {start_date} ini文件生成完成 | "
        f"路径: {merged_ini_path} | 股票数量: {merged_stock_count}"
    )
    db.redis_con_localhost.set(f"盘前纪要.py:{start_date}", datetime.now().strftime('%Y%m%d %H:%M:%S'))
    db.redis_con_localhost.delete(f"run_check:盘前纪要.py")
    pass

if __name__ == '__main__':
    韭研公社盘前纪要采集(start_date=datetime.now().strftime('%Y%m%d'))