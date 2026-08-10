import math
import time
from typing import Dict

import pandas as pd
import requests
from akshare.utils.tqdm import get_tqdm
from loguru import logger
from datetime import datetime
from datetime import timedelta

from utils import db, api

proxies = {
    'http': 'http://127.0.0.1:7897',
    'https': 'http://127.0.0.1:7897'
}


def fetch_paginated_data(url: str, base_params: Dict, timeout: int = 15):
    """
    东方财富-分页获取数据并合并结果
    https://quote.eastmoney.com/f1.html?newcode=0.000001
    :param url: 股票代码
    :type url: str
    :param base_params: 基础请求参数
    :type base_params: dict
    :param timeout: 请求超时时间
    :type timeout: str
    :return: 合并后的数据
    :rtype: pandas.DataFrame
    """
    # 复制参数以避免修改原始参数
    params = base_params.copy()
    # 获取第一页数据，用于确定分页信息
    r = requests.get(url, params=params, proxies=proxies)
    data_json = r.json()
    # 计算分页信息
    per_page_num = len(data_json["data"]["diff"])
    total_page = math.ceil(data_json["data"]["total"] / per_page_num)
    # 存储所有页面数据
    temp_list = []
    # 添加第一页数据
    temp_list.append(pd.DataFrame(data_json["data"]["diff"]))
    # 获取进度条
    tqdm = get_tqdm()
    # 获取剩余页面数据
    for page in tqdm(range(2, total_page + 1), leave=False):
        params.update({"pn": page})
        r = requests.get(url, params=params, proxies=proxies)
        data_json = r.json()
        inner_temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_list.append(inner_temp_df)
    # 合并所有数据
    temp_df = pd.concat(temp_list, ignore_index=True)
    temp_df["f3"] = pd.to_numeric(temp_df["f3"], errors="coerce")
    temp_df.sort_values(by=["f3"], ascending=False, inplace=True, ignore_index=True)
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df["index"].astype(int) + 1
    return temp_df


def index_code_id_map_em() -> dict:
    """
    东方财富-股票和市场代码
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 股票和市场代码
    :rtype: dict
    """
    url = "https://80.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "100",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "b:MK0010,m:1+t:1,m:0 t:5,m:1+s:3,m:0+t:5,m:2",
        "fields": "f3,f12,f13",
    }
    temp_df = fetch_paginated_data(url, params)
    code_id_dict = dict(zip(temp_df["f12"], temp_df["f13"]))
    return code_id_dict


def index_zh_a_hist(symbol, period, start_date, end_date):
    code_id_dict = index_code_id_map_em()
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    try:
        params = {
            "secid": f"{code_id_dict[symbol]}.{symbol}",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": period_dict[period],
            "fqt": "0",
            "beg": "0",
            "end": "20500000",
        }
    except KeyError:
        params = {
            "secid": f"1.{symbol}",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": period_dict[period],
            "fqt": "0",
            "beg": "0",
            "end": "20500000",
        }
        r = requests.get(url, params=params, proxies=proxies)
        data_json = r.json()
        if data_json["data"] is None:
            params = {
                "secid": f"0.{symbol}",
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": period_dict[period],
                "fqt": "0",
                "beg": "0",
                "end": "20500000",
            }
            r = requests.get(url, params=params, proxies=proxies)
            data_json = r.json()
            if data_json["data"] is None:
                params = {
                    "secid": f"2.{symbol}",
                    "ut": "7eea3edcaed734bea9cbfc24409ed989",
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                    "klt": period_dict[period],
                    "fqt": "0",
                    "beg": "0",
                    "end": "20500000",
                }
                r = requests.get(url, params=params, proxies=proxies)
                data_json = r.json()
                if data_json["data"] is None:
                    params = {
                        "secid": f"47.{symbol}",
                        "ut": "7eea3edcaed734bea9cbfc24409ed989",
                        "fields1": "f1,f2,f3,f4,f5,f6",
                        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                        "klt": period_dict[period],
                        "fqt": "0",
                        "beg": "0",
                        "end": "20500000",
                    }
    r = requests.get(url, params=params, proxies=proxies)
    data_json = r.json()
    try:
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["klines"]]
        )
    except:  # noqa: E722
        # 兼容 000859(中证国企一路一带) 和 000861(中证央企创新)
        params = {
            "secid": f"2.{symbol}",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": period_dict[period],
            "fqt": "0",
            "beg": "0",
            "end": "20500000",
        }
        r = requests.get(url, params=params, proxies=proxies)
        data_json = r.json()
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["klines"]]
        )
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
    ]
    temp_df.index = pd.to_datetime(temp_df["日期"], errors="coerce")
    temp_df = temp_df[start_date:end_date]
    temp_df.reset_index(inplace=True, drop=True)
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
    temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
    temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    return temp_df


def update(start_date, end_date):
    logger.info(f"更新开始")
    rs = api.bs.query_history_k_data_plus('sh.000001',
                                          'date,code,open,close,high,low,volume,amount,adjustflag,turn,pctChg',
                                          start_date=(datetime.strptime(str(start_date), "%Y%m%d")
                                                      - timedelta(days=20)).strftime("%Y-%m-%d"),
                                          end_date=datetime.strptime(str(end_date), '%Y%m%d').strftime("%Y-%m-%d"),
                                          frequency="d", adjustflag="3")
    if rs.error_code != '0':
        logger.error('读取异常' + rs.error_msg)
        logger.error('baostock登录超时，重新登录后重试。')
        api.bs_login()
        return update(start_date, end_date)
    data = rs.data
    stock_zh_index_daily_list = []
    columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
    pre_close = float(data[0][3])
    for rs_row in data:
        # while rs.next():
        #     # 获取一条记录，将记录合并在一起
        #     rs_row = rs.get_row_data()
        date = int(str(rs_row[0]).replace("-", ""))
        振幅 = (float(rs_row[4]) - float(rs_row[5])) / pre_close * 100
        pre_close = float(rs_row[3])
        stock_zh_index_daily_list.append([date,
                                          float(rs_row[2]), float(rs_row[3]), float(rs_row[4]), float(rs_row[5]),
                                          float(rs_row[6]) / 100, float(rs_row[7]), 振幅, float(rs_row[10]),
                                          float(rs_row[3]) - float(rs_row[2]),
                                          float(rs_row[9])])
    stock_zh_index_daily_df = pd.DataFrame(stock_zh_index_daily_list, columns=columns)
    # while True:
    #     try:
    #         stock_zh_index_daily_df = ak.index_zh_a_hist(symbol="000001", period="daily",
    #                                                      start_date=start_date, end_date=end_date)
    #         break
    #     except Exception as e:
    #         logger.error(e)
    #         logger.error(f"更新失败 开始使用本地代理更新")
    #         try:
    #             stock_zh_index_daily_df = index_zh_a_hist(symbol="000001", period="daily",
    #                                                       start_date=start_date, end_date=end_date)
    #             break
    #         except Exception as e:
    #             logger.error(e)
    #             logger.error(f"使用本地代理更新失败 睡眠后重试")
    #             time.sleep(1)
    # stock_zh_index_daily_df['日期'] = stock_zh_index_daily_df['日期'].str.replace('-', '')

    inserted_count = db.smart_insert_to_mysql(stock_zh_index_daily_df, "akshare_sh000001", db.engine, ['日期'])
    logger.info(f"更新完成。条数:{inserted_count}")
