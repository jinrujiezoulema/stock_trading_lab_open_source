import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import StringIO
from pathlib import Path
import re

import pandas as pd
import py_mini_racer
import requests
from akshare.datasets import get_ths_js
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy import text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CONSTITUENT_COLUMNS = ["板块类型", "板块名称", "板块代码", "页面代码", "股票代码", "股票名称"]
MYSQL_BOARD_TABLE = "t_同花顺板块列表"
MYSQL_CONSTITUENT_TABLE = "t_同花顺板块成分股"
MYSQL_STOCK_RELATION_TABLE = "t_同花顺股票板块概念对应关系"
MYSQL_CREATE_TABLE_SQLS = [
    f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_BOARD_TABLE}` (
      `板块代码` varchar(16) NOT NULL COMMENT '同花顺导入代码，概念一般为88开头',
      `板块类型` varchar(16) NOT NULL COMMENT '概念/行业',
      `板块名称` varchar(64) NOT NULL,
      `页面代码` varchar(16) NOT NULL COMMENT 'q.10jqka.com.cn detail code',
      `详情路径` varchar(16) NOT NULL COMMENT 'gn/thshy',
      `采集日期` int NOT NULL,
      `更新时间` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`板块代码`) USING BTREE,
      UNIQUE KEY `uk_ths_board_type_page` (`板块类型`, `页面代码`) USING BTREE,
      KEY `idx_ths_board_type_name` (`板块类型`, `板块名称`) USING BTREE
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;
    """,
    f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_CONSTITUENT_TABLE}` (
      `板块代码` varchar(16) NOT NULL,
      `股票代码` varchar(16) NOT NULL,
      `板块类型` varchar(16) NOT NULL COMMENT '概念/行业',
      `板块名称` varchar(64) NOT NULL,
      `页面代码` varchar(16) NOT NULL,
      `股票名称` varchar(64) NOT NULL,
      `采集日期` int NOT NULL,
      `更新时间` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`板块代码`, `股票代码`) USING BTREE,
      KEY `idx_ths_constituent_stock` (`股票代码`) USING BTREE,
      KEY `idx_ths_constituent_type_name` (`板块类型`, `板块名称`) USING BTREE
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;
    """,
    f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_STOCK_RELATION_TABLE}` (
      `股票代码` varchar(16) NOT NULL,
      `股票名称` varchar(64) NOT NULL,
      `同花顺行业` text NULL,
      `同花顺行业代码` text NULL,
      `同花顺概念` text NULL,
      `同花顺概念代码` text NULL,
      `采集日期` int NOT NULL,
      `更新时间` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`股票代码`) USING BTREE,
      KEY `idx_ths_stock_relation_name` (`股票名称`) USING BTREE
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;
    """,
]


def _request_get(url, headers, timeout=20, retries=60, refresh_headers=None):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code in (401, 403) and refresh_headers is not None and attempt < retries:
                headers = refresh_headers()
                time.sleep(attempt)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            last_error = e
            if attempt < retries:
                status_code = getattr(getattr(e, "response", None), "status_code", None)
                if status_code in (401, 403) and refresh_headers is not None:
                    headers = refresh_headers()
                time.sleep(attempt)
        except Exception as e:
            logger.error(f"网络请求异常：{e} 睡眠 {attempt} 后重试")
            time.sleep(attempt)
    raise last_error


def _build_headers(referer="https://q.10jqka.com.cn/"):
    js_code = py_mini_racer.MiniRacer()
    with open(get_ths_js("ths.js"), encoding="utf-8") as file:
        js_code.eval(file.read())
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
        "Cookie": f"v={js_code.call('v')}",
        "Referer": referer,
    }


def _refresh_headers(headers, referer):
    headers.clear()
    headers.update(_build_headers(referer))
    return headers


def _extract_board_list(board_type, url, detail_path):
    response = _request_get(url, headers=_build_headers(url), timeout=20)
    soup = BeautifulSoup(response.text, features="lxml")
    cate_inner = soup.find(name="div", attrs={"class": "cate_inner"})
    if cate_inner is None:
        raise RuntimeError(f"未找到同花顺板块列表: {url}")

    rows = []
    for item in cate_inner.find_all("a"):
        href_parts = item["href"].strip("/").split("/")
        page_code = href_parts[-1]
        rows.append({
            "板块类型": board_type,
            "板块名称": item.text.strip(),
            "页面代码": page_code.zfill(6),
            "详情路径": detail_path,
        })
    return rows


def _get_import_code(board_row, headers):
    if board_row["详情路径"] == "thshy":
        return board_row["页面代码"]

    detail_url = f"https://q.10jqka.com.cn/{board_row['详情路径']}/detail/code/{board_row['页面代码']}/"
    response = _request_get(
        detail_url,
        headers=headers,
        timeout=20,
        refresh_headers=lambda: _refresh_headers(headers, detail_url),
    )
    soup = BeautifulSoup(response.text, features="lxml")
    clid = soup.find(name="input", attrs={"id": "clid"})
    if clid is None:
        return board_row["页面代码"]
    return str(clid["value"]).zfill(6)


def _constituent_page_url(board_row, page):
    if page == 1:
        return f"https://q.10jqka.com.cn/{board_row['详情路径']}/detail/code/{board_row['页面代码']}/"
    return (
        f"https://q.10jqka.com.cn/{board_row['详情路径']}/detail/field/199112/order/desc/"
        f"page/{page}/ajax/1/code/{board_row['页面代码']}/"
    )


def _request_constituent_page(board_row, page, headers):
    page_url = _constituent_page_url(board_row, page)
    return _request_get(
        page_url,
        headers=headers,
        timeout=20,
        refresh_headers=lambda: _refresh_headers(headers, page_url),
    )


def _parse_page_count(html):
    soup = BeautifulSoup(html, features="lxml")
    page_info = soup.find(name="span", attrs={"class": "page_info"})
    if page_info is None:
        return 1
    match = re.search(r"/\s*(\d+)", page_info.get_text(strip=True))
    if match is None:
        return 1
    return max(1, int(match.group(1)))


def _parse_constituent_table(html):
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return pd.DataFrame()

    table = None
    for candidate_table in tables:
        candidate_table = candidate_table.copy()
        candidate_table.columns = [str(column) for column in candidate_table.columns]
        if "代码" in candidate_table.columns and "名称" in candidate_table.columns:
            table = candidate_table
            break
    if table is None:
        return pd.DataFrame()
    table = table[["代码", "名称"]].copy()
    table["代码"] = table["代码"].astype(str).str.extract(r"(\d{1,6})", expand=False)
    table = table.dropna(subset=["代码"])
    table["股票代码"] = table["代码"].str.zfill(6)
    table["股票名称"] = table["名称"].astype(str).str.strip()
    table = table[~table["股票名称"].str.contains("暂无成份股数据", na=False)]
    return table[["股票代码", "股票名称"]].drop_duplicates()


def _parse_jsonp(text):
    start = text.index("{")
    end = text.rfind(")")
    if end == -1:
        end = len(text)
    return json.loads(text[start:end])


def _fetch_constituents_by_blockrank(board_row):
    index_code = str(board_row["板块代码"]).zfill(6)
    if not index_code.startswith("88"):
        return []

    headers = _build_headers("https://q.10jqka.com.cn/")
    headers["Host"] = "d.10jqka.com.cn"

    def request_blockrank(rank_code):
        url = f"https://d.10jqka.com.cn/v2/blockrank/{index_code}/8/{rank_code}.js"
        response = _request_get(url, headers=headers, timeout=20)
        return _parse_jsonp(response.text)

    first_json = request_blockrank("d15")
    total_count = int(float(first_json["block"].get("subcodeCount", 0)))
    if total_count <= 0:
        return []

    if total_count < 3000:
        request_codes = [f"d{math.ceil(total_count / 15) * 15}"]
    else:
        request_codes = ["a3000", "d3000"]

    stock_items = []
    for request_code in request_codes:
        stock_items.extend(request_blockrank(request_code).get("items", []))
        time.sleep(0.05)

    rows = []
    seen = set()
    for item in stock_items:
        stock_code = str(item.get("5", "")).strip().zfill(6)
        stock_name = str(item.get("55", "")).strip()
        if not re.fullmatch(r"\d{6}", stock_code) or not stock_name or stock_code in seen:
            continue
        seen.add(stock_code)
        rows.append({
            "板块类型": board_row["板块类型"],
            "板块名称": board_row["板块名称"],
            "板块代码": board_row["板块代码"],
            "页面代码": board_row["页面代码"],
            "股票代码": stock_code,
            "股票名称": stock_name,
        })

    if len(rows) < total_count:
        logger.warning(f"同花顺 blockrank 成分股数量低于声明数量: {index_code} {len(rows)}/{total_count}")
    return rows


def _fetch_constituents_by_pages(board_row, headers, max_pages=300):
    rows = []
    seen = set()
    first_response = _request_constituent_page(board_row, 1, headers)
    page_count = min(_parse_page_count(first_response.text), max_pages)

    for page in range(1, page_count + 1):
        if page == 1:
            page_df = _parse_constituent_table(first_response.text)
        else:
            page_response = _request_constituent_page(board_row, page, headers)
            page_df = _parse_constituent_table(page_response.text)

        if page_df.empty:
            if page == 1:
                break
            raise RuntimeError(f"同花顺分页无数据: page={page}/{page_count}")

        new_count = 0
        for _, stock_row in page_df.iterrows():
            stock_code = stock_row["股票代码"]
            if stock_code in seen:
                continue
            seen.add(stock_code)
            rows.append({
                "板块类型": board_row["板块类型"],
                "板块名称": board_row["板块名称"],
                "板块代码": board_row["板块代码"],
                "页面代码": board_row["页面代码"],
                "股票代码": stock_code,
                "股票名称": stock_row["股票名称"],
            })
            new_count += 1

        if new_count == 0:
            raise RuntimeError(f"同花顺分页未发现新增成分股: page={page}/{page_count}")
        time.sleep(0.05)
    return rows


def _fetch_constituents(board_row, headers, max_pages=300):
    blockrank_rows = _fetch_constituents_by_blockrank(board_row)
    if blockrank_rows:
        return blockrank_rows
    return _fetch_constituents_by_pages(board_row, headers, max_pages=max_pages)


def _load_or_fetch_board_list(force_refresh=False):
    board_path = DATA_DIR / "同花顺板块列表.csv"
    if board_path.exists() and not force_refresh:
        return pd.read_csv(board_path, dtype={"板块代码": str, "页面代码": str}, encoding="utf-8-sig")

    headers = _build_headers()
    board_rows = []
    board_rows.extend(_extract_board_list("概念", "https://q.10jqka.com.cn/gn/", "gn"))
    board_rows.extend(_extract_board_list("行业", "https://q.10jqka.com.cn/thshy/", "thshy"))

    board_df = pd.DataFrame(board_rows).drop_duplicates(subset=["板块类型", "板块名称", "页面代码"])
    import_codes = []
    for index, row in board_df.iterrows():
        import_code = _get_import_code(row, headers)
        import_codes.append(import_code)
        if (len(import_codes) % 50) == 0:
            logger.info(f"同花顺板块导入代码已获取 {len(import_codes)}/{len(board_df)}")
        time.sleep(0.05)
    board_df["板块代码"] = import_codes
    board_df = board_df[["板块类型", "板块名称", "板块代码", "页面代码", "详情路径"]]

    concept_board_df = board_df[board_df["板块类型"].eq("概念")]
    industry_board_df = board_df[board_df["板块类型"].eq("行业")]
    board_df.to_csv(DATA_DIR / "同花顺板块列表.csv", index=False, encoding="utf-8-sig")
    concept_board_df.to_csv(DATA_DIR / "同花顺概念列表.csv", index=False, encoding="utf-8-sig")
    industry_board_df.to_csv(DATA_DIR / "同花顺行业列表.csv", index=False, encoding="utf-8-sig")
    return board_df


def _fetch_constituents_worker(index, total, row):
    headers = _build_headers()
    try:
        rows = _fetch_constituents(row, headers)
        logger.info(f"同花顺{row['板块类型']}成分股 {index}/{total} {row['板块代码']} {row['板块名称']} 数量:{len(rows)}")
        return rows
    except Exception as e:
        logger.error(f"同花顺{row['板块类型']}成分股采集失败 {index}/{total} {row['板块代码']} {row['板块名称']}: {e}")
        return []


def _build_constituent_df(rows):
    return pd.DataFrame(rows, columns=CONSTITUENT_COLUMNS).drop_duplicates()


def _build_stock_relation_df(constituent_df):
    columns = ["股票代码", "股票名称", "同花顺行业", "同花顺行业代码", "同花顺概念", "同花顺概念代码"]
    if constituent_df.empty:
        return pd.DataFrame(columns=columns)

    def select_stock_name(name_series):
        names = name_series.dropna().astype(str).str.strip()
        names = names[names.ne("")]
        if names.empty:
            return ""
        counts = names.value_counts()
        candidates = counts[counts.eq(counts.max())].index.tolist()
        candidates.sort(key=lambda name: (name.upper().startswith(("C", "N")), -len(name), name))
        return candidates[0]

    def join_board_pairs(board_group_df):
        pair_df = board_group_df[["板块代码", "板块名称"]].copy()
        pair_df["板块代码"] = pair_df["板块代码"].astype(str).str.zfill(6)
        pair_df["板块名称"] = pair_df["板块名称"].astype(str).str.strip()
        pair_df = pair_df.drop_duplicates(subset=["板块代码"]).sort_values(["板块代码", "板块名称"])
        return ";".join(pair_df["板块名称"]), ";".join(pair_df["板块代码"])

    stock_relation_rows = []
    for stock_code, group_df in constituent_df.groupby("股票代码"):
        industry_group_df = group_df[group_df["板块类型"].eq("行业")]
        concept_group_df = group_df[group_df["板块类型"].eq("概念")]
        industry_names, industry_codes = join_board_pairs(industry_group_df)
        concept_names, concept_codes = join_board_pairs(concept_group_df)
        stock_relation_rows.append({
            "股票代码": stock_code,
            "股票名称": select_stock_name(group_df["股票名称"]),
            "同花顺行业": industry_names,
            "同花顺行业代码": industry_codes,
            "同花顺概念": concept_names,
            "同花顺概念代码": concept_codes,
        })
    return pd.DataFrame(stock_relation_rows, columns=columns)


def _get_missing_board_df(board_df, constituent_df):
    if board_df.empty:
        return pd.DataFrame(columns=board_df.columns)
    if constituent_df.empty:
        return board_df.copy()

    existing_codes = set(constituent_df["板块代码"].dropna().astype(str))
    return board_df[~board_df["板块代码"].astype(str).isin(existing_codes)].copy()


def _append_mysql_meta_columns(df, collect_date):
    result_df = df.copy()
    result_df["采集日期"] = int(collect_date)
    result_df["更新时间"] = datetime.now()
    return result_df


def _prepare_board_mysql_df(board_df, collect_date):
    mysql_df = board_df[["板块代码", "板块类型", "板块名称", "页面代码", "详情路径"]].copy()
    mysql_df["板块代码"] = mysql_df["板块代码"].astype(str).str.zfill(6)
    mysql_df["页面代码"] = mysql_df["页面代码"].astype(str).str.zfill(6)
    return _append_mysql_meta_columns(mysql_df, collect_date)


def _prepare_constituent_mysql_df(constituent_df, collect_date):
    mysql_df = constituent_df[["板块代码", "股票代码", "板块类型", "板块名称", "页面代码", "股票名称"]].copy()
    mysql_df["板块代码"] = mysql_df["板块代码"].astype(str).str.zfill(6)
    mysql_df["页面代码"] = mysql_df["页面代码"].astype(str).str.zfill(6)
    mysql_df["股票代码"] = mysql_df["股票代码"].astype(str).str.zfill(6)
    return _append_mysql_meta_columns(mysql_df, collect_date)


def _prepare_stock_relation_mysql_df(constituent_df, collect_date):
    mysql_df = _build_stock_relation_df(constituent_df)
    mysql_df["股票代码"] = mysql_df["股票代码"].astype(str).str.zfill(6)
    return _append_mysql_meta_columns(mysql_df, collect_date)


def 创建同花顺板块成分股mysql表():
    from utils import db

    with db.engine.begin() as connection:
        for sql in MYSQL_CREATE_TABLE_SQLS:
            connection.execute(text(sql))
    logger.info("同花顺板块/概念 MySQL 表结构检查完成")


def _replace_mysql_table(connection, table_name, df):
    connection.execute(text(f"DELETE FROM `{table_name}`"))
    if not df.empty:
        df.to_sql(
            table_name,
            con=connection,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=2000,
        )


def 写入同花顺板块成分股到mysql(board_df, constituent_df, collect_date=None):
    from utils import db

    collect_date = collect_date or datetime.now().strftime("%Y%m%d")
    missing_df = _get_missing_board_df(board_df, constituent_df)
    if not missing_df.empty:
        missing_names = "、".join(missing_df["板块名称"].astype(str).head(10).tolist())
        raise RuntimeError(f"同花顺成分股存在缺失板块，停止写入 MySQL。缺失:{len(missing_df)} {missing_names}")

    创建同花顺板块成分股mysql表()
    board_mysql_df = _prepare_board_mysql_df(board_df, collect_date)
    constituent_mysql_df = _prepare_constituent_mysql_df(constituent_df, collect_date)
    stock_relation_mysql_df = _prepare_stock_relation_mysql_df(constituent_df, collect_date)

    with db.engine.begin() as connection:
        _replace_mysql_table(connection, MYSQL_BOARD_TABLE, board_mysql_df)
        _replace_mysql_table(connection, MYSQL_CONSTITUENT_TABLE, constituent_mysql_df)
        _replace_mysql_table(connection, MYSQL_STOCK_RELATION_TABLE, stock_relation_mysql_df)

    logger.info(
        f"同花顺板块/概念数据写入 MySQL 完成。板块:{len(board_mysql_df)} "
        f"成分关系:{len(constituent_mysql_df)} 股票关系:{len(stock_relation_mysql_df)}"
    )
    return {
        "板块": len(board_mysql_df),
        "成分关系": len(constituent_mysql_df),
        "股票关系": len(stock_relation_mysql_df),
    }


def _save_constituent_files(constituent_df):
    concept_constituent_df = constituent_df[constituent_df["板块类型"].eq("概念")]
    industry_constituent_df = constituent_df[constituent_df["板块类型"].eq("行业")]
    stock_relation_df = _build_stock_relation_df(constituent_df)
    constituent_df.to_csv(DATA_DIR / "同花顺板块成分股.csv", index=False, encoding="utf-8-sig")
    concept_constituent_df.to_csv(DATA_DIR / "同花顺概念成分股.csv", index=False, encoding="utf-8-sig")
    industry_constituent_df.to_csv(DATA_DIR / "同花顺行业成分股.csv", index=False, encoding="utf-8-sig")
    stock_relation_df.to_csv(DATA_DIR / "同花顺股票板块概念对应关系.csv", index=False, encoding="utf-8-sig")
    return concept_constituent_df, industry_constituent_df


def 采集同花顺板块成分股(force_refresh=False, max_workers=8):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    board_df = _load_or_fetch_board_list(force_refresh=force_refresh)

    constituent_rows = []
    temp_path = DATA_DIR / "同花顺板块成分股_临时.csv"
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_constituents_worker, index + 1, len(board_df), row): index
            for index, row in board_df.iterrows()
        }
        for finished_count, future in enumerate(as_completed(futures), start=1):
            constituent_rows.extend(future.result())
            if finished_count % 25 == 0:
                _build_constituent_df(constituent_rows).to_csv(temp_path, index=False, encoding="utf-8-sig")
                logger.info(f"同花顺板块成分股临时保存 {finished_count}/{len(board_df)} 关系数:{len(constituent_rows)}")

    constituent_df = _build_constituent_df(constituent_rows)
    concept_constituent_df, industry_constituent_df = _save_constituent_files(constituent_df)

    logger.info(
        f"同花顺板块成分股采集完成。板块:{len(board_df)} 成分关系:{len(constituent_df)} "
        f"概念关系:{len(concept_constituent_df)} 行业关系:{len(industry_constituent_df)}"
    )
    return board_df, constituent_df


def 补采同花顺缺失板块成分股(max_workers=3):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    board_df = _load_or_fetch_board_list(force_refresh=False)
    constituent_path = DATA_DIR / "同花顺板块成分股.csv"

    if constituent_path.exists():
        constituent_df = pd.read_csv(
            constituent_path,
            dtype={"板块代码": str, "页面代码": str, "股票代码": str},
            encoding="utf-8-sig",
        )
    else:
        constituent_df = _build_constituent_df([])

    existing_codes = set(constituent_df["板块代码"].dropna()) if not constituent_df.empty else set()
    missing_df = board_df[~board_df["板块代码"].isin(existing_codes)].copy()
    if missing_df.empty:
        logger.info("同花顺板块成分股无需补采，所有板块均已有成分股关系")
        return board_df, constituent_df

    logger.info(f"同花顺板块成分股开始补采，缺失板块:{len(missing_df)}")
    supplement_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_constituents_worker, index + 1, len(missing_df), row): index
            for index, row in missing_df.reset_index(drop=True).iterrows()
        }
        for future in as_completed(futures):
            supplement_rows.extend(future.result())

    supplement_df = _build_constituent_df(supplement_rows)
    constituent_df = pd.concat([constituent_df, supplement_df], ignore_index=True).drop_duplicates()
    concept_constituent_df, industry_constituent_df = _save_constituent_files(constituent_df)
    logger.info(
        f"同花顺板块成分股补采完成。新增关系:{len(supplement_df)} 总关系:{len(constituent_df)} "
        f"概念关系:{len(concept_constituent_df)} 行业关系:{len(industry_constituent_df)}"
    )
    return board_df, constituent_df


def 每日更新同花顺板块成分股(max_workers=8):
    board_df, constituent_df = 采集同花顺板块成分股(force_refresh=True, max_workers=max_workers)
    missing_df = _get_missing_board_df(board_df, constituent_df)
    if not missing_df.empty:
        logger.warning(f"同花顺板块成分股存在缺失，开始补采。缺失板块:{len(missing_df)}")
        board_df, constituent_df = 补采同花顺缺失板块成分股(max_workers=3)

    mysql_counts = 写入同花顺板块成分股到mysql(board_df, constituent_df)
    return board_df, constituent_df, mysql_counts


if __name__ == "__main__":
    每日更新同花顺板块成分股()
