from DrissionPage import WebPage, ChromiumOptions
import re
import time
from loguru import logger
import pandas as pd
from datetime import datetime, timedelta
from functools import lru_cache
from io import StringIO
from pathlib import Path
from utils import common, db, ini_util, model_util
from utils import driver_chrome

driver_web = None

手动指定_map = {
    '公告': {
        'code': 885739,
        'name': '股权转让(并购重组)',
    },
    '其他': {
        'code': 881123,
        'name': '其他',
    },
    '被动元件': {
        'code': 881270,
        'name': '元件',
    },
    '光通信': {
        'code': 886084,
        'name': '光纤概念',
    },
    '机器人': {
        'code': 886069,
        'name': '人形机器人',
    },
    'AI硬件': {
        'code': 885887,
        'name': '数据中心',
    },
    'AI应用': {
        'code': 886108,
        'name': 'AI应用',
    },
    '消费': {
        'code': 883434,
        'name': '消费',
    },
    '大消费': {
        'code': 883434,
        'name': '消费',
    },
    '玻璃基板': {
        'code': 884094,
        'name': '面板',
    },
    '地产基建': {
        'code': 881153,
        'name': '房地产',
    },
    'AI大模型': {
        'code': 886108,
        'name': 'AI应用',
    },
    '金属有色': {
        'code': 881170,
        'name': '小金属',
    },
    '氧化锆': {
        'code': 881170,
        'name': '小金属',
    },
    '玻璃基板封装': {
        'code': 884094,
        'name': '面板',
    },
    '电池产业链': {
        'code': 886032,
        'name': '固态电池',
    },
    '锂电池产业链': {
        'code': 885710,
        'name': '锂电池概念',
    },
    '医疗医药': {
        'code': 886015,
        'name': '创新药',
    },
    '数据中心散热': {
        'code': 886044,
        'name': '液冷服务器',
    },
    '有色金属': {
        'code': 881170,
        'name': '小金属',
    },
    '大金融': {
        'code': 885456,
        'name': '互联网金融',
    },
}


def _同花顺板块列表缓存路径():
    return Path(__file__).resolve().parents[1] / 'data' / '同花顺板块列表.csv'


def _同花顺数据目录():
    data_dir = Path(__file__).resolve().parents[1] / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def 刷新同花顺板块列表缓存():
    import akshare as ak

    cache_path = _同花顺板块列表缓存路径()
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    industry_df = ak.stock_board_industry_name_ths().rename(columns={'name': '板块名称', 'code': '板块代码'})
    industry_df['板块类型'] = '行业'
    industry_df['页面代码'] = industry_df['板块代码']
    concept_df = ak.stock_board_concept_name_ths().rename(columns={'name': '板块名称', 'code': '板块代码'})
    concept_df['板块类型'] = '概念'
    concept_df['页面代码'] = concept_df['板块代码']
    ths_board_df = pd.concat([industry_df, concept_df], ignore_index=True)
    ths_board_df['板块代码'] = ths_board_df['板块代码'].astype(str).str.zfill(6)
    ths_board_df['页面代码'] = ths_board_df['页面代码'].astype(str).str.zfill(6)
    ths_board_df = ths_board_df[['板块类型', '板块名称', '板块代码', '页面代码']].drop_duplicates()
    ths_board_df.to_csv(cache_path, index=False, encoding='utf-8-sig')
    logger.info(f"同花顺板块列表缓存刷新完成。路径:{cache_path} 数量:{len(ths_board_df)}")
    return ths_board_df


@lru_cache(maxsize=1)
def _同花顺请求头():
    import py_mini_racer
    from akshare.datasets import get_ths_js

    js_code = py_mini_racer.MiniRacer()
    with open(get_ths_js('ths.js'), encoding='utf-8') as file:
        js_code.eval(file.read())
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
        'Cookie': f"v={js_code.call('v')}",
    }


def _加载同花顺板块列表():
    cache_path = _同花顺板块列表缓存路径()
    if cache_path.exists():
        ths_board_df = pd.read_csv(cache_path, dtype={'板块代码': str, '页面代码': str}, encoding='utf-8-sig')
        if '页面代码' not in ths_board_df.columns:
            ths_board_df['页面代码'] = ths_board_df['板块代码']
        return ths_board_df

    try:
        return 刷新同花顺板块列表缓存()
    except Exception as e:
        logger.error(f"同花顺板块列表缓存加载失败: {e}")
        return pd.DataFrame(columns=['板块类型', '板块名称', '板块代码', '页面代码'])


@lru_cache(maxsize=512)
def _获取同花顺概念导入代码(page_code):
    import requests
    from bs4 import BeautifulSoup

    response = requests.get(f'https://q.10jqka.com.cn/gn/detail/code/{page_code}/', headers=_同花顺请求头(), timeout=20)
    soup = BeautifulSoup(response.text, features='lxml')
    clid = soup.find(name='input', attrs={'id': 'clid'})
    if clid is None:
        return None
    return str(clid['value']).zfill(6)


def _同花顺板块导入代码(board_row):
    board_code = str(board_row['板块代码']).zfill(6)
    if board_row['板块类型'] != '概念' or board_code.startswith('88'):
        return board_code

    page_code = str(board_row.get('页面代码', board_code)).zfill(6)
    try:
        import_code = _获取同花顺概念导入代码(page_code)
        if import_code:
            return import_code
    except Exception as e:
        logger.error(f"同花顺概念导入代码获取失败: {board_row['板块名称']} {page_code} {e}")
    return board_code


def _规范化板块名称(name):
    board_name = str(name).strip().replace('_', '').replace(' ', '')
    for suffix in ['概念', '行业', '板块']:
        if board_name.endswith(suffix):
            board_name = board_name[:-len(suffix)]
    return board_name


def _匹配同花顺板块(板块, ths_board_df):
    if ths_board_df.empty:
        return None

    board_name = str(板块).strip()
    normalized_name = _规范化板块名称(board_name)
    ths_board_df = ths_board_df.copy()
    ths_board_df['规范名称'] = ths_board_df['板块名称'].map(_规范化板块名称)
    type_rank = {'行业': 0, '概念': 1}

    for candidates in [
        ths_board_df[ths_board_df['板块名称'].eq(board_name)],
        ths_board_df[ths_board_df['规范名称'].eq(normalized_name)],
        ths_board_df[ths_board_df['板块名称'].str.contains(board_name, regex=False, na=False)],
        ths_board_df[ths_board_df['规范名称'].str.contains(normalized_name, regex=False, na=False)],
    ]:
        if not candidates.empty:
            candidates = candidates.assign(类型排序=candidates['板块类型'].map(type_rank).fillna(9))
            return candidates.sort_values(by=['类型排序', '板块名称'], kind='mergesort').iloc[0]

    return None


def _解析同花顺成分股表(html):
    try:
        table_list = pd.read_html(StringIO(html))
    except ValueError:
        return pd.DataFrame(columns=['股票代码', '股票名称'])

    if not table_list:
        return pd.DataFrame(columns=['股票代码', '股票名称'])

    stock_df = table_list[0]
    if '代码' not in stock_df.columns or '名称' not in stock_df.columns:
        return pd.DataFrame(columns=['股票代码', '股票名称'])

    result_df = stock_df[['代码', '名称']].rename(columns={'代码': '股票代码', '名称': '股票名称'})
    result_df['股票代码'] = result_df['股票代码'].map(lambda code: str(code).split('.')[0].zfill(6))
    result_df['股票名称'] = result_df['股票名称'].astype(str).str.strip()
    result_df = result_df[result_df['股票代码'].str.fullmatch(r'\d{6}', na=False)]
    return result_df.drop_duplicates(subset=['股票代码'])


def _解析同花顺页数(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, features='lxml')
    page_info = soup.find(name='span', attrs={'class': 'page_info'})
    if page_info is None or '/' not in page_info.text:
        return 1
    return int(page_info.text.split('/')[-1])


def _抓取同花顺单个板块成分股(board_type, board_name, page_code):
    import requests
    from bs4 import BeautifulSoup

    path = 'gn' if board_type == '概念' else 'thshy'
    page_code = str(page_code).zfill(6)
    headers = _同花顺请求头()
    detail_url = f'https://q.10jqka.com.cn/{path}/detail/code/{page_code}/'
    response = requests.get(detail_url, headers=headers, timeout=20)
    response.raise_for_status()
    html = response.text

    import_code = page_code
    if board_type == '概念':
        soup = BeautifulSoup(html, features='lxml')
        clid = soup.find(name='input', attrs={'id': 'clid'})
        if clid is not None:
            import_code = str(clid['value']).zfill(6)

    page_count = _解析同花顺页数(html)
    stock_df_list = [_解析同花顺成分股表(html)]
    for page in range(2, page_count + 1):
        page_url = f'https://q.10jqka.com.cn/{path}/detail/field/199112/order/desc/page/{page}/ajax/1/code/{page_code}/'
        page_response = requests.get(page_url, headers=headers, timeout=20)
        if page_response.status_code != 200:
            logger.warning(
                f"同花顺成分股分页请求失败: {board_type} {board_name} page={page} status={page_response.status_code}")
            continue
        stock_df_list.append(_解析同花顺成分股表(page_response.text))

    stock_df = pd.concat(stock_df_list, ignore_index=True).drop_duplicates(subset=['股票代码'])
    stock_df.insert(0, '板块类型', board_type)
    stock_df.insert(1, '板块名称', board_name)
    stock_df.insert(2, '板块代码', import_code)
    stock_df.insert(3, '页面代码', page_code)
    return import_code, stock_df


def 刷新同花顺板块概念成分股缓存():
    import akshare as ak

    data_dir = _同花顺数据目录()
    industry_list_df = ak.stock_board_industry_name_ths().rename(columns={'name': '板块名称', 'code': '页面代码'})
    industry_list_df['板块类型'] = '行业'
    industry_list_df['页面代码'] = industry_list_df['页面代码'].astype(str).str.zfill(6)
    industry_list_df['板块代码'] = industry_list_df['页面代码']

    concept_list_df = ak.stock_board_concept_name_ths().rename(columns={'name': '板块名称', 'code': '页面代码'})
    concept_list_df['板块类型'] = '概念'
    concept_list_df['页面代码'] = concept_list_df['页面代码'].astype(str).str.zfill(6)
    concept_list_df['板块代码'] = concept_list_df['页面代码']

    board_list_df = pd.concat([industry_list_df, concept_list_df], ignore_index=True)
    board_list_df = board_list_df[['板块类型', '板块名称', '板块代码', '页面代码']].drop_duplicates()

    constituent_df_list = []
    updated_board_rows = []
    total_count = len(board_list_df)
    for index, (_, board_row) in enumerate(board_list_df.iterrows(), start=1):
        try:
            import_code, stock_df = _抓取同花顺单个板块成分股(
                board_row['板块类型'],
                board_row['板块名称'],
                board_row['页面代码'],
            )
            updated_board_rows.append({
                '板块类型': board_row['板块类型'],
                '板块名称': board_row['板块名称'],
                '板块代码': import_code,
                '页面代码': board_row['页面代码'],
            })
            constituent_df_list.append(stock_df)
            logger.info(
                f"同花顺成分股采集进度 {index}/{total_count} {board_row['板块类型']} {board_row['板块名称']} {import_code} 股票数:{len(stock_df)}")
        except Exception as e:
            updated_board_rows.append({
                '板块类型': board_row['板块类型'],
                '板块名称': board_row['板块名称'],
                '板块代码': board_row['板块代码'],
                '页面代码': board_row['页面代码'],
            })
            logger.error(
                f"同花顺成分股采集失败 {index}/{total_count} {board_row['板块类型']} {board_row['板块名称']}: {e}")

    board_list_df = pd.DataFrame(updated_board_rows)
    constituent_df = pd.concat(constituent_df_list, ignore_index=True) if constituent_df_list else pd.DataFrame(
        columns=['板块类型', '板块名称', '板块代码', '页面代码', '股票代码', '股票名称']
    )

    industry_board_df = board_list_df[board_list_df['板块类型'].eq('行业')]
    concept_board_df = board_list_df[board_list_df['板块类型'].eq('概念')]
    industry_cons_df = constituent_df[constituent_df['板块类型'].eq('行业')]
    concept_cons_df = constituent_df[constituent_df['板块类型'].eq('概念')]

    industry_board_df.to_csv(data_dir / '同花顺行业板块列表.csv', index=False, encoding='utf-8-sig')
    concept_board_df.to_csv(data_dir / '同花顺概念板块列表.csv', index=False, encoding='utf-8-sig')
    board_list_df.to_csv(data_dir / '同花顺板块列表.csv', index=False, encoding='utf-8-sig')
    industry_cons_df.to_csv(data_dir / '同花顺行业板块成分股.csv', index=False, encoding='utf-8-sig')
    concept_cons_df.to_csv(data_dir / '同花顺概念板块成分股.csv', index=False, encoding='utf-8-sig')
    constituent_df.to_csv(data_dir / '同花顺板块概念成分股.csv', index=False, encoding='utf-8-sig')

    stock_relation_rows = []
    for (stock_code, stock_name), group_df in constituent_df.groupby(['股票代码', '股票名称']):
        industry_group_df = group_df[group_df['板块类型'].eq('行业')]
        concept_group_df = group_df[group_df['板块类型'].eq('概念')]
        stock_relation_rows.append({
            '股票代码': stock_code,
            '股票名称': stock_name,
            '同花顺行业': ';'.join(sorted(set(industry_group_df['板块名称']))),
            '同花顺行业代码': ';'.join(sorted(set(industry_group_df['板块代码'].astype(str)))),
            '同花顺概念': ';'.join(sorted(set(concept_group_df['板块名称']))),
            '同花顺概念代码': ';'.join(sorted(set(concept_group_df['板块代码'].astype(str)))),
        })
    stock_relation_df = pd.DataFrame(stock_relation_rows)
    stock_relation_df.to_csv(data_dir / '同花顺股票板块概念对应关系.csv', index=False, encoding='utf-8-sig')
    logger.info(
        f"同花顺板块概念成分股缓存刷新完成。板块:{len(board_list_df)} 成分关系:{len(constituent_df)} 股票:{len(stock_relation_df)}")
    return board_list_df, constituent_df, stock_relation_df


def _sanitize_ini_filename(name):
    invalid_chars = '<>:"/\\|?*'
    filename = ''.join('_' if char in invalid_chars else char for char in str(name)).strip()
    filename = filename.rstrip('. ')
    return filename or '未命名板块'


def _解析几天几板(value):
    if pd.isna(value):
        return None
    match = re.search(r'(\d+)\s*天\s*(\d+)\s*板', str(value).strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _按几天几板和涨停时间排序(stock_df):
    if stock_df.empty:
        return stock_df

    sorted_df = stock_df.copy()
    sort_rows = []
    for value in sorted_df.get('几天几板', pd.Series(index=sorted_df.index, dtype=object)):
        parsed = _解析几天几板(value)
        if parsed is None:
            sort_rows.append((2, 0, 0))
            continue
        day_count, board_count = parsed
        if day_count == board_count:
            sort_rows.append((0, -day_count, 0))
        else:
            sort_rows.append((1, 0, -board_count))

    sort_key_df = pd.DataFrame(sort_rows, index=sorted_df.index, columns=['_身位排序', '_连板天数排序', '_非连板板数排序'])
    sorted_df = sorted_df.join(sort_key_df)
    return (
        sorted_df
        .sort_values(
            by=['_身位排序', '_连板天数排序', '_非连板板数排序', '涨停时间'],
            na_position='last',
            kind='mergesort',
        )
        .drop(columns=['_身位排序', '_连板天数排序', '_非连板板数排序'])
    )


def _按身位排序并去重(stock_df):
    return _按几天几板和涨停时间排序(stock_df).drop_duplicates(subset=['股票代码'])


def _生成板块股票ini(stock_df, date_str, 板块题材解析_dict):
    output_dir = Path(__file__).resolve().parents[1] / 'output' / '韭研公社异动板块' / str(date_str)
    output_dir.mkdir(parents=True, exist_ok=True)

    if stock_df.empty:
        logger.warning(f"韭研公社异动 {date_str} 未采集到数据，跳过生成板块ini文件")
        return

    generated_count = 0
    for 板块, group_df in stock_df.groupby('板块', sort=False):
        sorted_group_df = _按身位排序并去重(group_df)
        ini_path = output_dir / f"{len(sorted_group_df)}_{_sanitize_ini_filename(板块)}.ini"
        logger.warning(f"{len(sorted_group_df)}_{_sanitize_ini_filename(板块)}")
        ini_items = []
        for _, row in sorted_group_df.iterrows():
            stock_code = str(row['股票代码']).zfill(6)
            ini_items.append((stock_code, row['股票名称']))
        ini_util.写入列表ini(ini_items, output_dir, ini_path.name)
        generated_count += 1

    bottom_boards = {'公告', '其他', '新股'}
    ths_board_df = _加载同花顺板块列表()
    board_order_df = (
        stock_df[['板块', '板块个股数量']]
        .drop_duplicates(subset=['板块'])
        .assign(合并排序=lambda df: df['板块'].map(
            lambda name: 2 if name == 'ST板块' else 1 if name in bottom_boards else 0))
        .sort_values(by=['合并排序', '板块个股数量', '板块'], ascending=[True, False, True], kind='mergesort')
    )
    merged_items = []
    used_stock_codes = set()
    merged_stock_count = 0
    has_previous_board = False
    未匹配设置概念_set = {882001, 882002, 882003, 882004, 882005, 882006, 882007, 882008, 882009}
    for _, board_row in board_order_df.iterrows():
        if board_row['板块'] in ['ST板块']:
            continue
        board_stock_df = _按身位排序并去重(stock_df[stock_df['板块'].eq(board_row['板块'])])
        board_items = []
        for _, row in board_stock_df.iterrows():
            stock_code = str(row['股票代码']).zfill(6)
            if stock_code in used_stock_codes:
                continue
            used_stock_codes.add(stock_code)
            board_items.append((stock_code, row['股票名称']))
        if not board_items:
            continue
        板块题材解析 = 板块题材解析_dict[board_row['板块']]
        logger.warning(f"板块题材解析：{板块题材解析}。股票清单：{board_items}")
        if board_row['板块'] in 手动指定_map:
            merged_items.append((手动指定_map[board_row['板块']]['code'], 手动指定_map[board_row['板块']]['name']))
        else:
            matched_ths_board = _匹配同花顺板块(board_row['板块'], ths_board_df)
            if matched_ths_board is not None:
                import_code = _同花顺板块导入代码(matched_ths_board)
                logger.error(f"匹配到同花顺板块: {matched_ths_board['板块名称']}")
                merged_items.append((import_code, matched_ths_board['板块名称']))
            else:
                logger.warning(f"未匹配到同花顺板块: {board_row['板块']}，使用 未匹配设置概念_set。")
                pop = 未匹配设置概念_set.pop()
                merged_items.append((pop, pop))
                logger.error(f"未匹配到同花顺板块: {board_row['板块']},设置：{pop}")
            # ds_response = model_util.process_deepseek_query(query=f"""
            #     “板块题材解析：{板块题材解析}。”
            # """,prompt=f"""
            #     根据用户输入的题材解析判断，用哪个同花顺的板块或者同花顺概念去看走势比较合适？
            #     必须使用https://q.10jqka.com.cn/gn/和https://q.10jqka.com.cn/thshy/中的板块或者概念。
            #     请你返回1~3个最相关的板块或者概念的同花顺代码，按最相关排序。
            #     只返回对应的代码和名称。
            #     如用户输入：贵州茅台销量暴增
            #     返回：“881273,白酒;885525,白酒概念;”
            #     请严格按照格式返回。
            # """,temperature=1)
            # logger.warning(f"deepseek推荐板块：{ds_response}")
            # for ds_response_row in ds_response.split(";"):
            #     if ds_response_row == "":
            #         continue
            #     try:
            #         ds_response_row_arr = ds_response_row.split(",")
            #
            #         merged_items.append((ds_response_row_arr[0], ds_response_row_arr[1]))
            #     except Exception as e:
            #         logger.error(f"{ds_response_row} 解析异常：{e}")
            # logger.warning(f"未匹配到同花顺板块: {board_row['板块']}，使用上证指数分隔")
        for stock_code, stock_name in board_items:
            merged_items.append((stock_code, stock_name))
            merged_stock_count += 1
        has_previous_board = True

    merged_ini_path = output_dir / f"{merged_stock_count}_全部.ini"
    ini_util.写入列表ini(merged_items, output_dir, merged_ini_path.name)

    logger.info(
        f"韭研公社异动 {date_str} 板块ini文件生成完成。目录:{output_dir} 数量:{generated_count} 合并数量:{merged_stock_count}")


def init_driver():
    global driver_web
    driver_web = driver_chrome.initDriver()


@common.timer_statistics
def 韭研公社异动采集(date_str):
    if driver_web is None:
        init_driver()
    date_url = datetime.strptime(str(date_str), "%Y%m%d").strftime("%Y-%m-%d")
    driver_web.listen.start(['/jystock-app/api/v1/action/field', ])
    driver_web.get(f"https://www.jiuyangongshe.com/action/{date_url}")
    # time.sleep(2)  # 给页面一点加载时间

    # 使用 CSS 选择器精确点击
    while True:
        try:
            driver_web.ele('css=.yd-tabs_item.is-top', timeout=10).click()
            break
        except Exception as e:
            logger.error(f"{e} 未找到该标签，请检查页面结构或 class 名是否变化")
            time.sleep(1)
            continue
    data_list = []
    data_response = None
    for packet in driver_web.listen.steps():
        # logger.info(packet.url)  # 打印数据包url
        if type(packet.response.body) == dict:
            # logger.info(packet.response.body)
            data_response = packet.response.body
            if data_response['msg'] != '':
                logger.error(data_response['msg'])
                continue
            break
    板块题材解析_dict = {}
    for row in data_response['data'][1:]:
        板块 = row['name']
        板块个股数量 = row['count']
        板块题材解析_dict[板块] = row['reason']
        date = date_str
        for rs_row in row['list']:
            action_time = rs_row['article']['action_info']['time']
            涨停时间 = f'{date_url} {action_time}'
            if action_time is None or ':' not in action_time:
                涨停时间 = None
            if rs_row['article']['action_info']['shares_range'] / 100 < 9.5 or rs_row['article']['action_info'][
                'shares_range'] / 100 > 10.2:
                continue
            data_list.append(
                [f"{date}_{板块}_{int(rs_row['code'][2:])}", date, 板块, 板块个股数量, int(rs_row['code'][2:]),
                 rs_row['name'], rs_row['code'], 涨停时间, rs_row['article']['action_info']['num'],
                 rs_row['article']['action_info']['shares_range'] / 100, rs_row['article']['action_info']['expound']])
    stock_zh_index_daily_df = pd.DataFrame(data_list,
                                           columns=['data_id', 'date', '板块', '板块个股数量', '股票代码', '股票名称',
                                                    'code', '涨停时间', '几天几板', '涨幅', '涨停解析'])
    inserted_count = db.smart_insert_to_mysql(stock_zh_index_daily_df, "t_韭研公社异动解析", db.engine, ['data_id'])
    logger.info(f"_5_韭研公社异动 {date_str} 更新完成。条数:{inserted_count}")
    _生成板块股票ini(stock_zh_index_daily_df, date_str, 板块题材解析_dict)
    # 日内前排()
    pass


def 日内前排():
    max_date = db.mysql_localhost(sql="""
        select max(date) as date from `stock_trading_lab`.`t_韭研公社异动解析` 
    """, fetch=True)
    max_date = max_date[0]['date']
    # max_date = 20260507
    max_date_time = datetime.strptime(str(max_date), "%Y%m%d").strftime("%Y-%m-%d")
    pass
    start_date = (datetime.strptime(str(max_date), "%Y%m%d") - timedelta(days=180)).strftime(
        '%Y%m%d')
    # 活跃板块_list = db.mysql_localhost(sql=f"""
    #     SELECT distinct 板块 FROM `stock_trading_lab`.`t_韭研公社异动解析`
    #     WHERE DATE > {start_date}
    #     AND DATE < {max_date}
    #     AND 板块个股数量 > 5
    # """, fetch=True)
    # 活跃板块 = [row['板块'] for row in 活跃板块_list]
    data_list = db.mysql_localhost(sql=f"""
        SELECT 板块,板块个股数量,code,股票名称,涨停时间,几天几板,涨停解析 FROM `stock_trading_lab`.`t_韭研公社异动解析` 
        WHERE DATE = {max_date}
        AND 板块 != 'ST板块'
         ORDER BY `板块个股数量` DESC,板块,涨停时间
    """, fetch=True)
    sort_name = None
    sort_name_first = None
    sort_name_index = 1
    name_list = []
    概念统计 = {}
    概念身位统计 = {}
    概念身位统计_时间 = {}
    for row in data_list:
        if row['涨停时间'] is None:
            continue
        sort_name = row['板块']
        涨停解析_arr = row['涨停解析'].split("\n")[0].split("+")
        涨停解析_arr.append(sort_name)
        for 概念 in set(涨停解析_arr):
            概念 = 概念.split("(")[0].split("（")[0]
            # if 概念 not in 活跃板块:
            #     continue
            if 概念 in ['其他']:
                continue
            if 概念 not in 概念统计:
                概念统计[概念] = {}
                概念身位统计[概念] = {}
                概念身位统计_时间[概念] = {}
            概念统计[概念][row['股票名称']] = row['涨停时间']
            if row['几天几板']:
                概念身位统计_时间[概念][row['股票名称']] = row['涨停时间']
                概念身位统计[概念][row['股票名称']] = row['几天几板']
        # if sort_name != sort_name_first:
        #     sort_name_first = sort_name
        #     sort_name_index = 1
        # if row['几天几板'] is None:
        #     row['几天几板'] = f"龙{sort_name_index}"
        #
        # logger.info(
        #     f"{row['板块']} 家数：{row['板块个股数量']} {row['股票名称']} {row['code']} 涨停时间：{row['涨停时间']} 几天几板：{row['几天几板']}")
        # name_list.append(row['股票名称'])
        # sort_name_index += 1
    # logger.info(len(name_list))
    # arr_print = []
    # for i in range(len(name_list)):
    #     arr_print.append(name_list[i])
    #     if len(arr_print) % 10 == 0:
    #         logger.error(','.join(arr_print))
    #         arr_print = []
    # logger.error(','.join(arr_print))
    概念统计_by_count_desc = dict(sorted(概念统计.items(), key=lambda x: len(x[1]), reverse=True))
    info_str = "\n板块\t\t前排"
    logger.error(info_str)
    for 概念, 统计 in 概念统计_by_count_desc.items():
        sorted_data = sorted(统计.items(), key=lambda x: x[1])
        info_str = f"{概念} 家数：{len(sorted_data)}"
        logger.error(info_str)
        info_str = "前排："
        for row in sorted_data[0:]:
            info_str += f"{row[0]} {row[1].hour}:{row[1].minute}:{row[1].second}\t"
        logger.info(info_str)
        info_str = f"身位版："

        for row in sorted(概念身位统计_时间[概念].items(), key=lambda x: x[1]):
            if row[1]:
                info_str += f"{row[0]} {概念身位统计[概念][row[0]]} {str(统计[row[0]])[11:]}\t"
            pass
        logger.info(info_str)
        pass
    pass


if __name__ == '__main__':
    # save_data(datetime.strptime(str(20200630), "%Y%m%d").strftime("%Y-%m-%d"))
    # 日内前排()
    # 刷新同花顺板块概念成分股缓存()
    # _加载同花顺板块列表()
    韭研公社异动采集(datetime.now().strftime('%Y%m%d'))
    # 韭研公社异动采集(datetime.now().strftime('%Y%m%d'))
    # 数据开始时间：20200630   一个账号一天只能扫80条。
    # date_list = db.mysql_localhost(sql="""
    #     SELECT distinct date FROM t_龙虎榜
    #     where date >= 20260401
    #     order by date
    # """, fetch=True)
    # for date_str in date_list:
    #     韭研公社异动采集(date_str['date'])
