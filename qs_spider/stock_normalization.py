import os
import time
import re
import chardet
import numpy as np
import pandas as pd
import requests
from functools import partial
from openpyxl import load_workbook
from faker import Faker
from multiprocessing.dummy import Pool as ThreadPool


# 网页请求
def get_html_ajex(url, post, header_txt, data=None, referer=None, UA=None, post_data_func=None):
    if not data:
        data = ''
    if not UA:
        UA = Faker(locale='zh_CN').chrome(version_from=80, version_to=84, build_from=3600, build_to=4200)
    try:
        headers = get_header(header_txt, user_agent=UA, referer=referer)
        time.sleep(np.random.random() * 1.2 + 0.5)
        res = requests.post(url, data=data, headers=headers) if post else requests.get(url + data, headers=headers)
        sta_code = res.status_code
        if sta_code == 405:
            if 'https:' in url:
                return '数据抓取失败。\n', None
            url = url.replace('http:', 'https:')
            return get_html_ajex(url, post, header_txt, data=data, UA=UA)
        elif sta_code == 304:
            if not post_data_func:
                return '数据抓取失败。\n', None
            data = post_data_func(N1=np.random.randint(1, 40))
            return get_html_ajex(url, post, header_txt, data=data, UA=UA)
        elif sta_code == 200:
            return '成功', res.content
        return '数据抓取失败。\n', None
    except requests.exceptions.ChunkedEncodingError:
        return b'ChunkedEncodingError', None
    except:
        return '数据抓取失败。\n', None


# 请求落地页
def get_html_load(url, header_txt, referer=None, data=None, UA=None, post_data_func=None):
    if not data:
        data = ''
    if not UA:
        UA = Faker(locale='zh_CN').chrome(version_from=80, version_to=84, build_from=3600, build_to=4200)
    try:
        headers = get_header(header_txt, user_agent=UA, referer=referer)
        time.sleep(np.random.random() * 1.2 + 0.5)
        res = requests.get(url + data, headers=headers)
        sta_code = res.status_code
        if sta_code == 405:
            if 'https:' in url:
                return '数据抓取失败。\n', None
            url = url.replace('http:', 'https:')
            return get_html_load(url, header_txt, referer, data, UA, post_data_func)
        elif sta_code == 304:
            if not post_data_func:
                return '数据抓取失败。\n', None
            data = post_data_func(N1=np.random.randint(1, 40))
            return get_html_load(url, header_txt, referer, data, UA, post_data_func)
        elif sta_code == 200:
            return '成功', res.content
        return '数据抓取失败。\n', None
    except requests.exceptions.ChunkedEncodingError:
        return b'ChunkedEncodingError', None
    except:
        return '数据抓取失败。\n', None


def get_data(url, post, header_txt, data, page_start, page_end, scrolling=None, referer=None, label=None,
             running_func=None, post_data_func=None, parse_html_func=None):
    res_text, html = get_html_ajex(url, post, header_txt, data, referer=referer, post_data_func=post_data_func)
    if res_text == '数据抓取失败。\n':
        return pd.DataFrame()
    encoding = chardet.detect(html)['encoding']
    encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] <= 0.99 else encoding
    html = html.decode(encoding)
    data, page_num = parse_html_func(html, label=label, exc=1)
    if data.empty:
        return data
    time.sleep(2)

    if page_num:
        if page_num == 2000:
            page_num = binary_html_get_page(url, header_txt, encoding, page_num, post_data_func, parse_html_func,
                                               referer, label=label)
            if not page_num:
                return pd.DataFrame()

        page_start = int(page_start)
        page_start = page_start if page_start >= 1 else 1
        if page_end == 'all' or int(page_end) >= page_num:
            page_end = page_num
        page_end = int(page_end)
        if scrolling:
            main_par = partial(running_func, referer=referer, encoding=encoding, n=1, label=label)
            with ThreadPool(4) as pool:
                df_list = pool.map(main_par, range(page_start, page_end + 1, 1))
            return pd.concat(df_list, ignore_index=True)
        else:
            return running_func(page_num, referer, encoding=encoding, label=label)
    else:
        return data


def get_header(header_text, user_agent=None, referer=None):
    if not user_agent:
        user_agent = Faker(locale='zh_CN').chrome(version_from=80, version_to=84, build_from=3600, build_to=4200)
    headers = {i.replace('://', '^&*').split(': ')[0].replace(r'^&*', '://'): i.replace('://', '^&*').
        split(': ')[1].replace(r'^&*', '://') for i in header_text.split('\n')}
    headers['User-Agent'] = user_agent
    if referer:
        headers['referer'] = referer
    return headers


# 规格化比例字段
def exchange_bzj(dfc, columns, date):
    df = dfc.copy()
    columns = list(set(columns).difference({'融资保证金比例', '融券保证金比例', '折算率'}))
    for i in columns:
        if i not in df.columns:
            if i == '日期':
                df[i] = date
            else:
                df[i] = ''

    def exchange_jys(x):
        if x in ['沪Ａ', 'SH', 'sh', 'sha', 'SHAG', '上海', '上海A', '上海Ａ', '上交所', '沪交所', '上海市场', '上海交易所', '上海证券交易所', '1']:
            return '上海'
        elif x in ['深Ａ', 'SZ', 'sz', 'sza', 'SZAG', '深圳', '深圳A', '深圳Ａ', '深交所', '深圳市场', '深圳交易所', '深圳证券交易所', '0', '2']:
            return '深圳'
        else:
            return ''

    def exchange_date(x):
        if type(x) == str:
            if '(' in x:
                x = int(re.search(r'\d{6,20}', x).group(0))
                return exchange_date(x)
            elif re.search(r'[-/.]', x):
                return x
            elif x == '本日发布':
                return date
            elif len(x) == 8:
                return x
            else:
                x = int(re.search(r'\d{6,20}', x).group(0))
                return exchange_date(x)
        else:
            if len(str(x)) == 8:
                return str(x)
            elif x > time.time():
                x = x // 1000
            return time.strftime("%Y-%m-%d", time.localtime(x))

    def bzjbl(x):
        if type(x) == str:
            if re.search(r'[%是Y]', x):
                return x
            elif re.search(r'[-否N]', x):
                return '--'
            x = round(float(x), 4)
        if x >= 5:
            return str(x) + '%'
        else:
            return str(round(x * 100, 2)) + '%'

    df['市场'] = df['市场'].map(exchange_jys)
    df['日期'] = df['日期'].map(exchange_date)
    df['证券代码'] = df['证券代码'].map(lambda x: x if len(x) == 6 else '0' * (6 - len(x)) + x)
    if '折算率' in df.columns:
        df['折算率'] = df['折算率'].map(bzjbl)
    if '融资保证金比例' in df.columns:
        df['融资保证金比例'] = df['融资保证金比例'].map(bzjbl)
    if '融券保证金比例' in df.columns:
        df['融券保证金比例'] = df['融券保证金比例'].map(bzjbl)
    df.replace(['0.00%', '0.0%', '0%', '-'], '0', inplace=True)
    return df


# 重名名字段名，筛选有效字段
def trans_data(datac, date):
    data = datac.copy()
    data['证券代码'] = data['证券代码'].map(str)
    data = data[data['证券代码'].str.contains(r'\d+')]
    data['证券代码'] = data['证券代码'].map(lambda x: str(int(x)))
    columns = ['市场', '证券代码', '证券简称', '融资保证金比例', '融券保证金比例', '折算率', '日期']
    data = data[[i for i in data.columns if i in columns]].copy()
    data.replace(r'^$', np.nan, regex=True, inplace=True)
    data.dropna(inplace=True, how='all')
    data.replace(' ', '', regex=True, inplace=True)
    data.replace('未定义', '', inplace=True)
    data = exchange_bzj(data, columns, date)
    dfcolumns = data.columns.tolist()
    dfcolumns.sort(key=columns.index)
    data = data[dfcolumns].copy()
    return data


# 二分递减
def binary_chop(data, pre_data=None, acc=True):
    if acc:
        if pre_data:
            data += abs(pre_data - data) // 2
        else:
            pre_data = data
            data = data * 2
    else:
        pre_data = data
        data = data // 2
    return pre_data, data + 1


# 字段格式规格化
def modify_field(df):
    df.drop_duplicates(inplace=True)
    df.replace(['（', '）'], ['(', ')'], regex=True, inplace=True)
    date_columns = [i for i in df.columns if '日期' in i]
    for i in date_columns:
        df[i] = df[i].str.replace(r'-|\.|/', '')
        df[i] = df[i].map(lambda x: x[:8])
    if {'融资保证金比例', '融券保证金比例'} <= set(df.columns):
        df_rz = df[['市场', '证券代码', '证券简称', '融资保证金比例', '日期']].copy()
        df_rq = df[['市场', '证券代码', '证券简称', '融券保证金比例', '日期']].copy()
        return df_rz, df_rq
    else:
        return df, pd.DataFrame()


def binary_html_get_page(url, header, encoding, page, post_data_func, parser_html_func, referer=None, label='其他'):
    get_num = 1
    pre_data = None
    data = pd.DataFrame()
    while get_num <= 4:
        p_data = list(post_data_func(page_num=page, label=label))[0]
        res_text, html = get_html_ajex(url, header, p_data, referer=referer, post_data_func=post_data_func)
        if res_text == '数据抓取失败。\n':
            return None
        html = html.decode(encoding)
        data, _ = parser_html_func(html, exc=1)
        get_num += 1
        if data.empty:
            pre_data, page = binary_chop(page, pre_data=pre_data, acc=False)
        else:
            pre_data, page = binary_chop(page, pre_data=pre_data, acc=True)
    return page if data.empty else pre_data


def save_to_file(df, path_name, label='可担保物'):
    if not os.path.exists(path_name):
        pd.DataFrame().to_excel(path_name, sheet_name=label)
    bl_column = [i for i in df.columns if '比例' in i or '折算率' in i][0]
    dfc = df[df[bl_column].str.contains(r'\d')]
    if dfc.empty:
        df.replace('Y', '是', inplace=True)
        dfc = df[df[bl_column].str.contains(r'是')].copy()
    else:
        bl_sum = dfc[bl_column].map(lambda x: round(float(x.replace('%', '')), 2))
        blsums = bl_sum.sum()
        if blsums == 0:
            return None
    dfc.drop_duplicates(inplace=True)
    book = load_workbook(path_name)
    with pd.ExcelWriter(path_name, engine='openpyxl', mode='w+') as writer:
        writer.book = book
        wb = writer.book
        try:
            ws = wb[label]
            wb.remove(ws)
        except:
            pass
        dfc.to_excel(writer, index=False, sheet_name=label)
    dfc['类别'] = label
    return dfc
