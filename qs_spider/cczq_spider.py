import time
import chardet
import re
import json
import datetime
from functools import partial
import numpy as np
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.cgws.com
Connection: keep-alive
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
Referer: http://www.cgws.com/cczq/ccyw/rzrq/xxgg/rzb/
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9"""


# unicode字符转换
def uni2utf(df):
    df = df.replace(' ', '')
    str1 = ''.join('\\u' + i for i in re.findall('u([a-zA-Z0-9]{4})', df))
    str2 = str1.replace('\\', '')
    if re.search(f'^{str2}', df):
        str1 = json.loads(f'"{str1}"') + df.replace(str2, '')
    elif re.search(f'{str2}$', df):
        str1 = df.replace(str2, '') + json.loads(f'"{str1}"')
    else:
        str1 = df.split(str2)[0] + json.loads(f'"{str1}"') + df.split(str2)[1]
    return str1


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    stock_name = '长城证券'
    url_ori = 'http://www.cgws.com/was5/web/de.jsp?channelid=229873&page='
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 5

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        df_list = []
        p_data = '1'
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        encoding = chardet.detect(html)['encoding']
        encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] <= 0.99 else encoding
        html = html.decode(encoding)
        data, page_total = self.parse_html(html)
        self.page_start = int(self.page_start)
        self.page_start = self.page_start if self.page_start >= 1 else 1
        if self.page_end == 'all' or int(self.page_end) >= page_total:
            self.page_end = page_total
        self.page_end = int(self.page_end)
        main_par = partial(self.running_main, encoding=encoding)
        with ThreadPool(4) as pool:
            df_list += pool.map(main_par, range(self.page_start, self.page_end + 1, 1))
        self.df_data = pd.concat(df_list, ignore_index=True)
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data, encoding):
        p_data = str(p_data)
        time.sleep(np.random.random() * 1.1 + 1)
        self.label2show = f"正在抓取第{p_data}页...\n"
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode(encoding)
        except AttributeError:
            encoding = chardet.detect(html)['encoding']
            encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] <= 0.99 else encoding
            time.sleep(15)
            return self.running_main(p_data, encoding)
        data, page_total = self.parse_html(html)
        self.label2show = f"第{p_data}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=20):
        data = {
            'pageSize': N1,
            'pageNo': page_num,
            'rq': '',
            'keyword': '',
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)
        page_total = int(df_json['total'].strip(',').strip(' '))
        df = pd.DataFrame(df_json['rows'])
        df_columns = {'market': '市场', 'code': '证券代码', 'name': '证券简称', 'rate': '折算率', 'pub_date': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df['证券简称'] = df['证券简称'].map(uni2utf)
        df = trans_data(df, self.date)
        return df, page_total

    # 数据储存为 excel 格式
    def data_save(self, df, file_path, label='融资标的'):
        dfc = df.copy()
        path_name = f'{file_path}/{self.stock_name}{self.date}.xlsx'
        dfc = save_to_file(dfc, path_name, label)
        return dfc


class rzrq(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    stock_name = '长城证券'
    url_ori = 'http://www.cgws.com/was5/web/de.jsp?channelid=257420'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 5

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        rz_rq_url = {'融资': '&searchword=KGNyZWRpdGZ1bmRjdHJsPTAp&page=',
                     '融券': '&searchword=KGNyZWRpdHN0a2N0cmw9MCk=&page='}
        for label in ['融资', '融券']:
            df_list = []
            p_data = '1'
            url = self.url_ori + rz_rq_url[label]
            res_text, html = get_html_ajex(url, self.post, self.h_txt, p_data,
                                           post_data_func=self.post_data)
            if res_text == '数据抓取失败。\n':
                self.label2show = '数据抓取失败。\n'
                return None, None
            encoding = 'utf-8'
            html = html.decode(encoding)
            data, page_total = self.parse_html(html, label)
            self.page_start = int(self.page_start)
            self.page_start = self.page_start if self.page_start >= 1 else 1
            if self.page_end == 'all' or int(self.page_end) >= page_total:
                self.page_end = page_total
            self.page_end = int(self.page_end)
            data_range = range(self.page_start, self.page_end + 1, 1)
            main_par = partial(self.running_main, url=url, encoding=encoding, label=label)
            with ThreadPool(4) as pool:
                df_list += pool.map(main_par, data_range)
            self.df_data = pd.concat(df_list, ignore_index=True)
            df, _ = modify_field(self.df_data)
            self.data_save(df, self.file_path, label=label + '标的')
        self.label2show = '所有信息抓取完毕。'

    # 运行函数
    def running_main(self, p_data, url, encoding, label):
        p_data = str(p_data)
        time.sleep(np.random.random() * 1.1 + 1)
        self.label2show = f"正在抓取第{p_data}页...\n"
        res_text, html = get_html_ajex(url, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode(encoding)
        except AttributeError:
            time.sleep(15)
            return self.running_main(p_data, url, encoding, label)
        data, page_total = self.parse_html(html, label)
        self.label2show = f"第{p_data}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=20):
        data = {
            'pageSize': N1,
            'pageNo': page_num,
            'rq': '',
            'keyword': '',
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资'):
        df_json = json.loads(html)
        page_total = int(df_json['total'].strip(',').strip(' '))
        df = pd.DataFrame(df_json['rows'])
        df_columns = {'market': '市场', 'code': '证券代码', 'name': '证券简称', 'rate': label + '保证金比例', 'pub_date': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df['证券简称'] = df['证券简称'].map(uni2utf)
        df = trans_data(df, self.date)
        return df, page_total

    # 数据储存为 excel 格式
    def data_save(self, df, file_path, label='融资标的'):
        dfc = df.copy()
        path_name = f'{file_path}/{self.stock_name}{self.date}.xlsx'
        dfc = save_to_file(dfc, path_name, label)
        return dfc


class by2by():

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_start = page_start
        self.page_end = page_end
        self.path = path
        self.datestr = datestr

    def thread_main(self):
        run = db(self.page_start, self.page_end, self.path, self.datestr)
        run1 = rzrq(self.page_start, self.page_end, self.path, self.datestr)
        run.thread_main()
        run1.thread_main()
