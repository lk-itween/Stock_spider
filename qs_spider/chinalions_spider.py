import time
import chardet
import re
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.chinalions.com
Connection: keep-alive
Upgrade-Insecure-Requests: 1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url_ori = 'http://www.chinalions.com'
    stock_name = '华林证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 10

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt,
                                       '/main/ProductsMall/rzrq/kcdbzjmdjzsl/index.shtml',
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        encoding = chardet.detect(html)['encoding']
        encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] < 99 else encoding
        html = html.decode(encoding)
        data, page_total = self.parse_html(html)
        time.sleep(2)
        self.df_data = self.running_main(page_total, encoding=encoding)
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data, encoding='utf-8', n=1):
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        #         self.label2show = f"正在抓取第{p_data['currPage']}页...\n"
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data, post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode(encoding)
        except AttributeError:
            time.sleep(25)
            n += 1
            return self.running_main(p_data, n=n)
        data, page_total = self.parse_html(html, flag=n)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=page_size):
        data = {
            'page': page_num,
            'size': N1,
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, flag=0):
        """
        --json数据： 获取数据总量
        param: page_total
        --html数据： 获取当前页面数量及总页数，计算出page_total
        param: pagecount   总页数
        param: numPerpage  前页面数量
        return df, pagetotal
        """
        page_total = None
        if not flag:
            return None, re.search(fr'<a href="(.*?{self.date}.*?)"[\s\S]+?可充抵保证金证券名单（{self.date}）', html).group(1)
        try:
            df_json = json.loads(html)
            page_total = df_json['count']
            page_total = page_total if type(page_total) == int else int(float(page_total))
            df = pd.DataFrame(df_json['result'])
        except json.JSONDecodeError:
            if '<table' in html:
                html = re.search(r'<table[\s\S]*?/table>', html).group(0)
                df = pd.read_html(html, header=0)[0].dropna()
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0].dropna()
        df_columns = {'交易市场': '市场', '证券代码': '证券代码', '证券简称': '证券简称', '实际折算率': '折算率',
                      'ctime': '日期'}
        df.rename(columns=df_columns, inplace=True)
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
    url_ori = 'http://www.chinalions.com'
    stock_name = '华林证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 10

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        p_data = '/main/ProductsMall/rzrq/bdzqmdjbzjbl/index.shtml'
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        encoding = chardet.detect(html)['encoding']
        encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] < 99 else encoding
        html = html.decode(encoding)
        data, page_total = self.parse_html(html)
        time.sleep(2)
        self.df_data = self.running_main(page_total, encoding=encoding)
        df_rz, _ = modify_field(self.df_data)
        self.data_save(df_rz, self.file_path, label='融资标的')
        self.label2show = '所有信息抓取完毕。'

    # 运行函数
    def running_main(self, p_data, encoding='utf-8', n=1, label='融资标的'):
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        #         self.label2show = f"正在抓取第{p_data['currPage']}页...\n"
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode(encoding)
        except AttributeError:
            time.sleep(25)
            n += 1
            self.label2show = f'第{n}次尝试\n'
            return self.running_main(p_data, n=n, label=label)
        data, page_total = self.parse_html(html, flag=n, label=label)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=page_size):
        data = {
            'page': page_num,
            'size': N1,
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的', flag=0):
        """
        --json数据： 获取数据总量
        param: page_total
        --html数据： 获取当前页面数量及总页数，计算出page_total
        param: pagecount   总页数
        param: numPerpage  前页面数量
        return df, pagetotal
        """
        page_total = None
        if not flag:
            return None, re.search(fr'<a href="(.*?{self.date}.*?)"[\s\S]+?融资标的证券及保证金比例（{self.date}）', html).group(1)
        try:
            df_json = json.loads(html)
            page_total = df_json['count']
            page_total = page_total if type(page_total) == int else int(float(page_total))
            df = pd.DataFrame(df_json['result'])
        except json.JSONDecodeError:
            if '<table' in html:
                df = pd.read_html(html, header=0)[0].dropna()
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0].dropna()
        df_columns = {'交易市场': '市场', '证券代码': '证券代码', '证券简称': '证券简称', '融资保证金比例': '融资保证金比例',
                      'marginratestk': '融券保证金比例', 'ctime': '日期'}
        df.rename(columns=df_columns, inplace=True)
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
