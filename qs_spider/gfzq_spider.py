import re
import time
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.gf.com.cn
Connection: keep-alive
Accept: */*
X-Requested-With: XMLHttpRequest
Referer: http://www.gf.com.cn/business/finance/news/guaranteed
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url = f'http://new.gf.com.cn/business/finance/ratiolist?'  # {date}
    stock_name = '广发证券'
    post = False
    page_size = 20

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        p_data = list(self.post_data())[0]
        res_text, html = get_html_ajex(self.url, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        html = html.decode('utf-8')
        data, page_total = self.parse_html(html)
        time.sleep(10)
        self.df_data = self.running_main(list(self.post_data(N1=page_total))[0])
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data):
        time.sleep(np.random.random() * 1.1 + 1)
        #         self.label2show = f"正在抓取第{p_data['currPage']}页...\n"
        res_text, html = get_html_ajex(self.url, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(25)
            return self.running_main(p_data)
        data, page_total = self.parse_html(html)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=20):
        data = f'pageSize={N1}&init_date={self.date}'
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)
        page_total = df_json['count'] if type(df_json['count']) == int else int(
            (df_json['count'].strip(',').strip(' ')))
        df_str = df_json['result']
        df_iter = re.findall(
            'stock"><span>(.*?)<[\s\S]*?"code">(\d{6})</span>[\s\S]*?ratio">(.*?)<[\s\S]*?date">(\d{4}-\d{2}-\d{2})<',
            df_str)
        df = pd.DataFrame(df_iter, columns=['证券简称', '证券代码', '折算率', '日期'])
        df.replace(r'^$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)
        df = df.assign(市场='')
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
    url_rz = 'http://www.gf.com.cn/business/finance/targetlist?type=fin&'
    url_rq = 'http://www.gf.com.cn/business/finance/targetlist?type=slo&'
    stock_name = '广发证券'
    post = False
    page_size = 20

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        for url, label in zip([self.url_rz, self.url_rq], ['融资标的', '融券标的']):
            p_data = list(self.post_data())[0]
            res_text, html = get_html_ajex(url, self.post, self.h_txt, p_data,
                                           post_data_func=self.post_data)
            if res_text == '数据抓取失败。\n':
                self.label2show = '数据抓取失败。\n'
                continue
            html = html.decode('utf-8')
            data, page_total = self.parse_html(html)
            time.sleep(10)
            self.df_data = self.running_main(url, list(self.post_data(N1=page_total))[0], label=label)
            self.df_data, _ = modify_field(self.df_data)
            self.data_save(self.df_data, self.file_path, label=label)
            self.label2show = f'{label}抓取完毕。'
        # writer.save()
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, url, p_data, label='融资标的'):
        time.sleep(np.random.random() * 1.1 + 1)
        #         self.label2show = f"正在抓取第{p_data['currPage']}页...\n"
        res_text, html = get_html_ajex(url, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(25)
            return self.running_main(url, p_data, label=label)
        data, page_total = self.parse_html(html, label=label)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=20):
        data = f'pageSize={N1}&init_date={self.date}'
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的'):
        df_json = json.loads(html)
        page_total = df_json['count'] if type(df_json['count']) == int else int(
            (df_json['count'].strip(',').strip(' ')))
        df_str = df_json['result']
        df_iter = re.findall(
            'stock"><span>(.*?)<[\s\S]*?"code">(\d{6})</span>[\s\S]*?ratio">(.*?)<[\s\S]*?date">(\d{4}-\d{2}-\d{2})<',
            df_str)
        df = pd.DataFrame(df_iter, columns=['证券简称', '证券代码', label[:2] + '保证金比例', '日期'])
        df.replace(r'^$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)
        df = df.assign(市场='')
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
