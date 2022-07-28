import time
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = '''Host: www.csc108.com
Connection: keep-alive
sec-ch-ua: "Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"
Accept: */*
X-Requested-With: XMLHttpRequest
sec-ch-ua-mobile: ?0
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Referer: https://www.csc108.com/kyrz/xxggIndex.jspx
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9'''


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url_ori = 'https://www.csc108.com/kyrz/kcdbzjList.json?'
    stock_name = '中信建投'
    post = False
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
        p_data = list(self.post_data())[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        html = html.decode('utf-8')
        data, page_total = self.parse_html(html)
        time.sleep(2)
        self.df_data = self.running_main(list(self.post_data(N1=page_total))[0])
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data, n=1):
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        #         self.label2show = f"正在抓取第{p_data['currPage']}页...\n"
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(25)
            n += 1
            self.label2show = f'第{n}次尝试\n'
            return self.running_main(p_data, n)
        data, page_total = self.parse_html(html)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=9):
        data = f'curPage={page_num}&pageSize={N1}'
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)
        page_total = df_json['totalCount']
        df = pd.DataFrame(df_json['list'])
        df_columns = {'market': '市场', 'stkCode': '证券代码', 'stkName': '证券简称', 'pledgerate': '折算率',
                      }
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
    url_ori = 'https://www.csc108.com/kyrz/bdzqmdList.json?'
    stock_name = '中信建投'
    post = False
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
        p_data = list(self.post_data())[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        html = html.decode('utf-8')
        data, page_total = self.parse_html(html)
        time.sleep(2)
        self.df_data = self.running_main(list(self.post_data(N1=page_total))[0])
        df_rz, df_rq = modify_field(self.df_data)
        self.data_save(df_rz, self.file_path, label='融资标的')
        self.data_save(df_rq, self.file_path, label='融券标的')
        #         self.label2show = f'{label}抓取完毕。'
        self.label2show = '所有信息抓取完毕。'

    #         return self.df_data

    # 运行函数
    def running_main(self, p_data, n=1, label='融资标的'):
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        #         self.label2show = f"正在抓取第{p_data['currPage']}页...\n"
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(25)
            n += 1
            self.label2show = f'第{n}次尝试\n'
            return self.running_main(p_data, n, label)
        data, page_total = self.parse_html(html, label)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=9):
        data = f'curPage={page_num}&pageSize={N1}'
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的'):
        df_json = json.loads(html)
        page_total = df_json['totalCount']
        df = pd.DataFrame(df_json['list']['pageList'])
        df_columns = {'market': '市场', 'stkCode': '证券代码', 'stkName': '证券简称',
                      'marginRateFund': '融资保证金比例', 'marginRateStk': '融券保证金比例', }
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
