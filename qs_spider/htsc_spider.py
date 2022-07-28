import time
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
from multiprocessing.dummy import Pool as ThreadPool

import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.htsc.com.cn
Connection: keep-alive
Content-Length: 73
sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
Origin: https://www.htsc.com.cn
Referer: https://www.htsc.com.cn/browser/rzrq/marginTrading/bdzqc/target_stock_pool_bail.jsp
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9"""


class rzrq(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date2 = date0.strftime('%Y%m%d')
    date1 = date0.strftime('%Y-%m-%d')
    url_ori = 'https://www.htsc.com.cn/browser/rzrqPool/getBdZqc.do'
    stock_name = '华泰证券'
    post = True
    page_size = 5

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date2 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        df_list = []
        p_data = list(self.post_data())[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        html = html.decode('utf-8')
        data, page_total = self.parse_html(html)
        self.page_start = int(self.page_start)
        self.page_start = self.page_start if self.page_start >= 1 else 1
        if self.page_end == 'all' or int(self.page_end) >= page_total:
            self.page_end = page_total
        self.page_end = int(self.page_end)
        p2_data = [list(self.post_data(_, self.page_size))[0] for _ in range(self.page_start, self.page_end + 1, 1)]

        with ThreadPool(4) as pool:
            df_list += pool.map(self.running_main, p2_data)
        self.df_data = pd.concat(df_list, ignore_index=True)
        df_rz, df_rq = modify_field(self.df_data)
        self.data_save(df_rz, self.file_path, label='融资标的')
        self.data_save(df_rq, self.file_path, label='融券标的')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data):
        time.sleep(np.random.random() * 1.1 + 1)
        self.label2show = f"正在抓取第{p_data['hsPage']}页...\n"
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(15)
            return self.running_main(p_data)
        data, page_total = self.parse_html(html)
        self.label2show = f"第{p_data['hsPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=5):
        data = {
            'date': self.date1,
            'stockCode': '',
            'hsPage': page_num,  # 沪市
            'hsPageSize': N1,
            'ssPage': page_num,  # 深市
            'ssPageSize': N1,
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)['result']
        hs_page_total = int(np.ceil(int(df_json['bdHsCount']) / self.page_size))
        ss_page_total = int(np.ceil(int(df_json['bdSsCount']) / self.page_size))
        try:
            df_hs = pd.DataFrame(df_json['bdHs'])
        except ValueError:
            df_hs = pd.DataFrame()
        try:
            df_ss = pd.DataFrame(df_json['bdSs'])
        except ValueError:
            df_ss = pd.DataFrame()
        df = df_hs.append(df_ss)
        df_columns = {'exchangeType': '市场', 'stockCode': '证券代码', 'stockName': '证券简称', 'dataDt': '日期',
                      'finRatio': '融资保证金比例', 'sloRatio': '融券保证金比例',
                      }
        df.rename(columns=df_columns, inplace=True)
        df = trans_data(df, self.date)
        return df, max(hs_page_total, ss_page_total)

    # 数据储存为 excel 格式
    def data_save(self, df, file_path, label='融资标的'):
        dfc = df.copy()
        path_name = f'{file_path}/{self.stock_name}{self.date}.xlsx'
        dfc = save_to_file(dfc, path_name, label)
        return dfc


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date2 = date0.strftime('%Y%m%d')
    date1 = date0.strftime('%Y-%m-%d')
    url_ori = 'https://www.htsc.com.cn/browser/rzrqPool/getDbZqc.do'
    stock_name = '华泰证券'
    post = True
    page_size = 5

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date2 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        df_list = []
        p_data = list(self.post_data())[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        html = html.decode('utf-8')
        data, page_total = self.parse_html(html)
        self.page_start = int(self.page_start)
        self.page_start = self.page_start if self.page_start >= 1 else 1
        if self.page_end == 'all' or int(self.page_end) >= page_total:
            self.page_end = page_total
        self.page_end = int(self.page_end)
        p2_data = [list(self.post_data(_, self.page_size))[0] for _ in range(self.page_start, self.page_end + 1, 1)]

        with ThreadPool(4) as pool:
            df_list += pool.map(self.running_main, p2_data)
        self.df_data = pd.concat(df_list, ignore_index=True)
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data):
        time.sleep(np.random.random() * 1.1 + 1)
        self.label2show = f"正在抓取第{p_data['hsPage']}页...\n"
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(15)
            return self.running_main(p_data)
        data, page_total = self.parse_html(html)
        self.label2show = f"第{p_data['hsPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=5):
        data = {
            'date': self.date1,
            'stockCode': '',
            'hsPage': page_num,  # 沪市
            'hsPageSize': N1,
            'ssPage': page_num,  # 深市
            'ssPageSize': N1,
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)['result']
        hs_page_total = int(np.ceil(int(df_json['dbHsCount']) / self.page_size))
        ss_page_total = int(np.ceil(int(df_json['dbSsCount']) / self.page_size))
        try:
            df_hs = pd.DataFrame(df_json['dbHs'])
        except ValueError:
            df_hs = pd.DataFrame()
        try:
            df_ss = pd.DataFrame(df_json['dbSs'])
        except ValueError:
            df_ss = pd.DataFrame()
        df = df_hs.append(df_ss)
        df_columns = {'exchangeType': '市场', 'stockCode': '证券代码', 'stockName': '证券简称', 'dataDt': '日期',
                      'assureRatio': '折算率',
                      }
        df.rename(columns=df_columns, inplace=True)
        df = trans_data(df, self.date)
        return df, max(hs_page_total, ss_page_total)

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
