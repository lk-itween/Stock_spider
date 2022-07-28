import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.gtja.com
Connection: keep-alive
sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"
Accept: text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
sec-ch-ua-mobile: ?0
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Referer: https://www.gtja.com/content/margintrade/information/finance-bizparam/bizparam-offset.html
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url_ori = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json?jsonpcallback=jQuery351024614588214628186_1632733277599&keyword='
    stock_name = '国泰君安'
    post = False
    page_size = 4

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        html = html.decode('utf-8')
        self.df_data, page_total = self.parse_html(html)
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        html = html.replace('\r', '')
        html = '{' + html.split('(\n{')[1].strip('\n);')
        df_json = json.loads(html)
        page_total = 0
        df = pd.DataFrame(df_json['offset'])
        df_columns = {'branch': '市场', 'secCode': '证券代码', 'secAbbr': '证券简称', 'createTime': '日期',
                      'rate': '折算率',
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
    url_ori = 'https://www.gtja.com/cos/rest/margin/path/fuzzy.json?jsonpcallback=jQuery351024614588214628186_1632733277599&keyword='
    stock_name = '国泰君安'
    post = False

    # page_size = 20

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        html = html.decode('utf-8')
        df_rz, _ = self.parse_html(html, label='融资标的')
        df_rq, _ = self.parse_html(html, label='融券标的')
        df_rz, _ = modify_field(df_rz)
        df_rq, _ = modify_field(df_rq)
        self.data_save(df_rz, self.file_path, label='融资标的')
        self.data_save(df_rq, self.file_path, label='融券标的')
        self.label2show = '所有信息抓取完毕。'
        return df_rz, df_rq

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的'):
        html = html.replace('\r', '')
        html = '{' + html.split('(\n{')[1].strip('\n);')
        df_json = json.loads(html)
        page_total = 0
        df = pd.DataFrame()
        if label == '融资标的':
            df = pd.DataFrame(df_json['finance'])
        elif label == '融券标的':
            df = pd.DataFrame(df_json['security'])
        df_columns = {'branch': '市场', 'secCode': '证券代码', 'secAbbr': '证券简称', 'createTime': '日期',
                      'rate': label[:2] + '保证金比例',
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
