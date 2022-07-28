import time
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.gkzq.com.cn
Connection: keep-alive
Content-Length: 110
charset: UTF-8
Content-Type: application/x-www-form-urlencoded
Accept: */*
Origin: http://www.gkzq.com.cn
Referer: http://www.gkzq.com.cn/gkzq/rzrq/rzrq_xxgg.jsp?classid=00010001000500010001&fxjs=00010001000500010005
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url_ori = 'http://www.gkzq.com.cn/gkzq/rzrq/server/rzrqCdzqService.jsp'
    stock_name = '国开证券'
    post = True
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
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data, n=1):
        self.label2show = f'第{n}次尝试\n'
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
            return self.running_main(p_data, n)
        data, page_total = self.parse_html(html)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, rzrq_type='1', page_num=1, N1=10):
        data = {
            'cdzq_jys': 'all',
            'pageIndex': page_num,
            'pageSize': N1,
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)
        page_total = df_json['totalCounts']
        page_total = page_total if type(page_total) == int else int(float(page_total))
        df = pd.DataFrame(df_json['info'])
        df_columns = {'extendchar0': '市场', 'extendchar1': '证券代码', 'extendchar2': '证券简称', 'extendchar3': '折算率',
                      'busi_date': '日期'}
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
    url_ori = 'http://www.gkzq.com.cn/gkzq/rzrq/server/rzrqBdzqService.jsp'
    stock_name = '国开证券'
    post = True
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
        for rzrq_type, label in zip(['1', '2'], ['融资标的', '融券标的']):
            p_data = list(self.post_data(rzrq_type=rzrq_type))[0]
            res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                           post_data_func=self.post_data)
            if res_text == '数据抓取失败。\n':
                self.label2show = '数据抓取失败。\n'
                return None
            html = html.decode('utf-8')
            data, page_total = self.parse_html(html, label=label)
            if not page_total:
                continue
            time.sleep(2)
            self.df_data = self.running_main(list(self.post_data(rzrq_type=rzrq_type, N1=page_total))[0], label=label)
            df, _ = modify_field(self.df_data)
            self.data_save(df, self.file_path, label=label)
            #         self.data_save(df_rq, self.file_path, label='融券标的')
            self.label2show = f'{label}抓取完毕。'
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
    def post_data(self, rzrq_type='1', page_num=1, N1=10):
        data = {
            'rzrq_jys': 'all',
            'rzrq_type': rzrq_type,
            'pageIndex': page_num,
            'pageSize': N1,
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的'):
        df_json = json.loads(html)
        if df_json['result'] == 'fail':
            return None, None
        page_total = df_json['totalCounts']
        page_total = page_total if type(page_total) == int else int(float(page_total))
        df = pd.DataFrame(df_json['info'])
        df_columns = {'extendchar1': '市场', 'extendchar2': '证券代码', 'extendchar3': '证券简称',
                      'extendchar5': label[:2] + '保证金比例', 'margin_value': '融券保证金比例', 'bond_date': '日期'}
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
