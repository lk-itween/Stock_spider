import time
import chardet
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_data, get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.i618.com.cn
Connection: keep-alive
Content-Length: 73
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
Origin: http://www.i618.com.cn
Referer: http://www.i618.com.cn/gsyw/xyyw/rzrq/xxgs/kcdb/index.shtml
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url_ori = 'http://www.i618.com.cn/servlet/json'
    stock_name = '山西证券'
    post = True  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url，仅当post为True时生效
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
        label = '可担保物'
        save_path = f'{self.file_path}/{self.stock_name}{self.date}.xlsx'
        p_data = list(self.post_data(page_num=2))[0]
        data = get_data(self.url_ori, self.post, self.h_txt, p_data, self.page_start, self.page_end,
                        self.scrollwithauto, label=label,
                        running_func=self.running_main, post_data_func=self.post_data, parse_html_func=self.parse_html)
        if data.empty:
            self.label2show = '数据抓取失败。\n'
            return data
        self.df_data = data
        self.df_data, _ = modify_field(self.df_data)
        dfc = save_to_file(self.df_data, save_path, label=label)
        self.label2show = '可担保物信息抓取完毕。\n'
        return dfc

    # 运行函数
    def running_main(self, p_data, referer, encoding='utf-8', n=1, label='融资标的'):
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)

        if self.scrollwithauto:
            self.label2show = f"正在抓取第{p_data}页...\n"
            data_nump = list(self.post_data(page_num=p_data, label=label))[0]
        else:
            data_nump = list(self.post_data(page_num=1, N1=p_data, label=label))[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, data_nump, referer=referer,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode(encoding)
        except (AttributeError, UnicodeDecodeError) as e:
            encoding = chardet.detect(html)['encoding']
            encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] <= 0.99 else encoding
            time.sleep(25)
            n += 1
            return self.running_main(p_data, referer, encoding=encoding, n=n, label=label)
        data, pageOrTotal = self.parse_html(html, exc=n, label=label)
        if self.scrollwithauto:
            self.label2show = f"第{p_data}页已抓取。\n"
        else:
            self.label2show = f"当前页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=10, label='label'):
        data = {
            'funcNo': '840002',
            'curPage': page_num,
            'numPerPage': N1,
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='label', exc=1):
        df_json = json.loads(html)['results'][0]
        page_total = df_json['totalRows']
        page_total = page_total if type(page_total) == int else int(float(page_total))
        df = pd.DataFrame(df_json['data'])
        df_columns = {'SecMarket': '市场', 'secucode': '证券代码', 'name': '证券简称', 'discountrate': '折算率',
                      'EffectiveDate': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df = trans_data(df, self.date)
        return df, page_total


class rzrq(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url_ori = 'http://www.i618.com.cn/servlet/json'
    stock_name = '山西证券'
    post = True  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url，仅当post为True时生效
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
        label = '融资标的'
        save_path = f'{self.file_path}/{self.stock_name}{self.date}.xlsx'
        p_data = list(self.post_data())[0]
        data = get_data(self.url_ori, self.post, self.h_txt, p_data, self.page_start, self.page_end,
                        self.scrollwithauto, label=label,
                        running_func=self.running_main, post_data_func=self.post_data, parse_html_func=self.parse_html)
        if data.empty:
            self.label2show = '数据抓取失败。\n'
            return data
        self.df_data = data
        df_rz, df_rq = modify_field(self.df_data)
        save_to_file(df_rz, save_path, label='融资标的')
        save_to_file(df_rq, save_path, label='融券标的')
        self.label2show = '所有信息抓取完毕。'

        # return self.df_data

    # 运行函数
    def running_main(self, p_data, referer, pn=1, encoding='utf-8', label=None, n=1):
        if isinstance(p_data, list):
            pn = p_data[0]
            p_data = p_data[1]
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        time.sleep(np.random.randint(5))
        if self.scrollwithauto:
            self.label2show = f"正在抓取第{p_data}页...\n"
            data_nump = list(self.post_data(page_num=p_data))[0]
        else:
            data_nump = list(self.post_data(page_num=pn, N1=p_data))[0]  # 第 1 页无法访问！
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, data_nump, referer=referer,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode(encoding)
        except (AttributeError, UnicodeDecodeError) as e:
            encoding = chardet.detect(html)['encoding']
            encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] <= 0.99 else encoding
            time.sleep(15)
            n += 1
            return self.running_main(p_data, referer, pn=pn, encoding=encoding, label=label, n=n)
        data, pageOrTotal = self.parse_html(html, exc=n)
        if self.scrollwithauto:
            self.label2show = f"第{p_data}页已抓取。\n"
        else:
            self.label2show = f"当前页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=10):
        data = {
            'funcNo': '840001',
            'curPage': page_num,
            'numPerPage': N1,
        }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的', exc=1):
        df_json = json.loads(html)['results'][0]
        page_total = df_json['totalRows']
        page_total = page_total if type(page_total) == int else int(float(page_total))
        df = pd.DataFrame(df_json['data'])
        df_columns = {'SecMarket': '市场', 'stockcode': '证券代码', 'stockname': '证券简称', 'capitalrate': '融资保证金比例',
                      'stockrate': '融券保证金比例', 'EffectiveDate': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df = trans_data(df, self.date)
        return df, page_total


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
