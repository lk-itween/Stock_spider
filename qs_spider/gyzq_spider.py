import time
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.gyzq.com.cn
Connection: keep-alive
Content-Length: 54
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
Origin: http://www.gyzq.com.cn
Referer: http://www.gyzq.com.cn/main/company_business/credit_business/rzrq/publicity/index.html
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url_ori = 'http://www.gyzq.com.cn/servlet/json'  # {date}
    stock_name = '国元证券'
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
        df_szsh = []
        for num in ['0', '1']:
            df_szsh.append(self.running_main(list(self.post_data(jys=num))[0]))
        self.df_data = pd.concat(df_szsh)
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
    def post_data(self, jys='0', page_num=1, N1=10):
        data = {'funcNo': '904104',
                'jys': jys,
                'date': self.date0.strftime('%Y-%m-%d'), }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)['results']
        page_total = 0
        df = pd.DataFrame(df_json)
        df_columns = {'secu_code': '证券代码', 'secu_name': '证券简称', 'exchange_rate': '折算率',
                      'effectivedate': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df = df[['证券代码', '证券简称', '折算率', '日期']]
        df.replace(r'^$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)

        def sc(df):
            if df.split('.')[1] == 'SH':
                return '上海'
            elif df.split('.')[1] == 'SZ':
                return '深圳'
            else:
                return ''

        df['市场'] = df['证券代码'].map(sc)
        df['日期'] = df['日期'].map(lambda x: x.split(' ')[0])
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
    url_ori = 'http://www.gyzq.com.cn/servlet/json'
    stock_name = '国元证券'
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
        for cxlx, label in zip(['0', '1'], ['融资标的', '融券标的']):
            df_szsh = []
            for num in ['0', '1']:
                df_szsh.append(self.running_main(list(self.post_data(jys=num, cxlx=cxlx))[0], label=label))
            self.df_data = pd.concat(df_szsh)
            df, _ = modify_field(self.df_data)
            self.data_save(df, self.file_path, label=label)
            self.label2show = f'{label}抓取完毕。'
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

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
    def post_data(self, jys='0', cxlx='0', N1=10):
        data = {'funcNo': '904103',
                'jys': jys,
                'cxlx': cxlx,
                'date': self.date0.strftime('%Y-%m-%d'), }
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的'):
        df_json = json.loads(html)['results']
        page_total = 0
        df = pd.DataFrame(df_json)

        if label == '融资标的':
            df_columns = {'secu_code': '证券代码', 'secu_name': '证券简称', 'rz_ratio': label[:2] + '保证金比例',
                          'effectivedate': '日期'}
        else:
            df_columns = {'secu_code': '证券代码', 'secu_name': '证券简称', 'rq_ratio': label[:2] + '保证金比例',
                          'effectivedate': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df = df[['证券代码', '证券简称', label[:2] + '保证金比例', '日期']]
        df.replace(r'^$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)

        def sc(df):
            if df.split('.')[1] == 'SH':
                return '上海'
            elif df.split('.')[1] == 'SZ':
                return '深圳'
            else:
                return ''

        df['市场'] = df['证券代码'].map(sc)
        df['日期'] = df['日期'].map(lambda x: x.split(' ')[0])
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
