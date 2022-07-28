import re
import time
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.s10000.com
Connection: keep-alive
Content-Length: 39
Cache-Control: max-age=0
sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"
sec-ch-ua-mobile: ?0
Upgrade-Insecure-Requests: 1
Origin: https://www.s10000.com
Content-Type: application/x-www-form-urlencoded
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: navigate
Sec-Fetch-User: ?1
Sec-Fetch-Dest: document
Referer: https://www.s10000.com/cdzq/rzrq/detail_zsl.jsp?classid=000100020012001400060002
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    #     date = '20201211'
    url_ori = 'http://www.s10000.com/cdzq/rzrq/detail_zsl.jsp?classid=000100020012001400060002&'  # {date}
    stock_name = '财达证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 25

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 多线程，设置线程池
    def thread_main(self):
        p_data = list(self.post_data(N1=1))[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        encoding = 'utf-8'
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        html = html.decode(encoding)
        data, page_total = self.parse_html(html)
        time.sleep(2)
        self.df_data = self.running_main(list(self.post_data(N1=page_total))[0])
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data):
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
            return self.running_main(p_data)
        data, page_total = self.parse_html(html)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=20):
        data = f'pageIndex={page_num}&pageSize={N1}'
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        page_total = int(re.search('共(\d+)页', html).group(1))
        data = re.search('<table cellspacing[.\s\S]*?/table>', html).group(0)
        df = pd.read_html(data, header=0)[0]
        df.columns = ['序号', '市场', '证券代码', '证券简称', '折算率']
        df.replace(r'^$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)
        df = trans_data(df, self.date)
        return df, page_total

    # 字段格式规格化
    def modify_field(self, df):
        df.replace(['（', '）'], ['(', ')'], regex=True, inplace=True)
        date_columns = [i for i in df.columns if '日期' in i]
        for i in date_columns:
            df[i] = df[i].str.replace('-', '')
        return df

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
    url_rz = 'http://www.s10000.com/cdzq/rzrq/detail.jsp?classid=000100020012001400060001&'
    url_rq = 'http://www.s10000.com/cdzq/rzrq/detail.jsp?classid=000100020012001400060006&'
    stock_name = '财达证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
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
            p_data = list(self.post_data(N1=1))[0]
            res_text, html = get_html_ajex(url, self.post, self.h_txt, p_data,
                                           post_data_func=self.post_data)
            if res_text == '数据抓取失败。\n':
                self.label2show = '数据抓取失败。\n'
                return None
            html = html.decode('utf-8')
            data, page_total = self.parse_html(html)
            time.sleep(2)
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
        data = f'pageIndex={page_num}&pageSize={N1}'
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的'):
        #         df_json = json.loads(html)
        page_total = int(re.search('共(\d+)页', html).group(1))
        data = re.search('<table cellspacing[.\s\S]*?/table>', html).group(0)
        df = pd.read_html(data, header=0)[0]
        df.columns = ['序号', '证券代码', '证券简称']
        df.replace(r'^$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)
        df = df.assign(市场='')
        df['融资保证金比例'] = '是'
        df['融券保证金比例'] = '是'
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
