import time
import chardet
import re
import json
import datetime
from functools import partial
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, binary_html_get_page, save_to_file
from multiprocessing.dummy import Pool as ThreadPool

import warnings

warnings.filterwarnings("ignore")

h_txt = '''Host: www.gjzq.com.cn
Connection: keep-alive
sec-ch-ua: "Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
sec-ch-ua-mobile: ?0
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Referer: https://www.gjzq.com.cn/main/margin/gsxx/tbzq.html
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9'''


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    url_ori = 'https://www.gjzq.com.cn/service/rzrqStocks'
    stock_name = '国金证券'
    post = False  # 数据请求方式
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 25  # 页面数据量大小

    #######################################

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
        ########  需要修改的部分
        p_data = list(self.post_data())[0]

        ################################################
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        encoding = chardet.detect(html)['encoding']
        encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] < 99 else encoding
        html = html.decode(encoding)
        data, pageOrTotal = self.parse_html(html, exc=1)
        time.sleep(2)

        if pageOrTotal:
            if pageOrTotal == 2000:
                pageOrTotal = binary_html_get_page(self.url_ori, self.h_txt, encoding, pageOrTotal,
                                                   self.post_data, self.parse_html, label='1')
                if not pageOrTotal:
                    self.label2show = '数据抓取失败。\n'

            self.page_start = int(self.page_start)
            self.page_start = self.page_start if self.page_start >= 1 else 1
            if self.page_end == 'all' or int(self.page_end) >= pageOrTotal:
                self.page_end = pageOrTotal
            self.page_end = int(self.page_end)

            self.df_data = pd.DataFrame()
            if self.scrollwithauto:
                main_par = partial(self.running_main, encoding=encoding, n=1)
                with ThreadPool(4) as pool:
                    df_list += pool.map(main_par, range(self.page_start, self.page_end + 1, 1))
                self.df_data = pd.concat(df_list, ignore_index=True)
            else:
                self.df_data = self.running_main(pageOrTotal, encoding=encoding)
        else:
            self.df_data = data
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '可担保物信息抓取完毕。\n'
        return self.df_data

    # 运行函数
    def running_main(self, p_data, encoding='utf-8', n=1):
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)

        if self.scrollwithauto:
            self.label2show = f"正在抓取第{p_data}页...\n"
            data_nump = list(self.post_data(page_num=p_data))[0]
        else:
            data_nump = list(self.post_data(page_num=1, N1=p_data))[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, data_nump,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode(encoding)
        except AttributeError:
            time.sleep(25)
            n += 1
            return self.running_main(p_data, n=n)
        data, pageOrTotal = self.parse_html(html, exc=n)
        if self.scrollwithauto:
            self.label2show = f"第{p_data}页已抓取。\n"
        else:
            self.label2show = f"当前页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, label='101202', N1=page_size):
        """ post: data参数
        每次重新编辑需要修改
        """
        datapost = {
            'page_no': page_num,
            'page_size': N1,
            'type': '1',
        }
        dataget = '?' + '&'.join([f'{x}={y}' for x, y in datapost.items()])
        yield datapost if self.post else dataget

    # 解析初始网页 json 数据
    def parse_html(self, html, exc=0):
        """
        --json数据： 获取数据总量
        return pageOrTotal
        --html数据： 获取当前页面数量及总页数，计算出page_total
        param: pagecount   总页数
        param: numPerpage  当前页面数量
        return df, pageOrTotal
        """
        ########  需要修改的部分
        pageOrTotal = None
        if not exc:
            return None, re.search(fr'<a href="(.*?{self.date}.*?)"[\s\S]+?可充抵保证金证券名单（{self.date}）', html).group(1)
        try:
            df_json = json.loads(html)
            try:
                pageOrTotal = df_json['total']
                pageOrTotal = pageOrTotal if type(pageOrTotal) == int else int(float(pageOrTotal))
            #                 pageOrTotal = pageOrTotal * 18
            except KeyError:
                pageOrTotal = 0
            df = pd.DataFrame(df_json['data'])
        except json.JSONDecodeError:
            pageOrTotal = 0
            if '<table' in html:
                html = re.search(r'<table[\s\S]*?证券代码[\s\S]*?/table>', html).group(0)
                html = re.sub(r'\s+', ' ', html)
                df = pd.read_html(html, header=0)[0].dropna()
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                html1 = re.sub(r'\s+', ' ', html1)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0].dropna()
        #             pageOrTotal = pageOrTotal if pageOrTotal else 2000

        df_columns = {'marketName': '市场', 'stockCode': '证券代码', 'stockName': '证券简称', 'converRate': '折算率',
                      'updateDate': '日期', '状态': '状态', }
        df.rename(columns=df_columns, inplace=True)
        #         df = df[df['状态']=='正常']
        #######################################
        df = trans_data(df, self.date)
        return df, pageOrTotal

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
    ########  需要修改的部分
    url_ori = 'https://www.gjzq.com.cn/service/rzrqStocks'
    stock_name = '国金证券'
    post = False  # 数据请求方式
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 25  # 页面数据量大小
    combineflag = {0: ['融资融券'],
                   1: ['融资标的', '融券标的'],
                   2: ['融资标的'],
                   3: ['融券标的'],
                   }  # 融资融券展示方式

    #######################################

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 构造请求数据字典
    def post_data(self, page_num=1, label='融资融券', N1=page_size):
        """ post: data参数
        每次重新编辑需要修改
        """
        rzrq_dict = {'融资标的': '',
                     '融券标的': '',
                     '融资融券': '', }
        datapost = {
            'page_no': page_num,
            'page_size': N1,
            'type': '0',
        }
        dataget = '?' + '&'.join([f'{x}={y}' for x, y in datapost.items()])
        yield datapost if self.post else dataget

    # 多线程，设置线程池
    def thread_main(self):
        df_list = []
        ########  需要修改的部分
        rzrq = self.combineflag[1]

        #######################################
        for label in rzrq:
            p_data = list(self.post_data(label=label))[0]
            res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                           post_data_func=self.post_data)
            if res_text == '数据抓取失败。\n':
                self.label2show = '数据抓取失败。\n'
                return None
            encoding = chardet.detect(html)['encoding']
            encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] < 99 else encoding
            html = html.decode(encoding)
            data, pageOrTotal = self.parse_html(html, label=label, exc=1)
            time.sleep(2)
            if pageOrTotal:
                if pageOrTotal == 2000:
                    pageOrTotal = binary_html_get_page(self.url_ori, self.h_txt, encoding, pageOrTotal,
                                                       self.post_data, self.parse_html, label=label)
                    if not pageOrTotal:
                        self.label2show = '数据抓取失败。\n'

                self.page_start = int(self.page_start)
                self.page_start = self.page_start if self.page_start >= 1 else 1
                if self.page_end == 'all' or int(self.page_end) >= pageOrTotal:
                    self.page_end = pageOrTotal
                self.page_end = int(self.page_end)

                self.df_data = pd.DataFrame()
                if self.scrollwithauto:
                    main_par = partial(self.running_main, encoding=encoding, n=1, label=label)
                    with ThreadPool(4) as pool:
                        df_list += pool.map(main_par, range(self.page_start, self.page_end + 1, 1))
                    self.df_data = pd.concat(df_list, ignore_index=True)
                else:
                    self.df_data = self.running_main(pageOrTotal, encoding=encoding, label=label)
            else:
                self.df_data = data
            df_rz, df_rq = modify_field(self.df_data)
            if df_rq.empty:
                self.data_save(df_rz, self.file_path, label=label)
                self.label2show = f'{self.stock_name}{label}信息抓取完毕。\n'
            else:
                self.data_save(df_rz, self.file_path, label='融资标的')
                self.data_save(df_rq, self.file_path, label='融券标的')
                self.label2show = f'{self.stock_name}融资标的信息抓取完毕。\n{self.stock_name}融券标的信息抓取完毕。\n'
            self.label2show = f'{self.stock_name}融资融券信息抓取完毕。\n'

    # 运行函数
    def running_main(self, p_data, encoding='utf-8', n=1, label='融资标的'):
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)

        if self.scrollwithauto:
            self.label2show = f"正在抓取第{p_data}页...\n"
            data_nump = list(self.post_data(page_num=p_data, label=label))[0]
        else:
            data_nump = list(self.post_data(page_num=1, N1=p_data, label=label))[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, data_nump,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode(encoding)
        except AttributeError:
            time.sleep(25)
            n += 1
            return self.running_main(p_data, n=n, label=label)
        data, pageOrTotal = self.parse_html(html, exc=n, label=label)
        if self.scrollwithauto:
            self.label2show = f"第{p_data}页已抓取。\n"
        else:
            self.label2show = f"当前页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的', exc=0):
        """
        exc   额外情况处理
        --json数据： 获取数据总量
        param: page_total
        --html数据： 获取当前页面数量及总页数，计算出page_total
        param: pagecount   总页数
        param: numPerpage  当前页面数量
        return df, page_total
        """
        ########  需要修改的部分
        pageOrTotal = None
        if not exc:
            return None, re.search(fr'<a href="(.*?{self.date}.*?)"[\s\S]+?融资标的证券及保证金比例（{self.date}）', html).group(1)
        try:
            df_json = json.loads(html)
            try:
                pageOrTotal = df_json['total']
                pageOrTotal = pageOrTotal if type(pageOrTotal) == int else int(float(pageOrTotal))
            #                 pageOrTotal = pageOrTotal * 18
            except KeyError:
                pageOrTotal = 0
            df = pd.DataFrame(df_json['data'])
        except json.JSONDecodeError:
            pageOrTotal = 0
            if '<table' in html:
                #                 html = re.search(r'查 询[\s\S]*?<table[\s\S]*?证券代码[\s\S]*?/table>', html).group(0)
                html = re.search(r'<table[\s\S]*?证券代码[\s\S]*?/table>', html).group(0)
                html = re.sub(r'\s+', ' ', html)
                df = pd.read_html(html, header=0)[0].dropna()
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                html1 = re.sub(r'\s+', ' ', html1)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0].dropna()
        #             pageOrTotal = pageOrTotal if pageOrTotal else 2000
        df_columns = {'marketName': '市场', 'stockCode': '证券代码', 'stockName': '证券简称', '融资保证金比例': '融资保证金比例',
                      '融券保证金比例': '融券保证金比例', 'updateDate': '日期', 'converRate': '保证金比例', '保证金比例（%）': '保证金比例',
                      'underlyingType': '标的类别'}
        #         df['融资保证金比例'] = '是'
        #         df['融券保证金比例'] = '是'
        #######################################
        df.rename(columns=df_columns, inplace=True)
        if '保证金比例' in df.columns:
            if '标的类别' in df.columns:
                df = df[df['标的类别'] == label[:2]]
            df.rename(columns={'保证金比例': label[:2] + '保证金比例'}, inplace=True)
        df = trans_data(df, self.date)
        return df, pageOrTotal

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
