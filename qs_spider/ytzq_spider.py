import time
import re
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_data, get_html_ajex, trans_data, modify_field, save_to_file

import warnings

warnings.filterwarnings("ignore")

h_txt = '''Host: www.ytzq.com
Connection: keep-alive
Content-Length: 63
sec-ch-ua: "Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
sec-ch-ua-mobile: ?0
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
Origin: https://www.ytzq.com
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Referer: https://www.ytzq.com/web/html/introduction/ywjs.html
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9'''


class db(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'https://www.ytzq.com/web/bus/json'
    stock_name = '银泰证券'
    post = True  # 数据请求方式
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 15  # 页面数据量大小

    #######################################

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.url_ori = self.urlo
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

        label = '可担保物'
        save_path = f'{self.file_path}/{self.stock_name}{self.date}.xlsx'
        data = get_data(self.url_ori, self.post, self.h_txt, p_data, self.page_start, self.page_end,
                        self.scrollwithauto, label=label,
                        running_func=self.running_main, post_data_func=self.post_data, parse_html_func=self.parse_html)
        if data.empty:
            self.label2show = '数据抓取失败。\n'
            return data
        self.df_data = data
        self.df_data, _ = modify_field(self.df_data)
        save_to_file(self.df_data, save_path, label='可担保物')
        self.label2show = '可担保物信息抓取完毕。\n'
        return self.df_data

    # 运行函数
    def running_main(self, p_data, referer=None, encoding='utf-8', n=1, label=None):
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
            'curPage': page_num,
            'numPerPage': N1,
            'funcNo': label,
            'type': '4',
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
            df_json = json.loads(html)['results'][0]
            try:
                pageOrTotal = df_json['totalRows']
                pageOrTotal = pageOrTotal if type(pageOrTotal) == int else int(float(pageOrTotal))
            except KeyError:
                pageOrTotal = 0
            df = pd.DataFrame(df_json['data'])
        except json.JSONDecodeError:
            pagecount = re.search(r'pagecount:(\d+)', html).group(1)
            pagecount = pagecount if type(pagecount) == int else int(float(pagecount))
            numPerPage = re.search(r'numPerPage:(\d+)', html).group(1)
            numPerPage = pagecount if type(numPerPage) == int else int(float(numPerPage))
            pageOrTotal = pagecount * numPerPage
            if '<table' in html:
                html = re.search(r'<table[\s\S]*?/table>', html).group(0)
                html = re.sub(r'\s+', ' ', html)
                df = pd.read_html(html, header=0)[0].dropna()
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                html1 = re.sub(r'\s+', ' ', html1)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0].dropna()
            pageOrTotal = pageOrTotal if pageOrTotal else 2000

        df_columns = {'bourse': '市场', 'stockcode': '证券代码', 'stockname': '证券简称', 'margin': '折算率',
                      'insert_date': '日期'}
        #######################################
        df.rename(columns=df_columns, inplace=True)
        df = trans_data(df, self.date)
        return df, pageOrTotal


class rzrq(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'https://www.ytzq.com/web/bus/json'
    stock_name = '银泰证券'
    post = True  # 数据请求方式
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 15  # 页面数据量大小
    combineflag = {0: ['融资融券'],
                   1: ['融资标的', '融券标的'],
                   2: ['融资标的'],
                   3: ['融券标的'],
                   }  # 融资融券展示方式

    #######################################

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.url_ori = self.urlo
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
        rzrq_dict = {'融资标的': 0,
                     '融券标的': 'rq',
                     '融资融券': '101202', }
        datapost = {
            'curPage': page_num,
            'numPerPage': N1,
            'funcNo': rzrq_dict[label],
            'type': '3',
        }
        dataget = '?' + '&'.join([f'{x}={y}' for x, y in datapost.items()])
        yield datapost if self.post else dataget

    # 多线程，设置线程池
    def thread_main(self):
        ########  需要修改的部分
        rzrq = self.combineflag[0]
        save_path = f'{self.file_path}/{self.stock_name}{self.date}.xlsx'
        #######################################
        for label in rzrq:
            p_data = list(self.post_data(label=label))[0]
            data = get_data(self.url_ori, self.post, self.h_txt, p_data, self.page_start, self.page_end,
                            self.scrollwithauto, label=label,
                            running_func=self.running_main, post_data_func=self.post_data,
                            parse_html_func=self.parse_html)
            if data.empty:
                self.label2show = '数据抓取失败。\n'
                return data
            self.df_data = data
            df_rz, df_rq = modify_field(self.df_data)
            if df_rq.empty:
                save_to_file(df_rz, save_path, label=label)
                self.label2show = f'{self.stock_name}{label}信息抓取完毕。\n'
            else:
                save_to_file(df_rz, save_path, label='融资标的')
                save_to_file(df_rq, save_path, label='融券标的')
                self.label2show = f'{self.stock_name}融资标的信息抓取完毕。\n{self.stock_name}融券标的信息抓取完毕。\n'
            self.label2show = f'{self.stock_name}融资融券信息抓取完毕。\n'

    #         return self.df_data

    # 运行函数
    def running_main(self, p_data, referer=None, encoding='utf-8', n=1, label='融资标的'):
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
            df_json = json.loads(html)['results'][0]
            try:
                pageOrTotal = df_json['totalRows']
                pageOrTotal = pageOrTotal if type(pageOrTotal) == int else int(float(pageOrTotal))
            except KeyError:
                pageOrTotal = 0
            df = pd.DataFrame(df_json['data'])
        except json.JSONDecodeError:
            pagecount = re.search(r'pagecount:(\d+)', html).group(1)
            pagecount = pagecount if type(pagecount) == int else int(float(pagecount))
            numPerPage = re.search(r'numPerPage:(\d+)', html).group(1)
            numPerPage = pagecount if type(numPerPage) == int else int(float(numPerPage))
            pageOrTotal = pagecount * numPerPage
            if '<table' in html:
                html = re.search(r'<table[\s\S]*?/table>', html).group(0)
                html = re.sub(r'\s+', ' ', html)
                df = pd.read_html(html, header=0)[0].dropna()
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                html1 = re.sub(r'\s+', ' ', html1)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0].dropna()
            pageOrTotal = pageOrTotal if pageOrTotal else 2000
        df_columns = {'bourse': '市场', 'stockcode': '证券代码', 'stockname': '证券简称', 'rzbd': '融资保证金比例',
                      'rqbd': '融券保证金比例', 'fbrq': '日期', '保证金比例': '保证金比例',
                      'undersecType': '标的类别'}
        df['融资保证金比例'] = '是'
        df['融券保证金比例'] = '是'
        #######################################
        df.rename(columns=df_columns, inplace=True)
        if '保证金比例' in df.columns:
            if '标的类别' in df.columns:
                df = df[df['标的类别'] == label[:2]]
            df.rename(columns={'保证金比例': label[:2] + '保证金比例'}, inplace=True)
        df = trans_data(df, self.date)
        return df, pageOrTotal


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
