import time
import chardet
import re
import json
import datetime
from functools import partial
import numpy as np
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from .stock_normalization import get_data, get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.ajzq.com
Connection: keep-alive
sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
sec-ch-ua-mobile: ?0
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'https://www.ajzq.com/service/assurecodes'
    stock_name = '爱建证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url，仅当post为True时生效
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 10  # 页面数据量大小

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
    def post_data(self, page_num=1, label='830009', N1=page_size):
        """ post: data参数
        每次重新编辑需要修改
        """
        datapost = {
            'page_no': page_num,
            'page_size': N1,
        }
        dataget = '&'.join([f'{x}={y}' for x, y in datapost.items()])
        dataget = '&' + dataget if '?' in self.urlo else '?' + dataget
        if self.postbut and self.post:
            self.url_ori = self.urlo + dataget
            yield json.dumps(datapost)
        yield datapost if self.post else dataget

    # 多线程，设置线程池
    def thread_main(self):
        referer = 'https://www.ajzq.com/main/rzrq/detail.html?p=3'
        p_data = list(self.post_data(page_num=2))[0]
        label = '可担保物'
        save_path = f'{self.file_path}/{self.stock_name}{self.date}.xlsx'
        data = get_data(self.url_ori, self.post, self.h_txt, p_data, self.page_start, self.page_end,
                        self.scrollwithauto, referer=referer, label=label,
                        running_func=self.running_main, post_data_func=self.post_data, parse_html_func=self.parse_html)
        if data.empty:
            self.label2show = '数据抓取失败。\n'
            return data
        self.df_data = data
        self.df_data, _ = modify_field(self.df_data)
        self.df_data = save_to_file(self.df_data, save_path, label=label)
        self.label2show = '可担保物信息抓取完毕。\n'
        return self.df_data

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
        if 'Error' in html and not self.scrollwithauto:
            df_list = []
            sub_par = partial(self.runningerror, referer, encoding=encoding, n=1)
            with ThreadPool(4) as pool:
                df_list += pool.map(sub_par, range(1, p_data // 500 + 2, 1))
            data = pd.concat(df_list, ignore_index=True)
            self.label2show = f"当前页已抓取。\n"
            time.sleep(np.random.random() + 2.5)
            return data
        data, pageOrTotal = self.parse_html(html, exc=n)
        if self.scrollwithauto:
            self.label2show = f"第{p_data}页已抓取。\n"
        else:
            self.label2show = f"当前页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # Error running
    # 'Error' in html 时中运行
    def runningerror(self, p_data, referer, encoding='utf-8', n=1):
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        data_nump = list(self.post_data(page_num=p_data, N1=500))[0]
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
            return self.runningerror(p_data, referer, encoding=encoding, n=n)
        data, pageOrTotal = self.parse_html(html, exc=n)
        return data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='可担保物', exc=0):
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
                if not self.scrollwithauto:
                    pageOrTotal = pageOrTotal if pageOrTotal * self.page_size >= 10000 else pageOrTotal * self.page_size
            except (KeyError, TypeError, IndexError) as e:
                pageOrTotal = 0
            df = pd.DataFrame(df_json['data'])
        except json.JSONDecodeError:
            html = re.sub(r'\s+', ' ', html)
            pagecount = re.search(r'pagecount:(\d+)', html).group(1)
            pagecount = pagecount if type(pagecount) == int else int(float(pagecount))
            pageOrTotal = pagecount * self.page_size
            if '<table' in html:
                html = re.search(r'<table[\s\S]*?代码[\s\S]*?/table>', html).group(0)
                df = pd.read_html(html, header=0)[0]
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0]
            df_use_col = df.columns[[1, 2, 3]]
            df.dropna(subset=df_use_col, inplace=True)

        df_columns = {'exchangeType': '市场', 'stockCode': '证券代码', 'stockName': '证券简称', 'assureRatio': '折算率',
                      'modifyDate': '日期', 'assureStatus': '状态', }
        df.rename(columns=df_columns, inplace=True)
        df = df[df['状态'] == '0']
        #######################################
        df = trans_data(df, self.date)
        return df, pageOrTotal


class rzrq(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'https://www.ajzq.com/service/underlycodes'
    stock_name = '爱建证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 10  # 页面数据量大小
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
        rzrq_dict = {'融资标的': 2,
                     '融券标的': 3,
                     '融资融券': '830009', }
        datapost = {
            'page_no': page_num,
            'page_size': N1,
        }
        dataget = '&'.join([f'{x}={y}' for x, y in datapost.items()])
        dataget = '&' + dataget if '?' in self.urlo else '?' + dataget
        if self.postbut and self.post:
            self.url_ori = self.urlo + dataget
            yield json.dumps({})
        yield datapost if self.post else dataget

    # 多线程，设置线程池
    def thread_main(self):
        df_list = []
        ########  需要修改的部分
        rzrq = self.combineflag[2]
        df_rz = df_rq = pd.DataFrame()
        referer = 'https://www.ajzq.com/main/rzrq/detail.html?p=3'
        save_path = f'{self.file_path}/{self.stock_name}{self.date}.xlsx'
        #######################################
        for label in rzrq:
            p_data = list(self.post_data(label=label))[0]
            data = get_data(self.url_ori, self.post, self.h_txt, p_data, self.page_start, self.page_end,
                            self.scrollwithauto, referer=referer, label=label,
                            running_func=self.running_main, post_data_func=self.post_data,
                            parse_html_func=self.parse_html)
            if data.empty:
                self.label2show = '数据抓取失败。\n'
                return data, pd.DataFrame()
            self.df_data = data
            if len(rzrq) == 2:
                self.df_data = self.df_data[['市场', '证券代码', '证券简称', label[:2] + '保证金比例', '日期']]
            df_rz, df_rq = modify_field(self.df_data)
            if df_rq.empty:
                df_rz = save_to_file(df_rz, save_path, label=label)
                self.label2show = f'{self.stock_name}{label}信息抓取完毕。\n'
            else:
                df_rz = save_to_file(df_rz, save_path, label='融资标的')
                df_rq = save_to_file(df_rq, save_path, label='融券标的')
                self.label2show = f'{self.stock_name}融资标的信息抓取完毕。\n{self.stock_name}融券标的信息抓取完毕。\n'
            self.label2show = f'{self.stock_name}融资融券信息抓取完毕。\n'
        return df_rz, df_rq

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
        if 'Error' in html and not self.scrollwithauto:
            df_list = []
            sub_par = partial(self.runningerror, referer, encoding=encoding, n=1, label=label)
            with ThreadPool(4) as pool:
                df_list += pool.map(sub_par, range(1, p_data // 500 + 2, 1))
            data = pd.concat(df_list, ignore_index=True)
            self.label2show = f"当前页已抓取。\n"
            time.sleep(np.random.random() + 2.5)
            return data
        data, pageOrTotal = self.parse_html(html, exc=n, label=label)
        if self.scrollwithauto:
            self.label2show = f"第{p_data}页已抓取。\n"
        else:
            self.label2show = f"当前页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # Error running
    # 'Error' in html 时中运行
    def runningerror(self, p_data, referer, encoding='utf-8', n=1, label='融资标的'):
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        time.sleep(np.random.randint(5))
        data_nump = list(self.post_data(page_num=p_data, N1=500))[0]
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
            return self.runningerror(p_data, referer, encoding=encoding, n=n, label=label)
        data, pageOrTotal = self.parse_html(html, exc=n)
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
                if not self.scrollwithauto:
                    pageOrTotal = pageOrTotal if pageOrTotal * self.page_size >= 10000 else pageOrTotal * self.page_size
            except (KeyError, TypeError, IndexError) as e:
                pageOrTotal = 0
            df = pd.DataFrame(df_json['data'])
        except json.JSONDecodeError:
            html = re.sub(r'\s+', ' ', html)
            pagecount = re.search(r'pagecount:(\d+)', html).group(1)
            pagecount = pagecount if type(pagecount) == int else int(float(pagecount))
            pageOrTotal = pagecount * self.page_size
            if '<table' in html:
                html = re.search(r'<table[\s\S]*?代码[\s\S]*?/table>', html).group(0)
                df = pd.read_html(html, header=0)[0]
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0]
            df_use_col = df.columns[[1, 2, 3]]
            df.dropna(subset=df_use_col, inplace=True)
        if df.empty:
            return pd.DataFrame(), 0
        df_columns = {'exchangeType': '市场', 'stockCode': '证券代码', 'stockName': '证券简称', 'finRatio': '融资保证金比例',
                      '融券保证金比例': '融券保证金比例', 'modifyDate': '日期', 'ratio': '保证金比例', '保证金比例（%）': '保证金比例',
                      'underlyingType': '标的类别', 'finStatus': '状态'}
        df.rename(columns=df_columns, inplace=True)
        df = df[df['状态'] == '0']
        if '保证金比例' in df.columns:
            if '标的类别' in df.columns:
                df = df[df['标的类别'] == label[:2]]
            df.rename(columns={'保证金比例': label[:2] + '保证金比例'}, inplace=True)

        #######################################
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
        db_df = run.thread_main()
        rz_df, rq_df = run1.thread_main()
        # return db_df, rz_df, rq_df


