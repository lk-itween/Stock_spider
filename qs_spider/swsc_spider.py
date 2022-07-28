import time
import chardet
import re
import json
import datetime
from functools import partial
import numpy as np
import pandas as pd
from .stock_normalization import get_data, get_html_ajex, trans_data, modify_field, save_to_file
from multiprocessing.dummy import Pool as ThreadPool
import warnings

warnings.filterwarnings("ignore")

h_txt = '''Host: www.swsc.com.cn
Connection: keep-alive
sec-ch-ua: " Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"
sec-ch-ua-mobile: ?0
Upgrade-Insecure-Requests: 1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: navigate
Sec-Fetch-User: ?1
Sec-Fetch-Dest: document
Referer: https://www.swsc.com.cn/business/deal/rzrq/pubInfo/offset/index_3058.jhtml
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9'''


class db(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date_', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'https://www.swsc.com.cn/business/deal/rzrq/pubInfo/offset/index_'
    stock_name = '西南证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = True  # 是否翻页抓取数据
    page_size = 10  # 页面数据量大小

    #######################################

    def __init__(self, page_start: str, page_end: str, path: str, datestr=None):
        self.url_ori = self.urlo
        self.page_end = page_end
        self.page_start = page_start
        self.file_path = path
        self.date_ = self.date0
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 构造请求数据字典
    def post_data(self, page_num=1, label='2', N1=page_size):
        """ post: data参数
        每次重新编辑需要修改
        """
        yield str(page_num) + '.jhtml'

    # 多线程，设置线程池
    def thread_main(self, timedelta=0):
        p_data = list(self.post_data(page_num=1))[0]
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
    def running_main(self, p_data, referer=None, pn=1, encoding='utf-8', n=1, label=None):
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
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, data_nump,
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
            return self.running_main(p_data, pn=pn, encoding=encoding, n=n)
        if 'Error' in html and self.scrollwithauto == False:
            #             time.sleep(20)
            #             return self.running_main(p_data, pn=pn, encoding=encoding, n=n)
            df_list = []
            sub_par = partial(self.runningerror, encoding=encoding, n=1)
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
    def runningerror(self, p_data, encoding='utf-8', n=1):
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        data_nump = list(self.post_data(page_num=p_data, N1=500))[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, data_nump,
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
            return self.runningerror(p_data, encoding=encoding, n=n)
        data, pageOrTotal = self.parse_html(html, exc=n)
        return data

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
            df_json = json.loads(html)['result']
            try:
                pageOrTotal = df_json['allSize']
                pageOrTotal = pageOrTotal if type(pageOrTotal) == int else int(float(pageOrTotal))
                if not self.scrollwithauto:
                    pageOrTotal = pageOrTotal if pageOrTotal * self.page_size >= 10000 else pageOrTotal * self.page_size
            except (KeyError, TypeError, IndexError) as e:
                pageOrTotal = 100000
            df = pd.DataFrame(df_json['data'])
        except json.JSONDecodeError:
            html = re.sub(r'\s+', ' ', html)
            pagecount = re.search(r'共 ?(\d+)页', html).group(1)
            pagecount = pagecount if type(pagecount) == int else int(float(pagecount))
            pageOrTotal = pagecount
            if '<table' in html:
                #                 html = re.search(r'查 询[\s\S]*?<table w[\s\S]*?证券代码[\s\S]*?/table>', html).group(0)
                html = re.search(r'<table[\s\S]*?代码[\s\S]*?/table>', html).group(0)
                html = re.sub('<!--[\s\S]*?-->', '', html)
                if '是' not in html:
                    return pd.DataFrame(), pageOrTotal
                df = pd.read_html(html, header=0)[0]
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0]
        df_columns = {'marketCode': '市场', '证券代码': '证券代码', '证券简称': '证券简称', '折算率': '折算率',
                      'etlDate': '日期', '可充抵保证金证券': '状态', }
        df.rename(columns=df_columns, inplace=True)
        df = df[df['状态'] == '是']
        #######################################
        df = trans_data(df, self.date)
        return df, pageOrTotal


class rzrq(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date_', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'https://www.swsc.com.cn/business/deal/rzrq/pubInfo/target/index_'
    stock_name = '西南证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = True  # 是否翻页抓取数据
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
        self.date_ = self.date0
        self.date = self.date1 if not datestr else datestr
        self.h_txt = h_txt
        self.label2show = '开始抓取信息...\n'

    # 构造请求数据字典
    def post_data(self, page_num=1, label='融资融券', N1=page_size):
        """ post: data参数
        每次重新编辑需要修改
        """
        yield str(page_num) + '.jhtml'

    # 多线程，设置线程池
    def thread_main(self, timedelta=0):
        ########  需要修改的部分
        rzrq = self.combineflag[1]
        for label in rzrq:
            p_data = list(self.post_data(label=label))[0]
            save_path = f'{self.file_path}/{self.stock_name}{self.date}.xlsx'
            data = get_data(self.url_ori, self.post, self.h_txt, p_data, self.page_start, self.page_end,
                            self.scrollwithauto, label=label,
                            running_func=self.running_main, post_data_func=self.post_data,
                            parse_html_func=self.parse_html)
            if data.empty:
                self.label2show = '数据抓取失败。\n'
                return data
            self.df_data = data
            if len(rzrq) == 2:
                self.df_data = self.df_data[['市场', '证券代码', '证券简称', label[:2] + '保证金比例', '日期']]
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
        except (AttributeError, UnicodeDecodeError) as e:
            encoding = chardet.detect(html)['encoding']
            encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] <= 0.99 else encoding
            time.sleep(25)
            n += 1
            return self.running_main(p_data, encoding=encoding, n=n, label=label)
        if 'Error' in html and self.scrollwithauto == False:
            df_list = []
            sub_par = partial(self.runningerror, encoding=encoding, n=1, label=label)
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
    def runningerror(self, p_data, encoding='utf-8', n=1, label='融资标的', jys='1'):
        self.label2show = f'第{n}次尝试\n'
        if n >= 5:
            return None
        time.sleep(np.random.random() * 1.1 + 1)
        time.sleep(np.random.randint(5))
        data_nump = list(self.post_data(page_num=p_data, N1=500))[0]
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, data_nump,
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
            return self.runningerror(p_data, encoding=encoding, n=n, label=label, jys=jys)
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
            df_json = json.loads(html)['result']
            try:
                pageOrTotal = df_json['allSize']
                pageOrTotal = pageOrTotal if type(pageOrTotal) == int else int(float(pageOrTotal))
                if not self.scrollwithauto:
                    pageOrTotal = pageOrTotal if pageOrTotal * self.page_size >= 10000 else pageOrTotal * self.page_size
            except (KeyError, TypeError, IndexError) as e:
                pageOrTotal = 200000
            df = pd.DataFrame(df_json['data'])
        except json.JSONDecodeError:
            html = re.sub(r'\s+', ' ', html)
            pagecount = re.search(r'共 ?(\d+)页', html).group(1)
            pagecount = pagecount if type(pagecount) == int else int(float(pagecount))
            pageOrTotal = pagecount
            if '<table' in html:
                html = re.search(r'<table[\s\S]*?代码[\s\S]*?/table>', html).group(0)
                html = re.sub('<!--[\s\S]*?-->', '', html)
                if '是' not in html:
                    return pd.DataFrame(), pageOrTotal
                df = pd.read_html(html, header=0)[0]
            else:
                html1 = re.search(r'[\s\S]+<tr>', html).group(0)
                df = pd.read_html('<table>' + html1 + '</table>', header=0)[0]
        if df.empty:
            return pd.DataFrame(), 0
        df.columns = [i.strip() for i in df.columns]
        df_columns = {'marketCode': '市场', '证券代码': '证券代码', '证券简称': '证券简称', '融资保证金比例': '融资保证金比例',
                      '融券保证金比例': '融券保证金比例', 'etlDate': '日期', 'BAIL_RATIOO': '保证金比例', '保证金比例（%）': '保证金比例',
                      'FLAG': '标的类别', '融资标的': '融资状态', '融券标的': '融券状态', }
        #         df['融资保证金比例'] = '是'
        #         df['融券保证金比例'] = '是'
        df.rename(columns=df_columns, inplace=True)
        df = df[(df['融资状态'] == '是') | (df['融券状态'] == '是')]
        #######################################
        if '保证金比例' in df.columns:
            if '标的类别' in df.columns:
                df = df[df['标的类别'].str.contains(label[:2])]
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
