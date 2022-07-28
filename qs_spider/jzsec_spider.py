import time
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_data, get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.jzsec.com
Connection: keep-alive
Content-Length: 58
sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
sec-ch-ua-mobile: ?0
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
Origin: https://www.jzsec.com
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Referer: https://www.jzsec.com/business/business_xinyong/rzrq/gsxx/kechongdi/index.shtml
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    urlo = 'http://www.jzsec.com/servlet/json'  # {date}
    stock_name = '九州证券'
    post = True
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url，仅当post为True时生效
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 10

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
        p_data = list(self.post_data(N1=1))[0]
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
        save_to_file(self.df_data, save_path, label=label)
        self.label2show = '可担保物信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data, referer=None, encoding='utf-8', n=1, label=None):
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
            return self.running_main(p_data)
        data, page_total = self.parse_html(html)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, label='830009', N1=page_size):
        """ post: data参数
        每次重新编辑需要修改
        """
        datapost = {'funcNo': '160504',
                    'pagenow': page_num,
                    'pagenum': N1, }
        dataget = '&'.join([f'{x}={y}' for x, y in datapost.items()])
        dataget = '&' + dataget if '?' in self.urlo else '?' + dataget
        if self.postbut and self.post:
            self.url_ori = self.urlo + dataget
            yield json.dumps(datapost)
        yield datapost if self.post else dataget

    # 解析初始网页 json 数据
    def parse_html(self, html, label='可担保物', exc=0):
        df_json = json.loads(html)['results'][0]
        page_total = df_json['totalRows'] if type(df_json['totalRows']) == int else int(
            df_json['totalRows'].strip(',').strip(' '))
        df = pd.DataFrame(df_json['data'])
        df_columns = {'stock_code': '证券代码', 'stock_name': '证券简称', 'conversion_rate': '折算率', }
        df.rename(columns=df_columns, inplace=True)
        df.replace(r'^$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)
        df = df.assign(市场='')
        df = df.assign(日期=self.date)
        df = trans_data(df, self.date)
        return df, page_total


class rzrq(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    urlo = 'http://www.jzsec.com/servlet/json'
    stock_name = '九州证券'
    post = True
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url，仅当post为True时生效
    scrollwithauto = False  # 是否翻页抓取数据
    page_size = 10

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
        datapost = {'funcNo': '160503',
                    'pagenow': page_num,
                    'pagenum': N1, }
        dataget = '&'.join([f'{x}={y}' for x, y in datapost.items()])
        dataget = '&' + dataget if '?' in self.urlo else '?' + dataget
        if self.postbut and self.post:
            self.url_ori = self.urlo + dataget
            yield json.dumps(datapost)
        yield datapost if self.post else dataget

    # 多线程，设置线程池
    def thread_main(self):
        label = '融资标的'
        p_data = list(self.post_data(N1=1))[0]
        save_path = f'{self.file_path}/{self.stock_name}{self.date}.xlsx'
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
        return self.df_data

    # 运行函数
    def running_main(self, p_data, referer=None, encoding='utf-8', n=3, label='融资标的'):
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
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(25)
            return self.running_main(p_data=p_data, referer=referer, encoding=encoding, n=n, label=label)
        data, page_total = self.parse_html(html, label=label)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的', exc=0):
        df_json = json.loads(html)['results'][0]
        page_total = df_json['totalRows'] if type(df_json['totalRows']) == int else int(
            df_json['totalRows'].strip(',').strip(' '))
        df = pd.DataFrame(df_json['data'])
        df_columns = {'stock_code': '证券代码', 'stock_name': '证券简称',
                      'rz_proportion': '融资保证金比例', 'rq_proportion': '融券保证金比例'
                      }
        df.rename(columns=df_columns, inplace=True)
        df.replace(r'^$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)
        df = df.assign(市场='')
        df = df.assign(日期=self.date)
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
