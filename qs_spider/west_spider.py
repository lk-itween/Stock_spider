import time
import chardet
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_data, get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.west95582.com
Connection: keep-alive
Content-Length: 273
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
Origin: http://www.west95582.com
Referer: http://www.west95582.com/jdw/rzrq/kcdbzj.jsp?classid=0001000200070001
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    urlo = 'http://www.west95582.com/jdw/rzrq/getKcdbzj.jsp'
    #     url_ = '/main/ProductsMall/rzrq/kcdbzjmdjzsl/index.shtml'
    stock_name = '西部证券'
    post = True
    postbut = False
    scrollwithauto = False
    page_size = 21

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
        self.label2show = '所有信息抓取完毕。'
        return self.df_data

    # 运行函数
    def running_main(self, p_data, referer=None, encoding='utf-8', n=1, label=None):
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
            html = html.decode(chardet.detect(html)['encoding'])
        except AttributeError:
            time.sleep(25)
            n += 1
            return self.running_main(p_data, n)
        data, page_total = self.parse_html(html)
        #         self.label2show = f"第{p_data['currPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, label='830009', N1=page_size):
        datapost = {
            'pageIndex': page_num,
            'pageSize': N1,
        }
        dataget = '&'.join([f'{x}={y}' for x, y in datapost.items()])
        dataget = '&' + dataget if '?' in self.urlo else '?' + dataget
        if self.postbut and self.post:
            self.url_ori = self.urlo + dataget
            yield json.dumps(datapost)
        yield datapost if self.post else dataget

    # 解析初始网页 json 数据
    def parse_html(self, html, label='可担保物', exc=0):
        df_json = json.loads(html)
        page_total = df_json['sum']
        page_total = page_total if type(page_total) == int else int(float(page_total))
        df = pd.DataFrame(df_json['items'])
        df_columns = {'SecMarket': '市场', 'code': '证券代码', 'codename': '证券简称', 'zsl': '折算率',
                      'EffectiveDate': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df = trans_data(df, self.date)
        return df, page_total


class rzrq(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    urlo = 'http://www.west95582.com/jdw/rzrq/getBdzq_bzjbl.jsp'
    #     url_ = '/main/ProductsMall/rzrq/bdzqmdjbzjbl/index.shtml'
    stock_name = '西部证券'
    post = True
    postbut = False
    scrollwithauto = False
    page_size = 21

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
        for label in ['融资标的', '融券标的']:
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
            df, _ = modify_field(self.df_data)
            save_to_file(df, save_path, label=label)
            #             self.data_save(df_rq, self.file_path, label='融券标的')
            self.label2show = f'{label}抓取完毕。'
        self.label2show = '所有信息抓取完毕。'

    #         return self.df_data

    # 运行函数
    def running_main(self, p_data, referer=None, encoding='utf-8', n=1, label='融资标的'):
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
            html = html.decode(chardet.detect(html)['encoding'])
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
    def post_data(self, page_num=1, label='830009', N1=page_size):
        bdzq_map = {'融资标的': '0',
                    '融券标的': '1', }
        datapost = {
            'bdzq': bdzq_map[label],
            'pageIndex': page_num,
            'pageSize': N1,
        }
        dataget = '&'.join([f'{x}={y}' for x, y in datapost.items()])
        dataget = '&' + dataget if '?' in self.urlo else '?' + dataget
        if self.postbut and self.post:
            self.url_ori = self.urlo + dataget
            yield json.dumps(datapost)
        yield datapost if self.post else dataget

    # 解析初始网页 json 数据
    def parse_html(self, html, label='融资标的', exc=0):
        df_json = json.loads(html)
        page_total = df_json['sum']
        page_total = page_total if type(page_total) == int else int(float(page_total))
        df = pd.DataFrame(df_json['items'])
        df_columns = {'SecMarket': '市场', 'code': '证券代码', 'codename': '证券简称', 'bzjbl': label[:2] + '保证金比例',
                      'EffectiveDate': '日期'}
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
