import time
import json
import datetime
import numpy as np
import pandas as pd
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = """Host: www.guosen.com.cn
Connection: keep-alive
Content-Length: 33
sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
sec-ch-ua-mobile: ?0
Content-Type: application/x-www-form-urlencoded;charset=UTF-8
Origin: https://www.guosen.com.cn
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Referer: https://www.guosen.com.cn/gs/business/capital_margin_3.html
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9"""


class db(object):
    __slots__ = ['page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    url_ori = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqkcdbzjzq/1.0'
    stock_name = '国信证券'
    post = True
    page_size = 5

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
        df_data = self.running_main(list(self.post_data(N1=page_total))[0])
        df, _ = modify_field(df_data)
        self.data_save(df, self.file_path, label='可担保物')
        self.label2show = '所有信息抓取完毕。'
        return df_data

    # 运行函数
    def running_main(self, p_data):
        time.sleep(np.random.random() * 1.1 + 1)
        #         self.label2show = f"正在抓取第{p_data}页...\n"
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(15)
            return self.running_main(p_data)
        data, page_total = self.parse_html(html)
        #         self.label2show = f"第{p_data}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=20):
        data = {
            'pageSize': N1,
            'pageNo': page_num,
            'rq': '',
            'keyword': '',
        }
        #         data = json.dumps(data)
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)
        page_total = int(df_json['data'][0]['count'].strip(',').strip(' '))
        df = pd.DataFrame(df_json['data'])
        df_columns = {'zqdm': '证券代码', 'zqmc': '证券简称', 'zsl': '折算率', 'rq': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df = df.assign(市场='')
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
    url_rz = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqbdzq/1.0?type=0'
    url_rq = 'https://www.guosen.com.cn/gswz-web/sharebroking/getrzrqbdzq/1.0?type=1'
    stock_name = '国信证券'
    post = True
    page_size = 5

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
        res_text, html_rz = get_html_ajex(self.url_rz, self.post, self.h_txt, p_data,
                                          post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        res_text, html_rq = get_html_ajex(self.url_rq, self.post, self.h_txt, p_data,
                                          post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        html_rz = html_rz.decode('utf-8')
        html_rq = html_rq.decode('utf-8')
        data_rz, page_total_rz = self.parse_html(html_rz)
        data_rq, page_total_rq = self.parse_html(html_rq)
        df_data_rz = self.running_main(self.url_rz, list(self.post_data(N1=page_total_rz))[0])
        df_data_rq = self.running_main(self.url_rq, list(self.post_data(N1=page_total_rq))[0])
        df_rz, _ = modify_field(df_data_rz)
        df_rq, _ = modify_field(df_data_rq)
        self.data_save(df_rz, self.file_path, label='融资标的')
        self.data_save(df_rq, self.file_path, label='融券标的')
        self.label2show = '所有信息抓取完毕。'
        return df_data_rz, df_data_rq

    # 运行函数
    def running_main(self, url, p_data):
        time.sleep(np.random.random() * 1.1 + 1)
        #         self.label2show = f"正在抓取第{p_data['hsPage']}页...\n"
        res_text, html = get_html_ajex(url, self.post, self.h_txt, p_data,
                                       post_data_func=self.post_data)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        try:
            html = html.decode('utf-8')
        except AttributeError:
            time.sleep(15)
            return self.running_main(url, p_data)
        data, page_total = self.parse_html(html)
        #         self.label2show = f"第{p_data['hsPage']}页已抓取。\n"
        time.sleep(np.random.random() + 2.5)
        return data

    # 构造请求数据字典
    def post_data(self, page_num=1, N1=20):
        data = {
            'pageSize': N1,
            'pageNo': page_num,
            'rq': '',
            'keyword': '',
        }
        #         data = json.dumps(data)
        yield data

    # 解析初始网页 json 数据
    def parse_html(self, html):
        df_json = json.loads(html)
        page_total = int(df_json['data'][0]['count'].strip(',').strip(' '))
        df = pd.DataFrame(df_json['data'])
        df_columns = {'sc': '市场', 'zqdm': '证券代码', 'zqmc': '证券简称', 'rzbzjbl': '保证金比例', 'rq': '日期'}
        df.rename(columns=df_columns, inplace=True)
        df['市场'] = df['市场'].map(lambda x: '深圳' if x == '1' else '上海')
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
