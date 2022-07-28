import time
import chardet
import re
import datetime
import numpy as np
import pandas as pd
import pdfplumber
from .stock_normalization import get_html_ajex, trans_data, modify_field, save_to_file
import warnings

warnings.filterwarnings("ignore")

h_txt = '''sec-ch-ua: " Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"
sec-ch-ua-mobile: ?0
Upgrade-Insecure-Requests: 1
Referer: https://www.daton.com.cn/?q=dt_panel_fwzx/258/258'''


class db(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date_', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'https://www.daton.com.cn/?q=dt_panel_fwzx/84'
    stock_name = '大通证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
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

    # 多线程，设置线程池
    def thread_main(self, timedelta=0):
        df_list = []
        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        encoding = chardet.detect(html)['encoding']
        encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] <= 0.99 else encoding
        html = html.decode(encoding)
        if '没有相关数据' in html:
            timedelta += 1
            self.date_ = self.date_ + datetime.timedelta(days=-timedelta)
            self.thread_main(timedelta=timedelta)
            return None
        pdf_url = self.get_pdf_url(html)
        time.sleep(2)
        res_text, pdf_html = get_html_ajex(pdf_url, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        with open(f'{self.file_path}/{self.stock_name}{self.date}.pdf', 'wb') as file:
            file.write(pdf_html)

        with pdfplumber.open(f'{self.file_path}/{self.stock_name}{self.date}.pdf') as pdf:
            pages = pdf.pages
            table_settings = {
                'vertical_strategy': "lines",
                "horizontal_strategy": "lines",
            }
            pdf_shape = pd.DataFrame(pages[0].extract_table(table_settings)).shape[1]
            for i in pages:
                df_list.append(self.running_main(i, pdf_shape))
        self.df_data = pd.concat(df_list)
        self.df_data = self.parse_html(self.df_data)
        ######
        self.df_data, _ = modify_field(self.df_data)
        self.data_save(self.df_data, self.file_path, label='可担保物')
        self.label2show = '可担保物信息抓取完毕。\n'
        return self.df_data

    # 运行函数
    def running_main(self, pdf_page, df_shape=6):
        table_settings = {
            'vertical_strategy': "lines",
            "horizontal_strategy": "lines",
        }
        data = pd.DataFrame(pdf_page.extract_table(table_settings))
        if data.shape[1] == df_shape:
            return data
        else:
            return pd.DataFrame()

    # 请求落地页
    def get_pdf_url(self, html, date=date0):
        date = date.strftime('%Y-%m-%d')
        if not html:
            return None
        url = re.search(fr'href="(.*?)">可充抵保证金证券表（{date}）', html)
        if not url:
            return self.get_pdf_url(html,
                                    date=datetime.datetime.strptime(date, '%Y-%m-%d') + datetime.timedelta(days=-1))
        return url.group(1)

    # 解析初始网页 json 数据
    def parse_html(self, df):
        """
        解析数据
        """
        df = df.reset_index(drop=True).T.set_index(0).T
        df_columns = {'交易所': '市场', '证券代码': '证券代码', '证券简称': '证券简称', '公司折算率': '折算率',
                      'etlDate': '日期', '可充抵保证金证券': '状态', }
        df.rename(columns=df_columns, inplace=True)
        #######################################
        df['证券代码'] = df['证券代码'].map(str)
        df = df[df['证券代码'].str.contains(r'\d+')]
        df = trans_data(df, self.date)
        return df

    # 数据储存为 excel 格式
    def data_save(self, df, file_path, label='融资标的'):
        dfc = df.copy()
        path_name = f'{file_path}/{self.stock_name}{self.date}.xlsx'
        dfc = save_to_file(dfc, path_name, label)
        return dfc


class rzrq(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date_', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'https://www.daton.com.cn/?q=dt_panel_fwzx/84'
    stock_name = '大通证券'
    post = False  # 数据请求方式
    postbut = False  # 数据请求方式 为 post但是要按get方式构建url
    scrollwithauto = False  # 是否翻页抓取数据
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

    # 多线程，设置线程池
    def thread_main(self, timedelta=0):
        df_list = []

        res_text, html = get_html_ajex(self.url_ori, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        encoding = chardet.detect(html)['encoding']
        encoding = 'gbk' if encoding == 'GB2312' and chardet.detect(html)['confidence'] <= 0.99 else encoding
        html = html.decode(encoding)
        if '没有相关数据' in html:
            timedelta += 1
            self.date_ = self.date_ + datetime.timedelta(days=-timedelta)
            self.thread_main(timedelta=timedelta)
            return None
        pdf_url = self.get_pdf_url(html)
        time.sleep(2)
        res_text, pdf_html = get_html_ajex(pdf_url, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        with open(f'{self.file_path}/{self.stock_name}{self.date}.pdf', 'wb') as file:
            file.write(pdf_html)

        with pdfplumber.open(f'{self.file_path}/{self.stock_name}{self.date}.pdf') as pdf:
            pages = pdf.pages
            table_settings = {
                'vertical_strategy': "lines",
                "horizontal_strategy": "lines",
            }
            pdf_shape = pd.DataFrame(pages[0].extract_table(table_settings)).shape[1]
            for i in pages:
                df_list.append(self.running_main(i, pdf_shape))
        self.df_data = pd.concat(df_list)
        self.df_data = self.parse_html(self.df_data)

        df_rz, df_rq = modify_field(self.df_data)
        if df_rq.empty:
            self.data_save(df_rz, self.file_path, label='融资标的')
            self.label2show = f'{self.stock_name}融资标的信息抓取完毕。\n'
        else:
            self.data_save(df_rz, self.file_path, label='融资标的')
            self.data_save(df_rq, self.file_path, label='融券标的')
            self.label2show = f'{self.stock_name}融资标的信息抓取完毕。\n{self.stock_name}融券标的信息抓取完毕。\n'
        self.label2show = f'{self.stock_name}融资融券信息抓取完毕。\n'

    # 运行函数
    def running_main(self, pdf_page, df_shape=6):
        table_settings = {
            'vertical_strategy': "lines",
            "horizontal_strategy": "lines",
        }
        data = pd.DataFrame(pdf_page.extract_table(table_settings))
        if data.shape[1] == df_shape:
            return data
        else:
            return pd.DataFrame()

    # 请求落地页
    def get_pdf_url(self, html, date=date0):
        date = date.strftime('%Y-%m-%d')
        if not html:
            return None
        url = re.search(fr'href="(.*?)">融资融券标的证券名单（{date}）', html)
        if not url:
            return self.get_pdf_url(html,
                                    date=datetime.datetime.strptime(date, '%Y-%m-%d') + datetime.timedelta(days=-1))
        return url.group(1)

    # 解析初始网页 json 数据
    def parse_html(self, df):
        """
        解析数据
        """
        df = df.reset_index(drop=True).T.set_index(0).T
        df.columns = [i.strip() for i in df.columns]
        df_columns = {'市场': '市场', '证券代码': '证券代码', '证券简称': '证券简称', '融资保证金比例': '融资保证金比例',
                      '融券保证金比例': '融券保证金比例', }
        df.rename(columns=df_columns, inplace=True)
        #######################################
        df['证券代码'] = df['证券代码'].map(str)
        df = df[df['证券代码'].str.contains(r'\d+')]
        df = trans_data(df, self.date)
        return df

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
