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

h_txt = '''Host: www.hongtastock.com
Connection: keep-alive
Upgrade-Insecure-Requests: 1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
Referer: http://www.hongtastock.com/selevlpage.aspx?root=100052
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9'''


class db(object):
    __slots__ = ['url_ori', 'page_end', 'page_start', 'df_data', 'file_path', 'label2show', 'date_', 'date', 'h_txt']
    np.set_printoptions(suppress=True, threshold=1000)
    pd.set_option('display.max_columns', 10000, 'display.max_rows', 10000, 'display.float_format', lambda x: '%.2f' % x)

    date0 = datetime.datetime.now()
    date1 = date0.strftime('%Y%m%d')
    ########  需要修改的部分
    urlo = 'http://www.hongtastock.com/list.aspx?serial=100102'
    stock_name = '红塔证券'
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
        chart = chardet.detect(html)
        encoding = 'gbk' if chart['encoding'] == 'GB2312' and chart['confidence'] <= 0.99 else chart['encoding']
        html = html.decode(encoding)
        if '没有相关数据' in html:
            timedelta += 1
            self.date_ = self.date_ + datetime.timedelta(days=-timedelta)
            self.thread_main(timedelta=timedelta)
            return None
        pdf_url1, html_date = self.get_pdf_url(html, flag=1)
        time.sleep(2)
        res_text, pdf_html1 = get_html_ajex(pdf_url1, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        chart = chardet.detect(pdf_html1)
        encoding = 'gbk' if chart['encoding'] == 'GB2312' and chart['confidence'] <= 0.99 else chart['encoding']
        pdf_html1 = pdf_html1.decode(encoding)
        time.sleep(2)
        pdf_url2, html_date = self.get_pdf_url(pdf_html1, flag=2, date=html_date)
        time.sleep(2)
        res_text, pdf_html2 = get_html_ajex(pdf_url2, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
        with open(f'{self.file_path}/{self.stock_name}{self.date}.pdf', 'wb') as file:
            file.write(pdf_html2)

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
    def get_pdf_url(self, html, flag=1, date=date0):
        if (datetime.datetime.now() - date).days > 15:
            return None
        url_head = 'http://www.hongtastock.com/'
        if not html:
            return None
        url = None
        if flag == 1:
            date = date.strftime('%Y-%m-%d')
            url = re.search(fr'href=\'(.*?)\'>可充抵保证金证券</a>[\S\s]+?{date}</span', html)
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
        elif flag == 2:
            date = date.strftime('%Y%m%d')
            url = re.search(fr'href="(.*?)" target="_blank">附件1：可充抵保证金证券（{date}）.pdf', html)
            date = datetime.datetime.strptime(date, '%Y%m%d')
        if not url:
            return self.get_pdf_url(html, flag, date=date + datetime.timedelta(days=-1))
        return url_head + url.group(1), date

    # 解析初始网页 json 数据
    def parse_html(self, df):
        """
        解析数据
        """
        df = df.reset_index(drop=True).T.set_index(0).T
        df.columns = [i.strip() for i in df.columns]
        df_columns = {'市场': '市场', '证券代码': '证券代码', '证券名称': '证券简称', '折算率': '折算率',
                      'etlDate': '日期', '可充抵保证金证券': '状态', }
        df.rename(columns=df_columns, inplace=True)
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
    urlo = 'http://www.hongtastock.com/list.aspx?serial=100101'
    stock_name = '红塔证券'
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
        chart = chardet.detect(html)
        encoding = 'gbk' if chart['encoding'] == 'GB2312' and chart['confidence'] <= 0.99 else chart['encoding']
        html = html.decode(encoding)
        if '没有相关数据' in html:
            timedelta += 1
            self.date_ = self.date_ + datetime.timedelta(days=-timedelta)
            self.thread_main(timedelta=timedelta)
            return None
        pdf_url1, html_date = self.get_pdf_url(html, flag=1)
        time.sleep(2)
        res_text, pdf_html1 = get_html_ajex(self.url_ori, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        chart = chardet.detect(pdf_html1)
        encoding = 'gbk' if chart['encoding'] == 'GB2312' and chart['confidence'] <= 0.99 else chart['encoding']
        pdf_html1 = pdf_html1.decode(encoding)
        time.sleep(2)
        pdf_url2, html_date = self.get_pdf_url(pdf_html1, flag=2, date=html_date)
        time.sleep(2)
        res_text, pdf_html2 = get_html_ajex(self.url_ori, self.post, self.h_txt)
        if res_text == '数据抓取失败。\n':
            self.label2show = '数据抓取失败。\n'
            return None
        with open(f'{self.file_path}/{self.stock_name}{self.date}.pdf', 'wb') as file:
            file.write(pdf_html2)

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
    def get_pdf_url(self, html, flag=1, date=date0):
        if (datetime.datetime.now() - date).days > 15:
            return None
        url_head = 'http://www.hongtastock.com/'
        if not html:
            return None
        url = None
        if flag == 1:
            date = date.strftime('%Y-%m-%d')
            url = re.search(fr'href=\'(.*?)\'>标的证券名单</a>[\S\s]+?{date}</span', html)
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
        elif flag == 2:
            date = date.strftime('%Y%m%d')
            url = re.search(fr'href="(.*?)" target="_blank">附件1：标的证券名单（{date}）.pdf', html)
            date = datetime.datetime.strptime(date, '%Y%m%d')
        if not url:
            return self.get_pdf_url(html, flag, date=date + datetime.timedelta(days=-1))
        return url_head + url.group(1), date

    # 解析初始网页 json 数据
    def parse_html(self, df):
        """
        解析数据
        """
        df = df.reset_index(drop=True).T.set_index(0).T
        df.columns = [i.strip() for i in df.columns]
        df_columns = {'市场': '市场', '证券代码': '证券代码', '证券名称': '证券简称', '融资保证金比例': '融资保证金比例',
                      '融券保证金比例': '融券保证金比例', }
        df.rename(columns=df_columns, inplace=True)
        df = trans_data(df, self.date)
        return df

        # 数据储存为 excel 格式

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
