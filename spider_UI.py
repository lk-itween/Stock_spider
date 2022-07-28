import os
from base64 import b64decode
import datetime
import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox
import ctypes
import win32gui, win32print, win32con
import numpy as np
import threading
from qs_spider_select import qs_list, qs_pagesize, qs_spider, qs_spider_by2by
import deny_access
import file_path


class zjex_GUI(tk.Tk):
    try:  # win10
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
        # 调用api获得当前的缩放因子
        ScaleFactor = ctypes.windll.shcore.GetScaleFactorForDevice(0)
    except:  # win7
        ctypes.windll.user32.SetProcessDPIAware()
        hDC = win32gui.GetDC(0)
        dpi1 = win32print.GetDeviceCaps(hDC, win32con.DESKTOPHORZRES)
        dpi2 = win32print.GetDeviceCaps(hDC, win32con.HORZRES)
        ScaleFactor = dpi1 / dpi2
    datestr = datetime.datetime.now().strftime('%Y%m%d')
    btn_flag = 1
    pagesize = 20
    map_get_ = {}
    mythread_list = []

    def __init__(self):
        super(zjex_GUI, self).__init__()
        self.tk.call('tk', 'scaling', self.ScaleFactor / 75)
        self.sticky = tk.NSEW
        self.input_chk = {}
        self.createWidgets()
        self.title('券商信息采集器 v1.0')
        self.weight = 600
        self.high = 350
        r = hex(np.random.randint(255))[2:]
        g = hex(np.random.randint(255))[2:]
        b = hex(np.random.randint(255))[2:]
        r = r if len(r) == 2 else '0' + r
        g = g if len(g) == 2 else '0' + g
        b = b if len(b) == 2 else '0' + b
        self.fg = '#' + r + g + b
        self.combox_list = qs_list()
        self.pagesize_map = qs_pagesize()
        self.combox_select = [self.combox_list[0]]
        self.pagesize = self.label_select(self.combox_select)
        self.gui_init()
        self.center(self.weight, self.high)
        self.minsize(self.weight, self.high)
        ico = 'zjexico.ico'
        self.ico_path(ico)
        self.iconbitmap(ico)
        self.remove_path(ico)
        assert deny_access.deny('20220921'), messagebox.showwarning('工具已失效！！！', '工具已失效！！！')
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.mainloop()

    def sub_window(self):
        if self.btn_flag and self.mythread_list:
            assert False, messagebox.showwarning('请清空等待抓取列表数据！！！', '请清空等待抓取列表数据！！！')
        Sub_Win(self, weight=self.weight, high=self.high, input_chk=self.input_chk,
                input_list=list(self.combox_list))
        self.map_get_.clear()

    def sub_window_is_ok(self, data):
        self.input_chk = self.input_chk if self.input_chk == data else data
        self.combox_select.clear()
        self.combox_select = [k for k, v in self.input_chk.items() if v.get() == 1]
        self.pagesize = self.label_select(self.combox_select)

    def on_closing(self):
        if tk.messagebox.askokcancel("退出?", "是否退出?"):
            self.destroy()

    def ico_path(self, filename):
        img_base64 = b''
        with open(filename, 'wb+') as tmp:
            img_base64 = b64decode(
                eval(f"file_path.{filename.split('.')[0]}()"))
            tmp.write(img_base64)
        return os.path.abspath(filename)

    def remove_path(self, filename):
        os.remove(filename)

    def center(self, w: int, h: int):
        ws, hs = self.maxsize()
        x = (ws - w) / 2
        y = (hs - h) / 2
        self.geometry('%dx%d+%d+%d' % (w, h, x, y))

    def createWidgets(self):
        top = self.winfo_toplevel()
        i = 2
        top.columnconfigure(i, weight=i)
        self.columnconfigure(i, weight=i)
        for i in [1, 2, 4]:
            top.rowconfigure(i, weight=i)
            self.rowconfigure(i, weight=i)

    def gui_init(self):
        self.btn0 = self.left_button_set('网址选择', 0, 0, func=self.sub_window)
        self.date_entry, self.date_str = self.entry_set(self.datestr, 1, 0)
        self.label = self.gui_text(self.combox_list[0], fg=self.fg)
        self.label1 = self.label_set('起始记录数', 0, 2, fg=self.fg)
        self.label2 = self.label_set('截止记录数', 0, 4, fg=self.fg)
        self.label3 = self.label_set('起始页', 1, 2, fg=self.fg)
        self.label4 = self.label_set('截止页', 1, 4, fg=self.fg)
        self.entry1, self.t1 = self.entry_set('all', 0, 3)
        self.entry2, self.t2 = self.entry_set('all', 0, 5)
        self.entry3, self.t3 = self.entry_set('', 1, 3, state='readonly')
        self.entry4, self.t4 = self.entry_set('', 1, 5, state='readonly')
        self.after(100, self.entry_after)
        self.btn1 = self.left_button_set('确定', 0, 6, func=self.commit1)
        self.btn2 = self.left_button_set('开始抓取', 0, 6, func=self.commit2)
        self.btn2.grid_forget()
        self.special_button_set('<Alt-z>', self.cancel_space)
        self.btn3 = self.left_button_set('文件夹选择:', 1, 6, func=self.open)
        self.textshow = self.text_set('信息展示框\n', 2, 0, 6)
        self.path_get = ''
        if os.path.exists('D:\STOCKSPIDERGET.txt'):
            with open('D:\STOCKSPIDERGET.txt', 'r') as f:
                self.path_get = f.read()
        self.last_useful_path = self.path_get
        self.entry5, self.t5 = self.entry_set(self.last_useful_path, 2, 6)

    def gui_text(self, text, fg=None):
        try:
            self.label.grid_remove()
        except AttributeError:
            pass
        self.label_set(text, 0, 1, columnspan=2, fg=fg, sticky=tk.NSEW)

    def label_select(self, text):
        text = ''.join(text)
        if text in self.pagesize_map.keys():
            self.label = self.gui_text(text, self.fg)
            return self.pagesize_map.get(text)
        else:
            self.label = self.gui_text('网页数据提取')
            return 10

    def left_button_set(self, text: str, column: int, row: int, func=None):
        if text == '文件夹选择':
            bg = '#b8ebeb'
            btn = tk.Button(self, text=text, relief=tk.RAISED, bg=bg, command=func)
        elif text == '确定':
            fg = '#ff0000'
            bg = '#d3dfc6'
            btn = tk.Button(self, text=text, relief=tk.RAISED, font=("黑体", 13), bg=bg, fg=fg, command=func)
        elif text == '开始抓取':
            fg = '#0f0000'
            bg = '#d3dfc6'
            btn = tk.Button(self, text=text, relief=tk.RAISED, font=("黑体", 13), bg=bg, fg=fg, command=func)
        else:
            btn = tk.Button(self, text=text, relief=tk.RAISED, command=func)
        btn.grid(column=column, row=row, padx=5, pady=5, sticky=self.sticky)
        btn.flash()
        return btn

    # 特殊按键绑定
    def special_button_set(self, key=None, func=None):
        self.bind(key, func)

    def label_set(self, text: str, column: int, row: int, image=None, rowspan=1, columnspan=1, fg=None, sticky=tk.SW):
        if len(text) < 8:
            label = tk.Label(self, text=text, image=image, compound="top", fg=fg, font=("华文仿宋", 12))
        else:
            label = tk.Label(self, text=text, image=image, compound="top", fg=fg, font=("华文彩云", 14))
        label.grid(column=column, row=row, rowspan=rowspan, columnspan=columnspan, padx=5, pady=5, sticky=sticky)
        return label

    def text_set(self, text: str, column: int, row: int, rowspan=1, columnspan=1):
        txt = scrolledtext.ScrolledText(self, font=("TimesNewRoman", 12))
        txt.insert(tk.INSERT, text)
        txt.grid(column=column, row=row, rowspan=rowspan, columnspan=columnspan, padx=5, pady=5, sticky=self.sticky)
        return txt

    def entry_set(self, text: str, column: int, row: int, state='normal'):
        text_str = tk.StringVar(self)
        entry = tk.Entry(self, width=10, textvariable=text_str)
        entry['state'] = state
        text_str.set(text)
        entry.grid(column=column, row=row, padx=5, pady=5, sticky=self.sticky)
        return entry, text_str

    def entry_after(self):
        text1 = self.t1.get().strip()
        text2 = self.t2.get().strip()
        try:
            text1 = 0 if int(text1) <= 0 else int(text1)
            text1 = 'all' if text1 // self.pagesize == text1 else text1 // self.pagesize
        except ValueError:
            text1 = 0
        try:
            text2 = 0 if int(text2) <= 0 else int(text2)
            text2 = 'all' if text2 // self.pagesize == text2 else text2 // self.pagesize + 1
        except ValueError:
            text2 = 'all'
        self.entry3['state'] = 'normal'
        self.entry4['state'] = 'normal'
        self.t3.set(text1)
        self.t4.set(text2)
        self.entry3['state'] = 'readonly'
        self.entry4['state'] = 'readonly'
        self.after(200, self.entry_after)

    def commit_show(self, labeltext, last_text):
        if self.map_get_[labeltext].label2show != last_text:
            self.textshow.insert(tk.INSERT, self.map_get_[labeltext].label2show)
            self.textshow.see(tk.END)
            self.update()
        if '完毕' in self.map_get_[labeltext].label2show:
            return None
        self.after(100, self.commit_show, labeltext, self.map_get_[labeltext].label2show)

    def open(self):
        file_path = filedialog.askdirectory()
        file_path = self.path_get if file_path == '' else file_path
        self.last_useful_path = self.last_useful_path if file_path == '' else file_path
        self.t5.set(self.last_useful_path)
        with open('D:\STOCKSPIDERGET.txt', 'w') as f:
            f.write(self.last_useful_path)
        self.update()

    def cancel_space(self, event=None):
        self.mythread_list.clear()
        self.btn_flag = 1
        self.btn2.grid_forget()
        self.btn1.grid(column=0, row=6, padx=5, pady=5, sticky=self.sticky)
        self.textshow.delete(0.0, tk.END)
        self.textshow.insert(tk.INSERT, '已取消选择，请重新选择后点击"确定"\n')
        self.textshow.see(tk.END)
        self.update()

    def multi_run(self, flag=1):
        if flag:
            for mythread in self.mythread_list:
                mythread.start()
            flag = 0
        if not any([mythread.is_alive() for mythread in self.mythread_list]):
            self.textshow.insert(tk.INSERT, f'网址信息抓取工具执行完毕\n')
            self.textshow.see(tk.END)
            return None
        self.after(100, self.multi_run, flag)

    def modified(self, event):
        self.textshow.see(tk.END)

    def commit1(self):
        path = '.' if self.t5.get() == '' else self.t5.get()
        self.textshow.delete(0.0, tk.END)
        self.textshow.insert(tk.INSERT, '准备抓取,请点击 "开始抓取" 按键\n"Alt-z" 撤销确定键\n')
        if self.btn_flag:
            self.btn1.grid_forget()
            self.btn2.grid(column=0, row=6, padx=5, pady=5, sticky=self.sticky)
        self.btn_flag ^= 1
        self.mythread_list.clear()
        last_text = ''
        for labeltext in self.combox_select:
            if labeltext[:4] == last_text[:4]:
                self.map_get_.pop(last_text)
                self.map_get_[labeltext[:4]] = qs_spider_by2by(labeltext[:4], self.t3.get(), self.t4.get(), path,
                                                               self.date_str.get())
            else:
                last_text = labeltext
                self.map_get_[labeltext] = qs_spider(labeltext, self.t3.get(), self.t4.get(), path, self.date_str.get())
        for labeltext, function in self.map_get_.items():
            self.textshow.insert(tk.INSERT, f'准备抓取{labeltext}网址信息\n')
            self.textshow.see(tk.END)
            mythread = threading.Thread(target=function.thread_main)
            mythread.daemon = 1
            self.mythread_list.append(mythread)
            self.update()

    def commit2(self):
        if not self.btn_flag:
            self.btn_flag ^= 1
            self.textshow.insert(tk.INSERT, '开始抓取，请等待......\n')
            self.textshow.see(tk.END)
            if len(self.combox_select) == 1:
                self.mythread_list[0].start()
                self.commit_show(self.combox_select[0], '')
            else:
                self.multi_run()
            return None

        self.after(200, self.commit2)


class Sub_Win(tk.Toplevel):
    try:  # win10
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
        # 调用api获得当前的缩放因子
        ScaleFactor = ctypes.windll.shcore.GetScaleFactorForDevice(0)
    except:  # win7
        ctypes.windll.user32.SetProcessDPIAware()
        hDC = win32gui.GetDC(0)
        dpi1 = win32print.GetDeviceCaps(hDC, win32con.DESKTOPHORZRES)
        dpi2 = win32print.GetDeviceCaps(hDC, win32con.HORZRES)
        ScaleFactor = dpi1 / dpi2

    def __init__(self, master, **kwargs):
        super(Sub_Win, self).__init__()
        self.master = master
        self.weight = kwargs.pop('weight')
        self.high = kwargs.pop('high')
        self.chkbtn_dict_input = kwargs.pop('input_chk')
        self.chkbtn_list = kwargs.pop('input_list')
        self.sticky = tk.NSEW
        self.sort_num = 0
        self.gui_init()
        self.shift_right()
        self.check_changed4()
        self.createWidgets()
        w = 330
        h = 300
        self.center(w=w, h=h)
        self.minsize(w, h)
        self.resizable(False, False)
        ico = 'zjexico.ico'
        self.ico_path(ico)
        self.iconbitmap(ico)
        self.remove_path(ico)
        self.protocol("WM_DELETE_WINDOW", self.ondelete)

    def gui_init(self):
        self.checkvar1 = tk.IntVar()
        self.checkvar2 = tk.IntVar()
        chkbtn1 = tk.Checkbutton(self, text="全部", variable=self.checkvar1, onvalue=1, offvalue=0, height=2, width=20,
                                 font=("黑体", 10), command=self.check_changed)
        chkbtn2 = tk.Checkbutton(self, text="当前页", variable=self.checkvar2, onvalue=1, offvalue=0, height=2, width=20,
                                 font=("黑体", 10), command=self.check_changed2)
        chkbtn1.grid(row=0, column=0, sticky=self.sticky)
        chkbtn2.grid(row=0, column=1, sticky=self.sticky)

        self.chkbtn_dict = {}
        self.checkvar_dict = {}
        self.chklist = []
        self.lab_list = []

        for num, i in enumerate(self.chkbtn_list):
            checkvar = tk.IntVar()
            if self.chkbtn_dict_input:
                checkvar = self.chkbtn_dict_input[i]
            if num % 10 == 0:
                self.checkvar_dict[num // 10] = [checkvar]
            else:
                self.checkvar_dict[num // 10].append(checkvar)
            self.chkbtn_dict[i] = checkvar
            self.chklist.append(
                tk.Checkbutton(self, text=i, variable=checkvar, onvalue=1, offvalue=0, height=2, width=20,
                               font=("黑体", 10), command=self.check_changed3))
        self.check_changed3(self.chkbtn_dict_input)

        self.left_button_set('<', 0, 11, self.shift_num_sub)
        self.left_button_set('>', 1, 11, self.shift_num_add)
        self.left_button_set('确定', 0, 12, self.on_ok)
        self.left_button_set('取消', 1, 12, self.ondelete)

    def createWidgets(self):
        top = self.winfo_toplevel()
        for i in [0, 1]:
            top.columnconfigure(i, weight=i)
            self.columnconfigure(i, weight=i)
        for i in [0, 11, 12]:
            top.rowconfigure(i, weight=i)
            self.rowconfigure(i, weight=i)

    def center(self, w: int, h: int):
        ws, hs = self.maxsize()
        x = (ws - w) / 2
        y = (hs - h) / 2
        self.geometry('%dx%d+%d+%d' % (w, h, x, y))

    def ico_path(self, filename):
        img_base64 = b''
        with open(filename, 'wb+') as tmp:
            img_base64 = b64decode(
                eval(f"file_path.{filename.split('.')[0]}()"))
            tmp.write(img_base64)
        return os.path.abspath(filename)

    def remove_path(self, filename):
        os.remove(filename)

    def on_ok(self):
        self.master.sub_window_is_ok(self.chkbtn_dict)
        self.destroy()

    def ondelete(self):
        self.master.sub_window_is_ok(self.chkbtn_dict_input)
        self.destroy()

    def left_button_set(self, text: str, column: int, row: int, func=None):
        if text == '确定':
            fg = '#ff0000'
            bg = '#d3dfc6'
            btn = tk.Button(self, text=text, relief=tk.RAISED, font=("黑体", 8), bg=bg, fg=fg, command=func)
        elif text == '取消':
            fg = '#0f0000'
            bg = '#d3dfc6'
            btn = tk.Button(self, text=text, relief=tk.RAISED, font=("黑体", 8), bg=bg, fg=fg, command=func)
        else:
            btn = tk.Button(self, text=text, relief=tk.RAISED, font=("黑体", 8), command=func)
        btn.grid(column=column, row=row, padx=2, pady=2, sticky=self.sticky)
        btn.flash()
        return btn

    def shift_num_add(self):
        self.sort_num += 1
        if len(self.chklist) < 10:
            return None
        self.shift_right(flag=1)

    def shift_num_sub(self):
        self.sort_num -= 1
        if len(self.chklist) < 10:
            return None
        self.shift_right(flag=-1)

    def shift_right(self, flag=1):
        self.sort_num = 0 if self.sort_num <= 0 else self.sort_num
        self.sort_num = len(self.chklist) // 10 if self.sort_num > 0 and self.sort_num >= len(
            self.chklist) // 10 else self.sort_num
        M = 10 * self.sort_num
        self.checkvar2.set(0)
        Length = len(self.chklist[M:10 + M])
        for i in self.chklist[M - flag * 10:M - flag * 20]:
            i.grid_forget()
        for num, i in enumerate(self.chklist[M:10 + M]):
            i.grid(row=1 + num // 2, column=num % 2, sticky=self.sticky)
        if 0 < Length < 10:
            for num2 in range(Length, 10):
                lab = tk.Label(self, text='')
                self.lab_list.append(lab)
                lab.grid(row=1 + num2 // 2, column=num2 % 2, sticky=self.sticky)
        elif Length == 0:
            self.sort_num -= flag
        else:
            for i in self.lab_list:
                i.grid_remove()
            self.lab_list.clear()

    def check_changed(self):
        if self.checkvar1.get() == 1:
            self.checkvar2.set(1)
            for x in self.chkbtn_dict.values():
                x.set(1)
        elif self.checkvar1.get() == 0:
            self.checkvar2.set(0)
            for x in self.chkbtn_dict.values():
                x.set(0)

    def check_changed2(self):
        if self.checkvar2.get() == 1:
            for x in self.checkvar_dict[self.sort_num]:
                x.set(1)
        elif self.checkvar2.get() == 0:
            for x in self.checkvar_dict[self.sort_num]:
                x.set(0)

    def check_changed3(self, chkbtn=None):
        chkbtn_dict = {}
        if chkbtn:
            chkbtn_dict = chkbtn
        elif self.chkbtn_dict:
            chkbtn_dict = self.chkbtn_dict
        if 0 in [x.get() for x in chkbtn_dict.values()]:
            self.checkvar1.set(0)
        else:
            self.checkvar1.set(1)
        if 0 in [x.get() for x in self.checkvar_dict[self.sort_num]]:
            self.checkvar2.set(0)
        else:
            self.checkvar2.set(1)

    def check_changed4(self):
        if self.checkvar1.get() == 1:
            self.checkvar2.set(1)
        self.after(100, self.check_changed4)


if __name__ == '__main__':
    zjex_GUI()
