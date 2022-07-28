from pypinyin import lazy_pinyin  # 导入拼音模块
from qs_spider import (gsxx_spider,  # 中信证券
                       htsc_spider,  # 华泰证券
                       gtja_spider,  # 国泰君安
                       swhy_spider,  # 申万宏源
                       cczq_spider,  # 长城证券
                       cdzq_spider,  # 财达证券
                       # csc108_spider,  # 中信建投
                       gfzq_spider,  # 广发证券
                       guosen_spider,  # 国信证券
                       gyzq_spider,  # 国元证券
                       jzsec_spider,  # 九州证券
                       newone_spider,  # 招商证券
                       tebon_spider,  # 德邦证券
                       gkzq_spider,  # 国开证券
                       cfzq_spider,  # 财信证券
                       dgzq_spider,  # 东莞证券
                       shzq_spider,  # 上海证券
                       cinda_spider,  # 信达证券
                       grzq_spider,  # 国融证券
                       hxzq_spider,  # 宏信证券
                       cnht_spider,  # 恒泰证券
                       i618_spider,  # 山西证券
                       csco_spider,  # 世纪证券
                       west_spider,  # 西部证券
                       ykzq_spider,  # 粤开证券
                       avics_spider,  # 中航证券
                       ewww_spider,  # 渤海证券
                       zszq_spider,  # 中山证券
                       chinalions_spider,  # 华林证券
                       ccaizq_spider,  # 川财证券
                       longone_spider,  # 东海证券
                       nesc_spider,  # 东北证券
                       pingan_spider,  # 平安证券
                       sczq_spider,  # 首创证券
                       wkzq_spider,  # 五矿证券
                       xsdzq_spider,  # 新时代证券
                       ytzq_spider,  # 银泰证券
                       founder_spider,  # 方正证券
                       gjzq_spider,  # 国金证券
                       glsc_spider,  # 国联证券
                       tpyzq_spider,  # 太平洋证券
                       wlzq_spider,  # 万联证券
                       zheshang_spider,  # 浙商证券
                       zts_spider,  # 中泰证券
                       ccnew_spider,  # 中原证券
                       chinastock_spider,  # 中国银河
                       dfzq_spider,  # 东方证券
                       boci_spider,  # 中银国际
                       ciccwm_spider,  # 中国中投
                       dtsbc_spider,  # 大同证券
                       dxzq_spider,  # 东兴证券
                       hrsec_spider,  # 华融证券
                       lczq_spider,  # 联储证券
                       wanhesec_spider,  # 万和证券
                       ajzq_spider,  # 爱建证券
                       cjiang_spider,  # 长江证券
                       hazq_spider,  # 华安证券
                       hx168_spider,  # 华西证券
                       jyzq_spider,  # 金元证券
                       daton_spider,  # 大通证券
                       essence_spider,  # 安信证券
                       hlzq_spider,  # 华龙证券
                       hongta_spider,  # 红塔证券
                       htsec_spider,  # 海通证券
                       mszq_spider,  # 民生证券
                       swsc_spider,  # 西南证券
                       )


def qs_list():
    """
    return 网站中文名称列表，按拼音升序
    """
    stock = list(qs_funcmap()[0].keys())
    stock = sorted(stock, key=lambda x: lazy_pinyin(x))
    stock = [y for x in (zip([i for i in stock if '保证金' in i], [i for i in stock if '融资融券' in i])) for y in x]
    return stock


def qs_pagesize():
    """
    return 网站每页显示条目数
    """
    pagesize_map = {}
    for key, value in qs_funcmap()[0].items():
        try:
            pagesize = qs_spider(key, '1', '2', '.', None).page_size
        except AttributeError:
            pagesize = 10
        pagesize_map[key] = pagesize
    # pagesize_map = {key: qs_spider(key,'1','2','.',None).page_size for key, value in qs_funcmap()[0].items()}
    return pagesize_map


def qs_funcmap():
    """
    return 网址字典
    """
    func_map = {  # '浙江股权企业信息': 'zjex',
        '中信证券可充抵保证金': 'gsxx_spider',
        '中信证券融资融券': 'gsxx_spider',
        '国泰君安可充抵保证金': 'gtja_spider',
        '国泰君安融资融券': 'gtja_spider',
        '华泰证券可充抵保证金': 'htsc_spider',
        '华泰证券融资融券': 'htsc_spider',
        '申万宏源可充抵保证金': 'swhy_spider',
        '申万宏源融资融券': 'swhy_spider',
        '长城证券可充抵保证金': 'cczq_spider',
        '长城证券融资融券': 'cczq_spider',
        '财达证券可充抵保证金': 'cdzq_spider',
        '财达证券融资融券': 'cdzq_spider',
        # '中信建投可充抵保证金': 'csc108_spider',
        # '中信建投融资融券': 'csc108_spider',
        '广发证券可充抵保证金': 'gfzq_spider',
        '广发证券融资融券': 'gfzq_spider',
        '国信证券可充抵保证金': 'guosen_spider',
        '国信证券融资融券': 'guosen_spider',
        '国元证券可充抵保证金': 'gyzq_spider',
        '国元证券融资融券': 'gyzq_spider',
        '九州证券可充抵保证金': 'jzsec_spider',
        '九州证券融资融券': 'jzsec_spider',
        '招商证券可充抵保证金': 'newone_spider',
        '招商证券融资融券': 'newone_spider',
        '德邦证券可充抵保证金': 'tebon_spider',
        '德邦证券融资融券': 'tebon_spider',
        '国开证券可充抵保证金': 'gkzq_spider',
        '国开证券融资融券': 'gkzq_spider',
        '财信证券可充抵保证金': 'cfzq_spider',
        '财信证券融资融券': 'cfzq_spider',
        '东莞证券可充抵保证金': 'dgzq_spider',
        '东莞证券融资融券': 'dgzq_spider',
        '上海证券可充抵保证金': 'shzq_spider',
        '上海证券融资融券': 'shzq_spider',
        '信达证券可充抵保证金': 'cinda_spider',
        '信达证券融资融券': 'cinda_spider',
        '国融证券可充抵保证金': 'grzq_spider',
        '国融证券融资融券': 'grzq_spider',
        '宏信证券可充抵保证金': 'hxzq_spider',
        '宏信证券融资融券': 'hxzq_spider',
        '恒泰证券可充抵保证金': 'cnht_spider',
        '恒泰证券融资融券': 'cnht_spider',
        '山西证券可充抵保证金': 'i618_spider',
        '山西证券融资融券': 'i618_spider',
        '世纪证券可充抵保证金': 'csco_spider',
        '世纪证券融资融券': 'csco_spider',
        '西部证券可充抵保证金': 'west_spider',
        '西部证券融资融券': 'west_spider',
        '粤开证券可充抵保证金': 'ykzq_spider',
        '粤开证券融资融券': 'ykzq_spider',
        '中航证券可充抵保证金': 'avics_spider',
        '中航证券融资融券': 'avics_spider',
        '渤海证券可充抵保证金': 'ewww_spider',
        '渤海证券融资融券': 'ewww_spider',
        '中山证券可充抵保证金': 'zszq_spider',
        '中山证券融资融券': 'zszq_spider',
        '华林证券可充抵保证金': 'chinalions_spider',
        '华林证券融资融券': 'chinalions_spider',
        '川财证券可充抵保证金': 'ccaizq_spider',
        '川财证券融资融券': 'ccaizq_spider',
        '东海证券可充抵保证金': 'longone_spider',
        '东海证券融资融券': 'longone_spider',
        '东北证券可充抵保证金': 'nesc_spider',
        '东北证券融资融券': 'nesc_spider',
        '平安证券可充抵保证金': 'pingan_spider',
        '平安证券融资融券': 'pingan_spider',
        '首创证券可充抵保证金': 'sczq_spider',
        '首创证券融资融券': 'sczq_spider',
        '五矿证券可充抵保证金': 'wkzq_spider',
        '五矿证券融资融券': 'wkzq_spider',
        '新时代证可充抵保证金': 'xsdzq_spider',
        '新时代证融资融券': 'xsdzq_spider',
        '银泰证券可充抵保证金': 'ytzq_spider',
        '银泰证券融资融券': 'ytzq_spider',
        '方正证券可充抵保证金': 'founder_spider',
        '方正证券融资融券': 'founder_spider',
        '国金证券可充抵保证金': 'gjzq_spider',
        '国金证券融资融券': 'gjzq_spider',
        '国联证券可充抵保证金': 'glsc_spider',
        '国联证券融资融券': 'glsc_spider',
        '太平洋证可充抵保证金': 'tpyzq_spider',
        '太平洋证融资融券': 'tpyzq_spider',
        '万联证券可充抵保证金': 'wlzq_spider',
        '万联证券融资融券': 'wlzq_spider',
        '浙商证券可充抵保证金': 'zheshang_spider',
        '浙商证券融资融券': 'zheshang_spider',
        '中泰证券可充抵保证金': 'zts_spider',
        '中泰证券融资融券': 'zts_spider',
        '中原证券可充抵保证金': 'ccnew_spider',
        '中原证券融资融券': 'ccnew_spider',
        '中国银河可充抵保证金': 'chinastock_spider',
        '中国银河融资融券': 'chinastock_spider',
        '东方证券可充抵保证金': 'dfzq_spider',
        '东方证券融资融券': 'dfzq_spider',
        '中银国际可充抵保证金': 'boci_spider',
        '中银国际融资融券': 'boci_spider',
        '中国中投可充抵保证金': 'ciccwm_spider',
        '中国中投融资融券': 'ciccwm_spider',
        '大同证券可充抵保证金': 'dtsbc_spider',
        '大同证券融资融券': 'dtsbc_spider',
        '东兴证券可充抵保证金': 'dxzq_spider',
        '东兴证券融资融券': 'dxzq_spider',
        '华融证券可充抵保证金': 'hrsec_spider',
        '华融证券融资融券': 'hrsec_spider',
        '联储证券可充抵保证金': 'lczq_spider',
        '联储证券融资融券': 'lczq_spider',
        '万和证券可充抵保证金': 'wanhesec_spider',
        '万和证券融资融券': 'wanhesec_spider',
        '爱建证券可充抵保证金': 'ajzq_spider',
        '爱建证券融资融券': 'ajzq_spider',
        '长江证券可充抵保证金': 'cjiang_spider',
        '长江证券融资融券': 'cjiang_spider',
        '华安证券可充抵保证金': 'hazq_spider',
        '华安证券融资融券': 'hazq_spider',
        '华西证券可充抵保证金': 'hx168_spider',
        '华西证券融资融券': 'hx168_spider',
        '金元证券可充抵保证金': 'jyzq_spider',
        '金元证券融资融券': 'jyzq_spider',
        '大通证券可充抵保证金': 'daton_spider',
        '大通证券融资融券': 'daton_spider',
        '安信证券可充抵保证金': 'essence_spider',
        '安信证券融资融券': 'essence_spider',
        '华龙证券可充抵保证金': 'hlzq_spider',
        '华龙证券融资融券': 'hlzq_spider',
        '红塔证券可充抵保证金': 'hongta_spider',
        '红塔证券融资融券': 'hongta_spider',
        '海通证券可充抵保证金': 'htsec_spider',
        '海通证券融资融券': 'htsec_spider',
        '民生证券可充抵保证金': 'mszq_spider',
        '民生证券融资融券': 'mszq_spider',
        '西南证券可充抵保证金': 'swsc_spider',
        '西南证券融资融券': 'swsc_spider',
    }
    func_map2 = {k[:4]: v + '.by2by' for k, v in func_map.items()}
    return func_map, func_map2


def qs_spider(labeltext, start, end, path, date):
    """
    qs_spider(labeltext, start, end, path, date)
    param labeltext: 输入网址标签
    param start: 起始页码
    param end: 结束页码
    param path: 保存位置
    param date: 日期
    return 网址类
    """
    func_map = qs_funcmap()[0]
    for k, v in func_map.items():
        if '保证金' in k:
            v += '.db'
        else:
            v += '.rzrq'
        func_map[k] = v
    return eval(f'{func_map.get(labeltext)}("{start}", "{end}", "{path}", "{date}")')


def qs_spider_by2by(labeltext, start, end, path, date):
    """
    qs_spider_by2by(labeltext, start, end, path, date)
    param labeltext: 输入网址标签
    param start: 起始页码
    param end: 结束页码
    param path: 保存位置
    param date: 日期
    return 网址类
    """
    func_map = qs_funcmap()[1]
    return eval(f'{func_map.get(labeltext)}("{start}", "{end}", "{path}", "{date}")')
