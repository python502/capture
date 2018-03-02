#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 14:06
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : CaptureBase.py
# @Software: PyCharm
# @Desc    :
import re
import time
import os
import urllib2
import zlib
from MysqldbOperate import MysqldbOperate
from logger import logger
from retrying import retry
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
from decimal import Decimal
class TimeoutException(Exception):
    def __init__(self, err='operation timed out'):
        super(TimeoutException, self).__init__(err)


DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}
# DICT_MYSQL = {'host': '118.193.21.62', 'user': 'root', 'passwd': 'Avazu#2017', 'db': 'avazu_opay', 'port': 3306}
'''
classdocs
'''
class CaptureBase(object):
    TABLE_NAME_PRODUCT = 'market_product_raw'
    TABLE_NAME_BANNER = 'market_banner_raw'
    TABLE_NAME_RATE = 'exchange_rate_raw'
    def __init__(self, user_agent, proxy_ip=None):
        self.phantomjs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'phantomjs.exe')
        self.chrome_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chromedriver.exe')
        self.user_agent = user_agent
        self.proxy_ip = proxy_ip
        self.mysql = MysqldbOperate(DICT_MYSQL)
        if self.proxy_ip:
            proxy = urllib2.ProxyHandler(proxy_ip)
            opener = urllib2.build_opener(proxy)
            urllib2.install_opener(opener)

    def __del__(self):
        if self.mysql:
            del self.mysql

    '''
    function: 使用PHANTOMJS浏览器获取js执行后的html
    @url:  url
    @return: html
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getHtmlselenium(self, url, browser='phantomjs', sleep_time=0):
        driver = None
        try:
            # # 全请求头设定
            # # 使用copy()防止修改原代码定义dict
            # cap = DesiredCapabilities.PHANTOMJS.copy()
            # for key, value in self.header.items():
            #     cap['phantomjs.page.customHeaders.{}'.format(key)] = value

            if browser == 'phantomjs':
                # dcap = dict(DesiredCapabilities.PHANTOMJS)
                # dcap["phantomjs.page.settings.userAgent"] = self.user_agent
                # dcap["phantomjs.page.settings.loadImages"] = False  # 禁止加载图片
                driver = webdriver.PhantomJS(executable_path=self.phantomjs_path)
            elif browser == 'chrome':
                driver = webdriver.Chrome(executable_path=self.chrome_path)
            else:
                logger.error('error browser type: {},must be phantomjs or chrome'.format(browser))
                raise ValueError("browser type error")
            #加载页面的超时时间
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)
            driver.get(url)
            time.sleep(sleep_time)
            driver.implicitly_wait(30)
            page = driver.page_source.encode('utf-8') if isinstance(driver.page_source, (str, unicode)) else driver.page_source
            logger.debug('driver.page_source: {}'.format(page))
            return page
        except Exception, e:
            logger.error('getHtmlselenium error:{},retry it'.format(e))
            raise
        finally:
            if driver:
                driver.quit()
    '''
    function: urlopen 失败时进行retry, retry3次 间隔2秒
    @request: request
    @return: con or exception
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def __urlOpenRetry(self, request):
        # if isinstance(request, basestring):
        try:
            con = urllib2.urlopen(request, timeout=10)
            return con
        except urllib2.HTTPError, e:
            logger.error('urlopen error code: {}'.format(e.code))
            raise
        except urllib2.URLError, e:
            logger.error('urlopen error reason: {}'.format(e.reason))
            raise
        except Exception, e:
            logger.error('urlopen error e: {}'.format(e))
            raise

    '''
    function: 根据url获取页面的html
    @url:  url
    @header:  header
    @data:  data
    @return: html
    '''
    def getHtml(self, url, header, data=None):
        if not header:
            logger.error('header is None error')
            raise ValueError()
        try:
            req = urllib2.Request(url=url, headers=header, data=data)
            con = self.__urlOpenRetry(req)

            if 200 == con.getcode():
                doc = con.read()
                if con.headers.get('Content-Encoding'):
                    doc = zlib.decompress(doc, 16+zlib.MAX_WBITS)
                con.close()
                logger.debug('getHtml: url:{} getcode is 200'.format(url))
                return doc
            else:
                logger.debug('getHtml: url:{} getcode isn\'t 200,{}'.format(url, con.getcode()))
                raise ValueError()
        except Exception, e:
            logger.error('getHtml error: {}.'.format(e))
            raise

    '''
    function: get_html 根据str header 生成 dict header
    @strsource:  str header
    @return: dict header
    '''
    def _getDict4str(self, strsource, match=':'):
        outdict = {}
        lists = strsource.split('\n')
        for list in lists:
            list = list.strip()
            if list:
                strbegin = list.find(match)
                outdict[list[:strbegin]] = list[strbegin+1:] if strbegin != len(list) else ''
        return outdict
    '''
    function: 数据去重
    @select_sql： 判断商品是否已经在数据库中使用的sql
    @scr_datas： 原始数据
    @match： 去重使用的主键
    @return: 去重后数据
    '''
    def _rm_duplicate(self, scr_datas, match):
        key_value = []
        result = []
        for data in scr_datas:
            if data.get(match) in key_value:
                logger.debug('find repead data: {}'.format(data))
                continue
            else:
                key_value.append(data.get(match))
            result.append(data)
        return result

    '''
    function: 查看商品是否已经在数据库中，将原始数据分割成需要insert的和需要update的
    @select_sql： 判断商品是否已经在数据库中使用的sql
    @sourcedatas： 原始数据
    @columns： sql 查询的列名
    @return: True or False or raise
    '''
    def _checkDatas(self, select_sql, sourcedatas, columns):
        insert_datas=[]
        update_datas=[]
        for sourcedata in sourcedatas:
            try:
                sql = select_sql.format(**sourcedata)
                logger.debug('select sql: {}'.format(sql))
                result = self.mysql.sql_query(sql)
                if not result:
                    insert_datas.append(sourcedata)
                else:
                    if len(result) != 1:
                        logger.error('checkDatas get many lines:{}'.format(result))
                        logger.error('select_sql: {}'.format(sql))
                    for column in columns:
                        sourcedata[column.lower()] = result[0].get(column.upper())
                    update_datas.append(sourcedata)
            except Exception, e:
                logger.error('__checkDatas\'s error: {}.'.format(e))
                logger.error('__checkDatas\'s sourcedata: {}.'.format(sourcedata))
                continue
        return (insert_datas,update_datas)

    '''
    function: 存储商品信息
    @good_datas： 商品信息s
    @table：      存储到的表名
    @select_sql： 判断商品是否已经在数据库中使用的sql
    @replace_insert_columns： replace or insert 操作的列名
    @select_columns： sql 查询的列名
    @return: True or False or raise
    '''
    def _saveDatas(self, good_datas, table, select_sql, replace_insert_columns, select_columns):
        try:
            result_insert, result_update = True, True
            if not good_datas:
                logger.error('saveDatas not get datas')
                return False
            (insert_datas, update_datas) = self._checkDatas(select_sql, good_datas, select_columns)
            if insert_datas:
                operate_type = 'insert'
                l = len(insert_datas)
                logger.info('len insert_datas: {}'.format(l))
                result_insert = self.mysql.insert_batch(operate_type, table, replace_insert_columns, insert_datas)
                logger.info('saveDatas result_insert: {}'.format(result_insert))
            if update_datas:
                operate_type = 'replace'
                l = len(update_datas)
                logger.info('len update_datas: {}'.format(l))
                replace_insert_columns.insert(0, 'ID')
                result_update = self.mysql.insert_batch(operate_type, table, replace_insert_columns, update_datas)
                logger.info('saveDatas result_update: {}'.format(result_update))
            return result_insert and result_update
        except Exception, e:
            logger.error('saveDatas error: {}.'.format(e))
            return False

    @staticmethod
    def filter_emoji(desstr, restr=''):
        '''''
        过滤表情
        '''
        try:
            co = re.compile(u'[\U00010000-\U0010ffff]')
        except re.error:
            co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')
        return co.sub(restr, desstr)

    @staticmethod
    def format_price(res_price, digits):
        price = str(res_price)
        price = price.zfill(digits+1)
        price = price[:-digits]+'.'+price[-digits:]
        return Decimal(price).quantize(Decimal('0.00'))
    '''
    function: 获取并存储首页滚动栏的商品信息
    @return: True or Raise
    '''
    def dealHomeGoods(self):
        logger.info('dealHomeGoods does not exist')
        return True

    '''
    function: 获取所有分类的商品信息
    @
    @return: None
    '''
    def dealCategorys(self):
        logger.info('dealCategorys does not exist')
        return True