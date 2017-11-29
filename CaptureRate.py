#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureRate.py
# @Software: PyCharm
# @Desc    : 获取汇率信息并插入数据库
'''
Created on 2016年6月4日

@author: Administrator
'''
import random
import urllib2


import zlib
import cookielib
from CrawlingProxy import CrawlingProxy,useragent
from MysqldbOperate import MysqldbOperate
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
import time
# DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}
DICT_MYSQL = {'host': '118.193.21.62', 'user': 'root', 'passwd': 'Avazu#2017', 'db': 'avazu_opay', 'port': 3306}
TABLE_NAME_RATE = 'EXCHANGE_RATE_RAW'
'''
classdocs
'''
class CaptureRate(object):
    def __init__(self, user_agent, proxy_ip=None):
        '''
        Constructor
        '''
        # Cookie and Referer is'not necessary

        self.user_agent = user_agent
        self.header = ''
        self.insert = 0
        self.update = 0
        self.mysql = MysqldbOperate(DICT_MYSQL)
        #获得一个cookieJar实例
        self.cj = cookielib.CookieJar()
        #cookieJar作为参数，获得一个opener的实例
        opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cj))
        urllib2.install_opener(opener)

        proxy = urllib2.ProxyHandler(proxy_ip)
        opener = urllib2.build_opener(proxy)
        urllib2.install_opener(opener)

    def __del__(self):
        if self.mysql:
            del self.mysql
    '''
    function: getHtml 根据url获取页面的html
    @url:  url
    @return: html
    '''
    def getHtml(self, url):
        HEADER = self.header
        try:
            req = urllib2.Request(url=url, headers=HEADER)
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
        except Exception,e:
            logger.error('getHtml error: {}.'.format(e))
            raise

    '''
    function: get_html 根据str header 生成 dict header
    @strsource:  str header
    @return: dict header
    '''
    def __getDict4str(self, strsource):
        outdict = {}
        lists = strsource.split('\n')
        for list in lists:
            list = list.strip()
            if list:
                strbegin = list.find(':')
                outdict[list[:strbegin]] = list[strbegin+1:] if strbegin != len(list) else ''
        return outdict

    '''
    function: urlopen 失败时进行retry, retry3次 间隔2秒
    @request: request
    @return: con or exception
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def __urlOpenRetry(self, request):
        # if isinstance(request, basestring):
        try:
            con = urllib2.urlopen(request, timeout=30)
            return con
        except Exception, e:
            logger.error('urlopen error retry.e: {}'.format(e))
            raise
    '''
     function:获取CIMB SG 的汇率信息
     @return: [[],[]] or raise
     '''
    def getRateOfCimbSg(self):
        try:
            results = []
            rate_cimb_sg = 'https://www.cimbbank.com.sg/en/personal/support/help-and-support/rates-and-charges.html'
            HEADER = '''
                    Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
                    Accept-Encoding:gzip, deflate, br
                    Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
                    Cache-Control:max-age=0
                    Connection:keep-alive
                    Host:www.cimbbank.com.sg
                    User-Agent:{}
                    '''
            src_header = HEADER.format(self.user_agent)
            self.header = self.__getDict4str(src_header)
            html = self.getHtml(rate_cimb_sg)
            soup = BeautifulSoup(html, 'lxml')
            trs = soup.find('table').find_all('tr')[1:]
            for tr in trs:
                result = ['02', 'CIMB', 'SGD']
                result.append(tr.find_all('td')[1].getText())
                result.append(float(tr.find_all('td')[2].getText()))
                result.append(float(tr.find_all('td')[2].getText()))
                result.append(float(tr.find_all('td')[3].getText()))
                result.append(float(tr.find_all('td')[4].getText()))
                result.append(time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))
                results.append(result)
            logger.info('results: {}'.format(results))
            return results
        except Exception, e:
            logger.error('getRateOfCimbSg error:{}'.format(e))
            raise
    '''
     function:查询并入库CIMB SG 的汇率信息
     @return: True or False or raise
     '''
    def dealRateOfCimbSg(self):
        try:
            results = self.getRateOfCimbSg()
            select_sql = 'SELECT ID FROM exchange_rate_raw WHERE EXCHANGE_TYPE="{}" AND EXCHANGE_CHANNEL="{}" AND FROM_CURRENCY="{}" AND TO_CURRENCY="{}" ORDER BY CREATE_TIME DESC '
            return self.saveExchangeRate(select_sql, results)
        except Exception, e:
            logger.error('dealRateOfCimbSg error:{}'.format(e))
            raise

    '''
     function: 分类原始数据。区分insert 还是 update
     @select_sql: select sql
     @sourcedatas: 原始数据
     @return: (insert_datas, update_datas)
     '''
    def __checkRateDatas(self, select_sql, sourcedatas):
        insert_datas = []
        update_datas = []
        for sourcedata in sourcedatas:
            sql = select_sql.format(sourcedata[0], sourcedata[1], sourcedata[2], sourcedata[3])
            logger.debug('select sql: {}'.format(sql))
            try:
                result = self.mysql.sql_query(sql)
                if not result:
                    insert_datas.append(sourcedata)
                else:
                    if len(result) != 1:
                        logger.error('checkRateDatas get many lines:{}'.format(result))
                        logger.error('select_sql: {}'.format(sql))
                    sourcedata.insert(0, result[0].get('ID'))
                    update_datas.append(sourcedata)
            except Exception, e:
                logger.error('__checkRateDatas\'s error: {}.'.format(e))
                logger.error('__checkRateDatas\'s sourcedata: {}.'.format(sourcedata))
                continue
        return (insert_datas, update_datas)

    '''
     function: 将查询到的汇率信息存储进数据库，存在的replace,不存在的insert
     @select_sql: select sql 
     @exchange_rates: 原始数据
     @return: True or False
     '''
    def saveExchangeRate(self, select_sql, exchange_rates):
        try:
            if not exchange_rates:
                logger.error('not get exchange_rates to save')
                return False
            (insert_datas, update_datas) = self.__checkRateDatas(select_sql, exchange_rates)
            if insert_datas:
                l = len(insert_datas)
                self.insert += l
                logger.info('len insert_datas: {}'.format(l))
                insert_sql = 'INSERT INTO exchange_rate_raw(EXCHANGE_TYPE,EXCHANGE_CHANNEL,FROM_CURRENCY,TO_CURRENCY,SELL_TT,SELL_OD,BUY_TT,BUY_OD,CREATE_TIME) VALUES'
                result_insert = self.mysql.insert_batch(insert_sql, insert_datas)
                logger.info('result_insert: {}'.format(result_insert))
            if update_datas:
                l = len(update_datas)
                self.update += l
                logger.info('len update_datas: {}'.format(l))
                update_sql = 'REPLACE INTO exchange_rate_raw(ID,EXCHANGE_TYPE,EXCHANGE_CHANNEL,FROM_CURRENCY,TO_CURRENCY,SELL_TT,SELL_OD,BUY_TT,BUY_OD,CREATE_TIME) VALUES'
                result_update = self.mysql.insert_batch(update_sql, update_datas)
                logger.info('result_update: {}'.format(result_update))
            logger.info('insert: {}, update: {}'.format(self.insert, self.update))
            return True
        except Exception, e:
            logger.error('saveExchangeRate error: {}.'.format(e))
            return False
def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureRate = CaptureRate(useragent)
    # objCaptureRate.getRateOfCimbSg()
    objCaptureRate.dealRateOfCimbSg()


    endTime = datetime.now()

    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()


