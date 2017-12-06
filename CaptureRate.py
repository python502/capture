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

from CaptureBase import CaptureBase
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from datetime import datetime
import time
TABLE_NAME_RATE = 'EXCHANGE_RATE_RAW'
class CaptureRate(CaptureBase):
    def __init__(self, user_agent, proxy_ip=None):
        '''
        Constructor
        '''
        super(CaptureRate, self).__init__(user_agent, proxy_ip)

    def __del__(self):
        super(CaptureRate, self).__del__()

    '''
     function:获取CIMB SG 的汇率信息
     @return: [{},{}] or raise
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
            header = self._getDict4str(src_header)
            html = self.getHtml(rate_cimb_sg, header)
            soup = BeautifulSoup(html, 'lxml')
            trs = soup.find('table').find_all('tr')[1:]
            for tr in trs:
                result = {}
                result['EXCHANGE_TYPE'.lower()] = '02'
                result['EXCHANGE_CHANNEL'.lower()] = 'CIMB'
                result['FROM_CURRENCY'.lower()] = 'SGD'
                result['TO_CURRENCY'.lower()] = tr.find_all('td')[1].getText()
                result['SELL_TT'.lower()] = float(tr.find_all('td')[2].getText())
                result['SELL_OD'.lower()] = float(tr.find_all('td')[2].getText())
                result['BUY_TT'.lower()] = float(tr.find_all('td')[3].getText())
                result['BUY_OD'.lower()] = float(tr.find_all('td')[4].getText())
                result['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
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
            return self.saveExchangeRate(results)
        except Exception, e:
            logger.error('dealRateOfCimbSg error:{}'.format(e))
            raise

    '''
     function: 将查询到的汇率信息存储进数据库，存在的replace,不存在的insert
     @select_sql: select sql 
     @exchange_rates: 原始数据
     @return: True or False
     '''
    def saveExchangeRate(self, src_datas):
        try:
            result_insert, result_update = True, True
            table = TABLE_NAME_RATE
            select_sql = 'SELECT ID FROM exchange_rate_raw WHERE EXCHANGE_TYPE="{exchange_type}" AND EXCHANGE_CHANNEL="{exchange_channel}" \
AND FROM_CURRENCY="{from_currency}" AND TO_CURRENCY="{to_currency}" ORDER BY CREATE_TIME DESC '
            columns = ['EXCHANGE_TYPE', 'EXCHANGE_CHANNEL', 'FROM_CURRENCY', 'TO_CURRENCY',\
                       'SELL_TT', 'SELL_OD', 'BUY_TT', 'BUY_OD', 'CREATE_TIME']
            if not src_datas:
                logger.error('not get datas to save')
                return False
            (insert_datas, update_datas) = self._checkDatas(select_sql, src_datas, ['ID'])
            if insert_datas:
                operate_type = 'insert'
                l = len(insert_datas)
                logger.info('len insert_datas: {}'.format(l))
                result_insert = self.mysql.insert_batch(operate_type, table, columns, insert_datas)
                logger.info('result_insert: {}'.format(result_insert))
            if update_datas:
                operate_type = 'replace'
                l = len(update_datas)
                logger.info('len update_datas: {}'.format(l))
                columns.insert(0, 'ID')
                result_update = self.mysql.insert_batch(operate_type, table, columns, update_datas)
                logger.info('result_update: {}'.format(result_update))
            return result_insert and result_update
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


