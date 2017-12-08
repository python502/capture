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

class CaptureRate(CaptureBase):
    def __init__(self, user_agent, proxy_ip=None):
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
            format_select = 'SELECT ID FROM {} WHERE EXCHANGE_TYPE="{{exchange_type}}" AND EXCHANGE_CHANNEL="{{exchange_channel}}" AND FROM_CURRENCY="{{from_currency}}"\
 AND TO_CURRENCY="{{to_currency}}" ORDER BY CREATE_TIME DESC'
            good_datas = results
            select_sql = format_select.format(self.TABLE_NAME_RATE)
            table = self.TABLE_NAME_RATE
            replace_insert_columns = ['EXCHANGE_TYPE', 'EXCHANGE_CHANNEL', 'FROM_CURRENCY', 'TO_CURRENCY',\
                       'SELL_TT', 'SELL_OD', 'BUY_TT', 'BUY_OD', 'CREATE_TIME']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealRateOfCimbSg error:{}'.format(e))
            raise

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


