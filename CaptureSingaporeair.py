#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureSingaporeair.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
from CaptureBase import CaptureBase
import time
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from retrying import retry
from datetime import datetime
from urlparse import urljoin
from bs4 import BeautifulSoup

class CaptureSingaporeair(CaptureBase):
    home_url = 'http://www.singaporeair.com/en_UK/hk/home'
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Cache-Control:max-age=0
            Connection:keep-alive
            Host:www.singaporeair.com
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'singaporeair'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureSingaporeair, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureSingaporeair, self).__del__()

    '''
     function: 获取并存储首页滚动栏的商品信息
     @return: True or raise
     '''

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        result_datas = []
        try:
            page_source = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(page_source, 'lxml')
            pre_load_data = soup.findAll('figure', {'class': 'hero'})
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['LINK'.lower()] = urljoin(self.home_url, load_data.find('a').attrs['href'])
                    resultData['MAIN_IMAGE'.lower()] = urljoin(self.home_url, load_data.find('img').attrs['src'])
                    resultData['TITLE'.lower()] = load_data.find('img').attrs['alt']
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('get eLement error:{}'.format(e))
                    logger.error('load_data: {}'.format(load_data))
                    continue
            result_datas = self._rm_duplicate(result_datas, 'LINK'.lower())
            if len(result_datas) == 0:
                logger.error('page_source: {}'.format(page_source))
                raise ValueError('not get valid data')

            format_select = r'SELECT ID FROM {} WHERE CHANNEL="{{channel}}" and LINK="{{link}}" ORDER BY CREATE_TIME DESC'
            good_datas = result_datas
            select_sql = format_select.format(self.TABLE_NAME_BANNER)
            table = self.TABLE_NAME_BANNER
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'TITLE', 'CREATE_TIME', 'STATUS']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise

def main():
    startTime = datetime.now()
    objCaptureSingaporeair = CaptureSingaporeair(useragent)
    objCaptureSingaporeair.dealHomeGoods()
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

