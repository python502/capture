#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureUniqlo.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
import os
from CaptureBase import CaptureBase
import re
import time
import json
import copy
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

class CaptureUniqlo(CaptureBase):
    home_url = 'http://www.uniqlo.com/sg/'
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Cache-Control:max-age=0
            Connection:keep-alive
            Host:www.uniqlo.com
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'uniqlo'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureUniqlo, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureUniqlo, self).__del__()
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
            pre_load_data = soup.findAll('li', {'class': 'content_banner_item'})
            for load_data in pre_load_data:
                logger.debug('load_data: {}'.format(load_data))
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['STATUS'.lower()] = '01'
                try:
                    resultData['LINK'.lower()] = load_data.find('a').attrs['href']
                    resultData['TITLE'.lower()] = load_data.find('img').attrs['alt']
                    resultData['MAIN_IMAGE'.lower()] = urljoin(self.home_url, load_data.find('img').attrs['src'])
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                except Exception:
                    map_name = load_data.find('img').attrs['usemap'][1:]
                    maps = soup.find('map', {'name': map_name}).findAll('area')
                    for map in maps:
                        href = map.attrs['href']
                        if href.find('women') != -1:
                            resultData['MIN_IMAGE'.lower()] = 'WOMEN'
                        else:
                            resultData['MIN_IMAGE'.lower()] = 'MEN'
                        resultData['LINK'.lower()] = map.attrs['href']
                        resultData['TITLE'.lower()] = load_data.find('img').attrs['alt']
                        resultData['MAIN_IMAGE'.lower()] = urljoin(self.home_url, load_data.find('img').attrs['src'])
                        resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                        t_resultData = copy.copy(resultData)
                        result_datas.append(t_resultData)
                        continue
                    else:
                        continue
                result_datas.append(resultData)
            print len(result_datas)
            result_datas = self._rm_duplicate(result_datas, 'LINK'.lower())
            if len(result_datas) == 0:
                logger.error('page_source: {}'.format(page_source))
                raise ValueError('not get valid data')
            format_select = r'SELECT ID FROM {} WHERE CHANNEL="{{channel}}" and LINK="{{link}}" ORDER BY CREATE_TIME DESC'
            good_datas = result_datas
            select_sql = format_select.format(self.TABLE_NAME_BANNER)
            table = self.TABLE_NAME_BANNER
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'MIN_IMAGE', 'STATUS', 'TITLE']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise

def main():
    startTime = datetime.now()
    objCaptureUniqlo = CaptureUniqlo(useragent)
    objCaptureUniqlo.dealHomeGoods()
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

