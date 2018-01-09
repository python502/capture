#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureAgoda.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
from CaptureBase import CaptureBase
import time
import re
import json
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from retrying import retry
from datetime import datetime
from urlparse import urljoin

class CaptureAgoda(CaptureBase):
    home_url = 'https://www.agoda.com/zh-cn/deals'
    HEADER = '''
            accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'agoda'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureAgoda, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureAgoda, self).__del__()
    '''
    function: 获取并存储首页滚动栏的商品信息
    @return: True or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        result_datas = []
        try:
            page_source = self.getHtml(self.home_url, self.header)
            pattern_d = re.compile(r'var promoInboxPageParams = \{.*?\},"isMobile":false}', re.M)
            home_infos = json.loads(pattern_d.findall(page_source)[0][len('var promoInboxPageParams = '):])
            home_infos = home_infos.get('sections')
            pre_load_data = home_infos[1].get('promotions')
            for load_data in pre_load_data:
                logger.debug('load_data: {}'.format(load_data))
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['STATUS'.lower()] = '01'
                resultData['LINK'.lower()] = urljoin(self.home_url, load_data.get('searchUrl'))
                resultData['TITLE'.lower()] = load_data.get('infoTitle')
                resultData['MAIN_IMAGE'.lower()] = load_data.get('image').get('url').get('default')
                resultData['MIN_IMAGE'.lower()] = load_data.get('image').get('url').get('highRes')
                resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                result_datas.append(resultData)
            result_datas = self._rm_duplicate(result_datas, 'LINK'.lower())
            if len(result_datas) == 0:
                logger.error('page_source: {}'.format(page_source))
                raise ValueError('not get valid data')
            format_select = r'SELECT ID FROM {} WHERE CHANNEL="{{channel}}" and LINK="{{link}}" ORDER BY CREATE_TIME DESC'
            good_datas = result_datas
            select_sql = format_select.format(self.TABLE_NAME_BANNER)
            table = self.TABLE_NAME_BANNER
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS', 'TITLE', 'MIN_IMAGE']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise

def main():
    startTime = datetime.now()
    objCaptureAgoda = CaptureAgoda(useragent)
    objCaptureAgoda.dealHomeGoods()
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

