#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureAlthea.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
import os
from CaptureBase import CaptureBase
import re
import json
import time
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

class CaptureAlthea(CaptureBase):
    home_url = 'https://sg.althea.kr/'
    best_sellers_url = 'http://sg.althea.kr/best-sellers'
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Cache-Control:max-age=0
            Connection:keep-alive
            Host:sg.althea.kr
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'althea'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureAlthea, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureAlthea, self).__del__()

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory(self, category):
        page_source = self.getHtml(self.home_url, self.header)
        soup = BeautifulSoup(page_source, 'lxml')
        amount = soup.find('div', {'class': 'amount hidden-xs'}).find('strong').getText().strip('\n').strip()
        pattern = re.compile('\d+', re.M)
        count = int(pattern.findall(amount)[0])
        page_num = 36
        page_need = 4
        if count%36 == 0:
            page = count/36
        else:
            page = count / 36+1
        url_format = 'http://sg.althea.kr/best-sellers?limit={}&p={{}}'.format(page_num)
        page = min(page, page_need)+1
        page_results = []
        try:
            for i in range(1, page):
                page_url = url_format.format(i)
                page_result = self.getGoodInfos(category, page_url)
                page_results.extend(page_result)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(page_results)))
            return page_results
        except Exception, e:
            logger.error('dealCategory category: {} error: {}.'.format(category, e))
            return []

    '''
    function: 获取单页商品信息
    @category： 分类名
    @firsturl： 商品页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getGoodInfos(self, category, pageurl):
        try:
            logger.debug('pageurl: {}'.format(pageurl))
            result_datas = []
            page_source = self.getHtmlselenium(pageurl)
            soup = BeautifulSoup(page_source, 'lxml')
            goods_infos = soup.findAll('div', {'class': 'product-item'})
            for goods_info in goods_infos:
                result_data = {}
                result_data['CHANNEL'.lower()] = self.Channel
                result_data['KIND'.lower()] = category
                result_data['SITE'.lower()] = 'Best Sellers'
                result_data['STATUS'.lower()] = '01'
                result_data['Currency'.lower()] = 'SGD'
                top_actions_inner = goods_info.find('div', {'class': 'top-actions-inner'})
                products_list = goods_info.find('div', {'class': 'content products-list'})
                try:
                    result_data['PRODUCT_ID'.lower()] = top_actions_inner.find('div', {'class':'yotpo bottomLine yotpo-small'}).attrs['data-product-id']
                    result_data['LINK'.lower()] = top_actions_inner.find('a').attrs['href']
                    result_data['MAIN_IMAGE'.lower()] = products_list.find('img').attrs['src']
                    result_data['NAME'.lower()] = top_actions_inner.find('a').attrs['title']
                    try:
                        PriceInfo = top_actions_inner.find('div', {'class': 'special-price'}).find('span', {'class': 'price'}).getText().strip('\n').strip()
                        PriceInfo = PriceInfo[2:] if PriceInfo.startswith('S$') else PriceInfo
                        good_DealPrice = float(PriceInfo)
                        result_data['AMOUNT'.lower()] = good_DealPrice
                    except Exception:
                        try:
                            PriceInfo = top_actions_inner.find('span', {'class': 'regular-price'}).find('span', {
                                'class': 'price'}).getText().strip('\n').strip()
                            PriceInfo = PriceInfo[2:] if PriceInfo.startswith('S$') else PriceInfo
                            good_DealPrice = float(PriceInfo)
                            result_data['AMOUNT'.lower()] = good_DealPrice
                        except Exception:
                            result_data['AMOUNT'.lower()] = 0

                    try:
                        PriceInfo = top_actions_inner.find('div', {'class': 'old-price'}).find('div', {'class': 'price'}).getText().strip('\n').strip()
                        PriceInfo = PriceInfo[2:] if PriceInfo.startswith('S$') else PriceInfo
                        good_maxDealPrice = float(PriceInfo)
                        result_data['Before_AMOUNT'.lower()] = good_maxDealPrice
                    except Exception:
                        result_data['Before_AMOUNT'.lower()] = 0

                    try:
                        display_count = top_actions_inner.find('a', {'class':"text-m"}).getText().strip()
                        pattern = re.compile('\d+', re.M)
                        result_data['DISPLAY_COUNT'.lower()] = int(pattern.findall(display_count)[0])
                    except Exception:
                        result_data['DISPLAY_COUNT'.lower()] = 0

                    try:
                        result_data['RESERVE'.lower()] = top_actions_inner.find('span', {'class':"discount-per"}).getText().strip('\n').strip()
                    except Exception:
                        result_data['RESERVE'.lower()] = ''

                    result_data['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(result_data)
                except Exception, e:
                    logger.error('error: {}'.format(e))
                    logger.error('goods_info: {}'.format(goods_info))
                    continue
            if len(goods_infos) != len(result_datas) or not result_datas:
                logger.error('len goods_infos: {},len result_datas: {}'.format(goods_infos, result_datas))
                logger.error('result_datas: {}'.format(result_datas))
                raise ValueError('get result_datas error')
            return result_datas
        except Exception, e:
            logger.error('getGoodInfos error:{},retry it'.format(e))
            raise

    '''
    function: 获取所有分类的商品信息
    @
    @return: None
    '''
    def dealCategorys(self):
        try:
            category = 'best-sellers'
            resultDatas = self.dealCategory(category)
            logger.info('all categorys get data: {}'.format(len(resultDatas)))
            resultDatas = self._rm_duplicate(resultDatas, 'PRODUCT_ID'.lower())
            logger.info('After the data remove duplicates: {}'.format(len(resultDatas)))
            if not resultDatas:
                raise ValueError('dealCategorys get no resultDatas ')
            format_select = 'SELECT ID,STATUS FROM {} WHERE CHANNEL="{{channel}}" AND PRODUCT_ID="{{product_id}}" ORDER BY CREATE_TIME DESC'
            good_datas = resultDatas
            select_sql = format_select.format(self.TABLE_NAME_PRODUCT)
            table = self.TABLE_NAME_PRODUCT
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','STATUS','RESERVE']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')


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
            pre_load_data = soup.findAll('div', {'class': 'hidden-xs'})
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['LINK'.lower()] = load_data.find('a').attrs['href']
                    resultData['MAIN_IMAGE'.lower()] = load_data.find('img').attrs['src']
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('get eLement error:{}'.format(e))
                    logger.error('goodData: {}'.format(load_data))
                    continue
            result_datas = self._rm_duplicate(result_datas, 'LINK'.lower())
            if len(result_datas) == 0:
                logger.error('page_source: {}'.format(page_source))
                raise ValueError('not get valid data')

            format_select = r'SELECT ID FROM {} WHERE CHANNEL="{{channel}}" and LINK="{{link}}" ORDER BY CREATE_TIME DESC'
            good_datas = result_datas
            select_sql = format_select.format(self.TABLE_NAME_BANNER)
            table = self.TABLE_NAME_BANNER
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureAlthea = CaptureAlthea(useragent)
    # 查询并入库所有类别的商品信息
    objCaptureAlthea.dealCategorys()
    # objCaptureAlthea.dealHomeGoods()
    # html = objCaptureAlthea.getHtml('https://sg.althea.kr/', objCaptureAlthea.header)
    # print html


    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

