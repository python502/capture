#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureStyletribute.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
from CaptureBase import CaptureBase
import time
from CrawlingProxy import useragent
from logger import logger
from retrying import retry
from datetime import datetime
from urlparse import urljoin
from decimal import Decimal
import json

class CaptureStyletribute(CaptureBase):
    home_url = 'https://styletribute.com/'
    img_url = 'https://styletribute.com/cms/'
    bunner_url = 'https://styletribute.com/cms/wardrobes.json'
    product_url = 'https://styletribute.com/product/'
    country_url = 'https://api.styletribute.com/currencies'
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Host:api.styletribute.com
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    BUNNER_HEADER = '''
            Accept:application/json, text/plain, */*
            Referer:https://styletribute.com/
            User-Agent:{}
            '''
    RATE_HEADER = '''
            Accept:application/json, text/plain, */*
            # Origin:https://styletribute.com
            # Referer:https://styletribute.com/
            User-Agent:{}
            '''
    Country = 'HK'
    Channel = 'styletribute'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureStyletribute, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))
        self.bunner_header = self._getDict4str(self.BUNNER_HEADER.format(self.user_agent))
        self.rate_header = self._getDict4str(self.RATE_HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureStyletribute, self).__del__()

    '''
    function: 获取并存储首页滚动栏的商品信息
    @return: True or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        result_datas = []
        try:
            page_source = self.getHtml(self.bunner_url, self.bunner_header)
            pre_load_data = json.loads(page_source)
            for load_data in pre_load_data:
                logger.debug('load_data: {}'.format(load_data))
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['STATUS'.lower()] = '01'
                resultData['LINK'.lower()] = urljoin(self.home_url, load_data['url'])
                resultData['TITLE'.lower()] = load_data['name']
                resultData['MAIN_IMAGE'.lower()] = urljoin(self.img_url, load_data['img'])
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
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'MIN_IMAGE', 'STATUS', 'TITLE']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise


    '''
    function: 查询并将商品入库
    @department: 存储需要信息的列表
    @return: True or False
    '''
    def dealCategory(self, department, rate):
        goods_infos = []
        category = department[0]
        page_num = 2
        page_good = 100
        try:
            for i in range(page_num):
                begin_count = i*page_good
                page_url = 'https://api.styletribute.com/elasticsearch/query/{}/{}/%7B%22category%22%3A%5B%22{}%22%5D%7D'.format(page_good, begin_count, category)
                [total_pages, page_results] = self.getGoodInfos(category, page_url, rate)
                goods_infos.extend(page_results)

                if total_pages == i+1:
                    break
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(goods_infos)))
            return goods_infos
        except Exception, e:
            logger.error('category: {} error'.format(category))
            logger.error('dealCategory error: {}.'.format(e))
            return False

    '''
    function: 获取单页商品信息
    @category： 分类名
    @firsturl： 商品页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=3000)
    def getGoodInfos(self, category, page_url, rate):
        result_datas = []
        try:
            iterms_infos = self.getHtml(page_url, self.header)
            total_pages = json.loads(iterms_infos)['products']['total_pages']
            goods_infos = json.loads(iterms_infos)['products']['products']
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 'category'
                resultData['STATUS'.lower()] = '01'
                resultData['NAME'.lower()] = goods_info.get('name')
                resultData['LINK'.lower()] = urljoin(self.product_url, goods_info.get('urlpath'))
                resultData['PRODUCT_ID'.lower()] = goods_info.get('id')
                resultData['MAIN_IMAGE'.lower()] = urljoin(self.img_url, goods_info.get('exportImage'))

                BeforePriceInfo = goods_info.get('oldPrice', 0)
                resultData['Before_AMOUNT'.lower()] = Decimal(BeforePriceInfo*rate).quantize(Decimal('0'))

                PriceInfo = goods_info.get('price', 0)
                resultData['AMOUNT'.lower()] = Decimal(PriceInfo*rate).quantize(Decimal('0'))

                resultData['Currency'.lower()] = self.Country
                resultData['DISPLAY_COUNT'.lower()] = goods_info.get('wishlist_count')
                resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                result_datas.append(resultData)
            if not result_datas:
                raise ValueError('get result_datas error')
            return [total_pages, result_datas]
        except Exception, e:
            logger.error('getGoodInfos error:{}'.format(e))
            logger.error('category: {},page_url: {}'.format(category, page_url))
            raise

    def get_department(self):
        print '*'*40
        logger.info('get_department: {}'.format(self.__get_department()))
        print '*'*40


    '''
    function: 获取类别信息
    #[[,],[,]]
    @return: dict_res 
    '''
    def __get_department(self):
        # categorys = [['women', 'https://styletribute.com/s/category/women'], ['men', 'https://styletribute.com/s/category/men'], ['kids', 'https://styletribute.com/s/category/kids']]
        categorys = [['women'], ['men'], ['kids']]
        return categorys
    def __get_rate(self, country):
        iterms_infos = self.getHtml(self.country_url, self.rate_header)
        infos = json.loads(iterms_infos)
        for info in infos:
            if info['country'] == country:
                return info['rate']
    '''
    function: 获取所有分类的商品信息
    @
    @return: None
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealCategorys(self):
        try:
            departments = self.__get_department()
            logger.debug('departments: {}'.format(departments))
            rate = self.__get_rate(self.Country)
            logger.debug('rate: {}'.format(rate))
            resultDatas = []
            for department in departments:
                resultData = self.dealCategory(department, rate)
                if not resultData:
                    logger.error('deal one categorys error: {}'.format(department))
                    continue
                resultDatas.extend(resultData)
            logger.info('all categorys get data: {}'.format(len(resultDatas)))
            resultDatas = self._rm_duplicate(resultDatas, 'PRODUCT_ID'.lower())
            logger.info('After the data remove duplicates: {}'.format(len(resultDatas)))
            if not resultDatas:
                raise ValueError('dealCategorys get no resultDatas ')

            format_select = 'SELECT ID,STATUS FROM {} WHERE CHANNEL="{{channel}}" AND PRODUCT_ID="{{product_id}}" ORDER BY CREATE_TIME DESC'
            good_datas = resultDatas
            select_sql = format_select.format(self.TABLE_NAME_PRODUCT)
            table = self.TABLE_NAME_PRODUCT
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME', 'Currency', 'AMOUNT','Before_AMOUNT','CREATE_TIME','STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')


def main():
    startTime = datetime.now()
    objCaptureStyletribute = CaptureStyletribute(useragent)
    objCaptureStyletribute.dealCategorys()
    # objCaptureStyletribute.dealHomeGoods()
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

