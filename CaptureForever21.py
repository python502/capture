#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureForever21.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
from CaptureBase import CaptureBase
import time
import json
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

class CaptureForever21(CaptureBase):
    home_url = 'https://www.forever21.com/gl/Shop'
    banner_url = 'https://www.forever21.com/gl/shop/Home/SetHomepage?lang='
    products_url = 'https://www.forever21.com/gl/shop/Catalog/GetProducts'
    HEADER = '''
            accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            cache-control:max-age=0
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    HEADER_HOME = '''
            accept:application/json, text/javascript, */*; q=0.01
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            referer:https://www.forever21.com/gl/Shop
            User-Agent:{}
            '''
    HEADER_PRODUCTS = '''
            accept:application/json, text/javascript, */*; q=0.01
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            content-type:application/json
            origin:https://www.forever21.com
            referer:{}
            User-Agent:{}
            '''
    Channel = 'forever21'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureForever21, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))
        self.header_home = self._getDict4str(self.HEADER_HOME.format(self.user_agent))
    def __del__(self):
        super(CaptureForever21, self).__del__()
    '''
    function: 获取并存储首页滚动栏的商品信息
    @return: True or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        result_datas = []
        try:
            page_source = self.getHtml(self.banner_url, self.header_home)
            page_source = json.loads(page_source).get('HTML')
            soup = BeautifulSoup(page_source, 'lxml')
            pre_load_data = soup.findAll('img', {'class': 'show_desktop'})
            for load_data in pre_load_data:
                logger.debug('load_data: {}'.format(load_data))
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['STATUS'.lower()] = '01'
                resultData['LINK'.lower()] = urljoin(self.home_url, load_data.parent.attrs['href'])
                resultData['MAIN_IMAGE'.lower()] = urljoin(self.home_url, load_data.attrs['src'])
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
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'MIN_IMAGE', 'STATUS']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise
    def get_department(self):
        print '*'*40
        logger.info('get_department: {}'.format(self.__get_department()))
        print '*'*40
    '''
    function: 获取类别信息
    @return: dict_res 
    '''
    def __get_department(self):
        try:
            results = []
            html = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(html, 'lxml')
            catalogs = soup.findAll('div',{'class':"d_mega_menu"})
            for catalog in catalogs:
                kind = catalog.find('a').getText().strip()
                url = urljoin(self.home_url, catalog.find('a').attrs['href'])
                results.append([kind, url])
            return results
        except Exception, e:
            logger.error('__get_department error: {}'.format(e))
            raise

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
            resultDatas = []
            for department in departments:
                resultData = self.dealCategory(department)
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','DETAIL_IMAGE','DESCRIPTION','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')
    '''
    function: 查询并将商品入库
    @department: 存储需要信息的列表
    @return: True or False
    '''
    def dealCategory(self, department):
        goods_infos = []
        category = department[0]
        try:
            page_num = 120#每页显示多少商品
            get_page = 1

            for i in range(1, get_page+1):
                page_results = self.getGoodInfos(department, page_num, i)
                goods_infos.extend(page_results)
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
    @retry(stop_max_attempt_number=5, wait_fixed=3000)
    def getGoodInfos(self, department, pageSize, pageNo):
        category = department[0]
        product_url = 'https://www.forever21.com/gl/shop/Catalog/Product/sale/'
        img_url = 'https://www.forever21.com/images/default_330/'
        try:
            logger.debug('category: {}pageNo: {}'.format(category, pageNo))
            GetProducts = {
                'WOMEN': {"brand": "f21", "category": "app-main"},
                'MEN':{"brand": "21men", "category": "mens-main"},
                'PLUS SIZE':{"brand":"plus","category":"plus_size-main"},
                'GIRLS':{"brand":"girls","category":"girls_main"},
                'SALE':{"brand":"f21","category":"sale"}
            }
            data = {"page":{"pageNo":pageNo,"pageSize":pageSize},"filter":{"sizeList":[],"colorList":[],"price":{"minPrice":0,"maxPrice":250},"manualList":[]},"sort":{"sortType":""},"count":{"products":""}}
            data.update(GetProducts.get(category))
            header_produces = self._getDict4str(self.HEADER_PRODUCTS.format(department[1], self.user_agent))
            result_datas = []
            page_source = self.getHtml(self.products_url, header_produces, json.dumps(data))
            goods_infos = json.loads(page_source).get('CatalogProducts')
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = goods_info.get('CategoryName')
                resultData['STATUS'.lower()] = '01'
                try:
                    good_id = goods_info.get('ProductId')
                    resultData['LINK'.lower()] = urljoin(product_url, good_id)
                    resultData['PRODUCT_ID'.lower()] = good_id

                    resultData['MAIN_IMAGE'.lower()] = urljoin(img_url, goods_info.get('ImageFilename'))
                    resultData['NAME'.lower()] = goods_info.get('DisplayName')

                    resultData['AMOUNT'.lower()] = goods_info.get('ListPrice',0)
                    resultData['Currency'.lower()] = 'SGD'
                    resultData['Before_AMOUNT'.lower()] = goods_info.get('OriginalPrice',0)
                    resultData['DESCRIPTION'.lower()] = goods_info.get('ItemTag3')
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))

                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('error: {}'.format(e))
                    logger.error('goods_info: {}'.format(goods_info))
                    continue
            if len(goods_infos) != len(result_datas) or not result_datas:
                raise ValueError('get result_datas error')
            return result_datas
        except Exception, e:
            logger.error('getGoodInfos category error:{},retry it'.format(category, e))
            # logger.error('category: {},pageurl：{}'.format(category, pageurl))
            # logger.error('page_source: {}'.format(page_source))
            raise
def main():
    startTime = datetime.now()
    objCaptureForever21 = CaptureForever21(useragent)
    # objCaptureForever21.get_department()
    objCaptureForever21.dealCategorys()
    # objCaptureForever21.dealHomeGoods()
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

