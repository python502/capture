#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureRedmart.py
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
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

class CaptureRedmart(CaptureBase):
    department_url = 'https://api.redmart.com/v1.6.0/catalog/search?extent=0&depth=1'
    goods_url = 'https://api.redmart.com/v1.6.0/catalog/search?extent=2&pageSize={}&sort=1024&category={}'
    home_url = 'https://redmart.com/'
    product_url = 'https://redmart.com/product/'
    img_url = 'https://s3-ap-southeast-1.amazonaws.com/media.redmart.com/newmedia/150x/'
    white_department = ['RedMart Label', 'Christmas Shop', 'Fruit & Veg', 'Dairy & Chilled', 'Frozen',\
                        'Beverages', 'Food Cupboard', 'Meat & Seafood', 'Bakery', 'Beer, Wine & Spirits',\
                         'Health & Beauty', 'Household & Pet', 'Baby, Kids & Toys', 'Home & Outdoor']
    # white_department = [ u'RedMart Label']
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Connection:keep-alive
            Host:redmart.com
            User-Agent:{}
            '''
    Channel = 'redmart'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureRedmart, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureRedmart, self).__del__()

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory(self, department):
        goods_infos = []
        category = department[0]
        try:
            good_nums = 120
            pageurl = self.goods_url.format(good_nums/department[2], department[1])
            page_results = self.getGoodInfos(category, pageurl)
            goods_infos.extend(page_results)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(goods_infos)))
            return goods_infos
        except Exception, e:
            logger.error('categoryurl: {} error'.format(category))
            logger.error('dealCategory error: {}.'.format(e))
            return False

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
            page_source = self.getHtml(pageurl, self.header)
            goods_infos = json.loads(page_source)['productSets']
            for goods_info in goods_infos:
                for good in goods_info['products']:
                    result_data = {}
                    result_data['CHANNEL'.lower()] = self.Channel
                    result_data['KIND'.lower()] = category
                    result_data['SITE'.lower()] = goods_info['category']
                    result_data['STATUS'.lower()] = '01'
                    result_data['PRODUCT_ID'.lower()] = good['sku']
                    result_data['LINK'.lower()] = urljoin(self.product_url, good['details']['uri'])
                    # result_data['MAIN_IMAGE'.lower()] = urljoin(self.img_url, good['img']['name'])
                    result_data['MAIN_IMAGE'.lower()] = self.img_url+ good['img']['name'][1:]
                    result_data['NAME'.lower()] = good['title']
                    result_data['RESERVE'.lower()] = good['pricing'].get('savings_text')
                    result_data['Currency'.lower()] = 'USD'

                    if good['pricing']['promo_price']:
                        result_data['Before_AMOUNT'.lower()] = good['pricing']['price']
                        result_data['AMOUNT'.lower()] = good['pricing']['promo_price']
                    else:
                        result_data['Before_AMOUNT'.lower()] = good['pricing']['promo_price']
                        result_data['AMOUNT'.lower()] = good['pricing']['price']

                    result_data['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_data['DESCRIPTION'.lower()] = good['desc']
                    result_datas.append(result_data)
            if not result_datas:
                raise ValueError('get result_datas error')
            return result_datas
        except Exception, e:
            logger.error('getGoodInfos error:{},retry it'.format(e))
            raise

    def get_department(self):
        print '*'*40
        logger.info('get_department: {}'.format(self.__get_department()))
        print '*'*40

    '''
    function: 获取类别信息
    #[{'category': 'Arts, Crafts &amp; Sewing', 'nodeId': '2617941011'}, {'category': 'Automotive & Motorcycle', 'nodeId': '15684181'}]
    @return: dict_res 
    '''
    def __get_department(self):
        try:
            results = []
            html = self.getHtml(self.department_url, self.header)
            categories = json.loads(html)['categories']
            for categorie in categories:
                main_title = categorie['title'].encode('utf-8')
                main_uri = categorie['uri']
                if main_title not in self.white_department:
                    logger.error('kind {} not in white_department'.format(main_title))
                    continue
                results.append([main_title, main_uri, len(categorie['children'])])
            return results
        except Exception, e:
            logger.error('__get_department error: {}'.format(e))
            raise

    '''
    function: 获取所有分类的商品信息
    @
    @return: None
    '''
    def dealCategorys(self):
        try:
            departments = self.__get_department()
            logger.debug('departments: {}'.format(departments))
            resultDatas = []
            for department in departments:
                resultData = self.dealCategory(department)
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','DESCRIPTION','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','RESERVE','STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureRedmart = CaptureRedmart(useragent)
    # 获取所有类别id
    # objCaptureRedmart.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureRedmart.dealCategorys()
    # objCaptureRedmart.dealCategory(['Groceries', '/groceries/?q=sale', 19961])
    # print objCaptureRedmart.getGoodInfos('fdfd','https://www.lazada.sg/shop-fashion/?q=sale&itemperpage=60&page=0')
    # objCaptureRedmart.getHtml('https://api.redmart.com/v1.6.0/catalog/search?extent=0&depth=1', objCaptureRedmart.header)



    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

