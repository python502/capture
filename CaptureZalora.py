#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CapturZalora.py
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

class CaptureZalora(CaptureBase):
    department_url = 'https://www.lazada.sg/catalog/?q=sale'
    home_url = 'https://www.zalora.sg'
    HEADER = '''
            accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            upgrade-insecure-requests:1
            User-Agent:{}
            '''
    Channel = 'Zalora'
    Type = 'TOP SEARCHES'#'TOP SEARCHES'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureZalora, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureZalora, self).__del__()

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory(self, department):
        category = department[0]
        page_url = department[1]
        try:
            page_results = self.getGoodInfos(category, page_url)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(page_results)))
            return page_results
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
            pattern = re.compile('\'initialData\': (.*)', re.M)
            goods_infos = pattern.findall(page_source)
            if not goods_infos:
                logger.error('pageurl :{} get None googs'.format(pageurl))
                return []
            goods_infos = json.loads(goods_infos[0].strip())['result']['response']['docs']
            for goods_info in goods_infos:
                result_data = {}
                result_data['CHANNEL'.lower()] = self.Channel
                result_data['KIND'.lower()] = category
                result_data['SITE'.lower()] = self.Type
                result_data['STATUS'.lower()] = '01'
                try:
                    result_data['PRODUCT_ID'.lower()] = goods_info['meta']['id_catalog_config']
                    result_data['LINK'.lower()] = goods_info['link']
                    result_data['MAIN_IMAGE'.lower()] = goods_info['image']
                    result_data['NAME'.lower()] = goods_info['meta']['name']
                    result_data['Currency'.lower()] = 'SGD'
                    if goods_info['meta'].get('special_price', '0'):
                        result_data['AMOUNT'.lower()] = float(goods_info['meta'].get('special_price', '0').replace(',', ''))
                        result_data['Before_AMOUNT'.lower()] = float(goods_info['meta'].get('price', '0').replace(',', ''))
                    else:
                        result_data['AMOUNT'.lower()] = float(goods_info['meta'].get('price', '0').replace(',', ''))
                        result_data['Before_AMOUNT'.lower()] = 0
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
            results_brands = []
            results_searches = []
            html = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(html, 'lxml')
            catalogs = soup.find('div', {'class': "ftBrands lfloat box ptm"}).findAll('li')
            for catalog in catalogs:
                try:
                    #TOP SEARCHES
                    catalog.attrs['class']
                    url = urljoin(self.home_url, catalog.find('a').attrs['href'])
                    kind = catalog.find('a').attrs['title'].strip()
                    result = [kind.encode('utf-8'), url]
                    results_searches.append(result)
                except KeyError:
                    #TOP BRANDS
                    url = urljoin(self.home_url, catalog.find('a').attrs['href'])
                    kind = catalog.find('a').attrs['title'].strip()
                    result = [kind.encode('utf-8'), url]
                    results_brands.append(result)
            return results_brands if self.Type == 'TOP BRANDS' else results_searches
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')

def main():
    startTime = datetime.now()
    objCaptureZalora = CaptureZalora(useragent)
    # 获取所有类别id
    # objCaptureZalora.get_department()
    # # 查询并入库所有类别的商品信息
    objCaptureZalora.dealCategorys()

    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

