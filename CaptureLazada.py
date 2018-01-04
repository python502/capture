#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureLazada.py
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

class CaptureLazada(CaptureBase):
    home_url = 'https://www.lazada.sg'
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Connection:keep-alive
            Host:www.lazada.sg
            User-Agent:{}
            '''
    Channel = 'lazada'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureLazada, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureLazada, self).__del__()

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
            pattern = re.compile('<script>window.pageData=(.*?)</script>', re.S)
            goods_infos = pattern.findall(page_source)
            goods_infos = json.loads(goods_infos[0].strip())['mods']['listItems']
            for goods_info in goods_infos:
                result_data = {}
                result_data['CHANNEL'.lower()] = self.Channel
                result_data['KIND'.lower()] = category
                result_data['SITE'.lower()] = 'lazada.sg'
                result_data['STATUS'.lower()] = '01'
                try:
                    result_data['PRODUCT_ID'.lower()] = goods_info['nid']
                    result_data['LINK'.lower()] = urljoin(self.home_url, goods_info['productUrl'])
                    result_data['MAIN_IMAGE'.lower()] = goods_info['image']
                    result_data['NAME'.lower()] = goods_info['name'].strip().strip('"').replace('\n', '').replace('"', '\'').replace('\\', '')
                    result_data['DISPLAY_COUNT'.lower()] = int(goods_info['review'])
                    result_data['Currency'.lower()] = 'SGD'
                    result_data['AMOUNT'.lower()] = float(goods_info.get('price','0'))
                    result_data['Before_AMOUNT'.lower()] = float(goods_info.get('originalPrice','0'))

                    result_data['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    description = goods_info.get('description', [])
                    description = [i.strip().strip('"').replace('\n', '').replace('"', '\'').replace('\\', '') for i in description if i.strip()]
                    # result_data['DESCRIPTION'.lower()] = goods_info.get('description', [])[0].strip().replace('\n', '') if goods_info.get('description') else ''
                    result_data['DESCRIPTION'.lower()] = ''.join(description)
                    result_data['RESERVE'.lower()] = goods_info.get('discount')
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
            results = []
            html = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(html, 'lxml')
            catalogs = soup.findAll('li', {'class':"lzd-site-menu-sub-item"})
            for catalog in catalogs:
                url = urljoin(self.home_url, catalog.find('a').attrs['href'])
                kind = catalog.find('span').getText().strip('\n').strip()
                result = [kind.encode('utf-8'), url]
                results.append(result)
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','DESCRIPTION','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','DISPLAY_COUNT','STATUS','RESERVE']
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
            pre_load_data = soup.findAll('a', {'exp-tracking': 'bannerSlider'})
            for load_data in pre_load_data:
                logger.debug('load_data: {}'.format(load_data))
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['STATUS'.lower()] = '01'
                resultData['LINK'.lower()] = urljoin(self.home_url, load_data.attrs['href'])
                try:
                    img_src = load_data.find('img').attrs['data-ks-lazyload']
                except KeyError:
                    img_src = load_data.find('img').attrs['src']
                resultData['MAIN_IMAGE'.lower()] = urljoin(self.home_url, img_src)
                resultData['TITLE'.lower()] = load_data.attrs['title']
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
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS', 'TITLE']
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

    objCaptureLazada = CaptureLazada(useragent)
    # 获取所有类别id
    # objCaptureLazada.get_department()
    # # 查询并入库所有类别的商品信息
    objCaptureLazada.dealCategorys()
    objCaptureLazada.dealHomeGoods()
    # objCaptureLazada.dealCategory(['Groceries', '/groceries/?q=sale', 19961])
    # print objCaptureLazada.getGoodInfos('fdfd','https://www.lazada.sg/shop-feeding/')
    # print objCaptureLazada.getHtml('https://www.lazada.sg/shop-feeding/',objCaptureLazada.header)

    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

